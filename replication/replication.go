package replication

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"turnstone/protocol"
	"turnstone/store"
)

type ReplicaSource struct {
	LocalPartition  string
	RemotePartition string
}

type ReplicationManager struct {
	mu         sync.Mutex
	peers      map[string][]ReplicaSource // Address -> []ReplicaSource
	cancelFunc map[string]context.CancelFunc
	stores     map[string]*store.Store
	tlsConf    *tls.Config
	logger     *slog.Logger
}

func NewReplicationManager(stores map[string]*store.Store, tlsConf *tls.Config, logger *slog.Logger) *ReplicationManager {
	return &ReplicationManager{
		peers:      make(map[string][]ReplicaSource),
		cancelFunc: make(map[string]context.CancelFunc),
		stores:     stores,
		tlsConf:    tlsConf,
		logger:     logger,
	}
}

func (rm *ReplicationManager) Start() {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	for addr := range rm.peers {
		rm.spawnConnection(addr)
	}
}

func (rm *ReplicationManager) AddReplica(partitionName, sourceAddr, sourcePartition string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	parts, _ := rm.peers[sourceAddr]
	found := false
	for _, p := range parts {
		if p.LocalPartition == partitionName {
			found = true
			break
		}
	}

	if !found {
		rm.peers[sourceAddr] = append(rm.peers[sourceAddr], ReplicaSource{LocalPartition: partitionName, RemotePartition: sourcePartition})
		if cancel, exists := rm.cancelFunc[sourceAddr]; exists {
			cancel()
		}
		rm.spawnConnection(sourceAddr)
		rm.logger.Info("Added replica source", "partition", partitionName, "source", sourceAddr, "remote_partition", sourcePartition)
	}
}

func (rm *ReplicationManager) StopReplication(partitionName string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	for addr, parts := range rm.peers {
		newParts := make([]ReplicaSource, 0, len(parts))
		changed := false
		for _, p := range parts {
			if p.LocalPartition != partitionName {
				newParts = append(newParts, p)
			} else {
				changed = true
			}
		}

		if changed {
			rm.peers[addr] = newParts
			rm.logger.Info("Stopped replication", "partition", partitionName, "source", addr)

			if cancel, exists := rm.cancelFunc[addr]; exists {
				cancel()
			}

			if len(newParts) > 0 {
				rm.spawnConnection(addr)
			} else {
				delete(rm.peers, addr)
				delete(rm.cancelFunc, addr)
			}
			return
		}
	}
}

func (rm *ReplicationManager) IsReplica(partitionName string) bool {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	for _, sources := range rm.peers {
		for _, src := range sources {
			if src.LocalPartition == partitionName {
				return true
			}
		}
	}
	return false
}

func (rm *ReplicationManager) spawnConnection(addr string) {
	ctx, cancel := context.WithCancel(context.Background())
	rm.cancelFunc[addr] = cancel

	go rm.maintainConnection(ctx, addr)
}

func (rm *ReplicationManager) maintainConnection(ctx context.Context, addr string) {
	for {
		if ctx.Err() != nil {
			return
		}

		rm.mu.Lock()
		if _, ok := rm.peers[addr]; !ok {
			rm.mu.Unlock()
			return
		}
		parts := make([]ReplicaSource, len(rm.peers[addr]))
		copy(parts, rm.peers[addr])
		rm.mu.Unlock()

		if len(parts) == 0 {
			return
		}

		if err := rm.connectAndSync(ctx, addr, parts); err != nil {
			if ctx.Err() != nil {
				return
			}
			rm.logger.Error("Replication sync failed", "peer", addr, "err", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(3 * time.Second):
			}
		}
	}
}

func (rm *ReplicationManager) connectAndSync(ctx context.Context, addr string, parts []ReplicaSource) error {
	dialer := net.Dialer{Timeout: 5 * time.Second}
	conn, err := tls.DialWithDialer(&dialer, "tcp", addr, rm.tlsConf)
	if err != nil {
		return err
	}
	go func() {
		<-ctx.Done()
		conn.Close()
	}()
	defer conn.Close()

	rm.logger.Info("Connected to Primary for replication", "addr", addr, "count", len(parts))

	remoteToLocal := make(map[string][]string)

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint32(1))
	binary.Write(buf, binary.BigEndian, uint32(len(parts)))

	for _, cfg := range parts {
		stats := rm.stores[cfg.LocalPartition].Stats()
		logSeq := stats.NextLogSeq - 1
		binary.Write(buf, binary.BigEndian, uint32(len(cfg.RemotePartition)))
		buf.WriteString(cfg.RemotePartition)
		binary.Write(buf, binary.BigEndian, logSeq)
		rm.logger.Debug("Sending Hello for Partition", "remote_partition", cfg.RemotePartition, "local_partition", cfg.LocalPartition, "startLogSeq", logSeq)

		remoteToLocal[cfg.RemotePartition] = append(remoteToLocal[cfg.RemotePartition], cfg.LocalPartition)
	}

	header := make([]byte, 5)
	header[0] = protocol.OpCodeReplHello
	binary.BigEndian.PutUint32(header[1:], uint32(buf.Len()))
	if _, err := conn.Write(header); err != nil {
		return err
	}
	if _, err := conn.Write(buf.Bytes()); err != nil {
		return err
	}

	respHead := make([]byte, 5)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if _, err := io.ReadFull(conn, respHead); err != nil {
			return err
		}
		if respHead[0] == protocol.OpCodeReplBatch {
			ln := binary.BigEndian.Uint32(respHead[1:])
			payload := make([]byte, ln)
			if _, err := io.ReadFull(conn, payload); err != nil {
				return err
			}

			cursor := 0
			if cursor+4 > len(payload) {
				return fmt.Errorf("malformed batch: no partition len")
			}
			nLen := int(binary.BigEndian.Uint32(payload[cursor : cursor+4]))
			cursor += 4
			if cursor+nLen+4 > len(payload) {
				return fmt.Errorf("malformed batch: no partition name or count")
			}
			remotePartitionName := string(payload[cursor : cursor+nLen])
			cursor += nLen
			count := binary.BigEndian.Uint32(payload[cursor : cursor+4])
			cursor += 4
			data := payload[cursor:]

			localPartitionNames, ok := remoteToLocal[remotePartitionName]
			if !ok {
				rm.logger.Warn("Received batch for unknown remote partition", "remote_partition", remotePartitionName)
				continue
			}

			for _, localPartitionName := range localPartitionNames {
				if st, ok := rm.stores[localPartitionName]; ok {
					if maxID, err := applyReplicationBatch(st, count, data); err == nil {
						ackBuf := new(bytes.Buffer)
						binary.Write(ackBuf, binary.BigEndian, uint32(len(remotePartitionName)))
						ackBuf.WriteString(remotePartitionName)
						binary.Write(ackBuf, binary.BigEndian, maxID)

						h := make([]byte, 5)
						h[0] = protocol.OpCodeReplAck
						binary.BigEndian.PutUint32(h[1:], uint32(ackBuf.Len()))
						if _, err := conn.Write(h); err != nil {
							return err
						}
						if _, err := conn.Write(ackBuf.Bytes()); err != nil {
							return err
						}
					} else {
						rm.logger.Error("Failed to apply replication batch", "local_partition", localPartitionName, "err", err)
					}
				}
			}
		} else {
			rm.logger.Warn("Unexpected OpCode in replication stream", "op", respHead[0])
		}
	}
}

func applyReplicationBatch(store *store.Store, count uint32, data []byte) (uint64, error) {
	cursor := 0
	var ops []protocol.LogEntry
	var maxID uint64

	for i := 0; i < int(count); i++ {
		if cursor+9 > len(data) {
			return 0, fmt.Errorf("malformed batch entry header")
		}
		lid := binary.BigEndian.Uint64(data[cursor : cursor+8])
		op := data[cursor+8]
		cursor += 9

		if cursor+4 > len(data) {
			return 0, fmt.Errorf("malformed batch entry key len")
		}
		kLen := int(binary.BigEndian.Uint32(data[cursor : cursor+4]))
		cursor += 4

		if cursor+kLen > len(data) {
			return 0, fmt.Errorf("malformed batch entry key")
		}
		key := make([]byte, kLen)
		copy(key, data[cursor:cursor+kLen])
		cursor += kLen

		if cursor+4 > len(data) {
			return 0, fmt.Errorf("malformed batch entry val len")
		}
		vLen := int(binary.BigEndian.Uint32(data[cursor : cursor+4]))
		cursor += 4

		var val []byte
		if op != protocol.OpJournalDelete {
			if cursor+vLen > len(data) {
				return 0, fmt.Errorf("malformed batch entry val")
			}
			val = make([]byte, vLen)
			copy(val, data[cursor:cursor+vLen])
			cursor += vLen
		}
		ops = append(ops, protocol.LogEntry{LogSeq: lid, OpCode: op, Key: key, Value: val})
		if lid > maxID {
			maxID = lid
		}
	}

	return maxID, store.ReplicateBatch(ops)
}
