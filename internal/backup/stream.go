// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package backup

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"time"

	"turnstone/protocol"
)

type StreamOptions struct {
	Host     string
	DBName   string
	StartLSN uint64
	WaitIdle time.Duration
	ClientID string
	TLS      *tls.Config
}

type StreamResult struct {
	BaseLSN uint64
	EndLSN  uint64
	Bytes   int64
}

func StreamLogRange(ctx context.Context, opts StreamOptions, writer io.Writer) (StreamResult, error) {
	if opts.TLS == nil {
		return StreamResult{}, fmt.Errorf("TLS config is required")
	}
	if opts.ClientID == "" {
		opts.ClientID = "turnstone-backup"
	}
	if opts.WaitIdle <= 0 {
		opts.WaitIdle = 2 * time.Second
	}

	dialer := net.Dialer{Timeout: 10 * time.Second}
	conn, err := tls.DialWithDialer(&dialer, "tcp", opts.Host, opts.TLS)
	if err != nil {
		return StreamResult{}, fmt.Errorf("connect: %w", err)
	}

	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.SetWriteDeadline(time.Now().Add(500 * time.Millisecond))
			sendQuit(conn)
			conn.Close()
		case <-done:
		}
	}()
	defer conn.Close()

	if err := sendHello(conn, opts.ClientID, opts.DBName, opts.StartLSN); err != nil {
		return StreamResult{}, fmt.Errorf("handshake: %w", err)
	}

	reader := bufio.NewReader(conn)
	respHeader := make([]byte, 5)
	if _, err := io.ReadFull(reader, respHeader); err != nil {
		return StreamResult{}, fmt.Errorf("read handshake response: %w", err)
	}
	if respHeader[0] != protocol.ResStatusOK {
		body := make([]byte, binary.BigEndian.Uint32(respHeader[1:]))
		if _, err := io.ReadFull(reader, body); err != nil {
			return StreamResult{}, fmt.Errorf("handshake rejected")
		}
		return StreamResult{}, fmt.Errorf("handshake rejected: %s", string(body))
	}
	if ln := binary.BigEndian.Uint32(respHeader[1:]); ln > 0 {
		if _, err := io.ReadFull(reader, make([]byte, ln)); err != nil {
			return StreamResult{}, fmt.Errorf("read handshake body: %w", err)
		}
	}

	result := StreamResult{BaseLSN: opts.StartLSN, EndLSN: opts.StartLSN}
	lastDataTime := time.Now()

	for {
		if ctx.Err() != nil {
			return result, ctx.Err()
		}

		_ = conn.SetReadDeadline(lastDataTime.Add(opts.WaitIdle))
		if _, err := io.ReadFull(reader, respHeader); err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				break
			}
			if err == io.EOF {
				break
			}
			return result, fmt.Errorf("read header: %w", err)
		}

		opCode := respHeader[0]
		length := binary.BigEndian.Uint32(respHeader[1:])
		payload := make([]byte, length)
		if _, err := io.ReadFull(reader, payload); err != nil {
			return result, fmt.Errorf("read payload: %w", err)
		}

		if opCode == protocol.ResStatusErr {
			return result, fmt.Errorf("server error: %s", string(payload))
		}

		if opCode != protocol.OpCodeReplSafePoint && opCode != protocol.OpCodeReplLogRange {
			continue
		}
		if len(payload) < 4 {
			return StreamResult{}, fmt.Errorf("payload too short for CRC")
		}
		crcReceived := binary.BigEndian.Uint32(payload[:4])
		rawBody := payload[4:]
		if crc32.Checksum(rawBody, protocol.Crc32Table) != crcReceived {
			return StreamResult{}, fmt.Errorf("CRC mismatch on replication stream")
		}

		switch opCode {
		case protocol.OpCodeReplSafePoint:
		case protocol.OpCodeReplLogRange:
			endOff, segData, err := parseLogRangePayload(rawBody, opts.DBName)
			if err != nil {
				return result, err
			}
			if len(segData) == 0 {
				continue
			}
			if _, err := writer.Write(segData); err != nil {
				return result, fmt.Errorf("write backup data: %w", err)
			}
			result.Bytes += int64(len(segData))
			result.EndLSN = endOff
			lastDataTime = time.Now()
		}
	}

	if result.EndLSN > opts.StartLSN {
		_ = sendReplAck(conn, opts.DBName, result.EndLSN)
	}
	sendQuit(conn)
	return result, nil
}

func parseLogRangePayload(rawBody []byte, wantDB string) (endOff uint64, segData []byte, err error) {
	cursor := 0
	if cursor+4 > len(rawBody) {
		return 0, nil, fmt.Errorf("malformed log range packet")
	}
	nLen := int(binary.BigEndian.Uint32(rawBody[cursor : cursor+4]))
	cursor += 4
	if cursor+nLen+4+16 > len(rawBody) {
		return 0, nil, fmt.Errorf("malformed log range db name")
	}
	dbName := string(rawBody[cursor : cursor+nLen])
	cursor += nLen + 4
	if dbName != wantDB {
		return 0, nil, fmt.Errorf("unexpected database in stream: %s", dbName)
	}
	startOff := binary.BigEndian.Uint64(rawBody[cursor : cursor+8])
	endOff = binary.BigEndian.Uint64(rawBody[cursor+8 : cursor+16])
	cursor += 16
	segData = rawBody[cursor:]
	if endOff < startOff || int(endOff-startOff) != len(segData) {
		return 0, nil, fmt.Errorf("log range offset mismatch")
	}
	return endOff, segData, nil
}

func sendHello(conn net.Conn, clientID, dbName string, startLSN uint64) error {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint32(1))
	binary.Write(buf, binary.BigEndian, uint32(len(clientID)))
	buf.WriteString(clientID)
	binary.Write(buf, binary.BigEndian, uint32(1))
	binary.Write(buf, binary.BigEndian, uint32(len(dbName)))
	buf.WriteString(dbName)
	binary.Write(buf, binary.BigEndian, startLSN)

	header := make([]byte, 5)
	header[0] = protocol.OpCodeReplHello
	binary.BigEndian.PutUint32(header[1:], uint32(buf.Len()))
	if _, err := conn.Write(header); err != nil {
		return err
	}
	_, err := conn.Write(buf.Bytes())
	return err
}

func sendReplAck(conn net.Conn, dbName string, offset uint64) error {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint32(len(dbName)))
	buf.WriteString(dbName)
	binary.Write(buf, binary.BigEndian, offset)

	header := make([]byte, 5)
	header[0] = protocol.OpCodeReplAck
	binary.BigEndian.PutUint32(header[1:], uint32(buf.Len()))
	if _, err := conn.Write(header); err != nil {
		return err
	}
	_, err := conn.Write(buf.Bytes())
	return err
}

func sendQuit(conn net.Conn) {
	header := make([]byte, 5)
	header[0] = protocol.OpCodeQuit
	binary.BigEndian.PutUint32(header[1:], 0)
	_, _ = conn.Write(header)
}
