package server

import (
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"turnstone/protocol"
)

// Helper to check key consistency
func verifyKeys(t *testing.T, count int, c *testClient, prefix string) {
	t.Helper()
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("%s%d", prefix, i)
		val := readKey(t, c, key)
		if val == nil {
			t.Fatalf("Missing key %s on replica/new-primary", key)
		}
	}
}

// TestStepDown_DataIntegrity verifies that StepDown drains transactions and syncs replicas
// ensuring zero data loss before disconnecting.
func TestStepDown_DataIntegrity(t *testing.T) {
	baseDir, clientTLS := setupSharedCertEnv(t)
	adminTLS := getRoleTLS(t, baseDir, "admin")

	// 1. Start Primary & Replica
	_, primAddr, cancelPrim := startServerNode(t, baseDir, "prim_integrity", clientTLS)
	defer cancelPrim()
	promoteNode(t, baseDir, primAddr, "1")

	_, replAddr, cancelRepl := startServerNode(t, baseDir, "repl_integrity", clientTLS)
	defer cancelRepl()

	// Connect Clients
	cPrim := connectClient(t, primAddr, clientTLS)
	defer cPrim.Close()
	selectDatabase(t, cPrim, "1")

	cRepl := connectClient(t, replAddr, clientTLS)
	defer cRepl.Close()
	selectDatabase(t, cRepl, "1")

	cAdmin := connectClient(t, replAddr, adminTLS)
	defer cAdmin.Close()
	selectDatabase(t, cAdmin, "1")

	// Configure Replication
	configureReplication(t, cAdmin, primAddr, "1")

	// 2. Write Data
	keyCount := 500
	for i := 0; i < keyCount; i++ {
		writeKeyVal(t, cPrim, fmt.Sprintf("k%d", i), "v")
	}

	// 3. Start a "Late" Transaction that overlaps with StepDown
	// We'll perform it in a goroutine
	doneCh := make(chan struct{})
	startedCh := make(chan struct{})

	go func() {
		defer close(doneCh)
		cLate := connectClient(t, primAddr, clientTLS)
		defer cLate.Close()
		selectDatabase(t, cLate, "1")

		// Begin
		cLate.AssertStatus(protocol.OpCodeBegin, nil, protocol.ResStatusOK)
		close(startedCh) // Signal transaction active

		time.Sleep(100 * time.Millisecond) // Simulating work

		// Write
		k := []byte("k_late")
		v := []byte("v_late")
		pl := make([]byte, 4+len(k)+len(v))
		binary.BigEndian.PutUint32(pl[0:4], uint32(len(k)))
		copy(pl[4:], k)
		copy(pl[4+len(k):], v)
		cLate.AssertStatus(protocol.OpCodeSet, pl, protocol.ResStatusOK)

		// Commit - Should succeed if StepDown drains!
		cLate.Send(protocol.OpCodeCommit, nil)
		// Read response
		st, _ := cLate.Read()
		if st != protocol.ResStatusOK {
			fmt.Printf("Late TX failed: %x\n", st)
		}
	}()

	// Wait for TX to start
	select {
	case <-startedCh:
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for late transaction to begin")
	}

	// 4. Trigger StepDown on Primary
	cPrimAdmin := connectClient(t, primAddr, adminTLS)
	defer cPrimAdmin.Close()
	selectDatabase(t, cPrimAdmin, "1")

	// Ensure StepDown returns OK
	cPrimAdmin.AssertStatus(protocol.OpCodeStepDown, nil, protocol.ResStatusOK)

	// Wait for late TX
	<-doneCh

	// 5. Verify Replica Has Everything (including late key)
	waitForConditionOrTimeout(t, 5*time.Second, func() bool {
		val := readKey(t, cRepl, "k_late")
		return string(val) == "v_late"
	}, "Replica missing late transaction data after StepDown drain")

	verifyKeys(t, keyCount, cRepl, "k")
}

// TestTimeline_Propagation verifies that timeline upgrades are propagated to replicas.
func TestTimeline_Propagation(t *testing.T) {
	baseDir, clientTLS := setupSharedCertEnv(t)
	adminTLS := getRoleTLS(t, baseDir, "admin")

	// 1. Start Primary (TL=0, promote to 1)
	_, primAddr, cancelPrim := startServerNode(t, baseDir, "prim_tl", clientTLS)
	defer cancelPrim()
	promoteNode(t, baseDir, primAddr, "1")

	// 2. Start Replica
	_, replAddr, cancelRepl := startServerNode(t, baseDir, "repl_tl", clientTLS)
	defer cancelRepl()

	cReplAdmin := connectClient(t, replAddr, adminTLS)
	selectDatabase(t, cReplAdmin, "1")
	configureReplication(t, cReplAdmin, primAddr, "1")
	cReplAdmin.Close()

	// 3. Write on TL 1
	cPrim := connectClient(t, primAddr, clientTLS)
	selectDatabase(t, cPrim, "1")
	writeKeyVal(t, cPrim, "tl1_key", "val")
	cPrim.Close()

	// Wait for sync
	cRepl := connectClient(t, replAddr, clientTLS)
	defer cRepl.Close()
	selectDatabase(t, cRepl, "1")
	waitForConditionOrTimeout(t, 2*time.Second, func() bool {
		return readKey(t, cRepl, "tl1_key") != nil
	}, "Sync TL1 failed")

	// 4. Promote Primary AGAIN (TL 1 -> 2)
	// Primary must StepDown first to become UNDEFINED.
	cPrimAdmin := connectClient(t, primAddr, adminTLS)
	selectDatabase(t, cPrimAdmin, "1")
	cPrimAdmin.AssertStatus(protocol.OpCodeStepDown, nil, protocol.ResStatusOK)
	cPrimAdmin.Close()

	// Reconnect as Admin to Promote
	cPrimAdmin2 := connectClient(t, primAddr, adminTLS)
	defer cPrimAdmin2.Close()
	selectDatabase(t, cPrimAdmin2, "1")
	cPrimAdmin2.AssertStatus(protocol.OpCodePromote, make([]byte, 4), protocol.ResStatusOK)

	// 5. Write on TL 2
	cPrim2 := connectClient(t, primAddr, clientTLS)
	defer cPrim2.Close()
	selectDatabase(t, cPrim2, "1")
	writeKeyVal(t, cPrim2, "tl2_key", "val")

	// 6. Verify Replica receives TL2 data
	waitForConditionOrTimeout(t, 2*time.Second, func() bool {
		return readKey(t, cRepl, "tl2_key") != nil
	}, "Sync TL2 failed after Primary promotion")
}

// TestFailover_NoDataLoss verifies complete hand-off of data leadership
func TestFailover_NoDataLoss(t *testing.T) {
	baseDir, clientTLS := setupSharedCertEnv(t)
	adminTLS := getRoleTLS(t, baseDir, "admin")

	// Node A (Initial Primary)
	_, addrA, cancelA := startServerNode(t, baseDir, "node_a", clientTLS)
	defer cancelA()
	promoteNode(t, baseDir, addrA, "1")

	// Node B (Replica)
	_, addrB, cancelB := startServerNode(t, baseDir, "node_b", clientTLS)
	defer cancelB()

	// Setup B -> A
	cAdminB := connectClient(t, addrB, adminTLS)
	selectDatabase(t, cAdminB, "1")
	configureReplication(t, cAdminB, addrA, "1")
	cAdminB.Close()

	// 1. Write Data to A
	clientA := connectClient(t, addrA, clientTLS)
	selectDatabase(t, clientA, "1")
	for i := 0; i < 100; i++ {
		writeKeyVal(t, clientA, fmt.Sprintf("mk%d", i), "val")
	}
	clientA.Close()

	// 2. Graceful StepDown of A
	cAdminA := connectClient(t, addrA, adminTLS)
	selectDatabase(t, cAdminA, "1")
	cAdminA.AssertStatus(protocol.OpCodeStepDown, nil, protocol.ResStatusOK)
	cAdminA.Close()

	// 3. Promote B to Primary
	promoteNode(t, baseDir, addrB, "1")

	// 4. Write NEW Data to B
	clientB := connectClient(t, addrB, clientTLS)
	selectDatabase(t, clientB, "1")

	// Verify old data present
	verifyKeys(t, 100, clientB, "mk")

	// Write new data
	for i := 0; i < 50; i++ {
		writeKeyVal(t, clientB, fmt.Sprintf("nk%d", i), "val_new")
	}
	clientB.Close()

	// 5. Reconfigure A to Follow B
	// A is Undefined now.
	cAdminA2 := connectClient(t, addrA, adminTLS)
	defer cAdminA2.Close()
	selectDatabase(t, cAdminA2, "1")
	configureReplication(t, cAdminA2, addrB, "1")

	// 6. Verify A catches up (150 keys total)
	clientA2 := connectClient(t, addrA, clientTLS)
	defer clientA2.Close()
	selectDatabase(t, clientA2, "1")

	waitForConditionOrTimeout(t, 10*time.Second, func() bool {
		// Sample check
		val := readKey(t, clientA2, "nk49")
		if val == nil {
			return false
		}
		return true
	}, "Node A failed to catch up after becoming replica")

	verifyKeys(t, 100, clientA2, "mk")
	verifyKeys(t, 50, clientA2, "nk")
}
