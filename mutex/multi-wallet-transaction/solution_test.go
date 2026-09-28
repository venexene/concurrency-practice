package main

import (
	"fmt"
	"maps"
	"slices"
	"sync"
	"testing"
)

func TestLedgerApplyAndSnapshot(t *testing.T) {
	initial := map[WalletID]int64{"A": 5, "B": 3, "C": 0}
	ledger := NewLedger(initial)
	initial["A"] = 100
	delete(initial, "B")

	if got, want := ledger.Snapshot([]WalletID{"A", "B", "C"}), (map[WalletID]int64{"A": 5, "B": 3, "C": 0}); !maps.Equal(got, want) {
		t.Fatalf("initial balances = %v, want %v", got, want)
	}

	if !ledger.Apply(map[WalletID]int64{"A": -4, "B": -2, "C": 6}) {
		t.Fatal("valid transaction was rejected")
	}
	want := map[WalletID]int64{"A": 1, "B": 1, "C": 6}
	if got := ledger.Snapshot([]WalletID{"A", "B", "C"}); !maps.Equal(got, want) {
		t.Fatalf("balances after valid transaction = %v, want %v", got, want)
	}

	if ledger.Apply(map[WalletID]int64{"A": -2, "C": 2}) {
		t.Fatal("transaction with insufficient funds was accepted")
	}
	if got := ledger.Snapshot([]WalletID{"A", "B", "C"}); !maps.Equal(got, want) {
		t.Fatalf("failed transaction changed balances: got %v, want %v", got, want)
	}
	if !ledger.Apply(nil) {
		t.Fatal("empty transaction was rejected")
	}
	if got := ledger.Snapshot(nil); len(got) != 0 {
		t.Fatalf("empty snapshot = %v, want empty map", got)
	}
}

func TestLedgerSnapshotDoesNotChangeInputsOrLedger(t *testing.T) {
	ledger := NewLedger(map[WalletID]int64{"A": 4, "B": 7})
	ids := []WalletID{"B", "A", "B"}
	originalIDs := slices.Clone(ids)

	got := ledger.Snapshot(ids)
	if want := (map[WalletID]int64{"A": 4, "B": 7}); !maps.Equal(got, want) {
		t.Fatalf("snapshot with duplicate ID = %v, want %v", got, want)
	}
	if !slices.Equal(ids, originalIDs) {
		t.Fatalf("Snapshot changed ids: got %v, want %v", ids, originalIDs)
	}

	got["A"] = 100
	if next := ledger.Snapshot([]WalletID{"A"}); next["A"] != 4 {
		t.Fatalf("changing snapshot changed ledger: A = %d, want 4", next["A"])
	}
}

func TestLedgerConcurrentTransfersAndSnapshots(t *testing.T) {
	const (
		initialBalance = 10_000
		iterations     = 400
		workersPerSide = 4
	)
	ledger := NewLedger(map[WalletID]int64{"A": initialBalance, "B": initialBalance})
	forward := map[WalletID]int64{"A": -1, "B": 1}
	backward := map[WalletID]int64{"A": 1, "B": -1}
	ids := []WalletID{"B", "A", "A"}
	start := make(chan struct{})
	problems := make(chan error, 2*workersPerSide+2)
	var wg sync.WaitGroup

	for _, deltas := range []map[WalletID]int64{forward, backward} {
		for range workersPerSide {
			wg.Add(1)
			go func(deltas map[WalletID]int64) {
				defer wg.Done()
				<-start
				for range iterations {
					if !ledger.Apply(deltas) {
						problems <- fmt.Errorf("valid transaction was rejected: %v", deltas)
						return
					}
				}
			}(deltas)
		}
	}
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range iterations {
				snapshot := ledger.Snapshot(ids)
				if len(snapshot) != 2 || snapshot["A"]+snapshot["B"] != 2*initialBalance {
					problems <- fmt.Errorf("inconsistent snapshot: %v", snapshot)
					return
				}
			}
		}()
	}

	close(start)
	wg.Wait()
	close(problems)
	for err := range problems {
		t.Error(err)
	}
	if got, want := ledger.Snapshot(ids), (map[WalletID]int64{"A": initialBalance, "B": initialBalance}); !maps.Equal(got, want) {
		t.Errorf("final balances = %v, want %v", got, want)
	}
	if want := []WalletID{"B", "A", "A"}; !slices.Equal(ids, want) {
		t.Errorf("concurrent snapshots changed ids: got %v, want %v", ids, want)
	}
}

func TestLedgerConcurrentDisjointTransfers(t *testing.T) {
	ledger := NewLedger(map[WalletID]int64{"A": 1000, "B": 1000, "C": 1000, "D": 1000})
	transactions := []map[WalletID]int64{
		{"A": -1, "B": 1},
		{"A": 1, "B": -1},
		{"C": -1, "D": 1},
		{"C": 1, "D": -1},
	}
	var wg sync.WaitGroup
	for _, deltas := range transactions {
		wg.Add(1)
		go func(deltas map[WalletID]int64) {
			defer wg.Done()
			for range 200 {
				if !ledger.Apply(deltas) {
					t.Errorf("valid transaction was rejected: %v", deltas)
					return
				}
			}
		}(deltas)
	}
	wg.Wait()

	want := map[WalletID]int64{"A": 1000, "B": 1000, "C": 1000, "D": 1000}
	if got := ledger.Snapshot([]WalletID{"D", "B", "C", "A"}); !maps.Equal(got, want) {
		t.Errorf("final balances = %v, want %v", got, want)
	}
}
