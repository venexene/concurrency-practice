package main

import (
	"sync"
	"testing"
	"time"
)

func TestTransferSuccessAndInsufficientFunds(t *testing.T) {
	from := NewAccount(10)
	to := NewAccount(5)
	if !Transfer(from, to, 3) {
		t.Fatal("Transfer of 3 from balance 10 returned false")
	}
	if gotFrom, gotTo := from.Balance(), to.Balance(); gotFrom != 7 || gotTo != 8 {
		t.Fatalf("balances after transfer = (%d, %d), want (7, 8)", gotFrom, gotTo)
	}
	if Transfer(from, to, 8) {
		t.Fatal("Transfer of 8 from balance 7 returned true")
	}
	if gotFrom, gotTo := from.Balance(), to.Balance(); gotFrom != 7 || gotTo != 8 {
		t.Fatalf("balances after rejected transfer = (%d, %d), want (7, 8)", gotFrom, gotTo)
	}
}

func TestTransferToSameAccountCompletes(t *testing.T) {
	a := NewAccount(10)
	done := make(chan bool, 1)
	go func() { done <- Transfer(a, a, 7) }()

	select {
	case ok := <-done:
		if !ok {
			t.Fatal("Transfer to self returned false, want true")
		}
		if got := a.Balance(); got != 10 {
			t.Fatalf("balance after Transfer to self = %d, want 10", got)
		}
	case <-time.After(time.Second):
		t.Fatal("Transfer to self blocked; both accounts use the same mutex")
	}
}

func TestTransferToSameAccountInsufficientFunds(t *testing.T) {
	a := NewAccount(3)
	done := make(chan bool, 1)
	go func() { done <- Transfer(a, a, 7) }()

	select {
	case ok := <-done:
		if ok {
			t.Fatal("insufficient Transfer to self returned true, want false")
		}
		if got := a.Balance(); got != 3 {
			t.Fatalf("balance after insufficient Transfer to self = %d, want 3", got)
		}
	case <-time.After(time.Second):
		t.Fatal("insufficient Transfer to self blocked")
	}
}

func TestTransferConcurrentSameDirectionPreservesTotal(t *testing.T) {
	from := NewAccount(1000)
	to := NewAccount(0)
	const workers = 100
	start := make(chan struct{})
	results := make(chan bool, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results <- Transfer(from, to, 1)
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	for ok := range results {
		if !ok {
			t.Fatal("Transfer of 1 failed despite sufficient initial funds")
		}
	}
	if gotFrom, gotTo := from.Balance(), to.Balance(); gotFrom != 900 || gotTo != 100 {
		t.Fatalf("balances after concurrent transfers = (%d, %d), want (900, 100)", gotFrom, gotTo)
	}
}

func TestTransferOppositeDirectionsComplete(t *testing.T) {
	a := NewAccount(100)
	b := NewAccount(100)
	start := make(chan struct{})
	done := make(chan bool, 2)
	go func() {
		<-start
		done <- Transfer(a, b, 3)
	}()
	go func() {
		<-start
		done <- Transfer(b, a, 4)
	}()
	close(start)

	for range 2 {
		select {
		case ok := <-done:
			if !ok {
				t.Fatal("opposite-direction transfer returned false despite sufficient funds")
			}
		case <-time.After(time.Second):
			t.Fatal("opposite-direction transfers did not both complete")
		}
	}
	if gotA, gotB := a.Balance(), b.Balance(); gotA != 101 || gotB != 99 {
		t.Fatalf("balances after opposite transfers = (%d, %d), want (101, 99)", gotA, gotB)
	}
}

func TestNewAccountConcurrentIDsAreUnique(t *testing.T) {
	const count = 1000
	start := make(chan struct{})
	ids := make(chan uint64, count)
	var wg sync.WaitGroup
	for range count {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			ids <- NewAccount(0).id
		}()
	}
	close(start)
	wg.Wait()
	close(ids)

	seen := make(map[uint64]struct{}, count)
	for id := range ids {
		if _, exists := seen[id]; exists {
			t.Fatalf("duplicate account ID %d", id)
		}
		seen[id] = struct{}{}
	}
	if len(seen) != count {
		t.Fatalf("got %d unique IDs, want %d", len(seen), count)
	}
}
