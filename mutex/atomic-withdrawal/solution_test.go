package main

import (
	"sync"
	"testing"
)

func TestAccountSequentialOperations(t *testing.T) {
	a := NewAccount(10)
	if got := a.Balance(); got != 10 {
		t.Fatalf("initial Balance() = %d, want 10", got)
	}
	if a.Withdraw(11) {
		t.Fatal("Withdraw(11) from balance 10 succeeded")
	}
	if got := a.Balance(); got != 10 {
		t.Fatalf("Balance() after rejected withdrawal = %d, want 10", got)
	}
	if !a.Withdraw(10) || a.Balance() != 0 {
		t.Fatal("withdrawing the entire balance should leave zero")
	}
	a.Deposit(7)
	if !a.Withdraw(3) || a.Balance() != 4 {
		t.Fatal("Deposit(7) then Withdraw(3) should leave 4")
	}
}

func TestAccountConcurrentWithdrawalsSpendFundsOnce(t *testing.T) {
	a := NewAccount(10)
	const callers = 100
	start := make(chan struct{})
	results := make(chan bool, callers)
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results <- a.Withdraw(7)
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	successes := 0
	for ok := range results {
		if ok {
			successes++
		}
	}
	if got := a.Balance(); successes != 1 || got != 3 {
		t.Fatalf("after concurrent Withdraw(7): successes = %d, balance = %d; want 1 and 3", successes, got)
	}
}

func TestAccountConcurrentDeposits(t *testing.T) {
	a := NewAccount(0)
	const workers = 8
	const iterations = 500
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range iterations {
				a.Deposit(2)
			}
		}()
	}
	close(start)
	wg.Wait()

	if got, want := a.Balance(), int64(workers*iterations*2); got != want {
		t.Fatalf("Balance() after concurrent deposits = %d, want %d", got, want)
	}
}

func TestAccountConcurrentMixedOperations(t *testing.T) {
	a := NewAccount(0)
	const deposits = 500
	const withdrawals = 500
	start := make(chan struct{})
	var wg sync.WaitGroup
	successes := make(chan int, 1)
	wg.Add(3)
	go func() {
		defer wg.Done()
		<-start
		for range deposits {
			a.Deposit(1)
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		count := 0
		for range withdrawals {
			if a.Withdraw(1) {
				count++
			}
		}
		successes <- count
	}()
	go func() {
		defer wg.Done()
		<-start
		for range deposits + withdrawals {
			if got := a.Balance(); got < 0 || got > deposits {
				t.Errorf("Balance() during operations = %d, want value in [0, %d]", got, deposits)
				return
			}
		}
	}()
	close(start)
	wg.Wait()
	count := <-successes

	if got, want := a.Balance(), int64(deposits-count); got != want {
		t.Fatalf("final Balance() = %d, want %d after %d successful withdrawals", got, want, count)
	}
}
