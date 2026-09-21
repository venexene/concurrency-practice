package main

import (
	"testing"
	"testing/synctest"
)

func TestLatchBroadcastAndFutureWaiters(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := NewLatch()
		const waiters = 32
		done := make(chan struct{}, waiters+1)

		for range waiters {
			go func() {
				l.Wait()
				done <- struct{}{}
			}()
		}
		synctest.Wait()
		if got := len(done); got != 0 {
			t.Fatalf("before Release: %d Wait calls returned, want 0", got)
		}

		l.Release()
		synctest.Wait()
		if got := len(done); got != waiters {
			t.Fatalf("after Release: %d Wait calls returned, want %d", got, waiters)
		}

		go func() {
			l.Wait()
			done <- struct{}{}
		}()
		synctest.Wait()
		if got := len(done); got != waiters+1 {
			t.Fatalf("future Wait: %d total calls returned, want %d", got, waiters+1)
		}
	})
}

func TestLatchConcurrentRepeatedRelease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := NewLatch()
		const calls = 64
		released := make(chan struct{}, calls)
		waited := make(chan struct{}, calls)

		for range calls {
			go func() {
				l.Wait()
				waited <- struct{}{}
			}()
		}
		synctest.Wait()
		if got := len(waited); got != 0 {
			t.Fatalf("before Release: %d Wait calls returned, want 0", got)
		}

		for range calls {
			go func() {
				l.Release()
				released <- struct{}{}
			}()
		}
		synctest.Wait()
		if got := len(released); got != calls {
			t.Errorf("completed Release calls = %d, want %d", got, calls)
		}
		if got := len(waited); got != calls {
			t.Errorf("completed Wait calls = %d, want %d", got, calls)
		}
	})
}

func TestLatchInstancesAreIndependent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		first, second := NewLatch(), NewLatch()
		done := make(chan string, 2)
		go func() { first.Wait(); done <- "first" }()
		go func() { second.Wait(); done <- "second" }()
		synctest.Wait()

		first.Release()
		synctest.Wait()
		if got := len(done); got != 1 {
			t.Fatalf("after first Release: %d Wait calls returned, want 1", got)
		}
		if got := <-done; got != "first" {
			t.Fatalf("first completed waiter = %q, want first", got)
		}

		second.Release()
		synctest.Wait()
		if got := len(done); got != 1 {
			t.Fatalf("after second Release: %d Wait calls returned, want 1", got)
		}
		if got := <-done; got != "second" {
			t.Fatalf("second completed waiter = %q, want second", got)
		}
	})
}
