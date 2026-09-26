package main

import (
	"sync"
	"testing"
	"time"
)

func TestProgressImmediateAndAlreadyReachedTargets(t *testing.T) {
	p := NewProgress()
	waitCompletes(t, func() { p.WaitAtLeast(0) })
	p.Add(3)
	waitCompletes(t, func() { p.WaitAtLeast(2) })
	waitCompletes(t, func() { p.WaitAtLeast(3) })
}

func TestProgressDifferentThresholds(t *testing.T) {
	p := NewProgress()
	reachedTwo := make(chan struct{})
	reachedFive := make(chan struct{})
	go func() {
		p.WaitAtLeast(2)
		close(reachedTwo)
	}()
	go func() {
		p.WaitAtLeast(5)
		close(reachedFive)
	}()

	p.Add(2)
	mustComplete(t, reachedTwo, "waiter for 2")
	select {
	case <-reachedFive:
		t.Fatal("waiter for 5 completed when progress was only 2")
	default:
	}
	p.Add(0)
	select {
	case <-reachedFive:
		t.Fatal("Add(0) released waiter for 5 before threshold was reached")
	default:
	}
	p.Add(3)
	mustComplete(t, reachedFive, "waiter for 5")
}

func TestProgressBroadcastReleasesAllEligibleWaiters(t *testing.T) {
	p := NewProgress()
	const waiters = 100
	done := make(chan struct{}, waiters)
	for range waiters {
		go func() {
			p.WaitAtLeast(4)
			done <- struct{}{}
		}()
	}
	p.Add(4)
	for range waiters {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("not all waiters completed after reaching their threshold")
		}
	}
}

func TestProgressConcurrentAddsReachThreshold(t *testing.T) {
	p := NewProgress()
	const workers = 20
	const increments = 50
	waiterDone := make(chan struct{})
	go func() {
		p.WaitAtLeast(workers * increments)
		close(waiterDone)
	}()

	start := make(chan struct{})
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range increments {
				p.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	mustComplete(t, waiterDone, "waiter for total of concurrent Add calls")
}

func waitCompletes(t *testing.T, wait func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		wait()
		close(done)
	}()
	mustComplete(t, done, "WaitAtLeast")
}

func mustComplete(t *testing.T, done <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("%s did not complete", name)
	}
}
