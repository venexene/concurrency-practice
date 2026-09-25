package main

import (
	"sync"
	"testing"
)

func TestCounterZeroValueAndDeltas(t *testing.T) {
	var c Counter
	if got := c.Value(); got != 0 {
		t.Fatalf("initial Value() = %d, want 0", got)
	}

	for _, step := range []struct {
		delta int64
		want  int64
	}{
		{3, 3},
		{-1, 2},
		{0, 2},
		{5, 7},
	} {
		c.Add(step.delta)
		if got := c.Value(); got != step.want {
			t.Fatalf("after Add(%d), Value() = %d, want %d", step.delta, got, step.want)
		}
	}
}

func TestCounterConcurrentUpdates(t *testing.T) {
	var c Counter
	const workers = 12
	const iterations = 500
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range iterations {
				c.Add(3)
				c.Add(-2)
			}
		}()
	}
	close(start)
	wg.Wait()

	if got, want := c.Value(), int64(workers*iterations); got != want {
		t.Fatalf("Value() after concurrent updates = %d, want %d", got, want)
	}
}

func TestCounterReadsDuringUpdates(t *testing.T) {
	var c Counter
	const writers = 8
	const readers = 4
	const iterations = 500
	const want = int64(writers * iterations)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range iterations {
				c.Add(1)
			}
		}()
	}
	for range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			var previous int64
			for range iterations {
				got := c.Value()
				if got < previous || got > want {
					t.Errorf("Value() = %d after %d, want a nondecreasing value in [0, %d]", got, previous, want)
					return
				}
				previous = got
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := c.Value(); got != want {
		t.Fatalf("final Value() = %d, want %d", got, want)
	}
}
