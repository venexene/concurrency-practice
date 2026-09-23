package main

import (
	"sync"
	"testing"
)

func TestTryRecvEmptyAndNilChannels(t *testing.T) {
	for name, ch := range map[string]<-chan int{
		"empty buffered":   make(chan int, 1),
		"empty unbuffered": make(chan int),
		"nil":              nil,
	} {
		t.Run(name, func(t *testing.T) {
			if _, state := TryRecv(ch); state != "empty" {
				t.Fatalf("TryRecv() state = %q, want empty", state)
			}
		})
	}
}

func TestTryRecvOpenBufferedChannel(t *testing.T) {
	ch := make(chan int, 1)
	ch <- 12
	if value, state := TryRecv(ch); value != 12 || state != "value" {
		t.Fatalf("TryRecv() = (%d, %q), want (12, value)", value, state)
	}
	if _, state := TryRecv(ch); state != "empty" {
		t.Fatalf("after draining open channel state = %q, want empty", state)
	}
}

func TestTryRecvZeroThenClosed(t *testing.T) {
	ch := make(chan int, 1)
	ch <- 0
	close(ch)

	if value, state := TryRecv(ch); value != 0 || state != "value" {
		t.Fatalf("first TryRecv() = (%d, %q), want (0, value)", value, state)
	}
	if value, state := TryRecv(ch); value != 0 || state != "closed" {
		t.Fatalf("second TryRecv() = (%d, %q), want (0, closed)", value, state)
	}
}

func TestTryRecvDrainsClosedBufferInOrder(t *testing.T) {
	ch := make(chan int, 3)
	for _, value := range []int{7, -2, 9} {
		ch <- value
	}
	close(ch)

	for _, want := range []int{7, -2, 9} {
		value, state := TryRecv(ch)
		if value != want || state != "value" {
			t.Fatalf("TryRecv() = (%d, %q), want (%d, value)", value, state, want)
		}
	}
	if value, state := TryRecv(ch); value != 0 || state != "closed" {
		t.Fatalf("after draining buffer TryRecv() = (%d, %q), want (0, closed)", value, state)
	}
}

func TestTryRecvClosedEmptyChannel(t *testing.T) {
	ch := make(chan int)
	close(ch)
	if value, state := TryRecv(ch); value != 0 || state != "closed" {
		t.Fatalf("TryRecv() = (%d, %q), want (0, closed)", value, state)
	}
}

func TestTryRecvConcurrentReceivers(t *testing.T) {
	const receivers = 32
	const values = 5
	ch := make(chan int, values)
	for value := 0; value < values; value++ {
		ch <- value
	}
	close(ch)

	type result struct {
		value int
		state string
	}
	results := make(chan result, receivers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < receivers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			value, state := TryRecv(ch)
			results <- result{value, state}
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	seen := make(map[int]bool)
	closed := 0
	for got := range results {
		switch got.state {
		case "value":
			if got.value < 0 || got.value >= values || seen[got.value] {
				t.Errorf("unexpected or duplicate value %d", got.value)
			}
			seen[got.value] = true
		case "closed":
			if got.value != 0 {
				t.Errorf("closed result has value %d, want 0", got.value)
			}
			closed++
		default:
			t.Errorf("unexpected state %q", got.state)
		}
	}
	if len(seen) != values || closed != receivers-values {
		t.Fatalf("received %d values and %d closed results, want %d and %d", len(seen), closed, values, receivers-values)
	}
}
