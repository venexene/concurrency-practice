package main

import (
	"context"
	"testing"
	"time"
)

func receiveWithin(t *testing.T, ch <-chan int) int {
	t.Helper()
	select {
	case value, ok := <-ch:
		if !ok {
			t.Fatal("generator closed before sending the expected value")
		}
		return value
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for generator value")
		return 0
	}
}

func expectClosedAfterCancel(t *testing.T, ch <-chan int, maxExtra int) {
	t.Helper()
	timer := time.NewTimer(time.Second)
	defer timer.Stop()

	extra := 0
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
			extra++
			if extra > maxExtra {
				t.Fatalf("received %d values after cancellation, want at most %d", extra, maxExtra)
			}
		case <-timer.C:
			t.Fatal("generator did not close its channel after cancellation")
		}
	}
}

func TestCountSequenceAndCloseAfterCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := Count(ctx)

	for want := range 5 {
		if got := receiveWithin(t, ch); got != want {
			t.Fatalf("value = %d, want %d", got, want)
		}
	}
	cancel()
	expectClosedAfterCancel(t, ch, 1)
}

func TestCountCancelWithoutConsumer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ch := Count(ctx)
	cancel()
	expectClosedAfterCancel(t, ch, 1)
}

func TestCountAlreadyCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// If both select cases are ready, their choice varies between runs.
	for range 100 {
		ch := Count(ctx)
		expectClosedAfterCancel(t, ch, 0)
	}
}
