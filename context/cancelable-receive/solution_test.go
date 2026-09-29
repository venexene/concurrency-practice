package main

import (
	"context"
	"errors"
	"testing"
)

func TestRecvValueAndClosedBufferedChannel(t *testing.T) {
	in := make(chan int, 2)
	in <- 0
	in <- 7
	close(in)

	for _, want := range []int{0, 7} {
		got, err := Recv(context.Background(), in)
		if got != want || err != nil {
			t.Fatalf("Recv() = (%d, %v), want (%d, nil)", got, err, want)
		}
	}
	got, err := Recv(context.Background(), in)
	if got != 0 || !errors.Is(err, ErrClosed) {
		t.Fatalf("Recv() after buffer drained = (%d, %v), want (0, ErrClosed)", got, err)
	}
}

func TestRecvCanceledBeforeEntryHasPriority(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ready := make(chan int, 1)
	ready <- 9
	got, err := Recv(ctx, ready)
	if got != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("Recv() with ready value and canceled context = (%d, %v), want (0, context.Canceled)", got, err)
	}
	select {
	case value := <-ready:
		if value != 9 {
			t.Fatalf("value left in channel = %d, want 9", value)
		}
	default:
		t.Fatal("Recv() consumed the ready value despite prior cancellation")
	}

	closed := make(chan int)
	close(closed)
	got, err = Recv(ctx, closed)
	if got != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("Recv() with closed channel and canceled context = (%d, %v), want (0, context.Canceled)", got, err)
	}
}

func TestRecvWaitsForValue(t *testing.T) {
	in := make(chan int)
	result := make(chan struct {
		value int
		err   error
	}, 1)
	go func() {
		value, err := Recv(context.Background(), in)
		result <- struct {
			value int
			err   error
		}{value, err}
	}()

	in <- 42
	got := <-result
	if got.value != 42 || got.err != nil {
		t.Fatalf("Recv() = (%d, %v), want (42, nil)", got.value, got.err)
	}
}

func TestRecvCancellationWithUnavailableInput(t *testing.T) {
	for _, in := range []<-chan int{make(chan int), nil} {
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan struct {
			value int
			err   error
		}, 1)
		go func() {
			value, err := Recv(ctx, in)
			result <- struct {
				value int
				err   error
			}{value, err}
		}()

		cancel()
		got := <-result
		if got.value != 0 || !errors.Is(got.err, context.Canceled) {
			t.Errorf("Recv() after cancellation = (%d, %v), want (0, context.Canceled)", got.value, got.err)
		}
	}
}
