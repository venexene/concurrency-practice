package main

import (
	"context"
	"slices"
	"sync/atomic"
	"testing"
	"time"
)

func TestPartitionRoutesAndPreservesOrder(t *testing.T) {
	in := make(chan int, 5)
	for _, value := range []int{0, 1, 2, -1, 2} {
		in <- value
	}
	close(in)

	var calls atomic.Int32
	yes, no := Partition(context.Background(), in, func(value int) bool {
		calls.Add(1)
		return value%2 == 0
	})
	if cap(yes) != 0 || cap(no) != 0 {
		t.Fatalf("output capacities = (%d, %d), want (0, 0)", cap(yes), cap(no))
	}
	yesDone := make(chan []int, 1)
	go func() { yesDone <- collectPartition(yes) }()
	noValues := collectPartition(no)
	yesValues := <-yesDone

	if want := []int{0, 2, 2}; !slices.Equal(yesValues, want) {
		t.Errorf("yes = %v, want %v", yesValues, want)
	}
	if want := []int{1, -1}; !slices.Equal(noValues, want) {
		t.Errorf("no = %v, want %v", noValues, want)
	}
	if got := calls.Load(); got != 5 {
		t.Errorf("pred calls = %d, want 5", got)
	}
}

func TestPartitionClosesBothOutputsForEmptyInput(t *testing.T) {
	in := make(chan int)
	close(in)
	yes, no := Partition(context.Background(), in, func(int) bool {
		t.Error("pred called for empty input")
		return true
	})
	if got := collectPartition(yes); len(got) != 0 {
		t.Fatalf("yes = %v, want no values", got)
	}
	if got := collectPartition(no); len(got) != 0 {
		t.Fatalf("no = %v, want no values", got)
	}
}

func TestPartitionCancelWhileSelectedOutputIsUnread(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	in := make(chan int)
	yes, no := Partition(ctx, in, func(int) bool { return true })
	in <- 7 // Worker received the value; no reader is waiting on yes.
	cancel()

	select {
	case value, ok := <-no:
		if ok {
			t.Errorf("no received %d, want closed", value)
		}
	case <-time.After(time.Second):
		t.Error("no did not close after cancellation while yes was unread")
	}
	select {
	case value, ok := <-yes:
		if ok {
			t.Errorf("yes received %d after cancellation, want closed", value)
		}
	case <-time.After(time.Second):
		t.Error("yes did not close after cancellation")
	}
}

func TestPartitionCancelWithNilInput(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var in <-chan int
	yes, no := Partition(ctx, in, func(int) bool {
		t.Error("pred called with nil input")
		return true
	})
	cancel()

	select {
	case _, ok := <-yes:
		if ok {
			t.Error("yes should be closed without values")
		}
	case <-time.After(time.Second):
		t.Error("yes did not close after cancellation")
	}
	select {
	case _, ok := <-no:
		if ok {
			t.Error("no should be closed without values")
		}
	case <-time.After(time.Second):
		t.Error("no did not close after cancellation")
	}
}

func collectPartition(in <-chan int) []int {
	var values []int
	for value := range in {
		values = append(values, value)
	}
	return values
}
