package main

import (
	"context"
	"slices"
	"testing"
	"time"
)

func TestTeeCopiesValuesToBothOutputs(t *testing.T) {
	in := make(chan int, 3)
	for _, value := range []int{2, 5, 8} {
		in <- value
	}
	close(in)

	left, right := Tee(context.Background(), in)
	if cap(left) != 0 || cap(right) != 0 {
		t.Fatalf("output capacities = (%d, %d), want (0, 0)", cap(left), cap(right))
	}
	leftDone := make(chan []int, 1)
	go func() { leftDone <- collectTee(left) }()
	rightValues := collectTee(right)
	leftValues := <-leftDone

	want := []int{2, 5, 8}
	if !slices.Equal(leftValues, want) || !slices.Equal(rightValues, want) {
		t.Fatalf("outputs = (%v, %v), want (%v, %v)", leftValues, rightValues, want, want)
	}
}

func TestTeeClosesBothOutputsForEmptyInput(t *testing.T) {
	in := make(chan int)
	close(in)
	left, right := Tee(context.Background(), in)
	if got := collectTee(left); len(got) != 0 {
		t.Fatalf("left = %v, want no values", got)
	}
	if got := collectTee(right); len(got) != 0 {
		t.Fatalf("right = %v, want no values", got)
	}
}

func TestTeeCancelWhileWaitingForSecondConsumer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	in := make(chan int)
	left, right := Tee(ctx, in)

	in <- 7 // The worker has taken the value from the input.
	if value, ok := <-left; !ok || value != 7 {
		t.Fatalf("left first read = (%d, %v), want (7, true)", value, ok)
	}
	cancel() // The worker now needs to stop even though right has no reader.

	select {
	case value, ok := <-left:
		if ok {
			t.Errorf("left produced %d after cancellation, want closed", value)
		}
	case <-time.After(time.Second):
		t.Error("left did not close after cancellation while right was unread")
		// Let an implementation blocked on right finish before this test exits.
		select {
		case <-right:
		case <-time.After(time.Second):
			t.Error("right did not become readable during cleanup")
		}
	}

	select {
	case _, ok := <-right:
		if ok {
			t.Error("right delivered a value after cancellation")
		}
	case <-time.After(time.Second):
		t.Error("right did not close after cancellation")
	}
}

func TestTeeCancelWithoutInput(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	in := make(chan int)
	left, right := Tee(ctx, in)
	cancel()
	if got := collectTee(left); len(got) != 0 {
		t.Fatalf("left = %v, want no values", got)
	}
	if got := collectTee(right); len(got) != 0 {
		t.Fatalf("right = %v, want no values", got)
	}
}

func collectTee(in <-chan int) []int {
	var values []int
	for value := range in {
		values = append(values, value)
	}
	return values
}
