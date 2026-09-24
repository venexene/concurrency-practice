package main

import (
	"slices"
	"testing"
)

func TestTakeLeavesUnreadValues(t *testing.T) {
	in := make(chan int, 3)
	for _, value := range []int{4, 5, 6} {
		in <- value
	}

	if got, want := Take(in, 2), []int{4, 5}; !slices.Equal(got, want) {
		t.Fatalf("Take(in, 2) = %v, want %v", got, want)
	}
	if got := <-in; got != 6 {
		t.Fatalf("unread value = %d, want 6", got)
	}
	in <- 7 // Take must not close the caller's channel.
	if got := <-in; got != 7 {
		t.Fatalf("value sent after Take = %d, want 7", got)
	}
}

func TestTakeZeroDoesNotRead(t *testing.T) {
	var nilIn <-chan int
	if got := Take(nilIn, 0); len(got) != 0 {
		t.Fatalf("Take(nil, 0) = %v, want empty result", got)
	}

	in := make(chan int, 1)
	in <- 42
	if got := Take(in, 0); len(got) != 0 {
		t.Fatalf("Take(in, 0) = %v, want empty result", got)
	}
	if got := <-in; got != 42 {
		t.Fatalf("value after Take(in, 0) = %d, want 42", got)
	}
}

func TestTakeStopsAtClosedChannel(t *testing.T) {
	in := make(chan int, 2)
	in <- 0
	in <- 7
	close(in)

	if got, want := Take(in, 3), []int{0, 7}; !slices.Equal(got, want) {
		t.Fatalf("Take(in, 3) = %v, want %v", got, want)
	}
}

func TestTakeClosedEmptyChannel(t *testing.T) {
	in := make(chan int)
	close(in)
	if got := Take(in, 3); len(got) != 0 {
		t.Fatalf("Take(closed, 3) = %v, want empty result", got)
	}
}

func TestTakeUnbufferedStream(t *testing.T) {
	in := make(chan int)
	go func() {
		defer close(in)
		for _, value := range []int{-2, 0, 5} {
			in <- value
		}
	}()

	if got, want := Take(in, 3), []int{-2, 0, 5}; !slices.Equal(got, want) {
		t.Fatalf("Take(in, 3) = %v, want %v", got, want)
	}
	if _, ok := <-in; ok {
		t.Fatal("input should be closed after producer finishes")
	}
}
