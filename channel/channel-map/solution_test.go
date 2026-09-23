package main

import (
	"slices"
	"sync"
	"testing"
)

func TestMapTransformsValuesInOrder(t *testing.T) {
	in := make(chan int, 3)
	for _, n := range []int{1, 2, 3} {
		in <- n
	}
	close(in)

	got := collectMap(Map(in, func(n int) int { return n * 10 }))
	want := []int{10, 20, 30}
	if !slices.Equal(got, want) {
		t.Fatalf("Map() = %v, want %v", got, want)
	}
}

func TestMapCallsFunctionOncePerValue(t *testing.T) {
	in := make(chan int, 4)
	for _, n := range []int{-2, 0, 7, -2} {
		in <- n
	}
	close(in)

	var calls []int
	got := collectMap(Map(in, func(n int) int {
		calls = append(calls, n)
		return n + 1
	}))
	if want := []int{-1, 1, 8, -1}; !slices.Equal(got, want) {
		t.Fatalf("Map() = %v, want %v", got, want)
	}
	if want := []int{-2, 0, 7, -2}; !slices.Equal(calls, want) {
		t.Fatalf("f was called with %v, want %v", calls, want)
	}
}

func TestMapEmptyInputClosesOutput(t *testing.T) {
	in := make(chan int)
	close(in)

	out := Map(in, func(n int) int { return n * n })
	if got := collectMap(out); len(got) != 0 {
		t.Fatalf("Map() = %v, want no values", got)
	}
	if _, ok := <-out; ok {
		t.Fatal("output channel is still open")
	}
}

func TestMapConcurrentCalls(t *testing.T) {
	inputs := [][]int{{0, 1, 2}, {-3, -2}, {}}
	got := make([][]int, len(inputs))

	var wg sync.WaitGroup
	for i := range inputs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			in := make(chan int, len(inputs[i]))
			for _, n := range inputs[i] {
				in <- n
			}
			close(in)
			got[i] = collectMap(Map(in, func(n int) int { return n * n }))
		}()
	}
	wg.Wait()

	want := [][]int{{0, 1, 4}, {9, 4}, {}}
	for i := range want {
		if !slices.Equal(got[i], want[i]) {
			t.Errorf("call %d: Map() = %v, want %v", i, got[i], want[i])
		}
	}
}

func collectMap(in <-chan int) []int {
	result := make([]int, 0)
	for n := range in {
		result = append(result, n)
	}
	return result
}
