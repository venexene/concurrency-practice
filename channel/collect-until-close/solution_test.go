package main

import (
	"slices"
	"sync"
	"testing"
)

func TestCollect(t *testing.T) {
	tests := []struct {
		name string
		vals []int
		want []int
	}{
		{name: "empty closed channel", vals: nil, want: []int{}},
		{name: "one value", vals: []int{42}, want: []int{42}},
		{name: "preserves order and duplicates", vals: []int{4, -1, 4, 0, 7}, want: []int{4, -1, 4, 0, 7}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := make(chan int, len(tt.vals))
			for _, n := range tt.vals {
				in <- n
			}
			close(in)

			got := Collect(in)
			if !slices.Equal(got, tt.want) {
				t.Fatalf("Collect() = %v, want %v", got, tt.want)
			}
			if got == nil {
				t.Fatal("Collect() returned a nil slice for an empty input")
			}
		})
	}
}

func TestCollectFromUnbufferedChannel(t *testing.T) {
	in := make(chan int)
	go func() {
		defer close(in)
		for _, n := range []int{3, 1, 4, 1, 5} {
			in <- n
		}
	}()

	got := Collect(in)
	want := []int{3, 1, 4, 1, 5}
	if !slices.Equal(got, want) {
		t.Fatalf("Collect() = %v, want %v", got, want)
	}
}

func TestCollectConcurrentCalls(t *testing.T) {
	inputs := [][]int{
		{0, 1, 2},
		{-3, -2, -1},
		{},
	}
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
			got[i] = Collect(in)
		}()
	}
	wg.Wait()

	for i, want := range inputs {
		if !slices.Equal(got[i], want) {
			t.Errorf("call %d: Collect() = %v, want %v", i, got[i], want)
		}
	}
}
