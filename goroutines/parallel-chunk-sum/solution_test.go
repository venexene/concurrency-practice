package main

import (
	"slices"
	"sync"
	"testing"
)

func TestChunkSum(t *testing.T) {
	tests := []struct {
		name string
		nums []int64
		k    int
		want int64
	}{
		{name: "example", nums: []int64{1, 2, 3, 4, 5}, k: 3, want: 15},
		{name: "empty input", nums: []int64{}, k: 3, want: 0},
		{name: "nil input", nums: nil, k: 1, want: 0},
		{name: "one block", nums: []int64{4, -2, 7}, k: 1, want: 9},
		{name: "one item per block", nums: []int64{8, 1, -3}, k: 3, want: 6},
		{name: "more blocks than items", nums: []int64{5, -2}, k: 5, want: 3},
		{name: "two larger blocks", nums: []int64{1, 2, 4, 8, 16}, k: 3, want: 31},
		{name: "two larger then two smaller", nums: []int64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512}, k: 4, want: 1023},
		{name: "negative values and zero", nums: []int64{-9, 0, 2, -3, 10, -4}, k: 4, want: -4},
		{name: "maximum number of blocks", nums: []int64{7, -3, 1}, k: 1000, want: 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := slices.Clone(tt.nums)
			got := ChunkSum(tt.nums, tt.k)
			if got != tt.want {
				t.Fatalf("ChunkSum(%v, %d) = %d, want %d", tt.nums, tt.k, got, tt.want)
			}
			if !slices.Equal(tt.nums, original) || len(tt.nums) != len(original) {
				t.Fatalf("input changed: got %v, want %v", tt.nums, original)
			}
		})
	}
}

func TestChunkSumConcurrentCalls(t *testing.T) {
	type call struct {
		nums []int64
		k    int
		want int64
	}
	calls := []call{
		{nums: []int64{1, 2, 4, 8, 16}, k: 3, want: 31},
		{nums: []int64{-5, 3, 0, 8}, k: 2, want: 6},
		{nums: []int64{2, 3, 5}, k: 5, want: 10},
	}
	results := make([]int64, len(calls))
	var wg sync.WaitGroup
	for i := range calls {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i] = ChunkSum(calls[i].nums, calls[i].k)
		}()
	}
	wg.Wait()

	for i, call := range calls {
		if results[i] != call.want {
			t.Errorf("call %d: ChunkSum(%v, %d) = %d, want %d", i, call.nums, call.k, results[i], call.want)
		}
	}
}
