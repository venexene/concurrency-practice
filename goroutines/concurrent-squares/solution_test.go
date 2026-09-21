package main

import (
	"slices"
	"sync"
	"testing"
)

func TestSquares(t *testing.T) {
	tests := []struct {
		name string
		in   []int64
		want []int64
	}{
		{name: "nil input", in: nil, want: []int64{}},
		{name: "empty input", in: []int64{}, want: []int64{}},
		{name: "single value", in: []int64{-7}, want: []int64{49}},
		{name: "mixed signs and zero", in: []int64{3, -2, 0, 4, -5}, want: []int64{9, 4, 0, 16, 25}},
		{name: "distinct positions", in: []int64{9, 1, 8, 2, 7, 3}, want: []int64{81, 1, 64, 4, 49, 9}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := slices.Clone(tt.in)
			got := Squares(tt.in)
			if !slices.Equal(got, tt.want) || len(got) != len(tt.want) {
				t.Fatalf("Squares(%v) = %v, want %v", tt.in, got, tt.want)
			}
			if !slices.Equal(tt.in, original) || len(tt.in) != len(original) {
				t.Fatalf("input changed: got %v, want %v", tt.in, original)
			}
		})
	}
}

func TestSquaresMaximumLength(t *testing.T) {
	const size = 10_000
	in := make([]int64, size)
	for i := range in {
		in[i] = int64(i%201 - 100)
	}

	got := Squares(in)
	if len(got) != size {
		t.Fatalf("result length = %d, want %d", len(got), size)
	}
	for i, x := range in {
		if got[i] != x*x {
			t.Fatalf("result[%d] = %d, want %d", i, got[i], x*x)
		}
	}
}

func TestSquaresConcurrentCalls(t *testing.T) {
	inputs := [][]int64{
		{1, 2, 3},
		{-4, 0, 5},
		{6, -7, 8, -9},
	}
	results := make([][]int64, len(inputs))
	var wg sync.WaitGroup
	for i := range inputs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i] = Squares(inputs[i])
		}()
	}
	wg.Wait()

	for i, in := range inputs {
		if len(results[i]) != len(in) {
			t.Fatalf("call %d: result length = %d, want %d", i, len(results[i]), len(in))
		}
		for j, x := range in {
			if results[i][j] != x*x {
				t.Fatalf("call %d: result[%d] = %d, want %d", i, j, results[i][j], x*x)
			}
		}
	}
}
