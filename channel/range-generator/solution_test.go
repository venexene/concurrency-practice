package main

import (
	"slices"
	"sync"
	"testing"
)

func TestRange(t *testing.T) {
	tests := []struct {
		name   string
		lo, hi int
		want   []int
	}{
		{name: "positive range", lo: 3, hi: 7, want: []int{3, 4, 5, 6}},
		{name: "negative to positive", lo: -2, hi: 2, want: []int{-2, -1, 0, 1}},
		{name: "single value", lo: 5, hi: 6, want: []int{5}},
		{name: "equal bounds", lo: 5, hi: 5, want: []int{}},
		{name: "reversed bounds", lo: 7, hi: 3, want: []int{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ch := Range(tt.lo, tt.hi)
			got := make([]int, 0)
			for n := range ch {
				got = append(got, n)
			}

			if !slices.Equal(got, tt.want) {
				t.Fatalf("Range(%d, %d) produced %v, want %v", tt.lo, tt.hi, got, tt.want)
			}

			if _, ok := <-ch; ok {
				t.Fatal("channel is still open after the generator finished")
			}
		})
	}
}

func TestRangeConcurrentCalls(t *testing.T) {
	type call struct {
		lo, hi int
		want   []int
	}
	calls := []call{
		{lo: 0, hi: 4, want: []int{0, 1, 2, 3}},
		{lo: -3, hi: 0, want: []int{-3, -2, -1}},
		{lo: 10, hi: 10, want: []int{}},
	}

	got := make([][]int, len(calls))
	var wg sync.WaitGroup
	for i := range calls {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := range Range(calls[i].lo, calls[i].hi) {
				got[i] = append(got[i], n)
			}
		}()
	}
	wg.Wait()

	for i, call := range calls {
		if !slices.Equal(got[i], call.want) {
			t.Errorf("call %d: got %v, want %v", i, got[i], call.want)
		}
	}
}
