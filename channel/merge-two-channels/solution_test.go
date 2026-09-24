package main

import (
	"slices"
	"sync"
	"testing"
)

func TestMerge2PreservesOrderWithinEachSource(t *testing.T) {
	a := make(chan int, 3)
	b := make(chan int, 3)
	for _, value := range []int{1, 3, 5} {
		a <- value
	}
	for _, value := range []int{2, 4, 6} {
		b <- value
	}
	close(a)
	close(b)

	assertMerge2Values(t, collectMerge2(Merge2(a, b)), []int{1, 3, 5}, []int{2, 4, 6})
}

func TestMerge2NilInputs(t *testing.T) {
	t.Run("both nil", func(t *testing.T) {
		out := Merge2(nil, nil)
		if cap(out) != 0 {
			t.Fatalf("output capacity = %d, want 0", cap(out))
		}
		if got := collectMerge2(out); len(got) != 0 {
			t.Fatalf("Merge2(nil, nil) = %v, want no values", got)
		}
	})

	for _, side := range []string{"left", "right"} {
		t.Run(side, func(t *testing.T) {
			in := make(chan int, 2)
			in <- 8
			in <- 9
			close(in)
			var out <-chan int
			if side == "left" {
				out = Merge2(in, nil)
			} else {
				out = Merge2(nil, in)
			}
			if got, want := collectMerge2(out), []int{8, 9}; !slices.Equal(got, want) {
				t.Fatalf("Merge2() = %v, want %v", got, want)
			}
		})
	}
}

func TestMerge2WaitsForBothSourcesToFinish(t *testing.T) {
	a := make(chan int, 1)
	a <- 1
	close(a)
	b := make(chan int, 1)
	out := Merge2(a, b)

	if got := <-out; got != 1 {
		t.Fatalf("first value = %d, want 1", got)
	}
	b <- 2
	close(b)
	if got, want := collectMerge2(out), []int{2}; !slices.Equal(got, want) {
		t.Fatalf("remaining values = %v, want %v", got, want)
	}
}

func TestMerge2ConcurrentUnbufferedSources(t *testing.T) {
	a := make(chan int)
	b := make(chan int)
	out := Merge2(a, b)
	var wg sync.WaitGroup
	for _, source := range []struct {
		ch     chan int
		values []int
	}{
		{a, []int{11, 13, 15}},
		{b, []int{12, 14, 16}},
	} {
		wg.Add(1)
		go func(ch chan int, values []int) {
			defer wg.Done()
			defer close(ch)
			for _, value := range values {
				ch <- value
			}
		}(source.ch, source.values)
	}

	got := collectMerge2(out)
	wg.Wait()
	assertMerge2Values(t, got, []int{11, 13, 15}, []int{12, 14, 16})
}

func collectMerge2(in <-chan int) []int {
	var values []int
	for value := range in {
		values = append(values, value)
	}
	return values
}

func assertMerge2Values(t *testing.T, got, wantOdd, wantEven []int) {
	t.Helper()
	var odd, even []int
	for _, value := range got {
		if value%2 == 0 {
			even = append(even, value)
		} else {
			odd = append(odd, value)
		}
	}
	if !slices.Equal(odd, wantOdd) || !slices.Equal(even, wantEven) {
		t.Fatalf("merged values = %v; source subsequences = %v, %v; want %v, %v", got, odd, even, wantOdd, wantEven)
	}
}
