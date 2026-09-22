package main

import (
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
)

var fizzBuzzLaunchOrder = []string{"fizz", "buzz", "fizzbuzz", "number"}

func TestFizzBuzzOutput(t *testing.T) {
	for _, n := range []int{0, 1, 2, 3, 5, 15, 16, 30, 100, 10_000} {
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			got := collectFizzBuzz(n, fizzBuzzLaunchOrder)
			assertFizzBuzzOutput(t, got, expectedFizzBuzz(n))
		})
	}
}

func TestFizzBuzzLaunchOrder(t *testing.T) {
	orders := [][]string{
		{"number", "fizzbuzz", "buzz", "fizz"},
		{"buzz", "number", "fizz", "fizzbuzz"},
	}
	for _, order := range orders {
		got := collectFizzBuzz(30, order)
		assertFizzBuzzOutput(t, got, expectedFizzBuzz(30))
	}
}

func TestFizzBuzzConcurrentInstances(t *testing.T) {
	var wg sync.WaitGroup
	results := make([][]string, 2)
	for i, n := range []int{45, 61} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i] = collectFizzBuzz(n, fizzBuzzLaunchOrder)
		}()
	}
	wg.Wait()
	assertFizzBuzzOutput(t, results[0], expectedFizzBuzz(45))
	assertFizzBuzzOutput(t, results[1], expectedFizzBuzz(61))
}

func TestFizzBuzzCallbacksDoNotOverlap(t *testing.T) {
	fb := NewFizzBuzz(30)
	var active atomic.Int32
	var overlapped atomic.Bool
	var mu sync.Mutex
	got := make([]string, 0, 30)
	record := func(value string) {
		if active.Add(1) != 1 {
			overlapped.Store(true)
		}
		runtime.Gosched()
		mu.Lock()
		got = append(got, value)
		mu.Unlock()
		active.Add(-1)
	}

	var wg sync.WaitGroup
	for _, run := range []func(){
		func() { fb.Fizz(func() { record("fizz") }) },
		func() { fb.Buzz(func() { record("buzz") }) },
		func() { fb.FizzBuzz(func() { record("fizzbuzz") }) },
		func() { fb.Number(func(n int) { record(strconv.Itoa(n)) }) },
	} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			run()
		}()
	}
	wg.Wait()
	if overlapped.Load() {
		t.Error("callbacks ran concurrently")
	}
	assertFizzBuzzOutput(t, got, expectedFizzBuzz(30))
}

func collectFizzBuzz(n int, order []string) []string {
	fb := NewFizzBuzz(n)
	var mu sync.Mutex
	got := make([]string, 0, n)
	record := func(value string) {
		mu.Lock()
		got = append(got, value)
		mu.Unlock()
	}
	runners := map[string]func(){
		"fizz":     func() { fb.Fizz(func() { record("fizz") }) },
		"buzz":     func() { fb.Buzz(func() { record("buzz") }) },
		"fizzbuzz": func() { fb.FizzBuzz(func() { record("fizzbuzz") }) },
		"number":   func() { fb.Number(func(value int) { record(strconv.Itoa(value)) }) },
	}

	var wg sync.WaitGroup
	for _, name := range order {
		wg.Add(1)
		go func(run func()) {
			defer wg.Done()
			run()
		}(runners[name])
	}
	wg.Wait()
	return got
}

func expectedFizzBuzz(n int) []string {
	want := make([]string, 0, n)
	for value := 1; value <= n; value++ {
		switch {
		case value%15 == 0:
			want = append(want, "fizzbuzz")
		case value%3 == 0:
			want = append(want, "fizz")
		case value%5 == 0:
			want = append(want, "buzz")
		default:
			want = append(want, strconv.Itoa(value))
		}
	}
	return want
}

func assertFizzBuzzOutput(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("output length = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("output[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}
