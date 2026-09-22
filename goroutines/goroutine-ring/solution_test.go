package main

import (
	"context"
	"os"
	"os/exec"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestRingOrder(t *testing.T) {
	tests := []struct {
		name   string
		k      int
		rounds int
	}{
		{name: "one participant", k: 1, rounds: 3},
		{name: "example", k: 3, rounds: 2},
		{name: "many participants", k: 100, rounds: 2},
		{name: "many rounds", k: 1, rounds: 1000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var mu sync.Mutex
			got := make([]int, 0, tt.k*tt.rounds)
			Ring(tt.k, tt.rounds, func(n int) {
				mu.Lock()
				got = append(got, n)
				mu.Unlock()
			})

			want := make([]int, 0, tt.k*tt.rounds)
			for range tt.rounds {
				for i := range tt.k {
					want = append(want, i)
				}
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("Ring(%d, %d) emitted %v, want %v", tt.k, tt.rounds, got, want)
			}
		})
	}
}

func TestRingConcurrentCalls(t *testing.T) {
	var wg sync.WaitGroup
	for _, tc := range []struct{ k, rounds int }{{2, 4}, {5, 3}} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var mu sync.Mutex
			got := make([]int, 0, tc.k*tc.rounds)
			Ring(tc.k, tc.rounds, func(n int) {
				mu.Lock()
				got = append(got, n)
				mu.Unlock()
			})
			mu.Lock()
			defer mu.Unlock()
			if len(got) != tc.k*tc.rounds {
				t.Errorf("Ring(%d, %d) emitted %d values, want %d", tc.k, tc.rounds, len(got), tc.k*tc.rounds)
			}
		}()
	}
	wg.Wait()
}

// The child process prevents a nonterminating Ring(1, 0) from hanging the suite.
func TestRingZeroRounds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRingZeroRoundsChild$")
	cmd.Env = append(os.Environ(), "RING_ZERO_ROUNDS_CHILD=1")
	out, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("Ring(1, 0) did not return: %v", ctx.Err())
	}
	if err != nil {
		t.Fatalf("Ring(1, 0) emitted a value or failed: %v; output: %s", err, out)
	}
}

func TestRingZeroRoundsChild(t *testing.T) {
	if os.Getenv("RING_ZERO_ROUNDS_CHILD") != "1" {
		return
	}
	Ring(1, 0, func(int) { os.Exit(3) })
}
