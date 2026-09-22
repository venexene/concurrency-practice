package main

import (
	"sync"
	"testing"
	"testing/synctest"
)

func TestBarrierSingleParticipant(t *testing.T) {
	b := NewBarrier(1)
	for round := 0; round < 20; round++ {
		if got := b.ArriveAndWait(); got != round {
			t.Fatalf("round %d returned generation %d", round, got)
		}
	}
}

func TestBarrierWaitsForEveryParticipantAndReuses(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const participants, rounds = 3, 2
		b := NewBarrier(participants)
		type result struct{ participant, round, generation int }
		results := make(chan result, participants*rounds)
		var gates [participants][rounds]chan struct{}
		for participant := range participants {
			for round := range rounds {
				gates[participant][round] = make(chan struct{})
			}
			go func() {
				for round := range rounds {
					<-gates[participant][round]
					generation := b.ArriveAndWait()
					results <- result{participant, round, generation}
				}
			}()
		}

		for round := range rounds {
			close(gates[0][round])
			close(gates[1][round])
			synctest.Wait()
			if got := len(results); got != 0 {
				t.Fatalf("round %d returned %d participants before the third arrived", round, got)
			}

			close(gates[2][round])
			synctest.Wait()
			if got := len(results); got != participants {
				t.Fatalf("round %d returned %d participants, want %d", round, got, participants)
			}
			seen := [participants]bool{}
			for range participants {
				got := <-results
				if got.round != round || got.generation != round {
					t.Errorf("participant %d in round %d returned generation %d, want %d", got.participant, got.round, got.generation, round)
				}
				if seen[got.participant] {
					t.Errorf("participant %d returned twice in round %d", got.participant, round)
				}
				seen[got.participant] = true
			}
		}
	})
}

func TestBarrierManyRounds(t *testing.T) {
	const participants, rounds = 12, 100
	b := NewBarrier(participants)
	results := make([][]int, participants)
	var wg sync.WaitGroup
	for participant := range participants {
		results[participant] = make([]int, rounds)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for round := range rounds {
				results[participant][round] = b.ArriveAndWait()
			}
		}()
	}
	wg.Wait()
	for participant := range participants {
		for round := range rounds {
			if got := results[participant][round]; got != round {
				t.Fatalf("participant %d, round %d: generation = %d", participant, round, got)
			}
		}
	}
}

func TestBarrierMaximumParticipants(t *testing.T) {
	const participants = 1000
	b := NewBarrier(participants)
	results := make([]int, participants)
	var wg sync.WaitGroup
	for participant := range participants {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[participant] = b.ArriveAndWait()
		}()
	}
	wg.Wait()
	for participant, got := range results {
		if got != 0 {
			t.Fatalf("participant %d returned generation %d, want 0", participant, got)
		}
	}
}
