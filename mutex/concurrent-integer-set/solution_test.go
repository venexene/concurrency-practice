package main

import (
	"sync"
	"testing"
)

func TestIntSetBasicOperations(t *testing.T) {
	s := NewIntSet()
	if s.Contains(4) || s.Remove(4) {
		t.Fatal("new set should not contain 4")
	}
	if !s.Add(4) || !s.Contains(4) {
		t.Fatal("first Add(4) should insert 4")
	}
	if s.Add(4) {
		t.Fatal("duplicate Add(4) should return false")
	}
	if !s.Add(-3) || !s.Contains(-3) {
		t.Fatal("Add(-3) should insert a distinct key")
	}
	if !s.Remove(4) || s.Contains(4) {
		t.Fatal("Remove(4) should remove 4")
	}
	if s.Remove(4) || !s.Contains(-3) {
		t.Fatal("second Remove(4) should return false and leave -3 untouched")
	}
	if !s.Add(4) || !s.Contains(4) {
		t.Fatal("Add(4) after removal should insert 4 again")
	}
}

func TestIntSetConcurrentAddSameKey(t *testing.T) {
	s := NewIntSet()
	const workers = 100
	start := make(chan struct{})
	results := make(chan bool, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results <- s.Add(42)
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	inserted := 0
	for result := range results {
		if result {
			inserted++
		}
	}
	if inserted != 1 || !s.Contains(42) {
		t.Fatalf("concurrent Add(42): %d successful insertions, Contains(42) = %t; want 1 and true", inserted, s.Contains(42))
	}
}

func TestIntSetConcurrentRemoveSameKey(t *testing.T) {
	s := NewIntSet()
	s.Add(42)
	const workers = 100
	start := make(chan struct{})
	results := make(chan bool, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results <- s.Remove(42)
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	removed := 0
	for result := range results {
		if result {
			removed++
		}
	}
	if removed != 1 || s.Contains(42) {
		t.Fatalf("concurrent Remove(42): %d successful removals, Contains(42) = %t; want 1 and false", removed, s.Contains(42))
	}
}

func TestIntSetConcurrentDistinctKeysAndReads(t *testing.T) {
	s := NewIntSet()
	const workers = 8
	const keysPerWorker = 100
	start := make(chan struct{})
	var wg sync.WaitGroup
	for worker := range workers {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			<-start
			for offset := range keysPerWorker {
				key := worker*keysPerWorker + offset
				if !s.Add(key) {
					t.Errorf("first Add(%d) returned false", key)
					return
				}
				if !s.Contains(key) {
					t.Errorf("Contains(%d) after Add returned false", key)
					return
				}
			}
		}(worker)
	}
	close(start)
	wg.Wait()

	for key := range workers * keysPerWorker {
		if !s.Contains(key) {
			t.Errorf("missing key %d after all writers completed", key)
		}
	}
}
