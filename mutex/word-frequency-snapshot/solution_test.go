package main

import (
	"reflect"
	"sync"
	"testing"
)

func TestWordCounterCountsAndSnapshotIndependence(t *testing.T) {
	w := NewWordCounter()
	if got := w.Snapshot(); len(got) != 0 {
		t.Fatalf("initial Snapshot() = %v, want empty map", got)
	}

	w.Add("go")
	w.Add("go")
	w.Add("rust")
	first := w.Snapshot()
	wantFirst := map[string]int{"go": 2, "rust": 1}
	if !reflect.DeepEqual(first, wantFirst) {
		t.Fatalf("Snapshot() = %v, want %v", first, wantFirst)
	}

	w.Add("go")
	if !reflect.DeepEqual(first, wantFirst) {
		t.Fatalf("earlier snapshot changed after Add: got %v, want %v", first, wantFirst)
	}

	first["go"] = 99
	delete(first, "rust")
	first["other"] = 1
	wantCurrent := map[string]int{"go": 3, "rust": 1}
	if got := w.Snapshot(); !reflect.DeepEqual(got, wantCurrent) {
		t.Fatalf("Snapshot() after changing returned map = %v, want %v", got, wantCurrent)
	}
}

func TestWordCounterConcurrentAdds(t *testing.T) {
	w := NewWordCounter()
	const workers = 8
	const iterations = 500
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range iterations {
				w.Add("shared")
				w.Add("other")
			}
		}()
	}
	close(start)
	wg.Wait()

	want := map[string]int{
		"shared": workers * iterations,
		"other":  workers * iterations,
	}
	if got := w.Snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Snapshot() after concurrent Add = %v, want %v", got, want)
	}
}

func TestWordCounterSnapshotsDuringUpdates(t *testing.T) {
	w := NewWordCounter()
	const iterations = 1000
	const readers = 4
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for range iterations {
			w.Add("first")
			w.Add("second")
		}
	}()
	for range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range iterations {
				snapshot := w.Snapshot()
				first := snapshot["first"]
				second := snapshot["second"]
				if first < second || first-second > 1 {
					t.Errorf("inconsistent snapshot: first=%d, second=%d", first, second)
					return
				}
			}
		}()
	}
	close(start)
	wg.Wait()

	want := map[string]int{"first": iterations, "second": iterations}
	if got := w.Snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("final Snapshot() = %v, want %v", got, want)
	}
}
