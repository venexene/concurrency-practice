package main

import (
	"strings"
	"sync"
	"testing"
	"time"
)

func TestWaterMoleculeGroups(t *testing.T) {
	tests := []struct {
		name  string
		atoms string
	}{
		{name: "hydrogens first", atoms: "HHO"},
		{name: "oxygen first", atoms: "OHH"},
		{name: "interleaved", atoms: "HOHHOH"},
		{name: "oxygens first", atoms: "OOHHHH"},
		{name: "many molecules", atoms: strings.Repeat("H", 100) + strings.Repeat("O", 50)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			journal := runWater(tt.atoms)
			assertWaterJournal(t, journal, len(tt.atoms)/3)
		})
	}
}

func TestWaterIndependentInstances(t *testing.T) {
	var wg sync.WaitGroup
	journals := make([][]byte, 2)
	for i, atoms := range []string{"HHOOHH", "OHH"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			journals[i] = runWater(atoms)
		}()
	}
	wg.Wait()
	assertWaterJournal(t, journals[0], 2)
	assertWaterJournal(t, journals[1], 1)
}

func TestWaterWaitsForAllCallbacksBeforeNextMolecule(t *testing.T) {
	w := NewWater()
	var mu sync.Mutex
	journal := make([]byte, 0, 6)
	started := make(chan int, 6)
	entered := make(chan struct{}, 3)
	completed := make(chan struct{}, 6)
	gate := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(gate) }) }
	defer release()

	launch := func(atom byte, secondGroup bool) {
		go func() {
			if secondGroup {
				entered <- struct{}{}
			}
			emit := func() {
				mu.Lock()
				journal = append(journal, atom)
				index := len(journal)
				started <- index
				mu.Unlock()
				if index == 3 {
					<-gate
				}
			}
			if atom == 'H' {
				w.Hydrogen(emit)
			} else {
				w.Oxygen(emit)
			}
			completed <- struct{}{}
		}()
	}

	for _, atom := range "HHO" {
		launch(byte(atom), false)
	}
	deadline := time.After(5 * time.Second)
	for range 3 {
		select {
		case <-started:
		case <-deadline:
			t.Fatal("first molecule did not start all three callbacks")
		}
	}

	for _, atom := range "HHO" {
		launch(byte(atom), true)
	}
	deadline = time.After(5 * time.Second)
	for range 3 {
		select {
		case <-entered:
		case <-deadline:
			t.Fatal("second group did not start its method calls")
		}
	}

	premature := false
	select {
	case <-started:
		premature = true
	case <-time.After(100 * time.Millisecond):
	}
	release()

	deadline = time.After(5 * time.Second)
	for range 6 {
		select {
		case <-completed:
		case <-deadline:
			t.Fatal("not all atom methods returned after releasing the callback")
		}
	}
	if premature {
		t.Error("a callback from the next molecule started before the third callback returned")
	}
	assertWaterJournal(t, journal, 2)
}

func runWater(atoms string) []byte {
	w := NewWater()
	var mu sync.Mutex
	journal := make([]byte, 0, len(atoms))
	var wg sync.WaitGroup
	for _, atom := range atoms {
		wg.Add(1)
		go func() {
			defer wg.Done()
			emit := func() {
				mu.Lock()
				journal = append(journal, byte(atom))
				mu.Unlock()
			}
			if atom == 'H' {
				w.Hydrogen(emit)
			} else {
				w.Oxygen(emit)
			}
		}()
	}
	wg.Wait()
	return journal
}

func assertWaterJournal(t *testing.T, journal []byte, molecules int) {
	t.Helper()
	if len(journal) != 3*molecules {
		t.Fatalf("journal length = %d, want %d", len(journal), 3*molecules)
	}
	for i := 0; i < molecules; i++ {
		group := journal[3*i : 3*i+3]
		if strings.Count(string(group), "H") != 2 || strings.Count(string(group), "O") != 1 {
			t.Errorf("molecule %d = %q, want two H and one O", i, group)
		}
	}
}
