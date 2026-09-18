package main

import (
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
)

// Each launch order is enforced without assuming how fast goroutines run.
func TestOrderedLaunchOrders(t *testing.T) {
	orders := [][3]string{
		{"first", "second", "third"},
		{"first", "third", "second"},
		{"second", "first", "third"},
		{"second", "third", "first"},
		{"third", "first", "second"},
		{"third", "second", "first"},
	}

	for _, order := range orders {
		t.Run(strings.Join(order[:], "-"), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				o := NewOrdered()
				var log emissionLog
				done := make(chan string, 3)

				for _, name := range order {
					launchOrderedMethod(o, name, log.emit, done)
					synctest.Wait()
				}

				assertEmitted(t, &log, "first", "second", "third")
				assertReturned(t, done, "first", "second", "third")
			})
		})
	}
}

// A following callback must wait for the previous callback to finish, and
// each method must wait for its own callback before returning.
func TestOrderedWaitsForCallbacks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		o := NewOrdered()
		var log emissionLog
		done := make(chan string, 3)
		gates := map[string]chan struct{}{
			"first":  make(chan struct{}),
			"second": make(chan struct{}),
			"third":  make(chan struct{}),
		}
		defer func() {
			for _, gate := range gates {
				select {
				case <-gate:
				default:
					close(gate)
				}
			}
		}()

		emit := func(value string) {
			log.emit(value)
			gate, ok := gates[value]
			if !ok {
				t.Errorf("unexpected emit value: %q", value)
				return
			}
			<-gate
		}

		for _, name := range []string{"third", "second", "first"} {
			launchOrderedMethod(o, name, emit, done)
		}
		synctest.Wait()
		assertEmitted(t, &log, "first")
		assertReturned(t, done)

		close(gates["first"])
		synctest.Wait()
		assertEmitted(t, &log, "first", "second")
		assertReturned(t, done, "first")

		close(gates["second"])
		synctest.Wait()
		assertEmitted(t, &log, "first", "second", "third")
		assertReturned(t, done, "second")

		close(gates["third"])
		synctest.Wait()
		assertReturned(t, done, "third")
	})
}

func TestOrderedInstancesAreIndependent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a, b := NewOrdered(), NewOrdered()
		var logA, logB emissionLog
		doneA, doneB := make(chan string, 3), make(chan string, 3)
		releaseA := make(chan struct{})
		defer func() {
			select {
			case <-releaseA:
			default:
				close(releaseA)
			}
		}()

		emitA := func(value string) {
			logA.emit(value)
			if value == "first" {
				<-releaseA
			}
		}
		for _, name := range []string{"third", "second", "first"} {
			launchOrderedMethod(a, name, emitA, doneA)
		}
		synctest.Wait()
		assertEmitted(t, &logA, "first")
		assertReturned(t, doneA)

		for _, name := range []string{"third", "second", "first"} {
			launchOrderedMethod(b, name, logB.emit, doneB)
		}
		synctest.Wait()
		assertEmitted(t, &logB, "first", "second", "third")
		assertReturned(t, doneB, "first", "second", "third")
		assertEmitted(t, &logA, "first")
		assertReturned(t, doneA)

		close(releaseA)
		synctest.Wait()
		assertEmitted(t, &logA, "first", "second", "third")
		assertReturned(t, doneA, "first", "second", "third")
	})
}

type orderedMethods interface {
	First(func(string))
	Second(func(string))
	Third(func(string))
}

func launchOrderedMethod(o orderedMethods, name string, emit func(string), done chan<- string) {
	go func() {
		switch name {
		case "first":
			o.First(emit)
		case "second":
			o.Second(emit)
		case "third":
			o.Third(emit)
		default:
			panic("unknown method: " + name)
		}
		done <- name
	}()
}

// The recorder remains race-free even if a faulty solution overlaps callbacks.
type emissionLog struct {
	mu     sync.Mutex
	values []string
}

func (l *emissionLog) emit(value string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.values = append(l.values, value)
}

func assertEmitted(t *testing.T, log *emissionLog, want ...string) {
	t.Helper()
	log.mu.Lock()
	got := slices.Clone(log.values)
	log.mu.Unlock()
	if !slices.Equal(got, want) {
		t.Fatalf("emit journal = %q, want %q", got, want)
	}
}

// Drain only completed methods: a missing return is a failure, not a hang.
func assertReturned(t *testing.T, done <-chan string, want ...string) {
	t.Helper()
	var got []string
	for {
		select {
		case name := <-done:
			got = append(got, name)
		default:
			slices.Sort(got)
			expected := slices.Clone(want)
			slices.Sort(expected)
			if !slices.Equal(got, expected) {
				t.Fatalf("returned methods = %q, want %q", got, expected)
			}
			return
		}
	}
}
