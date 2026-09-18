package main

import (
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
)

func TestZeroEvenOddLaunchOrders(t *testing.T) {
	orders := [][3]string{
		{"zero", "even", "odd"},
		{"zero", "odd", "even"},
		{"even", "zero", "odd"},
		{"even", "odd", "zero"},
		{"odd", "zero", "even"},
		{"odd", "even", "zero"},
	}
	for _, n := range []int{0, 1, 2, 3, 4, 5, 9999, 10000} {
		for _, order := range orders {
			t.Run(fmt.Sprintf("n=%d/%s", n, strings.Join(order[:], "-")), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					z := NewZeroEvenOdd(n)
					var log zeroEvenOddLog
					done := make(chan string, 3)
					for _, role := range order {
						launchZeroEvenOddMethod(z, role, log.emitter(role), done)
						synctest.Wait()
					}
					assertZeroEvenOddReturned(t, done)
					assertZeroEvenOddEmitted(t, &log, expectedZeroEvenOdd(n))
				})
			})
		}
	}
}

func TestZeroEvenOddConcurrentStart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		z := NewZeroEvenOdd(1000)
		var log zeroEvenOddLog
		done := make(chan string, 3)
		start := make(chan struct{})
		for _, role := range []string{"zero", "even", "odd"} {
			go func() {
				<-start
				callZeroEvenOddMethod(z, role, log.emitter(role))
				done <- role
			}()
		}
		close(start)
		synctest.Wait()
		assertZeroEvenOddReturned(t, done)
		assertZeroEvenOddEmitted(t, &log, expectedZeroEvenOdd(1000))
	})
}

// Record the caller as well as the value: a correct journal emitted by the
// wrong method still violates the task's contract.
func TestZeroEvenOddWaitsForEmit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		z := NewZeroEvenOdd(3)
		var log zeroEvenOddLog
		done := make(chan string, 3)
		permit := make(chan struct{}, 1)
		defer close(permit)
		for _, role := range []string{"even", "odd", "zero"} {
			record := log.emitter(role)
			emit := func(value int) {
				record(value)
				<-permit
			}
			launchZeroEvenOddMethod(z, role, emit, done)
		}

		want := expectedZeroEvenOdd(3)
		returned := make(map[string]bool)
		for i, event := range want {
			synctest.Wait()
			assertZeroEvenOddEmitted(t, &log, want[:i+1])
			collectZeroEvenOddReturns(t, done, returned)
			if returned[event.role] {
				t.Fatalf("%s returned before its emit(%d) completed", event.role, event.value)
			}
			permit <- struct{}{}
		}
		synctest.Wait()
		assertZeroEvenOddEmitted(t, &log, want)
		collectZeroEvenOddReturns(t, done, returned)
		if len(returned) != 3 {
			t.Fatalf("returned methods = %v, want zero, even, odd", returned)
		}
	})
}

func TestZeroEvenOddInstancesAreIndependent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a, b := NewZeroEvenOdd(1), NewZeroEvenOdd(4)
		var logA, logB zeroEvenOddLog
		doneA, doneB := make(chan string, 3), make(chan string, 3)
		releaseA := make(chan struct{})
		defer func() {
			select {
			case <-releaseA:
			default:
				close(releaseA)
			}
		}()
		for _, role := range []string{"even", "odd", "zero"} {
			record := logA.emitter(role)
			emit := func(value int) {
				record(value)
				if role == "zero" {
					<-releaseA
				}
			}
			launchZeroEvenOddMethod(a, role, emit, doneA)
		}
		synctest.Wait()

		for _, role := range []string{"odd", "even", "zero"} {
			launchZeroEvenOddMethod(b, role, logB.emitter(role), doneB)
		}
		synctest.Wait()
		assertZeroEvenOddReturned(t, doneB)
		assertZeroEvenOddEmitted(t, &logB, expectedZeroEvenOdd(4))
		assertZeroEvenOddEmitted(t, &logA, expectedZeroEvenOdd(1)[:1])
		select {
		case role := <-doneA:
			if role == "zero" || role == "odd" {
				t.Fatalf("%s returned before the required callbacks completed", role)
			}
			// Even may return immediately when n=1. Keep its completion for
			// the final assertion rather than requiring it to wait needlessly.
			doneA <- role
		default:
		}

		close(releaseA)
		synctest.Wait()
		assertZeroEvenOddReturned(t, doneA)
		assertZeroEvenOddEmitted(t, &logA, expectedZeroEvenOdd(1))
	})
}

func launchZeroEvenOddMethod(z *ZeroEvenOdd, role string, emit func(int), done chan<- string) {
	go func() {
		callZeroEvenOddMethod(z, role, emit)
		done <- role
	}()
}

func callZeroEvenOddMethod(z *ZeroEvenOdd, role string, emit func(int)) {
	switch role {
	case "zero":
		z.Zero(emit)
	case "even":
		z.Even(emit)
	case "odd":
		z.Odd(emit)
	default:
		panic("unknown role: " + role)
	}
}

type zeroEvenOddEmission struct {
	role  string
	value int
}

type zeroEvenOddLog struct {
	mu     sync.Mutex
	events []zeroEvenOddEmission
}

func (l *zeroEvenOddLog) emitter(role string) func(int) {
	return func(value int) {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.events = append(l.events, zeroEvenOddEmission{role: role, value: value})
	}
}

func expectedZeroEvenOdd(n int) []zeroEvenOddEmission {
	want := make([]zeroEvenOddEmission, 0, 2*n)
	for value := 1; value <= n; value++ {
		role := "odd"
		if value%2 == 0 {
			role = "even"
		}
		want = append(want, zeroEvenOddEmission{"zero", 0}, zeroEvenOddEmission{role, value})
	}
	return want
}

func assertZeroEvenOddEmitted(t *testing.T, log *zeroEvenOddLog, want []zeroEvenOddEmission) {
	t.Helper()
	log.mu.Lock()
	got := slices.Clone(log.events)
	log.mu.Unlock()
	if len(got) != len(want) {
		t.Fatalf("emit count = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("emit[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func collectZeroEvenOddReturns(t *testing.T, done <-chan string, returned map[string]bool) {
	t.Helper()
	for {
		select {
		case role := <-done:
			if returned[role] {
				t.Fatalf("%s returned more than once", role)
			}
			returned[role] = true
		default:
			return
		}
	}
}

func assertZeroEvenOddReturned(t *testing.T, done <-chan string) {
	t.Helper()
	returned := make(map[string]bool)
	collectZeroEvenOddReturns(t, done, returned)
	if !returned["zero"] || !returned["even"] || !returned["odd"] {
		t.Fatalf("returned methods = %v, want zero, even, odd", returned)
	}
}
