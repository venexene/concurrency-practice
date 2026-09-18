package main

import (
	"fmt"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
)

func TestPingPongAlternation(t *testing.T) {
	for _, n := range []int{0, 1, 3, 10, 10000} {
		for _, order := range []string{"ping-first", "pong-first"} {
			t.Run(fmt.Sprintf("n=%d/%s", n, order), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					p := NewPingPong(n)
					var log pingPongLog
					done := make(chan struct{}, 2)
					methods := []func(func(string)){p.Ping, p.Pong}
					if order == "pong-first" {
						methods[0], methods[1] = methods[1], methods[0]
					}
					for _, method := range methods {
						launchPingPongMethod(method, log.emit, done)
						synctest.Wait()
					}
					assertPingPongReturned(t, done)
					assertPingPongSequence(t, &log, n)
				})
			})
		}
	}
}

// Release both callers together so the race detector also checks an
// execution without synctest.Wait between method launches.
func TestPingPongConcurrentStart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := NewPingPong(1000)
		var log pingPongLog
		done := make(chan struct{}, 2)
		start := make(chan struct{})
		for _, method := range []func(func(string)){p.Ping, p.Pong} {
			go func() {
				<-start
				method(log.emit)
				done <- struct{}{}
			}()
		}
		close(start)
		synctest.Wait()
		assertPingPongReturned(t, done)
		assertPingPongSequence(t, &log, 1000)
	})
}

func TestPingPongWaitsForEmit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := NewPingPong(2)
		var log pingPongLog
		done := make(chan struct{}, 2)
		permit := make(chan struct{}, 1)
		// Unblock callbacks even if an assertion fails.
		defer close(permit)
		emit := func(value string) {
			log.emit(value)
			<-permit
		}

		launchPingPongMethod(p.Pong, emit, done)
		synctest.Wait()
		if got := log.snapshot(); len(got) != 0 {
			t.Fatalf("Pong emitted before Ping started: %q", got)
		}
		launchPingPongMethod(p.Ping, emit, done)

		want := []string{"ping", "pong", "ping", "pong"}
		for i := range want {
			synctest.Wait()
			if got := log.snapshot(); !slices.Equal(got, want[:i+1]) {
				t.Fatalf("while emit %d is blocked: got %q, want %q", i+1, got, want[:i+1])
			}
			permit <- struct{}{}
		}
		synctest.Wait()
		assertPingPongSequence(t, &log, 2)
		assertPingPongReturned(t, done)
	})
}

func TestPingPongInstancesAreIndependent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a, b := NewPingPong(1), NewPingPong(3)
		var logA, logB pingPongLog
		doneA, doneB := make(chan struct{}, 2), make(chan struct{}, 2)
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
			if value == "ping" {
				<-releaseA
			}
		}
		launchPingPongMethod(a.Pong, emitA, doneA)
		launchPingPongMethod(a.Ping, emitA, doneA)
		synctest.Wait()

		launchPingPongMethod(b.Pong, logB.emit, doneB)
		launchPingPongMethod(b.Ping, logB.emit, doneB)
		synctest.Wait()
		assertPingPongReturned(t, doneB)
		assertPingPongSequence(t, &logB, 3)
		if got := logA.snapshot(); !slices.Equal(got, []string{"ping"}) {
			t.Fatalf("blocked instance emitted %q, want [ping]", got)
		}
		select {
		case <-doneA:
			t.Fatal("a method returned while its first emit was blocked")
		default:
		}

		close(releaseA)
		synctest.Wait()
		assertPingPongReturned(t, doneA)
		assertPingPongSequence(t, &logA, 1)
	})
}

func launchPingPongMethod(method func(func(string)), emit func(string), done chan<- struct{}) {
	go func() {
		method(emit)
		done <- struct{}{}
	}()
}

// A faulty solution must not introduce a race in the test's own recorder.
type pingPongLog struct {
	mu     sync.Mutex
	values []string
}

func (l *pingPongLog) emit(value string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.values = append(l.values, value)
}

func (l *pingPongLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.values)
}

func assertPingPongSequence(t *testing.T, log *pingPongLog, n int) {
	t.Helper()
	got := log.snapshot()
	if len(got) != 2*n {
		t.Fatalf("emit count = %d, want %d for n=%d", len(got), 2*n, n)
	}
	for i, value := range got {
		want := "ping"
		if i%2 == 1 {
			want = "pong"
		}
		if value != want {
			t.Fatalf("emit[%d] = %q, want %q", i, value, want)
		}
	}
}

func assertPingPongReturned(t *testing.T, done <-chan struct{}) {
	t.Helper()
	for i := 0; i < 2; i++ {
		select {
		case <-done:
		default:
			t.Fatalf("only %d of 2 methods returned", i)
		}
	}
}
