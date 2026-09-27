package main

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestWriterFirstRWLockReadersCanOverlap(t *testing.T) {
	l := NewWriterFirstRWLock()
	l.RLock()
	acquired := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	go func() {
		l.RLock()
		close(acquired)
		<-release
		l.RUnlock()
		close(done)
	}()

	mustReceive(t, acquired, "second reader while first reader holds lock")
	close(release)
	l.RUnlock()
	mustReceive(t, done, "second reader unlock")
}

func TestWriterFirstRWLockWriterIsExclusive(t *testing.T) {
	l := NewWriterFirstRWLock()
	l.Lock()
	readerEntered := make(chan struct{})
	writerEntered := make(chan struct{})
	go func() {
		l.RLock()
		close(readerEntered)
		l.RUnlock()
	}()
	go func() {
		l.Lock()
		close(writerEntered)
		l.Unlock()
	}()

	select {
	case <-readerEntered:
		t.Fatal("reader entered while writer held lock")
	case <-writerEntered:
		t.Fatal("second writer entered while first writer held lock")
	default:
	}
	l.Unlock()
	mustReceive(t, readerEntered, "reader after writer unlock")
	mustReceive(t, writerEntered, "second writer after writer unlock")
}

func TestWriterFirstRWLockWaitingWriterPrecedesNewReader(t *testing.T) {
	l := NewWriterFirstRWLock()
	l.RLock()

	events := make(chan string, 2)
	releaseWriter := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		l.Lock()
		events <- "writer"
		<-releaseWriter
		l.Unlock()
		close(writerDone)
	}()
	waitForRegisteredWriters(t, l, 1)

	readerDone := make(chan struct{})
	go func() {
		l.RLock()
		events <- "reader"
		l.RUnlock()
		close(readerDone)
	}()

	l.RUnlock()
	select {
	case first := <-events:
		if first != "writer" {
			t.Fatalf("first entrant = %s, want writer", first)
		}
	case <-time.After(time.Second):
		t.Fatal("waiting writer did not enter after readers left")
	}
	select {
	case next := <-events:
		t.Fatalf("%s entered while writer held lock", next)
	default:
	}

	close(releaseWriter)
	mustReceive(t, writerDone, "writer unlock")
	select {
	case next := <-events:
		if next != "reader" {
			t.Fatalf("second entrant = %s, want reader", next)
		}
	case <-time.After(time.Second):
		t.Fatal("reader did not enter after writer queue emptied")
	}
	mustReceive(t, readerDone, "reader unlock")
}

func TestWriterFirstRWLockWritersFollowRegistrationOrder(t *testing.T) {
	l := NewWriterFirstRWLock()
	l.RLock()
	entered := make(chan int, 2)
	releaseFirst := make(chan struct{})
	firstDone := make(chan struct{})
	secondDone := make(chan struct{})

	go func() {
		l.Lock()
		entered <- 1
		<-releaseFirst
		l.Unlock()
		close(firstDone)
	}()
	waitForRegisteredWriters(t, l, 1)
	go func() {
		l.Lock()
		entered <- 2
		l.Unlock()
		close(secondDone)
	}()
	waitForRegisteredWriters(t, l, 2)

	l.RUnlock()
	select {
	case first := <-entered:
		if first != 1 {
			t.Fatalf("first writer = %d, want 1", first)
		}
	case <-time.After(time.Second):
		t.Fatal("first registered writer did not enter")
	}
	select {
	case second := <-entered:
		t.Fatalf("writer %d entered while first writer held lock", second)
	default:
	}

	close(releaseFirst)
	mustReceive(t, firstDone, "first writer unlock")
	select {
	case second := <-entered:
		if second != 2 {
			t.Fatalf("second writer = %d, want 2", second)
		}
	case <-time.After(time.Second):
		t.Fatal("second registered writer did not enter")
	}
	mustReceive(t, secondDone, "second writer unlock")
}

func TestWriterFirstRWLockMixedAccess(t *testing.T) {
	l := NewWriterFirstRWLock()
	const readers = 8
	const writers = 8
	const iterations = 50
	var activeReaders atomic.Int32
	var activeWriters atomic.Int32
	var violations atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})

	for range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range iterations {
				l.RLock()
				activeReaders.Add(1)
				if activeWriters.Load() != 0 {
					violations.Add(1)
				}
				activeReaders.Add(-1)
				l.RUnlock()
			}
		}()
	}
	for range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range iterations {
				l.Lock()
				if activeWriters.Add(1) != 1 || activeReaders.Load() != 0 {
					violations.Add(1)
				}
				activeWriters.Add(-1)
				l.Unlock()
			}
		}()
	}
	close(start)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	mustReceive(t, done, "mixed readers and writers")
	if got := violations.Load(); got != 0 {
		t.Fatalf("observed %d overlapping read/write or write/write critical sections", got)
	}
}

func waitForRegisteredWriters(t *testing.T, l *WriterFirstRWLock, want int) {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	timeout := time.NewTimer(time.Second)
	defer timeout.Stop()

	for {
		l.cond.L.Lock()
		got := l.waitingWriters
		l.cond.L.Unlock()
		if got >= want {
			return
		}
		select {
		case <-ticker.C:
		case <-timeout.C:
			t.Fatalf("registered writers = %d, want at least %d", got, want)
		}
	}
}

func mustReceive(t *testing.T, done <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("%s did not complete", name)
	}
}
