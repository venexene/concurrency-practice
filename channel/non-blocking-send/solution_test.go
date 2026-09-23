package main

import (
	"sync"
	"testing"
)

func TestTrySendBufferedChannel(t *testing.T) {
	ch := make(chan int, 1)
	if !TrySend(ch, 8) {
		t.Fatal("first send to empty buffer returned false")
	}
	if TrySend(ch, 9) {
		t.Fatal("send to full buffer returned true")
	}
	if got := <-ch; got != 8 {
		t.Fatalf("received %d, want 8", got)
	}
	if !TrySend(ch, 0) {
		t.Fatal("send after draining buffer returned false")
	}
	if got := <-ch; got != 0 {
		t.Fatalf("received %d, want 0", got)
	}
}

func TestTrySendNilAndUnbufferedWithoutReceiver(t *testing.T) {
	var nilCh chan int
	if TrySend(nilCh, 1) {
		t.Fatal("send to nil channel returned true")
	}
	if TrySend(make(chan int), 2) {
		t.Fatal("send to unbuffered channel without receiver returned true")
	}
}

func TestTrySendConcurrentSenders(t *testing.T) {
	const senders = 32
	ch := make(chan int, 1)
	start := make(chan struct{})
	results := make(chan int, senders)
	var wg sync.WaitGroup
	for i := 0; i < senders; i++ {
		wg.Add(1)
		go func(value int) {
			defer wg.Done()
			<-start
			if TrySend(ch, value) {
				results <- value
			}
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)

	successes := 0
	winner := -1
	for value := range results {
		successes++
		winner = value
	}
	if successes != 1 {
		t.Fatalf("successful sends = %d, want 1", successes)
	}
	if got := <-ch; got != winner {
		t.Fatalf("received %d, successful sender sent %d", got, winner)
	}
}
