package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestErrClosedIsSentinel(t *testing.T) {
	if ErrClosed == nil {
		t.Fatal("ErrClosed is nil, want a non-nil sentinel error")
	}
}

func TestQueueFIFOAndCapacity(t *testing.T) {
	q := NewQueue(2)
	ctx := context.Background()
	for _, value := range []int{7, 8} {
		if err := q.Put(ctx, value); err != nil {
			t.Fatalf("Put(%d) = %v, want nil", value, err)
		}
	}
	wantGet(t, q, 7)
	if err := q.Put(ctx, 9); err != nil {
		t.Fatalf("Put(9) after Get = %v, want nil", err)
	}
	wantGet(t, q, 8)
	wantGet(t, q, 9)
	q.Close()
}

func TestQueuePutWaitsForSpace(t *testing.T) {
	q := NewQueue(1)
	ctx := context.Background()
	if err := q.Put(ctx, 1); err != nil {
		t.Fatal(err)
	}
	result := make(chan putResult, 1)
	go func() { result <- safePut(q, ctx, 2) }()
	wantGet(t, q, 1)
	select {
	case got := <-result:
		if got.panicked || got.err != nil {
			t.Fatalf("blocked Put after space freed = %+v, want nil error", got)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked Put did not complete after space was freed")
	}
	wantGet(t, q, 2)
	q.Close()
}

func TestQueueCancellationDoesNotChangeContents(t *testing.T) {
	q := NewQueue(1)
	if err := q.Put(context.Background(), 7); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := q.Put(ctx, 8); !errors.Is(err, context.Canceled) {
		t.Fatalf("Put on full queue with canceled context = %v, want context.Canceled", err)
	}
	wantGet(t, q, 7)
	if _, err := q.Get(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Get on empty queue with canceled context = %v, want context.Canceled", err)
	}
	q.Close()
}

func TestQueueCloseDrainsAndRejects(t *testing.T) {
	q := NewQueue(2)
	ctx := context.Background()
	for _, value := range []int{0, 8} {
		if err := q.Put(ctx, value); err != nil {
			t.Fatal(err)
		}
	}
	q.Close()
	q.Close()
	result := safePut(q, ctx, 9)
	if result.panicked || result.err == nil || !errors.Is(result.err, ErrClosed) {
		t.Errorf("Put after Close = %+v, want ErrClosed without panic", result)
	}
	wantGet(t, q, 0)
	wantGet(t, q, 8)
	wantClosedGet(t, q)
}

func TestQueueBlockedPutWakesOnClose(t *testing.T) {
	q := NewQueue(1)
	if err := q.Put(context.Background(), 4); err != nil {
		t.Fatal(err)
	}
	result := make(chan putResult, 1)
	go func() { result <- safePut(q, context.Background(), 5) }()
	q.Close()
	select {
	case got := <-result:
		if got.panicked || got.err == nil || !errors.Is(got.err, ErrClosed) {
			t.Errorf("blocked Put after Close = %+v, want ErrClosed without panic", got)
		}
	case <-time.After(time.Second):
		t.Error("blocked Put did not wake after Close")
	}
	wantGet(t, q, 4)
	wantClosedGet(t, q)
}

func TestQueueBlockedGetWakesOnClose(t *testing.T) {
	q := NewQueue(1)
	result := make(chan getResult, 1)
	go func() {
		value, err := q.Get(context.Background())
		result <- getResult{value, err}
	}()
	q.Close()
	select {
	case got := <-result:
		if got.value != 0 || got.err == nil || !errors.Is(got.err, ErrClosed) {
			t.Errorf("blocked Get after Close = %+v, want (0, ErrClosed)", got)
		}
	case <-time.After(time.Second):
		t.Error("blocked Get did not wake after Close")
	}
}

func TestQueueConcurrentPutAndClose(t *testing.T) {
	for i := 1; i <= 100; i++ {
		q := NewQueue(1)
		start := make(chan struct{})
		result := make(chan putResult, 1)
		go func(value int) {
			<-start
			result <- safePut(q, context.Background(), value)
		}(i)
		close(start)
		q.Close()

		var got putResult
		select {
		case got = <-result:
		case <-time.After(time.Second):
			t.Fatalf("iteration %d: concurrent Put did not finish after Close", i)
		}
		if got.panicked {
			t.Fatalf("iteration %d: concurrent Put panicked", i)
		}
		if got.err == nil {
			wantGet(t, q, i)
		} else if !errors.Is(got.err, ErrClosed) {
			t.Fatalf("iteration %d: Put error = %v, want ErrClosed", i, got.err)
		}
		wantClosedGet(t, q)
	}
}

type putResult struct {
	err      error
	panicked bool
}

type getResult struct {
	value int
	err   error
}

func safePut(q *Queue, ctx context.Context, value int) (result putResult) {
	defer func() {
		if recover() != nil {
			result.panicked = true
		}
	}()
	result.err = q.Put(ctx, value)
	return result
}

func wantGet(t *testing.T, q *Queue, want int) {
	t.Helper()
	value, err := q.Get(context.Background())
	if err != nil || value != want {
		t.Fatalf("Get() = (%d, %v), want (%d, nil)", value, err, want)
	}
}

func wantClosedGet(t *testing.T, q *Queue) {
	t.Helper()
	value, err := q.Get(context.Background())
	if value != 0 || err == nil || !errors.Is(err, ErrClosed) {
		t.Fatalf("Get() after drain = (%d, %v), want (0, ErrClosed)", value, err)
	}
}
