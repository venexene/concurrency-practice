package main

import (
	"errors"
	"testing"
	"time"
)

func TestQueueErrClosedSentinel(t *testing.T) {
	if ErrClosed == nil {
		t.Fatal("ErrClosed is nil")
	}
}

func TestQueueFIFOAndDrainAfterClose(t *testing.T) {
	q := NewQueue(2)
	for _, value := range []int{0, 7} {
		if result := safePut(q, value); result.panicked || result.err != nil {
			t.Fatalf("Put(%d) = %+v, want no error or panic", value, result)
		}
	}
	wantGet(t, q, 0)
	if result := safePut(q, 9); result.panicked || result.err != nil {
		t.Fatalf("Put(9) after Get = %+v, want no error or panic", result)
	}
	wantGet(t, q, 7)

	wantClose(t, q)
	wantClose(t, q)
	wantClosedPut(t, q, 10)
	wantGet(t, q, 9)
	wantClosedGet(t, q)
}

func TestQueuePutWaitsForSpace(t *testing.T) {
	q := NewQueue(1)
	if result := safePut(q, 1); result.panicked || result.err != nil {
		t.Fatalf("first Put = %+v", result)
	}
	result := make(chan putResult, 1)
	go func() { result <- safePut(q, 2) }()
	select {
	case got := <-result:
		t.Fatalf("Put on full queue returned before space was freed: %+v", got)
	default:
	}
	wantGet(t, q, 1)
	select {
	case got := <-result:
		if got.panicked || got.err != nil {
			t.Fatalf("Put after space was freed = %+v, want success", got)
		}
	case <-time.After(time.Second):
		t.Fatal("Put did not complete after space was freed")
	}
	wantGet(t, q, 2)
	wantClose(t, q)
}

func TestQueueGetWaitsForElement(t *testing.T) {
	q := NewQueue(1)
	result := make(chan getResult, 1)
	go func() { result <- safeGet(q) }()
	select {
	case got := <-result:
		t.Fatalf("Get on empty open queue returned before Put: %+v", got)
	default:
	}
	if got := safePut(q, 5); got.panicked || got.err != nil {
		t.Fatalf("Put(5) = %+v", got)
	}
	select {
	case got := <-result:
		if got.panicked || got.value != 5 || got.err != nil {
			t.Fatalf("waiting Get = %+v, want (5, nil)", got)
		}
	case <-time.After(time.Second):
		t.Fatal("Get did not complete after Put")
	}
	wantClose(t, q)
}

func TestQueueCloseWakesWaitingPutAndGet(t *testing.T) {
	t.Run("Put", func(t *testing.T) {
		q := NewQueue(1)
		if got := safePut(q, 4); got.panicked || got.err != nil {
			t.Fatalf("first Put = %+v", got)
		}
		result := make(chan putResult, 1)
		go func() { result <- safePut(q, 5) }()
		wantClose(t, q)
		select {
		case got := <-result:
			if got.panicked || !errors.Is(got.err, ErrClosed) {
				t.Fatalf("waiting Put after Close = %+v, want ErrClosed", got)
			}
		case <-time.After(time.Second):
			t.Fatal("waiting Put did not wake after Close")
		}
		wantGet(t, q, 4)
		wantClosedGet(t, q)
	})
	t.Run("Get", func(t *testing.T) {
		q := NewQueue(1)
		result := make(chan getResult, 1)
		go func() { result <- safeGet(q) }()
		wantClose(t, q)
		select {
		case got := <-result:
			if got.panicked || got.value != 0 || !errors.Is(got.err, ErrClosed) {
				t.Fatalf("waiting Get after Close = %+v, want (0, ErrClosed)", got)
			}
		case <-time.After(time.Second):
			t.Fatal("waiting Get did not wake after Close")
		}
	})
}

type putResult struct {
	err      error
	panicked bool
}

type getResult struct {
	value    int
	err      error
	panicked bool
}

func safePut(q *Queue, value int) (result putResult) {
	defer func() {
		if recover() != nil {
			result.panicked = true
		}
	}()
	result.err = q.Put(value)
	return result
}

func safeGet(q *Queue) (result getResult) {
	defer func() {
		if recover() != nil {
			result.panicked = true
		}
	}()
	result.value, result.err = q.Get()
	return result
}

func safeClose(q *Queue) (panicked bool) {
	defer func() {
		if recover() != nil {
			panicked = true
		}
	}()
	q.Close()
	return false
}

func wantClose(t *testing.T, q *Queue) {
	t.Helper()
	result := make(chan bool, 1)
	go func() { result <- safeClose(q) }()
	select {
	case panicked := <-result:
		if panicked {
			t.Fatal("Close panicked")
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not complete")
	}
}

func wantGet(t *testing.T, q *Queue, want int) {
	t.Helper()
	result := make(chan getResult, 1)
	go func() { result <- safeGet(q) }()
	select {
	case got := <-result:
		if got.panicked || got.value != want || got.err != nil {
			t.Fatalf("Get = %+v, want (%d, nil)", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("Get did not complete")
	}
}

func wantClosedGet(t *testing.T, q *Queue) {
	t.Helper()
	result := make(chan getResult, 1)
	go func() { result <- safeGet(q) }()
	select {
	case got := <-result:
		if got.panicked || got.value != 0 || !errors.Is(got.err, ErrClosed) {
			t.Fatalf("Get after drain = %+v, want (0, ErrClosed)", got)
		}
	case <-time.After(time.Second):
		t.Fatal("Get after drain did not complete")
	}
}

func wantClosedPut(t *testing.T, q *Queue, value int) {
	t.Helper()
	result := make(chan putResult, 1)
	go func() { result <- safePut(q, value) }()
	select {
	case got := <-result:
		if got.panicked || !errors.Is(got.err, ErrClosed) {
			t.Fatalf("Put after Close = %+v, want ErrClosed", got)
		}
	case <-time.After(time.Second):
		t.Fatal("Put after Close did not complete")
	}
}
