package main

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

type sharedCallResult struct {
	value string
	err   error
}

func TestSharedCallIndependentWaiterCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan context.Context, 1)
		release := make(chan struct{})
		s := NewSharedCall(context.Background(), func(ctx context.Context) (string, error) {
			started <- ctx
			select {
			case <-release:
				return "value", nil
			case <-ctx.Done():
				return "", ctx.Err()
			}
		})
		opCtx := <-started
		waiter, cancel := context.WithCancel(context.Background())
		defer cancel()
		a, b := make(chan sharedCallResult, 1), make(chan sharedCallResult, 1)
		go func() {
			value, err := s.Await(waiter)
			a <- sharedCallResult{value, err}
		}()
		go func() {
			value, err := s.Await(context.Background())
			b <- sharedCallResult{value, err}
		}()
		synctest.Wait()
		cancel()
		if got := <-a; got.value != "" || got.err != context.Canceled {
			t.Errorf("canceled waiter = %+v, want empty string and Canceled", got)
		}
		synctest.Wait()
		if err := opCtx.Err(); err != nil {
			t.Errorf("waiter cancellation reached operation: %v", err)
		}
		select {
		case got := <-b:
			t.Errorf("other waiter returned before operation completed: %+v", got)
		default:
		}
		close(release)
		if got := <-b; got.value != "value" || got.err != nil {
			t.Errorf("other waiter = %+v, want value and nil", got)
		}
		s.Close()
	})
}

func TestSharedCallContinuesWithoutWaiters(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan context.Context, 1)
		release := make(chan struct{})
		s := NewSharedCall(context.Background(), func(ctx context.Context) (string, error) {
			started <- ctx
			select {
			case <-release:
				return "cached", nil
			case <-ctx.Done():
				return "", ctx.Err()
			}
		})
		opCtx := <-started
		waiter, cancel := context.WithCancel(context.Background())
		returned := make(chan error, 1)
		go func() {
			_, err := s.Await(waiter)
			returned <- err
		}()
		synctest.Wait()
		cancel()
		if err := <-returned; err != context.Canceled {
			t.Errorf("waiter error = %v, want Canceled", err)
		}
		if err := opCtx.Err(); err != nil {
			t.Errorf("operation stopped after losing its only waiter: %v", err)
		}
		close(release)
		value, err := s.Await(context.Background())
		if value != "cached" || err != nil {
			t.Errorf("later waiter = (%q, %v), want (cached, nil)", value, err)
		}
		s.Close()
	})
}

func TestSharedCallCachesExactResultAndRunsOnce(t *testing.T) {
	workErr := errors.New("load failed")
	for _, result := range []struct {
		name  string
		value string
		err   error
	}{
		{"success", "value", nil},
		{"empty-success", "", nil},
		{"failure", "partial", workErr},
	} {
		t.Run(result.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var calls atomic.Int32
				var opCtx context.Context
				s := NewSharedCall(context.Background(), func(ctx context.Context) (string, error) {
					calls.Add(1)
					opCtx = ctx
					return result.value, result.err
				})
				var wg sync.WaitGroup
				for range 100 {
					wg.Add(1)
					go func() {
						defer wg.Done()
						value, err := s.Await(context.Background())
						if value != result.value || err != result.err {
							t.Errorf("result = (%q, %v), want (%q, %v)", value, err, result.value, result.err)
						}
					}()
				}
				wg.Wait()
				synctest.Wait()
				if opCtx == nil || opCtx.Err() != context.Canceled {
					t.Error("operation context was not released after normal completion")
				}
				s.Close()
				s.Close()
				value, err := s.Await(context.Background())
				if value != result.value || err != result.err {
					t.Errorf("result after Close = (%q, %v)", value, err)
				}
				if got := calls.Load(); got != 1 {
					t.Errorf("f calls = %d, want 1", got)
				}
			})
		})
	}
}

func TestSharedCallAlreadyCanceledWaiterWinsOverCachedResult(t *testing.T) {
	s := NewSharedCall(context.Background(), func(context.Context) (string, error) {
		return "ready", nil
	})
	defer s.Close()
	if value, err := s.Await(context.Background()); value != "ready" || err != nil {
		t.Fatalf("initial result = (%q, %v)", value, err)
	}
	for _, expired := range []bool{false, true} {
		var waiter context.Context
		var cancel context.CancelFunc
		want := context.Canceled
		if expired {
			waiter, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Hour))
			want = context.DeadlineExceeded
		} else {
			waiter, cancel = context.WithCancel(context.Background())
			cancel()
		}
		for range 100 {
			value, err := s.Await(waiter)
			if value != "" || err != want {
				t.Errorf("already canceled waiter = (%q, %v), want empty string and %v", value, err, want)
			}
		}
		cancel()
	}
}

func TestSharedCallServiceCancellationAndDeadline(t *testing.T) {
	for _, mode := range []string{"cancel", "already-canceled", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				type key struct{}
				base := context.WithValue(context.Background(), key{}, "trace")
				var service context.Context
				var cancel context.CancelFunc
				want := context.Canceled
				if mode == "deadline" {
					service, cancel = context.WithTimeout(base, 20*time.Millisecond)
					want = context.DeadlineExceeded
				} else {
					service, cancel = context.WithCancel(base)
				}
				defer cancel()
				if mode == "already-canceled" {
					cancel()
				}
				started := make(chan context.Context, 1)
				s := NewSharedCall(service, func(ctx context.Context) (string, error) {
					started <- ctx
					<-ctx.Done()
					return "", ctx.Err()
				})
				opCtx := <-started
				if got := opCtx.Value(key{}); got != "trace" {
					t.Errorf("operation value = %v, want trace", got)
				}
				if mode == "deadline" {
					got, ok := opCtx.Deadline()
					deadline, _ := service.Deadline()
					if !ok || !got.Equal(deadline) {
						t.Errorf("operation deadline = (%v, %v), want %v", got, ok, deadline)
					}
				} else {
					cancel()
				}
				value, err := s.Await(context.Background())
				if value != "" || err != want {
					t.Errorf("result = (%q, %v), want empty string and %v", value, err, want)
				}
				s.Close()
			})
		})
	}
}

func TestSharedCallConcurrentCloseWaitsForCallback(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		service, cancel := context.WithCancel(context.Background())
		defer cancel()
		started := make(chan struct{})
		canceled := make(chan struct{})
		release := make(chan struct{})
		finished := make(chan struct{})
		workErr := errors.New("cleanup finished")
		s := NewSharedCall(service, func(ctx context.Context) (string, error) {
			close(started)
			<-ctx.Done()
			close(canceled)
			<-release
			close(finished)
			return "final", workErr
		})
		<-started
		const count = 20
		closed := make(chan struct{}, count)
		for range count {
			go func() {
				s.Close()
				closed <- struct{}{}
			}()
		}
		<-canceled
		synctest.Wait()
		if got := len(closed); got != 0 {
			t.Errorf("%d Close calls returned before f finished", got)
		}
		if err := service.Err(); err != nil {
			t.Errorf("Close canceled service context: %v", err)
		}
		close(release)
		for range count {
			<-closed
		}
		select {
		case <-finished:
		default:
			t.Error("Close returned before callback finished")
		}
		value, err := s.Await(context.Background())
		if value != "final" || err != workErr {
			t.Errorf("result after Close = (%q, %v), want exact callback result", value, err)
		}
		s.Close()
	})
}
