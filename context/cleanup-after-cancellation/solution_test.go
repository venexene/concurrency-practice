package main

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

func TestCleanupDetachedContextAndOwnDeadline(t *testing.T) {
	for _, state := range []string{"active", "canceled", "expired", "shorter-deadline", "longer-deadline"} {
		t.Run(state, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				type requestKey struct{}
				base := context.WithValue(context.Background(), requestKey{}, "abc")
				start := time.Now()
				const budget = 100 * time.Millisecond
				var parent context.Context
				var cancel context.CancelFunc
				switch state {
				case "expired":
					parent, cancel = context.WithDeadline(base, start.Add(-time.Second))
				case "shorter-deadline":
					parent, cancel = context.WithTimeout(base, 10*time.Millisecond)
				case "longer-deadline":
					parent, cancel = context.WithTimeout(base, time.Second)
				default:
					parent, cancel = context.WithCancel(base)
				}
				defer cancel()
				if state == "canceled" {
					cancel()
				}

				calls := 0
				err := Cleanup(parent, budget, func(ctx context.Context) error {
					calls++
					if got := ctx.Value(requestKey{}); got != "abc" {
						t.Errorf("Value(requestKey) = %v, want abc", got)
					}
					if err := ctx.Err(); err != nil {
						t.Errorf("context initially canceled: %v", err)
					}
					deadline, ok := ctx.Deadline()
					if want := start.Add(budget); !ok || !deadline.Equal(want) {
						t.Errorf("Deadline() = (%v, %v), want (%v, true)", deadline, ok, want)
					}
					<-ctx.Done()
					return ctx.Err()
				})
				if err != context.DeadlineExceeded {
					t.Errorf("Cleanup error = %v, want DeadlineExceeded", err)
				}
				if calls != 1 {
					t.Errorf("callback calls = %d, want 1", calls)
				}
				if elapsed := time.Since(start); elapsed != budget {
					t.Errorf("elapsed virtual time = %v, want %v", elapsed, budget)
				}
			})
		})
	}
}

func TestCleanupIgnoresCancellationDuringCallback(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := Cleanup(parent, time.Hour, func(ctx context.Context) error {
		cancel()
		if err := ctx.Err(); err != nil {
			t.Errorf("request cancellation reached cleanup: %v", err)
		}
		select {
		case <-ctx.Done():
			t.Error("cleanup Done closed after request cancellation")
		default:
		}
		return nil
	})
	if err != nil {
		t.Errorf("Cleanup error = %v, want nil", err)
	}
}

func TestCleanupReturnsCallbackResultAndReleasesContext(t *testing.T) {
	workErr := errors.New("cleanup failed")
	for _, result := range []struct {
		name string
		err  error
	}{
		{"success", nil},
		{"failure", workErr},
	} {
		t.Run(result.name, func(t *testing.T) {
			parent, cancel := context.WithCancel(context.Background())
			cancel()
			var child context.Context
			calls := 0
			err := Cleanup(parent, time.Hour, func(ctx context.Context) error {
				calls++
				child = ctx
				return result.err
			})
			if err != result.err {
				t.Errorf("Cleanup error = %v, want exact callback error %v", err, result.err)
			}
			if calls != 1 || child == nil {
				t.Fatalf("callback calls = %d, captured context = %v", calls, child)
			}
			select {
			case <-child.Done():
			default:
				t.Error("cleanup context still active after return")
			}
			if err := child.Err(); err != context.Canceled {
				t.Errorf("context after return: Err() = %v, want Canceled", err)
			}
		})
	}
}

func TestCleanupWaitsForCallbackAfterBudget(t *testing.T) {
	for _, result := range []struct {
		name string
		err  error
	}{
		{"success", nil},
		{"failure", errors.New("cleanup failed after deadline")},
	} {
		t.Run(result.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				release := make(chan struct{})
				deadlineSeen := make(chan struct{})
				finished := make(chan struct{})
				returned := make(chan error, 1)
				go func() {
					returned <- Cleanup(context.Background(), 20*time.Millisecond, func(ctx context.Context) error {
						<-ctx.Done()
						close(deadlineSeen)
						<-release
						close(finished)
						return result.err
					})
				}()
				<-deadlineSeen
				synctest.Wait()
				select {
				case err := <-returned:
					close(release)
					t.Fatalf("Cleanup returned before callback finished: %v", err)
				default:
				}
				close(release)
				if err := <-returned; err != result.err {
					t.Errorf("Cleanup error = %v, want exact callback error %v", err, result.err)
				}
				select {
				case <-finished:
				default:
					t.Error("Cleanup returned before callback finished")
				}
			})
		})
	}
}
