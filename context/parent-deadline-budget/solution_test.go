package main

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

func TestCallWithBudgetParentDeadlineWins(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		parentDeadline := start.Add(30 * time.Millisecond)
		parent, cancel := context.WithDeadline(context.Background(), parentDeadline)
		defer cancel()

		calls := 0
		err := CallWithBudget(parent, 100*time.Millisecond, func(ctx context.Context) error {
			calls++
			deadline, ok := ctx.Deadline()
			if !ok || !deadline.Equal(parentDeadline) {
				t.Errorf("child deadline = %v (present: %v), want %v", deadline, ok, parentDeadline)
			}
			<-ctx.Done()
			return ctx.Err()
		})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("error = %v, want context.DeadlineExceeded", err)
		}
		if calls != 1 {
			t.Errorf("f called %d times, want 1", calls)
		}
		if elapsed := time.Since(start); elapsed != 30*time.Millisecond {
			t.Errorf("elapsed virtual time = %v, want 30ms", elapsed)
		}
	})
}

func TestCallWithBudgetOwnDeadlineWins(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		parent, cancel := context.WithDeadline(context.Background(), start.Add(100*time.Millisecond))
		defer cancel()

		err := CallWithBudget(parent, 20*time.Millisecond, func(ctx context.Context) error {
			deadline, ok := ctx.Deadline()
			want := start.Add(20 * time.Millisecond)
			if !ok || !deadline.Equal(want) {
				t.Errorf("child deadline = %v (present: %v), want %v", deadline, ok, want)
			}
			<-ctx.Done()
			return ctx.Err()
		})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("error = %v, want context.DeadlineExceeded", err)
		}
		if elapsed := time.Since(start); elapsed != 20*time.Millisecond {
			t.Errorf("elapsed virtual time = %v, want 20ms", elapsed)
		}
	})
}

func TestCallWithBudgetReturnsCallbackErrorAndCancelsChild(t *testing.T) {
	workErr := errors.New("work failed")
	type keyType struct{}
	parent := context.WithValue(context.Background(), keyType{}, "value")
	var child context.Context

	err := CallWithBudget(parent, time.Hour, func(ctx context.Context) error {
		child = ctx
		if got := ctx.Value(keyType{}); got != "value" {
			t.Errorf("inherited value = %v, want value", got)
		}
		return workErr
	})
	if !errors.Is(err, workErr) {
		t.Errorf("error = %v, want workErr", err)
	}
	if child == nil {
		t.Fatal("f was not called with a child context")
	}
	if !errors.Is(child.Err(), context.Canceled) {
		t.Errorf("child after return: Err() = %v, want context.Canceled", child.Err())
	}
}

func TestCallWithBudgetParentCancellation(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := CallWithBudget(parent, time.Hour, func(ctx context.Context) error {
		cancel()
		<-ctx.Done()
		return ctx.Err()
	})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error = %v, want context.Canceled", err)
	}
}

func TestCallWithBudgetSuccessfulCallbackCancelsChild(t *testing.T) {
	var child context.Context
	err := CallWithBudget(context.Background(), time.Hour, func(ctx context.Context) error {
		child = ctx
		return nil
	})
	if err != nil {
		t.Fatalf("error = %v, want nil", err)
	}
	if child == nil {
		t.Fatal("f was not called with a child context")
	}
	if !errors.Is(child.Err(), context.Canceled) {
		t.Errorf("child after return: Err() = %v, want context.Canceled", child.Err())
	}
}
