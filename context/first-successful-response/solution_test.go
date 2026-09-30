package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

type firstSuccessResult struct {
	value string
	err   error
}

type firstSuccessFunc func(context.Context, []func(context.Context) (string, error)) (string, error)

func forEachFirstSuccess(t *testing.T, test func(*testing.T, firstSuccessFunc)) {
	t.Helper()
	for _, implementation := range []struct {
		name string
		run  firstSuccessFunc
	}{
		{"FirstSuccess", FirstSuccess},
		{"FirstSuccessCoord", FirstSuccessCoord},
	} {
		t.Run(implementation.name, func(t *testing.T) {
			test(t, implementation.run)
		})
	}
}

func waitFirstSuccess(t *testing.T, done <-chan firstSuccessResult, cancel context.CancelFunc) firstSuccessResult {
	t.Helper()
	select {
	case result := <-done:
		return result
	case <-time.After(time.Second):
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
		}
		t.Fatal("FirstSuccess did not finish before the watchdog")
		return firstSuccessResult{}
	}
}

func waitCallsStarted(t *testing.T, started <-chan int, count int, cancel context.CancelFunc) {
	t.Helper()
	for range count {
		select {
		case <-started:
		case <-time.After(time.Second):
			cancel()
			t.Fatal("not all calls started concurrently")
		}
	}
}

func TestFirstSuccessCancelsOtherCallsAndWaits(t *testing.T) {
	forEachFirstSuccess(t, func(t *testing.T, run firstSuccessFunc) {
		parent, cancel := context.WithCancel(context.Background())
		defer cancel()
		started := make(chan int, 3)
		finished := make(chan struct{}, 1)
		releaseSuccess := make(chan struct{})
		bad := errors.New("failed")
		calls := []func(context.Context) (string, error){
			func(context.Context) (string, error) {
				started <- 0
				return "", bad
			},
			func(context.Context) (string, error) {
				started <- 1
				<-releaseSuccess
				return "ok", nil
			},
			func(ctx context.Context) (string, error) {
				started <- 2
				<-ctx.Done()
				finished <- struct{}{}
				return "", ctx.Err()
			},
		}
		done := make(chan firstSuccessResult, 1)
		go func() {
			value, err := run(parent, calls)
			done <- firstSuccessResult{value, err}
		}()

		waitCallsStarted(t, started, len(calls), cancel)
		close(releaseSuccess)
		result := waitFirstSuccess(t, done, cancel)
		if result.value != "ok" || result.err != nil {
			t.Errorf("FirstSuccess = (%q, %v), want (ok, nil)", result.value, result.err)
		}
		if len(finished) != 1 {
			t.Error("FirstSuccess returned before the canceled call finished")
		}
	})
}

func TestFirstSuccessAllFailedKeepsInputOrder(t *testing.T) {
	forEachFirstSuccess(t, func(t *testing.T, run firstSuccessFunc) {
		parent, cancel := context.WithCancel(context.Background())
		defer cancel()
		first := errors.New("first")
		second := errors.New("second")
		third := errors.New("third")
		errs := []error{first, second, third}
		started := make(chan int, 3)
		release := []chan struct{}{make(chan struct{}), make(chan struct{}), make(chan struct{})}
		calls := make([]func(context.Context) (string, error), len(errs))
		for i := range calls {
			i := i
			calls[i] = func(context.Context) (string, error) {
				started <- i
				<-release[i]
				return "", errs[i]
			}
		}
		done := make(chan firstSuccessResult, 1)
		go func() {
			value, err := run(parent, calls)
			done <- firstSuccessResult{value, err}
		}()

		waitCallsStarted(t, started, len(calls), cancel)
		for _, i := range []int{2, 0, 1} {
			close(release[i])
		}
		result := waitFirstSuccess(t, done, cancel)
		if result.value != "" {
			t.Errorf("value = %q, want empty string", result.value)
		}
		var all AllFailed
		if !errors.As(result.err, &all) {
			t.Fatalf("error = %v, want AllFailed", result.err)
		}
		if len(all.errors) != len(errs) {
			t.Fatalf("AllFailed contains %d errors, want %d", len(all.errors), len(errs))
		}
		for i, want := range errs {
			if !errors.Is(all.errors[i], want) {
				t.Errorf("AllFailed error[%d] = %v, want %v", i, all.errors[i], want)
			}
		}
	})
}

func TestFirstSuccessExternalCancellation(t *testing.T) {
	forEachFirstSuccess(t, func(t *testing.T, run firstSuccessFunc) {
		parent, cancel := context.WithCancel(context.Background())
		defer cancel()
		started := make(chan int, 2)
		finished := make(chan int, 2)
		calls := make([]func(context.Context) (string, error), 2)
		for i := range calls {
			i := i
			calls[i] = func(ctx context.Context) (string, error) {
				started <- i
				<-ctx.Done()
				finished <- i
				return "", ctx.Err()
			}
		}
		done := make(chan firstSuccessResult, 1)
		go func() {
			value, err := run(parent, calls)
			done <- firstSuccessResult{value, err}
		}()

		waitCallsStarted(t, started, len(calls), cancel)
		cancel()
		result := waitFirstSuccess(t, done, cancel)
		if result.value != "" || !errors.Is(result.err, context.Canceled) {
			t.Errorf("FirstSuccess = (%q, %v), want (empty, context.Canceled)", result.value, result.err)
		}
		if len(finished) != len(calls) {
			t.Errorf("FirstSuccess returned before all calls finished: %d/%d", len(finished), len(calls))
		}
	})
}

func TestFirstSuccessEmptyStringIsSuccess(t *testing.T) {
	forEachFirstSuccess(t, func(t *testing.T, run firstSuccessFunc) {
		value, err := run(context.Background(), []func(context.Context) (string, error){
			func(context.Context) (string, error) { return "", nil },
		})
		if value != "" || err != nil {
			t.Errorf("FirstSuccess = (%q, %v), want (empty, nil)", value, err)
		}
	})
}

func TestFirstSuccessExternalCancellationBeforeLateSuccess(t *testing.T) {
	forEachFirstSuccess(t, func(t *testing.T, run firstSuccessFunc) {
		parent, cancel := context.WithCancel(context.Background())
		defer cancel()
		started := make(chan struct{})
		done := make(chan firstSuccessResult, 1)
		go func() {
			value, err := run(parent, []func(context.Context) (string, error){
				func(ctx context.Context) (string, error) {
					close(started)
					<-ctx.Done()
					return "late", nil
				},
			})
			done <- firstSuccessResult{value, err}
		}()

		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("call did not start")
		}
		cancel()
		result := waitFirstSuccess(t, done, cancel)
		if result.value != "" || !errors.Is(result.err, context.Canceled) {
			t.Errorf("FirstSuccess = (%q, %v), want (empty, context.Canceled)", result.value, result.err)
		}
	})
}

func TestFirstSuccessKeepsWinnerAfterParentCancellation(t *testing.T) {
	forEachFirstSuccess(t, func(t *testing.T, run firstSuccessFunc) {
		parent, cancel := context.WithCancel(context.Background())
		defer cancel()

		started := make(chan int, 2)
		releaseWinner := make(chan struct{})
		loserCanceled := make(chan struct{})
		releaseLoser := make(chan struct{})
		defer close(releaseLoser)

		calls := []func(context.Context) (string, error){
			func(context.Context) (string, error) {
				started <- 0
				<-releaseWinner
				return "winner", nil
			},
			func(ctx context.Context) (string, error) {
				started <- 1
				<-ctx.Done()
				close(loserCanceled)
				<-releaseLoser
				return "late", nil
			},
		}

		done := make(chan firstSuccessResult, 1)
		go func() {
			value, err := run(parent, calls)
			done <- firstSuccessResult{value, err}
		}()

		waitCallsStarted(t, started, len(calls), cancel)
		close(releaseWinner)
		select {
		case <-loserCanceled:
		case <-time.After(time.Second):
			t.Fatal("the winning call did not cancel the other call")
		}

		cancel()
		releaseLoser <- struct{}{}
		result := waitFirstSuccess(t, done, cancel)
		if result.value != "winner" || result.err != nil {
			t.Errorf("got (%q, %v), want (winner, nil)", result.value, result.err)
		}
	})
}
