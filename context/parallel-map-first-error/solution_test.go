package main

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"
)

type mapAllResult struct {
	values []int
	err    error
}

func runMapAll(ctx context.Context, nums []int, f func(context.Context, int) (int, error)) ([]int, error) {
	return MapAll(ctx, nums, f)
}

func awaitMapAll(t *testing.T, done <-chan mapAllResult) mapAllResult {
	t.Helper()
	select {
	case result := <-done:
		return result
	case <-time.After(time.Second):
		t.Fatal("MapAll did not finish")
		return mapAllResult{}
	}
}

func TestMapAllEmpty(t *testing.T) {
	done := make(chan mapAllResult, 1)
	go func() {
		values, err := runMapAll(context.Background(), nil, func(context.Context, int) (int, error) {
			t.Error("f was called for empty input")
			return 0, nil
		})
		done <- mapAllResult{values, err}
	}()

	result := awaitMapAll(t, done)
	if result.err != nil || len(result.values) != 0 {
		t.Errorf("MapAll(nil) = (%v, %v), want empty result and nil error", result.values, result.err)
	}
}

func TestMapAllParallelResultsKeepInputOrder(t *testing.T) {
	input := []int{1, 2, 3}
	started := make(chan int, len(input))
	release := []chan struct{}{nil, make(chan struct{}), make(chan struct{}), make(chan struct{})}
	done := make(chan mapAllResult, 1)
	go func() {
		values, err := runMapAll(context.Background(), input, func(_ context.Context, x int) (int, error) {
			started <- x
			<-release[x]
			return x * 10, nil
		})
		done <- mapAllResult{values, err}
	}()

	for range input {
		select {
		case <-started:
		case <-time.After(time.Second):
			for _, ch := range release[1:] {
				close(ch)
			}
			t.Fatal("not all callbacks started concurrently")
		}
	}
	for _, x := range []int{3, 1, 2} {
		close(release[x])
	}
	result := awaitMapAll(t, done)
	if result.err != nil || !slices.Equal(result.values, []int{10, 20, 30}) {
		t.Errorf("MapAll = (%v, %v), want ([10 20 30], nil)", result.values, result.err)
	}
	if !slices.Equal(input, []int{1, 2, 3}) {
		t.Errorf("MapAll changed input: %v", input)
	}
}

func TestMapAllFirstErrorCancelsAndWaits(t *testing.T) {
	bad := errors.New("bad value")
	parent, cancelParent := context.WithCancel(context.Background())
	defer cancelParent()
	started := make(chan int, 3)
	finished := make(chan int, 2)
	releaseError := make(chan struct{})
	done := make(chan mapAllResult, 1)
	go func() {
		values, err := runMapAll(parent, []int{1, 2, 3}, func(ctx context.Context, x int) (int, error) {
			started <- x
			if x == 2 {
				<-releaseError
				return 0, bad
			}
			<-ctx.Done()
			finished <- x
			return 0, ctx.Err()
		})
		done <- mapAllResult{values, err}
	}()

	for range 3 {
		select {
		case <-started:
		case <-time.After(time.Second):
			close(releaseError)
			t.Fatal("not all callbacks started before the first error")
		}
	}
	close(releaseError)
	result := awaitMapAll(t, done)
	if result.values != nil || !errors.Is(result.err, bad) {
		t.Errorf("MapAll = (%v, %v), want (nil, bad)", result.values, result.err)
	}
	if len(finished) != 2 {
		t.Errorf("MapAll returned before all other callbacks finished: %d of 2", len(finished))
	}
}

func TestMapAllExternalCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{}, 2)
	finished := make(chan struct{}, 2)
	done := make(chan mapAllResult, 1)
	go func() {
		values, err := runMapAll(ctx, []int{1, 2}, func(ctx context.Context, _ int) (int, error) {
			started <- struct{}{}
			<-ctx.Done()
			finished <- struct{}{}
			return 0, ctx.Err()
		})
		done <- mapAllResult{values, err}
	}()

	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("not all callbacks started before cancellation")
		}
	}
	cancel()
	result := awaitMapAll(t, done)
	if result.values != nil || !errors.Is(result.err, context.Canceled) {
		t.Errorf("MapAll = (%v, %v), want (nil, context.Canceled)", result.values, result.err)
	}
	if len(finished) != 2 {
		t.Errorf("MapAll returned before all callbacks finished: %d of 2", len(finished))
	}
}
