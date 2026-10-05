package main

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

type versionResult struct {
	value   string
	version uint64
	err     error
}

func awaitVersion(v *Versioned, ctx context.Context, after uint64) <-chan versionResult {
	result := make(chan versionResult, 1)
	go func() {
		value, version, err := v.WaitNext(ctx, after)
		result <- versionResult{value, version, err}
	}()
	return result
}

func assertVersionResult(t *testing.T, got versionResult, value string, version uint64, err error) {
	t.Helper()
	if got.value != value || got.version != version || !errors.Is(got.err, err) {
		t.Errorf("result = (%q, %d, %v), want (%q, %d, %v)", got.value, got.version, got.err, value, version, err)
	}
}

func assertVersionStillWaiting(t *testing.T, result <-chan versionResult) {
	t.Helper()
	// Call synctest.Wait before this helper so the waiter can reach its wait.
	select {
	case got := <-result:
		t.Fatalf("WaitNext returned too early: %+v", got)
	default:
	}
}

func TestVersionedWaitsForStrictlyNewerVersion(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		v := NewVersioned("initial")
		defer v.Close()
		result := awaitVersion(v, context.Background(), 2)
		synctest.Wait()
		assertVersionStillWaiting(t, result)
		for _, value := range []string{"one", "two"} {
			if err := v.Set(value); err != nil {
				t.Fatal(err)
			}
			synctest.Wait()
			assertVersionStillWaiting(t, result)
		}
		if err := v.Set("three"); err != nil {
			t.Fatal(err)
		}
		assertVersionResult(t, <-result, "three", 3, nil)
	})
}

func TestVersionedInitialVersionIsNotAnUpdate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		v := NewVersioned("a")
		defer v.Close()
		result := awaitVersion(v, context.Background(), 0)
		synctest.Wait()
		assertVersionStillWaiting(t, result)
		if err := v.Set("a"); err != nil {
			t.Fatal(err)
		}
		assertVersionResult(t, <-result, "a", 1, nil)
	})
}

func TestVersionedReturnsAvailableVersionAndAllowsSkipping(t *testing.T) {
	v := NewVersioned("initial")
	defer v.Close()
	for _, value := range []string{"one", "two", ""} {
		if err := v.Set(value); err != nil {
			t.Fatal(err)
		}
	}
	for _, after := range []uint64{0, 1, 2} {
		value, version, err := v.WaitNext(context.Background(), after)
		assertVersionResult(t, versionResult{value, version, err}, "", 3, nil)
	}
}

func TestVersionedOneUpdateWakesAllWaiters(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		v := NewVersioned("initial")
		defer v.Close()
		results := make([]<-chan versionResult, 50)
		for i := range results {
			results[i] = awaitVersion(v, context.Background(), 0)
		}
		synctest.Wait()
		if err := v.Set("next"); err != nil {
			t.Fatal(err)
		}
		for _, result := range results {
			assertVersionResult(t, <-result, "next", 1, nil)
		}
	})
}

func TestVersionedCancellationIsIndependent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		v := NewVersioned("initial")
		defer v.Close()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		a := awaitVersion(v, ctx, 0)
		b := awaitVersion(v, context.Background(), 0)
		synctest.Wait()
		cancel()
		assertVersionResult(t, <-a, "", 0, context.Canceled)
		synctest.Wait()
		assertVersionStillWaiting(t, b)
		if err := v.Set("next"); err != nil {
			t.Fatal(err)
		}
		assertVersionResult(t, <-b, "next", 1, nil)
	})
}

func TestVersionedCanceledAndExpiredContexts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		v := NewVersioned("initial")
		defer v.Close()
		canceled, cancel := context.WithCancel(context.Background())
		cancel()
		assertVersionResult(t, <-awaitVersion(v, canceled, 0), "", 0, context.Canceled)
		expired, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer stop()
		start := time.Now()
		assertVersionResult(t, <-awaitVersion(v, expired, 0), "", 0, context.DeadlineExceeded)
		if elapsed := time.Since(start); elapsed != 20*time.Millisecond {
			t.Errorf("elapsed virtual time = %v, want 20ms", elapsed)
		}
		assertVersionResult(t, <-awaitVersion(v, expired, 100), "", 0, context.DeadlineExceeded)
	})
}

func TestVersionedCloseWakesWaitersAndRejectsOperations(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		v := NewVersioned("initial")
		if err := v.Set("latest"); err != nil {
			t.Fatal(err)
		}
		results := make([]<-chan versionResult, 50)
		for i := range results {
			results[i] = awaitVersion(v, context.Background(), 1)
		}
		synctest.Wait()
		v.Close()
		v.Close()
		for _, result := range results {
			assertVersionResult(t, <-result, "", 0, ErrClosed)
		}
		if err := v.Set("forbidden"); !errors.Is(err, ErrClosed) {
			t.Errorf("Set after Close = %v, want ErrClosed", err)
		}
		// Even a version already available before Close must not be returned.
		assertVersionResult(t, <-awaitVersion(v, context.Background(), 0), "", 0, ErrClosed)
	})
}

func TestVersionedRepeatedCancellationDoesNotLeaveBlockedWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		v := NewVersioned("initial")
		defer v.Close()
		for range 100 {
			ctx, cancel := context.WithCancel(context.Background())
			result := awaitVersion(v, ctx, 0)
			synctest.Wait()
			cancel()
			assertVersionResult(t, <-result, "", 0, context.Canceled)
			// Also lets already-started cancellation callbacks finish.
			synctest.Wait()
		}
		if err := v.Set("still usable"); err != nil {
			t.Fatal(err)
		}
		assertVersionResult(t, <-awaitVersion(v, context.Background(), 0), "still usable", 1, nil)
	})
}

func TestVersionedSetRacingWithStartOfWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for range 100 {
			v := NewVersioned("initial")
			start := make(chan struct{})
			result := make(chan versionResult, 1)
			setDone := make(chan error, 1)
			go func() {
				<-start
				value, version, err := v.WaitNext(context.Background(), 0)
				result <- versionResult{value, version, err}
			}()
			go func() {
				<-start
				setDone <- v.Set("next")
			}()
			close(start)
			if err := <-setDone; err != nil {
				t.Error(err)
			}
			assertVersionResult(t, <-result, "next", 1, nil)
			v.Close()
		}
	})
}

func TestVersionedConcurrentReadersSeeConsistentPairs(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		v := NewVersioned("0")
		defer v.Close()
		const updates = 100
		var wg sync.WaitGroup
		for range 30 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				var after uint64
				for after < updates {
					value, version, err := v.WaitNext(context.Background(), after)
					if err != nil || version <= after || version > updates {
						t.Errorf("invalid update after %d: (%q, %d, %v)", after, value, version, err)
						return
					}
					if value != strconv.FormatUint(version, 10) {
						t.Errorf("inconsistent pair: value=%q, version=%d", value, version)
					}
					after = version
				}
			}()
		}
		for i := uint64(1); i <= updates; i++ {
			if err := v.Set(strconv.FormatUint(i, 10)); err != nil {
				t.Fatal(err)
			}
			if i%10 == 0 {
				synctest.Wait()
			}
		}
		wg.Wait()
	})
}

func TestVersionedConcurrentSetIncrementsEveryTime(t *testing.T) {
	v := NewVersioned("initial")
	defer v.Close()
	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := v.Set("same"); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	value, version, err := v.WaitNext(context.Background(), 0)
	assertVersionResult(t, versionResult{value, version, err}, "same", 100, nil)
}

func TestVersionedConcurrentSetCancelAndClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for range 50 {
			v := NewVersioned("initial")
			ctx, cancel := context.WithCancel(context.Background())
			result := awaitVersion(v, ctx, 0)
			synctest.Wait()
			start := make(chan struct{})
			setDone := make(chan error, 1)
			var wg sync.WaitGroup
			wg.Add(3)
			go func() { defer wg.Done(); <-start; setDone <- v.Set("next") }()
			go func() { defer wg.Done(); <-start; cancel() }()
			go func() { defer wg.Done(); <-start; v.Close() }()
			close(start)
			wg.Wait()
			got := <-result
			setErr := <-setDone
			if setErr != nil && !errors.Is(setErr, ErrClosed) {
				t.Errorf("unexpected Set error: %v", setErr)
			}
			if got.err == nil {
				assertVersionResult(t, got, "next", 1, nil)
				if setErr != nil {
					t.Error("WaitNext succeeded although Set was rejected")
				}
			} else if errors.Is(got.err, ErrClosed) || got.err == context.Canceled {
				assertVersionResult(t, got, "", 0, got.err)
			} else {
				t.Errorf("unexpected WaitNext error: %v", got.err)
			}
			v.Close()
			assertVersionResult(t, <-awaitVersion(v, context.Background(), 0), "", 0, ErrClosed)
		}
	})
}
