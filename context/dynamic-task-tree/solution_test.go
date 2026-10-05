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

func startTree(ctx context.Context, root Task) <-chan error {
	result := make(chan error, 1)
	go func() { result <- RunTree(ctx, root) }()
	return result
}

func assertTreeWaiting(t *testing.T, result <-chan error) {
	t.Helper()
	// The caller must first let the test goroutines block with synctest.Wait.
	select {
	case err := <-result:
		t.Fatalf("RunTree returned while a task was still held: %v", err)
	default:
	}
}

func TestRunTreeRootResult(t *testing.T) {
	rootErr := errors.New("root failed")
	for _, result := range []struct {
		name string
		err  error
	}{
		{"success", nil},
		{"failure", rootErr},
	} {
		t.Run(result.name, func(t *testing.T) {
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			var taskCtx context.Context
			err := RunTree(parent, func(ctx context.Context, spawn func(Task) error) error {
				calls++
				taskCtx = ctx
				return result.err
			})
			if err != result.err || calls != 1 {
				t.Errorf("result=%v, root calls=%d; want %v and 1", err, calls, result.err)
			}
			if taskCtx == nil || taskCtx.Err() != context.Canceled {
				t.Error("operation context was not released after return")
			}
			if parent.Err() != nil {
				t.Error("RunTree canceled its parent")
			}
		})
	}
}

func TestRunTreeAlreadyCanceledParentDoesNotCallRoot(t *testing.T) {
	for _, expired := range []bool{false, true} {
		var ctx context.Context
		var cancel context.CancelFunc
		want := context.Canceled
		if expired {
			ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Hour))
			want = context.DeadlineExceeded
		} else {
			ctx, cancel = context.WithCancel(context.Background())
			cancel()
		}
		calls := 0
		err := RunTree(ctx, func(context.Context, func(Task) error) error {
			calls++
			return nil
		})
		cancel()
		if err != want || calls != 0 {
			t.Errorf("error=%v, calls=%d; want %v and 0", err, calls, want)
		}
	}
}

func TestRunTreeDescendantsOutliveRootAndOwnTheirSpawn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rootSpawn := make(chan func(Task) error, 1)
		releaseChild := make(chan struct{})
		grandchildStarted := make(chan struct{})
		releaseGrandchild := make(chan struct{})
		var rejectedCalls atomic.Int32
		var operationCtx context.Context
		result := startTree(context.Background(), func(ctx context.Context, spawn func(Task) error) error {
			operationCtx = ctx
			rootSpawn <- spawn
			return spawn(func(childCtx context.Context, childSpawn func(Task) error) error {
				<-releaseChild
				if childCtx != ctx || childCtx.Err() != nil {
					t.Error("child lost the shared active context after root returned")
				}
				return childSpawn(func(grandchildCtx context.Context, _ func(Task) error) error {
					if grandchildCtx != ctx || grandchildCtx.Err() != nil {
						t.Error("grandchild did not receive the shared active context")
					}
					close(grandchildStarted)
					<-releaseGrandchild
					return nil
				})
			})
		})
		saved := <-rootSpawn
		synctest.Wait()
		if err := saved(func(context.Context, func(Task) error) error {
			rejectedCalls.Add(1)
			return nil
		}); !errors.Is(err, ErrClosed) {
			t.Errorf("finished owner's spawn = %v, want ErrClosed", err)
		}
		assertTreeWaiting(t, result)
		close(releaseChild)
		<-grandchildStarted
		synctest.Wait()
		assertTreeWaiting(t, result)
		close(releaseGrandchild)
		if err := <-result; err != nil {
			t.Errorf("RunTree = %v, want nil", err)
		}
		if operationCtx.Err() != context.Canceled {
			t.Error("shared context was not released")
		}
		if err := saved(func(context.Context, func(Task) error) error {
			rejectedCalls.Add(1)
			return nil
		}); !errors.Is(err, ErrClosed) {
			t.Errorf("spawn after RunTree = %v, want ErrClosed", err)
		}
		synctest.Wait()
		if got := rejectedCalls.Load(); got != 0 {
			t.Errorf("rejected tasks executed %d times", got)
		}
	})
}

func TestRunTreeFirstTaskErrorCancelsSiblingsAndWaits(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		parent, cancelParent := context.WithCancel(context.Background())
		defer cancelParent()
		boom, later := errors.New("first failure"), errors.New("later failure")
		rootSpawn := make(chan func(Task) error, 1)
		fail := make(chan struct{})
		cancellationSeen := make(chan struct{})
		attempt := make(chan struct{})
		spawnResult := make(chan error, 1)
		releaseSibling := make(chan struct{})
		var rejectedCalls atomic.Int32
		rejected := func(context.Context, func(Task) error) error {
			rejectedCalls.Add(1)
			return nil
		}
		result := startTree(parent, func(ctx context.Context, spawn func(Task) error) error {
			rootSpawn <- spawn
			if err := spawn(func(context.Context, func(Task) error) error {
				<-fail
				return boom
			}); err != nil {
				return err
			}
			return spawn(func(ctx context.Context, spawn func(Task) error) error {
				<-ctx.Done()
				close(cancellationSeen)
				<-attempt
				spawnResult <- spawn(rejected)
				<-releaseSibling
				return later
			})
		})
		saved := <-rootSpawn
		synctest.Wait()
		close(fail)
		<-cancellationSeen
		synctest.Wait()
		assertTreeWaiting(t, result)
		if parent.Err() != nil {
			t.Error("task failure canceled parent")
		}
		cancelParent() // This later cancellation must not replace boom.
		close(attempt)
		if err := <-spawnResult; err != boom {
			t.Errorf("live owner's spawn = %v, want first task error", err)
		}
		if err := saved(rejected); !errors.Is(err, ErrClosed) {
			t.Errorf("finished owner's spawn after cancellation = %v, want ErrClosed", err)
		}
		close(releaseSibling)
		if err := <-result; err != boom {
			t.Errorf("RunTree = %v, want original error %v", err, boom)
		}
		synctest.Wait()
		if rejectedCalls.Load() != 0 {
			t.Error("rejected task was executed")
		}
	})
}

func TestRunTreeExternalCancellationRejectsSpawnAndWaits(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		parent, cancel := context.WithCancel(context.Background())
		defer cancel()
		started := make(chan struct{})
		spawnResult := make(chan error, 1)
		release := make(chan struct{})
		var rejectedCalls atomic.Int32
		result := startTree(parent, func(ctx context.Context, spawn func(Task) error) error {
			close(started)
			<-ctx.Done()
			spawnResult <- spawn(func(context.Context, func(Task) error) error {
				rejectedCalls.Add(1)
				return nil
			})
			<-release
			return nil // Cancellation must be registered independently of this result.
		})
		<-started
		cancel()
		if err := <-spawnResult; err != context.Canceled {
			t.Errorf("spawn = %v, want Canceled", err)
		}
		synctest.Wait()
		assertTreeWaiting(t, result)
		close(release)
		if err := <-result; err != context.Canceled {
			t.Errorf("RunTree = %v, want Canceled", err)
		}
		synctest.Wait()
		if rejectedCalls.Load() != 0 {
			t.Error("rejected task was executed")
		}
	})
}

func TestRunTreeExternalCancellationWithoutSpawn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		parent, cancel := context.WithCancel(context.Background())
		defer cancel()
		started := make(chan struct{})
		result := startTree(parent, func(ctx context.Context, _ func(Task) error) error {
			close(started)
			<-ctx.Done()
			return nil
		})
		<-started
		cancel()
		if err := <-result; err != context.Canceled {
			t.Errorf("RunTree = %v, want Canceled despite nil task result", err)
		}
	})
}

func TestRunTreeParentDeadlineAndValues(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		type key struct{}
		base := context.WithValue(context.Background(), key{}, "trace")
		parent, cancel := context.WithTimeout(base, 20*time.Millisecond)
		defer cancel()
		deadline, _ := parent.Deadline()
		start := time.Now()
		err := RunTree(parent, func(ctx context.Context, _ func(Task) error) error {
			if ctx == parent || ctx.Value(key{}) != "trace" {
				t.Error("expected a child context preserving parent's values")
			}
			got, ok := ctx.Deadline()
			if !ok || !got.Equal(deadline) {
				t.Errorf("deadline = (%v, %v), want %v", got, ok, deadline)
			}
			<-ctx.Done()
			return nil
		})
		if err != context.DeadlineExceeded {
			t.Errorf("RunTree = %v, want DeadlineExceeded", err)
		}
		if elapsed := time.Since(start); elapsed != 20*time.Millisecond {
			t.Errorf("virtual elapsed time = %v, want 20ms", elapsed)
		}
	})
}

func TestRunTreeRegisteredExternalCancellationWinsOverLaterTaskError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		parent, cancel := context.WithCancel(context.Background())
		defer cancel()
		started := make(chan struct{})
		registered := make(chan error, 1)
		release := make(chan struct{})
		later := errors.New("task failed later")
		result := startTree(parent, func(ctx context.Context, spawn func(Task) error) error {
			close(started)
			<-ctx.Done()
			registered <- spawn(func(context.Context, func(Task) error) error {
				t.Error("task accepted after external cancellation")
				return nil
			})
			<-release
			return later
		})
		<-started
		cancel()
		if err := <-registered; err != context.Canceled {
			t.Errorf("spawn = %v, want Canceled", err)
		}
		close(release)
		if err := <-result; err != context.Canceled {
			t.Errorf("RunTree = %v, want first registered cancellation", err)
		}
	})
}

func TestRunTreeAcceptedTasksStillRunAfterCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		parent, cancel := context.WithCancel(context.Background())
		defer cancel()
		var calls atomic.Int32
		const count = 100
		err := RunTree(parent, func(ctx context.Context, spawn func(Task) error) error {
			for range count {
				if err := spawn(func(ctx context.Context, _ func(Task) error) error {
					calls.Add(1)
					<-ctx.Done()
					return nil
				}); err != nil {
					return err
				}
			}
			cancel()
			return nil
		})
		if err != context.Canceled || calls.Load() != count {
			t.Errorf("error=%v, child calls=%d; want Canceled and %d", err, calls.Load(), count)
		}
	})
}

func TestRunTreeConcurrentSpawnAndDynamicGrandchildren(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		const count = 100
		err := RunTree(context.Background(), func(ctx context.Context, spawn func(Task) error) error {
			calls.Add(1)
			var callers sync.WaitGroup
			spawnErrors := make(chan error, count)
			for range count {
				callers.Add(1)
				go func() {
					defer callers.Done()
					spawnErrors <- spawn(func(ctx context.Context, spawn func(Task) error) error {
						calls.Add(1)
						return spawn(func(context.Context, func(Task) error) error {
							calls.Add(1)
							return nil
						})
					})
				}()
			}
			callers.Wait()
			for range count {
				if err := <-spawnErrors; err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			t.Errorf("RunTree = %v, want nil", err)
		}
		if got := calls.Load(); got != 1+2*count {
			t.Errorf("executed tasks = %d, want %d", got, 1+2*count)
		}
	})
}

func TestRunTreeSpawnRacingWithOwnerCompletion(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		saved := make(chan func(Task) error, 1)
		releaseRoot := make(chan struct{})
		releaseChildren := make(chan struct{})
		result := startTree(context.Background(), func(ctx context.Context, spawn func(Task) error) error {
			saved <- spawn
			<-releaseRoot
			return nil
		})
		spawn := <-saved
		start := make(chan struct{})
		var accepted, executed atomic.Int32
		var callers sync.WaitGroup
		for range 100 {
			callers.Add(1)
			go func() {
				defer callers.Done()
				<-start
				err := spawn(func(context.Context, func(Task) error) error {
					executed.Add(1)
					<-releaseChildren
					return nil
				})
				if err == nil {
					accepted.Add(1)
				} else if !errors.Is(err, ErrClosed) {
					t.Errorf("spawn race: unexpected error %v", err)
				}
			}()
		}
		close(start)
		close(releaseRoot)
		callers.Wait()
		synctest.Wait()
		if accepted.Load() > 0 {
			assertTreeWaiting(t, result)
		}
		close(releaseChildren)
		if err := <-result; err != nil {
			t.Errorf("RunTree = %v, want nil", err)
		}
		synctest.Wait()
		if accepted.Load() != executed.Load() {
			t.Errorf("accepted=%d, executed=%d", accepted.Load(), executed.Load())
		}
	})
}
