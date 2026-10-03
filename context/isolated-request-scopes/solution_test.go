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

func assertScopeActive(t *testing.T, ctx context.Context) {
	t.Helper()
	if ctx.Err() != nil || context.Cause(ctx) != nil {
		t.Errorf("active context: Err=%v, Cause=%v", ctx.Err(), context.Cause(ctx))
	}
	select {
	case <-ctx.Done():
		t.Error("active context has closed Done")
	default:
	}
}

func assertScopeCanceled(t *testing.T, ctx context.Context, err, cause error) {
	t.Helper()
	if ctx.Err() != err || context.Cause(ctx) != cause {
		t.Errorf("Err=%v, Cause=%v; want Err=%v, Cause=%v", ctx.Err(), context.Cause(ctx), err, cause)
	}
	select {
	case <-ctx.Done():
	default:
		t.Error("canceled context has open Done")
	}
}

func TestScopeChildIdentityAndIsolation(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := NewScope(parent)
	a, b := s.Child("A"), s.Child("B")
	assertScopeActive(t, a)
	assertScopeActive(t, b)
	if s.Child("A") != a || a == b {
		t.Fatal("repeated names must share a context, distinct names must not")
	}
	errA, errStop := errors.New("A failed"), errors.New("stop")
	s.CancelChild("A", errA)
	s.CancelChild("A", errors.New("later error"))
	assertScopeCanceled(t, a, context.Canceled, errA)
	assertScopeActive(t, b)
	assertScopeActive(t, parent)
	if s.Child("A") != a {
		t.Error("canceled branch was replaced")
	}
	s.CancelAll(errStop)
	assertScopeCanceled(t, a, context.Canceled, errA)
	assertScopeCanceled(t, b, context.Canceled, errStop)
	assertScopeActive(t, parent)
}

func TestScopeCancelChildNilCause(t *testing.T) {
	s := NewScope(context.Background())
	a := s.Child("A")
	s.CancelChild("A", nil)
	s.CancelChild("A", errors.New("later"))
	assertScopeCanceled(t, a, context.Canceled, context.Canceled)
}

func TestScopeFirstGlobalCancellationIncludesFutureChildren(t *testing.T) {
	stop, later := errors.New("stop"), errors.New("later")
	for _, first := range []struct {
		name  string
		cause error
	}{
		{"nil", nil},
		{"error", stop},
	} {
		t.Run(first.name, func(t *testing.T) {
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			s := NewScope(parent)
			a := s.Child("A")
			s.CancelAll(first.cause)
			s.CancelAll(later)
			s.CancelAll(nil)
			want := first.cause
			if want == nil {
				want = context.Canceled
			}
			assertScopeCanceled(t, a, context.Canceled, want)
			b := s.Child("B")
			assertScopeCanceled(t, b, context.Canceled, want)
			if s.Child("B") != b {
				t.Error("future canceled branch was replaced")
			}
			assertScopeActive(t, parent)
		})
	}
}

func TestScopeParentCancellation(t *testing.T) {
	parent, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	s := NewScope(parent)
	a, b := s.Child("A"), s.Child("B")
	errA, errParent := errors.New("A failed"), errors.New("parent stopped")
	s.CancelChild("A", errA)
	cancel(errParent)
	assertScopeCanceled(t, a, context.Canceled, errA)
	assertScopeCanceled(t, b, context.Canceled, errParent)
	assertScopeCanceled(t, s.Child("C"), context.Canceled, errParent)
}

func TestScopeGlobalCauseSurvivesLaterParentCancellation(t *testing.T) {
	parent, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	s := NewScope(parent)
	a := s.Child("A")
	stop := errors.New("scope stopped")
	s.CancelAll(stop)
	cancel(errors.New("parent stopped later"))
	assertScopeCanceled(t, a, context.Canceled, stop)
	assertScopeCanceled(t, s.Child("B"), context.Canceled, stop)
}

func TestScopeValuesAndDeadlines(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		type key struct{}
		deadline := time.Now().Add(20 * time.Millisecond)
		parent, cancel := context.WithDeadline(context.WithValue(context.Background(), key{}, "abc"), deadline)
		defer cancel()
		s := NewScope(parent)
		a := s.Child("A")
		checkInheritance := func(ctx context.Context) {
			t.Helper()
			got, ok := ctx.Deadline()
			if !ok || !got.Equal(deadline) {
				t.Errorf("Deadline=(%v, %v), want (%v, true)", got, ok, deadline)
			}
			if got := ctx.Value(key{}); got != "abc" {
				t.Errorf("Value=%v, want abc", got)
			}
		}
		checkInheritance(a)
		// A separate scope checks branches created after global cancellation.
		stopped := NewScope(parent)
		stop := errors.New("stop")
		stopped.CancelAll(stop)
		b := stopped.Child("B")
		checkInheritance(b)
		assertScopeCanceled(t, b, context.Canceled, stop)
		<-a.Done()
		assertScopeCanceled(t, a, context.DeadlineExceeded, context.DeadlineExceeded)
		c := s.Child("C")
		checkInheritance(c)
		assertScopeCanceled(t, c, context.DeadlineExceeded, context.DeadlineExceeded)
		d := stopped.Child("D")
		checkInheritance(d)
		assertScopeCanceled(t, d, context.Canceled, stop)
	})
}

func TestScopeConcurrentMethods(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := NewScope(parent)
	const count = 100
	shared := make([]context.Context, count)
	children := make([]context.Context, count)
	start := make(chan struct{})
	stop, local := errors.New("stop"), errors.New("local")
	var wg sync.WaitGroup
	for i := range count {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			shared[i] = s.Child("shared")
			name := strconv.Itoa(i)
			children[i] = s.Child(name)
			s.CancelChild(name, local)
			s.CancelAll(stop)
			if s.Child(name) != children[i] {
				t.Errorf("branch %s replaced", name)
			}
		}()
	}
	close(start)
	wg.Wait()
	for i := range count {
		if shared[i] != shared[0] {
			t.Errorf("shared context differs at index %d", i)
		}
		assertScopeCanceled(t, shared[i], context.Canceled, stop)
		cause := context.Cause(children[i])
		if cause != local && cause != stop {
			t.Errorf("branch %d: unexpected cause %v", i, cause)
		}
		assertScopeCanceled(t, children[i], context.Canceled, cause)
	}
	assertScopeCanceled(t, s.Child("future"), context.Canceled, stop)
	assertScopeActive(t, parent)
}
