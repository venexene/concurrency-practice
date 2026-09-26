package main

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLazyLoadsOnlyOnFirstGet(t *testing.T) {
	var calls atomic.Int32
	l := NewLazy(func() (Config, error) {
		calls.Add(1)
		return Config{Port: 8080}, nil
	})
	if got := calls.Load(); got != 0 {
		t.Fatalf("load called during construction: %d times", got)
	}

	for range 3 {
		cfg, err := l.Get()
		if cfg.Port != 8080 || err != nil {
			t.Fatalf("Get() = (%+v, %v), want (Port: 8080, nil)", cfg, err)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("load called %d times, want 1", got)
	}
}

func TestLazyConcurrentGetsWaitForOneLoad(t *testing.T) {
	const callers = 32
	var calls atomic.Int32
	loading := make(chan struct{})
	release := make(chan struct{})
	l := NewLazy(func() (Config, error) {
		calls.Add(1)
		close(loading)
		<-release
		return Config{Port: 9090}, nil
	})

	start := make(chan struct{})
	results := make(chan lazyResult, callers)
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			cfg, err := l.Get()
			results <- lazyResult{cfg, err}
		}()
	}
	close(start)

	select {
	case <-loading:
	case <-time.After(time.Second):
		t.Fatal("load did not start")
	}
	select {
	case result := <-results:
		t.Fatalf("Get returned before load completed: %+v", result)
	default:
	}
	close(release)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Get calls did not finish after load completed")
	}
	close(results)

	for result := range results {
		if result.cfg.Port != 9090 || result.err != nil {
			t.Errorf("Get() = (%+v, %v), want (Port: 9090, nil)", result.cfg, result.err)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("load called %d times, want 1", got)
	}
}

func TestLazyCachesCompleteErrorResult(t *testing.T) {
	wantErr := errors.New("load failed")
	var calls atomic.Int32
	l := NewLazy(func() (Config, error) {
		calls.Add(1)
		return Config{Port: 1234}, wantErr
	})

	for i := range 3 {
		cfg, err := l.Get()
		if cfg.Port != 1234 || err != wantErr {
			t.Errorf("Get call %d = (%+v, %v), want (Port: 1234, %v)", i+1, cfg, err, wantErr)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("load called %d times after error, want 1", got)
	}
}

func TestLazyReturnedConfigIsIndependentValue(t *testing.T) {
	l := NewLazy(func() (Config, error) {
		return Config{Port: 8080}, nil
	})
	cfg, err := l.Get()
	if err != nil {
		t.Fatal(err)
	}
	cfg.Port = 1
	if got, err := l.Get(); got.Port != 8080 || err != nil {
		t.Fatalf("Get() after changing returned value = (%+v, %v), want (Port: 8080, nil)", got, err)
	}
}

type lazyResult struct {
	cfg Config
	err error
}
