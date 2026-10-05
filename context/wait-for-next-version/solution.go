package main

import (
	"context"
	"errors"
	"sync"
)

func main() {

}

var ErrClosed = errors.New("Versioned is closed")

type Versioned struct {
	version uint64
	value string

	cond sync.Cond
	closed bool
}

func NewVersioned(initial string) *Versioned {
	return &Versioned{
		version: 0,
		value: initial,
		cond: *sync.NewCond(&sync.Mutex{}),
	}
}

func (v *Versioned) Set(value string) error {
	v.cond.L.Lock()
	defer v.cond.L.Unlock()

	if v.closed {
		return ErrClosed
	}

	v.version++
	v.value = value
	v.cond.Broadcast()
	return nil
}

func (v *Versioned) WaitNext(
	ctx context.Context,
	afterVersion uint64,
) (value string, version uint64, err error) {
	stop := context.AfterFunc(ctx, func() {
		v.cond.L.Lock()
		defer v.cond.L.Unlock()
		v.cond.Broadcast()
	})
	defer stop()

	v.cond.L.Lock()
	defer v.cond.L.Unlock()

	for v.version <= afterVersion && !v.closed && ctx.Err() == nil {
		v.cond.Wait()
	}

	if v.closed {
		return "", 0, ErrClosed
	} else if ctx.Err() != nil {
		return "", 0, ctx.Err()
	}
	return v.value, v.version, nil 
}

func (v *Versioned) Close() {
	v.cond.L.Lock()
	defer v.cond.L.Unlock()

	if v.closed {
		return
	}

	v.closed = true
	v.cond.Broadcast()
}