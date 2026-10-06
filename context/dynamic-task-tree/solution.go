package main

import (
	"context"
	"errors"
	"sync"
) 

func main() {

}

var ErrClosed = errors.New("Tasks tree closed")

type Task func(
    ctx context.Context,
    spawn func(child Task) error,
) error

func RunTree(ctx context.Context, root Task) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	cond := sync.NewCond(&sync.Mutex{})
	parentCtx := ctx

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	taskCounter := 1
	var firstErr error

	callbackDone := make(chan struct{})
	stop := context.AfterFunc(parentCtx, func() {
		defer close(callbackDone)
		cond.L.Lock()
		defer cond.L.Unlock()
		if firstErr == nil {
			firstErr = parentCtx.Err()
			cancel()
		}
	})
	defer func() {
		if !stop() {
			<-callbackDone
		}
	}()

	var run func(ctx context.Context, task Task) error
	run = func(ctx context.Context, task Task) error {
		finished := false
		res := task(ctx, func(child Task) error {
			cond.L.Lock()
			if finished {
				cond.L.Unlock()
				return ErrClosed
			}
			if firstErr != nil {
				cond.L.Unlock()
				return firstErr
			}
			if err := parentCtx.Err(); err != nil {
				firstErr = err
				cond.L.Unlock()
				return firstErr
			}
			taskCounter++
			cond.L.Unlock()

			go func() {
				run(ctx, child)
			}()

			return nil
		})

		cond.L.Lock()
		finished = true
		if res != nil && firstErr == nil {
			firstErr = res
			cancel()
		}
		taskCounter--
		cond.Broadcast()
		cond.L.Unlock()

		return res
	}
	
	go run(ctx, root)
	
	cond.L.Lock()
	defer cond.L.Unlock()

	for taskCounter != 0 {
		cond.Wait()
	}

	if firstErr == nil {
		firstErr = parentCtx.Err()
	}
	return firstErr
}