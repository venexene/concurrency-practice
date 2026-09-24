package main

import (
	"context"
	"errors"
	"sync"
)

func main() {

}

type Queue struct {
	capacity int
	size int
	ch chan int
	done chan struct{}
	notFull chan struct{}
	notEmpty chan struct{}
	mu sync.Mutex
	closed bool
	closeOnce sync.Once
}

var ErrClosed error = errors.New("closed")

func NewQueue(capacity int) *Queue {
	return &Queue{
		capacity: capacity, 
		ch: make(chan int, capacity),
		done: make(chan struct{}),
		notFull: make(chan struct{}),
		notEmpty: make(chan struct{}),
	}
}

func (q *Queue) Put(ctx context.Context, x int) error {
	for {
		q.mu.Lock()
		if q.closed {
			q.mu.Unlock()
			return ErrClosed
		}
		if q.size < q.capacity {
			q.ch <- x
			q.size++
			close(q.notEmpty)
			q.notEmpty = make(chan struct{})
			q.mu.Unlock()
			return nil
		}

		wait := q.notFull
		q.mu.Unlock()

		select {
		case <-wait:
			continue
		case <-q.done:
			return ErrClosed
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (q *Queue) Get(ctx context.Context) (int, error) {
	for {
		q.mu.Lock()
		
		if q.size > 0 {
			val := <-q.ch
			q.size--
			close(q.notFull)
			q.notFull = make(chan struct{})
			q.mu.Unlock()
			return val, nil
		} else if q.closed {
			q.mu.Unlock()
			return 0, ErrClosed
		} else {
			wait := q.notEmpty
			q.mu.Unlock()

			select {
			case <-wait:
				continue
			case <-q.done:
				continue
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		}
	}
}

func (q *Queue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closeOnce.Do(func() {
		close(q.done)
		q.closed = true
	})
}