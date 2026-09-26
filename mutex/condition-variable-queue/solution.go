package main

import (
	"errors"
	"sync"
)

func main() {

}

type Queue struct {
	capacity int
	size int
	head int
	ring []int
	putCond sync.Cond
	getCond sync.Cond
	closed bool
	closeOnce sync.Once
}

var ErrClosed error = errors.New("closed")

func NewQueue(capacity int) *Queue {
	mu := sync.Mutex{}

	return &Queue{
		capacity: capacity, 
		ring: make([]int, capacity),
		closed: false,
		putCond: *sync.NewCond(&mu),
		getCond: *sync.NewCond(&mu),
	}
}

func (q *Queue) Put(x int) error {
	for {
		q.putCond.L.Lock()
		defer q.putCond.L.Unlock()

		for q.size >= q.capacity && !q.closed {
			q.putCond.Wait()
		}

		if q.closed {
			return ErrClosed
		}
		
		if q.size < q.capacity {
			pos := (q.head + q.size)%q.capacity
			q.ring[pos] = x
			q.size++
			q.getCond.Broadcast()
			return nil
		}
	}
}

func (q *Queue) Get() (int, error) {
	for {
		q.getCond.L.Lock()
		defer q.getCond.L.Unlock()
		
		for q.size <= 0 && !q.closed {
			q.getCond.Wait()
		}

		if q.size > 0 {
			val := q.ring[q.head]
			q.ring[q.head] = 0 
			q.size--
			q.head = (q.head+1)%q.capacity
			q.putCond.Broadcast()
			return val, nil
		} else if q.closed {
			return 0, ErrClosed
		}
	}
}

func (q *Queue) Close() {
	q.putCond.L.Lock()
	defer q.putCond.L.Unlock()
	q.closeOnce.Do(func() {
		q.closed = true
		q.putCond.Broadcast()
		q.getCond.Broadcast()
	})
}