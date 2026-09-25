package main

import "sync"

func main() {

}

type Counter struct {
	val int64
	mu sync.RWMutex
}

func (c *Counter) Add(delta int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.val+=delta
}

func (c *Counter) Value() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.val
}
