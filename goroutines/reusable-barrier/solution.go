package main

import (
	"sync"
	"fmt"
)

func main() {
	var wg sync.WaitGroup

	n := 2
	barrier := NewBarrier(n)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			fmt.Printf("Go1Work%d\n", i)
			gen := barrier.ArriveAndWait()
			fmt.Println(gen)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			fmt.Printf("Go2Work%d\n", i)
			gen := barrier.ArriveAndWait()
			fmt.Println(gen)
		}
	}()

	wg.Wait()
}

type Barrier struct {
	n int

	arrived int
	gen int
	mu sync.Mutex
	ch chan struct{}
}

func NewBarrier(n int) *Barrier {
	return &Barrier{
		n: n,
		gen: -1,
		ch: make(chan struct{}),
	}
}

func (b *Barrier) ArriveAndWait() int {
	b.mu.Lock()
	ch := b.ch
	b.arrived++
	if b.arrived == b.n {
		b.arrived = 0
		b.gen++
		close(b.ch)
		b.ch = make(chan struct{})
	}
	b.mu.Unlock()
	<-ch
	return b.gen
}