package main

import (
	"fmt"
	"sync"
)

func Print(str int) {
	fmt.Println(str)
}

func main() {
	var wg sync.WaitGroup

	zeo := NewZeroEvenOdd(4)

	wg.Add(1)
	go func() {
		defer wg.Done()
		zeo.Zero(Print)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		zeo.Even(Print)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		zeo.Odd(Print)
	}()

	wg.Wait()
}

type ZeroEvenOdd struct {
	n int
	breakCh chan struct{}
	zeroCh chan int
	evenCh chan int
	oddCh chan int
}

func NewZeroEvenOdd(n int) *ZeroEvenOdd {
	return &ZeroEvenOdd{
		n: n,
		breakCh: make(chan struct{}, 1),
		zeroCh: make(chan int, 1),
		evenCh: make(chan int, 1),
		oddCh: make(chan int, 1),
	}
}

func (z *ZeroEvenOdd) Zero(emit func(int)) {
	z.zeroCh <- 0
	for {
		val := <-z.zeroCh
		if val >= z.n {
			close(z.breakCh)
			return
		}

		emit(0)

		val += 1
		if val % 2 == 0 {
			z.evenCh <- val
		} else {
			z.oddCh <- val
		}
	}
}

func (z *ZeroEvenOdd) Even(emit func(int)) {
	for {
		select {
		case val := <-z.evenCh:
			emit(val)
			z.zeroCh <- val
		case <-z.breakCh:
			return
		}
	}
}

func (z *ZeroEvenOdd) Odd(emit func(int)) {
	for {
		select {
		case val := <-z.oddCh:
			emit(val)
			z.zeroCh <- val
		case <-z.breakCh:
			return
		}
	}
}