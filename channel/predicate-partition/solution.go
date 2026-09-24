package main

import (
	"context"
	"fmt"
	"sync"
)

func main() {
	var wg sync.WaitGroup

	in := make(chan int, 4)
	in <- 1
	in <- 2
	in <- 3
	in <- 4
	close(in)
	yes, no := Partition(context.TODO(), in, func(x int) bool { return x % 2 == 0 })

	wg.Add(1)
	go func() {
		for n := range yes {
			fmt.Printf("Yes - %d\n", n)
		}
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		for n := range no {
			fmt.Printf("No - %d\n", n)
		}
		wg.Done()
	}()

	wg.Wait()
}

func Partition(ctx context.Context, in <-chan int, pred func(int) bool) (yes, no <-chan int) {
	yesCh := make(chan int)
	noCh := make(chan int)
	var ch chan int
	finished := false

	go func() {
		for !finished {
			select {
			case val, ok := <-in:
				if !ok {
					finished = true
					continue
				}

				if pred(val) {
					ch = yesCh
				} else {
					ch = noCh
				}

				select {
				case ch <- val:
				case <- ctx.Done():
					finished = true
				}

			case <-ctx.Done():
				finished = true
			}
		}
		close(yesCh)
		close(noCh)
	}()

	return yesCh, noCh
}
