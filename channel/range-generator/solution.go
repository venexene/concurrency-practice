package main

import "fmt"

func main() {
	ch := Range(0, 10)

	for n := range ch {
		fmt.Println(n)
	}
}

func Range(lo, hi int) <-chan int {
	ch := make(chan int)

	go func() {
		for i := lo; i < hi; i++ {
			ch <- i
		}
		close(ch)
	}()

	return ch
}