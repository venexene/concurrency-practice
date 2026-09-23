package main

import "fmt"

func main() {
	in := make(chan int)

	go func() {
		for i := 0; i < 10; i++ {
			in <- i
		}
		close(in)
	}()

	ch := Map(in, func(x int) int {return x*10})
	for n := range ch {
		fmt.Println(n)
	}
}

func Map(in <-chan int, f func(int) int) <-chan int {
	ch := make(chan int)
	go func() {
		for n := range in {
			ch <- f(n)
		}
		close(ch)
	}()
	return ch
}
