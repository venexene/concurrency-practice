package main

import "fmt"

func main() {
	a := make(chan int, 2)
	a <- 1
	a <- 3
	close(a)
	b := make(chan int, 2)
	b <- 2
	b <- 4
	close(b)
	ch := Merge2(a, b)
	for n := range ch {
		fmt.Println(n)
	}
}

func Merge2(a, b <-chan int) <-chan int {
	ch := make(chan int)
	go func() {
		for a != nil || b != nil {
			select {
			case val, ok := <-a:
				if !ok {
					a = nil
					continue
				}
				ch <- val
			case val, ok := <-b:
				if !ok {
					b = nil
					continue
				}
				ch <- val
			}
		}
		close(ch)
	}()
	
	return ch
}
