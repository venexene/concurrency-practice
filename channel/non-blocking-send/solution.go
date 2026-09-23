package main

import "fmt"

func main() {
	ch := make(chan int, 1)
	TrySend(ch, 8)
	TrySend(ch, 9)
	fmt.Println(<-ch)
}

func TrySend(ch chan<- int, x int) bool {
	select {
	case ch <- x:
		return true
	default:


		return false
	}
}