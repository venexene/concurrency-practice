package main

import "fmt"

func main() {
	ch := make(chan int, 1)
	fmt.Println(TryRecv(ch))
	ch <- 0
	close(ch)
	fmt.Println(TryRecv(ch))
	fmt.Println(TryRecv(ch))
}

func TryRecv(ch <-chan int) (value int, state string) {
	select {
	case val, ok := <-ch:
		if !ok {
			return 0, "closed"
		}
		return val, "value"
	default:
		return 0, "empty"
	}
}