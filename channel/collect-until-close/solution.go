package main

import "fmt"

func main() {
	ch := make(chan int)

	go func() {
		for i := 0; i < 10; i++ {
			ch <- i
		}
		close(ch)
	}()

	result := Collect(ch)
	fmt.Println(result)
}

func Collect(in <-chan int) []int {
	res := []int{}
	for n := range in {
		res = append(res, n)
	}
	return res
}