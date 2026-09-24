package main

import "fmt"

func main() {
	n := 2
	in := make(chan int, 3)
	in <- 4
	in <- 5
	in <- 6
	result := Take(in, n)
	fmt.Println(result)
}

func Take(in <-chan int, n int) []int {
	res := make([]int, 0, n)
	for i := 0; i < n; i++ {
		val, ok := <-in
		if !ok {
			break
		}
		res = append(res, val)
	}
	return res
}