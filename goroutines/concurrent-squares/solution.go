package main

import (
	"fmt"
	"sync"
)

func main() {
	nums := []int64{3, -2, 0}
	result := Squares(nums)
	fmt.Println(result)
}

func Squares(nums []int64) []int64 {
	var wg sync.WaitGroup

	results := make([]chan int64, len(nums))
	for i := 0; i < len(nums); i++ {
		results[i] = make(chan int64)
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i] <- nums[i]*nums[i]
		}()
	}

	res := make([]int64, len(nums))
	for i := 0; i < len(nums); i++ {
		res[i] = <-results[i]
	}

	wg.Wait()
	
	return res
}