package main

import (
	"fmt"
	"sync"
)

func main() {
	nums := []int64{1, 2, 3, 4, 5}
	k := 3
	result := ChunkSum(nums, k)
	fmt.Println(result)
}

func ChunkSum(nums []int64, k int) int64 {
	var wg sync.WaitGroup
	ch := make(chan int64)

	q := len(nums)/k
	r := len(nums)%k

	for i := 0; i < r; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			start := i * (q+1)
			end := start + (q+1)
			var sum int64
			for b := start; b < end && b < len(nums); b++ {
				sum += nums[b] 
			}
			ch <- sum
		}()
	}

	for j := 0; j < k - r; j++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			start := r * (q+1) + j * q
			end := start + q
			var sum int64
			for b := start; b < end && b < len(nums); b++ {
				sum += nums[b] 
			}
			ch <- sum
		}()
	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	var res int64
	for n := range ch {
		res += n
	}

	return res
}