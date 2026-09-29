package main

import (
	"context"
	"sync"
)

func main() {

}

func MapAll(ctx context.Context, nums []int, f func(context.Context, int) (int, error)) ([]int,error) {
	cCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	var once sync.Once
	errCh := make(chan error, 1)
	res := make([]int, len(nums))
	var err error

	for i := 0; i < len(nums); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case <-cCtx.Done():
				once.Do(func() {
						errCh <- cCtx.Err()
						cancel()
					})
				cancel()
			default:
				val, err := f(cCtx, nums[i])
				if err != nil {
					once.Do(func() {
						errCh <- err
						cancel()
					})
				}
				res[i] = val
			}
		}()
	}
	
	wg.Wait()

	select {
	case err = <-errCh:
	default:
	}

	if err != nil {
		return nil, err
	}
	return res, nil
}