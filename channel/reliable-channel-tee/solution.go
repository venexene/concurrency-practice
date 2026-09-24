package main

import "context"

func main() {

}

func Tee(ctx context.Context, in <-chan int) (left, right <-chan int) {
	lCh := make(chan int)
	rCh := make(chan int)
	finished := false
	go func() {
		for !finished {
			select {
			case val, ok := <-in:
				if !ok {
					finished = true
					continue
				}

				select {
				case lCh <- val:
				case <-ctx.Done():
					finished = true
					continue
				}

				select {
				case rCh <- val:
				case <-ctx.Done():
					finished = true
					continue
				}
			case <-ctx.Done():
				finished = true
			}
		}
		close(lCh)
		close(rCh)
	}()
	
	return lCh, rCh
}
