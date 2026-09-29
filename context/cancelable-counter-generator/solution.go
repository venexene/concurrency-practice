package main

import "context"

func main() {

}

func Count(ctx context.Context) <-chan int {
	ch := make(chan int)

	go func() {
		select {
		case <-ctx.Done():
			close(ch)
			return 
		default:
		}

		i := 0
		for {
			select {
			case ch <- i:
				select {
				case <-ctx.Done():
					close(ch)
					return 
				default:
				}
			case <-ctx.Done():
				close(ch)
				return 
			}
			i++
		}
	}()

	return ch
}