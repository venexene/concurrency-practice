package main

import (
	"context"
	"errors"
)

var ErrClosed = errors.New("channel closed")

func main() {

}

func Recv(ctx context.Context, in <-chan int) (int,error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	select {
	case val, ok := <-in:
		if !ok {
			return 0, ErrClosed
		}
		return val, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}