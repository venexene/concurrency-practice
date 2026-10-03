package main

import (
	"context"
	"time"
)

func main() {

}

func Cleanup(
	requestCtx context.Context, 
	budget time.Duration, 
	cleanup func(context.Context) error,
	) error {
	detached := context.WithoutCancel(requestCtx)
	ctx, cancel := context.WithTimeout(detached, budget)
	defer cancel()
	err := cleanup(ctx)
	return err
}
