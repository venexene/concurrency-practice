package main

import (
	"context"
	"time"
)

func main() {

}

func CallWithBudget(parent context.Context, budget time.Duration, f func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(parent, budget)
	defer cancel()
	return f(ctx)
}