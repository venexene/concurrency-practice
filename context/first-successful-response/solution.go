package main

import (
	"context"
	"sync"
)

func main() {

}

type AllFailed struct {
	errors []error
}

func (af AllFailed) Error() string {
	return "all calls failed"
}

func FirstSuccess(
	ctx context.Context, 
	calls []func(context.Context) (string, error),
) (string,error) {
	var wg sync.WaitGroup
	var once sync.Once
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	fails := AllFailed{errors: make([]error, len(calls))}
	resCh := make(chan string, 1)
	done := make(chan struct{})

	for i, call := range calls {
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case <-ctx.Done():
			default:
				val, err := call(ctx)
				if err == nil {
					once.Do(func() {
						select {
						case <-ctx.Done():
						default:
							close(done)
							cancel()
							resCh <- val	
						}
					})
				}
				fails.errors[i] = err
			}
		}()
	}

	wg.Wait()

	select {
	case <-done:
		return <-resCh, nil
	default:
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		default:
			return "", fails
		}
	}
}

type Result struct {
	val string
	idx int
	err error
}

func FirstSuccessCoord(
	ctx context.Context, 
	calls []func(context.Context) (string, error),
) (string,error) {
	var wg sync.WaitGroup
	parentCtx := ctx
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	fails := AllFailed{errors: make([]error, len(calls))}
	resCh := make(chan Result, len(calls))

	for i, call := range calls {
		wg.Add(1)
		go func() {
			defer wg.Done()

			val, err := call(ctx)

			resCh <- Result{
				idx: i,
				val: val,
				err: err,
			}
		}()
	}

	go func() {
		wg.Wait()
		close(resCh)
	}()


	var decided bool
	var finalErr error
	var winner string
	for {
		if decided {
			for range resCh {}
			return winner, finalErr
		}

		select {
		case <-parentCtx.Done():
			decided = true
			finalErr = parentCtx.Err()
			cancel()

		case res, ok := <-resCh:
			if !ok {
				if err := parentCtx.Err(); err != nil {
					return "", err
				}
				return "", fails
			}

			if res.err != nil {
				fails.errors[res.idx] = res.err
				continue
			}

			decided = true
			if err := parentCtx.Err(); err != nil {
				finalErr = err
			} else {
				winner = res.val
			}
			cancel()
		}
	}
}