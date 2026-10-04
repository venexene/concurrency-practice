package main

import (
	"context"
)

func main() {

}

type SharedCall struct {
    ctx context.Context
    cancel context.CancelFunc
    result Result
    done chan struct{}
}

type Result struct {
    str string
    err error
}

func NewSharedCall(
    serviceCtx context.Context,
    f func(context.Context) (string, error),
) *SharedCall {
    ctx, cancel := context.WithCancel(serviceCtx)

    s := &SharedCall{
        ctx: ctx,
        cancel: cancel,
        done: make(chan struct{}),
    }

    go func() {
        defer cancel()
        str, err := f(ctx)
        s.result = Result{str: str, err: err}
        close(s.done)
    }()
    
    return s
}

func (s *SharedCall) Await(waiterCtx context.Context) (string, error) {
    if err := waiterCtx.Err(); err != nil {
        return "", err
    }
        
    select {
    case <-s.done:
        return s.result.str, s.result.err
    case <-waiterCtx.Done():
        return "", waiterCtx.Err()
    }
}

func (s *SharedCall) Close() {
    s.cancel()
    <-s.done
}

