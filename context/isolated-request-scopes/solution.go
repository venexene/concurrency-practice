package main

import (
	"context"
	"sync"
	"time"
)

func main() {

}

type CancelContext struct {
	ctx context.Context
	cancel context.CancelCauseFunc
}

type DetachedContext struct {
	context.Context
	parent context.Context
}

func (c DetachedContext) Deadline() (time.Time, bool) {
	return c.parent.Deadline()
}

type Scope struct {
	parent context.Context
	ctxMap map[string]CancelContext
	generalCause error
	canceled bool
	mu sync.Mutex
}

func NewScope(parent context.Context) *Scope {
	return &Scope{
		parent: parent,
		ctxMap: map[string]CancelContext{},
	}
}

func (s *Scope) Child(name string) context.Context {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.ctxMap[name]; !ok {
		parent := s.parent
		if s.canceled {
			parent = DetachedContext{
				Context: context.WithoutCancel(s.parent),
				parent: s.parent,
			} 
		}
		ctx, cancel := context.WithCancelCause(parent)
		s.ctxMap[name] = CancelContext{ctx: ctx, cancel: cancel}
	}
	if s.canceled {
		s.ctxMap[name].cancel(s.generalCause)
	}
	return s.ctxMap[name].ctx
}

func (s *Scope) CancelChild(name string, cause error) {
	s.mu.Lock()
	ctx, ok := s.ctxMap[name]
	s.mu.Unlock()

	if ok {
		ctx.cancel(cause)
	}
}

func (s *Scope) CancelAll(cause error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.canceled {
		return
	}
	s.generalCause = cause
	for _, v := range s.ctxMap {
		v.cancel(s.generalCause)
	}
	s.canceled = true
}