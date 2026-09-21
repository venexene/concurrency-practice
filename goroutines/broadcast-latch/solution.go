package main

import "sync"

func main() {

}

type Latch struct {
	lockCh chan struct{}
	once sync.Once
}

func NewLatch() *Latch {
	return &Latch{lockCh: make(chan struct{})}
}

func (l *Latch) Wait() {
	<-l.lockCh
}

func (l *Latch) Release() {
	l.once.Do(func() {
		close(l.lockCh)
	})
}