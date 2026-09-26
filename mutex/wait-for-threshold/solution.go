package main

import "sync"

func main() {
	
}

type Progress struct {
	val int
	cond sync.Cond
}

func NewProgress() *Progress {
	return &Progress{cond: *sync.NewCond(&sync.Mutex{})}
}

func (p *Progress) Add(n int) {
	p.cond.L.Lock()
	defer p.cond.L.Unlock()
	p.val += n
	p.cond.Broadcast()
}

func (p *Progress) WaitAtLeast(target int) {
	p.cond.L.Lock()
	defer p.cond.L.Unlock()

	for p.val < target {
		p.cond.Wait()
	}
}