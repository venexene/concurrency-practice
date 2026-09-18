package main

import (
	"fmt"
	"sync"
)

type PingPong struct {
	n int
	pingCh chan struct{}
	pongCh chan struct{}
}

func main() {
	var wg sync.WaitGroup

	pp := NewPingPong(5)

	wg.Add(1)
	go func() {
		defer wg.Done()
		pp.Ping(Print)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		pp.Pong(Print)
	}()

	wg.Wait()
}

func Print(str string) {
	fmt.Println(str)
}

func NewPingPong(n int) *PingPong {
	return &PingPong{
		n: n,
		pingCh: make(chan struct{}, 1),
		pongCh: make(chan struct{}, 1),
	}
}

func (p *PingPong) Ping(emit func(string)) {
	p.pongCh <- struct{}{}
	for i := 0; i < p.n; i++ {
		<-p.pongCh
		emit("ping")
		p.pingCh <- struct{}{}
	}
}

func (p *PingPong) Pong(emit func(string)) {
	for i := 0; i < p.n; i++  {
		<-p.pingCh
		emit("pong")
		p.pongCh <- struct{}{}
	}
}