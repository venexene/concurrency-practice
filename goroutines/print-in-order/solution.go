package main

type Ordered struct {
	ch1 chan struct{}
	ch2 chan struct{}
}

func NewOrdered() *Ordered {
	return &Ordered{
		ch1: make(chan struct{}),
		ch2: make(chan struct{}),
	}
}

func (o *Ordered) First(emit func(string)) {
	emit("first")
	close(o.ch1)
}

func (o Ordered) Second(emit func(string)) {
	<-o.ch1
	emit("second")
	close(o.ch2)
}

func (o Ordered) Third(emit func(string)) {
	<-o.ch2
	emit("third")
}