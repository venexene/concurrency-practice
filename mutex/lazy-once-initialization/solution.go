package main

import (
	"sync"
)

func main() {

}

type Lazy struct {
	load func() (Config, error)
	cfg Config
	err error
	once sync.Once
}

type Config struct {
    Port int 
}

func NewLazy(load func() (Config, error)) *Lazy {
	return &Lazy{
		load: load,
	}
}

func (l *Lazy) Get() (Config, error) {
	l.once.Do(func() {
		l.cfg, l.err = l.load()
	})

	return l.cfg, l.err
}