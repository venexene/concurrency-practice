package main

import (
	"fmt"
	"sync"
)

func main() {
	var wg sync.WaitGroup

	water := NewWater()

	wg.Add(1)
	go func() {
		defer wg.Done()
		water.Oxygen(OEmit)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		water.Oxygen(OEmit)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		water.Hydrogen(HEmit)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		water.Hydrogen(HEmit)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		water.Hydrogen(HEmit)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		water.Hydrogen(HEmit)
	}()

	wg.Wait()
}

func OEmit() {
	fmt.Println("O")
}

func HEmit() {
	fmt.Println("H")
}

type Water struct {
	hReserved int
	oReserved int
	finished int

	cond sync.Cond
}

func NewWater() *Water {
	return &Water{cond: *sync.NewCond(&sync.Mutex{})}
}

func (w *Water) Hydrogen(emit func()) {
	w.cond.L.Lock()
	for w.hReserved == 2 {
		w.cond.Wait()
	}
	w.hReserved++
	w.cond.L.Unlock()
	
	emit()

	w.cond.L.Lock()
	w.finished++
	if w.finished == 3 {
		w.finished = 0
		w.hReserved = 0
		w.oReserved = 0
	}
	w.cond.Broadcast()
	w.cond.L.Unlock()
}

func (w *Water) Oxygen(emit func()) {
	w.cond.L.Lock()
	for w.oReserved == 1 {
		w.cond.Wait()
	}
	w.oReserved++
	w.cond.L.Unlock()

	emit()

	w.cond.L.Lock()
	w.finished++
	if w.finished == 3 {
		w.finished = 0
		w.hReserved = 0
		w.oReserved = 0
	}
	w.cond.Broadcast()
	w.cond.L.Unlock()
}