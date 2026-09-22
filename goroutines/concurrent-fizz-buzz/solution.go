package main

import (
	"fmt"
	"sync"
)

func main() {
	n := 15
	fizzBuzz := NewFizzBuzz(n)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		fizzBuzz.Fizz(PrintFizz)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		fizzBuzz.Buzz(PrintBuzz)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		fizzBuzz.FizzBuzz(PrintFizzBuzz)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		fizzBuzz.Number(PrintNum)
	}()

	wg.Wait()
}

func PrintFizz() {
	fmt.Println("fizz")
}

func PrintBuzz() {
	fmt.Println("buzz")
}

func PrintFizzBuzz() {
	fmt.Println("fizzbuzz")
}

func PrintNum(num int) {
	fmt.Println(num)
}

type FizzBuzz struct {
	n int

	cur int
	cond sync.Cond
}

func NewFizzBuzz(n int) *FizzBuzz {
	return &FizzBuzz{
		n:n,
		cur: 1,
		cond: *sync.NewCond(&sync.Mutex{}),
	}
}

func (f *FizzBuzz) Fizz(emit func()) {
	for {
		f.cond.L.Lock()

		for (f.cur % 3 != 0 || f.cur % 5 == 0) && f.cur <= f.n {
			f.cond.Wait()
		}

		if f.cur > f.n {
			f.cond.L.Unlock()
			return
		}

		emit()
		f.cur++
		f.cond.Broadcast()

		f.cond.L.Unlock()
	}
}

func (f *FizzBuzz) Buzz(emit func()) {
	for {
		f.cond.L.Lock()

		for (f.cur % 5 != 0 || f.cur % 3 == 0) && f.cur <= f.n {
			f.cond.Wait()
		}

		if f.cur > f.n {
			f.cond.L.Unlock()
			return
		}

		emit()
		f.cur++
		f.cond.Broadcast()

		f.cond.L.Unlock()
	}
}

func (f *FizzBuzz) FizzBuzz(emit func()) {
	for {
		f.cond.L.Lock()

		for (f.cur % 5 != 0 || f.cur % 3 != 0) && f.cur <= f.n {
			f.cond.Wait()
		}

		if f.cur > f.n {
			f.cond.L.Unlock()
			return
		}

		emit()
		f.cur++
		f.cond.Broadcast()

		f.cond.L.Unlock()
	}
}

func (f *FizzBuzz) Number(emit func(int)) {
	for {
		f.cond.L.Lock()

		for (f.cur % 5 == 0 || f.cur % 3 == 0) && f.cur <= f.n {
			f.cond.Wait()
		}

		if f.cur > f.n {
			f.cond.L.Unlock()
			return
		}

		emit(f.cur)
		f.cur++
		f.cond.Broadcast()

		f.cond.L.Unlock()
	}
}