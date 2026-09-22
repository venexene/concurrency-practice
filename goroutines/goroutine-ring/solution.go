package main

import (
	"fmt"
	"sync"
)

func main() {
	Ring(3, 5, Print)
}

func Print(num int) {
	fmt.Println(num)
}

func Ring(k, rounds int, emit func(int)) {
	if rounds <= 0 || k <= 0 {
		return
	}
	var wg sync.WaitGroup

	done := false
	cond := sync.NewCond(&sync.Mutex{})
	currentNum := 0
	currentRound := 0

	for i := 0; i < k; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				cond.L.Lock()
				for currentNum != i && !done {
					cond.Wait()
				}

				if done {
					cond.L.Unlock()
					return
				}

				emit(i)

				if i == k-1 {
					currentRound++
					if currentRound == rounds {
						done = true
						cond.Broadcast()
						cond.L.Unlock()
						return
					}
				}

				currentNum = (currentNum+1)%k
				cond.Broadcast()

				cond.L.Unlock()
			}
		}()
	}

	wg.Wait()
}