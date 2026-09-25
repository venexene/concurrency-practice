package main

import (
	"sync"
	"sync/atomic"
)

func main() {

}

var idCount atomic.Uint64

type Account struct {
	id uint64
	money int64
	mu sync.RWMutex
}

func NewAccount(m int64) *Account {
	id := idCount.Add(1)

	return &Account{
		id: id,
		money: m,
	}
}

func (a *Account) Deposit(n int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.money += n
}

func (a *Account) Withdraw(n int64) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.money >= n {
		a.money -= n
	} else {
		return false
	}
	return true
}

func (a *Account) Balance() int64 {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.money
}

func Transfer(from, to *Account, amount int64) bool {
	if from == to {
		from.mu.Lock()
		defer from.mu.Unlock()
		if from.money >= amount {
			return true
		}
		return false
	}

	first, second := from, to
	if from.id > to.id {
		first, second = to, from
	}

	first.mu.Lock()
	second.mu.Lock()
	defer second.mu.Unlock()
	defer first.mu.Unlock()

	if from.money >= amount {
		from.money -= amount
		to.money += amount
		return true
	}
	return false
}