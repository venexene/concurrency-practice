package main

import "sync"

func main() {

}

type Account struct {
	money int64
	mu sync.RWMutex
}

func NewAccount(m int64) *Account {
	return &Account{money: m}
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
