package main

import (
	"slices"
	"sync"
)

func main() {

}

type WalletID string

type Wallet struct {
	balance int64
	mu sync.RWMutex
}

type Ledger struct {
	wallets map[WalletID]*Wallet
}

func NewLedger(initial map[WalletID]int64) *Ledger {
	wallets := map[WalletID]*Wallet{}

	for id, balance := range initial {
		wallets[id] = &Wallet{balance: balance}
	}
	return &Ledger{
		wallets: wallets,
	}
}

func (l *Ledger) Apply(deltas map[WalletID]int64) bool {
	ids := make([]WalletID, 0, len(deltas))
	for id := range deltas {
		ids = append(ids, id)
	}
	slices.Sort(ids)

	for _, id := range ids {
		l.wallets[id].mu.Lock()
		defer l.wallets[id].mu.Unlock()
		if l.wallets[id].balance + deltas[id] < 0 {
			return false
		}
	}

	for _, id := range ids {
		l.wallets[id].balance += deltas[id]
	}

	return true
}

func (l *Ledger) Snapshot(ids []WalletID) map[WalletID]int64 {
	snapshot := make(map[WalletID]int64, len(ids))
	
	sIds := slices.Clone(ids)
	slices.Sort(sIds)
	sIds = slices.Compact(sIds)

	for _, id := range sIds {
		l.wallets[id].mu.RLock()
		defer l.wallets[id].mu.RUnlock()
		snapshot[id] = l.wallets[id].balance
	}

	return snapshot
}