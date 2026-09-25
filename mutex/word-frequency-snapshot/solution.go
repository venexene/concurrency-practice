package main

import (
	"maps"
	"sync"
)

func main() {

}

type WordCounter struct {
	words map[string]int
	mu sync.RWMutex
}

func NewWordCounter() *WordCounter {
	return &WordCounter{words: map[string]int{}}
}

func (w *WordCounter) Add(word string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.words[word]++
}

func (w *WordCounter) Snapshot() map[string]int {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return maps.Clone(w.words)
}