package main

import "sync"

func main() {

}

type IntSet struct {
	mp map[int]struct{}
	mu sync.RWMutex
}

func NewIntSet() *IntSet {
	return &IntSet{mp: map[int]struct{}{}}
}

func (s *IntSet) Add(x int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.mp[x]; ok {
		return false
	}
	s.mp[x] = struct{}{}
	return true
}

func (s *IntSet) Remove(x int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.mp[x]; ok {
		delete(s.mp, x)
		return true
	}
	return false
}

func (s *IntSet) Contains(x int) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.mp[x]
	return ok
}