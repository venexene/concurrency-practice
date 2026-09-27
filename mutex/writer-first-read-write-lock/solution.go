package main

import (
	"sync"
)

func main() {
	
}

type WriterFirstRWLock struct {
	cond sync.Cond

	readers int
	isWriterActive bool
	waitingWriters int

	nextWriterTicket int
	servingWriterTicket int
}

func NewWriterFirstRWLock() *WriterFirstRWLock {
	return &WriterFirstRWLock{
		cond: *sync.NewCond(&sync.Mutex{}),
	}
}

func (l *WriterFirstRWLock) RLock() {
	l.cond.L.Lock()
	defer l.cond.L.Unlock()

	for l.isWriterActive || l.waitingWriters > 0 {
		l.cond.Wait()
	}

	l.readers++
}

func (l *WriterFirstRWLock) RUnlock() {
	l.cond.L.Lock()
	defer l.cond.L.Unlock()

	l.readers--

	if l.readers <= 0 {
		l.cond.Broadcast()
	}
}

func (l *WriterFirstRWLock) Lock() {
	l.cond.L.Lock()
	defer l.cond.L.Unlock()

	myTicket := l.nextWriterTicket
	l.nextWriterTicket++
	l.waitingWriters++

	for l.readers > 0 || myTicket != l.servingWriterTicket {
		l.cond.Wait()
	}

	l.isWriterActive = true
	l.waitingWriters--
}

func (l *WriterFirstRWLock) Unlock() {
	l.cond.L.Lock()
	defer l.cond.L.Unlock()

	l.servingWriterTicket++
	l.isWriterActive = false

	l.cond.Broadcast()
}

