package raft

import (
	"log"
	"sync"
)

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// Mutex wraps the sync.Mutex
type Mutex struct {
	mu   sync.Mutex
	held bool
}

// Lock the lock
func (m *Mutex) Lock() {
	m.mu.Lock()
	m.held = true
}

// Unlock the lock
func (m *Mutex) Unlock() {
	m.held = false
	m.mu.Unlock()
}

func (m *Mutex) AssertHeld() {
	if !m.held {
		panic("lock is not held")
	}
}
