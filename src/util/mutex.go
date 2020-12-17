package util

import "sync"

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
