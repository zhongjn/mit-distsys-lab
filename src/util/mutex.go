package util

import (
	"runtime"
	"sync"
)

const callerInfo = false

// Mutex wraps the sync.Mutex
type Mutex struct {
	mu   sync.Mutex
	held bool

	callerLock  sync.Mutex
	callerValid bool
	callerFile  string
	callerLine  int
}

// Caller return the caller info
func (m *Mutex) Caller() (file string, line int, ok bool) {
	if !callerInfo {
		return "", -1, false
	}

	m.callerLock.Lock()
	defer m.callerLock.Unlock()
	return m.callerFile, m.callerLine, m.callerValid
}

// Lock the lock
func (m *Mutex) Lock() {
	m.mu.Lock()
	m.held = true

	if callerInfo {
		_, file, line, ok := runtime.Caller(1)
		if ok {
			m.callerLock.Lock()
			m.callerValid = true
			m.callerFile = file
			m.callerLine = line
			m.callerLock.Unlock()
		}
	}
}

// Unlock the lock
func (m *Mutex) Unlock() {
	if callerInfo {
		m.callerLock.Lock()
		m.held = false
		m.callerValid = false
		m.callerLock.Unlock()
	}

	m.mu.Unlock()
}

func (m *Mutex) AssertHeld() {
	if !m.held {
		panic("lock is not held")
	}
}
