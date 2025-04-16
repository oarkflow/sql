package integrations

import (
	"sync"
	"time"
)

type cbState int

const (
	Closed cbState = iota
	Open
	HalfOpen
)

type CircuitBreaker struct {
	failureCount int
	threshold    int
	state        cbState
	lastFailure  time.Time
	openDuration time.Duration
	lock         sync.Mutex
}

func NewCircuitBreaker(threshold int) *CircuitBreaker {
	return &CircuitBreaker{
		threshold:    threshold,
		state:        Closed,
		openDuration: 30 * time.Second,
	}
}

func (cb *CircuitBreaker) AllowRequest() bool {
	cb.lock.Lock()
	defer cb.lock.Unlock()
	now := time.Now()
	switch cb.state {
	case Closed:
		return true
	case Open:
		if now.Sub(cb.lastFailure) > cb.openDuration {
			cb.state = HalfOpen
			return true
		}
		return false
	case HalfOpen:
		return true
	default:
		return false
	}
}

func (cb *CircuitBreaker) RecordSuccess() {
	cb.lock.Lock()
	defer cb.lock.Unlock()
	cb.failureCount = 0
	cb.state = Closed
}

func (cb *CircuitBreaker) RecordFailure() {
	cb.lock.Lock()
	defer cb.lock.Unlock()
	cb.failureCount++
	if cb.failureCount >= cb.threshold {
		cb.state = Open
		cb.lastFailure = time.Now()
	}
}
