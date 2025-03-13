package transactions

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

// CircuitBreaker implements a simple circuit breaker.
type CircuitBreaker struct {
	threshold    int
	failureCount int
	open         bool
	openUntil    time.Time
	resetTimeout time.Duration
	mu           sync.Mutex
}

// NewCircuitBreaker creates a new CircuitBreaker.
func NewCircuitBreaker(threshold int, resetTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		threshold:    threshold,
		resetTimeout: resetTimeout,
	}
}

// Allow returns true if execution is permitted.
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if cb.open {
		if time.Now().After(cb.openUntil) {
			cb.open = false
			cb.failureCount = 0
			return true
		}
		return false
	}
	return true
}

// RecordFailure increases the failure count and opens the breaker if the threshold is met.
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failureCount++
	if cb.failureCount >= cb.threshold {
		cb.open = true
		cb.openUntil = time.Now().Add(cb.resetTimeout)
		log.Printf("Circuit breaker opened for %v after %d failures", cb.resetTimeout, cb.failureCount)
	}
}

// RecordSuccess resets the breaker.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failureCount = 0
	cb.open = false
}

// RetryWithCircuit retries a function using the provided circuit breaker.
func RetryWithCircuit(retryCount int, retryDelay time.Duration, cb *CircuitBreaker, fn func() error) error {
	var err error
	for i := 0; i < retryCount; i++ {
		if !cb.Allow() {
			return fmt.Errorf("circuit breaker is open")
		}
		err = fn()
		if err == nil {
			cb.RecordSuccess()
			return nil
		}
		cb.RecordFailure()
		log.Printf("Retry attempt %d failed: %v", i+1, err)
		jitter := 0.8 + rand.Float64()*0.4
		time.Sleep(time.Duration(float64(retryDelay) * jitter))
		retryDelay *= 2
	}
	return err
}

// SimpleRetry retries a function without circuit breaker logic.
func SimpleRetry(retryCount int, retryDelay time.Duration, fn func() error) error {
	var err error
	for i := 0; i < retryCount; i++ {
		err = fn()
		if err == nil {
			return nil
		}
		log.Printf("Retry attempt %d failed: %v", i+1, err)
		jitter := 0.8 + rand.Float64()*0.4
		time.Sleep(time.Duration(float64(retryDelay) * jitter))
		retryDelay *= 2
	}
	return err
}
