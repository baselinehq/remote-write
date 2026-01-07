package remotewrite

import (
	"sync"
	"time"
)

// RateLimiter implements a token bucket rate limiter for bytes per second.
//
// The limiter refills perSecondLimit tokens every second.
// Register() blocks when the bucket is empty.
type RateLimiter struct {
	perSecondLimit int64

	mu       sync.Mutex
	budget   int64
	deadline time.Time

	stopCh <-chan struct{}
}

// NewRateLimiter creates a new rate limiter with the given bytes per second limit.
// Pass stopCh to allow unblocking Register() when the limiter is no longer needed.
// If perSecondLimit <= 0, the limiter is disabled (Register is a no-op).
func NewRateLimiter(perSecondLimit int64, stopCh <-chan struct{}) *RateLimiter {
	return &RateLimiter{
		perSecondLimit: perSecondLimit,
		stopCh:         stopCh,
	}
}

// Register blocks until n bytes can be sent under the rate limit.
func (rl *RateLimiter) Register(n int) {
	if rl == nil || rl.perSecondLimit <= 0 {
		return
	}

	rl.mu.Lock()
	defer rl.mu.Unlock()

	// Block until we have enough budget to cover n bytes
	for rl.budget < int64(n) {
		// If deadline has passed, refill budget
		if time.Now().After(rl.deadline) {
			rl.budget += rl.perSecondLimit
			rl.deadline = time.Now().Add(time.Second)
		}

		// Cap budget at perSecondLimit to prevent excessive accumulation
		if rl.budget > rl.perSecondLimit {
			rl.budget = rl.perSecondLimit
		}

		// If still not enough, wait for next refill
		if rl.budget < int64(n) {
			// Calculate how much time is needed to get enough budget
			needed := int64(n) - rl.budget
			// If perSecondLimit is 0, this would be a division by zero.
			// However, the initial check `rl.perSecondLimit <= 0` handles this,
			// so we can assume perSecondLimit > 0 here.
			waitTime := time.Duration(float64(needed) / float64(rl.perSecondLimit) * float64(time.Second))

			// Ensure a minimum wait time to avoid busy-waiting in tight loops
			if waitTime < time.Millisecond {
				waitTime = time.Millisecond
			}

			rl.mu.Unlock()
			timer := getTimer(waitTime)
			select {
			case <-timer.C:
				// Timer fired, re-acquire lock and loop to re-evaluate budget
				putTimer(timer)
			case <-rl.stopCh:
				putTimer(timer)
				rl.mu.Lock()
				return // Don't consume budget if stopped
			}
			rl.mu.Lock()
		}
	}

	// Consume n bytes from budget
	rl.budget -= int64(n)
}

// Enabled returns true if the rate limiter is active.
func (rl *RateLimiter) Enabled() bool {
	return rl != nil && rl.perSecondLimit > 0
}
