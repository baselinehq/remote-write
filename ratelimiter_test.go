package remotewrite

import (
	"sync"
	"testing"
	"time"
)

func TestRateLimiter_Disabled(t *testing.T) {
	var rl *RateLimiter
	rl.Register(1000) // Should not panic

	// Zero limit should be disabled
	stopCh := make(chan struct{})
	rl = NewRateLimiter(0, stopCh)
	if rl.Enabled() {
		t.Error("limiter with 0 limit should be disabled")
	}
	rl.Register(1000) // Should not block
	close(stopCh)
}

func TestRateLimiter_Basic(t *testing.T) {
	stopCh := make(chan struct{})
	defer close(stopCh)

	// 1000 bytes per second
	rl := NewRateLimiter(1000, stopCh)
	if !rl.Enabled() {
		t.Error("limiter should be enabled")
	}

	// First 1000 bytes should not block
	start := time.Now()
	rl.Register(1000)
	if time.Since(start) > 50*time.Millisecond {
		t.Error("first batch should not block")
	}

	// Next batch should block until refill
	rl.Register(100)
	elapsed := time.Since(start)
	if elapsed < 900*time.Millisecond {
		t.Errorf("second batch should have blocked for ~1s, only blocked for %v", elapsed)
	}
}

func TestRateLimiter_StopChannel(t *testing.T) {
	stopCh := make(chan struct{})

	// Very low rate to force blocking
	rl := NewRateLimiter(10, stopCh)

	// Consume the budget
	rl.Register(10)

	// Start a goroutine that will be blocked
	done := make(chan struct{})
	go func() {
		rl.Register(100) // This should block
		close(done)
	}()

	// Wait a bit, then close stopCh to unblock
	time.Sleep(50 * time.Millisecond)
	close(stopCh)

	// Should unblock quickly
	select {
	case <-done:
		// Success
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Register did not unblock after stopCh closed")
	}
}

func TestRateLimiter_Concurrent(t *testing.T) {
	stopCh := make(chan struct{})
	defer close(stopCh)

	// High enough rate that concurrent registrations work
	rl := NewRateLimiter(1000000, stopCh)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				rl.Register(100)
			}
		}()
	}
	wg.Wait()
	// If we get here without deadlock, the test passes
}
