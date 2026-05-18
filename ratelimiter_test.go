package remotewrite

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestRateLimiter_Disabled(t *testing.T) {
	var rl *RateLimiter
	if err := rl.Register(context.Background(), 1000); err != nil {
		t.Fatalf("disabled nil limiter returned error: %v", err)
	}

	// Zero limit should be disabled
	stopCh := make(chan struct{})
	rl = NewRateLimiter(0, stopCh)
	if rl.Enabled() {
		t.Error("limiter with 0 limit should be disabled")
	}
	if err := rl.Register(context.Background(), 1000); err != nil {
		t.Fatalf("disabled zero-limit limiter returned error: %v", err)
	}
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
	if err := rl.Register(context.Background(), 1000); err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	if time.Since(start) > 50*time.Millisecond {
		t.Error("first batch should not block")
	}

	// Next batch should block until refill
	if err := rl.Register(context.Background(), 100); err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed < 900*time.Millisecond {
		t.Errorf("second batch should have blocked for ~1s, only blocked for %v", elapsed)
	}
}

func TestRateLimiter_LargeRegistrationChunks(t *testing.T) {
	stopCh := make(chan struct{})
	defer close(stopCh)

	rl := NewRateLimiter(32*1024, stopCh)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := rl.Register(ctx, 64*1024); err != nil {
		t.Fatalf("large registration should be chunked, got error: %v", err)
	}
}

func TestRateLimiter_ContextCancellation(t *testing.T) {
	stopCh := make(chan struct{})
	defer close(stopCh)

	rl := NewRateLimiter(10, stopCh)
	if err := rl.Register(context.Background(), 10); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := rl.Register(ctx, 10)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline exceeded, got %v", err)
	}
}

func TestRateLimiter_StopChannel(t *testing.T) {
	stopCh := make(chan struct{})

	// Very low rate to force blocking
	rl := NewRateLimiter(10, stopCh)

	// Consume the budget
	if err := rl.Register(context.Background(), 10); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	// Start a goroutine that will be blocked
	done := make(chan error)
	go func() {
		done <- rl.Register(context.Background(), 100) // This should block
	}()

	// Wait a bit, then close stopCh to unblock
	time.Sleep(50 * time.Millisecond)
	close(stopCh)

	// Should unblock quickly
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Register returned error after stopCh closed: %v", err)
		}
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
				if err := rl.Register(context.Background(), 100); err != nil {
					t.Errorf("Register failed: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	// If we get here without deadlock, the test passes
}
