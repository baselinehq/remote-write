package remotewrite

import (
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// isRetryableStatusCode returns true if the HTTP status code is retryable.
// We retry on 429 (Too Many Requests) and 5xx (Server Errors).
// We do NOT retry on other 4xx errors (client errors).
func isRetryableStatusCode(code int) bool {
	return code == http.StatusTooManyRequests || code >= 500
}

// isRetryableError checks if the error is worth retrying.
// Returns false for permanent failures like TLS errors, context cancellation, etc.
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	// Don't retry context cancellation or timeouts from user
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	// Check for TLS/certificate errors (permanent failures)
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		// Check if the underlying error is TLS-related
		errStr := urlErr.Err.Error()
		// x509 certificate errors are not retryable
		if strings.Contains(errStr, "x509:") || strings.Contains(errStr, "certificate") {
			return false
		}
		// TLS handshake failures are usually permanent
		if strings.Contains(errStr, "tls:") || strings.Contains(errStr, "TLS handshake") {
			return false
		}
	}

	// Network errors are generally retryable
	// This includes connection refused, timeouts, etc.
	return true
}

// isEOFError returns true if the error is an EOF (stale connection).
// These get a single immediate retry before entering the normal retry loop.
func isEOFError(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

// addJitter adds up to 10% random jitter to a duration.
// The maximum jitter is capped at 10 seconds.
// This prevents thundering herd when multiple clients retry simultaneously.
func addJitter(d time.Duration) time.Duration {
	jitter := d / 10
	if jitter > 10*time.Second {
		jitter = 10 * time.Second
	}
	if jitter <= 0 {
		return d
	}
	return d + time.Duration(rand.Int64N(int64(jitter)))
}

// getRetryDuration calculates the next retry duration.
// retryAfter from the Retry-After header takes precedence.
// Otherwise, we use exponential backoff (double the previous duration).
func getRetryDuration(retryAfter, currentDuration, maxDuration time.Duration) time.Duration {
	// Retry-After header has highest priority
	if retryAfter > 0 {
		return addJitter(retryAfter)
	}

	// Exponential backoff
	nextDuration := currentDuration * 2
	if nextDuration > maxDuration {
		nextDuration = maxDuration
	}
	return addJitter(nextDuration)
}

// parseRetryAfterHeader parses the Retry-After header value.
// It supports both seconds (integer) and HTTP-date formats per RFC 7231.
// Returns 0 if the header is empty or unparseable.
func parseRetryAfterHeader(value string) time.Duration {
	if value == "" {
		return 0
	}

	// Try parsing as seconds first (most common)
	if seconds, err := strconv.Atoi(value); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}

	// Try parsing as HTTP-date (RFC 7231)
	if t, err := time.Parse(http.TimeFormat, value); err == nil {
		d := time.Until(t)
		if d > 0 {
			return d
		}
	}

	return 0
}

// waitForRetry waits for the specified duration or until context is cancelled.
// Returns nil if the wait completed, or the context error if cancelled.
func waitForRetry(ctx context.Context, duration time.Duration) error {
	t := getTimer(duration)
	defer putTimer(t)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
