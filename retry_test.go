package remotewrite

import (
	"context"
	"net/http"
	"net/url"
	"testing"
	"time"
)

func TestParseRetryAfterHeader(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		expected time.Duration
	}{
		{
			name:     "empty",
			value:    "",
			expected: 0,
		},
		{
			name:     "seconds",
			value:    "120",
			expected: 120 * time.Second,
		},
		{
			name:     "zero_seconds",
			value:    "0",
			expected: 0,
		},
		{
			name:     "negative_seconds",
			value:    "-10",
			expected: 0,
		},
		{
			name:     "invalid",
			value:    "not-a-number",
			expected: 0,
		},
		// HTTP-date format tested separately as it depends on current time
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseRetryAfterHeader(tt.value)
			if result != tt.expected {
				t.Errorf("parseRetryAfterHeader(%q) = %v, want %v", tt.value, result, tt.expected)
			}
		})
	}
}

func TestParseRetryAfterHeader_HTTPDate(t *testing.T) {
	// Test HTTP-date format (RFC 7231 uses IMF-fixdate which is http.TimeFormat)
	futureTime := time.Now().Add(30 * time.Second).UTC()
	httpDate := futureTime.Format(http.TimeFormat)

	result := parseRetryAfterHeader(httpDate)

	// Should be approximately 30 seconds (with some tolerance for test execution time)
	if result < 25*time.Second || result > 35*time.Second {
		t.Errorf("parseRetryAfterHeader(%q) = %v, expected ~30s", httpDate, result)
	}
}

func TestAddJitter(t *testing.T) {
	d := 10 * time.Second

	// Run multiple times to ensure jitter is being added
	var hasJitter bool
	for i := 0; i < 100; i++ {
		result := addJitter(d)
		if result != d {
			hasJitter = true
		}
		// Result should be between d and d + 10% (max jitter)
		if result < d {
			t.Errorf("addJitter(%v) = %v, should be >= %v", d, result, d)
		}
		if result > d+d/10 {
			t.Errorf("addJitter(%v) = %v, should be <= %v", d, result, d+d/10)
		}
	}
	if !hasJitter {
		t.Error("addJitter should add random jitter, but all results were identical")
	}
}

func TestAddJitter_MaxCap(t *testing.T) {
	// Test that jitter is capped at 10 seconds
	d := 5 * time.Minute // 10% would be 30s, but cap is 10s

	for i := 0; i < 100; i++ {
		result := addJitter(d)
		maxExpected := d + 10*time.Second
		if result > maxExpected {
			t.Errorf("addJitter(%v) = %v, should be <= %v (10s cap)", d, result, maxExpected)
		}
	}
}

func TestNextRetryWait(t *testing.T) {
	tests := []struct {
		name        string
		retryAfter  time.Duration
		current     time.Duration
		expectedMin time.Duration
		expectedMax time.Duration
	}{
		{
			name:        "retry_after_takes_precedence",
			retryAfter:  5 * time.Second,
			current:     1 * time.Second,
			expectedMin: 5 * time.Second,
			expectedMax: 5*time.Second + 1*time.Second,
		},
		{
			name:        "first_retry_uses_current",
			retryAfter:  0,
			current:     1 * time.Second,
			expectedMin: 1 * time.Second,
			expectedMax: 1*time.Second + 1*time.Second/10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := nextRetryWait(tt.retryAfter, tt.current)
			if result < tt.expectedMin || result > tt.expectedMax {
				t.Errorf("nextRetryWait(%v, %v) = %v, expected [%v, %v]",
					tt.retryAfter, tt.current, result, tt.expectedMin, tt.expectedMax)
			}
		})
	}
}

func TestAdvanceRetryWait(t *testing.T) {
	if got := advanceRetryWait(time.Second, 30*time.Second); got != 2*time.Second {
		t.Errorf("advanceRetryWait(1s, 30s) = %v, want 2s", got)
	}
	if got := advanceRetryWait(20*time.Second, 30*time.Second); got != 30*time.Second {
		t.Errorf("advanceRetryWait(20s, 30s) = %v, want 30s (capped)", got)
	}
}

func TestIsRetryableStatusCode(t *testing.T) {
	tests := []struct {
		code      int
		retryable bool
	}{
		{200, false},
		{201, false},
		{204, false},
		{400, false},
		{401, false},
		{403, false},
		{404, false},
		{429, true}, // Too Many Requests - SHOULD retry
		{500, true},
		{502, true},
		{503, true},
		{504, true},
		{599, true},
		{600, false}, // Outside HTTP range
		{700, false},
	}

	for _, tt := range tests {
		t.Run(string(rune(tt.code)), func(t *testing.T) {
			result := isRetryableStatusCode(tt.code)
			if result != tt.retryable {
				t.Errorf("isRetryableStatusCode(%d) = %v, want %v", tt.code, result, tt.retryable)
			}
		})
	}
}

func TestIsRetryableError_HTTPClientTimeoutWrappedAsURLError(t *testing.T) {
	err := &url.Error{
		Op:  "Post",
		URL: "http://example.invalid",
		Err: context.DeadlineExceeded,
	}

	if !isRetryableError(err) {
		t.Fatal("expected wrapped timeout to be retryable")
	}
}

func TestIsRetryableError_UserDeadlineExceededIsNotRetryable(t *testing.T) {
	if isRetryableError(context.DeadlineExceeded) {
		t.Fatal("expected user deadline exceeded to be non-retryable")
	}
}
