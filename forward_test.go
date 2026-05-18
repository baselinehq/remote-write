package remotewrite

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"
)

// TestNew_UpstreamURLValidation verifies UpstreamURL must be an absolute URL
// with scheme and host.
func TestNew_UpstreamURLValidation(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("expected error for empty UpstreamURL")
	}

	for _, upstreamURL := range []string{
		"localhost:8428/write",
		"/api/v1/write",
		"http:///api/v1/write",
	} {
		t.Run(upstreamURL, func(t *testing.T) {
			if _, err := New(Config{UpstreamURL: upstreamURL}); err == nil {
				t.Fatal("expected error for UpstreamURL without scheme and host")
			}
		})
	}
}

// TestNew_TenantPlaceholderOnlyInPath verifies {tenant} can only appear once
// and only in the URL path.
func TestNew_TenantPlaceholderOnlyInPath(t *testing.T) {
	for _, upstreamURL := range []string{
		"http://{tenant}.example.com/write",
		"http://example.com/write?tenant={tenant}",
		"http://example.com/write/{tenant}?tenant={tenant}",
	} {
		t.Run(upstreamURL, func(t *testing.T) {
			c, err := New(Config{UpstreamURL: upstreamURL})
			if err == nil {
				c.Close()
				t.Fatal("expected error for {tenant} placeholder outside URL path")
			}
		})
	}
}

// TestForward_TenantPathEscaping verifies tenant IDs are path-escaped when
// substituted into UpstreamURL.
func TestForward_TenantPathEscaping(t *testing.T) {
	var receivedRequestURI string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedRequestURI = r.RequestURI
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, err := New(Config{
		UpstreamURL: server.URL + "/insert/{tenant}/prometheus/write",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer client.Close()

	tenantID := "team/a b%2F..?#x"
	resp, err := client.Forward(context.Background(), ForwardRequest{
		TenantID:  tenantID,
		BodyBytes: []byte("data"),
	})
	if err != nil {
		t.Fatalf("Forward: %v", err)
	}
	resp.Body.Close()

	want := "/insert/" + url.PathEscape(tenantID) + "/prometheus/write"
	if receivedRequestURI != want {
		t.Errorf("request URI = %q, want %q", receivedRequestURI, want)
	}
}

// TestForward_ExtraHeadersRejectReserved verifies ExtraHeaders cannot override
// reserved or configured headers.
func TestForward_ExtraHeadersRejectReserved(t *testing.T) {
	var attempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&attempts, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, err := New(Config{
		UpstreamURL:  server.URL,
		TenantHeader: "X-Scope-OrgID",
		BearerToken:  "my-token",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer client.Close()

	for _, headerName := range []string{
		"content-type",
		"Content-Encoding",
		"User-Agent",
		"X-Prometheus-Remote-Write-Version",
		"authorization",
		"X-Scope-OrgID",
	} {
		t.Run(headerName, func(t *testing.T) {
			_, err := client.Forward(context.Background(), ForwardRequest{
				TenantID:     "tenant-1",
				BodyBytes:    []byte("data"),
				ExtraHeaders: http.Header{headerName: {"override"}},
			})
			if err == nil {
				t.Fatal("expected reserved/protected header error")
			}
		})
	}

	if atomic.LoadInt32(&attempts) != 0 {
		t.Fatalf("expected validation to fail before upstream request, got %d attempts", attempts)
	}
}

// TestForward_RetryWithNoBody verifies retry mode does not panic and replays
// safely when the request has an empty or absent body.
func TestForward_RetryWithNoBody(t *testing.T) {
	for _, tt := range []struct {
		name string
		req  ForwardRequest
	}{
		{"empty request", ForwardRequest{}},
		{"empty BodyBytes", ForwardRequest{BodyBytes: []byte{}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var attempts int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Errorf("read body: %v", err)
				}
				if len(body) != 0 {
					t.Errorf("expected empty body, got %q", body)
				}
				if atomic.AddInt32(&attempts, 1) < 2 {
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()

			client, err := New(Config{
				UpstreamURL: server.URL,
				Retry: &RetryConfig{
					MaxRetries: 2,
					MinWait:    time.Millisecond,
					MaxWait:    time.Millisecond,
				},
			})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			defer client.Close()

			resp, err := client.Forward(context.Background(), tt.req)
			if err != nil {
				t.Fatalf("Forward: %v", err)
			}
			resp.Body.Close()

			if got := atomic.LoadInt32(&attempts); got != 2 {
				t.Errorf("attempts = %d, want 2", got)
			}
		})
	}
}

// TestForward_BufferedBodyTooLarge verifies that a known body larger than
// MaxBodySize returns a body-too-large error (rather than silently disabling
// retries) and that IsBodyTooLarge recognises the wrapped error.
func TestForward_BufferedBodyTooLarge(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, err := New(Config{
		UpstreamURL: server.URL,
		Retry: &RetryConfig{
			MaxRetries:  2,
			MinWait:     time.Millisecond,
			MaxWait:     time.Millisecond,
			MaxBodySize: 8,
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer client.Close()

	payload := bytes.Repeat([]byte("x"), 64)
	_, err = client.Forward(context.Background(), ForwardRequest{
		Body:          io.NopCloser(bytes.NewReader(payload)),
		ContentLength: int64(len(payload)),
	})
	if err == nil {
		t.Fatal("expected body-too-large error")
	}
	if !IsBodyTooLarge(err) {
		t.Fatalf("IsBodyTooLarge(%v) = false; want true (wrapped)", err)
	}
	wrapped := fmt.Errorf("upstream: %w", err)
	if !IsBodyTooLarge(wrapped) {
		t.Fatalf("IsBodyTooLarge did not unwrap %v", wrapped)
	}
}

// TestForward_FirstRetryUsesMinWait verifies that the first retry wait is
// approximately MinWait rather than 2*MinWait.
func TestForward_FirstRetryUsesMinWait(t *testing.T) {
	var times []time.Time
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		times = append(times, time.Now())
		if len(times) < 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	const minWait = 50 * time.Millisecond
	client, err := New(Config{
		UpstreamURL: server.URL,
		Retry: &RetryConfig{
			MaxRetries: 2,
			MinWait:    minWait,
			MaxWait:    time.Second,
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer client.Close()

	resp, err := client.Forward(context.Background(), ForwardRequest{BodyBytes: []byte("data")})
	if err != nil {
		t.Fatalf("Forward: %v", err)
	}
	resp.Body.Close()

	if len(times) != 2 {
		t.Fatalf("got %d attempts, want 2", len(times))
	}
	gap := times[1].Sub(times[0])
	// First retry should wait ~MinWait (+0–10% jitter). Must not be ~2*MinWait.
	if gap < minWait || gap > minWait*2-time.Millisecond {
		t.Fatalf("first retry gap = %v, expected ~%v (must not be ~%v)", gap, minWait, 2*minWait)
	}
}

// TestForward_StreamingBodyUnknownContentLength verifies that a non-nil Body
// with ContentLength == 0 is treated as unknown and the upstream actually
// receives all body bytes.
func TestForward_StreamingBodyUnknownContentLength(t *testing.T) {
	var received []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, err := New(Config{UpstreamURL: server.URL})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer client.Close()

	payload := []byte("hello world")
	resp, err := client.Forward(context.Background(), ForwardRequest{
		Body: io.NopCloser(bytes.NewReader(payload)),
		// ContentLength intentionally omitted (== 0).
	})
	if err != nil {
		t.Fatalf("Forward: %v", err)
	}
	resp.Body.Close()

	if !bytes.Equal(received, payload) {
		t.Fatalf("upstream received %q, want %q", received, payload)
	}
}

// TestForward_RateLimitRespectsContext verifies that a body larger than the
// per-second rate limit budget still respects context cancellation rather than
// blocking indefinitely.
func TestForward_RateLimitRespectsContext(t *testing.T) {
	var attempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&attempts, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, err := New(Config{
		UpstreamURL:          server.URL,
		RateLimitBytesPerSec: 32 * 1024,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err = client.Forward(ctx, ForwardRequest{
		BodyBytes: make([]byte, 64*1024),
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	if got := atomic.LoadInt32(&attempts); got != 0 {
		t.Fatalf("expected request not to reach upstream, got %d attempts", got)
	}
}
