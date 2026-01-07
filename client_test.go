package remotewrite

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestNew_Validation(t *testing.T) {
	// Empty URL should fail
	_, err := New(Config{})
	if err == nil {
		t.Fatal("expected error for empty UpstreamURL")
	}

	// Valid URL should succeed
	client, err := New(Config{UpstreamURL: "http://localhost:8428/write"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer client.Close()

	// Verify retries are disabled by default
	if client.RetriesEnabled() {
		t.Error("retries should be disabled by default")
	}
}

func TestNew_WithRetries(t *testing.T) {
	client, err := New(Config{
		UpstreamURL: "http://localhost:8428/write",
		Retry: &RetryConfig{
			MaxRetries: 3,
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer client.Close()

	if !client.RetriesEnabled() {
		t.Error("retries should be enabled when Retry config is provided")
	}
}

func TestForward_StreamingMode(t *testing.T) {
	var receivedBody []byte

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, err := New(Config{UpstreamURL: server.URL})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	body := []byte("test remote write payload")
	resp, err := client.Forward(context.Background(), ForwardRequest{
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
		ContentType:   "application/x-protobuf",
	})
	if err != nil {
		t.Fatalf("Forward failed: %v", err)
	}
	defer resp.Body.Close()

	if !bytes.Equal(receivedBody, body) {
		t.Errorf("body mismatch: got %q, want %q", receivedBody, body)
	}
}

func TestForward_BodyBytes(t *testing.T) {
	var receivedBody []byte

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, err := New(Config{UpstreamURL: server.URL})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	body := []byte("test payload via BodyBytes")
	resp, err := client.Forward(context.Background(), ForwardRequest{
		BodyBytes:   body,
		ContentType: "application/x-protobuf",
	})
	if err != nil {
		t.Fatalf("Forward failed: %v", err)
	}
	defer resp.Body.Close()

	if !bytes.Equal(receivedBody, body) {
		t.Errorf("body mismatch: got %q, want %q", receivedBody, body)
	}
}

func TestForward_Headers(t *testing.T) {
	var receivedHeaders http.Header

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedHeaders = r.Header.Clone()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, err := New(Config{
		UpstreamURL:  server.URL,
		TenantHeader: "X-Scope-OrgID",
		BearerToken:  "my-token",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	resp, err := client.Forward(context.Background(), ForwardRequest{
		TenantID:           "tenant-123",
		BodyBytes:          []byte("data"),
		ContentType:        "application/x-protobuf",
		ContentEncoding:    "snappy",
		RemoteWriteVersion: "0.1.0",
	})
	if err != nil {
		t.Fatalf("Forward failed: %v", err)
	}
	resp.Body.Close()

	if receivedHeaders.Get("Content-Type") != "application/x-protobuf" {
		t.Errorf("Content-Type mismatch: got %q", receivedHeaders.Get("Content-Type"))
	}
	if receivedHeaders.Get("Content-Encoding") != "snappy" {
		t.Errorf("Content-Encoding mismatch: got %q", receivedHeaders.Get("Content-Encoding"))
	}
	if receivedHeaders.Get("X-Prometheus-Remote-Write-Version") != "0.1.0" {
		t.Errorf("X-Prometheus-Remote-Write-Version mismatch")
	}
	if receivedHeaders.Get("X-Scope-OrgID") != "tenant-123" {
		t.Errorf("tenant header mismatch: got %q", receivedHeaders.Get("X-Scope-OrgID"))
	}
	if receivedHeaders.Get("Authorization") != "Bearer my-token" {
		t.Errorf("auth header mismatch: got %q", receivedHeaders.Get("Authorization"))
	}
}

func TestForward_TenantURLSubstitution(t *testing.T) {
	var receivedPath string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	urlWithTenant := strings.Replace(server.URL, "http://", "", 1)
	client, err := New(Config{
		UpstreamURL: "http://" + urlWithTenant + "/insert/{tenant}/prometheus/write",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	resp, err := client.Forward(context.Background(), ForwardRequest{
		TenantID:  "my-tenant-123",
		BodyBytes: []byte("data"),
	})
	if err != nil {
		t.Fatalf("Forward failed: %v", err)
	}
	resp.Body.Close()

	expected := "/insert/my-tenant-123/prometheus/write"
	if receivedPath != expected {
		t.Errorf("path mismatch: got %q, want %q", receivedPath, expected)
	}
}

func TestForward_RetryOn429(t *testing.T) {
	var attempts int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt := atomic.AddInt32(&attempts, 1)
		if attempt < 3 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, err := New(Config{
		UpstreamURL: server.URL,
		Retry: &RetryConfig{
			MaxRetries: 5,
			MinWait:    10 * time.Millisecond,
			MaxWait:    50 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	resp, err := client.Forward(context.Background(), ForwardRequest{
		BodyBytes: []byte("data"),
	})
	if err != nil {
		t.Fatalf("Forward failed: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}
	if atomic.LoadInt32(&attempts) != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}
}

func TestForward_NoRetryOn4xx(t *testing.T) {
	var attempts int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&attempts, 1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()

	client, err := New(Config{
		UpstreamURL: server.URL,
		Retry: &RetryConfig{
			MaxRetries: 5,
			MinWait:    10 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	resp, err := client.Forward(context.Background(), ForwardRequest{
		BodyBytes: []byte("data"),
	})
	if err != nil {
		t.Fatalf("Forward failed: %v", err)
	}
	resp.Body.Close()

	// Should NOT retry on 400
	if atomic.LoadInt32(&attempts) != 1 {
		t.Errorf("expected 1 attempt (no retry), got %d", attempts)
	}
}

func TestForward_NoRetryInStreamingMode(t *testing.T) {
	var attempts int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&attempts, 1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	// No Retry config = streaming mode, no retries
	client, err := New(Config{UpstreamURL: server.URL})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	resp, err := client.Forward(context.Background(), ForwardRequest{
		Body:          io.NopCloser(bytes.NewReader([]byte("data"))),
		ContentLength: 4,
	})
	if err != nil {
		t.Fatalf("Forward failed: %v", err)
	}
	resp.Body.Close()

	// Should only have 1 attemptm, no retries in streaming mode
	if atomic.LoadInt32(&attempts) != 1 {
		t.Errorf("expected 1 attempt (streaming mode), got %d", attempts)
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected status 503, got %d", resp.StatusCode)
	}
}

func TestForward_ContextCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()

	client, err := New(Config{
		UpstreamURL: server.URL,
		Retry: &RetryConfig{
			MaxRetries: 10,
			MinWait:    100 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err = client.Forward(ctx, ForwardRequest{
		BodyBytes: []byte("data"),
	})

	if err == nil {
		t.Fatal("expected error due to context cancellation")
	}
	if !strings.Contains(err.Error(), "context") {
		t.Errorf("expected context error, got: %v", err)
	}
}

func TestForward_ClosedClient(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, err := New(Config{UpstreamURL: server.URL})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	client.Close()

	_, err = client.Forward(context.Background(), ForwardRequest{
		BodyBytes: []byte("data"),
	})

	if err == nil {
		t.Fatal("expected error for closed client")
	}
	if !strings.Contains(err.Error(), "closed") {
		t.Errorf("expected 'closed' error, got: %v", err)
	}
}

func TestForward_GetBody(t *testing.T) {
	var attempts int32
	var receivedBodies [][]byte

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		receivedBodies = append(receivedBodies, body)

		attempt := atomic.AddInt32(&attempts, 1)
		if attempt < 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, err := New(Config{
		UpstreamURL: server.URL,
		Retry: &RetryConfig{
			MaxRetries: 3,
			MinWait:    10 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	bodyData := []byte("test body data")
	resp, err := client.Forward(context.Background(), ForwardRequest{
		Body:          io.NopCloser(bytes.NewReader(bodyData)),
		ContentLength: int64(len(bodyData)),
		GetBody: func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(bodyData)), nil
		},
	})
	if err != nil {
		t.Fatalf("Forward failed: %v", err)
	}
	resp.Body.Close()

	if atomic.LoadInt32(&attempts) != 2 {
		t.Errorf("expected 2 attempts, got %d", attempts)
	}

	// Both attempts should have received the same body
	for i, body := range receivedBodies {
		if !bytes.Equal(body, bodyData) {
			t.Errorf("attempt %d: body mismatch: got %q, want %q", i+1, body, bodyData)
		}
	}
}
