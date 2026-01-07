package remotewrite

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// BenchmarkForward_Streaming benchmarks streaming mode with no buffering
func BenchmarkForward_Streaming_64KB(b *testing.B) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, err := New(Config{
		UpstreamURL: server.URL,
		// No Retry = streaming mode
	})
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	body := make([]byte, 64*1024)
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(int64(len(body)))

	for i := 0; i < b.N; i++ {
		resp, err := client.Forward(ctx, ForwardRequest{
			Body:          io.NopCloser(bytes.NewReader(body)),
			ContentLength: int64(len(body)),
			ContentType:   "application/x-protobuf",
		})
		if err != nil {
			b.Fatalf("Forward failed: %v", err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
}

// BenchmarkForward_BodyBytes benchmarks using pre-buffered BodyBytes.
// Best performance when caller already has bytes (avoids io.Reader overhead)
func BenchmarkForward_BodyBytes_64KB(b *testing.B) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, err := New(Config{
		UpstreamURL: server.URL,
	})
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	body := make([]byte, 64*1024)
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(int64(len(body)))

	for i := 0; i < b.N; i++ {
		resp, err := client.Forward(ctx, ForwardRequest{
			BodyBytes:   body,
			ContentType: "application/x-protobuf",
		})
		if err != nil {
			b.Fatalf("Forward failed: %v", err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
}

// BenchmarkForward_WithRetries benchmarks with retries enabled
func BenchmarkForward_WithRetries_64KB(b *testing.B) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, err := New(Config{
		UpstreamURL: server.URL,
		Retry: &RetryConfig{
			MaxRetries: 3,
		},
	})
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	body := make([]byte, 64*1024)
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(int64(len(body)))

	for i := 0; i < b.N; i++ {
		resp, err := client.Forward(ctx, ForwardRequest{
			Body:          io.NopCloser(bytes.NewReader(body)),
			ContentLength: int64(len(body)),
		})
		if err != nil {
			b.Fatalf("Forward failed: %v", err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
}

// BenchmarkForward_Parallel benchmarks concurrent streaming forwards
func BenchmarkForward_Parallel(b *testing.B) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, err := New(Config{
		UpstreamURL: server.URL,
	})
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	body := make([]byte, 64*1024)
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(int64(len(body)))

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			resp, err := client.Forward(ctx, ForwardRequest{
				BodyBytes: body,
			})
			if err != nil {
				b.Errorf("Forward failed: %v", err)
				return
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
	})
}

// BenchmarkTimerPool benchmarks the timer pool vs raw time.NewTimer
func BenchmarkTimerPool(b *testing.B) {
	b.Run("pooled", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			t := getTimer(time.Millisecond)
			putTimer(t)
		}
	})

	b.Run("raw", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			t := time.NewTimer(time.Millisecond)
			t.Stop()
		}
	})
}

// emptyBody is a zero-allocation body reader
type emptyBody struct{}

func (emptyBody) Read([]byte) (int, error) { return 0, io.EOF }
func (emptyBody) Close() error             { return nil }

// stubRoundTripper is a zero-allocation RoundTripper for pure client benchmarking
type stubRoundTripper struct {
	resp http.Response
}

func newStubRT() *stubRoundTripper {
	rt := &stubRoundTripper{}
	rt.resp = http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     nil, // nil is valid
		Body:       emptyBody{},
	}
	return rt
}

func (s *stubRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	// Drain body without allocations
	if req.Body != nil {
		io.Copy(io.Discard, req.Body)
		req.Body.Close()
	}
	s.resp.Request = req
	return &s.resp, nil
}

// BenchmarkClientOnly_Streaming benchmarks client-only costs
func BenchmarkClientOnly_Streaming_64KB(b *testing.B) {
	client, err := New(Config{
		UpstreamURL: "http://localhost:8428/api/v1/write",
		Transport:   &http.Transport{},
	})
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}
	// Replace transport with stub
	client.httpClient.Transport = newStubRT()
	defer client.Close()

	body := make([]byte, 64*1024)
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(int64(len(body)))

	for i := 0; i < b.N; i++ {
		resp, err := client.Forward(ctx, ForwardRequest{
			Body:          io.NopCloser(bytes.NewReader(body)),
			ContentLength: int64(len(body)),
		})
		if err != nil {
			b.Fatalf("Forward failed: %v", err)
		}
		resp.Body.Close()
	}
}

// BenchmarkClientOnly_BodyBytes benchmarks client-only with BodyBytes
func BenchmarkClientOnly_BodyBytes_64KB(b *testing.B) {
	client, err := New(Config{
		UpstreamURL: "http://localhost:8428/api/v1/write",
		Transport:   &http.Transport{},
	})
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}
	client.httpClient.Transport = newStubRT()
	defer client.Close()

	body := make([]byte, 64*1024)
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(int64(len(body)))

	for i := 0; i < b.N; i++ {
		resp, err := client.Forward(ctx, ForwardRequest{
			BodyBytes: body,
		})
		if err != nil {
			b.Fatalf("Forward failed: %v", err)
		}
		resp.Body.Close()
	}
}

// BenchmarkClientOnly_WithDefaults benchmarks with default headers
func BenchmarkClientOnly_WithDefaults_64KB(b *testing.B) {
	client, err := New(Config{
		UpstreamURL:               "http://localhost:8428/api/v1/write",
		Transport:                 &http.Transport{},
		DefaultContentType:        "application/x-protobuf",
		DefaultContentEncoding:    "snappy",
		DefaultRemoteWriteVersion: "0.1.0",
	})
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}
	client.httpClient.Transport = newStubRT()
	defer client.Close()

	body := make([]byte, 64*1024)
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(int64(len(body)))

	for i := 0; i < b.N; i++ {
		resp, err := client.Forward(ctx, ForwardRequest{
			BodyBytes: body,
			// No headers - using defaults
		})
		if err != nil {
			b.Fatalf("Forward failed: %v", err)
		}
		resp.Body.Close()
	}
}

// BenchmarkClientOnly_WithTenant benchmarks tenant URL substitution cost
func BenchmarkClientOnly_WithTenant_64KB(b *testing.B) {
	client, err := New(Config{
		UpstreamURL: "http://localhost:8428/insert/{tenant}/prometheus/api/v1/write",
		Transport:   &http.Transport{},
	})
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}
	client.httpClient.Transport = newStubRT()
	defer client.Close()

	body := make([]byte, 64*1024)
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(int64(len(body)))

	for i := 0; i < b.N; i++ {
		resp, err := client.Forward(ctx, ForwardRequest{
			TenantID:  "tenant-12345",
			BodyBytes: body,
		})
		if err != nil {
			b.Fatalf("Forward failed: %v", err)
		}
		resp.Body.Close()
	}
}

// BenchmarkClientOnly_WithRetries benchmarks client-only with retries
func BenchmarkClientOnly_WithRetries_64KB(b *testing.B) {
	client, err := New(Config{
		UpstreamURL: "http://localhost:8428/api/v1/write",
		Transport:   &http.Transport{},
		Retry: &RetryConfig{
			MaxRetries: 3,
		},
	})
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}
	client.httpClient.Transport = newStubRT()
	defer client.Close()

	body := make([]byte, 64*1024)
	ctx := context.Background()

	// Reusable reader to avoid benchmark allocation noise
	reader := bytes.NewReader(body)

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(int64(len(body)))

	for i := 0; i < b.N; i++ {
		reader.Reset(body)
		resp, err := client.Forward(ctx, ForwardRequest{
			Body:          io.NopCloser(reader),
			ContentLength: int64(len(body)),
		})
		if err != nil {
			b.Fatalf("Forward failed: %v", err)
		}
		resp.Body.Close()
	}
}

// BenchmarkClientOnly_ActualRetry benchmarks retry path with actual retries
func BenchmarkClientOnly_ActualRetry_1Retry(b *testing.B) {
	// Server fails once then succeeds
	var attempts int
	rt := &retryRoundTripper{
		failUntil: 1,
		attempts:  &attempts,
	}

	client, err := New(Config{
		UpstreamURL: "http://localhost:8428/api/v1/write",
		Transport:   &http.Transport{},
		Retry: &RetryConfig{
			MaxRetries: 3,
			MinWait:    time.Millisecond,
			MaxWait:    10 * time.Millisecond,
		},
	})
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}
	client.httpClient.Transport = rt
	defer client.Close()

	body := make([]byte, 64*1024)
	ctx := context.Background()
	reader := bytes.NewReader(body)

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(int64(len(body)))

	for i := 0; i < b.N; i++ {
		attempts = 0
		reader.Reset(body)
		resp, err := client.Forward(ctx, ForwardRequest{
			Body:          io.NopCloser(reader),
			ContentLength: int64(len(body)),
		})
		if err != nil {
			b.Fatalf("Forward failed: %v", err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
}

// BenchmarkClientOnly_ActualRetry_2Retries benchmarks with 2 retries
func BenchmarkClientOnly_ActualRetry_2Retries(b *testing.B) {
	var attempts int
	rt := &retryRoundTripper{
		failUntil: 2,
		attempts:  &attempts,
	}

	client, err := New(Config{
		UpstreamURL: "http://localhost:8428/api/v1/write",
		Transport:   &http.Transport{},
		Retry: &RetryConfig{
			MaxRetries: 3,
			MinWait:    time.Millisecond,
			MaxWait:    10 * time.Millisecond,
		},
	})
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}
	client.httpClient.Transport = rt
	defer client.Close()

	body := make([]byte, 64*1024)
	ctx := context.Background()
	reader := bytes.NewReader(body)

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(int64(len(body)))

	for i := 0; i < b.N; i++ {
		attempts = 0
		reader.Reset(body)
		resp, err := client.Forward(ctx, ForwardRequest{
			Body:          io.NopCloser(reader),
			ContentLength: int64(len(body)),
		})
		if err != nil {
			b.Fatalf("Forward failed: %v", err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
}

// retryRoundTripper returns 503 for the first N attempts, then 200
type retryRoundTripper struct {
	failUntil int
	attempts  *int
}

func (r *retryRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Body != nil {
		io.Copy(io.Discard, req.Body)
		req.Body.Close()
	}

	*r.attempts++
	if *r.attempts <= r.failUntil {
		return &http.Response{
			StatusCode: http.StatusServiceUnavailable,
			Status:     "503 Service Unavailable",
			Proto:      "HTTP/1.1",
			ProtoMajor: 1,
			ProtoMinor: 1,
			Header:     http.Header{"Retry-After": []string{"0"}},
			Body:       io.NopCloser(bytes.NewReader([]byte("retry later"))),
			Request:    req,
		}, nil
	}

	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewReader(nil)),
		Request:    req,
	}, nil
}
