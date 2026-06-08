package benchmarks_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	remotewrite "github.com/baselinehq/remote-write"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/prompb"
)

// newLoopbackServer returns an httptest.Server that drains request bodies and
// replies 200. It is the same shape used for the Forward benchmarks, so Push
// numbers reflect the full encode + send path against a real HTTP transport.
func newLoopbackServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
}

// BenchmarkForward exercises the Forward path against a loopback HTTP server.
func BenchmarkForward(b *testing.B) {
	server := newLoopbackServer()
	defer server.Close()

	body := make([]byte, 64*1024)
	ctx := context.Background()

	run := func(b *testing.B, client *remotewrite.Client, build func() remotewrite.ForwardRequest) {
		b.ReportAllocs()
		b.SetBytes(int64(len(body)))
		for i := 0; i < b.N; i++ {
			resp, err := client.Forward(ctx, build())
			if err != nil {
				b.Fatalf("Forward: %v", err)
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
	}

	b.Run("stream_64KB", func(b *testing.B) {
		client, err := remotewrite.New(remotewrite.Config{UpstreamURL: server.URL})
		if err != nil {
			b.Fatalf("New: %v", err)
		}
		defer client.Close()
		run(b, client, func() remotewrite.ForwardRequest {
			return remotewrite.ForwardRequest{
				Body:          io.NopCloser(bytes.NewReader(body)),
				ContentLength: int64(len(body)),
			}
		})
	})

	b.Run("body_bytes_64KB", func(b *testing.B) {
		client, err := remotewrite.New(remotewrite.Config{UpstreamURL: server.URL})
		if err != nil {
			b.Fatalf("New: %v", err)
		}
		defer client.Close()
		run(b, client, func() remotewrite.ForwardRequest {
			return remotewrite.ForwardRequest{BodyBytes: body}
		})
	})

	b.Run("retryable_body_64KB", func(b *testing.B) {
		client, err := remotewrite.New(remotewrite.Config{
			UpstreamURL: server.URL,
			Retry:       &remotewrite.RetryConfig{MaxRetries: 3},
		})
		if err != nil {
			b.Fatalf("New: %v", err)
		}
		defer client.Close()
		run(b, client, func() remotewrite.ForwardRequest {
			return remotewrite.ForwardRequest{BodyBytes: body}
		})
	})
}

// BenchmarkPush exercises the gather + encode + send path against a loopback
// HTTP server.
func BenchmarkPush(b *testing.B) {
	server := newLoopbackServer()
	defer server.Close()

	newClient := func(b *testing.B) *remotewrite.Client {
		b.Helper()
		client, err := remotewrite.New(remotewrite.Config{UpstreamURL: server.URL})
		if err != nil {
			b.Fatalf("New: %v", err)
		}
		return client
	}

	now := func() time.Time { return time.Unix(1234, 0) }

	b.Run("small_batch", func(b *testing.B) {
		client := newClient(b)
		defer client.Close()

		reg := prometheus.NewRegistry()
		for i := 0; i < 50; i++ {
			c := prometheus.NewCounter(prometheus.CounterOpts{Name: fmt.Sprintf("metric_%d", i)})
			c.Inc()
			reg.MustRegister(c)
		}
		req := remotewrite.PushRequest{
			Gatherer:       reg,
			ExternalLabels: map[string]string{"job": "test"},
			Now:            now,
		}

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := client.Push(context.Background(), req); err != nil {
				b.Fatalf("Push: %v", err)
			}
		}
	})

	b.Run("large_batch", func(b *testing.B) {
		client := newClient(b)
		defer client.Close()

		reg := prometheus.NewRegistry()
		for i := 0; i < 1000; i++ {
			c := prometheus.NewCounter(prometheus.CounterOpts{
				Name:        fmt.Sprintf("large_metric_%d", i),
				ConstLabels: prometheus.Labels{"shard": fmt.Sprintf("%d", i%10)},
			})
			c.Add(float64(i))
			reg.MustRegister(c)
		}
		req := remotewrite.PushRequest{
			Gatherer:          reg,
			MaxSeriesPerBatch: 500,
			ExternalLabels:    map[string]string{"cluster": "prod", "region": "us-east-1"},
			Now:               now,
		}

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := client.Push(context.Background(), req); err != nil {
				b.Fatalf("Push: %v", err)
			}
		}
	})

	b.Run("histograms", func(b *testing.B) {
		client := newClient(b)
		defer client.Close()

		reg := prometheus.NewRegistry()
		for i := 0; i < 50; i++ {
			h := prometheus.NewHistogram(prometheus.HistogramOpts{
				Name:    fmt.Sprintf("request_duration_%d", i),
				Buckets: prometheus.DefBuckets,
			})
			for j := 0; j < 10; j++ {
				h.Observe(float64(j) * 0.1)
			}
			reg.MustRegister(h)
		}
		req := remotewrite.PushRequest{
			Gatherer:       reg,
			ExternalLabels: map[string]string{"service": "api"},
			Now:            now,
		}

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := client.Push(context.Background(), req); err != nil {
				b.Fatalf("Push: %v", err)
			}
		}
	})

	b.Run("parallel", func(b *testing.B) {
		client := newClient(b)
		defer client.Close()

		reg := prometheus.NewRegistry()
		for i := 0; i < 200; i++ {
			c := prometheus.NewCounter(prometheus.CounterOpts{Name: fmt.Sprintf("parallel_metric_%d", i)})
			c.Inc()
			reg.MustRegister(c)
		}
		req := remotewrite.PushRequest{
			Gatherer:       reg,
			ExternalLabels: map[string]string{"job": "parallel_test"},
			Now:            now,
		}

		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if err := client.Push(context.Background(), req); err != nil {
					b.Fatalf("Push: %v", err)
				}
			}
		})
	})
}

// BenchmarkPushTimeSeries measures the PushTimeSeries pipeline for 1000 series.
func BenchmarkPushTimeSeries(b *testing.B) {
	server := newLoopbackServer()
	defer server.Close()

	client, err := remotewrite.New(remotewrite.Config{UpstreamURL: server.URL})
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	defer client.Close()

	series := make([]prompb.TimeSeries, 1000)
	for i := range series {
		series[i] = prompb.TimeSeries{
			Labels: []prompb.Label{
				{Name: "__name__", Value: "bench_metric"},
				{Name: "id", Value: strconv.Itoa(i)},
			},
			Samples: []prompb.Sample{{Value: 1, Timestamp: int64(i + 1)}},
		}
	}

	req := remotewrite.PushTimeSeriesRequest{
		TimeSeries:        series,
		MaxSeriesPerBatch: 200,
		MaxBatchBytes:     1 << 20,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := client.PushTimeSeries(context.Background(), req); err != nil {
			b.Fatalf("PushTimeSeries: %v", err)
		}
	}
}
