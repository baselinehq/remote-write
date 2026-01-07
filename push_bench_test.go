package remotewrite

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/prompb"
)

// BenchmarkEncode_100Counters measures pure encoding cost for 100 counters.
func BenchmarkEncode_100Counters(b *testing.B) {
	reg := prometheus.NewRegistry()
	for i := 0; i < 100; i++ {
		c := prometheus.NewCounter(prometheus.CounterOpts{
			Name:        fmt.Sprintf("counter_%d", i),
			ConstLabels: prometheus.Labels{"service": "test", "env": "prod"},
		})
		c.Inc()
		reg.MustRegister(c)
	}
	mfs, _ := reg.Gather()
	ext := []prompb.Label{{Name: "cluster", Value: "A"}}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		tsBuf := getTimeSeriesSlice()
		var pooledLabels []*[]prompb.Label
		var pooledSamples []*[]prompb.Sample

		for _, mf := range mfs {
			convertMetricFamily(mf, tsBuf, ext, 1234, &pooledLabels, &pooledSamples)
		}

		for _, l := range pooledLabels {
			putLabelSlice(l)
		}
		for _, s := range pooledSamples {
			putSampleSlice(s)
		}
		putTimeSeriesSlice(tsBuf)
	}
}

// BenchmarkEncode_100Gauges measures pure encoding cost for 100 gauges.
func BenchmarkEncode_100Gauges(b *testing.B) {
	reg := prometheus.NewRegistry()
	for i := 0; i < 100; i++ {
		g := prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        fmt.Sprintf("gauge_%d", i),
			ConstLabels: prometheus.Labels{"service": "test", "env": "prod"},
		})
		g.Set(float64(i) * 1.5)
		reg.MustRegister(g)
	}
	mfs, _ := reg.Gather()
	ext := []prompb.Label{{Name: "cluster", Value: "A"}}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		tsBuf := getTimeSeriesSlice()
		var pooledLabels []*[]prompb.Label
		var pooledSamples []*[]prompb.Sample

		for _, mf := range mfs {
			convertMetricFamily(mf, tsBuf, ext, 1234, &pooledLabels, &pooledSamples)
		}

		for _, l := range pooledLabels {
			putLabelSlice(l)
		}
		for _, s := range pooledSamples {
			putSampleSlice(s)
		}
		putTimeSeriesSlice(tsBuf)
	}
}

// BenchmarkEncode_100Histograms measures encoding cost for 100 histograms (high series count).
func BenchmarkEncode_100Histograms(b *testing.B) {
	reg := prometheus.NewRegistry()
	for i := 0; i < 100; i++ {
		h := prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:        fmt.Sprintf("http_duration_%d", i),
			Buckets:     prometheus.DefBuckets, // 11 buckets
			ConstLabels: prometheus.Labels{"handler": fmt.Sprintf("/api/v%d", i%5)},
		})
		h.Observe(0.25)
		h.Observe(0.75)
		reg.MustRegister(h)
	}
	mfs, _ := reg.Gather()
	ext := []prompb.Label{{Name: "region", Value: "us-east-1"}}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		tsBuf := getTimeSeriesSlice()
		var pooledLabels []*[]prompb.Label
		var pooledSamples []*[]prompb.Sample

		for _, mf := range mfs {
			convertMetricFamily(mf, tsBuf, ext, 1234, &pooledLabels, &pooledSamples)
		}

		for _, l := range pooledLabels {
			putLabelSlice(l)
		}
		for _, s := range pooledSamples {
			putSampleSlice(s)
		}
		putTimeSeriesSlice(tsBuf)
	}
}

// BenchmarkEncode_100Summaries measures encoding cost for 100 summaries.
func BenchmarkEncode_100Summaries(b *testing.B) {
	reg := prometheus.NewRegistry()
	for i := 0; i < 100; i++ {
		s := prometheus.NewSummary(prometheus.SummaryOpts{
			Name:        fmt.Sprintf("rpc_duration_%d", i),
			Objectives:  map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
			ConstLabels: prometheus.Labels{"method": fmt.Sprintf("method_%d", i%10)},
		})
		s.Observe(0.1)
		s.Observe(0.5)
		reg.MustRegister(s)
	}
	mfs, _ := reg.Gather()
	ext := []prompb.Label{{Name: "datacenter", Value: "dc1"}}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		tsBuf := getTimeSeriesSlice()
		var pooledLabels []*[]prompb.Label
		var pooledSamples []*[]prompb.Sample

		for _, mf := range mfs {
			convertMetricFamily(mf, tsBuf, ext, 1234, &pooledLabels, &pooledSamples)
		}

		for _, l := range pooledLabels {
			putLabelSlice(l)
		}
		for _, s := range pooledSamples {
			putSampleSlice(s)
		}
		putTimeSeriesSlice(tsBuf)
	}
}

// BenchmarkEncode_MixedWorkload measures encoding for realistic mixed metric types.
func BenchmarkEncode_MixedWorkload(b *testing.B) {
	reg := prometheus.NewRegistry()

	// 40 counters
	for i := 0; i < 40; i++ {
		c := prometheus.NewCounter(prometheus.CounterOpts{
			Name:        fmt.Sprintf("requests_total_%d", i),
			ConstLabels: prometheus.Labels{"endpoint": fmt.Sprintf("/api/v%d", i%3)},
		})
		c.Add(float64(i * 100))
		reg.MustRegister(c)
	}

	// 30 gauges
	for i := 0; i < 30; i++ {
		g := prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        fmt.Sprintf("memory_bytes_%d", i),
			ConstLabels: prometheus.Labels{"type": "heap"},
		})
		g.Set(float64(i * 1024 * 1024))
		reg.MustRegister(g)
	}

	// 20 histograms (high series expansion)
	for i := 0; i < 20; i++ {
		h := prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    fmt.Sprintf("latency_%d", i),
			Buckets: prometheus.DefBuckets,
		})
		h.Observe(0.1)
		reg.MustRegister(h)
	}

	// 10 summaries
	for i := 0; i < 10; i++ {
		s := prometheus.NewSummary(prometheus.SummaryOpts{
			Name:       fmt.Sprintf("size_%d", i),
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		})
		s.Observe(1000)
		reg.MustRegister(s)
	}

	mfs, _ := reg.Gather()
	ext := []prompb.Label{
		{Name: "cluster", Value: "prod-1"},
		{Name: "environment", Value: "production"},
		{Name: "region", Value: "us-west-2"},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		tsBuf := getTimeSeriesSlice()
		var pooledLabels []*[]prompb.Label
		var pooledSamples []*[]prompb.Sample

		for _, mf := range mfs {
			convertMetricFamily(mf, tsBuf, ext, 1234, &pooledLabels, &pooledSamples)
		}

		for _, l := range pooledLabels {
			putLabelSlice(l)
		}
		for _, s := range pooledSamples {
			putSampleSlice(s)
		}
		putTimeSeriesSlice(tsBuf)
	}
}

// BenchmarkEncode_HighCardinality measures encoding with many unique labels.
func BenchmarkEncode_HighCardinality(b *testing.B) {
	reg := prometheus.NewRegistry()
	for i := 0; i < 100; i++ {
		c := prometheus.NewCounter(prometheus.CounterOpts{
			Name: "api_requests_total",
			ConstLabels: prometheus.Labels{
				"method":   fmt.Sprintf("method_%d", i%10),
				"endpoint": fmt.Sprintf("/api/v%d/resource_%d", i%5, i),
				"status":   fmt.Sprintf("%d", 200+i%5),
				"pod":      fmt.Sprintf("pod-%d", i),
			},
		})
		c.Inc()
		reg.MustRegister(c)
	}
	mfs, _ := reg.Gather()
	ext := []prompb.Label{
		{Name: "cluster", Value: "prod"},
		{Name: "namespace", Value: "default"},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		tsBuf := getTimeSeriesSlice()
		var pooledLabels []*[]prompb.Label
		var pooledSamples []*[]prompb.Sample

		for _, mf := range mfs {
			convertMetricFamily(mf, tsBuf, ext, 1234, &pooledLabels, &pooledSamples)
		}

		for _, l := range pooledLabels {
			putLabelSlice(l)
		}
		for _, s := range pooledSamples {
			putSampleSlice(s)
		}
		putTimeSeriesSlice(tsBuf)
	}
}

// BenchmarkPush_SmallBatch measures full Push pipeline with small batch (stub HTTP).
func BenchmarkPush_SmallBatch(b *testing.B) {
	client, _ := New(Config{UpstreamURL: "http://stub:9090"})
	client.httpClient.Transport = newStubRT()
	defer client.Close()

	reg := prometheus.NewRegistry()
	for i := 0; i < 50; i++ {
		c := prometheus.NewCounter(prometheus.CounterOpts{Name: fmt.Sprintf("metric_%d", i)})
		c.Inc()
		reg.MustRegister(c)
	}

	ctx := context.Background()
	req := PushRequest{
		Gatherer:       reg,
		ExternalLabels: map[string]string{"job": "test"},
		Now:            func() time.Time { return time.Unix(1234, 0) },
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := client.Push(ctx, req); err != nil {
			b.Fatalf("Push failed: %v", err)
		}
	}
}

// BenchmarkPush_LargeBatch measures full Push with large batch requiring multiple flushes.
func BenchmarkPush_LargeBatch(b *testing.B) {
	client, _ := New(Config{UpstreamURL: "http://stub:9090"})
	client.httpClient.Transport = newStubRT()
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

	ctx := context.Background()
	req := PushRequest{
		Gatherer:          reg,
		MaxSeriesPerBatch: 500, // Force 2 batches
		ExternalLabels:    map[string]string{"cluster": "prod", "region": "us-east-1"},
		Now:               func() time.Time { return time.Unix(1234, 0) },
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := client.Push(ctx, req); err != nil {
			b.Fatalf("Push failed: %v", err)
		}
	}
}

// BenchmarkPush_WithRetries measures Push with simulated retry logic.
func BenchmarkPush_WithRetries(b *testing.B) {
	attemptCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attemptCount++
		// Fail first attempt, succeed second
		if attemptCount%2 == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, _ := New(Config{
		UpstreamURL: server.URL,
		Retry: &RetryConfig{
			MinWait:    time.Millisecond,
			MaxWait:    10 * time.Millisecond,
			MaxRetries: 3,
		},
	})
	defer client.Close()

	reg := prometheus.NewRegistry()
	for i := 0; i < 100; i++ {
		c := prometheus.NewCounter(prometheus.CounterOpts{Name: fmt.Sprintf("retry_metric_%d", i)})
		c.Inc()
		reg.MustRegister(c)
	}

	ctx := context.Background()
	req := PushRequest{
		Gatherer: reg,
		Now:      func() time.Time { return time.Unix(1234, 0) },
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := client.Push(ctx, req); err != nil {
			b.Fatalf("Push failed: %v", err)
		}
	}
}

// BenchmarkPush_Parallel measures concurrent Push operations.
func BenchmarkPush_Parallel(b *testing.B) {
	client, _ := New(Config{UpstreamURL: "http://stub:9090"})
	client.httpClient.Transport = newStubRT()
	defer client.Close()

	reg := prometheus.NewRegistry()
	for i := 0; i < 200; i++ {
		c := prometheus.NewCounter(prometheus.CounterOpts{Name: fmt.Sprintf("parallel_metric_%d", i)})
		c.Inc()
		reg.MustRegister(c)
	}

	ctx := context.Background()
	req := PushRequest{
		Gatherer:       reg,
		ExternalLabels: map[string]string{"job": "parallel_test"},
		Now:            func() time.Time { return time.Unix(1234, 0) },
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := client.Push(ctx, req); err != nil {
				b.Fatalf("Push failed: %v", err)
			}
		}
	})
}

// BenchmarkPush_Histograms measures Push with high series count from histograms.
func BenchmarkPush_Histograms(b *testing.B) {
	client, _ := New(Config{UpstreamURL: "http://stub:9090"})
	client.httpClient.Transport = newStubRT()
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

	ctx := context.Background()
	req := PushRequest{
		Gatherer:       reg,
		ExternalLabels: map[string]string{"service": "api"},
		Now:            func() time.Time { return time.Unix(1234, 0) },
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := client.Push(ctx, req); err != nil {
			b.Fatalf("Push failed: %v", err)
		}
	}
}
