package remotewrite

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/klauspost/compress/snappy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPush_NilGatherer verifies that Push returns an error for nil Gatherer.
func TestPush_NilGatherer(t *testing.T) {
	client, err := New(Config{UpstreamURL: "http://stub:9090"})
	require.NoError(t, err)
	defer client.Close()

	err = client.Push(context.Background(), PushRequest{
		Gatherer: nil,
		TenantID: "test",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "gatherer is required")
}

// TestPush_Integration verifies the end-to-end flow with a mock server.
func TestPush_Integration(t *testing.T) {
	// 1. Setup Mock Server
	var receivedReq prompb.WriteRequest
	var receivedBody []byte

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify headers
		require.Equal(t, "application/x-protobuf", r.Header.Get("Content-Type"))
		require.Equal(t, "snappy", r.Header.Get("Content-Encoding"))
		require.Equal(t, "0.1.0", r.Header.Get("X-Prometheus-Remote-Write-Version"))
		require.Equal(t, "test-tenant", r.Header.Get("X-Scope-OrgID"))

		// Read Body
		var err error
		receivedBody, err = io.ReadAll(r.Body)
		require.NoError(t, err)

		// Decode Snappy
		decoded, err := snappy.Decode(nil, receivedBody)
		require.NoError(t, err)

		// Unmarshal Protobuf
		err = proto.Unmarshal(decoded, &receivedReq)
		require.NoError(t, err)

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// 2. Setup Client
	client, err := New(Config{
		UpstreamURL:  server.URL,
		TenantHeader: "X-Scope-OrgID",
	})
	require.NoError(t, err)
	defer client.Close()

	// 3. Setup Metrics
	reg := prometheus.NewRegistry()
	cnt := prometheus.NewCounter(prometheus.CounterOpts{
		Name:        "test_counter",
		Help:        "A test counter",
		ConstLabels: prometheus.Labels{"const": "label"},
	})
	cnt.Inc()
	reg.MustRegister(cnt)

	// 4. Push
	err = client.Push(context.Background(), PushRequest{
		TenantID: "test-tenant",
		Gatherer: reg,
		ExternalLabels: map[string]string{
			"env": "testing",
		},
		Now: func() time.Time { return time.Unix(1000, 0) },
	})
	require.NoError(t, err)

	// 5. Verify Results
	require.Len(t, receivedReq.Timeseries, 1)
	ts := receivedReq.Timeseries[0]

	// Check Samples
	require.Len(t, ts.Samples, 1)
	assert.Equal(t, float64(1), ts.Samples[0].Value)
	assert.Equal(t, int64(1000000), ts.Samples[0].Timestamp) // 1000s -> 1000000ms

	// Check Labels (sorted)
	expectedLabels := []prompb.Label{
		{Name: "__name__", Value: "test_counter"},
		{Name: "const", Value: "label"},
		{Name: "env", Value: "testing"},
	}
	assert.Equal(t, expectedLabels, ts.Labels)
}

// TestPush_Batching verifies that multiple batches are sent if limits are exceeded.
func TestPush_Batching(t *testing.T) {
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, err := New(Config{UpstreamURL: server.URL})
	require.NoError(t, err)
	defer client.Close()

	reg := prometheus.NewRegistry()
	// Create enough metrics to trigger 2 batches
	// MaxSeriesPerBatch = 2. Metrics = 3.
	for i := 0; i < 3; i++ {
		c := prometheus.NewCounter(prometheus.CounterOpts{
			Name: fmt.Sprintf("metric_%d", i),
		})
		c.Inc()
		reg.MustRegister(c)
	}

	err = client.Push(context.Background(), PushRequest{
		Gatherer:          reg,
		MaxSeriesPerBatch: 2,
	})
	require.NoError(t, err)
	assert.Equal(t, 2, requestCount, "Should have sent 2 batches")
}

// TestEncoder_Histogram verifies histogram encoding correctness.
func TestEncoder_Histogram(t *testing.T) {
	// Setup
	tsBuf := getTimeSeriesSlice()
	defer putTimeSeriesSlice(tsBuf)
	var pooledLabels []*[]prompb.Label
	var pooledSamples []*[]prompb.Sample
	defer func() {
		for _, l := range pooledLabels {
			putLabelSlice(l)
		}
		for _, s := range pooledSamples {
			putSampleSlice(s)
		}
	}()

	// Metric
	hist := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "http_req_dur",
		Buckets: []float64{0.1, 0.5},
	})
	hist.Observe(0.2) // goes into 0.5 bucket

	// Gather
	reg := prometheus.NewRegistry()
	reg.MustRegister(hist)
	mfs, _ := reg.Gather()
	require.Len(t, mfs, 1)

	// Convert
	convertMetricFamily(mfs[0], tsBuf, []prompb.Label{{Name: "cluster", Value: "A"}}, 1234, &pooledLabels, &pooledSamples)

	// Verify
	// Expect:
	// 1. bucket{le="0.1"} val=0
	// 2. bucket{le="0.5"} val=1
	// 3. bucket{le="+Inf"} val=1
	// 4. sum val=0.2
	// 5. count val=1
	require.Len(t, *tsBuf, 5)

	// Check bucket le="0.1" (value 0)
	assert.Equal(t, "http_req_dur_bucket", (*tsBuf)[0].Labels[0].Value) // __name__
	assert.Equal(t, "0.1", (*tsBuf)[0].Labels[2].Value)                 // le (after cluster)
	assert.Equal(t, float64(0), (*tsBuf)[0].Samples[0].Value)

	// Check bucket le="0.5" (value 1)
	assert.Equal(t, "0.5", (*tsBuf)[1].Labels[2].Value)
	assert.Equal(t, float64(1), (*tsBuf)[1].Samples[0].Value)

	// Check +Inf
	assert.Equal(t, "+Inf", (*tsBuf)[2].Labels[2].Value)
	assert.Equal(t, float64(1), (*tsBuf)[2].Samples[0].Value)

	// Check Sum
	assert.Equal(t, "http_req_dur_sum", (*tsBuf)[3].Labels[0].Value)
	assert.Equal(t, float64(0.2), (*tsBuf)[3].Samples[0].Value)

	// Check Count
	assert.Equal(t, "http_req_dur_count", (*tsBuf)[4].Labels[0].Value)
	assert.Equal(t, float64(1), (*tsBuf)[4].Samples[0].Value)
}
