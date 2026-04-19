package remotewrite

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/snappy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDurableEnvelopeRoundTripDeterministicHeaders(t *testing.T) {
	reqA := DurableRequest{
		TenantID:           "tenant-a",
		BodyBytes:          []byte("payload"),
		ContentType:        defaultRemoteWriteContentType,
		ContentEncoding:    defaultRemoteWriteContentEncoding,
		RemoteWriteVersion: defaultRemoteWriteVersion,
		ExtraHeaders: http.Header{
			"z-last":   {"z"},
			"x-header": {"1", "2"},
			"A-First":  {"a"},
		},
	}
	reqB := DurableRequest{
		TenantID:           "tenant-a",
		BodyBytes:          []byte("payload"),
		ContentType:        defaultRemoteWriteContentType,
		ContentEncoding:    defaultRemoteWriteContentEncoding,
		RemoteWriteVersion: defaultRemoteWriteVersion,
		ExtraHeaders: http.Header{
			"a-first":  {"a"},
			"X-Header": {"1", "2"},
			"Z-Last":   {"z"},
		},
	}

	encodedA, err := encodeDurableRequestEnvelope(reqA)
	require.NoError(t, err)
	encodedB, err := encodeDurableRequestEnvelope(reqB)
	require.NoError(t, err)
	require.Equal(t, encodedA, encodedB)

	decoded, err := decodeDurableRequestEnvelope(encodedA)
	require.NoError(t, err)
	assert.Equal(t, reqA.TenantID, decoded.TenantID)
	assert.Equal(t, reqA.BodyBytes, decoded.BodyBytes)
	assert.Equal(t, reqA.ContentType, decoded.ContentType)
	assert.Equal(t, reqA.ContentEncoding, decoded.ContentEncoding)
	assert.Equal(t, reqA.RemoteWriteVersion, decoded.RemoteWriteVersion)
	assert.Equal(t, http.Header{
		"A-First":  {"a"},
		"X-Header": {"1", "2"},
		"Z-Last":   {"z"},
	}, decoded.ExtraHeaders)

	encodedA[len(encodedA)-1] = 'X'
	assert.Equal(t, byte('X'), decoded.BodyBytes[len(decoded.BodyBytes)-1])
}

func TestDurableRecordRefRoundTrip(t *testing.T) {
	ref := encodeDurableRecordRef(42)
	recordID, err := decodeDurableRecordRef(ref)
	require.NoError(t, err)
	assert.Equal(t, uint64(42), recordID)
}

func TestDurableClientEnqueueAndDrainSuccess(t *testing.T) {
	receivedCh := make(chan durableHTTPPayload, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload := durableHTTPPayload{
			TenantID: r.Header.Get("X-Scope-OrgID"),
			Headers:  r.Header.Clone(),
		}
		payload.Body, _ = io.ReadAll(r.Body)
		receivedCh <- payload
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	dc, _, runErrCh := newRunningDurableClient(t, durableTestConfig(t, server.URL))
	defer closeRunningDurableClient(t, dc, runErrCh)

	err := dc.Enqueue(context.Background(), DurableRequest{
		TenantID:           "tenant-1",
		BodyBytes:          []byte("payload-1"),
		ContentType:        defaultRemoteWriteContentType,
		ContentEncoding:    defaultRemoteWriteContentEncoding,
		RemoteWriteVersion: defaultRemoteWriteVersion,
		ExtraHeaders:       http.Header{"X-Test": {"value"}},
	})
	require.NoError(t, err)

	select {
	case received := <-receivedCh:
		assert.Equal(t, "tenant-1", received.TenantID)
		assert.Equal(t, []byte("payload-1"), received.Body)
		assert.Equal(t, "value", received.Headers.Get("X-Test"))
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for durable payload")
	}

	require.Eventually(t, func() bool {
		return testutil.ToFloat64(dc.metrics.sentTotal) == 1 &&
			testutil.ToFloat64(dc.metrics.enqueuedTotal) == 1 &&
			testutil.ToFloat64(dc.metrics.droppedTotal) == 0 &&
			dc.pendingBytes.Load() == 0
	}, time.Second, 10*time.Millisecond)
}

func TestDurableClientRestartDurability(t *testing.T) {
	receivedCh := make(chan durableHTTPPayload, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload := durableHTTPPayload{}
		payload.Body, _ = io.ReadAll(r.Body)
		receivedCh <- payload
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	rootDir := t.TempDir()
	cfg := durableTestConfigWithDir(server.URL, rootDir)

	dc, err := NewDurable(cfg)
	require.NoError(t, err)

	err = dc.Enqueue(context.Background(), DurableRequest{
		BodyBytes:          []byte("restart-me"),
		ContentType:        defaultRemoteWriteContentType,
		ContentEncoding:    defaultRemoteWriteContentEncoding,
		RemoteWriteVersion: defaultRemoteWriteVersion,
	})
	require.NoError(t, err)
	require.NoError(t, dc.Close())

	restarted, _, runErrCh := newRunningDurableClient(t, cfg)
	defer closeRunningDurableClient(t, restarted, runErrCh)

	select {
	case received := <-receivedCh:
		assert.Equal(t, []byte("restart-me"), received.Body)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for restarted durable payload")
	}
}

func TestDurableClientRecoverySeedsQueueEvenWhenPersistenceDisabled(t *testing.T) {
	var (
		mu       sync.Mutex
		received []string
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		received = append(received, string(body))
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	rootDir := t.TempDir()
	require.NoError(t, writeDurableRecordFixture(rootDir, "durable", 1, DurableRequest{
		BodyBytes:          []byte("one"),
		ContentType:        defaultRemoteWriteContentType,
		ContentEncoding:    defaultRemoteWriteContentEncoding,
		RemoteWriteVersion: defaultRemoteWriteVersion,
	}))
	require.NoError(t, writeDurableRecordFixture(rootDir, "durable", 2, DurableRequest{
		BodyBytes:          []byte("two"),
		ContentType:        defaultRemoteWriteContentType,
		ContentEncoding:    defaultRemoteWriteContentEncoding,
		RemoteWriteVersion: defaultRemoteWriteVersion,
	}))

	cfg := durableTestConfigWithDir(server.URL, rootDir)
	cfg.MaxInMemoryBlocks = 1
	cfg.DisablePersistence = true

	dc, _, runErrCh := newRunningDurableClient(t, cfg)
	defer closeRunningDurableClient(t, dc, runErrCh)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(received) == 2
	}, 5*time.Second, 10*time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{"one", "two"}, received)
}

func TestDurableClientRetryableFailureThenSuccess(t *testing.T) {
	var attempts atomic.Int32
	receivedCh := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		receivedCh <- struct{}{}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	dc, _, runErrCh := newRunningDurableClient(t, durableTestConfig(t, server.URL))
	defer closeRunningDurableClient(t, dc, runErrCh)

	err := dc.Enqueue(context.Background(), DurableRequest{
		BodyBytes:          []byte("retry-once"),
		ContentType:        defaultRemoteWriteContentType,
		ContentEncoding:    defaultRemoteWriteContentEncoding,
		RemoteWriteVersion: defaultRemoteWriteVersion,
	})
	require.NoError(t, err)

	select {
	case <-receivedCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for successful retry")
	}

	require.Eventually(t, func() bool {
		return attempts.Load() == 2 &&
			testutil.ToFloat64(dc.metrics.retryableFailuresTotal) == 1 &&
			testutil.ToFloat64(dc.metrics.sendFailuresTotal) == 1 &&
			testutil.ToFloat64(dc.metrics.sentTotal) == 1
	}, time.Second, 10*time.Millisecond)
}

func TestDurableClientRetryAfterHonored(t *testing.T) {
	var attempts atomic.Int32
	start := time.Now()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	dc, _, runErrCh := newRunningDurableClient(t, durableTestConfig(t, server.URL))
	defer closeRunningDurableClient(t, dc, runErrCh)

	err := dc.Enqueue(context.Background(), DurableRequest{
		BodyBytes:          []byte("retry-after"),
		ContentType:        defaultRemoteWriteContentType,
		ContentEncoding:    defaultRemoteWriteContentEncoding,
		RemoteWriteVersion: defaultRemoteWriteVersion,
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return testutil.ToFloat64(dc.metrics.sentTotal) == 1
	}, 5*time.Second, 10*time.Millisecond)

	assert.GreaterOrEqual(t, time.Since(start), time.Second)
}

func TestDurableClientRetriesHTTPClientTimeout(t *testing.T) {
	var attempts atomic.Int32
	receivedCh := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) == 1 {
			time.Sleep(100 * time.Millisecond)
			w.WriteHeader(http.StatusOK)
			return
		}
		_, _ = io.Copy(io.Discard, r.Body)
		receivedCh <- struct{}{}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := durableTestConfig(t, server.URL)
	cfg.Client.Timeout = 20 * time.Millisecond
	dc, _, runErrCh := newRunningDurableClient(t, cfg)
	defer closeRunningDurableClient(t, dc, runErrCh)

	err := dc.Enqueue(context.Background(), DurableRequest{
		BodyBytes:          []byte("timeout-retry"),
		ContentType:        defaultRemoteWriteContentType,
		ContentEncoding:    defaultRemoteWriteContentEncoding,
		RemoteWriteVersion: defaultRemoteWriteVersion,
	})
	require.NoError(t, err)

	select {
	case <-receivedCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for retry after client timeout")
	}

	require.Eventually(t, func() bool {
		return attempts.Load() >= 2 &&
			testutil.ToFloat64(dc.metrics.retryableFailuresTotal) == 1 &&
			testutil.ToFloat64(dc.metrics.sentTotal) == 1
	}, time.Second, 10*time.Millisecond)
}

func TestDurableClientPermanentFailureDropsPayload(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()

	dc, _, runErrCh := newRunningDurableClient(t, durableTestConfig(t, server.URL))
	defer closeRunningDurableClient(t, dc, runErrCh)

	err := dc.Enqueue(context.Background(), DurableRequest{
		BodyBytes:          []byte("drop-me"),
		ContentType:        defaultRemoteWriteContentType,
		ContentEncoding:    defaultRemoteWriteContentEncoding,
		RemoteWriteVersion: defaultRemoteWriteVersion,
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return testutil.ToFloat64(dc.metrics.permanentFailuresTotal) == 1 &&
			testutil.ToFloat64(dc.metrics.droppedTotal) == 1 &&
			testutil.ToFloat64(dc.metrics.sentTotal) == 0 &&
			testutil.ToFloat64(dc.metrics.sendFailuresTotal) == 1 &&
			testutil.ToFloat64(dc.metrics.retryableFailuresTotal) == 0 &&
			dc.inflight.Load() == 0 &&
			dc.pendingBytes.Load() == 0
	}, 5*time.Second, 10*time.Millisecond)
}

func TestDurableClientPreservesFIFOWithDefaultConcurrency(t *testing.T) {
	var (
		mu    sync.Mutex
		order []string
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		order = append(order, string(body))
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	dc, _, runErrCh := newRunningDurableClient(t, durableTestConfig(t, server.URL))
	defer closeRunningDurableClient(t, dc, runErrCh)

	for _, payload := range []string{"one", "two", "three"} {
		err := dc.Enqueue(context.Background(), DurableRequest{
			BodyBytes:          []byte(payload),
			ContentType:        defaultRemoteWriteContentType,
			ContentEncoding:    defaultRemoteWriteContentEncoding,
			RemoteWriteVersion: defaultRemoteWriteVersion,
		})
		require.NoError(t, err)
	}

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(order) == 3
	}, 5*time.Second, 10*time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{"one", "two", "three"}, order)
}

func TestDurableClientPushEventuallyDelivers(t *testing.T) {
	requestCh := make(chan prompb.WriteRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		requestCh <- decodeWriteRequest(t, body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	dc, _, runErrCh := newRunningDurableClient(t, durableTestConfig(t, server.URL))
	defer closeRunningDurableClient(t, dc, runErrCh)

	reg := prometheus.NewRegistry()
	counter := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "durable_push_counter_total",
		Help: "Counter for durable push tests",
	})
	counter.Inc()
	reg.MustRegister(counter)

	err := dc.Push(context.Background(), PushRequest{
		TenantID: "tenant-push",
		Gatherer: reg,
		ExternalLabels: map[string]string{
			"env": "test",
		},
		Now: func() time.Time { return time.Unix(123, 0) },
	})
	require.NoError(t, err)

	select {
	case req := <-requestCh:
		require.Len(t, req.Timeseries, 1)
		assert.Equal(t, "durable_push_counter_total", req.Timeseries[0].Labels[0].Value)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for durable push request")
	}
}

func TestDurableClientPushTimeSeriesEventuallyDelivers(t *testing.T) {
	requestCh := make(chan prompb.WriteRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		requestCh <- decodeWriteRequest(t, body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	dc, _, runErrCh := newRunningDurableClient(t, durableTestConfig(t, server.URL))
	defer closeRunningDurableClient(t, dc, runErrCh)

	series := []prompb.TimeSeries{
		makeSeries("durable_ts_one", "a"),
		makeSeries("durable_ts_two", "b"),
	}
	err := dc.PushTimeSeries(context.Background(), PushTimeSeriesRequest{
		TenantID:   "tenant-series",
		TimeSeries: series,
	})
	require.NoError(t, err)

	select {
	case req := <-requestCh:
		require.Len(t, req.Timeseries, 2)
		assert.Equal(t, series, req.Timeseries)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for durable PushTimeSeries request")
	}
}

func TestDurableClientQueueBlockedSurfaced(t *testing.T) {
	registry := prometheus.NewRegistry()
	dc, err := NewDurable(DurableConfig{
		Client: Config{
			UpstreamURL: serverURLForQueueBlockedTest(),
			Retry: &RetryConfig{
				MinWait: 5 * time.Millisecond,
				MaxWait: 20 * time.Millisecond,
			},
		},
		QueueDir:           t.TempDir(),
		QueueName:          "blocked",
		MaxInMemoryBlocks:  1,
		DisablePersistence: true,
		Registerer:         registry,
	})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, dc.Close())
	}()

	first := DurableRequest{
		BodyBytes:          []byte("first"),
		ContentType:        defaultRemoteWriteContentType,
		ContentEncoding:    defaultRemoteWriteContentEncoding,
		RemoteWriteVersion: defaultRemoteWriteVersion,
	}
	require.NoError(t, dc.Enqueue(context.Background(), first))

	err = dc.Enqueue(context.Background(), DurableRequest{
		BodyBytes:          []byte("second"),
		ContentType:        defaultRemoteWriteContentType,
		ContentEncoding:    defaultRemoteWriteContentEncoding,
		RemoteWriteVersion: defaultRemoteWriteVersion,
	})
	require.ErrorIs(t, err, ErrDurableQueueBlocked)
	assert.Equal(t, float64(1), testutil.ToFloat64(dc.metrics.queueBlocked))
}

func TestDurableClientQuarantinesCorruptedRecordAndContinues(t *testing.T) {
	receivedCh := make(chan []byte, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		receivedCh <- body
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	rootDir := t.TempDir()
	recordsDir := filepath.Join(rootDir, "durable", "records")
	require.NoError(t, os.MkdirAll(recordsDir, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(recordsDir, recordFilename(1)), []byte("not-an-envelope"), 0o600))
	require.NoError(t, writeDurableRecordFixture(rootDir, "durable", 2, DurableRequest{
		BodyBytes:          []byte("valid-after-corrupt"),
		ContentType:        defaultRemoteWriteContentType,
		ContentEncoding:    defaultRemoteWriteContentEncoding,
		RemoteWriteVersion: defaultRemoteWriteVersion,
	}))

	dc, _, runErrCh := newRunningDurableClient(t, durableTestConfigWithDir(server.URL, rootDir))
	defer closeRunningDurableClient(t, dc, runErrCh)

	select {
	case body := <-receivedCh:
		assert.Equal(t, []byte("valid-after-corrupt"), body)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for valid record after corrupt record")
	}

	require.Eventually(t, func() bool {
		return testutil.ToFloat64(dc.metrics.corruptRecordsTotal) == 1 &&
			testutil.ToFloat64(dc.metrics.droppedTotal) == 1 &&
			dc.pendingBytes.Load() == 0
	}, time.Second, 10*time.Millisecond)

	corruptEntries, err := os.ReadDir(filepath.Join(rootDir, "durable", "corrupt"))
	require.NoError(t, err)
	require.NotEmpty(t, corruptEntries)
}

func TestDurableClientRejectsSecondClientForSameSpool(t *testing.T) {
	cfg := durableTestConfigWithDir(serverURLForQueueBlockedTest(), t.TempDir())

	dc, err := NewDurable(cfg)
	require.NoError(t, err)

	_, err = NewDurable(cfg)
	require.ErrorIs(t, err, ErrDurableSpoolLocked)

	require.NoError(t, dc.Close())

	reopened, err := NewDurable(cfg)
	require.NoError(t, err)
	require.NoError(t, reopened.Close())
}

type durableHTTPPayload struct {
	TenantID string
	Headers  http.Header
	Body     []byte
}

func durableTestConfig(t *testing.T, serverURL string) DurableConfig {
	return durableTestConfigWithDir(serverURL, t.TempDir())
}

func durableTestConfigWithDir(serverURL, dir string) DurableConfig {
	return DurableConfig{
		Client: Config{
			UpstreamURL:  serverURL,
			TenantHeader: "X-Scope-OrgID",
			Retry: &RetryConfig{
				MinWait: 5 * time.Millisecond,
				MaxWait: 20 * time.Millisecond,
			},
		},
		QueueDir:        dir,
		QueueName:       "durable",
		Registerer:      prometheus.NewRegistry(),
		SendConcurrency: 1,
	}
}

func newRunningDurableClient(t *testing.T, cfg DurableConfig) (*DurableClient, *prometheus.Registry, <-chan error) {
	t.Helper()

	var registry *prometheus.Registry
	if cfg.Registerer == nil {
		registry = prometheus.NewRegistry()
		cfg.Registerer = registry
	} else {
		var ok bool
		registry, ok = cfg.Registerer.(*prometheus.Registry)
		require.True(t, ok, "test helper expects *prometheus.Registry")
	}

	dc, err := NewDurable(cfg)
	require.NoError(t, err)

	errCh := make(chan error, 1)
	go func() {
		errCh <- dc.Run(context.Background())
	}()

	return dc, registry, errCh
}

func closeRunningDurableClient(t *testing.T, dc *DurableClient, runErrCh <-chan error) {
	t.Helper()
	require.NoError(t, dc.Close())
	select {
	case err := <-runErrCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			require.NoError(t, err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for durable client shutdown")
	}
}

func decodeWriteRequest(t *testing.T, compressed []byte) prompb.WriteRequest {
	t.Helper()
	raw, err := snappy.Decode(nil, compressed)
	require.NoError(t, err)

	var req prompb.WriteRequest
	require.NoError(t, req.Unmarshal(raw))
	return req
}

func writeDurableRecordFixture(rootDir, queueName string, recordID uint64, req DurableRequest) error {
	recordsDir := filepath.Join(rootDir, queueName, "records")
	if err := os.MkdirAll(recordsDir, 0o700); err != nil {
		return err
	}
	envelope, err := encodeDurableRequestEnvelope(req)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(recordsDir, recordFilename(recordID)), envelope, 0o600)
}

func serverURLForQueueBlockedTest() string {
	return "http://127.0.0.1:1"
}
