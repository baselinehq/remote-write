package remotewrite

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/klauspost/compress/snappy"
	"github.com/prometheus/prometheus/prompb"
)

func TestPushTimeSeries_SplitsByMaxSeriesPerBatch(t *testing.T) {
	rec := &recordingRoundTripper{}
	client, err := New(Config{UpstreamURL: "http://example"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	client.httpClient.Transport = rec

	series := []prompb.TimeSeries{
		makeSeries("m1", "a"),
		makeSeries("m2", "b"),
		makeSeries("m3", "c"),
		makeSeries("m4", "d"),
		makeSeries("m5", "e"),
	}

	err = client.PushTimeSeries(context.Background(), PushTimeSeriesRequest{
		TimeSeries:        series,
		MaxSeriesPerBatch: 2,
		MaxBatchBytes:     1 << 20,
	})
	if err != nil {
		t.Fatalf("PushTimeSeries: %v", err)
	}

	got := rec.Counts()
	want := []int{2, 2, 1}
	if !equalInts(got, want) {
		t.Fatalf("batch counts=%v, want %v", got, want)
	}
}

func TestPushTimeSeries_SplitsByMaxBatchBytes(t *testing.T) {
	rec := &recordingRoundTripper{}
	client, err := New(Config{UpstreamURL: "http://example"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	client.httpClient.Transport = rec

	large := strings.Repeat("x", 100)
	series := []prompb.TimeSeries{
		makeSeries("m1", large),
		makeSeries("m2", large),
		makeSeries("m3", large),
	}

	err = client.PushTimeSeries(context.Background(), PushTimeSeriesRequest{
		TimeSeries:        series,
		MaxSeriesPerBatch: 100,
		MaxBatchBytes:     100,
	})
	if err != nil {
		t.Fatalf("PushTimeSeries: %v", err)
	}

	got := rec.Counts()
	want := []int{1, 1, 1}
	if !equalInts(got, want) {
		t.Fatalf("batch counts=%v, want %v", got, want)
	}
}

type recordingRoundTripper struct {
	mu     sync.Mutex
	counts []int
}

func (r *recordingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	_ = req.Body.Close()

	raw, err := snappy.Decode(nil, body)
	if err != nil {
		return nil, err
	}
	var wr prompb.WriteRequest
	if err := wr.Unmarshal(raw); err != nil {
		return nil, err
	}

	r.mu.Lock()
	r.counts = append(r.counts, len(wr.Timeseries))
	r.mu.Unlock()

	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(bytes.NewReader(nil)),
		Header:     make(http.Header),
	}, nil
}

func (r *recordingRoundTripper) Counts() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]int, len(r.counts))
	copy(out, r.counts)
	return out
}

func makeSeries(name, labelValue string) prompb.TimeSeries {
	return prompb.TimeSeries{
		Labels: []prompb.Label{
			{Name: "__name__", Value: name},
			{Name: "label", Value: labelValue},
		},
		Samples: []prompb.Sample{
			{Value: 1, Timestamp: 1},
		},
	}
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
