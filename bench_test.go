package remotewrite

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strconv"
	"testing"

	"github.com/prometheus/prometheus/prompb"
)

func BenchmarkPushTimeSeries_BatchingAndEncode(b *testing.B) {
	client, err := New(Config{UpstreamURL: "http://example"})
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	client.httpClient.Transport = &discardTransport{}

	series := make([]prompb.TimeSeries, 1000)
	for i := range series {
		series[i] = prompb.TimeSeries{
			Labels: []prompb.Label{
				{Name: "__name__", Value: "bench_metric"},
				{Name: "id", Value: strconv.Itoa(i)},
			},
			Samples: []prompb.Sample{
				{Value: 1, Timestamp: int64(i + 1)},
			},
		}
	}

	req := PushTimeSeriesRequest{
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

type discardTransport struct{}

func (d *discardTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	_, _ = io.Copy(io.Discard, req.Body)
	_ = req.Body.Close()
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(bytes.NewReader(nil)),
		Header:     make(http.Header),
	}, nil
}
