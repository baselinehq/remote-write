package integration_test

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	remotewrite "github.com/baselinehq/remote-write"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func BenchmarkVictoriaMetricsPushTimeSeries(b *testing.B) {
	writeURL, cleanup := resolveVMWriteURL(b)
	defer cleanup()

	client, err := remotewrite.New(remotewrite.Config{UpstreamURL: writeURL})
	if err != nil {
		b.Fatalf("remotewrite.New: %v", err)
	}
	defer client.Close()

	// Warm up the connection and JIT so initial-iteration cost does not skew
	// the timed loop.
	warmup := buildSeries("rw_bench_warmup", 1, time.Now().UnixMilli())
	if err := client.PushTimeSeries(context.Background(), remotewrite.PushTimeSeriesRequest{TimeSeries: warmup}); err != nil {
		b.Fatalf("warmup PushTimeSeries: %v", err)
	}

	for _, n := range []int{1000, 10000} {
		n := n
		b.Run(fmt.Sprintf("%d_series", n), func(b *testing.B) {
			prefix := fmt.Sprintf("rw_bench_%d_%d", n, time.Now().UnixNano())
			baseTS := time.Now().UnixMilli()
			series := buildSeries(prefix, n, baseTS)

			// Estimate compressed payload size for SetBytes by encoding once
			// outside the timed loop. PushTimeSeries does the real compression
			// inside; this is only a reporting hint.
			encoded := encodedSize(b, series)
			b.SetBytes(int64(encoded))

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				ts := baseTS + int64(i+1)
				for j := range series {
					series[j].Samples[0].Timestamp = ts
				}
				if err := client.PushTimeSeries(context.Background(), remotewrite.PushTimeSeriesRequest{TimeSeries: series}); err != nil {
					b.Fatalf("PushTimeSeries: %v", err)
				}
			}
		})
	}
}

func resolveVMWriteURL(b *testing.B) (string, func()) {
	b.Helper()

	if writeURL := os.Getenv("REMOTEWRITE_VM_URL"); writeURL != "" {
		return writeURL, func() {}
	}
	if !envTrue(os.Getenv("REMOTEWRITE_VM_BENCH")) {
		b.Skip("set REMOTEWRITE_VM_BENCH=1 to start VictoriaMetrics via testcontainers, or REMOTEWRITE_VM_URL to use an existing endpoint")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        victoriaMetricsImage,
			ExposedPorts: []string{"8428/tcp"},
			Cmd:          []string{"-retentionPeriod=1d"},
			WaitingFor:   wait.ForHTTP("/health").WithPort("8428/tcp").WithStartupTimeout(60 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		b.Fatalf("start victoriametrics container: %v", err)
	}

	endpoint, err := container.Endpoint(context.Background(), "http")
	if err != nil {
		_ = container.Terminate(context.Background())
		b.Fatalf("container endpoint: %v", err)
	}
	return endpoint + "/api/v1/write", func() {
		_ = container.Terminate(context.Background())
	}
}

func envTrue(value string) bool {
	if value == "" {
		return false
	}
	v, err := strconv.ParseBool(value)
	if err == nil {
		return v
	}
	switch strings.ToLower(value) {
	case "yes", "y", "on":
		return true
	default:
		return false
	}
}

// encodedSize returns the size of the snappy-compressed remote_write protobuf
// payload for series. Used only as a SetBytes reporting hint, not in the
// timed loop.
func encodedSize(b *testing.B, series []prompb.TimeSeries) int {
	b.Helper()
	raw, err := (&prompb.WriteRequest{Timeseries: series}).Marshal()
	if err != nil {
		b.Fatalf("marshal WriteRequest: %v", err)
	}
	return len(snappy.Encode(nil, raw))
}

func buildSeries(metricPrefix string, n int, baseTS int64) []prompb.TimeSeries {
	series := make([]prompb.TimeSeries, n)
	for i := range series {
		series[i] = prompb.TimeSeries{
			Labels: []prompb.Label{
				{Name: "__name__", Value: metricPrefix},
				{Name: "job", Value: "remotewrite-bench"},
				{Name: "instance", Value: fmt.Sprintf("instance-%05d", i)},
				{Name: "shard", Value: fmt.Sprintf("%02d", i%16)},
			},
			Samples: []prompb.Sample{{Value: float64(i), Timestamp: baseTS}},
		}
	}
	return series
}
