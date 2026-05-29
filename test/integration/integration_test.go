// Package integration runs end-to-end remote_write tests against a
// VictoriaMetrics instance started via testcontainers.
package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"

	remotewrite "github.com/baselinehq/remote-write"
	"github.com/prometheus/prometheus/prompb"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const victoriaMetricsImage = "victoriametrics/victoria-metrics:v1.130.0"

func TestVictoriaMetricsIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in -short mode")
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
		t.Fatalf("start victoriametrics container: %v", err)
	}
	t.Cleanup(func() {
		_ = container.Terminate(context.Background())
	})

	endpoint, err := container.Endpoint(ctx, "http")
	if err != nil {
		t.Fatalf("container endpoint: %v", err)
	}
	writeURL := endpoint + "/api/v1/write"
	queryURL := endpoint + "/api/v1/query"

	client, err := remotewrite.New(remotewrite.Config{UpstreamURL: writeURL})
	if err != nil {
		t.Fatalf("remotewrite.New: %v", err)
	}
	defer client.Close()

	metric := fmt.Sprintf("rw_integration_%d", time.Now().UnixNano())
	want := 42.0
	sampleTS := time.Now().Add(-10 * time.Second).UnixMilli()
	series := []prompb.TimeSeries{{
		Labels: []prompb.Label{
			{Name: "__name__", Value: metric},
			{Name: "job", Value: "remotewrite-integration"},
		},
		Samples: []prompb.Sample{{Value: want, Timestamp: sampleTS}},
	}}

	if err := client.PushTimeSeries(ctx, remotewrite.PushTimeSeriesRequest{TimeSeries: series}); err != nil {
		t.Fatalf("PushTimeSeries: %v", err)
	}

	if err := waitForSample(ctx, queryURL, metric, want); err != nil {
		t.Fatalf("sample not visible: %v", err)
	}
}

func waitForSample(ctx context.Context, queryURL, metric string, want float64) error {
	deadline := time.Now().Add(15 * time.Second)
	httpClient := &http.Client{Timeout: 2 * time.Second}

	var lastErr error
	for time.Now().Before(deadline) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, queryURL+"?query="+url.QueryEscape(metric), nil)
		if err != nil {
			return err
		}
		resp, err := httpClient.Do(req)
		if err != nil {
			lastErr = err
		} else {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				found, got, perr := parseQueryResult(body)
				if perr != nil {
					lastErr = perr
				} else if found {
					if got != want {
						return fmt.Errorf("value mismatch: got %v want %v", got, want)
					}
					return nil
				}
			} else {
				lastErr = fmt.Errorf("query %s returned %s: %s", queryURL, resp.Status, body)
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
	if lastErr != nil {
		return lastErr
	}
	return fmt.Errorf("sample %s not found before deadline", metric)
}

func parseQueryResult(body []byte) (found bool, value float64, err error) {
	var resp struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Value [2]any `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return false, 0, err
	}
	if resp.Status != "success" || len(resp.Data.Result) == 0 {
		return false, 0, nil
	}
	s, ok := resp.Data.Result[0].Value[1].(string)
	if !ok {
		return false, 0, fmt.Errorf("unexpected value type %T", resp.Data.Result[0].Value[1])
	}
	if _, err := fmt.Sscanf(s, "%g", &value); err != nil {
		return false, 0, err
	}
	return true, value, nil
}
