package remotewrite

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/klauspost/compress/snappy"
	"github.com/prometheus/prometheus/prompb"
)

// PushTimeSeriesRequest contains the data for pushing pre-built time series.
type PushTimeSeriesRequest struct {
	// TenantID is the tenant to push metrics for.
	TenantID string

	// TimeSeries are the series to send.
	TimeSeries []prompb.TimeSeries

	// MaxBatchBytes is the target uncompressed batch size. Default: 3MB.
	MaxBatchBytes int

	// MaxSeriesPerBatch is the maximum number of series per batch. Default: 10000.
	MaxSeriesPerBatch int

	// ExtraHeaders are additional headers to forward.
	ExtraHeaders http.Header
}

// PushTimeSeries sends pre-built time series to the remote_write endpoint.
func (c *Client) PushTimeSeries(ctx context.Context, pr PushTimeSeriesRequest) error {
	if len(pr.TimeSeries) == 0 {
		return nil
	}

	maxBytes := pr.MaxBatchBytes
	if maxBytes <= 0 {
		maxBytes = 3 * 1024 * 1024
	}
	maxSeries := pr.MaxSeriesPerBatch
	if maxSeries <= 0 {
		maxSeries = 10000
	}

	estimatedBytes := 0
	batchStart := 0
	for i := range pr.TimeSeries {
		estimatedBytes += estimateSeriesBytes(&pr.TimeSeries[i])
		if (i-batchStart+1) >= maxSeries || estimatedBytes >= maxBytes {
			if err := c.sendTimeSeries(ctx, pr.TenantID, pr.TimeSeries[batchStart:i+1], pr.ExtraHeaders); err != nil {
				return err
			}
			batchStart = i + 1
			estimatedBytes = 0
		}
	}
	if batchStart < len(pr.TimeSeries) {
		return c.sendTimeSeries(ctx, pr.TenantID, pr.TimeSeries[batchStart:], pr.ExtraHeaders)
	}
	return nil
}

func (c *Client) sendTimeSeries(ctx context.Context, tenantID string, tss []prompb.TimeSeries, extraHeaders http.Header) error {
	req := prompb.WriteRequest{Timeseries: tss}

	pb := getProtoBuffer()
	defer putProtoBuffer(pb)

	if err := pb.Marshal(&req); err != nil {
		return fmt.Errorf("marshal failed: %w", err)
	}
	raw := pb.Bytes()

	maxEncoded := snappy.MaxEncodedLen(len(raw))
	compressed := getSnappyBuffer()
	defer putSnappyBuffer(compressed)

	if cap(*compressed) < maxEncoded {
		*compressed = make([]byte, maxEncoded)
	}
	encoded := snappy.Encode((*compressed)[:0], raw)

	fwReq := ForwardRequest{
		TenantID:           tenantID,
		BodyBytes:          encoded,
		ContentType:        "application/x-protobuf",
		ContentEncoding:    "snappy",
		RemoteWriteVersion: "0.1.0",
		ExtraHeaders:       extraHeaders,
	}

	resp, err := c.Forward(ctx, fwReq)
	if err != nil {
		return fmt.Errorf("forward failed: %w", err)
	}
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return fmt.Errorf("remote_write failed: status=%s body=%q", resp.Status, body)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return nil
}

func estimateSeriesBytes(ts *prompb.TimeSeries) int {
	size := 16
	for _, lbl := range ts.Labels {
		size += len(lbl.Name) + len(lbl.Value) + 2
	}
	size += len(ts.Samples) * 16
	return size
}
