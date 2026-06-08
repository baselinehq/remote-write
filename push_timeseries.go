package remotewrite

import (
	"context"
	"net/http"

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

	// ExtraHeaders are additional headers to forward. It must not contain
	// protocol headers or headers managed by client configuration.
	ExtraHeaders http.Header
}

func pushTimeSeries(ctx context.Context, pr PushTimeSeriesRequest, deliver func(context.Context, DurableRequest) error) error {
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
			if err := sendTimeSeriesBatch(ctx, pr.TenantID, pr.TimeSeries[batchStart:i+1], pr.ExtraHeaders, deliver); err != nil {
				return err
			}
			batchStart = i + 1
			estimatedBytes = 0
		}
	}
	if batchStart < len(pr.TimeSeries) {
		return sendTimeSeriesBatch(ctx, pr.TenantID, pr.TimeSeries[batchStart:], pr.ExtraHeaders, deliver)
	}
	return nil
}

func sendTimeSeriesBatch(ctx context.Context, tenantID string, tss []prompb.TimeSeries, extraHeaders http.Header, deliver func(context.Context, DurableRequest) error) error {
	return withEncodedTimeSeries(tss, func(encoded []byte) error {
		return deliver(ctx, DurableRequest{
			TenantID:           tenantID,
			BodyBytes:          encoded,
			ContentType:        defaultRemoteWriteContentType,
			ContentEncoding:    defaultRemoteWriteContentEncoding,
			RemoteWriteVersion: defaultRemoteWriteVersion,
			ExtraHeaders:       extraHeaders,
		})
	})
}

func estimateSeriesBytes(ts *prompb.TimeSeries) int {
	size := 16
	for _, lbl := range ts.Labels {
		size += len(lbl.Name) + len(lbl.Value) + 2
	}
	size += len(ts.Samples) * 16
	return size
}

// PushTimeSeries sends pre-built time series to the remote_write endpoint.
func (c *Client) PushTimeSeries(ctx context.Context, pr PushTimeSeriesRequest) error {
	return pushTimeSeries(ctx, pr, c.sendEncoded)
}
