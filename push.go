package remotewrite

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/relabel"
	"github.com/prometheus/prometheus/prompb"
)

// PushRequest contains the data for a push operation.
type PushRequest struct {
	// TenantID is the tenant to push metrics for.
	TenantID string

	// Gatherer is the source of metrics.
	Gatherer prometheus.Gatherer

	// ExternalLabels are labels to add to every metric if not present.
	ExternalLabels map[string]string

	// WriteRelabelConfigs are relabeling rules applied before sending.
	// These can be used to filter, modify, or drop metrics.
	WriteRelabelConfigs []relabel.Config

	// Now returns the current time. If nil, time.Now() is used.
	Now func() time.Time

	// MaxBatchBytes is the target uncompressed batch size. Default: 3MB.
	// This is soft limit; a single large metric family may exceed it.
	MaxBatchBytes int

	// MaxSeriesPerBatch is the maximum number of series per batch. Default: 10000.
	MaxSeriesPerBatch int

	// ExtraHeaders are additional headers to forward.
	ExtraHeaders http.Header
}

func pushGathered(ctx context.Context, pr PushRequest, deliver func(context.Context, DurableRequest) error) error {
	if pr.Gatherer == nil {
		return fmt.Errorf("gatherer is required")
	}

	// Validate relabel configs
	if len(pr.WriteRelabelConfigs) > 0 {
		for i := range pr.WriteRelabelConfigs {
			// Set the validation scheme for proper label name validation
			pr.WriteRelabelConfigs[i].NameValidationScheme = model.UTF8Validation
			if err := pr.WriteRelabelConfigs[i].Validate(model.UTF8Validation); err != nil {
				return fmt.Errorf("invalid write_relabel_configs: %w", err)
			}
		}
	}

	mfs, err := pr.Gatherer.Gather()
	if err != nil {
		return fmt.Errorf("gather failed: %w", err)
	}
	if len(mfs) == 0 {
		return nil
	}

	now := time.Now()
	if pr.Now != nil {
		now = pr.Now()
	}
	nowMs := now.UnixMilli()

	maxBytes := pr.MaxBatchBytes
	if maxBytes <= 0 {
		maxBytes = 3 * 1024 * 1024 // 3MB uncompressed default
	}
	maxSeries := pr.MaxSeriesPerBatch
	if maxSeries <= 0 {
		maxSeries = 10000
	}

	var extLabels []prompb.Label
	if len(pr.ExternalLabels) > 0 {
		extLabels = make([]prompb.Label, 0, len(pr.ExternalLabels))
		for k, v := range pr.ExternalLabels {
			extLabels = append(extLabels, prompb.Label{Name: k, Value: v})
		}
		sort.Slice(extLabels, func(i, j int) bool {
			return extLabels[i].Name < extLabels[j].Name
		})
	}

	// Batch and Send
	tsBuf := getTimeSeriesSlice()
	defer putTimeSeriesSlice(tsBuf)

	// Track pooled label and sample slices for cleanup after each batch.
	pooledLabels := make([]*[]prompb.Label, 0, 256)
	pooledSamples := make([]*[]prompb.Sample, 0, 256)

	// Ensure cleanup happens even on error paths
	cleanup := func() {
		for _, lbls := range pooledLabels {
			putLabelSlice(lbls)
		}
		pooledLabels = pooledLabels[:0]
		for _, smpls := range pooledSamples {
			putSampleSlice(smpls)
		}
		pooledSamples = pooledSamples[:0]
		*tsBuf = (*tsBuf)[:0]
	}
	defer cleanup()

	flush := func() error {
		if len(*tsBuf) == 0 {
			return nil
		}
		err := withEncodedTimeSeries(*tsBuf, func(encoded []byte) error {
			return deliver(ctx, DurableRequest{
				TenantID:           pr.TenantID,
				BodyBytes:          encoded,
				ContentType:        defaultRemoteWriteContentType,
				ContentEncoding:    defaultRemoteWriteContentEncoding,
				RemoteWriteVersion: defaultRemoteWriteVersion,
				ExtraHeaders:       pr.ExtraHeaders,
			})
		})
		if err != nil {
			return err
		}

		// Reset for next batch
		cleanup()
		return nil
	}

	// Process metric families and batch
	estimatedBytes := 0
	for _, mf := range mfs {
		// Check for context cancellation between batches
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		startIdx := len(*tsBuf)

		convertMetricFamily(mf, tsBuf, extLabels, nowMs, &pooledLabels, &pooledSamples)

		// Apply write_relabel_configs to filter/modify series
		if len(pr.WriteRelabelConfigs) > 0 {
			writeIdx := startIdx
			sb := labels.NewScratchBuilder(0)
			cfgPtrs := make([]*relabel.Config, len(pr.WriteRelabelConfigs))
			for i := range pr.WriteRelabelConfigs {
				cfgPtrs[i] = &pr.WriteRelabelConfigs[i]
			}

			for i := startIdx; i < len(*tsBuf); i++ {
				ts := &(*tsBuf)[i]

				// Convert prompb.Label to labels.Labels using ScratchBuilder
				sb.Reset()
				for _, lbl := range ts.Labels {
					sb.Add(lbl.Name, lbl.Value)
				}
				sb.Sort()
				promLabels := sb.Labels()

				// Apply relabel configs
				relabeledLabels, keep := relabel.Process(promLabels, cfgPtrs...)
				if !keep {
					continue // drop this series
				}

				// Convert back to prompb.Label
				ts.Labels = prompb.FromLabels(relabeledLabels, ts.Labels[:0])

				if writeIdx != i {
					(*tsBuf)[writeIdx] = *ts
				}
				writeIdx++
			}
			*tsBuf = (*tsBuf)[:writeIdx]
		}

		// Estimate batch size for flush decisions
		added := (*tsBuf)[startIdx:]
		for i := range added {
			ts := &added[i]
			estimatedBytes += 16
			for _, lbl := range ts.Labels {
				estimatedBytes += len(lbl.Name) + len(lbl.Value) + 2
			}
		}

		if len(*tsBuf) >= maxSeries || estimatedBytes >= maxBytes {
			if err := flush(); err != nil {
				return err
			}
			estimatedBytes = 0
		}
	}

	return flush()
}

// Push gathers metrics, batches them, and sends them to the remote write endpoint.
func (c *Client) Push(ctx context.Context, pr PushRequest) error {
	return pushGathered(ctx, pr, c.sendEncoded)
}
