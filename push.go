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
	// This is a soft target; a single metric family may push the batch over.
	MaxBatchBytes int

	// MaxSeriesPerBatch is a soft target for the number of series per batch.
	// Default: 10000. A batch may exceed this when the last appended metric
	// family pushes the count past the target — the series produced by a
	// single metric family are never split across batches.
	MaxSeriesPerBatch int

	// ExtraHeaders are additional headers to forward. It must not contain
	// protocol headers or headers managed by client configuration.
	ExtraHeaders http.Header
}

func pushGathered(ctx context.Context, pr PushRequest, deliver func(context.Context, DurableRequest) error) error {
	if pr.Gatherer == nil {
		return fmt.Errorf("gatherer is required")
	}

	relabelCfgs, err := prepareRelabelConfigs(pr.WriteRelabelConfigs)
	if err != nil {
		return err
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
	var sb labels.ScratchBuilder
	var lb *labels.Builder
	if relabelCfgs != nil {
		sb = labels.NewScratchBuilder(0)
		lb = labels.NewBuilder(labels.EmptyLabels())
	}
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
		if relabelCfgs != nil {
			writeIdx := startIdx
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
				lb.Reset(promLabels)
				if keep := relabel.ProcessBuilder(lb, relabelCfgs...); !keep {
					continue // drop this series
				}
				relabeledLabels := lb.Labels()

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

// prepareRelabelConfigs validates the caller's relabel configs against a
// private copy so Validate's mutation of NameValidationScheme does not leak
// back, and returns pointers suitable for relabel.Process. Returns nil when
// there are no configs.
func prepareRelabelConfigs(in []relabel.Config) ([]*relabel.Config, error) {
	if len(in) == 0 {
		return nil, nil
	}
	owned := make([]relabel.Config, len(in))
	ptrs := make([]*relabel.Config, len(in))
	for i, src := range in {
		owned[i] = src
		owned[i].NameValidationScheme = model.UTF8Validation
		if err := owned[i].Validate(model.UTF8Validation); err != nil {
			return nil, fmt.Errorf("invalid write_relabel_configs: %w", err)
		}
		ptrs[i] = &owned[i]
	}
	return ptrs, nil
}
