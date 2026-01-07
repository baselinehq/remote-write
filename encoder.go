package remotewrite

import (
	"math"
	"sort"
	"strconv"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/prometheus/prompb"
)

// convertMetricFamily converts a MetricFamily into TimeSeries and appends them to the buffer
func convertMetricFamily(mf *dto.MetricFamily, tsBuf *[]prompb.TimeSeries, externalLabels []prompb.Label, nowMs int64, labelTracker *[]*[]prompb.Label, sampleTracker *[]*[]prompb.Sample) {
	name := mf.GetName()
	mType := mf.GetType()

	for _, m := range mf.GetMetric() {
		ts := nowMs
		if m.TimestampMs != nil {
			ts = *m.TimestampMs
		}

		switch mType {
		case dto.MetricType_COUNTER:
			if m.Counter != nil {
				appendTimeSeries(tsBuf, name, m.Label, externalLabels, ts, m.Counter.GetValue(), labelTracker, sampleTracker)
			}
		case dto.MetricType_GAUGE:
			if m.Gauge != nil {
				appendTimeSeries(tsBuf, name, m.Label, externalLabels, ts, m.Gauge.GetValue(), labelTracker, sampleTracker)
			}
		case dto.MetricType_UNTYPED:
			if m.Untyped != nil {
				appendTimeSeries(tsBuf, name, m.Label, externalLabels, ts, m.Untyped.GetValue(), labelTracker, sampleTracker)
			}
		case dto.MetricType_HISTOGRAM:
			if hist := m.Histogram; hist != nil {
				// Precompute metric names to avoid allocations in loop
				bucketName := name + "_bucket"
				sumName := name + "_sum"
				countName := name + "_count"

				infSeen := false
				for _, bucket := range hist.Bucket {
					le := bucket.GetUpperBound()
					if math.IsInf(le, +1) {
						infSeen = true
					}

					appendHistogramBucket(tsBuf, bucketName, m.Label, externalLabels, ts, float64(bucket.GetCumulativeCount()), bucket.GetUpperBound(), labelTracker, sampleTracker)
				}
				if !infSeen {
					appendHistogramBucket(tsBuf, bucketName, m.Label, externalLabels, ts, float64(hist.GetSampleCount()), math.Inf(1), labelTracker, sampleTracker)
				}

				appendTimeSeries(tsBuf, sumName, m.Label, externalLabels, ts, hist.GetSampleSum(), labelTracker, sampleTracker)
				appendTimeSeries(tsBuf, countName, m.Label, externalLabels, ts, float64(hist.GetSampleCount()), labelTracker, sampleTracker)
			}
		case dto.MetricType_SUMMARY:
			if sum := m.Summary; sum != nil {
				// Precompute metric names to avoid allocations in loop
				sumName := name + "_sum"
				countName := name + "_count"

				for _, q := range sum.Quantile {
					appendSummaryQuantile(tsBuf, name, m.Label, externalLabels, ts, q.GetValue(), q.GetQuantile(), labelTracker, sampleTracker)
				}

				appendTimeSeries(tsBuf, sumName, m.Label, externalLabels, ts, sum.GetSampleSum(), labelTracker, sampleTracker)
				appendTimeSeries(tsBuf, countName, m.Label, externalLabels, ts, float64(sum.GetSampleCount()), labelTracker, sampleTracker)
			}
		}
	}
}

func appendTimeSeries(tsBuf *[]prompb.TimeSeries, name string, metricLabels []*dto.LabelPair, externalLabels []prompb.Label, timestamp int64, value float64, labelTracker *[]*[]prompb.Label, sampleTracker *[]*[]prompb.Sample) {
	smpls := getSampleSlice()
	*sampleTracker = append(*sampleTracker, smpls)
	*smpls = append(*smpls, prompb.Sample{Value: value, Timestamp: timestamp})

	ts := prompb.TimeSeries{
		Samples: *smpls,
	}

	ts.Labels = buildLabels(name, metricLabels, externalLabels, nil, labelTracker)
	*tsBuf = append(*tsBuf, ts)
}

func appendHistogramBucket(tsBuf *[]prompb.TimeSeries, name string, metricLabels []*dto.LabelPair, externalLabels []prompb.Label, timestamp int64, value float64, le float64, labelTracker *[]*[]prompb.Label, sampleTracker *[]*[]prompb.Sample) {
	smpls := getSampleSlice()
	*sampleTracker = append(*sampleTracker, smpls)
	*smpls = append(*smpls, prompb.Sample{Value: value, Timestamp: timestamp})

	ts := prompb.TimeSeries{
		Samples: *smpls,
	}

	leStr := formatFloat(le)
	extra := prompb.Label{Name: "le", Value: leStr}

	ts.Labels = buildLabels(name, metricLabels, externalLabels, &extra, labelTracker)
	*tsBuf = append(*tsBuf, ts)
}

func appendSummaryQuantile(tsBuf *[]prompb.TimeSeries, name string, metricLabels []*dto.LabelPair, externalLabels []prompb.Label, timestamp int64, value float64, quantile float64, labelTracker *[]*[]prompb.Label, sampleTracker *[]*[]prompb.Sample) {
	smpls := getSampleSlice()
	*sampleTracker = append(*sampleTracker, smpls)
	*smpls = append(*smpls, prompb.Sample{Value: value, Timestamp: timestamp})

	ts := prompb.TimeSeries{
		Samples: *smpls,
	}

	qStr := formatFloat(quantile)
	extra := prompb.Label{Name: "quantile", Value: qStr}

	ts.Labels = buildLabels(name, metricLabels, externalLabels, &extra, labelTracker)
	*tsBuf = append(*tsBuf, ts)
}

// buildLabels merges metric labels, external labels, and special labels (__name__, le, quantile)
func buildLabels(name string, metricLabels []*dto.LabelPair, externalLabels []prompb.Label, extra *prompb.Label, labelTracker *[]*[]prompb.Label) []prompb.Label {
	lbls := getLabelSlice()
	*labelTracker = append(*labelTracker, lbls)

	// 1. Add __name__
	*lbls = append(*lbls, prompb.Label{Name: "__name__", Value: name})

	if extra != nil {
		*lbls = append(*lbls, *extra)
	}

	for _, p := range metricLabels {
		if p.Name != nil && p.Value != nil {
			*lbls = append(*lbls, prompb.Label{Name: *p.Name, Value: *p.Value})
		}
	}

	sortLabels(*lbls)

	if len(externalLabels) > 0 {
		mergeExternalLabels(lbls, externalLabels)
	}

	return *lbls
}

func sortLabels(lbls []prompb.Label) {
	// Use bubble sort for small label sets (faster than sort.Slice)
	n := len(lbls)
	if n <= 16 {
		for i := 0; i < n; i++ {
			for j := 0; j < n-1-i; j++ {
				if lbls[j].Name > lbls[j+1].Name {
					lbls[j], lbls[j+1] = lbls[j+1], lbls[j]
				}
			}
		}
	} else {
		sort.Slice(lbls, func(i, j int) bool {
			return lbls[i].Name < lbls[j].Name
		})
	}
}

// mergeExternalLabels adds external labels that don't already exist in lbls
func mergeExternalLabels(lbls *[]prompb.Label, external []prompb.Label) {
	if len(external) == 0 {
		return
	}

	existing := *lbls
	if len(existing) == 0 {
		*lbls = append(*lbls, external...)
		return
	}

	need := len(existing) + len(external)
	if cap(existing) < need {
		newBuf := make([]prompb.Label, len(existing), need)
		copy(newBuf, existing)
		existing = newBuf
		*lbls = existing
	}

	if len(existing) <= 32 {
		var scratch [32]prompb.Label
		tmp := scratch[:len(existing)]
		copy(tmp, existing)

		out := existing[:0]
		i, j := 0, 0
		for i < len(tmp) && j < len(external) {
			a, b := tmp[i], external[j]
			if a.Name < b.Name {
				out = append(out, a)
				i++
			} else if a.Name > b.Name {
				out = append(out, b)
				j++
			} else {
				// Same name, keep existing
				out = append(out, a)
				i++
				j++
			}
		}
		out = append(out, tmp[i:]...)
		out = append(out, external[j:]...)
		*lbls = out
		return
	}

	// Fallback for large label sets
	tmp := make([]prompb.Label, len(existing))
	copy(tmp, existing)

	out := existing[:0]
	i, j := 0, 0
	for i < len(tmp) && j < len(external) {
		a, b := tmp[i], external[j]
		if a.Name < b.Name {
			out = append(out, a)
			i++
		} else if a.Name > b.Name {
			out = append(out, b)
			j++
		} else {
			out = append(out, a)
			i++
			j++
		}
	}
	out = append(out, tmp[i:]...)
	out = append(out, external[j:]...)
	*lbls = out
}

// formatFloat formats a float value for Prometheus label values (le, quantile)
func formatFloat(f float64) string {
	if math.IsInf(f, +1) {
		return "+Inf"
	}

	return strconv.FormatFloat(f, 'g', -1, 64)
}
