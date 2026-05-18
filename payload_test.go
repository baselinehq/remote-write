package remotewrite

import (
	"bytes"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
)

func TestWithEncodedWriteRequestMatchesProtoMarshal(t *testing.T) {
	req := &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: "__name__", Value: "payload_equivalence_total"},
					{Name: "job", Value: "remote-write"},
					{Name: "instance", Value: "localhost:9090"},
				},
				Samples: []prompb.Sample{
					{Value: 42.5, Timestamp: 1_700_000_000_000},
					{Value: 43.5, Timestamp: 1_700_000_010_000},
				},
			},
		},
		Metadata: []prompb.MetricMetadata{
			{
				Type:             prompb.MetricMetadata_COUNTER,
				MetricFamilyName: "payload_equivalence_total",
				Help:             "test metric",
				Unit:             "requests",
			},
		},
	}

	want, err := proto.Marshal(req)
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}

	err = withEncodedWriteRequest(req, func(encoded []byte) error {
		got, err := snappy.Decode(nil, encoded)
		if err != nil {
			t.Fatalf("snappy.Decode: %v", err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("encoded protobuf mismatch:\ngot  %x\nwant %x", got, want)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("withEncodedWriteRequest: %v", err)
	}
}
