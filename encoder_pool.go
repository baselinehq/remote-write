package remotewrite

import (
	"sync"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/prompb"
)

var (
	// tsPool pools []prompb.TimeSeries slices.
	// Pre-sized for histogram-heavy workloads (100 histograms = ~1300 series).
	tsPool = sync.Pool{
		New: func() interface{} {
			s := make([]prompb.TimeSeries, 0, 2048)
			return &s
		},
	}

	// lblPool pools []prompb.Label slices.
	// Pre-sized to fit typical labels + external labels without reallocation.
	lblPool = sync.Pool{
		New: func() interface{} {
			s := make([]prompb.Label, 0, 32)
			return &s
		},
	}

	// pbPool pools proto.Buffer for marshaling.
	pbPool = sync.Pool{
		New: func() interface{} {
			return proto.NewBuffer(nil)
		},
	}

	// snappyPool pools []byte buffers for snappy compression.
	snappyPool = sync.Pool{
		New: func() interface{} {
			b := make([]byte, 0, 64*1024)
			return &b
		},
	}

	// smplPool pools []prompb.Sample slices.
	// Almost always 1 sample per series for remote_write.
	smplPool = sync.Pool{
		New: func() interface{} {
			s := make([]prompb.Sample, 0, 1)
			return &s
		},
	}
)

func getTimeSeriesSlice() *[]prompb.TimeSeries {
	return tsPool.Get().(*[]prompb.TimeSeries)
}

func putTimeSeriesSlice(ts *[]prompb.TimeSeries) {
	if ts == nil {
		return
	}
	*ts = (*ts)[:0]
	tsPool.Put(ts)
}

func getLabelSlice() *[]prompb.Label {
	return lblPool.Get().(*[]prompb.Label)
}

func putLabelSlice(lbls *[]prompb.Label) {
	if lbls == nil {
		return
	}
	*lbls = (*lbls)[:0]
	lblPool.Put(lbls)
}

func getSampleSlice() *[]prompb.Sample {
	return smplPool.Get().(*[]prompb.Sample)
}

func putSampleSlice(smpls *[]prompb.Sample) {
	if smpls == nil {
		return
	}
	*smpls = (*smpls)[:0]
	smplPool.Put(smpls)
}

func getProtoBuffer() *proto.Buffer {
	return pbPool.Get().(*proto.Buffer)
}

func putProtoBuffer(buf *proto.Buffer) {
	if buf == nil {
		return
	}
	buf.Reset()
	pbPool.Put(buf)
}

func getSnappyBuffer() *[]byte {
	return snappyPool.Get().(*[]byte)
}

func putSnappyBuffer(b *[]byte) {
	if b == nil {
		return
	}
	// Don't pool excessively large buffers
	if cap(*b) > 10*1024*1024 {
		return
	}
	*b = (*b)[:0]
	snappyPool.Put(b)
}
