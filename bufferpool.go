package remotewrite

import (
	"bytes"
	"io"
	"sync"
)

// bufferPool provides pooled byte slices for retry mode
var bufferPool = &sizedPool{
	small: sync.Pool{
		New: func() any {
			return make([]byte, 64*1024)
		},
	},
	medium: sync.Pool{
		New: func() any {
			return make([]byte, 256*1024)
		},
	},
	large: sync.Pool{
		New: func() any {
			return make([]byte, 1024*1024)
		},
	},
}

type sizedPool struct {
	small  sync.Pool // 64KB buffers
	medium sync.Pool // 256KB buffers
	large  sync.Pool // 1MB buffers
}

// getBuffer returns a pooled buffer and its size class
func (p *sizedPool) getBuffer(size int) ([]byte, int) {
	if size <= 64*1024 {
		return p.small.Get().([]byte), 64 * 1024
	}

	if size <= 256*1024 {
		return p.medium.Get().([]byte), 256 * 1024
	}

	if size <= 1024*1024 {
		return p.large.Get().([]byte), 1024 * 1024
	}

	// Too large for pool
	return make([]byte, size), 0
}

// putBuffer returns a buffer to the pool
func (p *sizedPool) putBuffer(buf []byte, classSize int) {
	if cap(buf) == 0 || classSize == 0 {
		return
	}
	// Reset to full capacity before returning
	buf = buf[:cap(buf)]
	switch classSize {
	case 64 * 1024:
		p.small.Put(buf)
	case 256 * 1024:
		p.medium.Put(buf)
	case 1024 * 1024:
		p.large.Put(buf)
	}
}

// readAllPooled reads all data from r into a pooled buffer
func readAllPooled(r io.Reader, sizeHint int64, maxSize int64) (data []byte, classSize int, err error) {
	size := int(sizeHint)
	if size <= 0 {
		size = 64 * 1024
	}

	buf, classSize := bufferPool.getBuffer(size)
	pooled := true // track if we're still using a pooled buffer

	var total int
	for {
		if total >= len(buf) {
			// Buffer is full, probe for EOF before growing
			var probe [1]byte
			n, e := r.Read(probe[:])
			if e == io.EOF {
				// No more data, return without growing
				return buf[:total], classSize, nil
			}
			if e != nil {
				if pooled {
					bufferPool.putBuffer(buf, classSize)
				}
				return nil, 0, e
			}

			// Got more data, need to grow
			// Copy old buffer before returning to pool
			old := buf
			newBuf := make([]byte, len(old)*2)
			copy(newBuf, old[:total])
			if n > 0 {
				newBuf[total] = probe[0]
				total++
			}

			// Now safe to return old buffer to pool
			if pooled {
				bufferPool.putBuffer(old, classSize)
				pooled = false
				classSize = 0
			}
			buf = newBuf
			continue
		}

		n, e := r.Read(buf[total:])
		total += n

		// Check max size limit
		if maxSize > 0 && int64(total) > maxSize {
			if pooled {
				bufferPool.putBuffer(buf, classSize)
			}
			return nil, 0, errBodyTooLarge
		}

		if e == io.EOF {
			return buf[:total], classSize, nil
		}
		if e != nil {
			if pooled {
				bufferPool.putBuffer(buf, classSize)
			}
			return nil, 0, e
		}
	}
}

var errBodyTooLarge = &bodyTooLargeError{}

type bodyTooLargeError struct{}

func (e *bodyTooLargeError) Error() string {
	return "body exceeds maximum size for retry buffering"
}

// IsBodyTooLarge returns true if the error indicates the body exceeded MaxBodySize
func IsBodyTooLarge(err error) bool {
	_, ok := err.(*bodyTooLargeError)
	return ok
}

// byteBody is a pooled ReadCloser that wraps bytes.Reader
// This avoids allocating *bytes.Reader on every request
type byteBody struct {
	r bytes.Reader
}

func (b *byteBody) Read(p []byte) (n int, err error) {
	return b.r.Read(p)
}

// WriteTo implements io.WriterTo to preserve bytes.Reader fast-path optimization
func (b *byteBody) WriteTo(w io.Writer) (int64, error) {
	return b.r.WriteTo(w)
}

func (b *byteBody) Close() error {
	// Reset reader to drop references ASAP
	b.r.Reset(nil)
	// Return to pool - transport will call Close() when done
	byteBodyPool.Put(b)
	return nil
}

func (b *byteBody) reset(data []byte) {
	b.r.Reset(data)
}

var byteBodyPool = sync.Pool{
	New: func() any {
		return &byteBody{}
	},
}

// getByteBody returns a pooled byteBody initialized with data
func getByteBody(data []byte) io.ReadCloser {
	b := byteBodyPool.Get().(*byteBody)
	b.reset(data)
	return b
}
