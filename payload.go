package remotewrite

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/klauspost/compress/snappy"
	"github.com/prometheus/prometheus/prompb"
)

const (
	defaultRemoteWriteContentType     = "application/x-protobuf"
	defaultRemoteWriteContentEncoding = "snappy"
	defaultRemoteWriteVersion         = "0.1.0"
	remoteWriteErrorBodyLimit         = 4 << 10
)

// DurableRequest is a fully materialized remote_write request suitable for spooling.
type DurableRequest struct {
	// TenantID is used for tenant URL substitution or tenant header injection.
	TenantID string
	// BodyBytes contains the fully materialized request body to send or spool.
	BodyBytes []byte
	// ContentType is the Content-Type header for the upstream request.
	ContentType string
	// ContentEncoding is the Content-Encoding header for the upstream request.
	ContentEncoding string
	// RemoteWriteVersion is the X-Prometheus-Remote-Write-Version header value.
	RemoteWriteVersion string
	// ExtraHeaders are additional HTTP headers to forward upstream.
	ExtraHeaders http.Header
}

type deliveryError struct {
	Retryable  bool
	RetryAfter time.Duration
	StatusCode int
	Status     string
	Body       string
	Err        error
}

func (e *deliveryError) Unwrap() error {
	return e.Err
}

func (e *deliveryError) Error() string {
	switch {
	case e.Err != nil && e.Status != "":
		return fmt.Sprintf("remote_write failed: status=%s err=%v", e.Status, e.Err)
	case e.Err != nil:
		return fmt.Sprintf("remote_write failed: %v", e.Err)
	case e.Status != "":
		return fmt.Sprintf("remote_write failed: status=%s body=%q", e.Status, e.Body)
	default:
		return "remote_write failed"
	}
}

func forwardRequestFromDurable(req DurableRequest) ForwardRequest {
	return ForwardRequest{
		TenantID:           req.TenantID,
		BodyBytes:          req.BodyBytes,
		ContentType:        req.ContentType,
		ContentEncoding:    req.ContentEncoding,
		RemoteWriteVersion: req.RemoteWriteVersion,
		ExtraHeaders:       req.ExtraHeaders,
	}
}

// withEncodedWriteRequest marshals and snappy-compresses req, then passes the encoded bytes to fn.
// fn must not retain the provided slice after it returns, since the backing buffer is pooled.
func withEncodedWriteRequest(req *prompb.WriteRequest, fn func([]byte) error) error {
	pb := getProtoBuffer()
	defer putProtoBuffer(pb)

	if err := pb.Marshal(req); err != nil {
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

	return fn(encoded)
}

func withEncodedTimeSeries(tss []prompb.TimeSeries, fn func([]byte) error) error {
	return withEncodedWriteRequest(&prompb.WriteRequest{Timeseries: tss}, fn)
}

func readRemoteWriteFailure(resp *http.Response) *deliveryError {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, remoteWriteErrorBodyLimit))
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	return &deliveryError{
		Retryable:  isRetryableStatusCode(resp.StatusCode),
		RetryAfter: parseRetryAfterHeader(resp.Header.Get("Retry-After")),
		StatusCode: resp.StatusCode,
		Status:     resp.Status,
		Body:       string(body),
	}
}

func closeRemoteWriteSuccess(resp *http.Response) error {
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.Body.Close()
}

func classifyRemoteWriteAttempt(resp *http.Response, err error) error {
	if err != nil {
		if resp != nil && resp.Body != nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}

		return &deliveryError{
			Retryable: isRetryableError(err),
			Err:       err,
		}
	}

	if resp.StatusCode/100 == 2 {
		return closeRemoteWriteSuccess(resp)
	}

	return readRemoteWriteFailure(resp)
}

func (c *Client) sendEncoded(ctx context.Context, req DurableRequest) error {
	resp, err := c.Forward(ctx, forwardRequestFromDurable(req))
	return classifyRemoteWriteAttempt(resp, err)
}
