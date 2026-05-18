package remotewrite

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"
)

// Config configures the remote write client
type Config struct {
	// UpstreamURL is the base URL to send remote write requests to.
	// May contain {tenant} placeholder for tenant-based URL routing.
	UpstreamURL string

	// Timeout, if > 0, is applied to http.Client.Timeout and (for the default
	// transport) ResponseHeaderTimeout. Default: 0 (no client-level timeout).
	// Prefer context deadlines on each call.
	Timeout time.Duration

	// TenantHeader is the header name for tenant injection.
	TenantHeader string

	// BearerToken is an optional bearer token for authentication.
	BearerToken string

	// Transport is an optional custom http.Transport.
	Transport *http.Transport

	// MaxConnsPerHost limits connections to the upstream host. Default: 100.
	MaxConnsPerHost int

	// MaxIdleConnsPerHost limits idle connections per host. Default: 10.
	MaxIdleConnsPerHost int

	// Retry configures retry behavior. If nil, retries are DISABLED (streaming mode).
	Retry *RetryConfig

	// RateLimitBytesPerSec limits send rate. 0 = disabled.
	RateLimitBytesPerSec int64

	// UserAgent is the User-Agent header value. Default: "remotewrite-client".
	UserAgent string

	// DefaultContentType is the default Content-Type if not specified per request.
	// Common value: "application/x-protobuf"
	DefaultContentType string

	// DefaultContentEncoding is the default Content-Encoding if not specified per request.
	// Common value: "snappy"
	DefaultContentEncoding string

	// DefaultRemoteWriteVersion is the default X-Prometheus-Remote-Write-Version.
	// Common value: "0.1.0"
	DefaultRemoteWriteVersion string
}

// RetryConfig configures retry behavior.
type RetryConfig struct {
	// MaxRetries is the maximum number of retries. Must be >= 1.
	MaxRetries int

	// MinWait is the minimum wait between retries. Default: 1s.
	MinWait time.Duration

	// MaxWait is the maximum wait between retries. Default: 30s.
	MaxWait time.Duration

	// MaxBodySize is the maximum body size to buffer for retries.
	// Bodies larger than this will fail with an error if retries are enabled
	// and content length is unknown. Default: 10MB.
	MaxBodySize int64
}

// ForwardRequest contains the data to forward to the upstream.
type ForwardRequest struct {
	// TenantID for URL substitution or header injection.
	TenantID string

	// Body is the request body. It will be closed by the transport.
	// Caller should not close it manually until the request is complete.
	Body io.ReadCloser

	// BodyBytes provides pre-buffered body bytes (best performance).
	BodyBytes []byte

	// GetBody returns a fresh body for retries.
	GetBody func() (io.ReadCloser, error)

	// ContentLength is the body size. Set to -1 (or leave 0 with a non-nil
	// Body) when the size is unknown — Forward treats 0 with a streaming Body
	// as "unknown" so net/http does not advertise an empty body.
	ContentLength int64

	// ContentType header value.
	ContentType string

	// ContentEncoding header value.
	ContentEncoding string

	// RemoteWriteVersion header value.
	RemoteWriteVersion string

	// ExtraHeaders are additional headers to forward. It must not contain
	// protocol headers or headers managed by client configuration.
	ExtraHeaders http.Header
}

type Client struct {
	// Pre-parsed URL components to avoid per-request allocation
	baseURL          *url.URL // used when no tenant substitution
	tenantURLPrefix  *url.URL // parsed URL with path split at {tenant}
	tenantPathPrefix string   // path before {tenant}
	tenantPathSuffix string   // path after {tenant}
	tenantRawPrefix  string   // escaped path before {tenant}
	tenantRawSuffix  string   // escaped path after {tenant}
	hasTenantVar     bool

	httpClient *http.Client
	transport  *http.Transport

	tenantHeader string
	bearerHeader string // precomputed "Bearer " + token
	userAgent    string

	// Precomputed header value slices to avoid per-request allocs
	userAgentV       []string
	authV            []string
	contentTypeV     []string // default Content-Type
	contentEncodingV []string // default Content-Encoding
	remoteWriteVerV  []string // default Remote-Write-Version

	retryConfig *RetryConfig
	rateLimiter *RateLimiter

	stopCh chan struct{}
	closed atomic.Bool
}

// New creates a new remote write client.
func New(cfg Config) (*Client, error) {
	if cfg.UpstreamURL == "" {
		return nil, errors.New("UpstreamURL is required")
	}

	// Apply defaults
	if cfg.MaxConnsPerHost == 0 {
		cfg.MaxConnsPerHost = 100
	}
	if cfg.MaxIdleConnsPerHost == 0 {
		cfg.MaxIdleConnsPerHost = 10
	}
	if cfg.UserAgent == "" {
		cfg.UserAgent = "remotewrite-client"
	}

	transport := cfg.Transport
	if transport == nil {
		transport = &http.Transport{
			MaxConnsPerHost:     cfg.MaxConnsPerHost,
			MaxIdleConnsPerHost: cfg.MaxIdleConnsPerHost,
			MaxIdleConns:        100,
			IdleConnTimeout:     90 * time.Second,
			WriteBufferSize:     64 * 1024,
			ReadBufferSize:      64 * 1024,
			DisableCompression:  true,
			ForceAttemptHTTP2:   true,
			TLSHandshakeTimeout: 10 * time.Second,
			// Only set ResponseHeaderTimeout if Timeout is configured
			ResponseHeaderTimeout: cfg.Timeout,
			ExpectContinueTimeout: 1 * time.Second,
		}
	} else {
		if transport.WriteBufferSize == 0 {
			transport.WriteBufferSize = 64 * 1024
		}
		if transport.ReadBufferSize == 0 {
			transport.ReadBufferSize = 64 * 1024
		}
		// Apply ResponseHeaderTimeout if configured and not already set
		if cfg.Timeout > 0 && transport.ResponseHeaderTimeout == 0 {
			transport.ResponseHeaderTimeout = cfg.Timeout
		}
	}

	stopCh := make(chan struct{})

	// Create http.Client with optional timeout
	httpClient := &http.Client{Transport: transport}
	if cfg.Timeout > 0 {
		httpClient.Timeout = cfg.Timeout
	}

	client := &Client{
		httpClient:   httpClient,
		transport:    transport,
		tenantHeader: cfg.TenantHeader,
		userAgent:    cfg.UserAgent,
		stopCh:       stopCh,
	}

	// Precompute header value slices (avoid per-request allocs)
	client.userAgentV = []string{cfg.UserAgent}
	if cfg.BearerToken != "" {
		client.bearerHeader = "Bearer " + cfg.BearerToken
		client.authV = []string{client.bearerHeader}
	}
	if cfg.DefaultContentType != "" {
		client.contentTypeV = []string{cfg.DefaultContentType}
	}
	if cfg.DefaultContentEncoding != "" {
		client.contentEncodingV = []string{cfg.DefaultContentEncoding}
	}
	if cfg.DefaultRemoteWriteVersion != "" {
		client.remoteWriteVerV = []string{cfg.DefaultRemoteWriteVersion}
	}

	// Process retry config
	if cfg.Retry != nil {
		rc := *cfg.Retry
		if rc.MaxRetries < 1 {
			rc.MaxRetries = 1
		}
		if rc.MinWait == 0 {
			rc.MinWait = time.Second
		}
		if rc.MaxWait == 0 {
			rc.MaxWait = 30 * time.Second
		}
		if rc.MaxBodySize == 0 {
			rc.MaxBodySize = 10 * 1024 * 1024
		}
		client.retryConfig = &rc
	}

	parsedURL, err := parseUpstreamURL(cfg.UpstreamURL)
	if err != nil {
		return nil, err
	}

	if strings.Contains(cfg.UpstreamURL, "{tenant}") {
		if strings.Count(parsedURL.Path, "{tenant}") != 1 ||
			strings.Contains(parsedURL.Host, "{tenant}") ||
			strings.Contains(parsedURL.RawQuery, "{tenant}") ||
			strings.Contains(parsedURL.Fragment, "{tenant}") ||
			(parsedURL.User != nil && strings.Contains(parsedURL.User.String(), "{tenant}")) {
			return nil, fmt.Errorf("{tenant} placeholder is only supported once in URL path")
		}

		client.hasTenantVar = true
		client.tenantURLPrefix = parsedURL

		pathIdx := strings.Index(parsedURL.Path, "{tenant}")
		client.tenantPathPrefix = parsedURL.Path[:pathIdx]
		client.tenantPathSuffix = parsedURL.Path[pathIdx+len("{tenant}"):]

		escapedPath := parsedURL.EscapedPath()
		escapedPlaceholder := url.PathEscape("{tenant}")
		rawPathIdx := strings.Index(escapedPath, escapedPlaceholder)
		if rawPathIdx < 0 {
			return nil, fmt.Errorf("invalid UpstreamURL: failed to locate escaped {tenant} placeholder")
		}
		client.tenantRawPrefix = escapedPath[:rawPathIdx]
		client.tenantRawSuffix = escapedPath[rawPathIdx+len(escapedPlaceholder):]
	} else {
		client.baseURL = parsedURL
	}

	if cfg.RateLimitBytesPerSec > 0 {
		client.rateLimiter = NewRateLimiter(cfg.RateLimitBytesPerSec, stopCh)
	}

	return client, nil
}

func parseUpstreamURL(rawURL string) (*url.URL, error) {
	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("invalid UpstreamURL: %w", err)
	}
	if parsedURL.Scheme == "" || parsedURL.Host == "" {
		return nil, fmt.Errorf("invalid UpstreamURL: absolute URL with scheme and host required")
	}
	return parsedURL, nil
}

// Forward sends a remote write request to the upstream
func (c *Client) Forward(ctx context.Context, req ForwardRequest) (*http.Response, error) {
	if c.closed.Load() {
		return nil, errors.New("client is closed")
	}
	if err := c.validateExtraHeaders(req.ExtraHeaders); err != nil {
		return nil, err
	}

	// Build target URL
	var parsedURL *url.URL

	if c.hasTenantVar {
		// Use pre-parsed URL with tenant path substitution
		u := *c.tenantURLPrefix // copy struct
		u.Path = c.tenantPathPrefix + req.TenantID + c.tenantPathSuffix
		u.RawPath = c.tenantRawPrefix + url.PathEscape(req.TenantID) + c.tenantRawSuffix
		parsedURL = &u
	} else {
		parsedURL = c.baseURL
	}

	// bodyMode: 0=none, 1=BodyBytes, 2=GetBody, 3=buffered
	var body io.ReadCloser
	var getBody func() (io.ReadCloser, error)
	var contentLength int64 = req.ContentLength
	var poolBuf []byte
	var poolClass int
	var bodyMode int
	var bodyData []byte

	retriesEnabled := c.retryConfig != nil

	if len(req.BodyBytes) > 0 {
		bodyMode = 1
		bodyData = req.BodyBytes
		body = getByteBody(req.BodyBytes)
		contentLength = int64(len(req.BodyBytes))
	} else if req.GetBody != nil {
		bodyMode = 2
		getBody = req.GetBody
		if req.Body != nil {
			body = req.Body
		} else {
			var err error
			body, err = getBody()
			if err != nil {
				return nil, fmt.Errorf("failed to get initial body: %w", err)
			}
		}
	} else if req.Body != nil {
		// ContentLength == 0 with a non-nil Body almost always means the caller
		// did not set it. Treat as unknown so net/http does not advertise a
		// zero-length body for a Reader that may produce bytes. Callers that
		// genuinely have a zero-byte body should pass BodyBytes (or omit Body).
		if contentLength == 0 {
			contentLength = -1
		}
		if retriesEnabled {
			maxSize := c.retryConfig.MaxBodySize
			if maxSize > 0 && contentLength > maxSize {
				req.Body.Close()
				return nil, fmt.Errorf("failed to buffer body: %w", errBodyTooLarge)
			}
			bodyMode = 3
			data, pc, err := readAllPooled(req.Body, contentLength, maxSize)
			req.Body.Close()
			if err != nil {
				return nil, fmt.Errorf("failed to buffer body: %w", err)
			}
			poolBuf = data
			poolClass = pc
			bodyData = data
			body = getByteBody(data)
			contentLength = int64(len(data))
		} else {
			// Streaming mode, no buffering
			body = req.Body
		}
	} else {
		body = http.NoBody
	}

	// Build getBody lazily based on mode. Retry mode always needs a replay
	// function, even for requests with no body.
	if retriesEnabled && getBody == nil {
		// Only used during retries - create closure on demand
		switch bodyMode {
		case 1, 3: // BodyBytes or buffered
			getBody = func() (io.ReadCloser, error) {
				return getByteBody(bodyData), nil
			}
		default:
			getBody = func() (io.ReadCloser, error) {
				return http.NoBody, nil
			}
		}
	}

	// Return pooled buffer when done
	if poolClass > 0 {
		defer bufferPool.putBuffer(poolBuf, poolClass)
	}

	if c.rateLimiter != nil && contentLength > 0 {
		if err := c.rateLimiter.Register(ctx, contentLength); err != nil {
			body.Close()
			return nil, err
		}
	}

	httpReq, err := c.newRequest(ctx, parsedURL, body, contentLength, req)
	if err != nil {
		return nil, err
	}
	if getBody != nil {
		httpReq.GetBody = getBody
	}

	resp, err := c.httpClient.Do(httpReq)

	if err != nil && isEOFError(err) && getBody != nil {
		newBody, bodyErr := getBody()
		if bodyErr == nil {
			httpReq, err = c.newRequest(ctx, parsedURL, newBody, contentLength, req)
			if err == nil {
				httpReq.GetBody = getBody
				resp, err = c.httpClient.Do(httpReq)
			}
		}
	}

	if !retriesEnabled {
		return resp, err
	}

	// Check if we should retry
	if err == nil && !isRetryableStatusCode(resp.StatusCode) {
		return resp, nil
	}
	if err != nil {
		// Always close response body if present
		if resp != nil && resp.Body != nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			resp = nil
		}
		if !isRetryableError(err) {
			return nil, err
		}
	}

	// Retry loop. The first retry waits MinWait (plus jitter); subsequent
	// retries double up to MaxWait. Retry-After overrides this schedule for
	// the current attempt.
	retryDuration := c.retryConfig.MinWait
	maxDuration := c.retryConfig.MaxWait

	for attempt := 1; attempt <= c.retryConfig.MaxRetries; attempt++ {
		if resp != nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}

		var retryAfter time.Duration
		if resp != nil {
			retryAfter = parseRetryAfterHeader(resp.Header.Get("Retry-After"))
		}
		wait := nextRetryWait(retryAfter, retryDuration)
		retryDuration = advanceRetryWait(retryDuration, maxDuration)

		if err := waitForRetry(ctx, wait); err != nil {
			return nil, fmt.Errorf("retry cancelled: %w", err)
		}

		newBody, bodyErr := getBody()
		if bodyErr != nil {
			return nil, fmt.Errorf("retry %d: failed to get body: %w", attempt, bodyErr)
		}

		httpReq, err = c.newRequest(ctx, parsedURL, newBody, contentLength, req)
		if err != nil {
			return nil, fmt.Errorf("retry %d: failed to create request: %w", attempt, err)
		}
		httpReq.GetBody = getBody

		resp, err = c.httpClient.Do(httpReq)

		if err != nil && isEOFError(err) && getBody != nil {
			newBody, bodyErr = getBody()
			if bodyErr == nil {
				httpReq, err = c.newRequest(ctx, parsedURL, newBody, contentLength, req)
				if err == nil {
					httpReq.GetBody = getBody
					resp, err = c.httpClient.Do(httpReq)
				}
			}
		}

		if err == nil && !isRetryableStatusCode(resp.StatusCode) {
			return resp, nil
		}
		if err != nil && !isRetryableError(err) {
			return nil, err
		}
	}

	if err != nil {
		return nil, fmt.Errorf("max retries exceeded: %w", err)
	}
	return resp, nil
}

func (c *Client) validateExtraHeaders(headers http.Header) error {
	for key := range headers {
		canonicalKey := http.CanonicalHeaderKey(key)
		if isReservedExtraHeader(canonicalKey) {
			return fmt.Errorf("ExtraHeaders contains reserved header %q", key)
		}
		if len(c.authV) > 0 && canonicalKey == "Authorization" {
			return fmt.Errorf("ExtraHeaders contains protected header %q", key)
		}
		if c.tenantHeader != "" && canonicalKey == http.CanonicalHeaderKey(c.tenantHeader) {
			return fmt.Errorf("ExtraHeaders contains protected header %q", key)
		}
	}
	return nil
}

func isReservedExtraHeader(canonicalKey string) bool {
	switch canonicalKey {
	case "Content-Encoding",
		"Content-Type",
		"User-Agent",
		"X-Prometheus-Remote-Write-Version":
		return true
	default:
		return false
	}
}

// newRequest creates an HTTP request.
func (c *Client) newRequest(ctx context.Context, parsedURL *url.URL, body io.ReadCloser, contentLength int64, req ForwardRequest) (*http.Request, error) {
	if body == nil {
		body = http.NoBody
	}

	var httpReq *http.Request

	// Pre-allocate header map
	headerSize := 4 + len(req.ExtraHeaders)
	if c.tenantHeader != "" {
		headerSize++
	}
	if len(c.authV) > 0 {
		headerSize++
	}

	// Always use pre-parsed URL
	u := *parsedURL
	httpReq = &http.Request{
		Method:     http.MethodPost,
		URL:        &u,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     make(http.Header, headerSize),
		Body:       body,
		Host:       u.Host,
	}
	httpReq = httpReq.WithContext(ctx)

	if contentLength >= 0 {
		httpReq.ContentLength = contentLength
	}

	// Direct map assignment with precomputed slices
	h := httpReq.Header
	h["User-Agent"] = c.userAgentV

	// Use precomputed defaults if request doesn't override
	if req.ContentType != "" {
		h["Content-Type"] = []string{req.ContentType}
	} else if len(c.contentTypeV) > 0 {
		h["Content-Type"] = c.contentTypeV
	}

	if req.ContentEncoding != "" {
		h["Content-Encoding"] = []string{req.ContentEncoding}
	} else if len(c.contentEncodingV) > 0 {
		h["Content-Encoding"] = c.contentEncodingV
	}

	if req.RemoteWriteVersion != "" {
		h["X-Prometheus-Remote-Write-Version"] = []string{req.RemoteWriteVersion}
	} else if len(c.remoteWriteVerV) > 0 {
		h["X-Prometheus-Remote-Write-Version"] = c.remoteWriteVerV
	}

	if c.tenantHeader != "" && req.TenantID != "" {
		h[c.tenantHeader] = []string{req.TenantID}
	}
	if len(c.authV) > 0 {
		h["Authorization"] = c.authV
	}

	// Extra headers - direct assignment
	for key, values := range req.ExtraHeaders {
		if len(values) > 0 {
			h[key] = values
		}
	}

	return httpReq, nil
}

// Close shuts down the client.
func (c *Client) Close() error {
	if c.closed.Swap(true) {
		return nil
	}
	close(c.stopCh)
	c.transport.CloseIdleConnections()
	return nil
}

// RetriesEnabled returns true if retries are configured.
func (c *Client) RetriesEnabled() bool {
	return c.retryConfig != nil
}
