# remotewrite

A high-performance Go client for Prometheus `remote_write` protocol.

## What this library does
1. Encode Prometheus metrics from a `prometheus.Gatherer` (like `prometheus.DefaultGatherer`) and push them via remote_write protocol to VictoriaMetrics or any compatible endpoint.

2. Stream or replay incoming remote_write payloads to an upstream (proxy mode).

## Install

```bash
go get github.com/baselinehq/remote-write
```

## Quickstart

Forward an incoming remote_write request (typical proxy):

```go
client, err := remotewrite.New(remotewrite.Config{
    UpstreamURL:  "http://victoriametrics:8428/api/v1/write",
    TenantHeader: "X-Scope-OrgID",
    BearerToken:  "",
    // Retry: nil => streaming mode (fastest)
})
if err != nil {
    log.Fatal(err)
}
defer client.Close()

func handleRemoteWrite(w http.ResponseWriter, r *http.Request) {
    // In streaming mode, Forward will stream r.Body to the upstream once
    resp, err := client.Forward(r.Context(), remotewrite.ForwardRequest{
        TenantID:           r.Header.Get("X-Scope-OrgID"),
        Body:               r.Body,
        ContentLength:      r.ContentLength,
        ContentType:        r.Header.Get("Content-Type"),
        ContentEncoding:    r.Header.Get("Content-Encoding"),
        RemoteWriteVersion: r.Header.Get("X-Prometheus-Remote-Write-Version"),
    })

    if err != nil {
        http.Error(w, err.Error(), http.StatusBadGateway)
        return
    }
    defer resp.Body.Close()

    w.WriteHeader(resp.StatusCode)
    io.Copy(w, resp.Body)
}
```

### Best performance: pass BodyBytes

If your caller already has the payload as `[]byte`, this avoids per-request reader allocations and preserves the transport fast-path.

```go
resp, err := client.Forward(ctx, remotewrite.ForwardRequest{
    BodyBytes:   payload,
    ContentType: "application/x-protobuf",
})
```

## Encode and Send Metrics

Push metrics from a `prometheus.Gatherer` (like `prometheus.DefaultGatherer`) to a remote_write endpoint. The library handles protobuf encoding, snappy compression, and batching automatically.

### Basic Usage

```go
import (
    "github.com/prometheus/client_golang/prometheus"
    "github.com/baselinehq/remote-write"
)

client, err := remotewrite.New(remotewrite.Config{
    UpstreamURL: "http://victoriametrics:8428/api/v1/write",
})
if err != nil {
    log.Fatal(err)
}
defer client.Close()

requestCounter := prometheus.NewCounter(prometheus.CounterOpts{
    Name: "myapp_requests_total",
    Help: "Total number of requests",
})
prometheus.MustRegister(requestCounter)

// Push metrics periodically
err = client.Push(ctx, remotewrite.PushRequest{
    Gatherer: prometheus.DefaultGatherer,
    ExternalLabels: map[string]string{
        "job":      "my-daemon",
        "instance": hostname,
    },
})
if err != nil {
    log.Printf("push failed: %v", err)
}
```

### With Tenant and Retries

```go
client, err := remotewrite.New(remotewrite.Config{
    UpstreamURL:  "http://vm:8428/insert/{tenant}/prometheus/api/v1/write",
    TenantHeader: "X-Scope-OrgID",
    BearerToken:  "secret-token",
    Retry: &remotewrite.RetryConfig{
        MaxRetries: 3,
        MinWait:    time.Second,
        MaxWait:    30 * time.Second,
    },
})
if err != nil {
    log.Fatal(err)
}
defer client.Close()

err = client.Push(ctx, remotewrite.PushRequest{
    TenantID: "tenant-123",
    Gatherer: prometheus.DefaultGatherer,
    ExternalLabels: map[string]string{
        "job":      "my-daemon",
        "instance": hostname,
    },
})
```
### External Labels Merge Rule

External labels are merged with metric labels using this rule:
- **Metric labels take precedence** - if a series already has label "k", external label "k" is NOT applied.
- This allows external labels like `job` and `instance` to be overridden by individual metrics when needed.

### Supported Metric Types

The encoder correctly handles all Prometheus metric types:

| Type | Series Generated |
|------|------------------|
| Counter | `<name>` |
| Gauge | `<name>` |
| Untyped | `<name>` |
| Histogram | `<name>_bucket{le="..."}`, `<name>_count`, `<name>_sum` |
| Summary | `<name>{quantile="..."}`, `<name>_count`, `<name>_sum` |

## Configuration

### Config

#### UpstreamURL (required)

Can include `{tenant}` placeholder, e.g. `http://vm:8428/insert/{tenant}/prometheus/api/v1/write`

#### Timeout

If set, applied to `http.Client.Timeout` and (when using the default transport) `ResponseHeaderTimeout`.

For minimal overhead, prefer context deadlines and keep this as 0.

#### TenantHeader / BearerToken

#### Transport

Provide your own `*http.Transport` if you want full control. Otherwise a tuned transport is created.

#### MaxConnsPerHost, MaxIdleConnsPerHost

#### Retry (optional)

If nil, retries are disabled and requests are streamed once.

#### RateLimitBytesPerSec (optional)

#### UserAgent

### ForwardRequest

You can provide the body in one of three ways:

- `BodyBytes []byte` (fastest)
- `GetBody func() (io.ReadCloser, error)` (replayable, ideal for retry mode)
- `Body io.ReadCloser` (streaming; retry mode will buffer up to MaxBodySize if enabled)

Also supports forwarding request metadata:

- `ContentType`, `ContentEncoding`, `RemoteWriteVersion`
- `ExtraHeaders` (directly assigned; treat as immutable)

## Retries

Retries are disabled by default.

To enable:

```go
client, _ := remotewrite.New(remotewrite.Config{
    UpstreamURL: "http://upstream:8428/api/v1/write",
    Retry: &remotewrite.RetryConfig{
        MaxRetries:  3,
        MinWait:     1 * time.Second,
        MaxWait:     30 * time.Second,
        MaxBodySize: 10 * 1024 * 1024, // 10MB default if unset
    },
})
```

Retry behavior:

- Retries on 429 and 5xx.
- Honors `Retry-After` when present.
- Uses exponential backoff with jitter.
- Uses `GetBody` when provided. Otherwise it will buffer `Body` up to `MaxBodySize` to make retries possible.

## Tenant routing

Two supported patterns:

**URL substitution:**

```go
UpstreamURL: "http://vm:8428/insert/{tenant}/prometheus/api/v1/write"
```

**Header injection:**

```go
TenantHeader: "X-Scope-OrgID"
```

You can use both.

## How we achieve high performance

This client is optimized around a few principles:

### Forward API (Proxy Mode)

- Streaming by default, the fastest and safest path is send once, no buffering. Retries are opt-in because retries require replayable bodies.
- Pooled `io.ReadCloser` wrapper around `bytes.Reader` for `BodyBytes` and buffered retry bodies.
- Precomputed header value slices for common headers (User-Agent, Authorization) to avoid per-call `[]string{...}` allocations.
- Timer pooling for retry backoff waits to reduce GC pressure under sustained retries.
- Pooled retry buffers (size classes) to avoid repeated large allocations when buffering bodies for retries.

### Push API (Encoding + Sending)

- Compressed payload is passed directly to `Forward` via `BodyBytes`, avoiding reader allocations and enabling efficient retries.
- Reusable `[]prompb.TimeSeries` slices to reduce allocations during encoding.
- Reusable `[]prompb.Label` slices for label construction.
- Reusable `proto.Buffer` for marshaling to avoid per-encode allocations.
- Reusable compression output buffers sized via `snappy.MaxEncodedLen`.
- Bubble sort for small label sets (≤16 labels), avoiding `sort.Slice` overhead.
- External labels merged via two-pointer algorithm on sorted slices (no map allocations).
- Float formatting uses `strconv.FormatFloat` to avoid allocations in hot paths.
- Automatic batch splitting avoids building oversized payloads while minimizing HTTP overhead.

### Transport

- Connection reuse and sensible defaults for buffers and idle timeouts.
- Compression disabled (remote_write payload is already compressed at the application layer).

In practice, client-only benchmarks for 64KB payloads can reach sub-microsecond overhead per call with low allocation counts on modern hardware.

## Benchmarks

Run Forward API benchmarks:

```bash
go test -bench=ClientOnly -benchmem ./...
```

Run Push API / encoding benchmarks:

```bash
go test -bench=BenchmarkEncode -benchmem ./...
go test -bench=BenchmarkPush -benchmem ./...
```

If you're changing internals, prefer `GOMAXPROCS=1` for stable CPU numbers:

```bash
GOMAXPROCS=1 go test -bench=. -benchmem ./...
```

Example results (Apple M2 Max):

```
BenchmarkForward_Streaming_64KB                    25986             42315 ns/op        1548.76 MB/s        6049 B/op         72 allocs/op
BenchmarkForward_BodyBytes_64KB                    26215             44807 ns/op        1462.63 MB/s       38618 B/op         70 allocs/op
BenchmarkForward_WithRetries_64KB                  27031             45828 ns/op        1430.04 MB/s       38667 B/op         73 allocs/op
BenchmarkForward_Parallel                          26323             43738 ns/op        1498.39 MB/s       38546 B/op         68 allocs/op
BenchmarkTimerPool/pooled                       18455456                65.10 ns/op            0 B/op          0 allocs/op
BenchmarkTimerPool/raw                           9075631               133.0 ns/op           248 B/op          3 allocs/op
BenchmarkClientOnly_Streaming_64KB               2023741               613.1 ns/op      106885.92 MB/s      1704 B/op         12 allocs/op
BenchmarkClientOnly_BodyBytes_64KB               1934728               617.4 ns/op      106155.32 MB/s      1640 B/op         10 allocs/op
BenchmarkClientOnly_WithDefaults_64KB            1651048               785.4 ns/op      83439.00 MB/s       1688 B/op         10 allocs/op
BenchmarkClientOnly_WithTenant_64KB              1878255               691.3 ns/op      94804.70 MB/s       1688 B/op         11 allocs/op
BenchmarkClientOnly_WithRetries_64KB              605145              1943 ns/op        33735.47 MB/s       1713 B/op         14 allocs/op
BenchmarkClientOnly_ActualRetry_1Retry               505           2364135 ns/op          27.72 MB/s        4331 B/op         38 allocs/op
BenchmarkClientOnly_ActualRetry_2Retries             163           7346280 ns/op           8.92 MB/s        6699 B/op         58 allocs/op
BenchmarkEncode_100Counters                        52048             21717 ns/op            4353 B/op         16 allocs/op
BenchmarkEncode_100Gauges                          56329             22127 ns/op            4352 B/op         16 allocs/op
BenchmarkEncode_100Histograms                       2839            442420 ns/op          100567 B/op       2324 allocs/op
BenchmarkEncode_100Summaries                       10000            143830 ns/op           32476 B/op        820 allocs/op
BenchmarkEncode_MixedWorkload                       9842            114737 ns/op           27506 B/op        560 allocs/op
BenchmarkEncode_HighCardinality                    39134             30316 ns/op            4355 B/op         16 allocs/op
BenchmarkPush_SmallBatch                           23472             52018 ns/op           64952 B/op        540 allocs/op
BenchmarkPush_LargeBatch                            1137           1046436 ns/op          607070 B/op      10083 allocs/op
BenchmarkPush_WithRetries                              1        1089007083 ns/op          295032 B/op       1358 allocs/op
BenchmarkPush_Parallel                              7005            171989 ns/op          140957 B/op       2046 allocs/op
BenchmarkPush_Histograms                            3357            345561 ns/op          189372 B/op       3447 allocs/op
PASS
ok      github.com/baselinehq/remote-write      40.110s
```

## Closing

Call `Close()` when you're done to:

- Stop the rate limiter (if enabled)
- Close idle transport connections

```go
_ = client.Close()
```

## Notes

- `ExtraHeaders` are directly assigned into the outgoing request headers. Do not mutate `ExtraHeaders` after calling `Forward()`.
- If you want retries, prefer providing `GetBody` or `BodyBytes`. Buffering a large `Body` to enable retries costs memory and CPU by design.
