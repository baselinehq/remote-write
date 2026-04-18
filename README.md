# remotewrite

A Go client for Prometheus `remote_write` protocol with two transport layers:

- `Client` for immediate synchronous delivery.
- `DurableClient` for local disk spooling, restart survival, and background draining.

## What this library does
1. Encode Prometheus metrics from a `prometheus.Gatherer` and push them via `remote_write`.
2. Stream or replay incoming `remote_write` payloads to an upstream in proxy mode.
3. Persist encoded `remote_write` payloads to a local durable queue and drain them later.

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

## Durable Mode

Use `DurableClient` when a long-running process must survive restarts or temporary upstream outages without dropping already-accepted payloads.

`DurableClient` is transport-focused:

- it accepts fully materialized `remote_write` payload bytes,
- persists accepted payloads as local spool records on disk,
- schedules those records through an internal VictoriaMetrics fast queue,
- drains in the background,
- retries retryable failures with backoff,
- preserves FIFO semantics by default with `SendConcurrency: 1`.

The durable layer does not require Prometheus internals or a second process.

### Durable vs Synchronous

- `Client` sends now and returns the result now.
- `DurableClient` enqueues now and delivers later.
- `Client.Push(...)` / `Client.PushTimeSeries(...)` encode and send immediately.
- `DurableClient.Push(...)` / `DurableClient.PushTimeSeries(...)` encode and enqueue the resulting payloads.

### Durable Daemon Example

```go
queueDir := "/var/lib/my-agent/remotewrite"

dc, err := remotewrite.NewDurable(remotewrite.DurableConfig{
    Client: remotewrite.Config{
        UpstreamURL:  "https://mimir.example.com/api/v1/push",
        TenantHeader: "X-Scope-OrgID",
        Retry: &remotewrite.RetryConfig{
            MinWait: time.Second,
            MaxWait: 30 * time.Second,
        },
    },
    QueueDir:        queueDir,
    QueueName:       "metrics",
    MaxInMemoryBlocks: 128,
    MaxPendingBytes: 10 * 1024 * 1024 * 1024, // 10GiB budget
    SendConcurrency: 1,
    Registerer:      prometheus.DefaultRegisterer,
})
if err != nil {
    log.Fatal(err)
}
defer dc.Close()

go func() {
    if err := dc.Run(context.Background()); err != nil {
        log.Fatalf("durable drain stopped: %v", err)
    }
}()

ticker := time.NewTicker(15 * time.Second)
defer ticker.Stop()

for range ticker.C {
    if err := dc.Push(context.Background(), remotewrite.PushRequest{
        TenantID: "tenant-a",
        Gatherer: prometheus.DefaultGatherer,
        ExternalLabels: map[string]string{
            "job":      "my-agent",
            "instance": hostname,
        },
    }); err != nil {
        log.Printf("enqueue failed: %v", err)
    }
}
```

### Durable API

```go
type DurableRequest struct {
    TenantID           string
    BodyBytes          []byte
    ContentType        string
    ContentEncoding    string
    RemoteWriteVersion string
    ExtraHeaders       http.Header
}

func NewDurable(cfg DurableConfig) (*DurableClient, error)
func (dc *DurableClient) Run(ctx context.Context) error
func (dc *DurableClient) Close() error
func (dc *DurableClient) Enqueue(ctx context.Context, req DurableRequest) error
func (dc *DurableClient) Push(ctx context.Context, req PushRequest) error
func (dc *DurableClient) PushTimeSeries(ctx context.Context, req PushTimeSeriesRequest) error
```

`DurableClient.Enqueue(...)` requires replayable bytes. It does not accept a streaming `io.ReadCloser`.

### Queue Sizing Notes

- `MaxPendingBytes` bounds the total size of accepted spool records on disk. When the budget would be exceeded, `Enqueue(...)` returns `ErrDurableQueueBlocked`.
- `MaxInMemoryBlocks` controls how many record references stay in memory before the internal scheduler spills them to files.
- `DisablePersistence` disables normal file spillover for the internal scheduler queue. Accepted durable records are still written to disk, and their scheduler references may still be written during recovery or after acceptance.
- Each queued record stores request metadata plus body bytes, so on-disk usage is slightly larger than `len(BodyBytes)`.
- Use a single `DurableClient` per `QueueDir` + `QueueName` spool. `NewDurable(...)` takes an advisory spool lock and returns an error if another client already owns the same spool.
- The scheduler queue is rebuilt from spool records on startup. The startup reset is safe only while the spool lock is held.

### Ordering And Concurrency

- The default `SendConcurrency` is `1`.
- This preserves simple FIFO drain behavior and is the safest choice for Prometheus, Mimir, Cortex, Thanos, and similar backends that may reject out-of-order samples.
- Values greater than `1` can improve throughput, but they may produce out-of-order delivery.

### Retry And Reliability Notes

- Retryable failures keep the durable record on disk and retry later.
- Permanent failures drop the payload and increment `remotewrite_durable_permanent_failures_total` and `remotewrite_durable_dropped_total`.
- Corrupt spool records are moved to the queue's `corrupt/` directory, counted, and skipped so they do not block valid records behind them.
- The drain path is at-least-once. If the upstream accepts a payload and the process crashes before the durable record is removed, that payload can be replayed after restart.
- Accepted payload durability comes from the spool record itself, not from the destructive scheduler queue read path.
- Once `Enqueue(...)` durably writes a spool record, the payload is considered accepted and will be scheduled even if the caller's context is cancelled immediately afterward.
- `Enqueue(...)` performs a durable record write and directory sync before the payload is considered accepted, so it is more expensive than the synchronous `Client` fast path.
- `Run(...)` is intended for a single long-lived drain loop per `DurableClient`.

### Durable Metrics

The durable layer exports:

- `remotewrite_durable_enqueued_total`
- `remotewrite_durable_sent_total`
- `remotewrite_durable_send_failures_total`
- `remotewrite_durable_retryable_failures_total`
- `remotewrite_durable_permanent_failures_total`
- `remotewrite_durable_dropped_total`
- `remotewrite_durable_corrupt_records_total`
- `remotewrite_durable_queue_pending_bytes`
- `remotewrite_durable_queue_inmemory_blocks`
- `remotewrite_durable_queue_blocked`
- `remotewrite_durable_inflight`
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
