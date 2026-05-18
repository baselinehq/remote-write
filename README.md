# remotewrite

A Go client for the Prometheus `remote_write` protocol with two transports:

- `Client` for synchronous delivery (forward + push).
- `DurableClient` for local disk spooling, restart survival, and background draining.

## Install

```bash
go get github.com/baselinehq/remote-write
```

## Forward Quickstart

Forward an incoming remote_write request to an upstream (typical proxy):

```go
client, err := remotewrite.New(remotewrite.Config{
    UpstreamURL:  "http://localhost:9090/api/v1/write",
    TenantHeader: "X-Scope-OrgID",
    // Retry: nil => streaming mode (single attempt, no buffering)
})
if err != nil {
    log.Fatal(err)
}
defer client.Close()

func handleRemoteWrite(w http.ResponseWriter, r *http.Request) {
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

Use `BodyBytes` when the payload is already materialized:

```go
resp, err := client.Forward(ctx, remotewrite.ForwardRequest{
    BodyBytes:   payload,
    ContentType: "application/x-protobuf",
})
```

## Push Quickstart

Encode metrics from a `prometheus.Gatherer` and send them as a `remote_write`
payload. The client handles protobuf encoding, snappy compression, and batching.

```go
client, err := remotewrite.New(remotewrite.Config{
    UpstreamURL: "http://localhost:9090/api/v1/write",
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

The encoder supports classic Prometheus metric types: counter, gauge, untyped,
classic histogram (`_bucket`/`_count`/`_sum`), and classic summary
(`{quantile=...}`/`_count`/`_sum`). Native histograms, exemplars, metadata, and
remote-write 2.0 are not implemented.

## Durable Mode

`DurableClient` accepts fully materialized `remote_write` payloads, writes them
to disk under `QueueDir`, and drains them in a background `Run` loop. Use it
for long-running agents that must survive restarts or temporary upstream
outages without dropping accepted payloads.

```go
dc, err := remotewrite.NewDurable(remotewrite.DurableConfig{
    Client: remotewrite.Config{
        UpstreamURL:  "http://localhost:9090/api/v1/write",
        TenantHeader: "X-Scope-OrgID",
        Retry: &remotewrite.RetryConfig{
            MinWait: time.Second,
            MaxWait: 30 * time.Second,
        },
    },
    QueueDir:        "/var/lib/my-agent/remotewrite",
    QueueName:       "metrics",
    MaxPendingBytes: 10 * 1024 * 1024 * 1024,
    SendConcurrency: 1,
})
if err != nil {
    log.Fatal(err)
}
defer dc.Close()

go func() {
    if err := dc.Run(context.Background()); err != nil {
        log.Printf("durable drain stopped: %v", err)
    }
}()

err = dc.Push(ctx, remotewrite.PushRequest{
    TenantID: "tenant-a",
    Gatherer: prometheus.DefaultGatherer,
})
```

Durable delivery is at-least-once. If the upstream accepts a payload and the
process exits before the spool record is removed, the payload may be replayed
after restart. The default `SendConcurrency: 1` preserves FIFO drain order.
Setting it higher improves throughput but can produce out-of-order delivery.

The durable layer exports the following Prometheus metrics:

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

## Configuration

`UpstreamURL` is required and must be an absolute URL with scheme and host.
It may contain a single `{tenant}` placeholder in the path, which is replaced
(path-escaped) with `ForwardRequest.TenantID` per call.

`TenantHeader` sets the header name for tenant injection; `BearerToken` adds
an `Authorization: Bearer ...` header.

`ExtraHeaders` are forwarded but cannot override the reserved headers
(`Content-Type`, `Content-Encoding`, `User-Agent`, `X-Prometheus-Remote-Write-Version`)
or the configured auth/tenant headers.

`Timeout`, if non-zero, is applied to `http.Client.Timeout` and (for the
default transport) `ResponseHeaderTimeout`. Prefer context deadlines and
leave this at 0.

`RateLimitBytesPerSec` enforces a token-bucket budget on outgoing bytes. It
respects the caller's context, including when a single payload is larger than
the per-second budget.

External labels are merged with metric labels: metric labels take precedence
when names collide.

## Retries

Retries are disabled by default (streaming mode). Enable them by setting
`Retry`:

```go
client, _ := remotewrite.New(remotewrite.Config{
    UpstreamURL: "http://localhost:9090/api/v1/write",
    Retry: &remotewrite.RetryConfig{
        MaxRetries:  3,
        MinWait:     time.Second,
        MaxWait:     30 * time.Second,
        MaxBodySize: 10 * 1024 * 1024,
    },
})
```

Retry behavior:

- Retries on 429 and 5xx, with exponential backoff plus jitter.
- Honors `Retry-After` when present.
- Uses `GetBody` when provided. Otherwise the request `Body` is buffered up to
  `MaxBodySize` so it can be replayed.

## Benchmarks

The default benchmarks measure library and client overhead against a loopback
`httptest` server. They do not prove receiver throughput — a real remote_write
receiver is the dominant cost in production.

```bash
go test -run='^$' -bench=. -benchmem ./benchmarks/...
```

## Integration Tests and Receiver Benchmarks
Integration correctness test (Docker required):

```bash
cd test/integration
go test -timeout 5m ./...
```

Real-VictoriaMetrics PushTimeSeries benchmarks (Docker required, opt-in):

```bash
cd test/integration
REMOTEWRITE_VM_BENCH=1 go test -run='^$' -bench=BenchmarkVictoriaMetrics -benchmem ./...
```

Set `REMOTEWRITE_VM_URL=http://127.0.0.1:8428/api/v1/write` to use an existing
VictoriaMetrics instance instead of starting one.
