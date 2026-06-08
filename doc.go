// Package remotewrite provides clients for the Prometheus remote_write
// protocol.
//
// It supports three common paths:
//   - forwarding existing remote_write requests to another endpoint,
//   - gathering Prometheus metrics, encoding them, and sending them upstream,
//   - durably spooling encoded payloads on disk and draining them in the
//     background.
//
// The default Forward path streams request bodies once and does not buffer or
// retry unless retry behavior is explicitly configured. Push and PushTimeSeries
// encode remote_write protobuf payloads, snappy-compress them, and send the
// encoded bytes through the same transport.
//
// # Forwarding Requests
//
// Forward an incoming remote_write request to an upstream endpoint:
//
//	client, err := remotewrite.New(remotewrite.Config{
//		UpstreamURL:  "http://localhost:9090/api/v1/write",
//		TenantHeader: "X-Scope-OrgID",
//	})
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer client.Close()
//
//	func handleRemoteWrite(w http.ResponseWriter, r *http.Request) {
//		resp, err := client.Forward(r.Context(), remotewrite.ForwardRequest{
//			TenantID:           r.Header.Get("X-Scope-OrgID"),
//			Body:               r.Body,
//			ContentLength:      r.ContentLength,
//			ContentType:        r.Header.Get("Content-Type"),
//			ContentEncoding:    r.Header.Get("Content-Encoding"),
//			RemoteWriteVersion: r.Header.Get("X-Prometheus-Remote-Write-Version"),
//		})
//		if err != nil {
//			http.Error(w, err.Error(), http.StatusBadGateway)
//			return
//		}
//		defer resp.Body.Close()
//
//		w.WriteHeader(resp.StatusCode)
//		io.Copy(w, resp.Body)
//	}
//
// If the request body is already materialized, pass BodyBytes so Forward can
// use net/http's in-memory body fast path:
//
//	resp, err := client.Forward(ctx, remotewrite.ForwardRequest{
//		BodyBytes:   payload,
//		ContentType: "application/x-protobuf",
//	})
//
// # Pushing Gathered Metrics
//
// Push metrics from a prometheus.Gatherer to a remote_write endpoint:
//
//	client, err := remotewrite.New(remotewrite.Config{
//		UpstreamURL: "http://localhost:9090/api/v1/write",
//	})
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer client.Close()
//
//	err = client.Push(ctx, remotewrite.PushRequest{
//		Gatherer: prometheus.DefaultGatherer,
//		ExternalLabels: map[string]string{
//			"job":      "my-service",
//			"instance": hostname,
//		},
//	})
//
// Use PushTimeSeries when callers already have prompb.TimeSeries values.
//
// # Durable Delivery
//
// DurableClient accepts fully materialized remote_write payloads, stores them
// under QueueDir, and drains them in a background Run loop. It is useful for
// long-running agents that should survive restarts or temporary upstream
// outages without dropping accepted payloads.
//
//	dc, err := remotewrite.NewDurable(remotewrite.DurableConfig{
//		Client: remotewrite.Config{
//			UpstreamURL:  "http://localhost:9090/api/v1/write",
//			TenantHeader: "X-Scope-OrgID",
//		},
//		QueueDir:        "/var/lib/my-agent/remotewrite",
//		QueueName:       "metrics",
//		MaxPendingBytes: 10 * 1024 * 1024 * 1024,
//		SendConcurrency: 1,
//	})
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer dc.Close()
//
//	go func() {
//		if err := dc.Run(context.Background()); err != nil {
//			log.Printf("durable drain stopped: %v", err)
//		}
//	}()
//
//	err = dc.Enqueue(ctx, remotewrite.DurableRequest{
//		TenantID:  "tenant-a",
//		BodyBytes: payload,
//	})
//
// Durable delivery is at-least-once. If an upstream accepts a payload and the
// process exits before the local record is removed, the payload may be replayed
// after restart.
//
// # Retries
//
// Retries are disabled by default (streaming mode). To enable retries:
//
//	client, err := remotewrite.New(remotewrite.Config{
//		UpstreamURL: "http://localhost:9090/api/v1/write",
//		Retry: &remotewrite.RetryConfig{
//			MaxRetries: 3,
//			MinWait:    time.Second,
//			MaxWait:    30 * time.Second,
//		},
//	})
//
// Retries occur on 429 (Too Many Requests) and 5xx responses. The client
// honors Retry-After headers and uses exponential backoff with jitter.
package remotewrite
