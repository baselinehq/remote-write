// Package remotewrite provides a high-performance client for the Prometheus
// remote_write protocol. It supports both forwarding existing remote_write
// requests (proxy mode) and pushing metrics from a prometheus.Gatherer.
//
//
// # Forward API Example
//
// Forward an incoming remote_write request to an upstream endpoint:
//
//	client, err := remotewrite.New(remotewrite.Config{
//		UpstreamURL:  "http://victoriametrics:8428/api/v1/write",
//		TenantHeader: "X-Scope-OrgID",
//		BearerToken:  "secret-token",
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
// # Push API Example
//
// Push metrics from a prometheus.Gatherer to a remote_write endpoint:
//
//	client, err := remotewrite.New(remotewrite.Config{
//		UpstreamURL: "http://victoriametrics:8428/api/v1/write",
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
// # Retries
//
// Retries are disabled by default (streaming mode). To enable retries:
//
//	client, err := remotewrite.New(remotewrite.Config{
//		UpstreamURL: "http://upstream:8428/api/v1/write",
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
