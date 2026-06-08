// Durable spool mode uses flock(2) and is unix-only.

//go:build unix

package remotewrite

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/persistentqueue"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	ErrDurableClientClosed = errors.New("durable client is closed")
	ErrDurableRunStarted   = errors.New("durable client run loop has already started")
	ErrDurableQueueBlocked = errors.New("durable queue is blocked")
	ErrDurableSpoolLocked  = errors.New("durable spool is already locked")
	ErrDurableRecordCorrupt = errors.New("durable record is corrupt")
)

const (
	defaultDurableQueueName      = "remote-write"
	defaultDurableInMemoryBlocks = 128
	durableRecordExtension       = ".record"
	durableTempExtension         = ".tmp"
)

// DurableConfig configures the persistent remote_write spooler.
type DurableConfig struct {
	// Client configures the underlying HTTP transport.
	Client Config

	// QueueDir is the base directory containing durable queue state.
	QueueDir string
	// QueueName scopes queue files within QueueDir.
	QueueName string
	// MaxInMemoryBlocks limits how many record references remain in memory before the
	// internal scheduler spills them to files.
	MaxInMemoryBlocks int
	// MaxPendingBytes bounds the total size of accepted spool records on disk.
	// When this budget is exceeded, Enqueue returns ErrDurableQueueBlocked.
	MaxPendingBytes int64
	// DisablePersistence disables normal file spillover for the internal scheduling queue.
	// Accepted durable spool records are still written to disk, and their scheduler
	// references may still be written during recovery or after acceptance.
	DisablePersistence bool

	// SendConcurrency controls how many queued payloads may be sent in parallel.
	SendConcurrency int

	// Logger receives durable queue lifecycle and failure logs.
	Logger *slog.Logger
	// Registerer receives durable Prometheus metrics. If nil, metrics are created but not registered.
	Registerer prometheus.Registerer
}

// DurableClient persists encoded remote_write payloads locally and drains
// them in the background.
//
// Accepted payloads are stored as record files under QueueDir, and the
// scheduler queue is used only to schedule record IDs for background
// delivery. This keeps accepted data durable even though the scheduler's read
// path is destructive.
//
// Retry policy. The drain loop retries retryable failures indefinitely with
// exponential backoff (RetryConfig.MinWait → MaxWait, plus jitter). It
// honours Retry-After when present. RetryConfig.MaxRetries from the wrapped
// Client config is intentionally ignored — durable payloads have already
// been accepted on disk, so dropping them after N attempts would be
// surprising. Use Close (or cancel Run's context) to stop draining.
//
// Platform support. Durable mode acquires an exclusive flock on the spool
// directory and is therefore Unix-only (Linux, macOS, *BSD). It is not
// supported on Windows.
type DurableClient struct {
	client *Client
	queue  *persistentqueue.FastQueue
	lockF  *os.File

	logger      *slog.Logger
	retry       RetryConfig
	metrics     *durableMetrics
	recordsDir  string
	corruptDir  string
	defaultType string
	defaultEnc  string
	defaultVer  string

	sendConcurrency int
	maxPendingBytes int64

	nextRecordID atomic.Uint64
	pendingBytes atomic.Int64
	inflight     atomic.Int64

	lifecycleMu sync.RWMutex
	closeCh     chan struct{}
	closed      atomic.Bool
	started     atomic.Bool
	runWG       sync.WaitGroup
}

type durableRecord struct {
	id   uint64
	path string
	size int64
}

type durableMetrics struct {
	enqueuedTotal          prometheus.Counter
	sentTotal              prometheus.Counter
	sendFailuresTotal      prometheus.Counter
	retryableFailuresTotal prometheus.Counter
	permanentFailuresTotal prometheus.Counter
	droppedTotal           prometheus.Counter
	corruptRecordsTotal    prometheus.Counter
	queueBlocked           prometheus.GaugeFunc
	collectors             []prometheus.Collector
	registerer             prometheus.Registerer
}

// NewDurable creates a durable remote_write client.
func NewDurable(cfg DurableConfig) (*DurableClient, error) {
	if cfg.QueueDir == "" {
		return nil, fmt.Errorf("QueueDir is required")
	}
	cfg = normalizeDurableConfig(cfg)

	rootDir := filepath.Join(cfg.QueueDir, cfg.QueueName)
	recordsDir := filepath.Join(rootDir, "records")
	corruptDir := filepath.Join(rootDir, "corrupt")
	scheduleDir := filepath.Join(rootDir, "schedule")

	if err := os.MkdirAll(recordsDir, 0o700); err != nil {
		return nil, fmt.Errorf("create records dir: %w", err)
	}
	if err := os.MkdirAll(corruptDir, 0o700); err != nil {
		return nil, fmt.Errorf("create corrupt records dir: %w", err)
	}

	lockF, err := acquireSpoolLock(rootDir)
	if err != nil {
		return nil, err
	}
	// The scheduler queue is reconstructed from durable record files on startup, so stale
	// persisted queue state is discarded in order to avoid duplicate deliveries.
	if err := os.RemoveAll(scheduleDir); err != nil {
		_ = releaseSpoolLock(lockF)
		return nil, fmt.Errorf("reset schedule dir: %w", err)
	}

	clientCfg := cfg.Client
	retryCfg := normalizeDurableRetryConfig(clientCfg.Retry)
	clientCfg.Retry = nil

	client, err := New(clientCfg)
	if err != nil {
		_ = releaseSpoolLock(lockF)
		return nil, err
	}

	queue, err := openFastQueue(scheduleDir, cfg.QueueName, cfg.MaxInMemoryBlocks, cfg.DisablePersistence)
	if err != nil {
		_ = client.Close()
		_ = releaseSpoolLock(lockF)
		return nil, err
	}

	records, maxRecordID, totalPendingBytes, err := scanDurableRecords(recordsDir)
	if err != nil {
		_ = closeQueue(queue)
		_ = client.Close()
		_ = releaseSpoolLock(lockF)
		return nil, err
	}

	dc := &DurableClient{
		client:          client,
		queue:           queue,
		lockF:           lockF,
		logger:          cfg.Logger,
		retry:           retryCfg,
		recordsDir:      recordsDir,
		corruptDir:      corruptDir,
		defaultType:     cfg.Client.DefaultContentType,
		defaultEnc:      cfg.Client.DefaultContentEncoding,
		defaultVer:      cfg.Client.DefaultRemoteWriteVersion,
		sendConcurrency: cfg.SendConcurrency,
		maxPendingBytes: cfg.MaxPendingBytes,
		closeCh:         make(chan struct{}),
	}
	dc.nextRecordID.Store(maxRecordID)
	dc.pendingBytes.Store(totalPendingBytes)

	if err := dc.seedQueue(records); err != nil {
		_ = closeQueue(queue)
		_ = client.Close()
		_ = releaseSpoolLock(lockF)
		return nil, err
	}

	metrics, err := newDurableMetrics(dc, cfg.Registerer)
	if err != nil {
		_ = closeQueue(queue)
		_ = client.Close()
		_ = releaseSpoolLock(lockF)
		return nil, err
	}
	dc.metrics = metrics

	if dc.sendConcurrency > 1 {
		dc.logger.Warn("durable remote_write send concurrency above 1 may trigger out-of-order sample rejections on some backends", "send_concurrency", dc.sendConcurrency)
	}
	if dc.maxPendingBytes > 0 {
		dc.logger.Info("durable spool byte budget enabled; enqueue returns ErrDurableQueueBlocked when the budget is exceeded", "max_pending_bytes", dc.maxPendingBytes)
	}

	return dc, nil
}

// Enqueue persists a fully materialized remote_write payload.
func (dc *DurableClient) Enqueue(ctx context.Context, req DurableRequest) error {
	dc.lifecycleMu.RLock()
	defer dc.lifecycleMu.RUnlock()

	if dc.closed.Load() {
		return ErrDurableClientClosed
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if dc.queueBlocked() {
		return ErrDurableQueueBlocked
	}

	normalized := dc.normalizeRequest(req)
	envelope, err := encodeDurableRequestEnvelope(normalized)
	if err != nil {
		return fmt.Errorf("encode durable request envelope: %w", err)
	}

	size := int64(len(envelope))
	if !dc.reservePendingBytes(size) {
		return ErrDurableQueueBlocked
	}

	record, err := dc.writeRecord(envelope)
	if err != nil {
		dc.releasePendingBytes(size)
		return err
	}

	// From this point the payload is accepted. The scheduler queue contains only
	// recoverable record references, so write the reference even when scheduler
	// persistence is disabled instead of returning an ambiguous enqueue error.
	if err := writeQueueBlock(dc.queue, encodeDurableRecordRef(record.id)); err != nil {
		return err
	}
	dc.metrics.enqueuedTotal.Inc()
	return nil
}

// Push gathers metrics, encodes them into remote_write payloads, and enqueues them.
func (dc *DurableClient) Push(ctx context.Context, req PushRequest) error {
	return pushGathered(ctx, req, dc.Enqueue)
}

// PushTimeSeries encodes pre-built series into remote_write payloads and enqueues them.
func (dc *DurableClient) PushTimeSeries(ctx context.Context, req PushTimeSeriesRequest) error {
	return pushTimeSeries(ctx, req, dc.Enqueue)
}

// Run starts draining the durable queue until ctx is cancelled, Close is called, or a fatal error occurs.
// It is intended to be called once for the lifetime of the DurableClient.
func (dc *DurableClient) Run(ctx context.Context) error {
	if dc.closed.Load() {
		return ErrDurableClientClosed
	}
	if !dc.started.CompareAndSwap(false, true) {
		return ErrDurableRunStarted
	}

	runCtx, cancel := context.WithCancel(ctx)
	helperDone := make(chan struct{})

	dc.runWG.Add(1)
	defer dc.runWG.Done()
	defer cancel()

	go func() {
		defer close(helperDone)
		select {
		case <-dc.closeCh:
			cancel()
			dc.queue.UnblockAllReaders()
		case <-ctx.Done():
			cancel()
			dc.queue.UnblockAllReaders()
		case <-runCtx.Done():
		}
	}()

	err := dc.runLoop(runCtx, cancel)
	cancel()
	<-helperDone

	if err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	if dc.closed.Load() {
		return nil
	}

	return err
}

// Close stops the drain loop and closes the underlying queue and transport.
func (dc *DurableClient) Close() error {
	dc.lifecycleMu.Lock()
	if dc.closed.Swap(true) {
		dc.lifecycleMu.Unlock()
		return nil
	}
	close(dc.closeCh)
	dc.lifecycleMu.Unlock()

	dc.runWG.Wait()
	queueErr := closeQueue(dc.queue)
	dc.metrics.unregister()

	return errors.Join(queueErr, dc.client.Close(), releaseSpoolLock(dc.lockF))
}

// openFastQueue, writeQueueBlock, readQueueBlock, and closeQueue wrap the
// VictoriaMetrics persistentqueue Must* APIs so that environmental failures
// (disk full, IO errors, corrupted scheduler state) surface as errors instead
// of crashing the calling process.

func openFastQueue(scheduleDir, queueName string, maxInmemoryBlocks int, disablePersistence bool) (q *persistentqueue.FastQueue, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("open durable scheduler queue %q: %v", scheduleDir, r)
		}
	}()
	q = persistentqueue.MustOpenFastQueue(scheduleDir, queueName, maxInmemoryBlocks, 0, disablePersistence)
	return q, nil
}

func writeQueueBlock(q *persistentqueue.FastQueue, ref []byte) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("write durable scheduler queue: %v", r)
		}
	}()
	q.MustWriteBlockIgnoreDisabledPQ(ref)
	return nil
}

func readQueueBlock(q *persistentqueue.FastQueue, dst []byte) (out []byte, ok bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("read durable scheduler queue: %v", r)
		}
	}()
	out, ok = q.MustReadBlock(dst)
	return out, ok, nil
}

func closeQueue(q *persistentqueue.FastQueue) (err error) {
	if q == nil {
		return nil
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("close durable scheduler queue: %v", r)
		}
	}()
	q.MustClose()
	return nil
}

func (dc *DurableClient) runLoop(ctx context.Context, cancel context.CancelFunc) error {
	workCh := make(chan durableRecord, dc.sendConcurrency)
	errCh := make(chan error, dc.sendConcurrency+1)
	var workerWG sync.WaitGroup

	reportErr := func(err error) {
		if err == nil || errors.Is(err, context.Canceled) {
			return
		}
		select {
		case errCh <- err:
		default:
		}
		cancel()
		dc.queue.UnblockAllReaders()
	}

	for i := 0; i < dc.sendConcurrency; i++ {
		workerWG.Add(1)
		go func() {
			defer workerWG.Done()
			reportErr(dc.worker(ctx, workCh))
		}()
	}

	dispatchDone := make(chan struct{})
	go func() {
		defer close(dispatchDone)
		defer close(workCh)
		reportErr(dc.dispatch(ctx, workCh))
	}()

	var fatalErr error
	select {
	case fatalErr = <-errCh:
	case <-ctx.Done():
	}

	<-dispatchDone
	workerWG.Wait()

	if fatalErr == nil {
		select {
		case fatalErr = <-errCh:
		default:
		}
	}
	if fatalErr != nil {
		return fatalErr
	}
	if dc.closed.Load() {
		return nil
	}

	return ctx.Err()
}

func (dc *DurableClient) dispatch(ctx context.Context, workCh chan<- durableRecord) error {
	var refBuf []byte
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		var (
			ok      bool
			readErr error
		)
		refBuf, ok, readErr = readQueueBlock(dc.queue, refBuf[:0])
		if readErr != nil {
			return readErr
		}
		if !ok {
			if err := ctx.Err(); err != nil {
				return err
			}
			return nil
		}

		recordID, err := decodeDurableRecordRef(refBuf)
		if err != nil {
			return fmt.Errorf("decode durable record ref: %w", err)
		}

		record := durableRecord{
			id:   recordID,
			path: dc.recordPath(recordID),
		}
		if err := dc.submitRecord(ctx, workCh, record); err != nil {
			return err
		}
	}
}

func (dc *DurableClient) submitRecord(ctx context.Context, workCh chan<- durableRecord, record durableRecord) error {
	select {
	case workCh <- record:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (dc *DurableClient) worker(ctx context.Context, workCh <-chan durableRecord) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case record, ok := <-workCh:
			if !ok {
				return nil
			}
			if err := dc.processRecord(ctx, record); err != nil {
				return err
			}
		}
	}
}

func (dc *DurableClient) processRecord(ctx context.Context, record durableRecord) error {
	dc.inflight.Add(1)
	defer dc.inflight.Add(-1)

	record, req, err := dc.readRecord(record)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if errors.Is(err, ErrDurableRecordCorrupt) {
			return dc.quarantineRecord(record, err)
		}
		return err
	}

	delay := dc.retry.MinWait
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		err := dc.client.sendEncoded(ctx, req)
		if err == nil {
			dc.metrics.sentTotal.Inc()
			return dc.removeRecord(record)
		}
		if errors.Is(err, context.Canceled) {
			return err
		}

		dc.metrics.sendFailuresTotal.Inc()

		attemptErr := deliveryAttempt(err)
		if attemptErr != nil && attemptErr.Retryable {
			dc.metrics.retryableFailuresTotal.Inc()
			dc.logger.Debug("durable remote_write send failed and will be retried", "record_id", record.id, "error", attemptErr)

			wait := delay
			if attemptErr.RetryAfter > 0 {
				wait = attemptErr.RetryAfter
			}
			if delay < dc.retry.MaxWait {
				delay *= 2
				if delay > dc.retry.MaxWait {
					delay = dc.retry.MaxWait
				}
			}
			if err := waitForRetry(ctx, addJitter(wait)); err != nil {
				return err
			}
			continue
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return err
		}

		dc.metrics.permanentFailuresTotal.Inc()
		dc.metrics.droppedTotal.Inc()
		dc.logger.Error("dropping durable remote_write request after permanent failure", "record_id", record.id, "error", err)
		return dc.removeRecord(record)
	}
}

func deliveryAttempt(err error) *deliveryError {
	var attemptErr *deliveryError
	if errors.As(err, &attemptErr) {
		return attemptErr
	}

	return nil
}

func (dc *DurableClient) normalizeRequest(req DurableRequest) DurableRequest {
	normalized := req
	if normalized.ContentType == "" {
		normalized.ContentType = dc.defaultType
	}
	if normalized.ContentEncoding == "" {
		normalized.ContentEncoding = dc.defaultEnc
	}
	if normalized.RemoteWriteVersion == "" {
		normalized.RemoteWriteVersion = dc.defaultVer
	}
	
	return normalized
}

func (dc *DurableClient) reservePendingBytes(size int64) bool {
	if dc.maxPendingBytes <= 0 {
		dc.pendingBytes.Add(size)
		return true
	}
	for {
		current := dc.pendingBytes.Load()
		if current+size > dc.maxPendingBytes {
			return false
		}
		if dc.pendingBytes.CompareAndSwap(current, current+size) {
			return true
		}
	}
}

func (dc *DurableClient) releasePendingBytes(size int64) {
	dc.pendingBytes.Add(-size)
}

func (dc *DurableClient) queueBlocked() bool {
	if dc.queue.IsWriteBlocked() {
		return true
	}

	return dc.maxPendingBytes > 0 && dc.pendingBytes.Load() >= dc.maxPendingBytes
}

func (dc *DurableClient) writeRecord(data []byte) (durableRecord, error) {
	recordID := dc.nextRecordID.Add(1)
	record := durableRecord{
		id:   recordID,
		path: dc.recordPath(recordID),
		size: int64(len(data)),
	}
	tmpPath := record.path + durableTempExtension

	file, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL|os.O_TRUNC, 0o600)
	if err != nil {
		return durableRecord{}, fmt.Errorf("create durable record %q: %w", tmpPath, err)
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		_ = os.Remove(tmpPath)
		return durableRecord{}, fmt.Errorf("write durable record %q: %w", tmpPath, err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = os.Remove(tmpPath)
		return durableRecord{}, fmt.Errorf("sync durable record %q: %w", tmpPath, err)
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return durableRecord{}, fmt.Errorf("close durable record %q: %w", tmpPath, err)
	}
	if err := os.Rename(tmpPath, record.path); err != nil {
		_ = os.Remove(tmpPath)
		return durableRecord{}, fmt.Errorf("rename durable record %q: %w", record.path, err)
	}
	if err := syncDirBestEffort(dc.recordsDir); err != nil {
		_ = os.Remove(record.path)
		return durableRecord{}, fmt.Errorf("sync records dir %q: %w", dc.recordsDir, err)
	}

	return record, nil
}

func (dc *DurableClient) readRecord(record durableRecord) (durableRecord, DurableRequest, error) {
	data, err := os.ReadFile(record.path)
	if err != nil {
		return durableRecord{}, DurableRequest{}, err
	}
	record.size = int64(len(data))

	req, err := decodeDurableRequestEnvelope(data)
	if err != nil {
		return record, DurableRequest{}, fmt.Errorf("%w: decode durable record %q: %v", ErrDurableRecordCorrupt, record.path, err)
	}

	return record, req, nil
}

func (dc *DurableClient) removeRecord(record durableRecord) error {
	err := os.Remove(record.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("remove durable record %q: %w", record.path, err)
	}
	dc.releasePendingBytes(record.size)
	if err := syncDirBestEffort(dc.recordsDir); err != nil {
		return fmt.Errorf("sync records dir %q: %w", dc.recordsDir, err)
	}

	return nil
}

func (dc *DurableClient) quarantineRecord(record durableRecord, cause error) error {
	target := filepath.Join(dc.corruptDir, filepath.Base(record.path))
	if _, err := os.Stat(target); err == nil {
		target = fmt.Sprintf("%s.%d", target, time.Now().UnixNano())
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("stat corrupt record target %q: %w", target, err)
	}

	if err := os.Rename(record.path, target); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("quarantine corrupt durable record %q: %w", record.path, err)
	}

	dc.releasePendingBytes(record.size)
	dc.metrics.corruptRecordsTotal.Inc()
	dc.metrics.droppedTotal.Inc()
	dc.logger.Error("quarantined corrupt durable remote_write record", "record_id", record.id, "path", target, "error", cause)
	if err := syncDirBestEffort(dc.recordsDir); err != nil {
		dc.logger.Warn("failed to sync records dir after quarantining corrupt record", "record_id", record.id, "error", err)
	}
	if err := syncDirBestEffort(dc.corruptDir); err != nil {
		dc.logger.Warn("failed to sync corrupt records dir after quarantining corrupt record", "record_id", record.id, "error", err)
	}
	
	return nil
}

func (dc *DurableClient) seedQueue(records []durableRecord) error {
	for _, record := range records {
		// Recovery must remain possible even when DisablePersistence is true, so recovered
		// record references are re-seeded with IgnoreDisabledPQ.
		if err := writeQueueBlock(dc.queue, encodeDurableRecordRef(record.id)); err != nil {
			return fmt.Errorf("seed scheduler queue with record %d: %w", record.id, err)
		}
	}
	return nil
}

func (dc *DurableClient) recordPath(recordID uint64) string {
	return filepath.Join(dc.recordsDir, recordFilename(recordID))
}

func scanDurableRecords(recordsDir string) ([]durableRecord, uint64, int64, error) {
	entries, err := os.ReadDir(recordsDir)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("read records dir %q: %w", recordsDir, err)
	}

	records := make([]durableRecord, 0, len(entries))
	var maxRecordID uint64
	var totalBytes int64

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		path := filepath.Join(recordsDir, name)
		if strings.HasSuffix(name, durableTempExtension) {
			_ = os.Remove(path)
			continue
		}

		recordID, ok := parseRecordFilename(name)
		if !ok {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			return nil, 0, 0, fmt.Errorf("stat durable record %q: %w", path, err)
		}

		record := durableRecord{
			id:   recordID,
			path: path,
			size: info.Size(),
		}
		records = append(records, record)
		totalBytes += record.size
		if recordID > maxRecordID {
			maxRecordID = recordID
		}
	}

	sort.Slice(records, func(i, j int) bool {
		return records[i].id < records[j].id
	})

	return records, maxRecordID, totalBytes, nil
}

func normalizeDurableConfig(cfg DurableConfig) DurableConfig {
	if cfg.QueueName == "" {
		cfg.QueueName = defaultDurableQueueName
	}
	if cfg.MaxInMemoryBlocks <= 0 {
		cfg.MaxInMemoryBlocks = defaultDurableInMemoryBlocks
	}
	if cfg.SendConcurrency <= 0 {
		cfg.SendConcurrency = 1
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return cfg
}

func newDurableMetrics(dc *DurableClient, registerer prometheus.Registerer) (*durableMetrics, error) {
	metrics := &durableMetrics{
		enqueuedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "remotewrite_durable_enqueued_total",
			Help: "Total number of remote_write payloads accepted into the durable spool.",
		}),
		sentTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "remotewrite_durable_sent_total",
			Help: "Total number of durable remote_write payloads delivered successfully.",
		}),
		sendFailuresTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "remotewrite_durable_send_failures_total",
			Help: "Total number of durable remote_write send attempts that failed.",
		}),
		retryableFailuresTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "remotewrite_durable_retryable_failures_total",
			Help: "Total number of retryable durable remote_write send failures.",
		}),
		permanentFailuresTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "remotewrite_durable_permanent_failures_total",
			Help: "Total number of durable remote_write payloads dropped after a permanent failure.",
		}),
		droppedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "remotewrite_durable_dropped_total",
			Help: "Total number of durable remote_write payloads dropped after acceptance.",
		}),
		corruptRecordsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "remotewrite_durable_corrupt_records_total",
			Help: "Total number of corrupt durable remote_write records quarantined without delivery.",
		}),
		registerer: registerer,
	}

	queuePendingBytes := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "remotewrite_durable_queue_pending_bytes",
		Help: "Bytes currently pending in durable spool record files.",
	}, func() float64 {
		return float64(dc.pendingBytes.Load())
	})
	queueInmemoryBlocks := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "remotewrite_durable_queue_inmemory_blocks",
		Help: "Number of record references currently buffered in the scheduler queue's in-memory segment.",
	}, func() float64 {
		return float64(dc.queue.GetInmemoryQueueLen())
	})
	queueBlocked := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "remotewrite_durable_queue_blocked",
		Help: "Whether the durable spool is currently unable to accept more data because of byte budget or scheduler backpressure.",
	}, func() float64 {
		if dc.queueBlocked() {
			return 1
		}
		return 0
	})
	metrics.queueBlocked = queueBlocked
	inflight := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "remotewrite_durable_inflight",
		Help: "Number of durable remote_write payloads currently being delivered.",
	}, func() float64 {
		return float64(dc.inflight.Load())
	})

	metrics.collectors = []prometheus.Collector{
		metrics.enqueuedTotal,
		metrics.sentTotal,
		metrics.sendFailuresTotal,
		metrics.retryableFailuresTotal,
		metrics.permanentFailuresTotal,
		metrics.droppedTotal,
		metrics.corruptRecordsTotal,
		queuePendingBytes,
		queueInmemoryBlocks,
		queueBlocked,
		inflight,
	}

	if registerer != nil {
		registered := make([]prometheus.Collector, 0, len(metrics.collectors))
		for _, collector := range metrics.collectors {
			if err := registerer.Register(collector); err != nil {
				for _, prior := range registered {
					registerer.Unregister(prior)
				}
				return nil, err
			}
			registered = append(registered, collector)
		}
	}

	return metrics, nil
}

func (m *durableMetrics) unregister() {
	if m == nil || m.registerer == nil {
		return
	}
	for _, collector := range m.collectors {
		m.registerer.Unregister(collector)
	}
}

func normalizeDurableRetryConfig(cfg *RetryConfig) RetryConfig {
	if cfg == nil {
		return RetryConfig{
			MinWait: time.Second,
			MaxWait: 30 * time.Second,
		}
	}

	normalized := *cfg
	if normalized.MinWait <= 0 {
		normalized.MinWait = time.Second
	}
	if normalized.MaxWait <= 0 {
		normalized.MaxWait = 30 * time.Second
	}
	if normalized.MaxWait < normalized.MinWait {
		normalized.MaxWait = normalized.MinWait
	}

	return normalized
}

func recordFilename(recordID uint64) string {
	return fmt.Sprintf("%016x%s", recordID, durableRecordExtension)
}

func parseRecordFilename(name string) (uint64, bool) {
	if !strings.HasSuffix(name, durableRecordExtension) {
		return 0, false
	}
	rawID := strings.TrimSuffix(name, durableRecordExtension)
	if rawID == "" {
		return 0, false
	}
	recordID, err := strconv.ParseUint(rawID, 16, 64)
	if err != nil {
		return 0, false
	}

	return recordID, true
}

func acquireSpoolLock(rootDir string) (*os.File, error) {
	lockPath := filepath.Join(rootDir, ".lock")
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create durable spool lock %q: %w", lockPath, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, fmt.Errorf("%w: %s", ErrDurableSpoolLocked, rootDir)
		}
		return nil, fmt.Errorf("lock durable spool %q: %w", lockPath, err)
	}

	return f, nil
}

func releaseSpoolLock(f *os.File) error {
	if f == nil {
		return nil
	}

	return errors.Join(syscall.Flock(int(f.Fd()), syscall.LOCK_UN), f.Close())
}

// syncDirBestEffort fsyncs a directory when the platform supports it. Some platforms return
// EINVAL or ENOTSUP for directory syncs; those cases are ignored because there is no stronger
// portable option available here.
func syncDirBestEffort(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	if err := file.Sync(); err != nil {
		if errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.ENOTSUP) {
			return nil
		}
		return err
	}

	return nil
}
