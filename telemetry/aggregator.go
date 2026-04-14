package telemetry

import (
	"context"
	"sync"
	"time"

	"github.com/databricks/databricks-sql-go/logger"
)

const (
	exportWorkerCount = 10   // Fixed worker pool size — matches JDBC's newFixedThreadPool(10)
	exportQueueSize   = 1000 // Buffered queue — effectively unbounded for normal workloads
)

// exportJob holds a batch of metrics pending async export.
type exportJob struct {
	ctx     context.Context
	metrics []*telemetryMetric
}

// metricsAggregator aggregates metrics by statement and batches for export.
type metricsAggregator struct {
	mu sync.RWMutex

	statements map[string]*statementMetrics
	batch      []*telemetryMetric
	exporter   *telemetryExporter

	batchSize     int
	flushInterval time.Duration
	stopCh        chan struct{}
	flushTimer    *time.Ticker

	closeOnce   sync.Once
	ctx         context.Context // Cancellable context — cancelled on close to stop workers
	cancel      context.CancelFunc
	exportQueue chan exportJob // Worker queue; drop batch only when full (matches JDBC LinkedBlockingQueue)
	inFlight    sync.WaitGroup // tracks jobs submitted to exportQueue but not yet exported
}

// statementMetrics holds aggregated metrics for a statement.
type statementMetrics struct {
	statementID     string
	sessionID       string
	totalLatency    time.Duration
	chunkCount      int
	bytesDownloaded int64
	pollCount       int
	errors          []string
	tags            map[string]interface{}
}

// newMetricsAggregator creates a new metrics aggregator.
func newMetricsAggregator(exporter *telemetryExporter, cfg *Config) *metricsAggregator {
	ctx, cancel := context.WithCancel(context.Background()) //nolint:gosec // cancel stored in agg.cancel and called on close
	agg := &metricsAggregator{
		statements:    make(map[string]*statementMetrics),
		batch:         make([]*telemetryMetric, 0, cfg.BatchSize),
		exporter:      exporter,
		batchSize:     cfg.BatchSize,
		flushInterval: cfg.FlushInterval,
		stopCh:        make(chan struct{}),
		ctx:           ctx,
		cancel:        cancel,
		exportQueue:   make(chan exportJob, exportQueueSize),
	}

	// Start fixed worker pool — matches JDBC's newFixedThreadPool(10)
	for i := 0; i < exportWorkerCount; i++ {
		go agg.exportWorker()
	}

	// Start background flush timer
	go agg.flushLoop()

	return agg
}

// exportWorker processes export jobs from the queue until the aggregator is cancelled.
func (agg *metricsAggregator) exportWorker() {
	for {
		select {
		case job, ok := <-agg.exportQueue:
			if !ok {
				return
			}
			func() {
				defer agg.inFlight.Done()
				agg.exporter.export(job.ctx, job.metrics)
			}()
		case <-agg.ctx.Done():
			return
		}
	}
}

// recordMetric records a metric for aggregation.
func (agg *metricsAggregator) recordMetric(ctx context.Context, metric *telemetryMetric) {
	// Swallow all errors
	defer func() {
		if r := recover(); r != nil {
			logger.Trace().Msgf("telemetry: recordMetric panic: %v", r)
		}
	}()

	agg.mu.Lock()
	defer agg.mu.Unlock()

	switch metric.metricType {
	case "connection":
		// Connection events flush immediately: lifecycle events must be captured before
		// the connection closes, as we won't have another opportunity to flush.
		agg.batch = append(agg.batch, metric)
		agg.flushUnlocked(ctx)

	case "operation":
		agg.batch = append(agg.batch, metric)
		// Terminal operations (session/statement close) flush immediately so metrics
		// are not lost if the connection closes before the next batch flush — matching
		// JDBC behavior where CLOSE_STATEMENT and DELETE_SESSION trigger immediate export.
		opType, _ := metric.tags["operation_type"].(string)
		if isTerminalOperationType(opType) || len(agg.batch) >= agg.batchSize {
			agg.flushUnlocked(ctx)
		}

	case "statement":
		// Aggregate by statement ID
		stmt, exists := agg.statements[metric.statementID]
		if !exists {
			stmt = &statementMetrics{
				statementID: metric.statementID,
				sessionID:   metric.sessionID,
				tags:        make(map[string]interface{}),
			}
			agg.statements[metric.statementID] = stmt
		}

		// Update aggregated values
		stmt.totalLatency += time.Duration(metric.latencyMs) * time.Millisecond
		if chunkCount, ok := metric.tags["chunk_count"].(int); ok {
			stmt.chunkCount += chunkCount
		}
		if bytes, ok := metric.tags["bytes_downloaded"].(int64); ok {
			stmt.bytesDownloaded += bytes
		}
		if pollCount, ok := metric.tags["poll_count"].(int); ok {
			stmt.pollCount += pollCount
		}

		// Store error if present
		if metric.errorType != "" {
			stmt.errors = append(stmt.errors, metric.errorType)
		}

		// Merge tags
		for k, v := range metric.tags {
			stmt.tags[k] = v
		}

	case "error":
		// Check if terminal error
		if metric.errorType != "" && isTerminalError(&simpleError{msg: metric.errorType}) {
			// Flush terminal errors immediately
			agg.batch = append(agg.batch, metric)
			agg.flushUnlocked(ctx)
		} else {
			// Buffer non-terminal errors with statement
			if stmt, exists := agg.statements[metric.statementID]; exists {
				stmt.errors = append(stmt.errors, metric.errorType)
			}
		}
	}
}

// completeStatement marks a statement as complete and emits aggregated metric.
func (agg *metricsAggregator) completeStatement(ctx context.Context, statementID string, failed bool) {
	defer func() {
		if r := recover(); r != nil {
			logger.Trace().Msgf("telemetry: completeStatement panic: %v", r)
		}
	}()

	agg.mu.Lock()
	defer agg.mu.Unlock()

	stmt, exists := agg.statements[statementID]
	if !exists {
		return
	}
	delete(agg.statements, statementID)

	// Create aggregated metric
	metric := &telemetryMetric{
		metricType:  "statement",
		timestamp:   time.Now(),
		statementID: stmt.statementID,
		sessionID:   stmt.sessionID,
		latencyMs:   stmt.totalLatency.Milliseconds(),
		tags:        stmt.tags,
	}

	// Add aggregated counts
	metric.tags["chunk_count"] = stmt.chunkCount
	metric.tags["bytes_downloaded"] = stmt.bytesDownloaded
	metric.tags["poll_count"] = stmt.pollCount

	// Add error information if failed
	if failed && len(stmt.errors) > 0 {
		// Use the first error as the primary error type
		metric.errorType = stmt.errors[0]
	}

	agg.batch = append(agg.batch, metric)

	// Flush if batch full
	if len(agg.batch) >= agg.batchSize {
		agg.flushUnlocked(ctx)
	}
}

// flushLoop runs periodic flush in background.
func (agg *metricsAggregator) flushLoop() {
	agg.flushTimer = time.NewTicker(agg.flushInterval)
	defer agg.flushTimer.Stop()

	for {
		select {
		case <-agg.flushTimer.C:
			agg.flush(agg.ctx) // Use cancellable context so exports stop on shutdown
		case <-agg.stopCh:
			return
		}
	}
}

// flush flushes pending metrics to exporter asynchronously (fire-and-forget).
func (agg *metricsAggregator) flush(ctx context.Context) {
	agg.mu.Lock()
	defer agg.mu.Unlock()
	agg.flushUnlocked(ctx)
}

// flushSync flushes pending metrics synchronously, blocking until the export
// completes. Used on connection close to guarantee delivery before returning.
func (agg *metricsAggregator) flushSync(ctx context.Context) {
	agg.mu.Lock()
	if len(agg.batch) == 0 {
		agg.mu.Unlock()
		return
	}
	metrics := make([]*telemetryMetric, len(agg.batch))
	copy(metrics, agg.batch)
	agg.batch = agg.batch[:0]
	agg.mu.Unlock()

	agg.exporter.export(ctx, metrics)
}

// flushUnlocked submits the current batch to the export worker queue (caller must hold lock).
// Matches JDBC's executorService.submit() — drops only when the queue is full, not merely
// when workers are busy. Workers drain the queue until the aggregator context is cancelled.
func (agg *metricsAggregator) flushUnlocked(ctx context.Context) {
	if len(agg.batch) == 0 {
		return
	}

	metrics := make([]*telemetryMetric, len(agg.batch))
	copy(metrics, agg.batch)
	agg.batch = agg.batch[:0]

	// Increment before send so close()'s inFlight.Wait() sees the job
	// even if a worker picks it up before the drain step runs.
	agg.inFlight.Add(1)
	select {
	case agg.exportQueue <- exportJob{ctx: ctx, metrics: metrics}:
	default:
		// Queue full — drop batch silently (matches JDBC's RejectedExecutionException path)
		agg.inFlight.Done() // undo Add — job was dropped, not queued
		logger.Debug().Msg("telemetry: export queue full, dropping metrics batch")
	}
}

// close stops the aggregator and flushes pending metrics.
// Safe to call multiple times — subsequent calls are no-ops (closeOnce).
//
// Shutdown order matters:
//  1. Stop periodic flush (close stopCh) so no new async flushes are queued.
//  2. Flush agg.batch synchronously (direct export, bypasses worker queue).
//  3. Drain agg.exportQueue — export any jobs still sitting in the queue
//     synchronously, bypassing workers (matches their inFlight.Done() call).
//  4. Wait for jobs already picked up by workers to finish their HTTP exports.
//     Without this step, cancel() in step 5 could fire while a worker is
//     mid-export, causing EXECUTE_STATEMENT/CLOSE_STATEMENT to be silently lost.
//  5. Cancel the aggregator context to stop the 10 export worker goroutines.
func (agg *metricsAggregator) close(ctx context.Context) error {
	agg.closeOnce.Do(func() {
		close(agg.stopCh)  // 1. Stop periodic flush loop
		agg.flushSync(ctx) // 2. Flush agg.batch directly

		// 3. Drain any jobs still sitting in the exportQueue synchronously.
		// Each job was counted by inFlight.Add(1) in flushUnlocked; call Done()
		// here to match, since the worker won't process these jobs.
		for {
			select {
			case job := <-agg.exportQueue:
				agg.exporter.export(job.ctx, job.metrics)
				agg.inFlight.Done()
			default:
				goto drained
			}
		}
	drained:
		// 4. Wait for jobs already picked up by workers before the drain above.
		// Workers call inFlight.Done() after their HTTP export completes.
		// Use a goroutine + select so we respect the caller's context deadline;
		// without this, a hung HTTP export would block conn.Close() indefinitely.
		waitCh := make(chan struct{})
		go func() { agg.inFlight.Wait(); close(waitCh) }()
		select {
		case <-waitCh:
		case <-ctx.Done():
			logger.Debug().Msg("telemetry: close timed out waiting for in-flight exports")
		}

		agg.cancel() // 5. Stop export workers (all in-flight exports complete)
	})
	return nil
}

// simpleError is a simple error implementation for testing.
type simpleError struct {
	msg string
}

func (e *simpleError) Error() string {
	return e.msg
}
