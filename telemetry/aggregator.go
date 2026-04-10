package telemetry

import (
	"context"
	"sync"
	"time"

	"github.com/databricks/databricks-sql-go/logger"
)

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

	closeOnce sync.Once
	ctx       context.Context    // Cancellable context for in-flight exports
	cancel    context.CancelFunc // Cancels ctx on close
	exportSem chan struct{}      // Bounds concurrent export goroutines
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
	ctx, cancel := context.WithCancel(context.Background())
	agg := &metricsAggregator{
		statements:    make(map[string]*statementMetrics),
		batch:         make([]*telemetryMetric, 0, cfg.BatchSize),
		exporter:      exporter,
		batchSize:     cfg.BatchSize,
		flushInterval: cfg.FlushInterval,
		stopCh:        make(chan struct{}),
		ctx:           ctx,
		cancel:        cancel,
		exportSem:     make(chan struct{}, 8), // Bound to 8 concurrent exports
	}

	// Start background flush timer
	go agg.flushLoop()

	return agg
}

// recordMetric records a metric for aggregation.
func (agg *metricsAggregator) recordMetric(ctx context.Context, metric *telemetryMetric) {
	// Swallow all errors
	defer func() {
		if r := recover(); r != nil {
			// Log at trace level only
			// logger.Trace().Msgf("telemetry: recordMetric panic: %v", r)
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
		// Operation events batch normally to avoid a goroutine-per-call if more
		// operation types are recorded per connection in the future.
		agg.batch = append(agg.batch, metric)
		if len(agg.batch) >= agg.batchSize {
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
			// Log at trace level only
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

// flush flushes pending metrics to exporter.
func (agg *metricsAggregator) flush(ctx context.Context) {
	agg.mu.Lock()
	defer agg.mu.Unlock()
	agg.flushUnlocked(ctx)
}

// flushUnlocked flushes without locking (caller must hold lock).
func (agg *metricsAggregator) flushUnlocked(ctx context.Context) {
	if len(agg.batch) == 0 {
		return
	}

	// Copy batch and clear
	metrics := make([]*telemetryMetric, len(agg.batch))
	copy(metrics, agg.batch)
	agg.batch = agg.batch[:0]

	// Acquire semaphore slot; skip export if already at capacity to prevent goroutine leaks
	select {
	case agg.exportSem <- struct{}{}:
	default:
		logger.Debug().Msg("telemetry: export semaphore full, dropping metrics batch")
		return
	}

	// Export asynchronously
	go func() {
		defer func() {
			<-agg.exportSem
			if r := recover(); r != nil {
				// Log at trace level only
			}
		}()
		agg.exporter.export(ctx, metrics)
	}()
}

// close stops the aggregator and flushes pending metrics.
// Safe to call multiple times — subsequent calls are no-ops for the stop/cancel step.
func (agg *metricsAggregator) close(ctx context.Context) error {
	agg.closeOnce.Do(func() {
		close(agg.stopCh)
		agg.cancel() // Cancel in-flight periodic export goroutines
	})
	agg.flush(ctx)
	return nil
}

// simpleError is a simple error implementation for testing.
type simpleError struct {
	msg string
}

func (e *simpleError) Error() string {
	return e.msg
}
