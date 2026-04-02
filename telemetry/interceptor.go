package telemetry

import (
	"context"
	"time"

	"github.com/databricks/databricks-sql-go/logger"
)

// Interceptor wraps driver operations to collect metrics.
// Exported for use by the driver package.
type Interceptor struct {
	aggregator *metricsAggregator
	enabled    bool
}

// metricContext holds metric collection state in context.
//
//nolint:unused // Will be used in Phase 8+
type metricContext struct {
	sessionID   string
	statementID string
	startTime   time.Time
	tags        map[string]interface{}
}

//nolint:unused // Will be used in Phase 8+
type contextKey int

//nolint:unused // Will be used in Phase 8+
const metricContextKey contextKey = 0

// newInterceptor creates a new telemetry interceptor.
func newInterceptor(aggregator *metricsAggregator, enabled bool) *Interceptor {
	return &Interceptor{
		aggregator: aggregator,
		enabled:    enabled,
	}
}

// withMetricContext adds metric context to the context.
//
//nolint:unused // Will be used in Phase 8+
func withMetricContext(ctx context.Context, mc *metricContext) context.Context {
	return context.WithValue(ctx, metricContextKey, mc)
}

// getMetricContext retrieves metric context from the context.
//
//nolint:unused // Will be used in Phase 8+
func getMetricContext(ctx context.Context) *metricContext {
	if mc, ok := ctx.Value(metricContextKey).(*metricContext); ok {
		return mc
	}
	return nil
}

// BeforeExecute is called before statement execution.
// Returns a new context with metric tracking attached.
// Exported for use by the driver package.
func (i *Interceptor) BeforeExecute(ctx context.Context, sessionID string, statementID string) context.Context {
	if !i.enabled {
		return ctx
	}

	mc := &metricContext{
		sessionID:   sessionID,
		statementID: statementID,
		startTime:   time.Now(),
		tags:        make(map[string]interface{}),
	}

	return withMetricContext(ctx, mc)
}

// BeforeExecuteWithTime is called before statement execution with a custom start time.
// This is useful when the statement ID is not known until after execution starts.
// Exported for use by the driver package.
func (i *Interceptor) BeforeExecuteWithTime(ctx context.Context, sessionID string, statementID string, startTime time.Time) context.Context {
	if !i.enabled {
		return ctx
	}

	mc := &metricContext{
		sessionID:   sessionID,
		statementID: statementID,
		startTime:   startTime,
		tags:        make(map[string]interface{}),
	}

	return withMetricContext(ctx, mc)
}

// AfterExecute is called after statement execution.
// Records the metric with timing and error information.
// Exported for use by the driver package.
func (i *Interceptor) AfterExecute(ctx context.Context, err error) {
	if !i.enabled {
		return
	}

	mc := getMetricContext(ctx)
	if mc == nil {
		return
	}

	// Swallow all panics
	defer func() {
		if r := recover(); r != nil {
			logger.Debug().Msgf("telemetry: afterExecute panic: %v", r)
		}
	}()

	metric := &telemetryMetric{
		metricType:  "statement",
		timestamp:   mc.startTime,
		sessionID:   mc.sessionID,
		statementID: mc.statementID,
		latencyMs:   time.Since(mc.startTime).Milliseconds(),
		tags:        mc.tags,
	}

	if err != nil {
		metric.errorType = classifyError(err)
	}

	// Non-blocking send to aggregator
	i.aggregator.recordMetric(ctx, metric)
}

// AddTag adds a tag to the current metric context.
// Exported for use by the driver package.
func (i *Interceptor) AddTag(ctx context.Context, key string, value interface{}) {
	if !i.enabled {
		return
	}

	mc := getMetricContext(ctx)
	if mc != nil {
		mc.tags[key] = value
	}
}

// recordConnection records a connection event.
//
//nolint:unused // Will be used in Phase 8+
func (i *Interceptor) recordConnection(ctx context.Context, tags map[string]interface{}) {
	if !i.enabled {
		return
	}

	defer func() {
		if r := recover(); r != nil {
			logger.Debug().Msgf("telemetry: recordConnection panic: %v", r)
		}
	}()

	metric := &telemetryMetric{
		metricType: "connection",
		timestamp:  time.Now(),
		tags:       tags,
	}

	i.aggregator.recordMetric(ctx, metric)
}

// CompleteStatement marks a statement as complete and flushes aggregated metrics.
// Exported for use by the driver package.
func (i *Interceptor) CompleteStatement(ctx context.Context, statementID string, failed bool) {
	if !i.enabled {
		return
	}

	i.aggregator.completeStatement(ctx, statementID, failed)
}

// RecordOperation records an operation with type and latency.
// Exported for use by the driver package.
func (i *Interceptor) RecordOperation(ctx context.Context, sessionID string, operationType string, latencyMs int64) {
	if !i.enabled {
		return
	}

	defer func() {
		if r := recover(); r != nil {
			// Silently handle panics
		}
	}()

	metric := &telemetryMetric{
		metricType: "operation",
		timestamp:  time.Now(),
		sessionID:  sessionID,
		latencyMs:  latencyMs,
		tags:       map[string]interface{}{"operation_type": operationType},
	}

	i.aggregator.recordMetric(ctx, metric)
}

// Close flushes any pending per-connection metrics.
// Does NOT close the shared aggregator — its lifecycle is managed via
// ReleaseForConnection, which uses reference counting across all connections
// to the same host.
// Exported for use by the driver package.
func (i *Interceptor) Close(ctx context.Context) error {
	if !i.enabled {
		return nil
	}

	i.aggregator.flush(ctx)
	return nil
}
