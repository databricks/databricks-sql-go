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

// beforeExecute is called before statement execution.
// Returns a new context with metric tracking attached.
//
//nolint:unused // Will be used in Phase 8+
func (i *Interceptor) beforeExecute(ctx context.Context, statementID string) context.Context {
	if !i.enabled {
		return ctx
	}

	mc := &metricContext{
		statementID: statementID,
		startTime:   time.Now(),
		tags:        make(map[string]interface{}),
	}

	return withMetricContext(ctx, mc)
}

// afterExecute is called after statement execution.
// Records the metric with timing and error information.
//
//nolint:unused // Will be used in Phase 8+
func (i *Interceptor) afterExecute(ctx context.Context, err error) {
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

// addTag adds a tag to the current metric context.
//
//nolint:unused // Will be used in Phase 8+
func (i *Interceptor) addTag(ctx context.Context, key string, value interface{}) {
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

// completeStatement marks a statement as complete and flushes aggregated metrics.
//
//nolint:unused // Will be used in Phase 8+
func (i *Interceptor) completeStatement(ctx context.Context, statementID string, failed bool) {
	if !i.enabled {
		return
	}

	i.aggregator.completeStatement(ctx, statementID, failed)
}

// Close shuts down the interceptor and flushes pending metrics.
// Exported for use by the driver package.
func (i *Interceptor) Close(ctx context.Context) error {
	if !i.enabled {
		return nil
	}

	return i.aggregator.close(ctx)
}
