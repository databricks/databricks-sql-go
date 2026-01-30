package telemetry

import (
	"context"
	"time"
)

// Interceptor wraps driver operations to collect metrics.
// Exported for use by the driver package.
type Interceptor struct {
	aggregator *metricsAggregator
	enabled    bool
}

// metricContext holds metric collection state in context.
type metricContext struct {
	statementID string
	startTime   time.Time
	tags        map[string]interface{}
}

type contextKey int

const metricContextKey contextKey = 0

// newInterceptor creates a new telemetry interceptor.
func newInterceptor(aggregator *metricsAggregator, enabled bool) *Interceptor {
	return &Interceptor{
		aggregator: aggregator,
		enabled:    enabled,
	}
}

// withMetricContext adds metric context to the context.
func withMetricContext(ctx context.Context, mc *metricContext) context.Context {
	return context.WithValue(ctx, metricContextKey, mc)
}

// getMetricContext retrieves metric context from the context.
func getMetricContext(ctx context.Context) *metricContext {
	if mc, ok := ctx.Value(metricContextKey).(*metricContext); ok {
		return mc
	}
	return nil
}

// BeforeExecute is called before statement execution.
// Returns a new context with metric tracking attached.
// Exported for use by the driver package.
func (i *Interceptor) BeforeExecute(ctx context.Context, statementID string) context.Context {
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
			// Log at trace level only
			// logger.Trace().Msgf("telemetry: afterExecute panic: %v", r)
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
func (i *Interceptor) recordConnection(ctx context.Context, tags map[string]interface{}) {
	if !i.enabled {
		return
	}

	defer func() {
		if r := recover(); r != nil {
			// Log at trace level only
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

// Close shuts down the interceptor and flushes pending metrics.
// Exported for use by the driver package.
func (i *Interceptor) Close(ctx context.Context) error {
	if !i.enabled {
		return nil
	}

	return i.aggregator.close(ctx)
}
