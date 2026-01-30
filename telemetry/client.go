package telemetry

import (
	"context"
	"net/http"
	"sync"
	"time"
)

// telemetryClient represents a client for sending telemetry data to Databricks.
//
// Thread-Safety and Sharing:
// - One telemetryClient instance is shared across ALL connections to the same host
// - This prevents rate limiting by consolidating telemetry from multiple connections
// - The client MUST be fully thread-safe as it will be accessed concurrently
// - All methods (start, close, and export methods) use proper synchronization
//
// The mu mutex protects the started and closed flags.
// The aggregator handles thread-safe metric collection and batching.
type telemetryClient struct {
	host       string
	httpClient *http.Client
	cfg        *Config

	exporter   *telemetryExporter
	aggregator *metricsAggregator

	mu         sync.Mutex // Protects started and closed flags
	started    bool
	closed     bool
}

// newTelemetryClient creates a new telemetry client for the given host.
func newTelemetryClient(host string, driverVersion string, httpClient *http.Client, cfg *Config) *telemetryClient {
	// Create exporter
	exporter := newTelemetryExporter(host, httpClient, cfg)

	// Create aggregator with exporter
	aggregator := newMetricsAggregator(exporter, cfg)

	return &telemetryClient{
		host:       host,
		httpClient: httpClient,
		cfg:        cfg,
		exporter:   exporter,
		aggregator: aggregator,
	}
}

// start starts the telemetry client's background operations.
// The aggregator starts its background flush timer automatically.
func (c *telemetryClient) start() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.started {
		return nil
	}

	c.started = true
	// Aggregator already started in newTelemetryClient
	return nil
}

// close stops the telemetry client and flushes any pending data.
// Provides graceful shutdown with a timeout to flush pending metrics.
func (c *telemetryClient) close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()

	// Flush pending metrics with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return c.aggregator.close(ctx)
}

// GetInterceptor returns a new interceptor for a connection.
// Each connection gets its own interceptor, but they all share the same aggregator.
// Exported for use by the driver package.
func (c *telemetryClient) GetInterceptor(enabled bool) *Interceptor {
	return newInterceptor(c.aggregator, enabled)
}
