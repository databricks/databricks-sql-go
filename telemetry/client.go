package telemetry

import (
	"net/http"
	"sync"
)

// telemetryClient represents a client for sending telemetry data to Databricks.
// This is a minimal stub implementation that will be fully implemented in Phase 4.
type telemetryClient struct {
	host       string
	httpClient *http.Client
	cfg        *Config
	mu         sync.Mutex
	started    bool
	closed     bool
}

// newTelemetryClient creates a new telemetry client for the given host.
func newTelemetryClient(host string, httpClient *http.Client, cfg *Config) *telemetryClient {
	return &telemetryClient{
		host:       host,
		httpClient: httpClient,
		cfg:        cfg,
	}
}

// start starts the telemetry client's background operations.
// This is a stub implementation that will be fully implemented in Phase 4.
func (c *telemetryClient) start() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.started = true
	return nil
}

// close stops the telemetry client and flushes any pending data.
// This is a stub implementation that will be fully implemented in Phase 4.
func (c *telemetryClient) close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	return nil
}
