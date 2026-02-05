package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// telemetryExporter exports metrics to Databricks telemetry service.
type telemetryExporter struct {
	host           string
	port           int
	httpPath       string
	driverVersion  string
	httpClient     *http.Client
	circuitBreaker *circuitBreaker
	cfg            *Config
	// Connection parameters for telemetry
	connParams *DriverConnectionParameters
}

// telemetryMetric represents a metric to export.
type telemetryMetric struct {
	metricType  string
	timestamp   time.Time
	workspaceID string
	sessionID   string
	statementID string
	latencyMs   int64
	errorType   string
	tags        map[string]interface{}
}

// newTelemetryExporter creates a new exporter.
func newTelemetryExporter(host string, port int, httpPath string, driverVersion string, httpClient *http.Client, cfg *Config, connParams *DriverConnectionParameters) *telemetryExporter {
	// Build connection parameters if not provided
	if connParams == nil {
		connParams = &DriverConnectionParameters{
			Host:     host,
			Port:     port,
			HTTPPath: httpPath,
		}
	}

	return &telemetryExporter{
		host:           host,
		port:           port,
		httpPath:       httpPath,
		driverVersion:  driverVersion,
		httpClient:     httpClient,
		circuitBreaker: getCircuitBreakerManager().getCircuitBreaker(host),
		cfg:            cfg,
		connParams:     connParams,
	}
}

// export exports metrics to Databricks service.
// All errors are swallowed to ensure telemetry never impacts driver operation.
func (e *telemetryExporter) export(ctx context.Context, metrics []*telemetryMetric) {
	// Swallow all errors and panics
	defer func() {
		if r := recover(); r != nil {
			// Intentionally swallow panic - telemetry must not impact driver
			_ = r // Log at trace level only: logger.Trace().Msgf("telemetry: export panic: %v", r)
		}
	}()

	// Check circuit breaker
	err := e.circuitBreaker.execute(ctx, func() error {
		return e.doExport(ctx, metrics)
	})

	if err == ErrCircuitOpen {
		// Drop metrics silently when circuit is open
		return
	}

	if err != nil {
		// Intentionally swallow error - telemetry must not impact driver
		_ = err // Log at trace level only: logger.Trace().Msgf("telemetry: export error: %v", err)
	}
}

// doExport performs the actual export with retries and exponential backoff.
func (e *telemetryExporter) doExport(ctx context.Context, metrics []*telemetryMetric) error {
	// Create telemetry request with protoLogs format (matches JDBC/Node.js)
	payload, err := createTelemetryRequest(metrics, e.driverVersion, e.connParams)
	if err != nil {
		return fmt.Errorf("failed to create telemetry request: %w", err)
	}

	// Serialize request
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal telemetry request: %w", err)
	}

	// TODO: Remove debug logging
	fmt.Printf("[TELEMETRY DEBUG] Payload: %s\n", string(data))

	// Determine endpoint
	// Support both plain hosts and full URLs (for testing)
	var endpoint string
	if strings.HasPrefix(e.host, "http://") || strings.HasPrefix(e.host, "https://") {
		endpoint = fmt.Sprintf("%s/telemetry-ext", e.host)
	} else {
		endpoint = fmt.Sprintf("https://%s/telemetry-ext", e.host)
	}

	// TODO: Remove debug logging
	fmt.Printf("[TELEMETRY DEBUG] Exporting %d metrics to %s\n", len(metrics), endpoint)
	fmt.Printf("[TELEMETRY DEBUG] ProtoLogs count: %d\n", len(payload.ProtoLogs))

	// Retry logic with exponential backoff
	maxRetries := e.cfg.MaxRetries
	for attempt := 0; attempt <= maxRetries; attempt++ {
		// Exponential backoff (except for first attempt)
		if attempt > 0 {
			backoff := time.Duration(1<<uint(attempt-1)) * e.cfg.RetryDelay
			select {
			case <-time.After(backoff):
				// Backoff completed, continue to retry
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		// Create request
		req, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewReader(data))
		if err != nil {
			return fmt.Errorf("failed to create request: %w", err)
		}

		req.Header.Set("Content-Type", "application/json")

		// Execute request
		resp, err := e.httpClient.Do(req)
		if err != nil {
			if attempt == maxRetries {
				return fmt.Errorf("failed after %d retries: %w", maxRetries, err)
			}
			continue
		}

		// Close body to allow connection reuse
		resp.Body.Close()

		// Check status code
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			// TODO: Remove debug logging
			fmt.Printf("[TELEMETRY DEBUG] Export successful: %d metrics sent, HTTP %d\n", len(metrics), resp.StatusCode)
			return nil // Success
		}

		// TODO: Remove debug logging
		fmt.Printf("[TELEMETRY DEBUG] Export failed: HTTP %d (attempt %d/%d)\n", resp.StatusCode, attempt+1, maxRetries+1)

		// Check if retryable
		if !isRetryableStatus(resp.StatusCode) {
			return fmt.Errorf("non-retryable status: %d", resp.StatusCode)
		}

		if attempt == maxRetries {
			return fmt.Errorf("failed after %d retries: status %d", maxRetries, resp.StatusCode)
		}
	}

	return nil
}

// isRetryableStatus returns true if HTTP status is retryable.
// Retryable statuses: 429 (Too Many Requests), 503 (Service Unavailable), 5xx (Server Errors)
func isRetryableStatus(status int) bool {
	return status == 429 || status == 503 || status >= 500
}
