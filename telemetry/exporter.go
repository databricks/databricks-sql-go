package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/databricks/databricks-sql-go/logger"
)

const (
	telemetryEndpointPath = "/telemetry-ext"
	httpPrefix            = "http://"
	httpsPrefix           = "https://"
	defaultScheme         = "https://"
)

// telemetryExporter exports metrics to Databricks telemetry service.
type telemetryExporter struct {
	host           string
	driverVersion  string
	httpClient     *http.Client
	circuitBreaker *circuitBreaker
	cfg            *Config
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

// exportedMetric is a single metric in the payload.
type exportedMetric struct {
	MetricType  string                 `json:"metric_type"`
	Timestamp   string                 `json:"timestamp"` // RFC3339
	WorkspaceID string                 `json:"workspace_id,omitempty"`
	SessionID   string                 `json:"session_id,omitempty"`
	StatementID string                 `json:"statement_id,omitempty"`
	LatencyMs   int64                  `json:"latency_ms,omitempty"`
	ErrorType   string                 `json:"error_type,omitempty"`
	Tags        map[string]interface{} `json:"tags,omitempty"`
}

// ensureHTTPScheme adds https:// prefix to host if no scheme is present.
func ensureHTTPScheme(host string) string {
	if strings.HasPrefix(host, httpPrefix) || strings.HasPrefix(host, httpsPrefix) {
		return host
	}
	return defaultScheme + host
}

// newTelemetryExporter creates a new exporter.
func newTelemetryExporter(host string, driverVersion string, httpClient *http.Client, cfg *Config) *telemetryExporter {
	return &telemetryExporter{
		host:           host,
		driverVersion:  driverVersion,
		httpClient:     httpClient,
		circuitBreaker: getCircuitBreakerManager().getCircuitBreaker(host),
		cfg:            cfg,
	}
}

// export exports metrics to Databricks service.
// All errors are swallowed to ensure telemetry never impacts driver operation.
func (e *telemetryExporter) export(ctx context.Context, metrics []*telemetryMetric) {
	// Swallow all errors and panics
	defer func() {
		if r := recover(); r != nil {
			logger.Trace().Msgf("telemetry: export panic: %v", r)
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
		logger.Trace().Msgf("telemetry: export error: %v", err)
	}
}

// doExport performs the actual export with retries and exponential backoff.
func (e *telemetryExporter) doExport(ctx context.Context, metrics []*telemetryMetric) error {
	// Create telemetry request with base64-encoded logs
	request, err := createTelemetryRequest(metrics, e.driverVersion)
	if err != nil {
		return fmt.Errorf("failed to create telemetry request: %w", err)
	}

	// Serialize request
	data, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	// Determine endpoint
	hostURL := ensureHTTPScheme(e.host)
	endpoint := hostURL + telemetryEndpointPath

	// Retry logic with exponential backoff
	maxRetries := e.cfg.MaxRetries
	for attempt := 0; attempt <= maxRetries; attempt++ {
		// Exponential backoff (except for first attempt)
		if attempt > 0 {
			backoff := time.Duration(1<<uint(attempt-1)) * e.cfg.RetryDelay
			select {
			case <-time.After(backoff):
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

		// Read response body
		_, _ = io.ReadAll(resp.Body)
		resp.Body.Close()

		// Check status code
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil // Success
		}

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

// toExportedMetric converts internal metric to exported format with tag filtering.
func (m *telemetryMetric) toExportedMetric() *exportedMetric {
	// Filter tags based on export scope
	filteredTags := make(map[string]interface{})
	for k, v := range m.tags {
		if shouldExportToDatabricks(m.metricType, k) {
			filteredTags[k] = v
		}
	}

	return &exportedMetric{
		MetricType:  m.metricType,
		Timestamp:   m.timestamp.Format(time.RFC3339),
		WorkspaceID: m.workspaceID,
		SessionID:   m.sessionID,
		StatementID: m.statementID,
		LatencyMs:   m.latencyMs,
		ErrorType:   m.errorType,
		Tags:        filteredTags,
	}
}

// isRetryableStatus returns true if HTTP status is retryable.
// Retryable statuses: 429 (Too Many Requests), 503 (Service Unavailable), 5xx (Server Errors)
func isRetryableStatus(status int) bool {
	return status == 429 || status == 503 || status >= 500
}
