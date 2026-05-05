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
	userAgent      string
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

// ensureHTTPScheme adds https:// prefix to host if no scheme is present.
func ensureHTTPScheme(host string) string {
	if strings.HasPrefix(host, httpPrefix) || strings.HasPrefix(host, httpsPrefix) {
		return host
	}
	return defaultScheme + host
}

// newTelemetryExporter creates a new exporter.
func newTelemetryExporter(host string, driverVersion string, userAgent string, httpClient *http.Client, cfg *Config) *telemetryExporter {
	return &telemetryExporter{
		host:           host,
		driverVersion:  driverVersion,
		userAgent:      userAgent,
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

// doExport sends one telemetry request. It does NOT retry — retries are
// handled by the underlying retryablehttp-wrapped HTTP client (see
// internal/client.RetryableClient), which already retries 429/5xx with the
// server-provided Retry-After header. Any non-2xx outcome here is therefore
// the *post-retry* result, and is returned to the caller so the circuit
// breaker counts it as one failure per export.
func (e *telemetryExporter) doExport(ctx context.Context, metrics []*telemetryMetric) error {
	request, err := createTelemetryRequest(metrics, e.driverVersion)
	if err != nil {
		return fmt.Errorf("failed to create telemetry request: %w", err)
	}

	data, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	endpoint := ensureHTTPScheme(e.host) + telemetryEndpointPath

	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if e.userAgent != "" {
		req.Header.Set("User-Agent", e.userAgent)
	}

	resp, err := e.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("telemetry export failed: %w", err)
	}
	_, _ = io.ReadAll(resp.Body)
	resp.Body.Close() //nolint:errcheck,gosec // G104: close after response is read

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	return fmt.Errorf("telemetry export failed: status %d", resp.StatusCode)
}
