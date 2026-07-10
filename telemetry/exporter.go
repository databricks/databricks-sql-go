package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/databricks/databricks-sql-go/internal/client"
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

// doExport sends one telemetry request. It is a single HTTP transaction —
// transport-layer retries are suppressed via WithSkipTransientRetries so
// the breaker is the sole arbiter of backoff and sees one outcome per
// export. With no ClientMethod on the context, generic 5xx/transport
// errors are already one-shot per internal/client.RetryPolicy; combined,
// every non-2xx (429, 5xx, transport) returns directly to the breaker as a
// single failure signal.
//
// On 429 or 503 we parse Retry-After and feed it to the breaker via
// extendOpenStateAtLeast so the open-state interval honours the server's
// hint.
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

	// Opt out of retryablehttp's transient-response retries — the
	// circuit breaker owns the backoff and needs one HTTP transaction per
	// call so it can see each 429/503 directly. Otherwise the transport
	// layer would honour Retry-After once internally and the breaker
	// would honour it again, stacking waits.
	ctx = client.WithSkipTransientRetries(ctx)

	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if e.userAgent != "" {
		req.Header.Set("User-Agent", e.userAgent)
	}

	// With SkipTransientRetries set, retryablehttp returns (resp, err)
	// for a 429/503: a wrapped error AND the actual response. Inspect
	// the response before short-circuiting on err so we can read
	// Retry-After.
	resp, doErr := e.httpClient.Do(req)
	if resp != nil {
		_, _ = io.ReadAll(resp.Body)
		resp.Body.Close() //nolint:errcheck,gosec // G104: close after response is read

		if resp.StatusCode == http.StatusTooManyRequests ||
			resp.StatusCode == http.StatusServiceUnavailable {
			if hint := parseRetryAfter(resp.Header.Get("Retry-After")); hint > 0 {
				e.circuitBreaker.extendOpenStateAtLeast(hint)
			}
			return fmt.Errorf("telemetry export failed: status %d", resp.StatusCode)
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}
		if doErr == nil {
			return fmt.Errorf("telemetry export failed: status %d", resp.StatusCode)
		}
	}
	if doErr != nil {
		return fmt.Errorf("telemetry export failed: %w", doErr)
	}
	return nil
}

// parseRetryAfter parses the Retry-After header per RFC 7231. Only the
// delta-seconds form is honored; HTTP-date is rare in practice for rate
// limiting and we'd rather under-back-off than mis-parse. Returns 0 on
// any failure.
func parseRetryAfter(s string) time.Duration {
	if s == "" {
		return 0
	}
	sec, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil || sec <= 0 {
		return 0
	}
	return time.Duration(sec) * time.Second
}
