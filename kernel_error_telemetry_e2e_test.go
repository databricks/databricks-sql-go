//go:build cgo && databricks_kernel

package dbsql

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/databricks/databricks-sql-go/internal/config"
	dbsqlerrint "github.com/databricks/databricks-sql-go/internal/errors"
	"github.com/databricks/databricks-sql-go/telemetry"
)

// captureTransport records the JSON body of every /telemetry-ext POST so a test can
// inspect what the driver actually put on the wire, while still hitting the real
// server for everything else.
type captureTransport struct {
	base http.RoundTripper
	mu   sync.Mutex
	logs []*telemetry.TelemetryEvent
}

func (t *captureTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if strings.Contains(req.URL.Path, "/telemetry-ext") && req.Body != nil {
		body, _ := io.ReadAll(req.Body)
		req.Body = io.NopCloser(strings.NewReader(string(body)))
		t.mu.Lock()
		t.record(body)
		t.mu.Unlock()
	}
	return t.base.RoundTrip(req)
}

func (t *captureTransport) record(body []byte) {
	var r telemetry.TelemetryRequest
	if json.Unmarshal(body, &r) != nil {
		return
	}
	for _, pl := range r.ProtoLogs {
		var fl telemetry.TelemetryFrontendLog
		if json.Unmarshal([]byte(pl), &fl) == nil && fl.Entry != nil && fl.Entry.SQLDriverLog != nil {
			t.logs = append(t.logs, fl.Entry.SQLDriverLog)
		}
	}
}

func (t *captureTransport) errorNames() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	var names []string
	for _, ev := range t.logs {
		if ev.ErrorInfo != nil && ev.ErrorInfo.ErrorName != "" {
			names = append(names, ev.ErrorInfo.ErrorName)
		}
	}
	return names
}

// TestKernelE2EErrorTelemetryCategory is the live proof of the change: a kernel SQL
// failure must report its code-derived category (execute_statement_failed for a
// SqlError) rather than the "error" message-fallback the classifier used before the
// KernelError.Category() mapping existed.
func TestKernelE2EErrorTelemetryCategory(t *testing.T) {
	host, httpPath, token := pecoTestingCreds(t)

	cap := &captureTransport{base: http.DefaultTransport}
	// Explicit telemetry opt-in + the capture transport, otherwise the same kernel
	// connector a real consumer uses.
	conn, err := NewConnector(
		WithServerHostname(host),
		WithHTTPPath(httpPath),
		WithAccessToken(token),
		WithUseKernel(true),
		func(cfg *config.Config) {
			cfg.EnableTelemetry = config.NewConfigValue(true)
			cfg.TelemetryBatchSize = 1
		},
	)
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}
	c := conn.(*connector)
	base := c.client.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	cap.base = base
	c.client.Transport = cap

	db := sql.OpenDB(conn)
	defer db.Close()

	// A reference to a table that does not exist is a server-side SqlError: it
	// carries a statement id (so the failure metric fires) and maps to
	// CategoryExecuteStatement.
	_, qErr := db.ExecContext(context.Background(),
		"INSERT INTO does_not_exist_"+strings.Repeat("x", 8)+" VALUES (1)")
	if qErr == nil {
		t.Fatal("expected the bad-table statement to fail")
	}

	// Flush pending telemetry synchronously.
	db.Close()

	names := cap.errorNames()
	want := string(dbsqlerrint.CategoryExecuteStatement)
	for _, n := range names {
		if n == want {
			return
		}
	}
	t.Fatalf("no error telemetry with error_name=%q; captured error names=%v", want, names)
}
