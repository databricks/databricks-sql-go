package thrift

// Demonstrates that step debug logging (internal/debuglog) fires through the
// Thrift backend's execute path, producing ordered, function-tagged events, so a
// regression that silently drops the instrumentation is caught.

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/databricks/databricks-sql-go/internal/backend"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/databricks/databricks-sql-go/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type logEntry struct {
	Fn    string `json:"fn"`
	Phase string `json:"phase"`
}

// captureBuf is a concurrency-safe sink for the debug logger.
type captureBuf struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (c *captureBuf) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.Write(p)
}

func (c *captureBuf) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.String()
}

func TestBackend_DebugLoggingOrderedAndNested(t *testing.T) {
	prevLevel := logger.Logger.GetLevel()
	var buf captureBuf
	logger.SetLogOutput(&buf)
	require.NoError(t, logger.SetLogLevel("debug"))
	t.Cleanup(func() {
		logger.SetLogOutput(os.Stderr)
		logger.Logger.Logger = logger.Logger.Level(prevLevel)
	})

	// A successful direct-results execute so we traverse Execute -> runQuery ->
	// executeStatement without needing a poll loop.
	executeStatement := func(ctx context.Context, req *cli_service.TExecuteStatementReq) (*cli_service.TExecuteStatementResp, error) {
		return &cli_service.TExecuteStatementResp{
			Status:          &cli_service.TStatus{StatusCode: cli_service.TStatusCode_SUCCESS_STATUS},
			OperationHandle: &cli_service.TOperationHandle{OperationId: &cli_service.THandleIdentifier{GUID: []byte("0123456789abcdef"), Secret: []byte("s")}},
			DirectResults: &cli_service.TSparkDirectResults{
				OperationStatus: &cli_service.TGetOperationStatusResp{
					Status:         &cli_service.TStatus{StatusCode: cli_service.TStatusCode_SUCCESS_STATUS},
					OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_FINISHED_STATE),
				},
			},
		}, nil
	}
	be := newTestBackend(&client.TestClient{FnExecuteStatement: executeStatement}, getTestSession(), config.WithDefaults())

	_, err := be.Execute(context.Background(), backend.ExecRequest{Query: "select 1"})
	assert.NoError(t, err)

	entries := parseEntries(t, buf.String())

	// All three nested steps must be present.
	for _, fn := range []string{"thrift.Backend.Execute", "thrift.Backend.runQuery", "thrift.Backend.executeStatement"} {
		assert.True(t, hasStep(entries, fn, "enter"), "expected an enter for %s", fn)
		assert.True(t, hasStep(entries, fn, "done"), "expected a done for %s", fn)
	}

	// Enter+done for all three nested steps land as ordered lines on the shared
	// logger — the single stream that kernel-side stderr logs interleave into by
	// execution order.
	require.GreaterOrEqual(t, len(entries), 6, "expected at least enter+done for 3 nested steps")

	// The outermost Execute enters before the inner executeStatement enters and
	// finishes after it — the nesting is reflected in the stream order.
	execEnter := indexOfStep(entries, "thrift.Backend.Execute", "enter")
	stmtEnter := indexOfStep(entries, "thrift.Backend.executeStatement", "enter")
	execDone := indexOfStep(entries, "thrift.Backend.Execute", "done")
	require.True(t, execEnter >= 0 && stmtEnter >= 0 && execDone >= 0, "all three markers present")
	assert.Less(t, execEnter, stmtEnter, "Execute should enter before executeStatement")
	assert.Less(t, stmtEnter, execDone, "executeStatement should run inside Execute")
}

func parseEntries(t *testing.T, out string) []logEntry {
	t.Helper()
	var entries []logEntry
	for _, line := range strings.Split(strings.TrimRight(out, "\n"), "\n") {
		if line == "" {
			continue
		}
		var e logEntry
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatalf("line is not JSON: %q (%v)", line, err)
		}
		if e.Fn != "" { // ignore any non-debug lines (e.g. the logger's init info line)
			entries = append(entries, e)
		}
	}
	return entries
}

func hasStep(entries []logEntry, fn, phase string) bool {
	return indexOfStep(entries, fn, phase) >= 0
}

func indexOfStep(entries []logEntry, fn, phase string) int {
	for i, e := range entries {
		if e.Fn == fn && e.Phase == phase {
			return i
		}
	}
	return -1
}
