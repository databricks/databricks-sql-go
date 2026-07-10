package debuglog

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/databricks/databricks-sql-go/driverctx"
	"github.com/databricks/databricks-sql-go/logger"
)

// entry is one parsed zerolog line from the shared logger's sink.
type entry struct {
	Level   string  `json:"level"`
	Time    string  `json:"time"`
	Fn      string  `json:"fn"`
	Phase   string  `json:"phase"`
	Message string  `json:"message"`
	Elapsed float64 `json:"elapsed"`
	ConnID  string  `json:"connId"`
	CorrID  string  `json:"corrId"`
	QueryID string  `json:"queryId"`
	raw     map[string]any
}

// withCapture routes the shared logger into a fresh buffer at debug level for
// the duration of fn, then restores the prior level and output. Every test goes
// through this so global logger state never leaks between tests.
func withCapture(t *testing.T, fn func(entries func() []entry)) {
	t.Helper()
	prevLevel := logger.Logger.GetLevel()
	var buf syncBuf
	logger.SetLogOutput(&buf)
	if err := logger.SetLogLevel("debug"); err != nil {
		t.Fatalf("SetLogLevel: %v", err)
	}
	t.Cleanup(func() {
		logger.SetLogOutput(os.Stderr)
		logger.Logger.Logger = logger.Logger.Level(prevLevel)
	})
	fn(func() []entry { return parse(t, buf.String()) })
}

// parse decodes the captured JSON lines into entries, ignoring any line without
// an fn field (e.g. the logger's own init/level lines).
func parse(t *testing.T, out string) []entry {
	t.Helper()
	var entries []entry
	for _, line := range strings.Split(strings.TrimRight(out, "\n"), "\n") {
		if line == "" {
			continue
		}
		var raw map[string]any
		if err := json.Unmarshal([]byte(line), &raw); err != nil {
			t.Fatalf("line is not JSON: %q (%v)", line, err)
		}
		if _, ok := raw["fn"]; !ok {
			continue
		}
		var e entry
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatalf("cannot decode entry: %q (%v)", line, err)
		}
		e.raw = raw
		entries = append(entries, e)
	}
	return entries
}

func TestSilentWhenLevelAboveDebug(t *testing.T) {
	prevLevel := logger.Logger.GetLevel()
	var buf syncBuf
	logger.SetLogOutput(&buf)
	if err := logger.SetLogLevel("warn"); err != nil {
		t.Fatalf("SetLogLevel: %v", err)
	}
	t.Cleanup(func() {
		logger.SetLogOutput(os.Stderr)
		logger.Logger.Logger = logger.Logger.Level(prevLevel)
	})

	Logf(context.Background(), "pkg.Fn", "should not appear")
	done := Track(context.Background(), "pkg.Fn", "step")
	done()

	if es := parse(t, buf.String()); len(es) != 0 {
		t.Fatalf("expected no step entries at warn level, got %+v", es)
	}
	if Enabled() {
		t.Fatal("Enabled() should be false at warn level")
	}
}

func TestEnabledFollowsLogLevel(t *testing.T) {
	withCapture(t, func(_ func() []entry) {
		if !Enabled() {
			t.Fatal("Enabled() should be true at debug level")
		}
	})
}

func TestLogfCarriesFunctionAndMessage(t *testing.T) {
	withCapture(t, func(entries func() []entry) {
		Logf(context.Background(), "thrift.Backend.Execute", "sql=%q rows=%d", "select 1", 42)
		es := entries()
		if len(es) != 1 {
			t.Fatalf("expected 1 entry, got %d: %+v", len(es), es)
		}
		e := es[0]
		if e.Fn != "thrift.Backend.Execute" {
			t.Errorf("fn = %q, want thrift.Backend.Execute", e.Fn)
		}
		if e.Message != `sql="select 1" rows=42` {
			t.Errorf("message = %q, want formatted step", e.Message)
		}
		if _, ok := e.raw["phase"]; ok {
			t.Errorf("point log should have no phase field: %v", e.raw)
		}
	})
}

// TestFormatMatchesDriverLogs pins the parity the collapse buys: step lines
// carry the same level and time fields as every other driver log line, because
// they go through the same logger.
func TestFormatMatchesDriverLogs(t *testing.T) {
	withCapture(t, func(entries func() []entry) {
		Logf(context.Background(), "pkg.Fn", "step")
		e := entries()[0]
		if e.Level != "debug" {
			t.Errorf("level = %q, want debug (shared-logger parity)", e.Level)
		}
		if e.Time == "" {
			t.Error("time field missing — should come from the shared logger")
		}
	})
}

func TestTrackEmitsEnterAndDoneWithElapsed(t *testing.T) {
	withCapture(t, func(entries func() []entry) {
		done := Track(context.Background(), "kernel.Session.Open", "host=%s", "example")
		done()

		es := entries()
		if len(es) != 2 {
			t.Fatalf("expected enter+done (2 entries), got %d: %+v", len(es), es)
		}
		if es[0].Phase != "enter" || es[1].Phase != "done" {
			t.Fatalf("phases = %q,%q want enter,done", es[0].Phase, es[1].Phase)
		}
		if es[0].Message != "host=example" {
			t.Errorf("enter message = %q, want host=example", es[0].Message)
		}
		if _, ok := es[1].raw["elapsed"]; !ok {
			t.Errorf("done entry should carry an elapsed field: %v", es[1].raw)
		}
	})
}

func TestContextIDsAppear(t *testing.T) {
	withCapture(t, func(entries func() []entry) {
		ctx := driverctx.NewContextWithConnId(context.Background(), "conn-123")
		ctx = driverctx.NewContextWithCorrelationId(ctx, "corr-456")
		ctx = driverctx.NewContextWithQueryId(ctx, "query-789")
		Logf(ctx, "pkg.Fn", "step")
		e := entries()[0]
		if e.ConnID != "conn-123" || e.CorrID != "corr-456" || e.QueryID != "query-789" {
			t.Errorf("ids = %q/%q/%q, want conn-123/corr-456/query-789", e.ConnID, e.CorrID, e.QueryID)
		}
	})
}

func TestEmptyContextIDsAreOmitted(t *testing.T) {
	withCapture(t, func(entries func() []entry) {
		Logf(context.Background(), "pkg.Fn", "step")
		raw := entries()[0].raw
		for _, unwanted := range []string{"connId", "corrId", "queryId"} {
			if _, ok := raw[unwanted]; ok {
				t.Errorf("empty id field %q should be omitted: %v", unwanted, raw)
			}
		}
	})
}

func TestNilContextIsSafe(t *testing.T) {
	withCapture(t, func(entries func() []entry) {
		//nolint:staticcheck // intentionally passing nil ctx to prove it is safe
		Logf(nil, "pkg.Fn", "step")
		if es := entries(); len(es) != 1 || es[0].Fn != "pkg.Fn" {
			t.Errorf("nil context should still log a pkg.Fn entry, got %+v", es)
		}
	})
}

// syncBuf is a concurrency-safe buffer for capturing log output under -race.
type syncBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// ExampleTrack shows the intended defer-Track idiom.
func ExampleTrack() {
	prevLevel := logger.Logger.GetLevel()
	_ = logger.SetLogLevel("debug")
	defer func() { logger.Logger.Logger = logger.Logger.Level(prevLevel) }()
	fn := func(ctx context.Context) {
		defer Track(ctx, "pkg.example", "doing work")()
	}
	fn(context.Background())
	fmt.Println("ran")
	// Output: ran
}
