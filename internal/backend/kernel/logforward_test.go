package kernel

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/databricks/databricks-sql-go/logger"
	"github.com/rs/zerolog"
)

// The level mapping and nil-sink no-op are pure Go, so they run under
// CGO_ENABLED=0. Actual kernel→callback delivery is exercised by the tagged
// TestLogCallbackRoundTrip (which needs the linked kernel).

// A nil sink must be a safe no-op (the trampoline guards against a missing/typed
// handle by calling forward on a possibly-nil sink).
func TestLogSinkForwardNilIsNoOp(t *testing.T) {
	var s *logSink
	// Must not panic.
	s.forward(kernelLevelError, "databricks::sql::kernel", "boom")
}

// forward must map each kernel level to the right zerolog level and carry the
// target + message through. A buffer-backed TraceLevel sink (mirroring the
// production snapshot) lets us assert the emitted JSON rather than only "no
// panic" — a bug swapping e.g. ERROR→Debug would otherwise ship green.
//
// Two policies are exercised: the DEFAULT demotes the operationally-loud kernel
// ERROR/WARN one step (→Warn/Info) so kernel-internal retries don't inflate the
// driver's alert-keyed WARN/ERROR rate, tagging the demoted line with a
// kernelLevel="error"/"warn" string so its true origin stays visible; and
// nativeSeverity (DBSQL_KERNEL_DEBUG) preserves the native mapping.
func TestLogSinkForwardMapsLevels(t *testing.T) {
	cases := []struct {
		kernelLevel   int
		wantLevel     string // zerolog "level" field under the DEFAULT (demoting) policy
		wantNative    string // zerolog "level" field with nativeSeverity=true
		wantKernelStr string // kernelLevel string field on the default line ("" = none expected)
		unknown       bool   // unmapped → Debug WITH a numeric kernelLevel diagnostic field
	}{
		{kernelLevelError, "warn", "error", "error", false},
		{kernelLevelWarn, "info", "warn", "warn", false},
		{kernelLevelInfo, "info", "info", "", false},
		{kernelLevelDebug, "debug", "debug", "", false},
		{kernelLevelTrace, "trace", "trace", "", false},
		{0, "debug", "debug", "", true},
		{99, "debug", "debug", "", true},
		{-1, "debug", "debug", "", true},
	}
	for _, c := range cases {
		// Default (demoting) policy.
		var buf bytes.Buffer
		s := &logSink{log: zerolog.New(&buf).Level(zerolog.TraceLevel)}
		s.forward(c.kernelLevel, "databricks::sql::kernel", "hello")
		var rec map[string]any
		if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &rec); err != nil {
			t.Fatalf("kernelLevel=%d: emitted non-JSON %q: %v", c.kernelLevel, buf.String(), err)
		}
		if rec["level"] != c.wantLevel {
			t.Errorf("kernelLevel=%d (default policy): level=%v, want %s", c.kernelLevel, rec["level"], c.wantLevel)
		}
		// The unknown-level branch adds a NUMERIC kernelLevel field — the only thing
		// distinguishing an unmapped level from a real Debug line. A demoted
		// error/warn adds a STRING kernelLevel naming its true origin. Assert both so
		// dropping either ships red.
		switch {
		case c.unknown:
			if got, ok := rec["kernelLevel"]; !ok || got != float64(c.kernelLevel) {
				t.Errorf("kernelLevel=%d: expected numeric kernelLevel field=%d, got %v (present=%t)",
					c.kernelLevel, c.kernelLevel, got, ok)
			}
		case c.wantKernelStr != "":
			if got, ok := rec["kernelLevel"]; !ok || got != c.wantKernelStr {
				t.Errorf("kernelLevel=%d: expected demoted line to carry kernelLevel=%q, got %v (present=%t)",
					c.kernelLevel, c.wantKernelStr, got, ok)
			}
		default:
			if _, ok := rec["kernelLevel"]; ok {
				t.Errorf("kernelLevel=%d: an undemoted mapped level should NOT carry a kernelLevel field", c.kernelLevel)
			}
		}
		if rec["target"] != "databricks::sql::kernel" || rec["message"] != "hello" {
			t.Errorf("kernelLevel=%d: target/message not carried through: %v", c.kernelLevel, rec)
		}

		// nativeSeverity policy: ERROR/WARN keep their native level and carry no
		// demotion tag.
		var nbuf bytes.Buffer
		ns := &logSink{log: zerolog.New(&nbuf).Level(zerolog.TraceLevel), nativeSeverity: true}
		ns.forward(c.kernelLevel, "databricks::sql::kernel", "hello")
		var nrec map[string]any
		if err := json.Unmarshal(bytes.TrimSpace(nbuf.Bytes()), &nrec); err != nil {
			t.Fatalf("kernelLevel=%d (native): emitted non-JSON %q: %v", c.kernelLevel, nbuf.String(), err)
		}
		if nrec["level"] != c.wantNative {
			t.Errorf("kernelLevel=%d (native policy): level=%v, want %s", c.kernelLevel, nrec["level"], c.wantNative)
		}
		if !c.unknown {
			if _, ok := nrec["kernelLevel"]; ok {
				t.Errorf("kernelLevel=%d (native): should NOT carry a kernelLevel field", c.kernelLevel)
			}
		}
	}
}

// A TraceLevel snapshot must emit events the driver's own level would suppress —
// this is the anti-double-gating property (forwarded kernel events the kernel
// already approved must not be re-dropped by a driver still at Warn). Drives the
// PRODUCTION newLogSink() against a Warn-level global logger, so a regression of
// newLogSink to `&logSink{log: logger.Logger}` (routing through the live,
// driver-gated logger) fails here rather than passing against a hand-rolled replica.
func TestLogSinkForwardNotReGatedByDriverLevel(t *testing.T) {
	// Point the global driver logger at a buffer and pin it to Warn — the state
	// newLogSink() reads at install. Restore afterward so other tests are
	// unaffected.
	var buf bytes.Buffer
	prevLevel := logger.Logger.GetLevel()
	logger.SetLogOutput(&buf)
	_ = logger.SetLogLevel("warn")
	t.Cleanup(func() {
		logger.SetLogOutput(os.Stderr)
		logger.Logger.Logger = logger.Logger.Level(prevLevel)
	})

	s := newLogSink() // snapshots logger.Logger at TraceLevel — the real path
	s.forward(kernelLevelDebug, "databricks::sql::kernel", "debug-line")
	if !strings.Contains(buf.String(), "debug-line") {
		t.Errorf("a DEBUG kernel event was dropped despite newLogSink's TraceLevel snapshot: %q", buf.String())
	}
}
