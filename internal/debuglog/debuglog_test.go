package debuglog

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/databricks/databricks-sql-go/driverctx"
	"github.com/databricks/databricks-sql-go/logger"
)

// entry is one parsed zerolog line from the debug sink.
type entry struct {
	TS      string  `json:"ts"`
	Seq     uint64  `json:"seq"`
	Fn      string  `json:"fn"`
	Phase   string  `json:"phase"`
	Message string  `json:"message"`
	Elapsed float64 `json:"elapsed"`
	ConnID  string  `json:"connId"`
	CorrID  string  `json:"corrId"`
	QueryID string  `json:"queryId"`
	raw     map[string]any
}

// withCapture enables logging into a fresh buffer for the duration of fn, then
// restores the prior enabled state, output, and clock. Every test goes through
// this so global state never leaks between tests.
func withCapture(t *testing.T, fn func(entries func() []entry)) {
	t.Helper()
	prevEnabled := SetEnabled(true)
	var buf syncBuf
	logger.SetLogOutput(&buf)
	t.Cleanup(func() {
		SetEnabled(prevEnabled)
		logger.SetLogOutput(nil)
		setClock(nil) // reset to time.Now
	})
	fn(func() []entry { return parse(t, buf.String()) })
}

// parse decodes the captured JSON lines into entries.
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
		var e entry
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatalf("cannot decode entry: %q (%v)", line, err)
		}
		e.raw = raw
		entries = append(entries, e)
	}
	return entries
}

func TestDisabledByDefaultIsSilent(t *testing.T) {
	prev := SetEnabled(false) // force disabled regardless of ambient env / prior tests
	defer SetEnabled(prev)

	var buf syncBuf
	logger.SetLogOutput(&buf)
	defer logger.SetLogOutput(nil)

	Logf(context.Background(), "pkg.Fn", "should not appear")
	done := Track(context.Background(), "pkg.Fn", "step")
	done()

	if buf.String() != "" {
		t.Fatalf("expected no output when disabled, got: %q", buf.String())
	}
	if Enabled() {
		t.Fatal("Enabled() should be false")
	}
}

func TestEnvEnabled(t *testing.T) {
	cases := map[string]bool{
		"1": true, "true": true, "TRUE": true, "  yes ": true, "On": true,
		"": false, "0": false, "false": false, "no": false, "nope": false,
	}
	for in, want := range cases {
		if got := envEnabled(in); got != want {
			t.Errorf("envEnabled(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestLogfCarriesFunctionMessageAndSeq(t *testing.T) {
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
		if e.Seq == 0 {
			t.Errorf("seq should be a positive counter, got %d", e.Seq)
		}
		if _, ok := e.raw["phase"]; ok {
			t.Errorf("point log should have no phase field: %v", e.raw)
		}
	})
}

func TestTimestampIsNanosecondPrecise(t *testing.T) {
	withCapture(t, func(entries func() []entry) {
		// A fixed instant carrying sub-second nanoseconds — the coarse RFC3339
		// (whole-second) default would drop the fractional part.
		fixed := time.Date(2026, 7, 5, 12, 0, 0, 123456789, time.UTC)
		setClock(func() time.Time { return fixed })

		Logf(context.Background(), "pkg.Fn", "step")
		e := entries()[0]

		if e.TS == "" {
			t.Fatal("ts field missing")
		}
		parsed, err := time.Parse(time.RFC3339Nano, e.TS)
		if err != nil {
			t.Fatalf("ts %q is not RFC3339Nano: %v", e.TS, err)
		}
		if parsed.Nanosecond() != 123456789 {
			t.Errorf("ts lost sub-second precision: got %d ns, want 123456789 (ts=%q)", parsed.Nanosecond(), e.TS)
		}
	})
}

func TestTrackEmitsEnterAndDoneWithElapsed(t *testing.T) {
	withCapture(t, func(entries func() []entry) {
		// Deterministic clock: enter at t0, done 5ms later.
		t0 := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
		times := []time.Time{t0, t0.Add(5 * time.Millisecond)}
		i := 0
		setClock(func() time.Time {
			idx := i
			if idx >= len(times) {
				idx = len(times) - 1
			}
			i++
			return times[idx]
		})

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
		// zerolog renders Dur in milliseconds by default; 5ms -> 5.
		if es[1].Elapsed != 5 {
			t.Errorf("done elapsed = %v ms, want 5", es[1].Elapsed)
		}
	})
}

func TestSequenceIsMonotonic(t *testing.T) {
	withCapture(t, func(entries func() []entry) {
		for n := 0; n < 5; n++ {
			Logf(context.Background(), "pkg.Fn", "line %d", n)
		}
		es := entries()
		if len(es) != 5 {
			t.Fatalf("expected 5 entries, got %d", len(es))
		}
		for i := 1; i < len(es); i++ {
			if es[i].Seq <= es[i-1].Seq {
				t.Errorf("sequence not strictly increasing: %d then %d", es[i-1].Seq, es[i].Seq)
			}
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

func TestDisableMidStepSuppressesDone(t *testing.T) {
	withCapture(t, func(entries func() []entry) {
		done := Track(context.Background(), "pkg.Fn", "step")
		SetEnabled(false) // caller flips it off before the deferred done fires
		done()
		SetEnabled(true) // restore so cleanup is consistent
		es := entries()
		if len(es) != 1 || es[0].Phase != "enter" {
			t.Fatalf("expected only the enter entry, got %+v", es)
		}
	})
}

// TestConcurrentLoggingIsRaceFree exercises the atomic gate and the atomic clock
// override under -race: many goroutines log while others flip SetEnabled and
// setClock concurrently.
func TestConcurrentLoggingIsRaceFree(t *testing.T) {
	prev := SetEnabled(true)
	defer SetEnabled(prev)
	logger.SetLogOutput(&syncBuf{})
	defer logger.SetLogOutput(nil)
	defer setClock(nil)

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				Logf(context.Background(), "pkg.Fn", "g=%d i=%d", g, i)
				done := Track(context.Background(), "pkg.Fn", "step")
				done()
			}
		}(g)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			SetEnabled(i%2 == 0)
		}
		SetEnabled(true)
	}()
	// Flip the clock override concurrently with logging — reads of clockOverride
	// happen in every logged event, so this proves the override is race-free too.
	wg.Add(1)
	go func() {
		defer wg.Done()
		fixed := func() time.Time { return time.Unix(0, 0) }
		for i := 0; i < 50; i++ {
			if i%2 == 0 {
				setClock(fixed)
			} else {
				setClock(nil)
			}
		}
	}()
	wg.Wait()
}

// TestConcurrentSetLogOutputIsRaceFree exercises the debug logger swap under
// -race: goroutines emit step traces (reading logger.DebugLogger()) while
// another goroutine calls logger.SetLogOutput, which reinstalls the sibling
// logger. A plain (non-atomic) global would be a torn read/write of the
// multi-field zerolog.Logger struct here.
func TestConcurrentSetLogOutputIsRaceFree(t *testing.T) {
	prev := SetEnabled(true)
	defer SetEnabled(prev)
	defer logger.SetLogOutput(nil)

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				Logf(context.Background(), "pkg.Fn", "g=%d i=%d", g, i)
				done := Track(context.Background(), "pkg.Fn", "step")
				done()
			}
		}(g)
	}
	// Swap the output sink repeatedly while the tracers above are reading the
	// sibling logger on every event.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			logger.SetLogOutput(&syncBuf{})
		}
	}()
	wg.Wait()
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
	SetEnabled(true)
	defer SetEnabled(false)
	fn := func(ctx context.Context) {
		defer Track(ctx, "pkg.example", "doing work")()
	}
	fn(context.Background())
	fmt.Println("ran")
	// Output: ran
}
