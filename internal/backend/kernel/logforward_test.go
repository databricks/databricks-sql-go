package kernel

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/databricks/databricks-sql-go/logger"
	"github.com/rs/zerolog"
)

func TestLogSinkForwardMapsLevels(t *testing.T) {
	cases := []struct {
		level string
		want  string
	}{
		{"error", "error"},
		{"warn", "warn"},
		{"info", "info"},
		{"debug", "debug"},
		{"trace", "trace"},
		{"future", "debug"},
	}
	t.Cleanup(func() { logger.SetLogOutput(os.Stderr) })
	emittedAt := time.Now()
	for _, tc := range cases {
		var buf bytes.Buffer
		// The sink forwards through the shared output; point it at a buffer to
		// capture the single record and confirm it is one hook-free JSON line whose
		// only timestamp is the emission time forward stamps.
		logger.SetLogOutput(&buf)
		sink := newLogSink()
		sink.forward(emittedAt, tc.level, "databricks::sql::kernel", "hello")
		var record map[string]any
		if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &record); err != nil {
			t.Fatalf("level %q: %v", tc.level, err)
		}
		if record["level"] != tc.want || record["target"] != "databricks::sql::kernel" || record["message"] != "hello" {
			t.Errorf("level %q: record = %#v", tc.level, record)
		}
		if _, ok := record[zerolog.TimestampFieldName]; !ok {
			t.Errorf("level %q: record missing %q field: %#v", tc.level, zerolog.TimestampFieldName, record)
		}
	}
}

func TestLogSinkFollowsLocalFileRetarget(t *testing.T) {
	prevLevel := logger.Logger.GetLevel()
	t.Cleanup(func() {
		logger.SetLogOutput(os.Stderr)
		logger.Logger.Logger = logger.Logger.Level(prevLevel)
	})

	first, err := os.CreateTemp(t.TempDir(), "kernel-first-*.log")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := first.Close(); err != nil {
			t.Errorf("close first log: %v", err)
		}
	})
	second, err := os.CreateTemp(t.TempDir(), "kernel-second-*.log")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := second.Close(); err != nil {
			t.Errorf("close second log: %v", err)
		}
	})

	logger.SetLogOutput(first)
	sink := newLogSink()
	sink.forward(time.Now(), "debug", "databricks::sql::kernel", "kernel first destination")
	logger.SetLogOutput(second)
	sink.forward(time.Now(), "warn", "databricks::sql::kernel", "kernel second destination")

	if err := first.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := second.Sync(); err != nil {
		t.Fatal(err)
	}
	firstBytes, err := os.ReadFile(first.Name())
	if err != nil {
		t.Fatal(err)
	}
	secondBytes, err := os.ReadFile(second.Name())
	if err != nil {
		t.Fatal(err)
	}
	if got := string(firstBytes); !strings.Contains(got, "kernel first destination") || strings.Contains(got, "kernel second destination") {
		t.Fatalf("first log contents = %q", got)
	}
	if got := string(secondBytes); !strings.Contains(got, "kernel second destination") || strings.Contains(got, "kernel first destination") {
		t.Fatalf("second log contents = %q", got)
	}
}
