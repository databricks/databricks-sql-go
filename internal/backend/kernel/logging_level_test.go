package kernel

import (
	"testing"

	"github.com/databricks/databricks-sql-go/logger"
	"github.com/rs/zerolog"
)

// kernelLogLevel maps the driver's zerolog level to the kernel_init_logging level
// string, so DATABRICKS_LOG_LEVEL drives the kernel's Rust logs too. Pin the
// mapping (incl. the fatal/panic→OFF collapse and the default→WARN fallback) so a
// level the kernel would reject can't silently ship. fatal/panic map to OFF, not
// ERROR: the Go driver suppresses even Error() lines at those levels, so the Rust
// subscriber must stay at least as quiet as the driver the user configured. Lives
// in the untagged file so it runs under CGO_ENABLED=0.
func TestKernelLogLevel(t *testing.T) {
	cases := []struct {
		in   zerolog.Level
		want string
	}{
		{zerolog.TraceLevel, "TRACE"},
		{zerolog.DebugLevel, "DEBUG"},
		{zerolog.InfoLevel, "INFO"},
		{zerolog.WarnLevel, "WARN"},
		{zerolog.ErrorLevel, "ERROR"},
		{zerolog.FatalLevel, "OFF"}, // driver suppresses Error() here → kernel stays silent, not louder
		{zerolog.PanicLevel, "OFF"},
		{zerolog.Disabled, "OFF"},
		{zerolog.NoLevel, "WARN"}, // unrecognized → the kernel's own default
	}
	for _, c := range cases {
		if got := kernelLogLevel(c.in); got != c.want {
			t.Errorf("kernelLogLevel(%v) = %q, want %q", c.in, got, c.want)
		}
	}
}

// TestResolveKernelLogArg pins the level-vs-NULL decision: DBSQL_KERNEL_DEBUG (any
// non-empty value) yields useNULL=true so the kernel honors RUST_LOG; otherwise the
// driver level is mapped in. Empty is treated as unset (the gate is os.Getenv != "").
func TestResolveKernelLogArg(t *testing.T) {
	// t.Setenv restores the var after the test; save/restore the global logger level
	// separately so nothing leaks to sibling tests.
	prevLevel := logger.Logger.GetLevel()
	t.Cleanup(func() { logger.Logger.Logger = logger.Logger.Level(prevLevel) })

	// Set → NULL level regardless of the driver level (pinned at debug to prove the
	// override wins over the mapped level).
	logger.Logger.Logger = logger.Logger.Level(zerolog.DebugLevel)
	t.Setenv("DBSQL_KERNEL_DEBUG", "1")
	if lvl, useNULL := resolveKernelLogArg(); !useNULL || lvl != "" {
		t.Errorf("with DBSQL_KERNEL_DEBUG=1: got (level=%q, useNULL=%v), want (\"\", true)", lvl, useNULL)
	}

	// Empty is treated as unset → the mapped level is used, not the override.
	t.Setenv("DBSQL_KERNEL_DEBUG", "")
	if lvl, useNULL := resolveKernelLogArg(); useNULL || lvl != "DEBUG" {
		t.Errorf("with DBSQL_KERNEL_DEBUG=\"\" at debug: got (level=%q, useNULL=%v), want (\"DEBUG\", false)", lvl, useNULL)
	}

	// Mapped level tracks the logger, including the fatal→OFF collapse.
	logger.Logger.Logger = logger.Logger.Level(zerolog.WarnLevel)
	if lvl, useNULL := resolveKernelLogArg(); useNULL || lvl != "WARN" {
		t.Errorf("unset at warn: got (level=%q, useNULL=%v), want (\"WARN\", false)", lvl, useNULL)
	}
	logger.Logger.Logger = logger.Logger.Level(zerolog.FatalLevel)
	if lvl, useNULL := resolveKernelLogArg(); useNULL || lvl != "OFF" {
		t.Errorf("unset at fatal: got (level=%q, useNULL=%v), want (\"OFF\", false)", lvl, useNULL)
	}
}
