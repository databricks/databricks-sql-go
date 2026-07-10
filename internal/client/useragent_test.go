package client

import (
	"testing"

	"github.com/databricks/databricks-sql-go/internal/config"
)

// clearAgentEnv blanks every env var agent.Detect inspects so the test
// is deterministic regardless of the host shell. t.Setenv restores the
// previous value when the test ends.
func clearAgentEnv(t *testing.T) {
	t.Helper()
	for _, v := range []string{
		"ANTIGRAVITY_AGENT", "CLAUDECODE", "CLINE_ACTIVE",
		"CODEX_CI", "CURSOR_AGENT", "GEMINI_CLI", "OPENCODE",
	} {
		t.Setenv(v, "")
	}
}

func TestBuildUserAgent(t *testing.T) {
	t.Run("plain driver name and version", func(t *testing.T) {
		clearAgentEnv(t)
		cfg := &config.Config{
			DriverName:    "godatabrickssqlconnector",
			DriverVersion: "9.9.9",
		}
		got := BuildUserAgent(cfg)
		want := "godatabrickssqlconnector/9.9.9"
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	})

	t.Run("with UserAgentEntry", func(t *testing.T) {
		clearAgentEnv(t)
		cfg := &config.Config{
			DriverName:    "godatabrickssqlconnector",
			DriverVersion: "9.9.9",
			UserConfig: config.UserConfig{
				UserAgentEntry: "partner+product",
			},
		}
		got := BuildUserAgent(cfg)
		want := "godatabrickssqlconnector/9.9.9 (partner+product)"
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	})

	t.Run("with detected agent appended", func(t *testing.T) {
		clearAgentEnv(t)
		t.Setenv("CLAUDECODE", "1")
		cfg := &config.Config{
			DriverName:    "godatabrickssqlconnector",
			DriverVersion: "9.9.9",
		}
		got := BuildUserAgent(cfg)
		want := "godatabrickssqlconnector/9.9.9 agent/claude-code"
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	})

	t.Run("UserAgentEntry and detected agent compose", func(t *testing.T) {
		clearAgentEnv(t)
		t.Setenv("CLAUDECODE", "1")
		cfg := &config.Config{
			DriverName:    "godatabrickssqlconnector",
			DriverVersion: "9.9.9",
			UserConfig: config.UserConfig{
				UserAgentEntry: "partner+product",
			},
		}
		got := BuildUserAgent(cfg)
		want := "godatabrickssqlconnector/9.9.9 (partner+product) agent/claude-code"
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	})
}
