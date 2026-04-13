package agent

import (
	"testing"
)

func envWith(vars map[string]string) func(string) string {
	return func(key string) string {
		return vars[key]
	}
}

func TestDetectsSingleAgent(t *testing.T) {
	cases := []struct {
		envVar  string
		product string
	}{
		{"ANTIGRAVITY_AGENT", Antigravity},
		{"CLAUDECODE", ClaudeCode},
		{"CLINE_ACTIVE", Cline},
		{"CODEX_CI", Codex},
		{"CURSOR_AGENT", Cursor},
		{"GEMINI_CLI", GeminiCLI},
		{"OPENCODE", OpenCode},
	}
	for _, tc := range cases {
		t.Run(tc.product, func(t *testing.T) {
			got := detect(envWith(map[string]string{tc.envVar: "1"}))
			if got != tc.product {
				t.Errorf("detect() = %q, want %q", got, tc.product)
			}
		})
	}
}

func TestReturnsEmptyWhenNoAgent(t *testing.T) {
	got := detect(envWith(map[string]string{}))
	if got != "" {
		t.Errorf("detect() = %q, want empty", got)
	}
}

func TestReturnsEmptyWhenMultipleAgents(t *testing.T) {
	got := detect(envWith(map[string]string{
		"CLAUDECODE":   "1",
		"CURSOR_AGENT": "1",
	}))
	if got != "" {
		t.Errorf("detect() = %q, want empty", got)
	}
}

func TestIgnoresEmptyValues(t *testing.T) {
	got := detect(envWith(map[string]string{"CLAUDECODE": ""}))
	if got != "" {
		t.Errorf("detect() = %q, want empty", got)
	}
}

func TestDetectUsesOsGetenv(t *testing.T) {
	// Clear all known agent env vars, then set one
	for _, a := range knownAgents {
		t.Setenv(a.envVar, "")
	}
	t.Setenv("CLAUDECODE", "1")

	got := Detect()
	if got != ClaudeCode {
		t.Errorf("Detect() = %q, want %q", got, ClaudeCode)
	}
}
