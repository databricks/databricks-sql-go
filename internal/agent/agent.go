// Package agent detects whether the Go SQL driver is being invoked by an AI
// coding agent by checking for well-known environment variables that agents set
// in their spawned shell processes.
//
// Detection only succeeds when exactly one agent environment variable is
// present, to avoid ambiguous attribution when multiple agent environments
// overlap.
//
// Adding a new agent requires only a new constant and a new entry in
// knownAgents.
//
// References for each environment variable:
//   - ANTIGRAVITY_AGENT: Closed source. Google Antigravity sets this variable.
//   - CLAUDECODE: https://github.com/anthropics/claude-code (sets CLAUDECODE=1)
//   - CLINE_ACTIVE: https://github.com/cline/cline (shipped in v3.24.0)
//   - CODEX_CI: https://github.com/openai/codex (part of UNIFIED_EXEC_ENV array in codex-rs)
//   - CURSOR_AGENT: Closed source. Referenced in a gist by johnlindquist.
//   - GEMINI_CLI: https://google-gemini.github.io/gemini-cli/docs/tools/shell.html (sets GEMINI_CLI=1)
//   - OPENCODE: https://github.com/opencode-ai/opencode (sets OPENCODE=1)
package agent

import "os"

const (
	Antigravity = "antigravity"
	ClaudeCode  = "claude-code"
	Cline       = "cline"
	Codex       = "codex"
	Cursor      = "cursor"
	GeminiCLI   = "gemini-cli"
	OpenCode    = "opencode"
)

var knownAgents = []struct {
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

// Detect returns the product string of the AI coding agent driving the current
// process, or an empty string if no agent (or multiple agents) are detected.
func Detect() string {
	return detect(os.Getenv)
}

// detect is the internal implementation that accepts an env lookup function
// for testability.
func detect(getenv func(string) string) string {
	var detected []string
	for _, a := range knownAgents {
		if getenv(a.envVar) != "" {
			detected = append(detected, a.product)
		}
	}
	if len(detected) == 1 {
		return detected[0]
	}
	return ""
}
