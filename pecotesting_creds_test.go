package dbsql

import (
	"encoding/json"
	"os"
	"testing"
)

// tokenFromConfigFile returns the access token from the JSON file named by
// DATABRICKS_TEST_CONFIG_FILE, or "" when the variable is unset/empty/unreadable.
//
// Why this indirection exists: the engineer-bot (databricks-bot-engine) runs the
// e2e suite inside an agent-driven subprocess whose environment has every
// credential-shaped variable — anything matching *TOKEN* / *SECRET* / *PASSWORD*
// etc. — stripped for safety (the engine's shared/env_scrub.py). The
// DATABRICKS_PECOTESTING_TOKEN is therefore removed before the tests start, so
// without this fallback pecoTestingCreds would see an empty token and t.Skip the
// repro. The bot instead writes the token to a file and points at it with
// DATABRICKS_TEST_CONFIG_FILE — a name the scrub deliberately preserves. Normal
// CI and local dev leave that variable unset, so this returns "" and the
// DATABRICKS_PECOTESTING_* env vars are used unchanged.
func tokenFromConfigFile(t *testing.T) string {
	t.Helper()
	path := os.Getenv("DATABRICKS_TEST_CONFIG_FILE")
	if path == "" {
		return ""
	}
	// #nosec G304,G703 -- the path comes from DATABRICKS_TEST_CONFIG_FILE, a
	// maintainer-controlled CI env var; reading the operator-supplied config
	// file it names is the intended behavior (test-only code, never in the
	// shipped driver).
	data, err := os.ReadFile(path)
	if err != nil {
		// The env var was explicitly set (engineer-bot path) but the file is
		// unreadable. Log loudly so the misconfiguration is visible in test
		// output instead of masquerading as a benign t.Skip downstream.
		t.Logf("DATABRICKS_TEST_CONFIG_FILE=%q set but unreadable: %v", path, err)
		return ""
	}
	var cfg struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		t.Logf("DATABRICKS_TEST_CONFIG_FILE=%q set but contains malformed JSON: %v", path, err)
		return ""
	}
	return cfg.Token
}

// pecoTestingCreds reads the shared DATABRICKS_PECOTESTING_* warehouse credentials
// used by every credential-gated E2E / parity test, or skips when they are unset.
// The token comes from _TOKEN, falling back to _TOKEN_PERSONAL. Untagged so both
// the tagged kernel tests and the default-build Thrift tests can share one copy
// instead of duplicating the read + skip block at each site.
func pecoTestingCreds(t *testing.T) (host, httpPath, token string) {
	t.Helper()
	host = os.Getenv("DATABRICKS_PECOTESTING_SERVER_HOSTNAME")
	httpPath = os.Getenv("DATABRICKS_PECOTESTING_HTTP_PATH2")
	token = os.Getenv("DATABRICKS_PECOTESTING_TOKEN")
	if token == "" {
		token = os.Getenv("DATABRICKS_PECOTESTING_TOKEN_PERSONAL")
	}
	// Env vars win; fall back to the config file only when the token is absent
	// (used by the engineer-bot, whose subprocess env is scrubbed of
	// credential-shaped vars — see tokenFromConfigFile). Normal CI/local dev is
	// unchanged.
	if token == "" {
		token = tokenFromConfigFile(t)
	}
	if host == "" || httpPath == "" || token == "" {
		t.Skip("set DATABRICKS_PECOTESTING_SERVER_HOSTNAME, DATABRICKS_PECOTESTING_HTTP_PATH2, and DATABRICKS_PECOTESTING_TOKEN to run")
	}
	return host, httpPath, token
}
