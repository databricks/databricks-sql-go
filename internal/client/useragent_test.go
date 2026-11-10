package client

import (
	"net/http"
	"testing"

	"github.com/databricks/databricks-sql-go/auth/noop"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/stretchr/testify/require"
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

type recordingAuth struct {
	client *http.Client
}

func (a *recordingAuth) Authenticate(*http.Request) error { return nil }
func (a *recordingAuth) SetHTTPClient(c *http.Client)     { a.client = c }

type recordingRoundTripper struct {
	userAgent string
}

func (rt *recordingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	rt.userAgent = req.Header.Get("User-Agent")
	return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody, Request: req}, nil
}

func TestPooledClientInjectsUserAgentClientIntoAuthenticator(t *testing.T) {
	clearAgentEnv(t)
	base := &recordingRoundTripper{}
	authr := &recordingAuth{}
	cfg := &config.Config{
		DriverName:    "godatabrickssqlconnector",
		DriverVersion: "9.9.9",
		UserConfig: config.UserConfig{
			UserAgentEntry: "isv/product",
			Authenticator:  authr,
			Transport:      base,
		},
	}

	PooledClient(cfg)

	require.NotNil(t, authr.client, "authenticator should receive an http client")
	req, _ := http.NewRequest(http.MethodPost, "https://host/oidc/token", nil)
	resp, err := authr.client.Transport.RoundTrip(req)
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, BuildUserAgent(cfg), base.userAgent)
	require.Empty(t, req.Header.Get("User-Agent"), "caller's request must not be mutated")
}

func TestPooledClientSkipsAuthenticatorsWithoutSetHTTPClient(t *testing.T) {
	cfg := &config.Config{UserConfig: config.UserConfig{Authenticator: &noop.NoopAuth{}}}
	require.NotNil(t, PooledClient(cfg))
}
