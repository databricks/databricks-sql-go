package client

import (
	"context"
	"crypto/tls"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//nolint:gosec // The subprocess only reexecutes this test binary.
func TestOAuthTransportEnvironment(t *testing.T) {
	if mode := os.Getenv("DATABRICKS_OAUTH_ENV_TEST"); mode != "" {
		cfg := config.WithDefaults()
		cfg.Transport = &http.Transport{TLSClientConfig: &tls.Config{ServerName: "sql-only.invalid"}}
		cfg.TLSConfig.InsecureSkipVerify = true
		authenticator := &oauthHTTPClientAuthenticator{}
		cfg.Authenticator = authenticator
		PooledClient(cfg)
		transport := authenticator.client.Transport.(*userAgentTransport).base.(*http.Transport)
		defer transport.CloseIdleConnections()
		response, err := authenticator.client.Get(os.Getenv("DATABRICKS_OAUTH_ENV_URL"))
		if response != nil {
			require.NoError(t, response.Body.Close())
		}
		if mode == "proxy" {
			require.ErrorContains(t, err, "Bad Gateway")
		} else {
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, response.StatusCode)
		}
		return
	}
	for _, mode := range []string{"proxy", "system-roots"} {
		t.Run(mode, func(t *testing.T) {
			var hits atomic.Int32
			handler := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				hits.Add(1)
				if mode == "proxy" {
					assert.Equal(t, http.MethodConnect, request.Method)
					assert.Equal(t, "accounts.cloud.databricks.com:443", request.Host)
					http.Error(writer, "OAuth proxy used", http.StatusBadGateway)
					return
				}
				_, _ = io.WriteString(writer, "trusted OAuth transport")
			})
			var server *httptest.Server
			environment := []string{"HTTPS_PROXY=", "https_proxy=", "HTTP_PROXY=", "http_proxy=", "NO_PROXY=", "no_proxy="}
			target := "https://accounts.cloud.databricks.com/token"
			if mode == "proxy" {
				server = httptest.NewServer(handler)
				environment = append(environment, "HTTPS_PROXY="+server.URL)
			} else {
				server = httptest.NewTLSServer(handler)
				target = server.URL
				certFile := filepath.Join(t.TempDir(), "oauth-ca.pem")
				require.NoError(t, os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600))
				environment = append(environment, "SSL_CERT_FILE="+certFile)
			}
			t.Cleanup(server.Close)
			executable, err := os.Executable()
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, executable, "-test.run=^TestOAuthTransportEnvironment$", "-test.timeout=15s")
			command.Env = append(os.Environ(), environment...)
			command.Env = append(command.Env, "DATABRICKS_OAUTH_ENV_TEST="+mode, "DATABRICKS_OAUTH_ENV_URL="+target)
			output, err := command.CombinedOutput()
			require.NoError(t, err, "%s", output)
			assert.Equal(t, int32(1), hits.Load())
		})
	}
}
