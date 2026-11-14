package client

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/databricks/databricks-sql-go/auth/oauth/m2m"
	"github.com/databricks/databricks-sql-go/internal/config"
)

type oauthBenchmarkTransport struct{ blocked bool }

func (transport oauthBenchmarkTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if transport.blocked {
		return nil, errors.New("OAuth bypassed connector transport")
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": {"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"access_token":"benchmark-token","token_type":"Bearer","expires_in":1}`)),
		Request:    request,
	}, nil
}

func BenchmarkM2MTokenGrant(benchmark *testing.B) {
	previousClient := http.DefaultClient
	http.DefaultClient = &http.Client{Transport: oauthBenchmarkTransport{blocked: true}}
	benchmark.Cleanup(func() { http.DefaultClient = previousClient })
	cfg := config.WithDefaults()
	cfg.Authenticator = m2m.NewAuthenticator("benchmark-client", "benchmark-secret", "adb-test.azuredatabricks.net")
	cfg.Transport = oauthBenchmarkTransport{}
	cfg.UserAgentEntry = "Issue413/benchmark"
	authenticator := PooledClient(cfg).Transport.(*Transport).Authr
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://adb-test.azuredatabricks.net/query", nil)
	if err != nil {
		benchmark.Fatal(err)
	}
	benchmark.ReportAllocs()
	for benchmark.Loop() {
		if err := authenticator.Authenticate(request); err != nil {
			benchmark.Fatal(err)
		}
	}
}
