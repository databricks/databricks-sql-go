package dbsql

import (
	"net/http"
	"testing"
	"time"

	"github.com/databricks/databricks-sql-go/auth"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type m2mConnectorClientRecorder struct {
	auth.Authenticator
	client *http.Client
}

func (recorder *m2mConnectorClientRecorder) WithHTTPClient(client *http.Client) auth.Authenticator {
	recorder.client = client
	return recorder.Authenticator.(interface {
		WithHTTPClient(*http.Client) auth.Authenticator
	}).WithHTTPClient(client)
}

func TestM2MConnectorHTTPConfiguration(t *testing.T) {
	for _, host := range []string{"test.cloud.databricks.com", "test.gcp.databricks.com", "adb-test.azuredatabricks.net"} {
		for _, order := range []string{"auth-first", "auth-last", "dsn", "dsn-inferred-auth"} {
			t.Run(host+"/"+order, func(t *testing.T) {
				transport := &http.Transport{}
				t.Cleanup(transport.CloseIdleConnections)
				options := []ConnOption{WithServerHostname(host), WithPort(443), WithHTTPPath("/query")}
				httpOptions := []ConnOption{WithUserAgentEntry("Issue413/partner"), WithTransport(transport)}
				switch order {
				case "auth-first":
					options = append(options, WithClientCredentials("test-client", "test-secret"))
					options = append(options, httpOptions...)
				case "auth-last":
					options = append(options, httpOptions...)
					options = append(options, WithClientCredentials("test-client", "test-secret"))
				default:
					dsn := host + ":443/query?clientID=test-client&clientSecret=test-secret&userAgentEntry=Issue413%2Fpartner"
					if order == "dsn" {
						dsn = "https://" + dsn + "&authType=OauthM2M"
					}
					userConfig, err := config.ParseDSN(dsn)
					require.NoError(t, err)
					options = []ConnOption{withUserConfig(userConfig), WithTransport(transport)}
				}
				var recorder *m2mConnectorClientRecorder
				options = append(options, func(cfg *config.Config) {
					cfg.ClientTimeout = 7 * time.Second
					recorder = &m2mConnectorClientRecorder{Authenticator: cfg.Authenticator}
					cfg.Authenticator = recorder
				})
				created, err := NewConnector(options...)
				require.NoError(t, err)
				connector := created.(*connector)
				require.NotNil(t, recorder.client)
				assert.Equal(t, 7*time.Second, recorder.client.Timeout)
				assert.NotSame(t, transport, recorder.client.Transport)
				assert.Contains(t, client.BuildUserAgent(connector.cfg), "Issue413/partner")
				retryable := connector.client.Transport.(*retryablehttp.RoundTripper).Client
				assert.Same(t, transport, retryable.HTTPClient.Transport.(*client.Transport).Base)
			})
		}
	}
}
