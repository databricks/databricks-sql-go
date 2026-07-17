package dbsql

import (
	"os"
	"testing"
)

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
	if host == "" || httpPath == "" || token == "" {
		t.Skip("set DATABRICKS_PECOTESTING_SERVER_HOSTNAME, DATABRICKS_PECOTESTING_HTTP_PATH2, and DATABRICKS_PECOTESTING_TOKEN to run")
	}
	return host, httpPath, token
}
