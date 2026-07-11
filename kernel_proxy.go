package dbsql

import (
	"net/http"
	"net/url"

	"github.com/databricks/databricks-sql-go/internal/config"
)

// This file is intentionally NOT behind the `cgo && databricks_kernel` build tag:
// proxy resolution is pure Go (no kernel C symbol), so keeping it in the default
// build lets its test run under CGO_ENABLED=0 rather than being dead behind the
// tag. newKernelBackend (tagged) calls proxyForEndpoint.

// proxyForEndpoint resolves the proxy the Thrift path would use for this
// connection, via the same http.ProxyFromEnvironment (HTTP(S)_PROXY / NO_PROXY)
// the Thrift transport applies at request time. Building the endpoint request
// lets ProxyFromEnvironment apply the NO_PROXY rules for this exact host, so the
// kernel sees the same effective proxy decision — returning "" (direct) when
// NO_PROXY excludes the host or no proxy is set. No extra dependency: this is the
// stdlib function the driver already relies on.
func proxyForEndpoint(cfg *config.Config) string {
	return proxyForEndpointFunc(cfg, http.ProxyFromEnvironment)
}

// proxyForEndpointFunc is the testable core: it builds the endpoint request and
// asks resolve for the proxy, returning "" (direct) on any error, no proxy, or an
// unbuildable endpoint. resolve is http.ProxyFromEnvironment in production; tests
// inject a deterministic resolver to exercise proxy-set / NO_PROXY / direct
// without depending on http.ProxyFromEnvironment's process-wide env caching.
func proxyForEndpointFunc(cfg *config.Config, resolve func(*http.Request) (*url.URL, error)) string {
	endpoint, err := cfg.ToEndpointURL()
	if err != nil {
		return ""
	}
	req, err := http.NewRequest(http.MethodPost, endpoint, nil)
	if err != nil {
		return ""
	}
	proxyURL, err := resolve(req)
	if err != nil || proxyURL == nil {
		return ""
	}
	return proxyURL.String()
}
