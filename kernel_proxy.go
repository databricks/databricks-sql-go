package dbsql

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/databricks/databricks-sql-go/v2/internal/config"
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

// kernelEndpointHost is the host:port the kernel dials, used only to pick the proxy
// (HTTPS_PROXY / NO_PROXY). It applies the kernel's simple port rule without the
// kernel's validation: a port of 0 or 443 leaves the host as given; a host that already
// carries a port keeps it (an embedded port wins over WithPort, as in the kernel); any
// other port is appended, bracketing an IPv6 literal. A trailing "/" is trimmed. A
// malformed host is passed through, since the kernel rejects it at session open. It
// returns no error and never echoes the host, so it cannot leak credentials.
func kernelEndpointHost(host string, port int) string {
	host = strings.TrimRight(host, "/")
	if port == 0 || port == 443 {
		return host
	}
	if _, _, err := net.SplitHostPort(host); err == nil {
		return host
	}
	bare := strings.TrimSuffix(strings.TrimPrefix(host, "["), "]")
	return net.JoinHostPort(bare, strconv.Itoa(port))
}

// proxyForEndpointFunc is the testable core: it builds the endpoint request and
// asks resolve for the proxy, returning "" (direct) on any error, no proxy, or an
// unbuildable endpoint. resolve is http.ProxyFromEnvironment in production; tests
// inject a deterministic resolver to exercise proxy-set / NO_PROXY / direct
// without depending on http.ProxyFromEnvironment's process-wide env caching.
func proxyForEndpointFunc(cfg *config.Config, resolve func(*http.Request) (*url.URL, error)) string {
	// Build the endpoint from scheme/host/port directly rather than via
	// cfg.ToEndpointURL(): that requires a non-empty HTTPPath and errors in the
	// warehouse-id addressing mode the kernel prefers (HTTPPath == ""), which would
	// swallow the error and silently return "" (direct) — ignoring HTTPS_PROXY.
	// Only scheme+host matter for the proxy/NO_PROXY decision; the path is
	// irrelevant, so omitting it is correct here.
	if cfg.Host == "" {
		return ""
	}
	scheme := cfg.Protocol
	if scheme == "" {
		scheme = "https"
	}
	endpoint := fmt.Sprintf("%s://%s", scheme, kernelEndpointHost(cfg.Host, cfg.Port))
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
