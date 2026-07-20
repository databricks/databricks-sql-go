package kernel

// This file is intentionally NOT behind the `cgo && databricks_kernel` build tag:
// Config is a plain data struct with no cgo, so it compiles in the default build
// too. That lets the connector's pure config-assembly (buildKernelConfig, package
// dbsql) name kernel.Config and be unit-tested under CGO_ENABLED=0; OpenSession
// (tagged) is what maps the assembled Config onto the kernel's C setters.

import "time"

// Config is the flat connection config for the kernel backend. The connector
// fills it from the driver's config so the user-facing options are unchanged.
// Zero-valued fields are simply not applied.
type Config struct {
	Host        string // workspace hostname, no scheme
	HTTPPath    string // e.g. /sql/1.0/warehouses/abc123 (carries ?o= org routing)
	WarehouseID string // bare warehouse id; preferred over HTTPPath when set
	Auth        Auth   // PAT / OAuth M2M / OAuth U2M

	// SessionConf carries server-bound session confs verbatim — the same map the
	// Thrift backend forwards (STATEMENT_TIMEOUT, QUERY_TAGS, TIMEZONE, …).
	SessionConf map[string]string

	// TLSSkipVerify accepts any server cert (maps the driver's
	// WithSkipTLSHostVerify / TLSConfig.InsecureSkipVerify). crypto/tls's
	// InsecureSkipVerify disables both chain validation and the hostname check,
	// so the kernel path relaxes both to match.
	TLSSkipVerify bool

	// Experimental kernel-only TLS knobs (from the WithKernel* options). These
	// have no Thrift-path equivalent and are set via config.KernelExperimental.
	// Empty/false fields are simply not applied. TLSTrustedCertsPEM is a custom CA
	// bundle added on top of the system roots; TLSSkipHostnameVerify skips only the
	// hostname check (finer-grained than the blanket TLSSkipVerify above).
	TLSTrustedCertsPEM    []byte
	TLSSkipHostnameVerify bool

	// TLSClientCertPEM / TLSClientKeyPEM are the mTLS client cert (+ optional
	// chain) and matching private key (from WithKernelClientCertificate). They
	// travel as a pair — both set or both empty — and are forwarded via the single
	// paired kernel_session_config_set_tls_client_certificate. The key is never
	// logged.
	TLSClientCertPEM []byte
	TLSClientKeyPEM  []byte

	// ProxyURL configures an HTTP proxy, already resolved for this endpoint from
	// the same HTTP(S)_PROXY / NO_PROXY environment the Thrift path uses (NO_PROXY
	// is applied during resolution). Empty leaves the kernel on a direct
	// connection.
	ProxyURL string

	// Location is the session time zone used to render DATE / TIMESTAMP values,
	// matching the Thrift path which returns them in this location. nil means UTC.
	Location *time.Location

	// Catalog / Schema select the initial namespace. The kernel C ABI has no
	// catalog/schema config setter, so OpenSession applies them post-connect by
	// running USE CATALOG / USE SCHEMA. Empty leaves the session in the server
	// default namespace.
	Catalog string
	Schema  string
}
