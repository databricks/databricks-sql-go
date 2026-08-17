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
	// IdentityFederationClientID selects mandatory SP-wide workload identity
	// federation. Empty preserves BYOT / account-wide behavior.
	IdentityFederationClientID string

	// UserAgent is forwarded as the User-Agent header so the kernel path is
	// attributed to this driver (not the kernel's built-in UA). Empty leaves it unset.
	UserAgent string

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

	// ProxyURL configures an HTTP proxy. It is either resolved for this endpoint
	// from the same HTTP(S)_PROXY / NO_PROXY environment the Thrift path uses
	// (NO_PROXY applied during resolution), or set explicitly via WithKernelProxy.
	// Empty leaves the kernel on a direct connection.
	//
	// ProxyUsername / ProxyPassword are out-of-band basic-auth credentials (an
	// alternative to embedding them in ProxyURL's userinfo). ProxyBypassHosts is
	// a comma-separated no-proxy list honored kernel-side. All three are only
	// meaningful with an explicit WithKernelProxy — the env-var path folds
	// credentials into the URL and consumes NO_PROXY during resolution, so it
	// leaves these empty. Empty fields are passed as NULL (kernel default).
	ProxyURL         string
	ProxyUsername    string
	ProxyPassword    string
	ProxyBypassHosts string

	// Retry carries the driver's WithRetries backoff/attempt policy (RetryWaitMin
	// / RetryWaitMax / RetryMax) plus the kernel-only overall retry budget. nil
	// leaves the kernel on its own default retry policy; non-nil forwards to
	// set_retry_config so the caller's policy is authoritative. A pointer so
	// "unset" is distinct from an explicit zero-retry (disable) request.
	Retry *RetryConfig

	// Location is the session time zone used to render DATE / TIMESTAMP values,
	// matching the Thrift path which returns them in this location. nil means UTC.
	Location *time.Location

	// Catalog / Schema select the initial namespace. The kernel C ABI has no
	// catalog/schema config setter, so OpenSession applies them post-connect by
	// running USE CATALOG / USE SCHEMA. Empty leaves the session in the server
	// default namespace.
	Catalog string
	Schema  string

	// DecimalAsFloat scans top-level DECIMAL columns to a lossy float64 instead of
	// the exact string (from WithKernelDecimalAsFloat). Kernel still sends Decimal128.
	DecimalAsFloat bool
}

// RetryConfig is the driver's HTTP retry policy forwarded to the kernel: the
// backoff-wait bounds, the maximum number of retries after the initial attempt
// (MaxRetries == 0 disables retries), and the cumulative retry budget across all
// attempts (OverallTimeout; zero keeps the kernel's default 900s budget). The
// connector fills MinWait/MaxWait/MaxRetries from WithRetries and OverallTimeout
// from WithKernelRetryOverallTimeout; the kernel's own retry policy applies when
// Config.Retry is nil. Maps to kernel_session_config_set_retry_config.
type RetryConfig struct {
	MinWait    time.Duration
	MaxWait    time.Duration
	MaxRetries uint32
	// OverallTimeout is the cumulative retry budget; zero => keep the kernel
	// default (900s). Mirrors the pyo3/napi retry_overall_timeout knob.
	OverallTimeout time.Duration
}
