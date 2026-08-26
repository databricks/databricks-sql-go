package dbsql

import (
	"context"
	"crypto/tls"
	"database/sql/driver"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/databricks/databricks-sql-go/auth"
	"github.com/databricks/databricks-sql-go/auth/oauth/m2m"
	"github.com/databricks/databricks-sql-go/auth/pat"
	"github.com/databricks/databricks-sql-go/auth/tokenprovider"
	"github.com/databricks/databricks-sql-go/driverctx"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/databricks/databricks-sql-go/internal/backend"
	"github.com/databricks/databricks-sql-go/internal/backend/thrift"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/databricks/databricks-sql-go/internal/debuglog"
	"github.com/databricks/databricks-sql-go/logger"
	"github.com/databricks/databricks-sql-go/telemetry"
)

type connector struct {
	cfg    *config.Config
	client *http.Client
}

// interactiveU2MAuthenticator is satisfied only by the browser-based U2M
// authenticator (U2MClientID is unique to it; PAT/M2M lack it). Matches the
// structural check the kernel backend uses to detect U2M.
type interactiveU2MAuthenticator interface {
	U2MClientID() string
}

// federatedTokenAuthenticator preserves the base provider for the kernel.
type federatedTokenAuthenticator struct {
	auth.Authenticator
	provider tokenprovider.TokenProvider
	clientID string
}

// Connect returns a connection to the Databricks database from a connection pool.
func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	defer debuglog.Track(ctx, "connector.Connect", "host=%s", c.cfg.Host)()

	// Build the execution backend. Thrift is the default; the SEA-via-kernel
	// backend is selected when UseKernel is set. newKernelBackend is build-tag
	// gated: in the default pure-Go build it returns a clear "not linked in"
	// error, so the kernel path compiles and links only under -tags
	// databricks_kernel + CGO_ENABLED=1.
	var be backend.Backend
	var err error
	if c.cfg.UseKernel {
		be, err = newKernelBackend(ctx, c.cfg)
	} else {
		// The experimental WithKernel* options have no Thrift-path equivalent — reject
		// them loudly rather than silently ignore, so a caller who sets one (a
		// trusted-CA bundle, a hostname-verify skip, a proxy, a retry budget, or a
		// CloudFetch chunk cap) and forgets WithUseKernel learns the option had no
		// effect instead of connecting as if it were never set. Every WithKernel*
		// option allocates KernelExperimental, so this one gate covers them all; the
		// message names the family rather than a stale subset that drifts as options
		// are added.
		if c.cfg.KernelExperimental != nil {
			return nil, fmt.Errorf("databricks: a WithKernel* option %w; "+
				"add WithUseKernel(true) or remove it", dbsqlerr.ErrRequiresKernelBackend)
		}
		be, err = thrift.New(ctx, c.cfg, c.client)
	}
	if err != nil {
		return nil, err
	}

	sessionStart := time.Now()
	if err := be.OpenSession(ctx); err != nil {
		return nil, err
	}
	sessionLatencyMs := time.Since(sessionStart).Milliseconds()

	conn := &conn{
		id:      be.SessionID(),
		cfg:     c.cfg,
		backend: be,
	}
	log := logger.WithContext(conn.id, driverctx.CorrelationIdFromContext(ctx), "")

	// Extract SPOG routing headers from HTTPPath. When the workspace ID is
	// available via ?o=<workspaceId> or a cluster /o/<workspaceId>/ path segment,
	// wrap the HTTP client used for telemetry + feature-flag calls with a
	// transport that injects x-databricks-org-id. Thrift routes via the URL so
	// its own c.client doesn't need wrapping.
	telemetryClient := c.client
	if spogHeaders := extractSpogHeaders(c.cfg.HTTPPath); len(spogHeaders) > 0 {
		telemetryClient = withSpogHeaders(c.client, spogHeaders)
	}

	// Skip telemetry on the kernel U2M path: the kernel owns the interactive browser
	// flow, so the telemetry/feature-flag call through the interactive authenticator
	// would launch a second, redundant browser (and can block connect on its
	// callback). Telemetry is best-effort, so it's dropped here rather than made to
	// prompt. Unauthenticated telemetry (Python/Node parity) is tracked in PECOBLR-3839.
	skipTelemetry := false
	if c.cfg.UseKernel {
		if _, isU2M := c.cfg.Authenticator.(interactiveU2MAuthenticator); isU2M {
			skipTelemetry = true
			log.Debug().Msg("telemetry skipped: kernel U2M owns the interactive auth flow")
		}
	}

	// Initialize telemetry: client config overlay decides; if unset, feature flags decide
	if !skipTelemetry {
		conn.telemetry = telemetry.InitializeForConnection(ctx, telemetry.TelemetryInitOptions{
			Host:            c.cfg.Host,
			DriverVersion:   c.cfg.DriverVersion,
			UserAgent:       client.BuildUserAgent(c.cfg),
			HTTPClient:      telemetryClient,
			EnableTelemetry: c.cfg.EnableTelemetry,
			BatchSize:       c.cfg.TelemetryBatchSize,
			FlushInterval:   c.cfg.TelemetryFlushInterval,
		})
	}
	if conn.telemetry != nil {
		log.Debug().Msg("telemetry initialized for connection")
		conn.telemetry.RecordOperation(ctx, conn.id, "", telemetry.OperationTypeCreateSession, sessionLatencyMs, nil)
		// Connection-configuration telemetry on the kernel path only, so the
		// default (Thrift) path's emitted telemetry stays byte-identical (the
		// Thrift path has never populated DriverConnectionParameters). Emits mode /
		// auth mech+flow / proxy / arrow / query-tags / metric-view for the
		// just-opened session. Gated on the kernel backend, not just WithUseKernel,
		// so it never fires when the kernel wasn't actually selected.
		if _, ok := be.(*thrift.Backend); !ok {
			conn.telemetry.RecordConnectionConfig(ctx, conn.id, kernelConnectionTelemetry(c.cfg))
		}
	}

	// ServerProtocolVersion is Thrift-specific (not on the neutral backend
	// interface); the kernel backend has no negotiated Thrift protocol, so log it
	// only when present.
	if tb, ok := be.(*thrift.Backend); ok {
		log.Info().Msgf("connect: host=%s port=%d httpPath=%s serverProtocolVersion=0x%X", c.cfg.Host, c.cfg.Port, c.cfg.HTTPPath, tb.ServerProtocolVersion())
	} else {
		log.Info().Msgf("connect: host=%s port=%d httpPath=%s backend=kernel", c.cfg.Host, c.cfg.Port, c.cfg.HTTPPath)
	}

	return conn, nil
}

// Driver returns underlying databricksDriver for compatibility with sql.DB Driver method
func (c *connector) Driver() driver.Driver {
	return &databricksDriver{}
}

var _ driver.Connector = (*connector)(nil)

type ConnOption func(*config.Config)

// NewConnector creates a connection that can be used with `sql.OpenDB()`.
// This is an easier way to set up the DB instead of having to construct a DSN string.
func NewConnector(options ...ConnOption) (driver.Connector, error) {
	// config with default options
	cfg := config.WithDefaults()
	cfg.DriverVersion = DriverVersion

	for _, opt := range options {
		opt(cfg)
	}

	client := client.RetryableClient(cfg)

	return &connector{cfg: cfg, client: client}, nil
}

// clusterPathOrgIDPattern matches the workspace ID inside an all-purpose-compute
// Thrift path of the form [/]sql/protocolv1/o/<workspace-id>/<cluster-id>[/...].
var (
	orgIDPattern            = regexp.MustCompile(`^[0-9]+$`)
	clusterPathOrgIDPattern = regexp.MustCompile(`^/?sql/protocolv1/o/([0-9]+)/[^/?]+`)
)

// extractSpogHeaders inspects httpPath for the workspace ID and returns it as an
// x-databricks-org-id header dict for SPOG routing.
//
// Two sources are checked, in priority order:
//  1. ?o=<workspace-id> query parameter (warehouse paths on SPOG typically use
//     this form, e.g. /sql/1.0/warehouses/<id>?o=<workspace-id>).
//  2. /sql/protocolv1/o/<workspace-id>/<cluster-id> path segment (all-purpose
//     cluster paths embed the workspace in the path itself).
//
// Thrift requests are routed by the URL itself, but other endpoints
// (telemetry, feature flags) run on separate paths that don't carry the
// workspace ID — without this header, PoPP on SPOG hosts can't determine the
// workspace and redirects the request to /login.
//
// Returns nil if no workspace ID can be determined.
func extractSpogHeaders(httpPath string) map[string]string {
	if httpPath == "" {
		return nil
	}

	// 1) ?o=<wsid> query parameter.
	if strings.Contains(httpPath, "?") {
		parts := strings.SplitN(httpPath, "?", 2)
		params, err := url.ParseQuery(parts[1])
		if err != nil {
			logger.Debug().Msgf(
				"SPOG header extraction: malformed query string in httpPath, falling back to path inspection: %s",
				err)
		} else if orgID := params.Get("o"); orgID != "" {
			if !orgIDPattern.MatchString(orgID) {
				logger.Debug().Msg(
					"SPOG header extraction: ignoring non-numeric ?o= value in httpPath, falling back to path inspection")
			} else {
				logger.Debug().Msgf(
					"SPOG header extraction: injecting x-databricks-org-id=%s (extracted from ?o= in httpPath)",
					orgID)
				return map[string]string{"x-databricks-org-id": orgID}
			}
		}
	}

	// 2) /sql/protocolv1/o/<wsid>/<cluster> path segment.
	if match := clusterPathOrgIDPattern.FindStringSubmatch(httpPath); match != nil {
		orgID := match[1]
		logger.Debug().Msgf(
			"SPOG header extraction: injecting x-databricks-org-id=%s (extracted from cluster path segment)",
			orgID)
		return map[string]string{"x-databricks-org-id": orgID}
	}

	logger.Debug().Msg(
		"SPOG header extraction: no workspace ID found in httpPath, " +
			"skipping x-databricks-org-id injection")
	return nil
}

// withSpogHeaders returns a new *http.Client that reuses the transport of the
// provided client, wrapped to inject the given SPOG headers on every outbound
// request. The original client is left unchanged. If a request already has a
// given header set (e.g., the caller set it explicitly), the wrapper does not
// override it.
//
// This is how the driver gets x-databricks-org-id onto both the feature-flag
// check and the telemetry push without touching the telemetry package's
// signatures.
func withSpogHeaders(base *http.Client, headers map[string]string) *http.Client {
	baseTransport := base.Transport
	if baseTransport == nil {
		baseTransport = http.DefaultTransport
	}
	return &http.Client{
		Transport: &headerInjectingTransport{
			base:    baseTransport,
			headers: headers,
		},
		CheckRedirect: base.CheckRedirect,
		Jar:           base.Jar,
		Timeout:       base.Timeout,
	}
}

// headerInjectingTransport wraps an http.RoundTripper and sets a fixed set of
// headers on every outbound request. Caller-supplied headers with the same
// name are not overridden.
type headerInjectingTransport struct {
	base    http.RoundTripper
	headers map[string]string
}

// RoundTrip implements http.RoundTripper.
func (t *headerInjectingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Clone per RoundTripper contract — must not mutate the caller's request.
	req2 := req.Clone(req.Context())
	for k, v := range t.headers {
		if req2.Header.Get(k) == "" {
			req2.Header.Set(k, v)
		}
	}
	return t.base.RoundTrip(req2)
}

func withUserConfig(ucfg config.UserConfig) ConnOption {
	return func(c *config.Config) {
		c.UserConfig = ucfg
		// The useArrowNativeDecimal DSN parameter is carried on UserConfig (all
		// ParseDSN can return) but is consumed from ArrowConfig. This is the one
		// place that bridges the two.
		c.ArrowConfig.UseArrowNativeDecimal = ucfg.UseArrowNativeDecimalDSN
		// The tokenCache DSN parameter is carried on UserConfig but is consumed from
		// KernelExperimental (kernel-only). This is the one place that bridges the two.
		//
		// Intentional divergence from WithTokenCache: we only allocate/set
		// KernelExperimental when tokenCache=true. WithTokenCache(false) allocates it
		// unconditionally, which makes KernelExperimental != nil and is rejected on the
		// Thrift path with ErrRequiresKernelBackend. Here we treat DSN tokenCache=false
		// as a harmless no-op (in-memory is already the default) rather than opting the
		// connection into the kernel-only backend just to disable a feature.
		//
		// Precedence: because false is a no-op, a tokenCache=false DSN cannot reset a
		// TokenCacheEnabled that a prior WithTokenCache(true) set, while tokenCache=true
		// does override. This only matters if a caller mixes DSN and option sources;
		// with a single source the default (false) is correct.
		if ucfg.TokenCacheEnabledDSN {
			kernelExperimental(c).TokenCacheEnabled = true
		}
	}
}

// WithServerHostname sets up the server hostname. Mandatory.
func WithServerHostname(host string) ConnOption {
	return func(c *config.Config) {
		protocol, hostname := parseHostName(host)
		if protocol != "" {
			c.Protocol = protocol
		}

		c.Host = hostname
	}
}

func parseHostName(host string) (protocol, hostname string) {
	hostname = host
	if strings.HasPrefix(host, "https") {
		hostname = strings.TrimPrefix(host, "https")
		protocol = "https"
	} else if strings.HasPrefix(host, "http") {
		hostname = strings.TrimPrefix(host, "http")
		protocol = "http"
	}

	if protocol != "" {
		hostname = strings.TrimPrefix(hostname, ":")
		hostname = strings.TrimPrefix(hostname, "//")
	}

	if hostname == "localhost" && protocol == "" {
		protocol = "http"
	}

	return
}

// WithPort sets up the server port. Mandatory.
func WithPort(port int) ConnOption {
	return func(c *config.Config) {
		c.Port = port
	}
}

// WithRetries sets up retrying logic. Sane defaults are provided. Negative retryMax will disable retry behavior
// By default retryWaitMin = 1 * time.Second
// By default retryWaitMax = 30 * time.Second
// By default retryMax = 4
func WithRetries(retryMax int, retryWaitMin time.Duration, retryWaitMax time.Duration) ConnOption {
	return func(c *config.Config) {
		c.RetryWaitMax = retryWaitMax
		c.RetryWaitMin = retryWaitMin
		c.RetryMax = retryMax
	}
}

// WithAccessToken sets up the Personal Access Token. Mandatory for now.
func WithAccessToken(token string) ConnOption {
	return func(c *config.Config) {
		if token != "" {
			c.AccessToken = token
			pat := &pat.PATAuth{
				AccessToken: token,
			}
			c.Authenticator = pat
		}
	}
}

// WithHTTPPath sets up the endpoint to the warehouse. Mandatory.
func WithHTTPPath(path string) ConnOption {
	return func(c *config.Config) {
		if !strings.HasPrefix(path, "/") {
			path = "/" + path
		}
		c.HTTPPath = path
	}
}

// WithUseKernel selects the SEA-via-kernel backend instead of the default
// Thrift backend. It has effect only in a build compiled with
// `-tags databricks_kernel` and CGO_ENABLED=1; in the default pure-Go build a
// connection made with this option set returns a clear error at connect time
// (the kernel backend is not linked in).
func WithUseKernel(useKernel bool) ConnOption {
	return func(c *config.Config) {
		c.UseKernel = useKernel
	}
}

// WithWarehouseID sets the bare SQL warehouse id. It has no effect unless
// WithUseKernel(true) is also set: the kernel backend addresses a warehouse by id
// (preferred over the http path when set), while the default Thrift backend
// ignores it entirely and continues to route by http path.
func WithWarehouseID(id string) ConnOption {
	return func(c *config.Config) {
		c.WarehouseID = id
	}
}

// WithMaxRows sets up the max rows fetched per request. Default is 100000
func WithMaxRows(n int) ConnOption {
	return func(c *config.Config) {
		if n != 0 {
			c.MaxRows = n
		}
	}
}

// WithTimeout adds timeout for the server query execution. Default is no timeout.
func WithTimeout(n time.Duration) ConnOption {
	return func(c *config.Config) {
		c.QueryTimeout = n
	}
}

// Sets the initial catalog name and schema name in the session.
// Use <select * from foo> instead of <select * from catalog.schema.foo>
func WithInitialNamespace(catalog, schema string) ConnOption {
	return func(c *config.Config) {
		c.Catalog = catalog
		c.Schema = schema
	}
}

// Used to identify partners. Set as a string with format <isv-name+product-name>.
func WithUserAgentEntry(entry string) ConnOption {
	return func(c *config.Config) {
		c.UserAgentEntry = entry
	}
}

// Session parameters are passed directly in TOpenSessionReq.Configuration during session creation.
func WithSessionParams(params map[string]string) ConnOption {
	return func(c *config.Config) {
		for k, v := range params {
			if strings.ToLower(k) == "timezone" {
				if loc, err := time.LoadLocation(v); err != nil {
					logger.Error().Msgf("timezone %s is not valid", v)
				} else {
					c.Location = loc
				}
			}
		}
		c.SessionParams = params
	}
}

// WithQueryTags sets session-level query tags from a map.
// Tags are serialized and passed as QUERY_TAGS in the session configuration.
// All queries in the session will carry these tags unless overridden at the statement level.
// This is the preferred way to set session-level query tags, as it handles serialization
// and escaping automatically (consistent with the statement-level API).
func WithQueryTags(tags map[string]string) ConnOption {
	return func(c *config.Config) {
		serialized := SerializeQueryTags(tags)
		if serialized != "" {
			if c.SessionParams == nil {
				c.SessionParams = make(map[string]string)
			}
			c.SessionParams["QUERY_TAGS"] = serialized
		}
	}
}

// WithSkipTLSHostVerify disables the verification of the hostname in the TLS certificate.
// WARNING:
// When this option is used, TLS is susceptible to machine-in-the-middle attacks.
// Please only use this option when the hostname is an internal private link hostname
func WithSkipTLSHostVerify() ConnOption {
	return func(c *config.Config) {
		if c.TLSConfig == nil {
			c.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: true} // #nosec G402
		} else {
			c.TLSConfig.InsecureSkipVerify = true // #nosec G402
		}
	}
}

// WithAuthenticator sets up the Authentication. Mandatory if access token is not provided.
func WithAuthenticator(authr auth.Authenticator) ConnOption {
	return func(c *config.Config) {
		c.Authenticator = authr
	}
}

// WithTransport sets up the transport configuration to be used by the httpclient.
func WithTransport(t http.RoundTripper) ConnOption {
	return func(c *config.Config) {
		c.Transport = t

		if c.HTTPClient == nil {
			c.HTTPClient = &http.Client{
				Transport: t,
			}
		}
	}
}

// WithCloudFetch sets up the use of cloud fetch for query execution. Default is true.
func WithCloudFetch(useCloudFetch bool) ConnOption {
	return func(c *config.Config) {
		c.UseCloudFetch = useCloudFetch
	}
}

// WithMaxDownloadThreads sets up maximum download threads for cloud fetch. Default is 10.
func WithMaxDownloadThreads(numThreads int) ConnOption {
	return func(c *config.Config) {
		c.MaxDownloadThreads = numThreads
	}
}

// WithEnableMetricViewMetadata enables metric view metadata support. Default is false.
// When enabled, adds spark.sql.thriftserver.metadata.metricview.enabled=true to session configuration.
func WithEnableMetricViewMetadata(enable bool) ConnOption {
	return func(c *config.Config) {
		c.EnableMetricViewMetadata = enable
	}
}

// WithArrowNativeDecimal controls whether DECIMAL columns are returned as native
// Arrow decimal128 values. Default is false, in which case the server returns
// DECIMAL columns as strings.
//
// When enabled, DECIMAL columns retrieved via GetArrowBatches carry the native
// arrow.Decimal128 type. When scanned through the standard database/sql Rows
// interface, DECIMAL values are returned as lossless, scale-applied strings to
// avoid the precision loss that a float64 would introduce.
//
// See https://github.com/databricks/databricks-sql-go/issues/274.
func WithArrowNativeDecimal(useNativeDecimal bool) ConnOption {
	return func(c *config.Config) {
		c.ArrowConfig.UseArrowNativeDecimal = useNativeDecimal
	}
}

// Setup of Oauth M2m authentication
func WithClientCredentials(clientID, clientSecret string) ConnOption {
	return func(c *config.Config) {
		if clientID != "" && clientSecret != "" {
			authr := m2m.NewAuthenticator(clientID, clientSecret, c.Host)
			c.Authenticator = authr
		}
	}
}

// WithTokenProvider sets up authentication using a custom token provider
func WithTokenProvider(provider tokenprovider.TokenProvider) ConnOption {
	return func(c *config.Config) {
		if provider != nil {
			c.Authenticator = tokenprovider.NewAuthenticator(provider)
		}
	}
}

// WithExternalToken sets up authentication using an external token function (passthrough)
func WithExternalToken(tokenFunc func() (string, error)) ConnOption {
	return func(c *config.Config) {
		if tokenFunc != nil {
			provider := tokenprovider.NewExternalTokenProvider(tokenFunc)
			c.Authenticator = tokenprovider.NewAuthenticator(provider)
		}
	}
}

// WithStaticToken sets up authentication using a static token
func WithStaticToken(token string) ConnOption {
	return func(c *config.Config) {
		if token != "" {
			provider := tokenprovider.NewStaticTokenProvider(token)
			c.Authenticator = tokenprovider.NewAuthenticator(provider)
		}
	}
}

// WithFederatedTokenProvider sets up authentication using token federation
// It wraps the base provider and automatically handles token exchange if needed
func WithFederatedTokenProvider(baseProvider tokenprovider.TokenProvider) ConnOption {
	return func(c *config.Config) {
		if baseProvider != nil {
			// Wrap with federation provider that auto-detects need for token exchange
			federationProvider := tokenprovider.NewFederationProvider(baseProvider, c.Host)
			c.Authenticator = &federatedTokenAuthenticator{
				Authenticator: tokenprovider.NewAuthenticator(federationProvider),
				provider:      baseProvider,
			}
		}
	}
}

// WithFederatedTokenProviderAndClientID sets up SP-wide token federation
func WithFederatedTokenProviderAndClientID(baseProvider tokenprovider.TokenProvider, clientID string) ConnOption {
	return func(c *config.Config) {
		if baseProvider != nil {
			// Wrap with federation provider for SP-wide federation
			federationProvider := tokenprovider.NewFederationProviderWithClientID(baseProvider, c.Host, clientID)
			c.Authenticator = &federatedTokenAuthenticator{
				Authenticator: tokenprovider.NewAuthenticator(federationProvider),
				provider:      baseProvider,
				clientID:      clientID,
			}
		}
	}
}

// ─── Experimental kernel-only options ─────────────────────────────────────────
//
// These configure the SEA-via-kernel backend (WithUseKernel) only; they expose a
// richer TLS surface than the backend-neutral WithSkipTLSHostVerify. They have no
// equivalent on the default (Thrift) path, which rejects them loudly at connect.
// They are deliberately NOT part of the stable DSN/UserConfig surface — they hang
// off config.Config.KernelExperimental (mirroring Node's non-exported
// InternalConnectionOptions). The WithKernel* prefix signals both "kernel-backend
// only" and "experimental" so they read distinctly from the backend-neutral
// options above (e.g. WithSkipTLSHostVerify).

// kernelExperimental lazily allocates and returns the experimental config block.
func kernelExperimental(c *config.Config) *config.KernelExperimentalConfig {
	if c.KernelExperimental == nil {
		c.KernelExperimental = &config.KernelExperimentalConfig{}
	}
	return c.KernelExperimental
}

// WithKernelDecimalAsFloat makes the kernel path scan top-level DECIMAL columns to
// a lossy float64 instead of the exact fixed-point string. The kernel still
// receives native Arrow Decimal128; this only changes how the Go scanner
// materializes each cell, skipping per-cell string formatting for a cheap scalar.
// Precision beyond ~15-17 digits is lost, so it is opt-in and off by default;
// it mirrors the Thrift driver's pre-UseArrowNativeDecimal behavior.
//
// EXPERIMENTAL, kernel-only: the default (Thrift) backend rejects this at connect
// (use WithArrowNativeDecimal there instead).
func WithKernelDecimalAsFloat(asFloat bool) ConnOption {
	return func(c *config.Config) {
		kernelExperimental(c).DecimalAsFloat = asFloat
	}
}

// WithKernelTrustedCerts adds a PEM CA-certificate bundle to the kernel's TLS
// trust store on top of the system roots — for a corporate re-signing proxy or an
// on-prem CA. Required (rather than relying on SSL_CERT_FILE) because the kernel's
// rustls stack does not read that environment variable.
//
// EXPERIMENTAL, kernel-only: the default (Thrift) backend rejects this at connect.
func WithKernelTrustedCerts(pem []byte) ConnOption {
	return func(c *config.Config) {
		// Copy defensively (matching KernelExperimentalConfig.DeepCopy) so a
		// caller mutating pem between NewConnector and Connect can't change the
		// trust store out from under us.
		if len(pem) > 0 {
			kernelExperimental(c).TLSTrustedCertsPEM = append([]byte(nil), pem...)
		} else {
			kernelExperimental(c).TLSTrustedCertsPEM = pem
		}
	}
}

// WithKernelClientCertificate configures the PEM-encoded client certificate and
// matching private key used when a server requires mutual TLS (mTLS). certPEM
// contains the leaf certificate followed by any intermediate certificates;
// keyPEM contains the matching unencrypted private key. PKCS#8 is recommended
// for portability across the kernel's TLS backends.
//
// Both values are required and must be non-empty. The driver copies them
// defensively and validates the pair at connect time. Server trust remains
// independent and strict by default; use WithKernelTrustedCerts when the server
// certificate chains to a private CA.
//
// EXPERIMENTAL, kernel-only: the default (Thrift) backend rejects this at connect.
func WithKernelClientCertificate(certPEM, keyPEM []byte) ConnOption {
	return func(c *config.Config) {
		ke := kernelExperimental(c)
		ke.TLSClientCertConfigured = true
		ke.TLSClientCertPEM = append([]byte(nil), certPEM...)
		ke.TLSClientKeyPEM = append([]byte(nil), keyPEM...)
	}
}

// WithKernelSkipHostnameVerify skips only the certificate hostname-vs-SNI check on
// the kernel backend, while keeping chain validation. This is finer-grained than
// WithSkipTLSHostVerify, which relaxes both chain and hostname checks.
// WARNING:
// Skipping hostname verification still weakens TLS: a certificate issued by a
// trusted CA for a different host will be accepted, opening a machine-in-the-middle
// vector. Only use this when the hostname is an internal private-link hostname that
// legitimately differs from the certificate's subject.
//
// EXPERIMENTAL, kernel-only: the default (Thrift) backend rejects this at connect.
func WithKernelSkipHostnameVerify() ConnOption {
	return func(c *config.Config) {
		kernelExperimental(c).TLSSkipHostnameVerify = true
	}
}

// KernelProxy is the explicit-proxy configuration for WithKernelProxy. Its fields
// are named so a call site can't transpose the credentials — the four values are
// all strings, so a positional signature would let a Username/Password swap (or a
// misplaced BypassHosts) compile cleanly and fail only at runtime with wrong proxy
// credentials.
type KernelProxy struct {
	// URL is the proxy URL (e.g. "http://proxy.internal:3128"). Required; empty is a
	// no-op, leaving the environment-derived proxy (if any) in effect.
	URL string
	// Username / Password are optional out-of-band basic-auth credentials, supplied
	// here rather than embedded in the URL userinfo. Empty means unset.
	Username string
	Password string
	// BypassHosts is an optional comma-separated no-proxy host list. NO_PROXY is
	// consumed during environment resolution and not forwarded to the kernel, so this
	// is the only way to give the kernel a structured bypass list. Empty means unset.
	BypassHosts string
}

// WithKernelProxy configures an explicit HTTP proxy for the kernel backend, with
// optional out-of-band basic-auth credentials and a comma-separated bypass
// (no-proxy) host list. It overrides the HTTP(S)_PROXY / NO_PROXY environment the
// kernel path otherwise mirrors from the Thrift path.
//
// Use this instead of the proxy environment when you need the "advanced" fields
// the env-var path can't express: a structured bypass list (NO_PROXY is consumed
// during environment resolution, not forwarded to the kernel) or basic-auth
// credentials supplied out of band rather than embedded in the URL userinfo.
// KernelProxy.Username / Password / BypassHosts may be empty (passed to the kernel
// as NULL, i.e. unset). An empty KernelProxy.URL is a no-op — the environment-derived
// proxy, if any, stays in effect. A malformed URL is rejected at connect
// (errors.Is ErrInvalidKernelConfig).
//
// An explicit WithKernelProxy takes precedence over the environment: consulting
// both would be ambiguous, and an explicit proxy is a deliberate override.
//
// EXPERIMENTAL, kernel-only: the default (Thrift) backend rejects this at connect.
func WithKernelProxy(p KernelProxy) ConnOption {
	return func(c *config.Config) {
		ke := kernelExperimental(c)
		ke.ProxyURL = p.URL
		ke.ProxyUsername = p.Username
		ke.ProxyPassword = p.Password
		ke.ProxyBypassHosts = p.BypassHosts
	}
}

// WithKernelRetryOverallTimeout sets the cumulative retry budget across all
// attempts on the kernel backend — the total time the kernel may spend retrying a
// single logical request before giving up. This is the 4th retry knob, alongside
// the backoff bounds and max attempts carried by the backend-neutral WithRetries
// (RetryWaitMin / RetryWaitMax / RetryMax, which the kernel path also honors).
//
// It is a kernel-only option because the Thrift-path WithRetries surface has no
// overall-budget equivalent; it mirrors the pyo3/napi retry_overall_timeout knob.
// Zero (the default) keeps the kernel's built-in budget (900s).
//
// EXPERIMENTAL, kernel-only: the default (Thrift) backend rejects this at connect.
func WithKernelRetryOverallTimeout(d time.Duration) ConnOption {
	return func(c *config.Config) {
		kernelExperimental(c).RetryOverallTimeout = d
	}
}

// WithKernelMaxChunksInMemory bounds how many decompressed CloudFetch chunks the
// kernel holds in memory at once on the kernel backend — the knob that trades
// large-result throughput for peak RSS. Lower it (e.g. 4) to cap memory on wide,
// row-heavy result sets; raise it for more download parallelism at higher memory.
// A value <= 0 (the default) leaves the kernel's built-in default (16) in place.
//
// It is forwarded as the kernel's client-only "cloudfetch_max_chunks_in_memory"
// session conf, which the kernel applies to its result config and strips before
// the SEA wire — so it never reaches the server.
//
// EXPERIMENTAL, kernel-only: the default (Thrift) backend has no in-memory-chunk
// knob and rejects this at connect.
func WithKernelMaxChunksInMemory(n int) ConnOption {
	return func(c *config.Config) {
		kernelExperimental(c).MaxChunksInMemory = n
	}
}

// WithTokenCache controls the kernel's on-disk OAuth U2M token-cache persistence.
// When enabled is true, the refresh token is persisted encrypted to
// ~/.config/databricks-sql-kernel/oauth/ so the user is not sent through the browser
// on every connection. When false (the default), tokens are held in memory only.
// U2M-only: PAT and M2M ignore this setting.
//
// EXPERIMENTAL, kernel-only: the default (Thrift) backend rejects this at connect.
// Mirrors the EnableTokenCache option exposed by the ODBC driver, but does not expose
// a passphrase option — pass NULL (empty) to the kernel (derived key).
func WithTokenCache(enabled bool) ConnOption {
	return func(c *config.Config) {
		kernelExperimental(c).TokenCacheEnabled = enabled
	}
}
