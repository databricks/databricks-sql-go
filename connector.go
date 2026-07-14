package dbsql

import (
	"context"
	"crypto/tls"
	"database/sql/driver"
	"errors"
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
		// The experimental WithKernel* TLS options have no Thrift-path equivalent —
		// reject them loudly rather than silently ignore, so a caller who sets a
		// trusted-CA bundle / an independent hostname skip and forgets WithUseKernel
		// learns the option had no effect instead of connecting with a
		// weaker-than-intended (or unconfigured) TLS trust store.
		if c.cfg.KernelExperimental != nil {
			return nil, errors.New("databricks: the WithKernel* options " +
				"(WithKernelTrustedCerts / WithKernelSkipHostnameVerify) require the kernel backend; " +
				"add WithUseKernel(true) or remove them")
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

	// Initialize telemetry: client config overlay decides; if unset, feature flags decide
	conn.telemetry = telemetry.InitializeForConnection(ctx, telemetry.TelemetryInitOptions{
		Host:            c.cfg.Host,
		DriverVersion:   c.cfg.DriverVersion,
		UserAgent:       client.BuildUserAgent(c.cfg),
		HTTPClient:      telemetryClient,
		EnableTelemetry: c.cfg.EnableTelemetry,
		BatchSize:       c.cfg.TelemetryBatchSize,
		FlushInterval:   c.cfg.TelemetryFlushInterval,
	})
	if conn.telemetry != nil {
		log.Debug().Msg("telemetry initialized for connection")
		conn.telemetry.RecordOperation(ctx, conn.id, "", telemetry.OperationTypeCreateSession, sessionLatencyMs, nil)
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

// WithMaxRows sets up the max rows fetched per request. Default is 10000
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

// WithCloudFetch sets up the use of cloud fetch for query execution. Default is false.
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
			c.Authenticator = tokenprovider.NewAuthenticator(federationProvider)
		}
	}
}

// WithFederatedTokenProviderAndClientID sets up SP-wide token federation
func WithFederatedTokenProviderAndClientID(baseProvider tokenprovider.TokenProvider, clientID string) ConnOption {
	return func(c *config.Config) {
		if baseProvider != nil {
			// Wrap with federation provider for SP-wide federation
			federationProvider := tokenprovider.NewFederationProviderWithClientID(baseProvider, c.Host, clientID)
			c.Authenticator = tokenprovider.NewAuthenticator(federationProvider)
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

// WithKernelTrustedCerts adds a PEM CA-certificate bundle to the kernel's TLS
// trust store on top of the system roots — for a corporate re-signing proxy or an
// on-prem CA. Required (rather than relying on SSL_CERT_FILE) because the kernel's
// rustls stack does not read that environment variable.
//
// EXPERIMENTAL, kernel-only: the default (Thrift) backend rejects this at connect.
func WithKernelTrustedCerts(pem []byte) ConnOption {
	return func(c *config.Config) {
		kernelExperimental(c).TLSTrustedCertsPEM = pem
	}
}

// WithKernelSkipHostnameVerify skips only the certificate hostname-vs-SNI check on
// the kernel backend, while keeping chain validation. This is finer-grained than
// WithSkipTLSHostVerify, which relaxes both chain and hostname checks.
//
// EXPERIMENTAL, kernel-only: the default (Thrift) backend rejects this at connect.
func WithKernelSkipHostnameVerify() ConnOption {
	return func(c *config.Config) {
		kernelExperimental(c).TLSSkipHostnameVerify = true
	}
}
