package dbsql

import (
	"crypto/tls"
	"errors"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/databricks/databricks-sql-go/auth/oauth"
	"github.com/databricks/databricks-sql-go/auth/pat"
	"github.com/databricks/databricks-sql-go/auth/tokenprovider"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/databricks/databricks-sql-go/internal/backend/kernel"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
)

// nonPATAuth stands in for any non-PAT, non-OAuth authenticator (custom token
// provider / external / static) — the kernel backend must reject it. It implements neither
// auth.M2MCredentialsProvider nor auth.U2MCredentialsProvider.
type nonPATAuth struct{}

func (nonPATAuth) Authenticate(*http.Request) error { return nil }

// fakeM2MAuth / fakeU2MAuth implement the credential-provider interfaces the kernel
// backend asserts on. Used instead of the real m2m/u2m authenticators in unit tests
// because the real u2m.NewAuthenticator does live OIDC discovery at construction
// (needs a resolvable host); the kernel only needs the interface, so a fake is both
// sufficient and hermetic. The real authenticators' method implementations are
// trivial field returns (verified in auth/oauth/{m2m,u2m}).
type fakeM2MAuth struct {
	id, secret string
	scopes     []string
}

func (fakeM2MAuth) Authenticate(*http.Request) error   { return nil }
func (f fakeM2MAuth) M2MCredentials() (string, string) { return f.id, f.secret }
func (f fakeM2MAuth) M2MScopes() []string              { return f.scopes }

type fakeU2MAuth struct{ id string }

func (fakeU2MAuth) Authenticate(*http.Request) error { return nil }
func (f fakeU2MAuth) U2MClientID() string            { return f.id }

func baseKernelConfig() *config.Config {
	c := config.WithDefaults()
	c.Host = "h.databricks.com"
	c.Port = 443
	c.HTTPPath = "/sql/1.0/warehouses/abc"
	c.AccessToken = "dapi-x"
	return c
}

// validateKernelConfig enforces the kernel backend's "nothing silently ignored"
// contract: unsupported options are rejected loudly and the PAT is resolved. This
// is pure Go (no cgo), so these run in the default CGO_ENABLED=0 build.
func TestValidateKernelConfig(t *testing.T) {
	t.Run("supported config ok", func(t *testing.T) {
		c := baseKernelConfig()
		c.SessionParams = map[string]string{"QUERY_TAGS": "a:1"}
		if _, err := validateKernelConfig(c); err != nil {
			t.Errorf("a supported config should validate, got %v", err)
		}
	})

	t.Run("catalog accepted (applied post-connect via USE CATALOG)", func(t *testing.T) {
		c := baseKernelConfig()
		c.Catalog = "main"
		if _, err := validateKernelConfig(c); err != nil {
			t.Errorf("initial catalog is now forwarded (USE CATALOG post-connect), want no error, got %v", err)
		}
	})

	t.Run("schema accepted (applied post-connect via USE SCHEMA)", func(t *testing.T) {
		c := baseKernelConfig()
		c.Schema = "sys"
		if _, err := validateKernelConfig(c); err != nil {
			t.Errorf("initial schema is now forwarded (USE SCHEMA post-connect), want no error, got %v", err)
		}
	})

	t.Run("metric view accepted (folded into session conf)", func(t *testing.T) {
		c := baseKernelConfig()
		c.EnableMetricViewMetadata = true
		if _, err := validateKernelConfig(c); err != nil {
			t.Errorf("metric-view metadata is now forwarded backend-neutrally, want no error, got %v", err)
		}
	})

	t.Run("PAT resolves to a PAT auth descriptor", func(t *testing.T) {
		c := baseKernelConfig() // AccessToken = "dapi-x"
		a, err := validateKernelConfig(c)
		if err != nil {
			t.Fatalf("PAT should validate, got %v", err)
		}
		if a.Mode != kernel.AuthPAT || a.Token != "dapi-x" {
			t.Errorf("auth = %+v, want mode=PAT token=dapi-x", a)
		}
	})

	// The still-unsupported options must (a) error and (b) wrap ErrNotSupportedByKernel,
	// since that sentinel is the documented programmatic fallback-detection contract —
	// asserting only err != nil would let a dropped or malformed %w wrap ship green.
	// Table-driven so a new rejection is covered by adding one row. (Catalog/schema/
	// metric-view moved to forwarded above; a non-PAT authenticator is also
	// sentinel-wrapped but needs its own AccessToken="" setup, so it's asserted separately below.)
	rejections := []struct {
		name string
		mut  func(*config.Config)
	}{
		{"query timeout", func(c *config.Config) { c.QueryTimeout = 30 * time.Second }},
		{"non-https protocol", func(c *config.Config) { c.Protocol = "http" }},
		{"non-default port", func(c *config.Config) { c.Port = 8443 }},
		{"custom transport", func(c *config.Config) { c.Transport = http.DefaultTransport }},
	}
	for _, tc := range rejections {
		t.Run(tc.name+" rejected", func(t *testing.T) {
			c := baseKernelConfig()
			tc.mut(c)
			_, err := validateKernelConfig(c)
			if err == nil {
				t.Fatalf("expected an error when %s is set", tc.name)
			}
			if !errors.Is(err, dbsqlerr.ErrNotSupportedByKernel) {
				t.Errorf("%s rejection should wrap ErrNotSupportedByKernel, got %v", tc.name, err)
			}
		})
	}

	t.Run("PAT via WithAuthenticator resolves the token", func(t *testing.T) {
		c := baseKernelConfig()
		c.AccessToken = ""
		c.Authenticator = &pat.PATAuth{AccessToken: "dapi-y"}
		a, err := validateKernelConfig(c)
		if err != nil {
			t.Fatalf("PAT via WithAuthenticator should validate, got %v", err)
		}
		if a.Mode != kernel.AuthPAT || a.Token != "dapi-y" {
			t.Errorf("auth = %+v, want mode=PAT token=dapi-y (sourced from the authenticator)", a)
		}
	})

	t.Run("OAuth M2M resolves to an M2M descriptor", func(t *testing.T) {
		c := baseKernelConfig()
		c.AccessToken = ""
		// An M2M authenticator is the single source of truth; resolveKernelAuth reads
		// the creds off it via the auth.M2MCredentialsProvider interface. Default
		// scopes ({"all-apis"}, matching the kernel default) forward fine.
		c.Authenticator = fakeM2MAuth{id: "cid", secret: "sec", scopes: []string{"all-apis"}}
		a, err := validateKernelConfig(c)
		if err != nil {
			t.Fatalf("M2M should validate, got %v", err)
		}
		if a.Mode != kernel.AuthM2M || a.ClientID != "cid" || a.ClientSecret != "sec" {
			t.Errorf("auth = %+v, want mode=M2M clientID=cid clientSecret=sec", a)
		}
	})

	t.Run("OAuth M2M with custom scopes rejected", func(t *testing.T) {
		c := baseKernelConfig()
		c.AccessToken = ""
		// The kernel's set_auth_m2m can't carry scopes, so a custom set must be
		// rejected (not silently downgraded to the kernel default) and wrap
		// ErrNotSupportedByKernel like every other unsupported option.
		c.Authenticator = fakeM2MAuth{id: "cid", secret: "sec", scopes: []string{"all-apis", "custom-scope"}}
		_, err := validateKernelConfig(c)
		if !errors.Is(err, dbsqlerr.ErrNotSupportedByKernel) {
			t.Errorf("custom-scope M2M rejection should wrap ErrNotSupportedByKernel, got %v", err)
		}
	})

	t.Run("OAuth U2M resolves to a U2M descriptor", func(t *testing.T) {
		c := baseKernelConfig()
		c.AccessToken = ""
		// A U2M authenticator is the single source of truth; resolveKernelAuth reads
		// its (cloud-inferred) client id via the auth.U2MCredentialsProvider interface.
		c.Authenticator = fakeU2MAuth{id: "databricks-sql-connector"}
		a, err := validateKernelConfig(c)
		if err != nil {
			t.Fatalf("U2M should validate, got %v", err)
		}
		if a.Mode != kernel.AuthU2M || a.ClientID != "databricks-sql-connector" {
			t.Errorf("auth = %+v, want mode=U2M clientID=databricks-sql-connector", a)
		}
		// Scopes must match what the Thrift path requests for this host, so both
		// backends authorize against the same client identically (not the kernel's
		// all-apis default). baseKernelConfig's host is AWS → [offline_access, sql].
		if want := oauth.GetScopes(c.Host, nil); !reflect.DeepEqual(a.Scopes, want) {
			t.Errorf("U2M scopes = %v, want %v (Thrift parity)", a.Scopes, want)
		}
	})

	t.Run("federated provider supplies PAT auth", func(t *testing.T) {
		cases := []struct {
			name     string
			option   func(tokenprovider.TokenProvider) ConnOption
			clientID string
		}{
			{"account-wide", WithFederatedTokenProvider, ""},
			{"SP-wide", func(p tokenprovider.TokenProvider) ConnOption {
				return WithFederatedTokenProviderAndClientID(p, "federation-client")
			}, "federation-client"},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				c := baseKernelConfig()
				c.AccessToken = ""
				calls := 0
				tc.option(tokenprovider.NewExternalTokenProvider(func() (string, error) {
					calls++
					return "subject-token", nil
				}))(c)
				a, err := validateKernelConfig(c)
				if err != nil {
					t.Fatalf("federated provider should validate, got %v", err)
				}
				if a.Mode != kernel.AuthPAT || a.Token != "subject-token" || a.ClientID != tc.clientID {
					t.Errorf("auth = %+v, want PAT token=subject-token clientID=%q", a, tc.clientID)
				}
				if mech, flow := kernelAuthMech(c); mech != "PAT" || flow != "" {
					t.Errorf("kernelAuthMech = (%q, %q), want (PAT, empty)", mech, flow)
				}
				if calls != 1 {
					t.Errorf("connection-config telemetry classification resolved the provider: calls = %d, want 1", calls)
				}
			})
		}
	})

	t.Run("last-applied auth wins: M2M then PAT resolves to PAT", func(t *testing.T) {
		// Regression for the auth-mode divergence: cfg.Authenticator is the single
		// source of truth, so setting an M2M authenticator and then a PAT (a later
		// WithAccessToken) must resolve to PAT on the kernel path — matching Thrift's
		// last-writer-wins on cfg.Authenticator. (Previously a parallel OAuth carrier
		// field could keep the kernel on M2M while Thrift used PAT.)
		c := baseKernelConfig()
		c.Authenticator = fakeM2MAuth{id: "cid", secret: "sec"} // earlier
		c.Authenticator = &pat.PATAuth{AccessToken: "dapi-z"}   // later wins
		c.AccessToken = "dapi-z"
		a, err := validateKernelConfig(c)
		if err != nil {
			t.Fatalf("PAT (last applied) should validate, got %v", err)
		}
		if a.Mode != kernel.AuthPAT || a.Token != "dapi-z" {
			t.Errorf("auth = %+v, want mode=PAT token=dapi-z (last-applied wins)", a)
		}
	})

	t.Run("empty token rejected", func(t *testing.T) {
		// Missing-required-config, NOT an unsupported-feature rejection, so this is
		// intentionally NOT expected to wrap ErrNotSupportedByKernel.
		c := baseKernelConfig()
		c.AccessToken = ""
		c.Authenticator = &pat.PATAuth{AccessToken: ""}
		if _, err := validateKernelConfig(c); err == nil {
			t.Error("expected an error when the resolved PAT is empty")
		}
	})

	t.Run("non-PAT/non-OAuth authenticator rejected", func(t *testing.T) {
		c := baseKernelConfig()
		c.AccessToken = ""
		c.Authenticator = nonPATAuth{}
		_, err := validateKernelConfig(c)
		if err == nil {
			t.Fatal("expected an error for a custom token-provider/external/static authenticator")
		}
		// An unsupported authenticator is a "kernel can't honor this" rejection, so it
		// must wrap ErrNotSupportedByKernel like every other unsupported option — the
		// contract doc.go advertises for programmatic fallback via errors.Is.
		if !errors.Is(err, dbsqlerr.ErrNotSupportedByKernel) {
			t.Errorf("unsupported-auth rejection should wrap ErrNotSupportedByKernel, got %v", err)
		}
	})

	t.Run("positive retry tuning + maxrows accepted", func(t *testing.T) {
		c := baseKernelConfig()
		c.RetryMax = 8
		c.MaxRows = 5000
		if _, err := validateKernelConfig(c); err != nil {
			t.Errorf("positive retry/maxrows tuning should validate, got %v", err)
		}
	})

	t.Run("disable retries (WithRetries(-1)) accepted", func(t *testing.T) {
		// The disable form (RetryMax < 0) maps to zero kernel retries via the
		// retry-config setter, so it validates rather than erroring.
		c := baseKernelConfig()
		c.RetryMax = -1
		if _, err := validateKernelConfig(c); err != nil {
			t.Errorf("WithRetries(-1) (disable) should validate, got %v", err)
		}
	})

	t.Run("default https/443 accepted", func(t *testing.T) {
		c := baseKernelConfig() // WithDefaults sets Protocol=https, Port=443
		if _, err := validateKernelConfig(c); err != nil {
			t.Errorf("the default https/443 endpoint should validate, got %v", err)
		}
	})

	t.Run("valid WithKernelProxy URL accepted", func(t *testing.T) {
		c := baseKernelConfig()
		WithKernelProxy(KernelProxy{URL: "http://proxy.internal:3128", Username: "u", Password: "p", BypassHosts: "*.internal"})(c)
		if _, err := validateKernelConfig(c); err != nil {
			t.Errorf("a well-formed proxy URL should validate, got %v", err)
		}
	})

	t.Run("malformed WithKernelProxy URL rejected as ErrInvalidKernelConfig", func(t *testing.T) {
		// A malformed URL must be caught in the Go layer with an errors.Is-able
		// config error, not surface as an opaque "kernel: set_proxy: …" wrap at
		// connect (or, worse, a URL missing a scheme/host that the C ABI can't use).
		for _, tc := range []struct {
			name, proxyURL string
		}{
			{"control chars", "http://a\x7f:3128"}, // url.Parse returns an error
			{"no scheme or host", "proxy:3128"},    // parses, but unusable shape
			{"scheme only", "http://"},             // parses, empty host
		} {
			t.Run(tc.name, func(t *testing.T) {
				c := baseKernelConfig()
				WithKernelProxy(KernelProxy{URL: tc.proxyURL})(c)
				_, err := validateKernelConfig(c)
				if !errors.Is(err, dbsqlerr.ErrInvalidKernelConfig) {
					t.Errorf("proxy URL %q should be rejected as ErrInvalidKernelConfig, got %v", tc.proxyURL, err)
				}
			})
		}
	})
}

// kernelConfigFieldDisposition records, for every UserConfig field, how the kernel
// backend treats it. UserConfig is the user-facing option surface ("Only
// UserConfig are currently exposed to users"). Adding a field to config.UserConfig
// without classifying it here fails TestKernelConfigFieldsClassified — forcing a
// deliberate decision (forward it, reject it loudly, or accept it as inert) rather
// than silently dropping it on the kernel path. (TLSConfig and ArrowConfig live on
// the outer config.Config, not
// UserConfig; newKernelBackend reads TLSConfig.InsecureSkipVerify explicitly, and
// the kernel renders decimals exactly regardless of ArrowConfig.)
var kernelConfigFieldDisposition = map[string]string{
	// Forwarded to kernel.Config (see newKernelBackend).
	"Host":          "forwarded",
	"HTTPPath":      "forwarded",
	"WarehouseID":   "forwarded",
	"AccessToken":   "forwarded", // as the resolved PAT (kc.Auth.Token)
	"Authenticator": "forwarded", // resolved to the auth descriptor (PAT/M2M/U2M)
	"Location":      "forwarded",
	"SessionParams": "forwarded",
	"UseKernel":     "forwarded", // the routing flag itself
	// Folded into SessionConf by config.EffectiveSessionParams (metric-view conf),
	// sent identically on both backends.
	"EnableMetricViewMetadata": "forwarded",
	// Applied post-connect via USE CATALOG / USE SCHEMA (no kernel config setter).
	"Catalog": "forwarded",
	"Schema":  "forwarded",

	// Rejected loudly by validateKernelConfig.
	"QueryTimeout": "rejected", // when > 0 (WithTimeout)
	"Protocol":     "rejected", // kernel is https-only; non-default rejected
	"Port":         "rejected", // kernel connects on 443; non-default rejected
	"Transport":    "rejected", // custom RoundTripper; kernel uses its own HTTP stack, so reject rather than drop

	// Forwarded to the kernel's HTTP retry config via kernelRetryConfig →
	// set_retry_config (WithRetries: backoff bounds + max attempts, incl. disable).
	"RetryMax":     "forwarded",
	"RetryWaitMin": "forwarded",
	"RetryWaitMax": "forwarded",

	// Accepted but intentionally inert on the kernel path (documented in doc.go):
	// the kernel manages these internally, below the C ABI, with no user knob.
	"MaxRows":           "inert",
	"UseLz4Compression": "inert", // kernel negotiates compression internally

	// Rides in the forwarded User-Agent header (set_custom_header).
	"UserAgentEntry": "forwarded",

	// Not applicable to the kernel path (Thrift/HTTP-transport or telemetry knobs
	// that don't reach the kernel binding).
	"EnableTelemetry":          "inert",
	"TelemetryBatchSize":       "inert",
	"TelemetryFlushInterval":   "inert",
	"UseArrowNativeDecimalDSN": "inert", // DSN carrier; kernel renders decimals exactly regardless

	// Fields promoted from the embedded CloudFetchConfig. The kernel does
	// CloudFetch internally (below the C ABI), so none is forwarded — but each is
	// classified individually so a new CloudFetch option can't slip the guard.
	"UseCloudFetch":                "inert",
	"MaxDownloadThreads":           "inert",
	"MaxFilesInMemory":             "inert",
	"MinTimeToExpiry":              "inert",
	"CloudFetchSpeedThresholdMbps": "inert",
	"HTTPClient":                   "inert",
}

// kernelConfigClassifiedNames returns every UserConfig field name the kernel gate
// must account for, flattening anonymous embedded structs (e.g. CloudFetchConfig)
// into their promoted fields — reflect's NumField reports an embed as one field,
// which would hide new sub-fields from the drop guard below.
func kernelConfigClassifiedNames(t reflect.Type) []string {
	var names []string
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.Anonymous && f.Type.Kind() == reflect.Struct {
			names = append(names, kernelConfigClassifiedNames(f.Type)...)
			continue
		}
		names = append(names, f.Name)
	}
	return names
}

func TestKernelConfigFieldsClassified(t *testing.T) {
	names := kernelConfigClassifiedNames(reflect.TypeOf(config.UserConfig{}))
	classified := make(map[string]bool, len(names))
	for _, name := range names {
		classified[name] = true
		if _, ok := kernelConfigFieldDisposition[name]; !ok {
			t.Errorf("config.UserConfig field %q (incl. promoted embed fields) is not classified "+
				"for the kernel backend. Add it to kernelConfigFieldDisposition as "+
				"forwarded/rejected/inert (and wire it in validateKernelConfig / newKernelBackend if "+
				"it must be honored) so it isn't silently dropped on the kernel path.", name)
		}
	}
	// Guard the reverse too: a disposition entry for a field that no longer exists
	// is stale.
	for name := range kernelConfigFieldDisposition {
		if !classified[name] {
			t.Errorf("kernelConfigFieldDisposition has %q but config.UserConfig (incl. embeds) no longer does; remove it", name)
		}
	}
}

// buildKernelConfig is the pure config-assembly half of newKernelBackend. The
// experimental KernelExperimental TLS knobs (WithKernelTrustedCerts /
// WithKernelSkipHostnameVerify) are forwarded into kernel.Config ONLY here, and a
// dropped forwarding line would otherwise pass every other test (the reflective
// TestKernelExperimentalFieldsClassified only asserts the disposition map, not the
// runtime copy). These run in the default CGO_ENABLED=0 build.
func TestBuildKernelConfig(t *testing.T) {
	t.Run("experimental TLS fields forwarded", func(t *testing.T) {
		c := baseKernelConfig()
		c.KernelExperimental = &config.KernelExperimentalConfig{
			TLSTrustedCertsPEM:    []byte("ca-bundle"),
			TLSSkipHostnameVerify: true,
		}
		kc := buildKernelConfig(c, kernel.Auth{Mode: kernel.AuthPAT, Token: "dapi-x"})
		if got := string(kc.TLSTrustedCertsPEM); got != "ca-bundle" {
			t.Errorf("TLSTrustedCertsPEM = %q, want %q (WithKernelTrustedCerts not forwarded)", got, "ca-bundle")
		}
		if !kc.TLSSkipHostnameVerify {
			t.Error("TLSSkipHostnameVerify = false, want true (WithKernelSkipHostnameVerify not forwarded)")
		}
	})

	t.Run("nil KernelExperimental leaves TLS fields zero", func(t *testing.T) {
		c := baseKernelConfig() // KernelExperimental nil
		kc := buildKernelConfig(c, kernel.Auth{Mode: kernel.AuthPAT, Token: "dapi-x"})
		if kc.TLSTrustedCertsPEM != nil || kc.TLSSkipHostnameVerify {
			t.Errorf("expected zero experimental TLS fields with nil KernelExperimental, got certs=%v skipHost=%v",
				kc.TLSTrustedCertsPEM, kc.TLSSkipHostnameVerify)
		}
	})

	t.Run("InsecureSkipVerify maps to blanket TLSSkipVerify", func(t *testing.T) {
		c := baseKernelConfig()
		c.TLSConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // test asserts the mapping, not a real connection
		kc := buildKernelConfig(c, kernel.Auth{Mode: kernel.AuthPAT, Token: "dapi-x"})
		if !kc.TLSSkipVerify {
			t.Error("TLSSkipVerify = false, want true (TLSConfig.InsecureSkipVerify not mapped)")
		}
	})

	t.Run("core fields + auth forwarded", func(t *testing.T) {
		c := baseKernelConfig()
		c.Catalog = "main"
		c.Schema = "sys"
		kauth := kernel.Auth{Mode: kernel.AuthPAT, Token: "dapi-x"}
		kc := buildKernelConfig(c, kauth)
		if kc.Host != c.Host || kc.HTTPPath != c.HTTPPath {
			t.Errorf("host/httpPath not forwarded: got host=%q httpPath=%q", kc.Host, kc.HTTPPath)
		}
		if kc.Catalog != "main" || kc.Schema != "sys" {
			t.Errorf("catalog/schema not forwarded: got catalog=%q schema=%q", kc.Catalog, kc.Schema)
		}
		if kc.Auth.Mode != kauth.Mode || kc.Auth.Token != kauth.Token {
			t.Errorf("auth not forwarded: got %+v, want %+v", kc.Auth, kauth)
		}
		// UserAgent must be the driver's composed UA, non-empty — else query
		// history mis-attributes SEA-path queries to the kernel's built-in UA.
		if want := client.BuildUserAgent(c); kc.UserAgent == "" || kc.UserAgent != want {
			t.Errorf("UserAgent not forwarded: got %q, want %q", kc.UserAgent, want)
		}
	})

	t.Run("MaxChunksInMemory injected into kernel SessionConf", func(t *testing.T) {
		c := baseKernelConfig()
		c.KernelExperimental = &config.KernelExperimentalConfig{MaxChunksInMemory: 4}
		kc := buildKernelConfig(c, kernel.Auth{Mode: kernel.AuthPAT, Token: "dapi-x"})
		if got := kc.SessionConf[config.KernelMaxChunksInMemoryConfKey]; got != "4" {
			t.Errorf("SessionConf[%q] = %q, want %q", config.KernelMaxChunksInMemoryConfKey, got, "4")
		}
	})

	t.Run("MaxChunksInMemory unset leaves the key absent (kernel default)", func(t *testing.T) {
		c := baseKernelConfig() // KernelExperimental nil
		kc := buildKernelConfig(c, kernel.Auth{Mode: kernel.AuthPAT, Token: "dapi-x"})
		if _, ok := kc.SessionConf[config.KernelMaxChunksInMemoryConfKey]; ok {
			t.Errorf("SessionConf should not carry %q when unset (kernel keeps its default)", config.KernelMaxChunksInMemoryConfKey)
		}
		// A zero/negative explicit value is also a no-op.
		c.KernelExperimental = &config.KernelExperimentalConfig{MaxChunksInMemory: 0}
		kc = buildKernelConfig(c, kernel.Auth{Mode: kernel.AuthPAT, Token: "dapi-x"})
		if _, ok := kc.SessionConf[config.KernelMaxChunksInMemoryConfKey]; ok {
			t.Errorf("SessionConf should not carry %q for a zero value", config.KernelMaxChunksInMemoryConfKey)
		}
	})

	t.Run("MaxChunksInMemory is not leaked to the shared server params", func(t *testing.T) {
		// The knob is kernel-only: it must NOT appear in EffectiveSessionParams
		// (which the Thrift path also sends to the server).
		c := baseKernelConfig()
		c.KernelExperimental = &config.KernelExperimentalConfig{MaxChunksInMemory: 4}
		if _, ok := c.EffectiveSessionParams()[config.KernelMaxChunksInMemoryConfKey]; ok {
			t.Errorf("%q must not be in EffectiveSessionParams (kernel-only, not a server param)", config.KernelMaxChunksInMemoryConfKey)
		}
	})

	t.Run("MaxChunksInMemory conf key matches the kernel's cross-repo contract", func(t *testing.T) {
		// Unlike the sibling retry knob (a typed C setter whose signature drift is a
		// link/compile error), this knob rides a stringly-typed session-conf key whose
		// only consumer is the kernel's apply_client_result_overrides
		// (CLIENT_CONF_CLOUDFETCH_MAX_CHUNKS in databricks-sql-kernel src/session.rs).
		// There is no build-time coupling, so pin the exact literal here: an accidental
		// edit to the Go constant fails in CGO_ENABLED=0 PR CI (not just the
		// warehouse-gated nightly), and this test is the greppable anchor a kernel-side
		// rename must update in lockstep.
		const wantKey = "cloudfetch_max_chunks_in_memory"
		if config.KernelMaxChunksInMemoryConfKey != wantKey {
			t.Errorf("KernelMaxChunksInMemoryConfKey = %q, want %q — the kernel's "+
				"apply_client_result_overrides reads this exact key; a mismatch silently "+
				"no-ops the knob or leaks it to the server",
				config.KernelMaxChunksInMemoryConfKey, wantKey)
		}
	})
}

// TestKernelRetryConfig covers the pure resolution of the driver's WithRetries
// policy into the kernel retry descriptor: the defaults forward the connector's
// positive backoff bounds + max attempts; the disable form maps to zero retries;
// WithRetries(n, 0, 0) still forwards the caller's attempt count (with placeholder
// waits) rather than silently dropping to the kernel default; a fully zero-value
// range (a Config without WithDefaults and no attempt count) returns nil so a stray
// zero can't fail the connect; and the kernel-only overall-timeout knob is read from
// KernelExperimental. Runs in the default CGO_ENABLED=0 build.
func TestKernelRetryConfig(t *testing.T) {
	t.Run("defaults forward backoff + max attempts, no overall budget", func(t *testing.T) {
		c := baseKernelConfig() // WithDefaults: RetryMax=4, RetryWaitMin=1s, RetryWaitMax=30s
		r := kernelRetryConfig(c)
		if r == nil {
			t.Fatal("kernelRetryConfig returned nil for the default (positive) range")
		}
		if r.MinWait != time.Second || r.MaxWait != 30*time.Second || r.MaxRetries != 4 {
			t.Errorf("resolved retry = %+v, want {1s, 30s, 4}", r)
		}
		if r.OverallTimeout != 0 {
			t.Errorf("OverallTimeout = %v, want 0 (keep kernel default) when unset", r.OverallTimeout)
		}
	})

	t.Run("disable form maps to zero retries", func(t *testing.T) {
		c := baseKernelConfig()
		c.RetryMax = -1 // WithRetries(-1) disable
		r := kernelRetryConfig(c)
		if r == nil {
			t.Fatal("disable form should still forward a config (0 retries), got nil")
		}
		if r.MaxRetries != 0 {
			t.Errorf("MaxRetries = %d, want 0 (disable maps to zero kernel retries)", r.MaxRetries)
		}
	})

	t.Run("idiomatic disable WithRetries(-1,0,0) honored despite zero waits", func(t *testing.T) {
		// The idiomatic disable zeroes the waits too. The resolver must still honor
		// the disable (MaxRetries=0) and substitute a valid placeholder range so the
		// kernel setter accepts it (it rejects min==0), rather than returning nil
		// (which would leave the kernel's DEFAULT retry policy in place — not disabled).
		c := baseKernelConfig()
		c.RetryMax = -1
		c.RetryWaitMin = 0
		c.RetryWaitMax = 0
		r := kernelRetryConfig(c)
		if r == nil {
			t.Fatal("WithRetries(-1,0,0) must forward a disable config, got nil (kernel default would apply — not disabled)")
		}
		if r.MaxRetries != 0 {
			t.Errorf("MaxRetries = %d, want 0", r.MaxRetries)
		}
		if r.MinWait <= 0 || r.MaxWait < r.MinWait {
			t.Errorf("placeholder waits = {%v, %v}, want a valid range the kernel setter accepts", r.MinWait, r.MaxWait)
		}
	})

	t.Run("WithRetries(n,0,0) honors the attempt count despite zero waits", func(t *testing.T) {
		// The regression this guards: WithDefaults() runs before options, so
		// WithRetries(10, 0, 0) — valid per its godoc, which promises sane wait
		// defaults — overwrites the waits to zero. The resolver must still forward the
		// caller's RetryMax (with placeholder waits), not return nil (which would drop
		// to the kernel's default policy and silently ignore the requested attempts).
		c := baseKernelConfig()
		WithRetries(10, 0, 0)(c)
		r := kernelRetryConfig(c)
		if r == nil {
			t.Fatal("WithRetries(10,0,0) must forward the caller's RetryMax, got nil (kernel default would apply)")
		}
		if r.MaxRetries != 10 {
			t.Errorf("MaxRetries = %d, want 10 (caller's attempt count honored)", r.MaxRetries)
		}
		if r.MinWait <= 0 || r.MaxWait < r.MinWait {
			t.Errorf("placeholder waits = {%v, %v}, want a valid range the kernel setter accepts", r.MinWait, r.MaxWait)
		}
	})

	t.Run("overall timeout forwarded from KernelExperimental", func(t *testing.T) {
		c := baseKernelConfig()
		WithKernelRetryOverallTimeout(5 * time.Minute)(c)
		r := kernelRetryConfig(c)
		if r == nil || r.OverallTimeout != 5*time.Minute {
			t.Errorf("OverallTimeout not forwarded: %+v", r)
		}
	})

	t.Run("zero-value range with no attempt count returns nil (keep kernel default)", func(t *testing.T) {
		// A Config assembled without WithDefaults and no RetryMax: zero waits are a
		// nonsense range the kernel setter would reject and there is no caller attempt
		// count to preserve, so resolve to nil rather than fail connect.
		c := &config.Config{UserConfig: config.UserConfig{RetryWaitMin: 0, RetryWaitMax: 0}}
		if r := kernelRetryConfig(c); r != nil {
			t.Errorf("zero-value range with RetryMax 0 should return nil, got %+v", r)
		}
	})

	t.Run("overall budget forwarded even when retries are zeroed", func(t *testing.T) {
		// The regression this guards: WithRetries(0,0,0) leaves RetryMax==0 and a
		// degenerate wait range, but an explicit WithKernelRetryOverallTimeout must
		// still reach the kernel. Returning nil here (the old behavior) discarded the
		// caller's overall budget and left the kernel's 900s default — the exact
		// "silently ignored option" failure the kernel gate was built to prevent.
		c := baseKernelConfig()
		WithRetries(0, 0, 0)(c)
		WithKernelRetryOverallTimeout(5 * time.Minute)(c)
		r := kernelRetryConfig(c)
		if r == nil {
			t.Fatal("overall budget with zeroed retries must forward a config, got nil (5m budget dropped, kernel default would apply)")
		}
		if r.OverallTimeout != 5*time.Minute {
			t.Errorf("OverallTimeout = %v, want 5m (forwarded despite zero retries)", r.OverallTimeout)
		}
		if r.MaxRetries != 0 {
			t.Errorf("MaxRetries = %d, want 0", r.MaxRetries)
		}
		if r.MinWait <= 0 || r.MaxWait < r.MinWait {
			t.Errorf("placeholder waits = {%v, %v}, want a valid range the kernel setter accepts", r.MinWait, r.MaxWait)
		}
	})

	t.Run("sub-millisecond waits are clamped up to a 1ms floor", func(t *testing.T) {
		// The regression this guards: kernelRetryConfig validates waits in Duration
		// space, but applyRetry forwards them via time.Duration.Milliseconds(). A valid
		// wait in (0, 1ms) passes the guard yet truncates to min_wait_ms == 0, which the
		// kernel setter rejects (InvalidArgument) — a connect failure the Thrift path,
		// which accepts any Duration, does not have. The resolver must clamp up so the
		// forwarded millisecond value stays > 0.
		c := baseKernelConfig()
		WithRetries(5, 999*time.Microsecond, 30*time.Second)(c)
		r := kernelRetryConfig(c)
		if r == nil {
			t.Fatal("sub-ms MinWait should still resolve a config, got nil")
		}
		if r.MinWait.Milliseconds() < 1 {
			t.Errorf("MinWait = %v (%d ms), want clamped to >= 1ms so the kernel setter accepts it", r.MinWait, r.MinWait.Milliseconds())
		}
		if r.MaxWait < r.MinWait {
			t.Errorf("MaxWait = %v < MinWait = %v after clamp", r.MaxWait, r.MinWait)
		}
		if r.MaxRetries != 5 {
			t.Errorf("MaxRetries = %d, want 5 (attempt count preserved)", r.MaxRetries)
		}
	})
}
