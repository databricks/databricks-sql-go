package dbsql

import (
	"errors"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/databricks/databricks-sql-go/auth/pat"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/databricks/databricks-sql-go/internal/backend/kernel"
	"github.com/databricks/databricks-sql-go/internal/config"
)

// nonPATAuth stands in for any non-PAT, non-OAuth authenticator (token-provider /
// external / federated) — the kernel backend must reject it. It implements neither
// auth.M2MCredentialsProvider nor auth.U2MCredentialsProvider.
type nonPATAuth struct{}

func (nonPATAuth) Authenticate(*http.Request) error { return nil }

// fakeM2MAuth / fakeU2MAuth implement the credential-provider interfaces the kernel
// backend asserts on. Used instead of the real m2m/u2m authenticators in unit tests
// because the real u2m.NewAuthenticator does live OIDC discovery at construction
// (needs a resolvable host); the kernel only needs the interface, so a fake is both
// sufficient and hermetic. The real authenticators' method implementations are
// trivial field returns (verified in auth/oauth/{m2m,u2m}).
type fakeM2MAuth struct{ id, secret string }

func (fakeM2MAuth) Authenticate(*http.Request) error   { return nil }
func (f fakeM2MAuth) M2MCredentials() (string, string) { return f.id, f.secret }

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
	// metric-view moved to forwarded above; a non-PAT authenticator is rejected too but
	// not sentinel-wrapped, so it's asserted separately below.)
	rejections := []struct {
		name string
		mut  func(*config.Config)
	}{
		{"query timeout", func(c *config.Config) { c.QueryTimeout = 30 * time.Second }},
		{"disable retries", func(c *config.Config) { c.RetryMax = -1 }},
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
		// the creds off it via the auth.M2MCredentialsProvider interface.
		c.Authenticator = fakeM2MAuth{id: "cid", secret: "sec"}
		a, err := validateKernelConfig(c)
		if err != nil {
			t.Fatalf("M2M should validate, got %v", err)
		}
		if a.Mode != kernel.AuthM2M || a.ClientID != "cid" || a.ClientSecret != "sec" {
			t.Errorf("auth = %+v, want mode=M2M clientID=cid clientSecret=sec", a)
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
			t.Fatal("expected an error for a token-provider/external/federated authenticator")
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

	t.Run("default https/443 accepted", func(t *testing.T) {
		c := baseKernelConfig() // WithDefaults sets Protocol=https, Port=443
		if _, err := validateKernelConfig(c); err != nil {
			t.Errorf("the default https/443 endpoint should validate, got %v", err)
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
	"RetryMax":     "rejected", // when < 0 (disable retries)
	"Protocol":     "rejected", // kernel is https-only; non-default rejected
	"Port":         "rejected", // kernel connects on 443; non-default rejected
	"Transport":    "rejected", // custom RoundTripper; kernel uses its own HTTP stack, so reject rather than drop

	// Accepted but intentionally inert on the kernel path (documented in doc.go):
	// the kernel manages these internally, below the C ABI, with no user knob.
	"MaxRows":           "inert",
	"RetryWaitMin":      "inert",
	"RetryWaitMax":      "inert",
	"UseLz4Compression": "inert", // kernel negotiates compression internally

	// Not applicable to the kernel path (Thrift/HTTP-transport or telemetry knobs
	// that don't reach the kernel binding).
	"UserAgentEntry":           "inert", // TODO: forward once the kernel exposes a UA setter
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
