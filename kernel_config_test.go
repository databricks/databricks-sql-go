package dbsql

import (
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/databricks/databricks-sql-go/auth/pat"
	"github.com/databricks/databricks-sql-go/internal/config"
)

// nonPATAuth stands in for any non-PAT authenticator (OAuth / token-provider /
// external / federated) — the kernel backend must reject it.
type nonPATAuth struct{}

func (nonPATAuth) Authenticate(*http.Request) error { return nil }

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

	t.Run("catalog rejected", func(t *testing.T) {
		c := baseKernelConfig()
		c.Catalog = "main"
		if _, err := validateKernelConfig(c); err == nil {
			t.Error("expected an error when a catalog is set")
		}
	})

	t.Run("schema rejected", func(t *testing.T) {
		c := baseKernelConfig()
		c.Schema = "sys"
		if _, err := validateKernelConfig(c); err == nil {
			t.Error("expected an error when a schema is set")
		}
	})

	t.Run("metric view rejected", func(t *testing.T) {
		c := baseKernelConfig()
		c.EnableMetricViewMetadata = true
		if _, err := validateKernelConfig(c); err == nil {
			t.Error("expected an error when metric-view metadata is enabled")
		}
	})

	t.Run("PAT via WithAuthenticator resolves the token", func(t *testing.T) {
		c := baseKernelConfig()
		c.AccessToken = ""
		c.Authenticator = &pat.PATAuth{AccessToken: "dapi-y"}
		tok, err := validateKernelConfig(c)
		if err != nil {
			t.Fatalf("PAT via WithAuthenticator should validate, got %v", err)
		}
		if tok != "dapi-y" {
			t.Errorf("token = %q, want dapi-y (sourced from the authenticator)", tok)
		}
	})

	t.Run("empty token rejected", func(t *testing.T) {
		c := baseKernelConfig()
		c.AccessToken = ""
		c.Authenticator = &pat.PATAuth{AccessToken: ""}
		if _, err := validateKernelConfig(c); err == nil {
			t.Error("expected an error when the resolved PAT is empty")
		}
	})

	t.Run("non-PAT authenticator rejected", func(t *testing.T) {
		c := baseKernelConfig()
		c.Authenticator = nonPATAuth{}
		if _, err := validateKernelConfig(c); err == nil {
			t.Error("expected an error for a non-PAT authenticator")
		}
	})

	t.Run("query timeout rejected", func(t *testing.T) {
		c := baseKernelConfig()
		c.QueryTimeout = 30 * time.Second
		if _, err := validateKernelConfig(c); err == nil {
			t.Error("expected an error when WithTimeout (query timeout) is set")
		}
	})

	t.Run("disabling retries rejected", func(t *testing.T) {
		c := baseKernelConfig()
		c.RetryMax = -1
		if _, err := validateKernelConfig(c); err == nil {
			t.Error("expected an error when retries are disabled (WithRetries(-1))")
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
	"AccessToken":   "forwarded", // as the resolved PAT (kc.Token)
	"Authenticator": "forwarded", // PAT authenticator resolved to the token
	"Location":      "forwarded",
	"SessionParams": "forwarded",
	"Port":          "forwarded", // part of the endpoint URL (host:port)
	"Protocol":      "forwarded", // part of the endpoint URL (scheme)
	"UseKernel":     "forwarded", // the routing flag itself

	// Rejected loudly by validateKernelConfig.
	"Catalog":                  "rejected",
	"Schema":                   "rejected",
	"EnableMetricViewMetadata": "rejected",
	"QueryTimeout":             "rejected", // when > 0 (WithTimeout)
	"RetryMax":                 "rejected", // when < 0 (disable retries)

	// Accepted but intentionally inert on the kernel path (documented in doc.go):
	// the kernel manages these internally, below the C ABI, with no user knob.
	"MaxRows":           "inert",
	"RetryWaitMin":      "inert",
	"RetryWaitMax":      "inert",
	"UseLz4Compression": "inert", // kernel negotiates compression internally

	// Not applicable to the kernel path (Thrift/HTTP-transport or telemetry knobs
	// that don't reach the kernel binding).
	"UserAgentEntry":           "inert", // TODO: forward once the kernel exposes a UA setter
	"Transport":                "inert", // custom RoundTripper; kernel uses its own HTTP stack
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
