package oauth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestInferCloudFromHost(t *testing.T) {
	cases := []struct {
		host string
		want CloudType
	}{
		// Standard per-workspace hosts.
		{"dbc-1234.cloud.databricks.com", AWS},
		{"example.cloud.databricks.us", AWS},
		{"foo.dev.databricks.com", AWS},
		{"adb-123.azuredatabricks.net", Azure},
		{"x.databricks.azure.us", Azure},
		{"y.databricks.azure.cn", Azure},
		{"ws.gcp.databricks.com", GCP},
		// SPOG / unified custom-URL AWS hosts (the fix): must classify as AWS,
		// not Unknown, and must NOT be swallowed by the GCP/Azure checks.
		{"pecoaws.databricks.com", AWS},
		{"dogfood.staging.databricks.com", AWS},
		// Azure SPOG stays Azure.
		{"dogfood-spog.staging.azuredatabricks.net", Azure},
		// GCP custom host must remain GCP even though it contains "databricks.com".
		{"foo.gcp.databricks.com", GCP},
		// Truly unrelated host stays Unknown.
		{"example.com", Unknown},
	}

	for _, tc := range cases {
		t.Run(tc.host, func(t *testing.T) {
			if got := InferCloudFromHost(tc.host); got != tc.want {
				t.Fatalf("InferCloudFromHost(%q) = %v, want %v", tc.host, got, tc.want)
			}
		})
	}
}

func TestGetAzureDnsZone(t *testing.T) {
	// Documents current behavior: the generic suffix is matched first, so staging
	// and dev Azure hosts resolve to the prod tenant. (Separate fix tracked.)
	cases := []struct {
		host string
		want string
	}{
		{"adb-123.azuredatabricks.net", ".azuredatabricks.net"},
		{"x.databricks.azure.us", ".databricks.azure.us"},
		{"nope.example.com", ""},
	}
	for _, tc := range cases {
		t.Run(tc.host, func(t *testing.T) {
			if got := GetAzureDnsZone(tc.host); got != tc.want {
				t.Fatalf("GetAzureDnsZone(%q) = %q, want %q", tc.host, got, tc.want)
			}
		})
	}
}

// TestResolveOIDCIssuer drives resolveOIDCIssuer end-to-end against an httptest
// server: the server stands in for the connection host, so a 200 with a given
// databricks-config body exercises the real fetch + substitution + validation +
// fallback wiring.
func TestResolveOIDCIssuer(t *testing.T) {
	const fallbackForUnreachable = "https://no-such-host.invalid/oidc"

	cases := []struct {
		name string
		// body served at /.well-known/databricks-config (status 200). Empty body
		// with status!=200 simulates a missing endpoint.
		status int
		body   string
		// want is the exact resolved issuer. If wantFallback is true we instead
		// assert the bare-host fallback (https://<server-host>/oidc).
		want         string
		wantFallback bool
	}{
		{
			name:   "unified host substitutes account_id",
			status: 200,
			body:   `{"oidc_endpoint":"https://spog.databricks.com/oidc/accounts/{account_id}","account_id":"acc-123","host_type":"unified"}`,
			want:   "https://spog.databricks.com/oidc/accounts/acc-123",
		},
		{
			name:   "workspace host returned unchanged",
			status: 200,
			body:   `{"oidc_endpoint":"https://ws.cloud.databricks.com/oidc","account_id":"acc-123","host_type":"workspace"}`,
			want:   "https://ws.cloud.databricks.com/oidc",
		},
		{
			name:         "placeholder with empty account_id falls back",
			status:       200,
			body:         `{"oidc_endpoint":"https://spog.databricks.com/oidc/accounts/{account_id}","account_id":""}`,
			wantFallback: true,
		},
		{
			name:         "empty oidc_endpoint falls back",
			status:       200,
			body:         `{"account_id":"acc-123"}`,
			wantFallback: true,
		},
		{
			name:         "non-https endpoint falls back",
			status:       200,
			body:         `{"oidc_endpoint":"http://spog.databricks.com/oidc","account_id":"acc-123"}`,
			wantFallback: true,
		},
		{
			name:         "non-databricks host falls back",
			status:       200,
			body:         `{"oidc_endpoint":"https://evil.example.com/oidc","account_id":"acc-123"}`,
			wantFallback: true,
		},
		{
			name:         "404 falls back",
			status:       404,
			wantFallback: true,
		},
		{
			name:         "garbage body falls back",
			status:       200,
			body:         `not json`,
			wantFallback: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/.well-known/databricks-config" {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				if tc.status != 200 {
					w.WriteHeader(tc.status)
					return
				}
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()

			host := strings.TrimPrefix(srv.URL, "https://")
			got := resolveOIDCIssuer(context.Background(), srv.Client(), host)

			if tc.wantFallback {
				if want := "https://" + host + "/oidc"; got != want {
					t.Fatalf("issuer = %q, want fallback %q", got, want)
				}
				return
			}
			if got != tc.want {
				t.Fatalf("issuer = %q, want %q", got, tc.want)
			}
		})
	}

	// Sanity: an unreachable host falls back without panicking.
	if got := resolveOIDCIssuer(context.Background(), &http.Client{}, "no-such-host.invalid"); got != fallbackForUnreachable {
		t.Fatalf("unreachable host issuer = %q, want %q", got, fallbackForUnreachable)
	}
}

func TestIsValidDatabricksIssuer(t *testing.T) {
	cases := []struct {
		issuer string
		want   bool
	}{
		{"https://spog.databricks.com/oidc/accounts/acc-123", true},
		{"https://ws.cloud.databricks.com/oidc", true},
		{"https://adb-1.azuredatabricks.net/oidc", true},
		{"https://spog.databricks.com/oidc/accounts/{account_id}", false}, // unresolved placeholder
		{"http://spog.databricks.com/oidc", false},                        // not https
		{"https://evil.example.com/oidc", false},                          // not a databricks host
		{"://bad", false},                                                 // unparseable
		{"", false},
	}
	for _, tc := range cases {
		t.Run(tc.issuer, func(t *testing.T) {
			if got := isValidDatabricksIssuer(tc.issuer); got != tc.want {
				t.Fatalf("isValidDatabricksIssuer(%q) = %v, want %v", tc.issuer, got, tc.want)
			}
		})
	}
}

func TestFetchHostMetadata_failuresFallBack(t *testing.T) {
	t.Run("404", func(t *testing.T) {
		srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		defer srv.Close()
		if _, ok := fetchHostMetadata(context.Background(), srv.Client(), srv.URL); ok {
			t.Fatal("ok=true on 404, want false (fallback)")
		}
	})

	t.Run("garbage body", func(t *testing.T) {
		srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte("not json"))
		}))
		defer srv.Close()
		if _, ok := fetchHostMetadata(context.Background(), srv.Client(), srv.URL); ok {
			t.Fatal("ok=true on garbage body, want false (fallback)")
		}
	})

	t.Run("unreachable", func(t *testing.T) {
		if _, ok := fetchHostMetadata(context.Background(), &http.Client{}, "https://127.0.0.1:1/nope"); ok {
			t.Fatal("ok=true on unreachable host, want false (fallback)")
		}
	})
}
