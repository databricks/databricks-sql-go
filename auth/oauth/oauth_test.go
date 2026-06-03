package oauth

import (
	"context"
	"net/http"
	"net/http/httptest"
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

func TestResolveOIDCIssuer_substitutesAccountID(t *testing.T) {
	// Unified / SPOG host advertises an account-rooted endpoint with a placeholder.
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/.well-known/databricks-config" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = w.Write([]byte(`{
			"oidc_endpoint": "https://spog.example.com/oidc/accounts/{account_id}",
			"account_id": "7a99b43c-b46c-432b-b0a7-814217701909",
			"host_type": "unified"
		}`))
	}))
	defer srv.Close()

	meta, ok := fetchHostMetadata(context.Background(), srv.Client(), srv.URL+"/.well-known/databricks-config")
	if !ok {
		t.Fatal("fetchHostMetadata returned ok=false, want true")
	}
	got := substituteAccountID(meta)
	want := "https://spog.example.com/oidc/accounts/7a99b43c-b46c-432b-b0a7-814217701909"
	if got != want {
		t.Fatalf("issuer = %q, want %q", got, want)
	}
}

func TestResolveOIDCIssuer_workspaceHostUnchanged(t *testing.T) {
	// Normal workspace host: endpoint has no placeholder, so it is returned as-is
	// (equivalent to the historical https://<host>/oidc).
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{
			"oidc_endpoint": "https://ws.cloud.databricks.com/oidc",
			"account_id": "7a99b43c-b46c-432b-b0a7-814217701909",
			"host_type": "workspace"
		}`))
	}))
	defer srv.Close()

	meta, ok := fetchHostMetadata(context.Background(), srv.Client(), srv.URL+"/.well-known/databricks-config")
	if !ok {
		t.Fatal("fetchHostMetadata returned ok=false, want true")
	}
	if got := substituteAccountID(meta); got != "https://ws.cloud.databricks.com/oidc" {
		t.Fatalf("issuer = %q, want unchanged workspace endpoint", got)
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
