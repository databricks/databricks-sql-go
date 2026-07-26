//go:build cgo && databricks_kernel

package kernel

import "testing"

// httpPathCarriesOrgRouting decides whether OpenSession reroutes a warehouse-id
// connection through set_http_path so the kernel receives the SPOG ?o= org id.
// It must fire ONLY for a canonical warehouses/endpoints path with a non-empty
// o= value, so a reroute can never hand set_http_path a path from_http_path would
// reject. Runs under CGO_ENABLED=0 (pure Go, no kernel tag).
func TestHTTPPathCarriesOrgRouting(t *testing.T) {
	cases := []struct {
		name     string
		httpPath string
		want     bool
	}{
		// SPOG paths → reroute (the bug fix).
		{"warehouses with o=", "/sql/1.0/warehouses/abc123?o=42", true},
		{"endpoints with o=", "/sql/1.0/endpoints/abc123?o=42", true},
		{"no leading slash with o=", "sql/1.0/warehouses/abc123?o=42", true},
		{"o= among other params", "/sql/1.0/warehouses/abc?foo=1&o=42&bar=2", true},

		// Non-SPOG → keep existing warehouse-id routing (must NOT reroute).
		{"warehouses no query", "/sql/1.0/warehouses/abc123", false},
		{"empty o=", "/sql/1.0/warehouses/abc123?o=", false},
		{"query but no o=", "/sql/1.0/warehouses/abc123?foo=1", false},
		{"empty path", "", false},

		// Guard cases: a ?o= on a NON-warehouses path must NOT reroute (from_http_path
		// would reject it, so we must leave such a config on set_warehouse).
		{"o= on non-warehouse path", "/some/other/path?o=42", false},
		{"o= on bare query", "?o=42", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := httpPathCarriesOrgRouting(tc.httpPath); got != tc.want {
				t.Errorf("httpPathCarriesOrgRouting(%q) = %v, want %v", tc.httpPath, got, tc.want)
			}
		})
	}
}
