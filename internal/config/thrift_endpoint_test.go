package config

import "testing"

// TestThriftEndpointUnchanged pins the Thrift endpoint for the port inputs the kernel custom-port
// change decides on. Thrift is not changed by that work, so these strings are what it builds today.
// An embedded host port plus the default 443 yields an unparseable URL on Thrift; that is the baseline.
func TestThriftEndpointUnchanged(t *testing.T) {
	cases := []struct {
		name string
		host string
		port int
		want string
	}{
		{"bare host, default port", "h.io", 443, "https://h.io:443/sql/1.0/warehouses/x"},
		{"bare host, WithPort", "h.io", 8443, "https://h.io:8443/sql/1.0/warehouses/x"},
		{"embedded port, default port", "h.io:8443", 443, "https://h.io:8443:443/sql/1.0/warehouses/x"},
		{"embedded port, WithPort", "h.io:8443", 8443, "https://h.io:8443:8443/sql/1.0/warehouses/x"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &Config{Protocol: "https", Host: tc.host, Port: tc.port, HTTPPath: "/sql/1.0/warehouses/x"}
			got, err := c.ToEndpointURL()
			if err != nil {
				t.Fatalf("unexpected err %v", err)
			}
			if got != tc.want {
				t.Errorf("ToEndpointURL() = %q, want %q", got, tc.want)
			}
		})
	}
}
