//go:build cgo && databricks_kernel

package dbsql

import (
	"context"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func TestKernelHTTP2Transport(t *testing.T) {
	for _, name := range []string{"HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy", "ALL_PROXY", "all_proxy"} {
		if os.Getenv(name) != "" {
			t.Skipf("unset %s to test direct HTTP/2 negotiation", name)
		}
	}
	cases := []struct {
		name        string
		options     []ConnOption
		wantVersion int
	}{
		{name: "default", wantVersion: 2},
		{name: "opt_out", options: []ConnOption{WithEnableHTTP2(false)}, wantVersion: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			versions := make(chan int, 4)
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				versions <- r.ProtoMajor
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"session_id":"http2-test"}`))
			}))
			server.EnableHTTP2 = true
			server.StartTLS()
			defer server.Close()
			ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
			options := []ConnOption{
				WithServerHostname(server.URL),
				WithHTTPPath("/sql/1.0/warehouses/test"),
				WithAccessToken("test-token"),
				WithUseKernel(true),
				WithKernelTrustedCerts(ca),
				WithRetries(-1, 0, 0),
			}
			connector, err := NewConnector(append(options, tc.options...)...)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			connection, err := connector.Connect(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := connection.Close(); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				select {
				case got := <-versions:
					if got != tc.wantVersion {
						t.Fatalf("HTTP/%d received, want HTTP/%d", got, tc.wantVersion)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
		})
	}
}
