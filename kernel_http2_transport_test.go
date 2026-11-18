//go:build cgo && databricks_kernel

package dbsql

import (
	"context"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestKernelHTTPSProtocolNegotiation(t *testing.T) {
	const child = "DATABRICKS_HTTP2_DIRECT_TEST"
	if os.Getenv(child) != "1" {
		command := exec.Command(os.Args[0], "-test.run=^TestKernelHTTPSProtocolNegotiation$", "-test.v") //nolint:gosec // Re-executes this test binary only.
		command.Env = append(os.Environ(), child+"=1",
			"HTTP_PROXY=", "http_proxy=", "HTTPS_PROXY=", "https_proxy=", "ALL_PROXY=", "all_proxy=")
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("protocol helper failed: %v\n%s", err, output)
		}
		if strings.Count(string(output), "--- PASS: TestKernelHTTPSProtocolNegotiation/") != 4 {
			t.Fatalf("protocol helper did not pass all four cases:\n%s", output)
		}
		return
	}
	for _, tc := range []struct {
		name        string
		serverHTTP2 bool
		option      []ConnOption
		want        int
	}{
		{name: "default", serverHTTP2: true, want: 2},
		{name: "enabled", serverHTTP2: true, option: []ConnOption{WithEnableHTTP2(true)}, want: 2},
		{name: "opt out", serverHTTP2: true, option: []ConnOption{WithEnableHTTP2(false)}, want: 1},
		{name: "fallback", serverHTTP2: false, want: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			versions := make(chan int, 4)
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				versions <- r.ProtoMajor
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"session_id":"http2-test"}`))
			}))
			server.EnableHTTP2 = tc.serverHTTP2
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
			connector, err := NewConnector(append(options, tc.option...)...)
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
					if got != tc.want {
						t.Fatalf("HTTP/%d received, want HTTP/%d", got, tc.want)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
		})
	}
}
