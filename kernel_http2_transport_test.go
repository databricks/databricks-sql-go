//go:build cgo && databricks_kernel

package dbsql

import (
	"context"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestKernelHTTP2OptOut(t *testing.T) {
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
	connector, err := NewConnector(
		WithServerHostname(server.URL),
		WithHTTPPath("/sql/1.0/warehouses/test"),
		WithAccessToken("test-token"),
		WithUseKernel(true),
		WithEnableHTTP2(false),
		WithKernelTrustedCerts(ca),
		WithRetries(-1, 0, 0),
	)
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
			if got != 1 {
				t.Fatalf("HTTP/%d received, want HTTP/1.1", got)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
}
