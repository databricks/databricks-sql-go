//go:build cgo && databricks_kernel

package dbsql

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// This suite exercises the complete Go → cgo → kernel → rustls path against a
// local server that requires a verified client certificate. The server is not a
// SEA endpoint, so connection setup eventually fails at the application layer;
// reaching its handler proves the TLS handshake completed.

type mtlsTestCertPair struct {
	certPEM []byte
	keyPEM  []byte
	tlsCert tls.Certificate
}

func makeMTLSTestCA(t *testing.T) (*x509.Certificate, *ecdsa.PrivateKey, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "databricks-sql-go mTLS test CA"},
		NotBefore:             time.Unix(1_600_000_000, 0),
		NotAfter:              time.Unix(4_000_000_000, 0),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA certificate: %v", err)
	}
	ca, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse CA certificate: %v", err)
	}
	return ca, key, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func makeMTLSTestLeaf(
	t *testing.T,
	ca *x509.Certificate,
	caKey *ecdsa.PrivateKey,
	commonName string,
	serverIP net.IP,
) mtlsTestCertPair {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate leaf key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: commonName},
		NotBefore:    time.Unix(1_600_000_000, 0),
		NotAfter:     time.Unix(4_000_000_000, 0),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	if serverIP != nil {
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		template.IPAddresses = []net.IP{serverIP}
	} else {
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create leaf certificate: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal leaf key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("parse leaf key pair: %v", err)
	}
	return mtlsTestCertPair{certPEM: certPEM, keyPEM: keyPEM, tlsCert: tlsCert}
}

type mtlsProbe struct {
	server  *httptest.Server
	reached atomic.Bool
	sawCert atomic.Bool
}

func startMTLSProbe(
	t *testing.T,
	serverCert tls.Certificate,
	clientCAs *x509.CertPool,
	clientAuth tls.ClientAuthType,
) *mtlsProbe {
	t.Helper()
	probe := &mtlsProbe{}
	probe.server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		probe.reached.Store(true)
		probe.sawCert.Store(r.TLS != nil && len(r.TLS.PeerCertificates) > 0)
		http.Error(w, "not a SEA endpoint", http.StatusServiceUnavailable)
	}))
	probe.server.TLS = &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   clientAuth,
		ClientCAs:    clientCAs,
		MinVersion:   tls.VersionTLS12,
	}
	probe.server.StartTLS()
	t.Cleanup(probe.server.Close)
	return probe
}

func runMTLSKernelConnect(t *testing.T, host string, options ...ConnOption) {
	t.Helper()
	base := []ConnOption{
		WithServerHostname(host),
		WithHTTPPath("/sql/1.0/warehouses/hermetic"),
		WithAccessToken("dapi-hermetic-placeholder"),
		WithUseKernel(true),
		WithRetries(-1, 0, 0),
	}
	connector, err := NewConnector(append(base, options...)...)
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if conn, err := connector.Connect(ctx); err == nil {
		_ = conn.Close()
	}
}

func TestKernelMTLSHandshake(t *testing.T) {
	ca, caKey, caPEM := makeMTLSTestCA(t)
	serverCert := makeMTLSTestLeaf(t, ca, caKey, "127.0.0.1", net.ParseIP("127.0.0.1"))
	clientCert := makeMTLSTestLeaf(t, ca, caKey, "hermetic-client", nil)
	clientCAs := x509.NewCertPool()
	clientCAs.AddCert(ca)

	t.Run("client identity reaches server", func(t *testing.T) {
		probe := startMTLSProbe(t, serverCert.tlsCert, clientCAs, tls.RequireAndVerifyClientCert)
		runMTLSKernelConnect(
			t,
			probe.server.Listener.Addr().String(),
			WithKernelTrustedCerts(caPEM),
			WithKernelClientCertificate(clientCert.certPEM, clientCert.keyPEM),
		)
		if !probe.reached.Load() {
			t.Fatal("server handler not reached: mTLS handshake did not complete")
		}
		if !probe.sawCert.Load() {
			t.Fatal("server handler reached without the configured client certificate")
		}
	})

	t.Run("unset client identity is not presented", func(t *testing.T) {
		// The kernel repository's C-ABI E2E suite proves a server requiring a
		// certificate rejects an anonymous client. Here the server requests and
		// verifies a certificate when present but permits anonymity, which proves
		// the Go option's unset path without provoking a peer-closed TLS write that
		// can deliver SIGPIPE from the linked native stack.
		probe := startMTLSProbe(t, serverCert.tlsCert, clientCAs, tls.VerifyClientCertIfGiven)
		runMTLSKernelConnect(
			t,
			probe.server.Listener.Addr().String(),
			WithKernelTrustedCerts(caPEM),
		)
		if !probe.reached.Load() {
			t.Fatal("server handler not reached for anonymous TLS connection")
		}
		if probe.sawCert.Load() {
			t.Fatal("server saw a client certificate although the option was unset")
		}
	})
}
