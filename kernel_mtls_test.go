//go:build cgo && databricks_kernel

package dbsql

// Hermetic TLS-feature coverage for the kernel backend: mutual TLS
// (WithKernelClientCertificate), custom-CA trust (WithKernelTrustedCerts), and
// hostname-skip (WithKernelSkipHostnameVerify), exercised end-to-end through the
// real cgo → kernel → rustls stack against a local httptest TLS server — no
// warehouse, so this runs in the tagged build-and-test-kernel CI job (and nightly).
//
// The kernel speaks SEA (JSON over HTTPS); a bare httptest server is not a SEA
// endpoint, so the *query* always fails at the application layer. That is fine: the
// property under test is whether the TLS/mTLS HANDSHAKE completes with the forwarded
// material. The server handler records, per connection, whether it was reached and
// whether a client certificate was presented — reaching the handler AT ALL means the
// handshake (including client-cert verification when demanded) succeeded. Each case
// asserts the reached/​saw-cert signal, then tears the connect down immediately
// rather than waiting out the kernel's connect-retry budget, so the whole file runs
// in a few seconds.

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// testCertPair is a leaf certificate + its PKCS#8 private key, both PEM-encoded.
type testCertPair struct {
	certPEM []byte
	keyPEM  []byte
	tlsCert tls.Certificate
}

// mkTestCA returns a self-signed CA cert (parsed, for signing leaves) plus its key
// and PEM encoding (for WithKernelTrustedCerts).
func mkTestCA(t *testing.T) (*x509.Certificate, *ecdsa.PrivateKey, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("CA key: %v", err)
	}
	// Fixed validity window (no time.Now) so the cert is deterministic; well inside
	// any realistic test clock.
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "databricks-sql-go test CA"},
		NotBefore:             time.Unix(1_600_000_000, 0),
		NotAfter:              time.Unix(4_000_000_000, 0),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("CA cert: %v", err)
	}
	ca, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse CA: %v", err)
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	return ca, key, caPEM
}

// mkTestLeaf mints a CA-signed leaf. serverSANs (IPs or DNS names) are set for a
// server leaf; an empty slice yields a client-auth leaf. The key is emitted as
// PKCS#8 — the form the kernel's tls-rustls Identity::from_pem accepts.
func mkTestLeaf(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, cn string, serverSANs []string) testCertPair {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("leaf key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Unix(1_600_000_000, 0),
		NotAfter:     time.Unix(4_000_000_000, 0),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	if len(serverSANs) > 0 {
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		for _, s := range serverSANs {
			if ip := net.ParseIP(s); ip != nil {
				tmpl.IPAddresses = append(tmpl.IPAddresses, ip)
			} else {
				tmpl.DNSNames = append(tmpl.DNSNames, s)
			}
		}
	} else {
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatalf("leaf cert: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("x509 key pair: %v", err)
	}
	return testCertPair{certPEM: certPEM, keyPEM: keyPEM, tlsCert: tlsCert}
}

// tlsProbe is a local httptest TLS server that records, across connections,
// whether its handler was reached and whether a client cert was presented.
type tlsProbe struct {
	srv     *httptest.Server
	reached atomic.Bool
	sawCert atomic.Bool
}

func (p *tlsProbe) host() string { return p.srv.Listener.Addr().String() }

// startTLSProbe serves HTTPS with the given server cert and client-auth policy.
// The handler always returns 503 (not a real SEA endpoint); the point is that
// reaching it proves the handshake completed.
func startTLSProbe(t *testing.T, serverCert tls.Certificate, clientCAs *x509.CertPool, clientAuth tls.ClientAuthType) *tlsProbe {
	t.Helper()
	p := &tlsProbe{}
	p.srv = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p.reached.Store(true)
		if r.TLS != nil && len(r.TLS.PeerCertificates) > 0 {
			p.sawCert.Store(true)
		}
		http.Error(w, "not a SEA endpoint", http.StatusServiceUnavailable)
	}))
	p.srv.TLS = &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   clientAuth,
		ClientCAs:    clientCAs,
		MinVersion:   tls.VersionTLS12,
	}
	p.srv.StartTLS()
	t.Cleanup(p.srv.Close)
	return p
}

// runKernelConnect opens a kernel-backed connection to host and drives a query in a
// goroutine to force the TLS handshake. It returns as soon as onReached() reports
// the outcome is decided (the handshake succeeded, observed via the probe) OR the
// query returns on its own OR settle elapses — whichever first — then cancels the
// query. This avoids waiting out the kernel's connect-retry budget on the negative
// (handshake-should-fail) cases. It does not assert; the caller inspects the probe.
func runKernelConnect(t *testing.T, host string, settle time.Duration, onReached func() bool, extra ...ConnOption) {
	t.Helper()
	opts := append([]ConnOption{
		WithServerHostname(host),
		WithHTTPPath("/sql/1.0/warehouses/hermetic"),
		WithAccessToken("dapi-hermetic-placeholder"),
		WithUseKernel(true),
	}, extra...)
	connector, err := NewConnector(opts...)
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		var x int64
		_ = db.QueryRowContext(ctx, "SELECT 1").Scan(&x)
	}()

	// Poll the decision signal frequently; bail (cancel) the instant it flips so a
	// positive case finishes sub-second instead of riding the retry budget. On a
	// negative case the signal never flips, so we wait out `settle` — long enough
	// for at least one handshake attempt to complete-or-fail — then cancel.
	deadline := time.Now().Add(settle)
	for time.Now().Before(deadline) {
		if onReached() {
			break
		}
		select {
		case <-done:
			return
		case <-time.After(20 * time.Millisecond):
		}
	}
	cancel()
	<-done
}

// TestKernelMTLSHandshake proves WithKernelClientCertificate is forwarded and
// enforced: with a client cert the mTLS handshake completes (handler reached, cert
// seen); without one, a RequireAndVerifyClientCert server rejects the handshake
// (handler never reached).
func TestKernelMTLSHandshake(t *testing.T) {
	ca, caKey, caPEM := mkTestCA(t)
	server := mkTestLeaf(t, ca, caKey, "127.0.0.1", []string{"127.0.0.1"})
	client := mkTestLeaf(t, ca, caKey, "hermetic-client", nil)
	clientCAs := x509.NewCertPool()
	clientCAs.AddCert(ca)

	t.Run("client cert forwarded -> handshake succeeds", func(t *testing.T) {
		p := startTLSProbe(t, server.tlsCert, clientCAs, tls.RequireAndVerifyClientCert)
		runKernelConnect(t, p.host(), 10*time.Second, p.reached.Load,
			WithKernelTrustedCerts(caPEM),
			WithKernelClientCertificate(client.certPEM, client.keyPEM),
		)
		if !p.reached.Load() {
			t.Fatal("handler never reached: the mTLS handshake failed — client cert not forwarded/accepted")
		}
		if !p.sawCert.Load() {
			t.Error("handler reached but no client cert was presented — mTLS identity not forwarded")
		}
	})

	t.Run("no client cert -> handshake rejected", func(t *testing.T) {
		p := startTLSProbe(t, server.tlsCert, clientCAs, tls.RequireAndVerifyClientCert)
		// Trust the CA so the SERVER cert validates; present no client identity. The
		// server demands one, so the handshake must fail before the handler runs.
		runKernelConnect(t, p.host(), 3*time.Second, p.reached.Load,
			WithKernelTrustedCerts(caPEM),
		)
		if p.reached.Load() {
			t.Error("handler was reached without a client cert — mTLS was not enforced")
		}
	})
}

// TestKernelCustomCATrust proves WithKernelTrustedCerts is forwarded: a
// privately-signed server cert validates only when its CA is trusted.
func TestKernelCustomCATrust(t *testing.T) {
	ca, caKey, caPEM := mkTestCA(t)
	server := mkTestLeaf(t, ca, caKey, "127.0.0.1", []string{"127.0.0.1"})

	t.Run("CA trusted -> TLS validates", func(t *testing.T) {
		p := startTLSProbe(t, server.tlsCert, nil, tls.NoClientCert)
		runKernelConnect(t, p.host(), 10*time.Second, p.reached.Load,
			WithKernelTrustedCerts(caPEM))
		if !p.reached.Load() {
			t.Fatal("handler not reached with the CA trusted — WithKernelTrustedCerts not forwarded")
		}
	})

	t.Run("CA untrusted -> TLS fails", func(t *testing.T) {
		p := startTLSProbe(t, server.tlsCert, nil, tls.NoClientCert)
		// No trusted certs: the private CA is unknown, so the chain must not validate.
		runKernelConnect(t, p.host(), 3*time.Second, p.reached.Load)
		if p.reached.Load() {
			t.Error("handler reached without trusting the CA — an unknown issuer was accepted")
		}
	})
}

// TestKernelSkipHostnameVerify proves WithKernelSkipHostnameVerify is forwarded: a
// server cert whose SAN does not cover the dialed host is rejected on hostname
// grounds unless the skip is set (chain validation stays on either way).
func TestKernelSkipHostnameVerify(t *testing.T) {
	ca, caKey, caPEM := mkTestCA(t)
	// SAN deliberately does NOT include 127.0.0.1 (the dialed host).
	wrongHost := mkTestLeaf(t, ca, caKey, "wrong.example", []string{"not-the-host.example"})

	t.Run("wrong SAN, no skip -> hostname check fails", func(t *testing.T) {
		p := startTLSProbe(t, wrongHost.tlsCert, nil, tls.NoClientCert)
		runKernelConnect(t, p.host(), 3*time.Second, p.reached.Load,
			WithKernelTrustedCerts(caPEM)) // chain trusted, but hostname won't match
		if p.reached.Load() {
			t.Error("handler reached despite a hostname mismatch and no skip — hostname verification not enforced")
		}
	})

	t.Run("wrong SAN, skip on -> handshake succeeds", func(t *testing.T) {
		p := startTLSProbe(t, wrongHost.tlsCert, nil, tls.NoClientCert)
		runKernelConnect(t, p.host(), 10*time.Second, p.reached.Load,
			WithKernelTrustedCerts(caPEM), WithKernelSkipHostnameVerify())
		if !p.reached.Load() {
			t.Fatal("handler not reached with hostname-skip on — WithKernelSkipHostnameVerify not forwarded")
		}
	})
}
