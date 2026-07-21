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

// runKernelConnect makes ONE synchronous kernel-backed connect to host to force the
// TLS handshake, then returns. It does not assert; the caller inspects the probe
// (which records, as a side effect of the handshake, whether its handler was reached
// and whether a client cert was presented).
//
// It drives the connect directly through driver.Connector.Connect rather than
// sql.OpenDB + a query, deliberately: database/sql's background connectionOpener
// would spawn a connect goroutine this function does not own and db.Close() does not
// synchronously drain. Because kernel_session_open is a blocking cgo call that cannot
// observe a Go ctx cancel mid-call, such a connect can still be in flight — writing
// kernel debug logs — after the test returns and `go test` has closed the test pipe,
// which surfaces as a "signal: broken pipe" package failure. A single synchronous
// Connect has no background opener: when it returns, the connect is complete and
// nothing lingers. The connect chain exercised (Connect → newKernelBackend →
// OpenSession → applyKernelTLS → the C-ABI TLS setters) is identical to the query
// path, so the handshake — the property under test — is covered the same way.
//
// The connect itself always fails at the application layer (the httptest probe is not
// a SEA endpoint), so Connect returns an error even on the positive cases; that is
// expected and ignored. connectTimeout bounds the call as a safety net; WithRetries
// disable keeps it to a single attempt so a handshake-should-fail case returns at once
// rather than riding the kernel's default retry budget.
func runKernelConnect(t *testing.T, host string, connectTimeout time.Duration, extra ...ConnOption) {
	t.Helper()
	opts := append([]ConnOption{
		WithServerHostname(host),
		WithHTTPPath("/sql/1.0/warehouses/hermetic"),
		WithAccessToken("dapi-hermetic-placeholder"),
		WithUseKernel(true),
		// One attempt only (the WithRetries disable form, honored on the kernel
		// path): these cases assert the handshake OUTCOME, not retry behaviour, so a
		// failing handshake should return immediately instead of retrying.
		WithRetries(-1, 0, 0),
	}, extra...)
	connector, err := NewConnector(opts...)
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()
	// Synchronous: returns only when the connect (and thus the TLS handshake) is
	// done. The error is expected (non-SEA endpoint) and irrelevant — the caller
	// reads the probe for the handshake outcome.
	if conn, err := connector.Connect(ctx); err == nil {
		_ = conn.Close()
	}
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
		runKernelConnect(t, p.host(), 10*time.Second,
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
		runKernelConnect(t, p.host(), 3*time.Second,
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
		runKernelConnect(t, p.host(), 10*time.Second,
			WithKernelTrustedCerts(caPEM))
		if !p.reached.Load() {
			t.Fatal("handler not reached with the CA trusted — WithKernelTrustedCerts not forwarded")
		}
	})

	t.Run("CA untrusted -> TLS fails", func(t *testing.T) {
		p := startTLSProbe(t, server.tlsCert, nil, tls.NoClientCert)
		// No trusted certs: the private CA is unknown, so the chain must not validate.
		runKernelConnect(t, p.host(), 3*time.Second)
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
		runKernelConnect(t, p.host(), 3*time.Second,
			WithKernelTrustedCerts(caPEM)) // chain trusted, but hostname won't match
		if p.reached.Load() {
			t.Error("handler reached despite a hostname mismatch and no skip — hostname verification not enforced")
		}
	})

	t.Run("wrong SAN, skip on -> handshake succeeds", func(t *testing.T) {
		p := startTLSProbe(t, wrongHost.tlsCert, nil, tls.NoClientCert)
		runKernelConnect(t, p.host(), 10*time.Second,
			WithKernelTrustedCerts(caPEM), WithKernelSkipHostnameVerify())
		if !p.reached.Load() {
			t.Fatal("handler not reached with hostname-skip on — WithKernelSkipHostnameVerify not forwarded")
		}
	})
}
