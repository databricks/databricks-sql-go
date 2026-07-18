package dbsql

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/databricks/databricks-sql-go/internal/config"

	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
)

// This file is untagged (no cgo) so its tests — the experimental-option
// classification guard, the option→config wiring, and the Thrift fail-loud
// signal — run in the default CGO_ENABLED=0 build alongside the other
// kernel_config tests.

// kernelExperimentalFieldDisposition records how each experimental (kernel-only)
// option is handled. Every experimental field is forwarded to the kernel C ABI
// (there is no "inert" experimental knob — they exist precisely because the kernel
// supports them) and rejected on the Thrift path (the connector fails loud when
// KernelExperimental is non-nil). A new field on config.KernelExperimentalConfig
// without an entry here fails TestKernelExperimentalFieldsClassified, forcing a
// deliberate decision and a setter in KernelBackend.OpenSession so it can't be
// silently dropped.
var kernelExperimentalFieldDisposition = map[string]string{
	"TLSTrustedCertsPEM":    "forwarded", // set_tls_trusted_certs
	"TLSSkipHostnameVerify": "forwarded", // set_tls_skip_hostname_verification
	"ProxyURL":              "forwarded", // set_proxy (url)
	"ProxyUsername":         "forwarded", // set_proxy (username)
	"ProxyPassword":         "forwarded", // set_proxy (password)
	"ProxyBypassHosts":      "forwarded", // set_proxy (bypass_hosts)
}

func TestKernelExperimentalFieldsClassified(t *testing.T) {
	tp := reflect.TypeOf(config.KernelExperimentalConfig{})
	classified := make(map[string]bool, tp.NumField())
	for i := 0; i < tp.NumField(); i++ {
		name := tp.Field(i).Name
		classified[name] = true
		if _, ok := kernelExperimentalFieldDisposition[name]; !ok {
			t.Errorf("config.KernelExperimentalConfig field %q is not classified. Add it to "+
				"kernelExperimentalFieldDisposition and wire a setter in KernelBackend.OpenSession / "+
				"newKernelBackend so it isn't silently dropped on the kernel path.", name)
		}
	}
	for name := range kernelExperimentalFieldDisposition {
		if !classified[name] {
			t.Errorf("kernelExperimentalFieldDisposition has %q but config.KernelExperimentalConfig no longer does; remove it", name)
		}
	}
}

// The experimental WithKernel* options are rejected on the default (Thrift) path
// so a caller who forgets WithUseKernel learns the option had no effect. The
// option builders set config.KernelExperimental; the connector's backend-selection
// branch is what rejects it. We assert the option→config wiring here (a non-nil
// KernelExperimental after applying a WithKernel* option is the signal the Thrift
// branch keys off).
func TestWithKernelTLSOptionsSetExperimental(t *testing.T) {
	cases := []struct {
		name   string
		opt    ConnOption
		verify func(*config.KernelExperimentalConfig) bool
	}{
		{"trusted certs", WithKernelTrustedCerts([]byte("ca")), func(k *config.KernelExperimentalConfig) bool {
			return string(k.TLSTrustedCertsPEM) == "ca"
		}},
		{"skip hostname", WithKernelSkipHostnameVerify(), func(k *config.KernelExperimentalConfig) bool {
			return k.TLSSkipHostnameVerify
		}},
		{"proxy", WithKernelProxy("http://proxy:3128", "u", "p", "*.internal"), func(k *config.KernelExperimentalConfig) bool {
			return k.ProxyURL == "http://proxy:3128" && k.ProxyUsername == "u" &&
				k.ProxyPassword == "p" && k.ProxyBypassHosts == "*.internal"
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := config.WithDefaults()
			c.opt(cfg)
			if cfg.KernelExperimental == nil {
				t.Fatalf("%s: KernelExperimental should be non-nil after the option", c.name)
			}
			if !c.verify(cfg.KernelExperimental) {
				t.Errorf("%s: option did not set the expected field(s): %+v", c.name, cfg.KernelExperimental)
			}
		})
	}
}

// End-to-end: setting a WithKernel* option WITHOUT WithUseKernel must make
// Connect fail loud on the default (Thrift) path rather than silently connect
// with a weaker-than-intended trust store. This exercises the connector's
// reject branch (not just option→config wiring) and asserts the error wraps the
// ErrRequiresKernelBackend sentinel so callers can detect it with errors.Is —
// the mirror of TestKernelBackendNotCompiledIn. Runs in the default
// CGO_ENABLED=0 build (no kernel linked in).
func TestWithKernelOptionsRejectedOnThriftPath(t *testing.T) {
	cases := []struct {
		name string
		opt  ConnOption
	}{
		{"trusted certs", WithKernelTrustedCerts([]byte("ca"))},
		{"skip hostname", WithKernelSkipHostnameVerify()},
		{"proxy", WithKernelProxy("http://proxy:3128", "", "", "")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, err := NewConnector(
				WithServerHostname("example.cloud.databricks.com"),
				WithPort(443),
				WithHTTPPath("/sql/1.0/endpoints/12346a5b5b0e123a"),
				WithAccessToken("supersecret"),
				tc.opt,
			)
			if err != nil {
				t.Fatalf("NewConnector: %v", err)
			}
			// No WithUseKernel — the Thrift path must reject the kernel-only option.
			if _, err = c.Connect(context.Background()); err == nil {
				t.Fatal("Connect should reject a WithKernel* option on the Thrift path, got nil")
			} else if !errors.Is(err, dbsqlerr.ErrRequiresKernelBackend) {
				t.Errorf("error should wrap ErrRequiresKernelBackend; got: %v", err)
			}
		})
	}
}

// WithKernelTrustedCerts must copy the PEM defensively, so a caller mutating its
// slice after applying the option (but before Connect) can't change the stored
// trust store. This is the option-set counterpart to TestKernelExperimentalDeepCopy
// (which covers the per-conn DeepCopy path).
func TestWithKernelTrustedCertsCopiesPEM(t *testing.T) {
	pem := []byte("ca-bundle")
	cfg := config.WithDefaults()
	WithKernelTrustedCerts(pem)(cfg)

	// Mutate the caller's slice after the option ran.
	pem[0] = 'X'

	if cfg.KernelExperimental == nil {
		t.Fatal("KernelExperimental should be non-nil after WithKernelTrustedCerts")
	}
	if got := string(cfg.KernelExperimental.TLSTrustedCertsPEM); got != "ca-bundle" {
		t.Errorf("WithKernelTrustedCerts aliased the caller's slice; stored %q, want %q", got, "ca-bundle")
	}
}

// DeepCopy must copy the CA byte slice, not alias it — the connector may DeepCopy
// the whole Config per conn, and a shared backing array would let one conn's
// mutation reach another.
func TestKernelExperimentalDeepCopy(t *testing.T) {
	orig := &config.KernelExperimentalConfig{
		TLSTrustedCertsPEM:    []byte("ca-bundle"),
		TLSSkipHostnameVerify: true,
		ProxyURL:              "http://proxy:3128",
		ProxyUsername:         "u",
		ProxyPassword:         "p",
		ProxyBypassHosts:      "*.internal",
	}
	cp := orig.DeepCopy()
	if cp == nil || string(cp.TLSTrustedCertsPEM) != "ca-bundle" || !cp.TLSSkipHostnameVerify {
		t.Fatalf("DeepCopy lost data: %+v", cp)
	}
	if cp.ProxyURL != "http://proxy:3128" || cp.ProxyUsername != "u" ||
		cp.ProxyPassword != "p" || cp.ProxyBypassHosts != "*.internal" {
		t.Errorf("DeepCopy lost proxy fields: %+v", cp)
	}
	cp.TLSTrustedCertsPEM[0] = 'X'
	if orig.TLSTrustedCertsPEM[0] == 'X' {
		t.Error("DeepCopy aliased the CA byte slice; a copy mutation reached the original")
	}
	if (*config.KernelExperimentalConfig)(nil).DeepCopy() != nil {
		t.Error("nil.DeepCopy() should be nil")
	}
}
