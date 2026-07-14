package dbsql

import (
	"reflect"
	"testing"

	"github.com/databricks/databricks-sql-go/internal/config"
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

// DeepCopy must copy the CA byte slice, not alias it — the connector may DeepCopy
// the whole Config per conn, and a shared backing array would let one conn's
// mutation reach another.
func TestKernelExperimentalDeepCopy(t *testing.T) {
	orig := &config.KernelExperimentalConfig{
		TLSTrustedCertsPEM:    []byte("ca-bundle"),
		TLSSkipHostnameVerify: true,
	}
	cp := orig.DeepCopy()
	if cp == nil || string(cp.TLSTrustedCertsPEM) != "ca-bundle" || !cp.TLSSkipHostnameVerify {
		t.Fatalf("DeepCopy lost data: %+v", cp)
	}
	cp.TLSTrustedCertsPEM[0] = 'X'
	if orig.TLSTrustedCertsPEM[0] == 'X' {
		t.Error("DeepCopy aliased the CA byte slice; a copy mutation reached the original")
	}
	if (*config.KernelExperimentalConfig)(nil).DeepCopy() != nil {
		t.Error("nil.DeepCopy() should be nil")
	}
}
