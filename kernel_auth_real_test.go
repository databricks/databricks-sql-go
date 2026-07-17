package dbsql

import (
	"testing"

	"github.com/databricks/databricks-sql-go/auth/oauth/m2m"
	"github.com/databricks/databricks-sql-go/auth/oauth/u2m"
	"github.com/databricks/databricks-sql-go/internal/backend/kernel"
)

// resolveKernelAuth selects the M2M / U2M path by asserting cfg.Authenticator
// against the kernel.M2MCredentialsProvider / U2MCredentialsProvider interfaces,
// which the REAL m2m/u2m authenticators satisfy only structurally. The other kernel
// auth tests bind fakeM2MAuth / fakeU2MAuth, so a rename or reshaping of the real
// M2MCredentials / M2MScopes / U2MClientID methods would compile clean, keep those
// fake-based tests green, and only at runtime fall through to the "unsupported
// authenticator" default — silently disabling every OAuth-over-kernel connect.
//
// This test drives resolveKernelAuth with the real constructors so that breakage is
// caught at PR time. It cannot live as a `var _ kernel.M2MCredentialsProvider`
// assertion inside the m2m/u2m packages: those types are unexported, and importing
// the kernel package there forms an import cycle (kernel -> internal/config ->
// auth/oauth/{m2m,u2m}). It runs in the default CGO_ENABLED=0 build.
func TestResolveKernelAuthRealAuthenticators(t *testing.T) {
	t.Run("real M2M authenticator resolves to an M2M descriptor", func(t *testing.T) {
		c := baseKernelConfig()
		c.AccessToken = ""
		// m2m.NewAuthenticator is hermetic (no network at construction) and defaults
		// its scopes to {"all-apis"} (m2m.GetScopes), which the kernel forwards.
		c.Authenticator = m2m.NewAuthenticator("real-cid", "real-secret", "staging.cloud.databricks.com")
		a, err := validateKernelConfig(c)
		if err != nil {
			t.Fatalf("real M2M authenticator should validate; an 'unsupported authenticator' "+
				"error here means *authClient no longer satisfies kernel.M2MCredentialsProvider "+
				"(M2MCredentials/M2MScopes renamed?): %v", err)
		}
		if a.Mode != kernel.AuthM2M || a.ClientID != "real-cid" || a.ClientSecret != "real-secret" {
			t.Errorf("auth = %+v, want mode=M2M clientID=real-cid clientSecret=real-secret", a)
		}
	})

	t.Run("real U2M authenticator resolves to a U2M descriptor", func(t *testing.T) {
		// An Azure host avoids the AWS/GCP OIDC discovery network call in
		// u2m.NewAuthenticator (GetEndpoint returns hardcoded Azure endpoint URLs),
		// so the real authenticator constructs hermetically here.
		authr, err := u2m.NewAuthenticator("adb-1234567890.1.azuredatabricks.net", 0)
		if err != nil {
			// Construction is expected to be offline for Azure; if the environment
			// ever makes it fail, skip rather than flake — the property under test is
			// that the returned concrete type satisfies the provider interface, not
			// that construction succeeds offline.
			t.Skipf("u2m.NewAuthenticator (Azure) failed in this environment: %v", err)
		}

		c := baseKernelConfig()
		c.AccessToken = ""
		c.Authenticator = authr
		got, err := validateKernelConfig(c)
		if err != nil {
			t.Fatalf("real U2M authenticator should validate; an 'unsupported authenticator' "+
				"error here means *u2mAuthenticator no longer satisfies kernel.U2MCredentialsProvider "+
				"(U2MClientID renamed?): %v", err)
		}
		if got.Mode != kernel.AuthU2M || got.ClientID == "" {
			t.Errorf("auth = %+v, want mode=U2M with a non-empty (cloud-inferred) clientID", got)
		}
	})
}
