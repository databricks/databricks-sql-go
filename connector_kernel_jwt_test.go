//go:build cgo && databricks_kernel

package dbsql

import (
	"testing"

	"github.com/databricks/databricks-sql-go/internal/config"
)

func TestWithJWTPrivateKeyM2M(t *testing.T) {
	apply := func(cfg JWTPrivateKeyM2MConfig) *config.Config {
		c := &config.Config{}
		WithJWTPrivateKeyM2M(cfg)(c)
		return c
	}

	t.Run("full config installs the JWT authenticator", func(t *testing.T) {
		c := apply(JWTPrivateKeyM2MConfig{ClientID: "sp", KeyFile: "/k.pem", Kid: "kid-1"})
		if c.Authenticator == nil {
			t.Fatal("expected an authenticator to be installed for a complete JWT config")
		}
		// It must satisfy the interface the kernel backend asserts to select JWT M2M.
		if _, ok := c.Authenticator.(interface {
			JWTM2MCredentials() (string, string, string, string, string, string, []string)
		}); !ok {
			t.Fatalf("installed authenticator %T does not expose JWTM2MCredentials", c.Authenticator)
		}
	})

	t.Run("empty config is a silent no-op", func(t *testing.T) {
		c := apply(JWTPrivateKeyM2MConfig{})
		if c.Authenticator != nil {
			t.Fatalf("expected no authenticator for an empty config, got %T", c.Authenticator)
		}
	})

	t.Run("partial config installs no authenticator", func(t *testing.T) {
		// ClientID + KeyFile but no Kid: the required trio is incomplete, so no
		// authenticator is installed (and WithJWTPrivateKeyM2M warns).
		c := apply(JWTPrivateKeyM2MConfig{ClientID: "sp", KeyFile: "/k.pem"})
		if c.Authenticator != nil {
			t.Fatalf("expected no authenticator for a partial config, got %T", c.Authenticator)
		}
	})
}
