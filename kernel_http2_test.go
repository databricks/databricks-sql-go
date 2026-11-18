package dbsql

import (
	"testing"

	"github.com/databricks/databricks-sql-go/internal/backend/kernel"
	"github.com/databricks/databricks-sql-go/internal/config"
)

func TestHTTP2OptionAndDSN(t *testing.T) {
	for _, value := range []string{"", "true", "false"} {
		t.Run("dsn="+value, func(t *testing.T) {
			dsn := "token:test-token@example.databricks.com:443/sql/1.0/warehouses/test"
			if value != "" {
				dsn += "?enableHttp2=" + value
			}
			user, err := config.ParseDSN(dsn)
			if err != nil {
				t.Fatal(err)
			}
			cfg := baseKernelConfig()
			withUserConfig(user.DeepCopy())(cfg)
			got := buildKernelConfig(cfg, kernel.Auth{}).EnableHTTP2
			if value == "" {
				if got != nil {
					t.Fatal("omitted setting must leave the kernel default in effect")
				}
				return
			}
			want := value == "true"
			if got == nil || *got != want {
				t.Fatalf("EnableHTTP2 = %v, want %v", got, want)
			}
			WithEnableHTTP2(!want)(cfg)
			if got := buildKernelConfig(cfg, kernel.Auth{}).EnableHTTP2; got == nil || *got != !want {
				t.Fatal("connector option did not override the DSN setting")
			}
			withUserConfig(user)(cfg)
			if got := buildKernelConfig(cfg, kernel.Auth{}).EnableHTTP2; got == nil || *got != want {
				t.Fatal("DSN setting did not override the connector option")
			}
		})
	}
	if _, err := config.ParseDSN("token:test-token@example.databricks.com:443/sql/1.0/warehouses/test?enableHttp2=invalid"); err == nil {
		t.Fatal("invalid enableHttp2 must fail parsing")
	}
}

func TestHTTP2ConfigCopiesAreIndependent(t *testing.T) {
	cfg := baseKernelConfig()
	WithEnableHTTP2(false)(cfg)
	copied := cfg.KernelExperimental.DeepCopy()
	*copied.EnableHTTP2 = true
	if *cfg.KernelExperimental.EnableHTTP2 {
		t.Fatal("kernel config copy aliases the original HTTP/2 setting")
	}
	backendConfig := buildKernelConfig(cfg, kernel.Auth{})
	*backendConfig.EnableHTTP2 = true
	if *cfg.KernelExperimental.EnableHTTP2 {
		t.Fatal("backend config aliases the connector HTTP/2 setting")
	}
	value := false
	user := config.UserConfig{EnableHTTP2DSN: &value}
	userCopy := user.DeepCopy()
	*userCopy.EnableHTTP2DSN = true
	if *user.EnableHTTP2DSN {
		t.Fatal("user config copy aliases the original HTTP/2 setting")
	}
}
