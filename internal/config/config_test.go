package config

import (
	"crypto/tls"
	"reflect"
	"testing"
	"time"

	"github.com/databricks/databricks-sql-go/auth/noop"
	"github.com/databricks/databricks-sql-go/auth/pat"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
)

func TestParseConfig(t *testing.T) {
	type args struct {
		dsn string
	}
	tz, _ := time.LoadLocation("America/Vancouver")
	tests := []struct {
		name    string
		args    args
		wantCfg UserConfig
		wantURL string
		wantErr bool
	}{
		{
			name: "base case",
			args: args{dsn: "token:supersecret@example.cloud.databricks.com:443/sql/1.0/endpoints/12346a5b5b0e123a"},
			wantCfg: UserConfig{
				Protocol:      "https",
				Host:          "example.cloud.databricks.com",
				Port:          443,
				MaxRows:       defaultMaxRows,
				Authenticator: &pat.PATAuth{AccessToken: "supersecret"},
				HTTPPath:      "/sql/1.0/endpoints/12346a5b5b0e123a",
				SessionParams: make(map[string]string),
			},
			wantURL: "https://example.cloud.databricks.com:443/sql/1.0/endpoints/12346a5b5b0e123a",
			wantErr: false,
		},
		{
			name: "with https scheme",
			args: args{dsn: "https://token:supersecret@example.cloud.databricks.com:443/sql/1.0/endpoints/12346a5b5b0e123a"},
			wantCfg: UserConfig{
				Protocol:      "https",
				Host:          "example.cloud.databricks.com",
				Port:          443,
				MaxRows:       defaultMaxRows,
				Authenticator: &pat.PATAuth{AccessToken: "supersecret"},
				HTTPPath:      "/sql/1.0/endpoints/12346a5b5b0e123a",
				SessionParams: make(map[string]string),
			},
			wantURL: "https://example.cloud.databricks.com:443/sql/1.0/endpoints/12346a5b5b0e123a",
			wantErr: false,
		},
		{
			name: "with http scheme",
			args: args{dsn: "http://localhost:8080/sql/1.0/endpoints/12346a5b5b0e123a"},
			wantCfg: UserConfig{
				Protocol:      "http",
				Host:          "localhost",
				Port:          8080,
				Authenticator: &noop.NoopAuth{},
				MaxRows:       defaultMaxRows,
				HTTPPath:      "/sql/1.0/endpoints/12346a5b5b0e123a",
				SessionParams: make(map[string]string),
			},
			wantErr: false,
			wantURL: "http://localhost:8080/sql/1.0/endpoints/12346a5b5b0e123a",
		},
		{
			name: "with localhost",
			args: args{dsn: "http://localhost:8080"},
			wantCfg: UserConfig{
				Protocol:      "http",
				Host:          "localhost",
				Authenticator: &noop.NoopAuth{},
				Port:          8080,
				MaxRows:       defaultMaxRows,
				SessionParams: make(map[string]string),
			},
			wantErr: false,
			wantURL: "http://localhost:8080",
		},
		{
			name: "with query params",
			args: args{dsn: "token:supersecret@example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123a?timeout=100&maxRows=1000"},
			wantCfg: UserConfig{
				Protocol:      "https",
				Host:          "example.cloud.databricks.com",
				Port:          8000,
				Authenticator: &pat.PATAuth{AccessToken: "supersecret"},
				HTTPPath:      "/sql/1.0/endpoints/12346a5b5b0e123a",
				QueryTimeout:  100 * time.Second,
				MaxRows:       1000,
				SessionParams: make(map[string]string),
			},
			wantURL: "https://example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123a",
			wantErr: false,
		},
		{
			name: "with query params and session params",
			args: args{dsn: "token:supersecret@example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123a?timeout=100&maxRows=1000&timezone=America/Vancouver"},
			wantCfg: UserConfig{
				Protocol:      "https",
				Host:          "example.cloud.databricks.com",
				Port:          8000,
				Authenticator: &pat.PATAuth{AccessToken: "supersecret"},
				HTTPPath:      "/sql/1.0/endpoints/12346a5b5b0e123a",
				QueryTimeout:  100 * time.Second,
				MaxRows:       1000,
				Location:      tz,
				SessionParams: map[string]string{"timezone": "America/Vancouver"},
			},
			wantURL: "https://example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123a",
			wantErr: false,
		},
		{
			name: "bare",
			args: args{dsn: "example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123a"},
			wantCfg: UserConfig{
				Protocol:      "https",
				Host:          "example.cloud.databricks.com",
				Port:          8000,
				MaxRows:       defaultMaxRows,
				Authenticator: &noop.NoopAuth{},
				HTTPPath:      "/sql/1.0/endpoints/12346a5b5b0e123a",
				SessionParams: make(map[string]string),
			},
			wantURL: "https://example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123a",
			wantErr: false,
		},
		{
			name: "with catalog",
			args: args{dsn: "token:supersecret@example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123b?catalog=default"},
			wantCfg: UserConfig{
				Protocol:      "https",
				Host:          "example.cloud.databricks.com",
				Port:          8000,
				MaxRows:       defaultMaxRows,
				Authenticator: &pat.PATAuth{AccessToken: "supersecret"},
				HTTPPath:      "/sql/1.0/endpoints/12346a5b5b0e123b",
				Catalog:       "default",
				SessionParams: make(map[string]string),
			},
			wantURL: "https://example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123b",
			wantErr: false,
		},
		{
			name: "with user agent entry",
			args: args{dsn: "token:supersecret@example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123b?userAgentEntry=partner-name"},
			wantCfg: UserConfig{
				Protocol:       "https",
				Host:           "example.cloud.databricks.com",
				Port:           8000,
				MaxRows:        defaultMaxRows,
				Authenticator:  &pat.PATAuth{AccessToken: "supersecret"},
				HTTPPath:       "/sql/1.0/endpoints/12346a5b5b0e123b",
				UserAgentEntry: "partner-name",
				SessionParams:  make(map[string]string),
			},
			wantURL: "https://example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123b",
			wantErr: false,
		},
		{
			name: "with schema",
			args: args{dsn: "token:supersecret2@example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123a?schema=system"},
			wantCfg: UserConfig{
				Protocol:      "https",
				Host:          "example.cloud.databricks.com",
				Port:          8000,
				MaxRows:       defaultMaxRows,
				Authenticator: &pat.PATAuth{AccessToken: "supersecret2"},
				HTTPPath:      "/sql/1.0/endpoints/12346a5b5b0e123a",
				Schema:        "system",
				SessionParams: make(map[string]string),
			},
			wantURL: "https://example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123a",
			wantErr: false,
		},
		{
			name: "with everything",
			args: args{dsn: "token:supersecret2@example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123a?catalog=default&schema=system&userAgentEntry=partner-name&timeout=100&maxRows=1000&ANSI_MODE=true"},
			wantCfg: UserConfig{
				Protocol:       "https",
				Host:           "example.cloud.databricks.com",
				Port:           8000,
				Authenticator:  &pat.PATAuth{AccessToken: "supersecret2"},
				HTTPPath:       "/sql/1.0/endpoints/12346a5b5b0e123a",
				QueryTimeout:   100 * time.Second,
				MaxRows:        1000,
				UserAgentEntry: "partner-name",
				Catalog:        "default",
				Schema:         "system",
				SessionParams:  map[string]string{"ANSI_MODE": "true"},
			},
			wantURL: "https://example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123a",
			wantErr: false,
		},
		{
			name: "missing http path",
			args: args{dsn: "token:supersecret@example.cloud.databricks.com:443"},
			wantCfg: UserConfig{
				Protocol:      "https",
				Host:          "example.cloud.databricks.com",
				Port:          443,
				MaxRows:       defaultMaxRows,
				Authenticator: &pat.PATAuth{AccessToken: "supersecret"},
				SessionParams: make(map[string]string),
			},
			wantURL: "https://example.cloud.databricks.com:443",
			wantErr: false,
		},
		{
			name: "missing http path",
			args: args{dsn: "token:supersecret2@example.cloud.databricks.com:443?catalog=default&schema=system&timeout=100&maxRows=1000"},
			wantCfg: UserConfig{
				Protocol:      "https",
				Host:          "example.cloud.databricks.com",
				Port:          443,
				Authenticator: &pat.PATAuth{AccessToken: "supersecret2"},
				QueryTimeout:  100 * time.Second,
				MaxRows:       1000,
				Catalog:       "default",
				Schema:        "system",
				SessionParams: make(map[string]string),
			},
			wantURL: "https://example.cloud.databricks.com:443",
			wantErr: false,
		},
		{
			name:    "with wrong port",
			args:    args{dsn: "token:supersecret2@example.cloud.databricks.com:foo/sql/1.0/endpoints/12346a5b5b0e123a?catalog=default&schema=system&timeout=100&maxRows=1000"},
			wantCfg: UserConfig{},
			wantErr: true,
		},
		{
			name:    "missing port",
			args:    args{dsn: "token:supersecret2@example.cloud.databricks.com?catalog=default&schema=system&timeout=100&maxRows=1000"},
			wantCfg: UserConfig{},
			wantErr: true,
		},
		{
			name:    "with wrong username",
			args:    args{dsn: "jim:supersecret2@example.cloud.databricks.com:443/sql/1.0/endpoints/12346a5b5b0e123a?catalog=default&schema=system&timeout=100&maxRows=1000"},
			wantCfg: UserConfig{},
			wantErr: true,
		},
		{
			name:    "with token but no secret",
			args:    args{dsn: "token@example.cloud.databricks.com:443/sql/1.0/endpoints/12346a5b5b0e123a?catalog=default&schema=system&timeout=100&maxRows=1000"},
			wantCfg: UserConfig{},
			wantErr: true,
		},

		{
			name: "missing host",
			args: args{dsn: "token:supersecret2@:443?catalog=default&schema=system&timeout=100&maxRows=1000"},
			wantCfg: UserConfig{
				Port:          443,
				Protocol:      "https",
				Authenticator: &pat.PATAuth{AccessToken: "supersecret2"},
				MaxRows:       1000,
				QueryTimeout:  100 * time.Second,
				Catalog:       "default",
				Schema:        "system",
				SessionParams: make(map[string]string),
			},
			wantURL: "https://:443",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseDSN(tt.args.dsn)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.wantCfg) {
				t.Errorf("ParseConfig() = %v, want %v", got, tt.wantCfg)
				return
			}
			if err == nil {
				cfg := &Config{UserConfig: got}
				gotUrl := cfg.ToEndpointURL()
				if gotUrl != tt.wantURL {
					t.Errorf("ToEndpointURL() = %v, want %v", got, tt.wantErr)
					return
				}
			}
		})
	}
}

func TestUserConfig_DeepCopy(t *testing.T) {
	t.Run("copy empty config", func(t *testing.T) {
		cfg := UserConfig{}

		cfg_copy := cfg.DeepCopy()
		if !reflect.DeepEqual(cfg, cfg_copy) {
			t.Errorf("DeepCopy() = %v, want %v", cfg_copy, cfg)
		}
	})
	t.Run("copy defaults config", func(t *testing.T) {
		cfg := UserConfig{}.WithDefaults()

		cfg_copy := cfg.DeepCopy()
		if !reflect.DeepEqual(cfg, cfg_copy) {
			t.Errorf("DeepCopy() = %v, want %v", cfg_copy, cfg)
		}
	})
	t.Run("copy config with all values", func(t *testing.T) {
		location, _ := time.LoadLocation("Asia/Seoul")
		cfg := UserConfig{
			Protocol:       "a",
			Host:           "b",
			Port:           2,
			HTTPPath:       "foo/bar",
			Catalog:        "cat",
			Schema:         "s",
			AccessToken:    "123",
			MaxRows:        10,
			QueryTimeout:   time.Minute,
			UserAgentEntry: "test",
			Location:       location,
			SessionParams:  map[string]string{"a": "32", "b": "4"},
		}

		cfg_copy := cfg.DeepCopy()
		if !reflect.DeepEqual(cfg, cfg_copy) {
			t.Errorf("DeepCopy() = %v, want %v", cfg_copy, cfg)
		}
	})
}

func TestConfig_DeepCopy(t *testing.T) {
	t.Run("copy empty config", func(t *testing.T) {
		cfg := &Config{}

		cfg_copy := cfg.DeepCopy()
		if !reflect.DeepEqual(cfg, cfg_copy) {
			t.Errorf("DeepCopy() = %v, want %v", cfg_copy, cfg)
		}
	})
	t.Run("copy nil config", func(t *testing.T) {
		var cfg *Config

		cfg_copy := cfg.DeepCopy()
		if !reflect.DeepEqual(cfg, cfg_copy) {
			t.Errorf("DeepCopy() = %v, want %v", cfg_copy, cfg)
		}
	})
	t.Run("copy defaults config", func(t *testing.T) {
		cfg := WithDefaults()

		cfg_copy := cfg.DeepCopy()
		if !reflect.DeepEqual(cfg, cfg_copy) {
			t.Errorf("DeepCopy() = %v, want %v", cfg_copy, cfg)
		}
	})
	t.Run("copy config with all values", func(t *testing.T) {
		cfg := &Config{
			UserConfig:                UserConfig{}.WithDefaults(),
			TLSConfig:                 &tls.Config{MinVersion: tls.VersionTLS12},
			RunAsync:                  true,
			PollInterval:              1 * time.Second,
			ConnectTimeout:            60 * time.Second,
			ClientTimeout:             900 * time.Second,
			PingTimeout:               15 * time.Second,
			CanUseMultipleCatalogs:    true,
			DriverName:                "godatabrickssqlconnector", //important. Do not change
			DriverVersion:             "0.9.0",
			ThriftProtocol:            "binary",
			ThriftTransport:           "http",
			ThriftProtocolVersion:     cli_service.TProtocolVersion_SPARK_CLI_SERVICE_PROTOCOL_V6,
			ThriftDebugClientProtocol: false,
		}

		cfg_copy := cfg.DeepCopy()
		if !reflect.DeepEqual(cfg, cfg_copy) {
			t.Errorf("DeepCopy() = %v, want %v", cfg_copy, cfg)
		}
	})
}
