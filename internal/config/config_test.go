package config

import (
	"reflect"
	"testing"
)

func TestParseConfig(t *testing.T) {
	type args struct {
		dsn string
	}
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
				Protocol:    "https",
				Host:        "example.cloud.databricks.com",
				Port:        443,
				AccessToken: "supersecret",
				HTTPPath:    "/sql/1.0/endpoints/12346a5b5b0e123a",
			},
			wantURL: "https://token:supersecret@example.cloud.databricks.com:443/sql/1.0/endpoints/12346a5b5b0e123a",
			wantErr: false,
		},
		{
			name: "with https scheme",
			args: args{dsn: "https://token:supersecret@example.cloud.databricks.com:443/sql/1.0/endpoints/12346a5b5b0e123a"},
			wantCfg: UserConfig{
				Protocol:    "https",
				Host:        "example.cloud.databricks.com",
				Port:        443,
				AccessToken: "supersecret",
				HTTPPath:    "/sql/1.0/endpoints/12346a5b5b0e123a",
			},
			wantURL: "https://token:supersecret@example.cloud.databricks.com:443/sql/1.0/endpoints/12346a5b5b0e123a",
			wantErr: false,
		},
		{
			name: "with http scheme",
			args: args{dsn: "http://localhost:8080/sql/1.0/endpoints/12346a5b5b0e123a"},
			wantCfg: UserConfig{
				Protocol: "http",
				Host:     "localhost",
				Port:     8080,
				HTTPPath: "/sql/1.0/endpoints/12346a5b5b0e123a",
			},
			wantErr: false,
			wantURL: "http://localhost:8080/sql/1.0/endpoints/12346a5b5b0e123a",
		},
		{
			name: "with localhost",
			args: args{dsn: "http://localhost:8080"},
			wantCfg: UserConfig{
				Protocol: "http",
				Host:     "localhost",
				Port:     8080,
			},
			wantErr: false,
			wantURL: "http://localhost:8080",
		},
		{
			name: "with query params",
			args: args{dsn: "token:supersecret@example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123a?timeout=100&maxRows=1000"},
			wantCfg: UserConfig{
				Protocol:       "https",
				Host:           "example.cloud.databricks.com",
				Port:           8000,
				AccessToken:    "supersecret",
				HTTPPath:       "/sql/1.0/endpoints/12346a5b5b0e123a",
				TimeoutSeconds: 100,
				MaxRows:        1000,
			},
			wantURL: "https://token:supersecret@example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123a",
			wantErr: false,
		},
		{
			name: "with query params and session params",
			args: args{dsn: "token:supersecret@example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123a?timeout=100&maxRows=1000&timezone=PST"},
			wantCfg: UserConfig{
				Protocol:       "https",
				Host:           "example.cloud.databricks.com",
				Port:           8000,
				AccessToken:    "supersecret",
				HTTPPath:       "/sql/1.0/endpoints/12346a5b5b0e123a",
				TimeoutSeconds: 100,
				MaxRows:        1000,
				SessionParams:  map[string]string{"timezone": "PST"},
			},
			wantURL: "https://token:supersecret@example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123a",
			wantErr: false,
		},
		{
			name: "bare",
			args: args{dsn: "example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123a"},
			wantCfg: UserConfig{
				Protocol: "https",
				Host:     "example.cloud.databricks.com",
				Port:     8000,
				HTTPPath: "/sql/1.0/endpoints/12346a5b5b0e123a",
			},
			wantURL: "https://example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123a",
			wantErr: false,
		},
		{
			name: "with catalog",
			args: args{dsn: "token:supersecret@example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123b?catalog=default"},
			wantCfg: UserConfig{
				Protocol:    "https",
				Host:        "example.cloud.databricks.com",
				Port:        8000,
				AccessToken: "supersecret",
				HTTPPath:    "/sql/1.0/endpoints/12346a5b5b0e123b",
				Catalog:     "default",
			},
			wantURL: "https://token:supersecret@example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123b",
			wantErr: false,
		},
		{
			name: "with schema",
			args: args{dsn: "token:supersecret2@example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123a?schema=system"},
			wantCfg: UserConfig{
				Protocol:    "https",
				Host:        "example.cloud.databricks.com",
				Port:        8000,
				AccessToken: "supersecret2",
				HTTPPath:    "/sql/1.0/endpoints/12346a5b5b0e123a",
				Schema:      "system",
			},
			wantURL: "https://token:supersecret2@example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123a",
			wantErr: false,
		},
		{
			name: "with everything",
			args: args{dsn: "token:supersecret2@example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123a?catalog=default&schema=system&timeout=100&maxRows=1000&ANSI_MODE=true"},
			wantCfg: UserConfig{
				Protocol:       "https",
				Host:           "example.cloud.databricks.com",
				Port:           8000,
				AccessToken:    "supersecret2",
				HTTPPath:       "/sql/1.0/endpoints/12346a5b5b0e123a",
				TimeoutSeconds: 100,
				MaxRows:        1000,
				Catalog:        "default",
				Schema:         "system",
				SessionParams:  map[string]string{"ANSI_MODE": "true"},
			},
			wantURL: "https://token:supersecret2@example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123a",
			wantErr: false,
		},
		{
			name: "missing http path",
			args: args{dsn: "token:supersecret@example.cloud.databricks.com:443"},
			wantCfg: UserConfig{
				Protocol:    "https",
				Host:        "example.cloud.databricks.com",
				Port:        443,
				AccessToken: "supersecret",
			},
			wantURL: "https://token:supersecret@example.cloud.databricks.com:443",
			wantErr: false,
		},
		{
			name: "missing http path",
			args: args{dsn: "token:supersecret2@example.cloud.databricks.com:443?catalog=default&schema=system&timeout=100&maxRows=1000"},
			wantCfg: UserConfig{
				Protocol:       "https",
				Host:           "example.cloud.databricks.com",
				Port:           443,
				AccessToken:    "supersecret2",
				TimeoutSeconds: 100,
				MaxRows:        1000,
				Catalog:        "default",
				Schema:         "system",
			},
			wantURL: "https://token:supersecret2@example.cloud.databricks.com:443",
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
				Port:           443,
				Protocol:       "https",
				AccessToken:    "supersecret2",
				MaxRows:        1000,
				TimeoutSeconds: 100,
				Catalog:        "default",
				Schema:         "system",
			},
			wantURL: "https://token:supersecret2@:443",
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
