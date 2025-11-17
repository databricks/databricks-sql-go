package config

import (
	"crypto/tls"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/databricks/databricks-sql-go/auth/noop"
	"github.com/databricks/databricks-sql-go/auth/oauth/m2m"
	"github.com/databricks/databricks-sql-go/auth/pat"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
)

func TestParseConfig(t *testing.T) {
	type args struct {
		dsn string
	}

	defCloudConfig := CloudFetchConfig{}.WithDefaults()

	tz, _ := time.LoadLocation("America/Vancouver")
	tests := []struct {
		name            string
		args            args
		wantCfg         UserConfig
		wantURL         string
		wantErr         bool
		wantEndpointErr bool
	}{
		{
			name: "base case",
			args: args{dsn: "token:supersecret@example.cloud.databricks.com:443/sql/1.0/endpoints/12346a5b5b0e123a"},
			wantCfg: UserConfig{
				Protocol:          "https",
				Host:              "example.cloud.databricks.com",
				Port:              443,
				MaxRows:           defaultMaxRows,
				Authenticator:     &pat.PATAuth{AccessToken: "supersecret"},
				AccessToken:       "supersecret",
				HTTPPath:          "/sql/1.0/endpoints/12346a5b5b0e123a",
				SessionParams:     make(map[string]string),
				RetryMax:          4,
				RetryWaitMin:      1 * time.Second,
				RetryWaitMax:      30 * time.Second,
				ExecutionProtocol: "thrift",
				CloudFetchConfig:  defCloudConfig,
			},
			wantURL: "https://example.cloud.databricks.com:443/sql/1.0/endpoints/12346a5b5b0e123a",
			wantErr: false,
		},
		{
			name: "with https scheme",
			args: args{dsn: "https://token:supersecret@example.cloud.databricks.com:443/sql/1.0/endpoints/12346a5b5b0e123a"},
			wantCfg: UserConfig{
				Protocol:         "https",
				Host:             "example.cloud.databricks.com",
				Port:             443,
				MaxRows:          defaultMaxRows,
				AccessToken:      "supersecret",
				Authenticator:    &pat.PATAuth{AccessToken: "supersecret"},
				HTTPPath:         "/sql/1.0/endpoints/12346a5b5b0e123a",
				SessionParams:    make(map[string]string),
				RetryMax:         4,
				RetryWaitMin:     1 * time.Second,
				RetryWaitMax:      30 * time.Second,
				ExecutionProtocol: "thrift",
				CloudFetchConfig:  defCloudConfig,
			},
			wantURL: "https://example.cloud.databricks.com:443/sql/1.0/endpoints/12346a5b5b0e123a",
			wantErr: false,
		},
		{
			name: "with http scheme",
			args: args{dsn: "http://localhost:8080/sql/1.0/endpoints/12346a5b5b0e123a"},
			wantCfg: UserConfig{
				Protocol:         "http",
				Host:             "localhost",
				Port:             8080,
				MaxRows:          defaultMaxRows,
				Authenticator:    &noop.NoopAuth{},
				HTTPPath:         "/sql/1.0/endpoints/12346a5b5b0e123a",
				SessionParams:    make(map[string]string),
				RetryMax:         4,
				RetryWaitMin:     1 * time.Second,
				RetryWaitMax:      30 * time.Second,
				ExecutionProtocol: "thrift",
				CloudFetchConfig:  defCloudConfig,
			},
			wantErr: false,
			wantURL: "http://localhost:8080/sql/1.0/endpoints/12346a5b5b0e123a",
		},
		{
			name: "with localhost",
			args: args{dsn: "http://localhost:8080"},
			wantCfg: UserConfig{
				Protocol:         "http",
				Host:             "localhost",
				Port:             8080,
				Authenticator:    &noop.NoopAuth{},
				MaxRows:          defaultMaxRows,
				SessionParams:    make(map[string]string),
				RetryMax:         4,
				RetryWaitMin:     1 * time.Second,
				RetryWaitMax:      30 * time.Second,
				ExecutionProtocol: "thrift",
				CloudFetchConfig:  defCloudConfig,
			},
			wantErr: false,
			wantURL: "http://localhost:8080",
		},
		{
			name: "with query params",
			args: args{dsn: "token:supersecret@example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123a?timeout=100&maxRows=1000"},
			wantCfg: UserConfig{
				Protocol:         "https",
				Host:             "example.cloud.databricks.com",
				Port:             8000,
				AccessToken:      "supersecret",
				Authenticator:    &pat.PATAuth{AccessToken: "supersecret"},
				HTTPPath:         "/sql/1.0/endpoints/12346a5b5b0e123a",
				QueryTimeout:     100 * time.Second,
				MaxRows:          1000,
				SessionParams:    make(map[string]string),
				RetryMax:         4,
				RetryWaitMin:     1 * time.Second,
				RetryWaitMax:      30 * time.Second,
				ExecutionProtocol: "thrift",
				CloudFetchConfig:  defCloudConfig,
			},
			wantURL: "https://example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123a",
			wantErr: false,
		},
		{
			name: "with query params and session params",
			args: args{dsn: "token:supersecret@example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123a?timeout=100&maxRows=1000&timezone=America/Vancouver&QUERY_TAGS=team:testing,driver:go"},
			wantCfg: UserConfig{
				Protocol:         "https",
				Host:             "example.cloud.databricks.com",
				Port:             8000,
				AccessToken:      "supersecret",
				Authenticator:    &pat.PATAuth{AccessToken: "supersecret"},
				HTTPPath:         "/sql/1.0/endpoints/12346a5b5b0e123a",
				QueryTimeout:     100 * time.Second,
				MaxRows:          1000,
				Location:         tz,
				SessionParams:    map[string]string{"timezone": "America/Vancouver", "QUERY_TAGS": "team:testing,driver:go"},
				RetryMax:         4,
				RetryWaitMin:     1 * time.Second,
				RetryWaitMax:      30 * time.Second,
				ExecutionProtocol: "thrift",
				CloudFetchConfig:  defCloudConfig,
			},
			wantURL: "https://example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123a",
			wantErr: false,
		},
		{
			name: "bare",
			args: args{dsn: "example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123a"},
			wantCfg: UserConfig{
				Protocol:         "https",
				Host:             "example.cloud.databricks.com",
				Authenticator:    &noop.NoopAuth{},
				Port:             8000,
				MaxRows:          defaultMaxRows,
				HTTPPath:         "/sql/1.0/endpoints/12346a5b5b0e123a",
				SessionParams:    make(map[string]string),
				RetryMax:         4,
				RetryWaitMin:     1 * time.Second,
				RetryWaitMax:      30 * time.Second,
				ExecutionProtocol: "thrift",
				CloudFetchConfig:  defCloudConfig,
			},
			wantURL: "https://example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123a",
			wantErr: false,
		},
		{
			name: "with catalog",
			args: args{dsn: "token:supersecret@example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123b?catalog=default"},
			wantCfg: UserConfig{
				Protocol:         "https",
				Host:             "example.cloud.databricks.com",
				Port:             8000,
				MaxRows:          defaultMaxRows,
				AccessToken:      "supersecret",
				Authenticator:    &pat.PATAuth{AccessToken: "supersecret"},
				HTTPPath:         "/sql/1.0/endpoints/12346a5b5b0e123b",
				Catalog:          "default",
				SessionParams:    make(map[string]string),
				RetryMax:         4,
				RetryWaitMin:     1 * time.Second,
				RetryWaitMax:      30 * time.Second,
				ExecutionProtocol: "thrift",
				CloudFetchConfig:  defCloudConfig,
			},
			wantURL: "https://example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123b",
			wantErr: false,
		},
		{
			name: "with user agent entry",
			args: args{dsn: "token:supersecret@example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123b?userAgentEntry=partner-name"},
			wantCfg: UserConfig{
				Protocol:         "https",
				Host:             "example.cloud.databricks.com",
				Port:             8000,
				MaxRows:          defaultMaxRows,
				AccessToken:      "supersecret",
				Authenticator:    &pat.PATAuth{AccessToken: "supersecret"},
				HTTPPath:         "/sql/1.0/endpoints/12346a5b5b0e123b",
				UserAgentEntry:   "partner-name",
				SessionParams:    make(map[string]string),
				RetryMax:         4,
				RetryWaitMin:     1 * time.Second,
				RetryWaitMax:      30 * time.Second,
				ExecutionProtocol: "thrift",
				CloudFetchConfig:  defCloudConfig,
			},
			wantURL: "https://example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123b",
			wantErr: false,
		},
		{
			name: "with schema",
			args: args{dsn: "token:supersecret2@example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123a?schema=system"},
			wantCfg: UserConfig{
				Protocol:         "https",
				Host:             "example.cloud.databricks.com",
				Port:             8000,
				MaxRows:          defaultMaxRows,
				AccessToken:      "supersecret2",
				Authenticator:    &pat.PATAuth{AccessToken: "supersecret2"},
				HTTPPath:         "/sql/1.0/endpoints/12346a5b5b0e123a",
				Schema:           "system",
				SessionParams:    make(map[string]string),
				RetryMax:         4,
				RetryWaitMin:     1 * time.Second,
				RetryWaitMax:      30 * time.Second,
				ExecutionProtocol: "thrift",
				CloudFetchConfig:  defCloudConfig,
			},
			wantURL: "https://example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123a",
			wantErr: false,
		},
		{
			name: "with useCloudFetch",
			args: args{dsn: "token:supersecret@example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123b?useCloudFetch=true"},
			wantCfg: UserConfig{
				Protocol:      "https",
				Host:          "example.cloud.databricks.com",
				Port:          8000,
				MaxRows:       defaultMaxRows,
				AccessToken:   "supersecret",
				Authenticator: &pat.PATAuth{AccessToken: "supersecret"},
				HTTPPath:      "/sql/1.0/endpoints/12346a5b5b0e123b",
				SessionParams: make(map[string]string),
				RetryMax:      4,
				RetryWaitMin:  1 * time.Second,
				RetryWaitMax:      30 * time.Second,
				ExecutionProtocol: "thrift",
				CloudFetchConfig: CloudFetchConfig{
					UseCloudFetch:                true,
					MaxDownloadThreads:           10,
					MaxFilesInMemory:             10,
					CloudFetchSpeedThresholdMbps: 0.1,
				},
			},
			wantURL: "https://example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123b",
			wantErr: false,
		},
		{
			name: "with useCloudFetch and maxDownloadThreads",
			args: args{dsn: "token:supersecret@example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123b?useCloudFetch=true&maxDownloadThreads=15"},
			wantCfg: UserConfig{
				Protocol:      "https",
				Host:          "example.cloud.databricks.com",
				Port:          8000,
				MaxRows:       defaultMaxRows,
				AccessToken:   "supersecret",
				Authenticator: &pat.PATAuth{AccessToken: "supersecret"},
				HTTPPath:      "/sql/1.0/endpoints/12346a5b5b0e123b",
				SessionParams: make(map[string]string),
				RetryMax:      4,
				RetryWaitMin:  1 * time.Second,
				RetryWaitMax:      30 * time.Second,
				ExecutionProtocol: "thrift",
				CloudFetchConfig: CloudFetchConfig{
					UseCloudFetch:                true,
					MaxDownloadThreads:           15,
					MaxFilesInMemory:             10,
					CloudFetchSpeedThresholdMbps: 0.1,
				},
			},
			wantURL: "https://example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123b",
			wantErr: false,
		},
		{
			name: "with everything",
			args: args{dsn: "token:supersecret2@example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123a?catalog=default&schema=system&userAgentEntry=partner-name&timeout=100&maxRows=1000&ANSI_MODE=true&useCloudFetch=true&maxDownloadThreads=15"},
			wantCfg: UserConfig{
				Protocol:       "https",
				Host:           "example.cloud.databricks.com",
				Port:           8000,
				AccessToken:    "supersecret2",
				Authenticator:  &pat.PATAuth{AccessToken: "supersecret2"},
				HTTPPath:       "/sql/1.0/endpoints/12346a5b5b0e123a",
				QueryTimeout:   100 * time.Second,
				MaxRows:        1000,
				UserAgentEntry: "partner-name",
				Catalog:        "default",
				Schema:         "system",
				SessionParams:  map[string]string{"ANSI_MODE": "true"},
				RetryMax:       4,
				RetryWaitMin:   1 * time.Second,
				RetryWaitMax:      30 * time.Second,
				ExecutionProtocol: "thrift",
				CloudFetchConfig: CloudFetchConfig{
					UseCloudFetch:                true,
					MaxDownloadThreads:           15,
					MaxFilesInMemory:             10,
					CloudFetchSpeedThresholdMbps: 0.1,
				},
			},
			wantURL: "https://example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123a",
			wantErr: false,
		},
		{
			name: "missing http path",
			args: args{dsn: "token:supersecret@example.cloud.databricks.com:443"},
			wantCfg: UserConfig{
				Protocol:         "https",
				Host:             "example.cloud.databricks.com",
				Port:             443,
				MaxRows:          defaultMaxRows,
				AccessToken:      "supersecret",
				Authenticator:    &pat.PATAuth{AccessToken: "supersecret"},
				SessionParams:    make(map[string]string),
				RetryMax:         4,
				RetryWaitMin:     1 * time.Second,
				RetryWaitMax:      30 * time.Second,
				ExecutionProtocol: "thrift",
				CloudFetchConfig:  defCloudConfig,
			},
			wantURL:         "https://example.cloud.databricks.com:443",
			wantErr:         false,
			wantEndpointErr: true,
		},
		{
			name: "missing http path 2",
			args: args{dsn: "token:supersecret2@example.cloud.databricks.com:443?catalog=default&schema=system&timeout=100&maxRows=1000"},
			wantCfg: UserConfig{
				Protocol:         "https",
				Host:             "example.cloud.databricks.com",
				Port:             443,
				AccessToken:      "supersecret2",
				Authenticator:    &pat.PATAuth{AccessToken: "supersecret2"},
				QueryTimeout:     100 * time.Second,
				MaxRows:          1000,
				Catalog:          "default",
				Schema:           "system",
				SessionParams:    make(map[string]string),
				RetryMax:         4,
				RetryWaitMin:     1 * time.Second,
				RetryWaitMax:      30 * time.Second,
				ExecutionProtocol: "thrift",
				CloudFetchConfig:  defCloudConfig,
			},
			wantURL:         "https://example.cloud.databricks.com:443",
			wantErr:         false,
			wantEndpointErr: true,
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
			name:    "with wrong maxRows",
			args:    args{dsn: "token:supersecret2@example.cloud.databricks.com:443/sql/1.0/endpoints/12346a5b5b0e123a?catalog=default&schema=system&timeout=100&maxRows=foo"},
			wantCfg: UserConfig{},
			wantErr: true,
		},
		{
			name:    "with wrong timeout",
			args:    args{dsn: "token:supersecret2@example.cloud.databricks.com:443/sql/1.0/endpoints/12346a5b5b0e123a?catalog=default&schema=system&timeout=foo&maxRows=100"},
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
				Port:             443,
				Protocol:         "https",
				AccessToken:      "supersecret2",
				Authenticator:    &pat.PATAuth{AccessToken: "supersecret2"},
				MaxRows:          1000,
				QueryTimeout:     100 * time.Second,
				Catalog:          "default",
				Schema:           "system",
				SessionParams:    make(map[string]string),
				RetryMax:         4,
				RetryWaitMin:     1 * time.Second,
				RetryWaitMax:      30 * time.Second,
				ExecutionProtocol: "thrift",
				CloudFetchConfig:  defCloudConfig,
			},
			wantURL:         "https://:443",
			wantErr:         false,
			wantEndpointErr: true,
		},
		{
			name: "with accessToken param",
			args: args{dsn: "example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123a?catalog=default&schema=system&userAgentEntry=partner-name&timeout=100&maxRows=1000&ANSI_MODE=true&accessToken=supersecret2"},
			wantCfg: UserConfig{
				Protocol:         "https",
				Host:             "example.cloud.databricks.com",
				Port:             8000,
				AccessToken:      "supersecret2",
				Authenticator:    &pat.PATAuth{AccessToken: "supersecret2"},
				HTTPPath:         "/sql/1.0/endpoints/12346a5b5b0e123a",
				QueryTimeout:     100 * time.Second,
				MaxRows:          1000,
				UserAgentEntry:   "partner-name",
				Catalog:          "default",
				Schema:           "system",
				SessionParams:    map[string]string{"ANSI_MODE": "true"},
				RetryMax:         4,
				RetryWaitMin:     1 * time.Second,
				RetryWaitMax:      30 * time.Second,
				ExecutionProtocol: "thrift",
				CloudFetchConfig:  defCloudConfig,
			},
			wantURL: "https://example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123a",
			wantErr: false,
		},
		{
			name: "with accessToken param and client id/secret params",
			args: args{dsn: "example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123a?catalog=default&schema=system&userAgentEntry=partner-name&timeout=100&maxRows=1000&ANSI_MODE=true&accessToken=supersecret2&clientId=client_id&clientSecret=client_secret"},
			wantCfg: UserConfig{
				Protocol:         "https",
				Host:             "example.cloud.databricks.com",
				Port:             8000,
				AccessToken:      "supersecret2",
				Authenticator:    &pat.PATAuth{AccessToken: "supersecret2"},
				HTTPPath:         "/sql/1.0/endpoints/12346a5b5b0e123a",
				QueryTimeout:     100 * time.Second,
				MaxRows:          1000,
				UserAgentEntry:   "partner-name",
				Catalog:          "default",
				Schema:           "system",
				SessionParams:    map[string]string{"ANSI_MODE": "true"},
				RetryMax:         4,
				RetryWaitMin:     1 * time.Second,
				RetryWaitMax:      30 * time.Second,
				ExecutionProtocol: "thrift",
				CloudFetchConfig:  defCloudConfig,
			},
			wantURL: "https://example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123a",
			wantErr: false,
		},
		{
			name: "authType unknown with accessTokenParam",
			args: args{dsn: "example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123a?authType=unknown&catalog=default&schema=system&userAgentEntry=partner-name&timeout=100&maxRows=1000&ANSI_MODE=true&accessToken=supersecret2&clientId=client_id&clientSecret=client_secret"},
			wantCfg: UserConfig{
				Protocol:         "https",
				Host:             "example.cloud.databricks.com",
				Port:             8000,
				AccessToken:      "supersecret2",
				Authenticator:    &pat.PATAuth{AccessToken: "supersecret2"},
				HTTPPath:         "/sql/1.0/endpoints/12346a5b5b0e123a",
				QueryTimeout:     100 * time.Second,
				MaxRows:          1000,
				UserAgentEntry:   "partner-name",
				Catalog:          "default",
				Schema:           "system",
				SessionParams:    map[string]string{"ANSI_MODE": "true"},
				RetryMax:         4,
				RetryWaitMin:     1 * time.Second,
				RetryWaitMax:      30 * time.Second,
				ExecutionProtocol: "thrift",
				CloudFetchConfig:  defCloudConfig,
			},
			wantURL: "https://example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123a",
			wantErr: false,
		},
		{
			name:    "client id no secret",
			args:    args{dsn: "example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123a?authType=unknown&clientId=client_id&catalog=default&schema=system&userAgentEntry=partner-name&timeout=100&maxRows=1000&ANSI_MODE=true"},
			wantCfg: UserConfig{},
			wantErr: true,
		},
		{
			name:    "client secret no id",
			args:    args{dsn: "example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123a?authType=unknown&clientSecret=client_secret&catalog=default&schema=system&userAgentEntry=partner-name&timeout=100&maxRows=1000&ANSI_MODE=true"},
			wantCfg: UserConfig{},
			wantErr: true,
		},
		{
			name:    "authType Pat, no accessToken",
			args:    args{dsn: "example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123a?authType=pat&clientSecret=client_secret&catalog=default&schema=system&userAgentEntry=partner-name&timeout=100&maxRows=1000&ANSI_MODE=true"},
			wantCfg: UserConfig{},
			wantErr: true,
		},
		{
			name:    "authType m2m, no id",
			args:    args{dsn: "example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123a?authType=oauthm2m&clientSecret=client_secret&catalog=default&schema=system&userAgentEntry=partner-name&timeout=100&maxRows=1000&ANSI_MODE=true"},
			wantCfg: UserConfig{},
			wantErr: true,
		},
		{
			name:    "authType m2m, no secret",
			args:    args{dsn: "example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123a?authType=oauthm2m&clientId=client_id&client_secret=&catalog=default&schema=system&userAgentEntry=partner-name&timeout=100&maxRows=1000&ANSI_MODE=true"},
			wantCfg: UserConfig{},
			wantErr: true,
		},
		{
			name: "authType unknown with client id/secret",
			args: args{dsn: "example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123a?authType=unknown&clientId=client_id&clientSecret=client_secret&catalog=default&schema=system&userAgentEntry=partner-name&timeout=100&maxRows=1000&ANSI_MODE=true"},
			wantCfg: UserConfig{
				Protocol:         "https",
				Host:             "example.cloud.databricks.com",
				Port:             8000,
				Authenticator:    m2m.NewAuthenticator("client_id", "client_secret", "example.cloud.databricks.com"),
				HTTPPath:         "/sql/1.0/endpoints/12346a5b5b0e123a",
				QueryTimeout:     100 * time.Second,
				MaxRows:          1000,
				UserAgentEntry:   "partner-name",
				Catalog:          "default",
				Schema:           "system",
				SessionParams:    map[string]string{"ANSI_MODE": "true"},
				RetryMax:         4,
				RetryWaitMin:     1 * time.Second,
				RetryWaitMax:      30 * time.Second,
				ExecutionProtocol: "thrift",
				CloudFetchConfig:  defCloudConfig,
			},
			wantURL: "https://example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123a",
			wantErr: false,
		},
		{
			name:    "authType m2m with accessToken",
			args:    args{dsn: "example.cloud.databricks.com:8000/sql/1.0/endpoints/12346a5b5b0e123a?authType=oauthm2m&accessToken=supersecret2&catalog=default&schema=system&userAgentEntry=partner-name&timeout=100&maxRows=1000&ANSI_MODE=true"},
			wantCfg: UserConfig{},
			wantErr: true,
		},
		{
			name: "with protocol=thrift",
			args: args{dsn: "token:supersecret@example.cloud.databricks.com:443/sql/1.0/endpoints/12346a5b5b0e123a?protocol=thrift"},
			wantCfg: UserConfig{
				Protocol:          "https",
				Host:              "example.cloud.databricks.com",
				Port:              443,
				MaxRows:           defaultMaxRows,
				Authenticator:     &pat.PATAuth{AccessToken: "supersecret"},
				AccessToken:       "supersecret",
				HTTPPath:          "/sql/1.0/endpoints/12346a5b5b0e123a",
				SessionParams:     make(map[string]string),
				RetryMax:          4,
				RetryWaitMin:      1 * time.Second,
				RetryWaitMax:      30 * time.Second,
				ExecutionProtocol: "thrift",
				CloudFetchConfig:  defCloudConfig,
			},
			wantURL: "https://example.cloud.databricks.com:443/sql/1.0/endpoints/12346a5b5b0e123a",
			wantErr: false,
		},
		{
			name: "with protocol=rest and warehouse_id",
			args: args{dsn: "token:supersecret@example.cloud.databricks.com:443/sql/1.0/endpoints/12346a5b5b0e123a?protocol=rest&warehouse_id=abc123def456"},
			wantCfg: UserConfig{
				Protocol:          "https",
				Host:              "example.cloud.databricks.com",
				Port:              443,
				MaxRows:           defaultMaxRows,
				Authenticator:     &pat.PATAuth{AccessToken: "supersecret"},
				AccessToken:       "supersecret",
				HTTPPath:          "/sql/1.0/endpoints/12346a5b5b0e123a",
				SessionParams:     make(map[string]string),
				RetryMax:          4,
				RetryWaitMin:      1 * time.Second,
				RetryWaitMax:      30 * time.Second,
				ExecutionProtocol: "rest",
				WarehouseID:       "abc123def456",
				CloudFetchConfig:  defCloudConfig,
			},
			wantURL: "https://example.cloud.databricks.com:443/sql/1.0/endpoints/12346a5b5b0e123a",
			wantErr: false,
		},
		{
			name: "with protocol=rest and warehouseId (camelCase)",
			args: args{dsn: "token:supersecret@example.cloud.databricks.com:443/sql/1.0/endpoints/12346a5b5b0e123a?protocol=rest&warehouseId=xyz789abc"},
			wantCfg: UserConfig{
				Protocol:          "https",
				Host:              "example.cloud.databricks.com",
				Port:              443,
				MaxRows:           defaultMaxRows,
				Authenticator:     &pat.PATAuth{AccessToken: "supersecret"},
				AccessToken:       "supersecret",
				HTTPPath:          "/sql/1.0/endpoints/12346a5b5b0e123a",
				SessionParams:     make(map[string]string),
				RetryMax:          4,
				RetryWaitMin:      1 * time.Second,
				RetryWaitMax:      30 * time.Second,
				ExecutionProtocol: "rest",
				WarehouseID:       "xyz789abc",
				CloudFetchConfig:  defCloudConfig,
			},
			wantURL: "https://example.cloud.databricks.com:443/sql/1.0/endpoints/12346a5b5b0e123a",
			wantErr: false,
		},
		{
			name: "with executionProtocol (alternative param name)",
			args: args{dsn: "token:supersecret@example.cloud.databricks.com:443/sql/1.0/endpoints/12346a5b5b0e123a?executionProtocol=rest&warehouse_id=test123"},
			wantCfg: UserConfig{
				Protocol:          "https",
				Host:              "example.cloud.databricks.com",
				Port:              443,
				MaxRows:           defaultMaxRows,
				Authenticator:     &pat.PATAuth{AccessToken: "supersecret"},
				AccessToken:       "supersecret",
				HTTPPath:          "/sql/1.0/endpoints/12346a5b5b0e123a",
				SessionParams:     make(map[string]string),
				RetryMax:          4,
				RetryWaitMin:      1 * time.Second,
				RetryWaitMax:      30 * time.Second,
				ExecutionProtocol: "rest",
				WarehouseID:       "test123",
				CloudFetchConfig:  defCloudConfig,
			},
			wantURL: "https://example.cloud.databricks.com:443/sql/1.0/endpoints/12346a5b5b0e123a",
			wantErr: false,
		},
		{
			name:    "with protocol=rest but no warehouse_id (should fail)",
			args:    args{dsn: "token:supersecret@example.cloud.databricks.com:443/sql/1.0/endpoints/12346a5b5b0e123a?protocol=rest"},
			wantCfg: UserConfig{},
			wantErr: true,
		},
		{
			name: "with warehouse_id but thrift protocol (should succeed)",
			args: args{dsn: "token:supersecret@example.cloud.databricks.com:443/sql/1.0/endpoints/12346a5b5b0e123a?protocol=thrift&warehouse_id=abc123"},
			wantCfg: UserConfig{
				Protocol:          "https",
				Host:              "example.cloud.databricks.com",
				Port:              443,
				MaxRows:           defaultMaxRows,
				Authenticator:     &pat.PATAuth{AccessToken: "supersecret"},
				AccessToken:       "supersecret",
				HTTPPath:          "/sql/1.0/endpoints/12346a5b5b0e123a",
				SessionParams:     make(map[string]string),
				RetryMax:          4,
				RetryWaitMin:      1 * time.Second,
				RetryWaitMax:      30 * time.Second,
				ExecutionProtocol: "thrift",
				WarehouseID:       "abc123",
				CloudFetchConfig:  defCloudConfig,
			},
			wantURL: "https://example.cloud.databricks.com:443/sql/1.0/endpoints/12346a5b5b0e123a",
			wantErr: false,
		},
		{
			name: "default protocol is thrift (backward compatibility)",
			args: args{dsn: "token:supersecret@example.cloud.databricks.com:443/sql/1.0/endpoints/12346a5b5b0e123a"},
			wantCfg: UserConfig{
				Protocol:          "https",
				Host:              "example.cloud.databricks.com",
				Port:              443,
				MaxRows:           defaultMaxRows,
				Authenticator:     &pat.PATAuth{AccessToken: "supersecret"},
				AccessToken:       "supersecret",
				HTTPPath:          "/sql/1.0/endpoints/12346a5b5b0e123a",
				SessionParams:     make(map[string]string),
				RetryMax:          4,
				RetryWaitMin:      1 * time.Second,
				RetryWaitMax:      30 * time.Second,
				ExecutionProtocol: "thrift",
				CloudFetchConfig:  defCloudConfig,
			},
			wantURL: "https://example.cloud.databricks.com:443/sql/1.0/endpoints/12346a5b5b0e123a",
			wantErr: false,
		},
	}
	for i, tt := range tests {
		fmt.Println(i)
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
				gotUrl, err := cfg.ToEndpointURL()
				if (err != nil) != tt.wantEndpointErr {
					t.Errorf("ParseConfig() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
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
			PollInterval:              1 * time.Second,
			MaxPollInterval:           60 * time.Second,
			PollBackoffMultiplier:     2.0,
			ClientTimeout:             900 * time.Second,
			PingTimeout:               15 * time.Second,
			CanUseMultipleCatalogs:    true,
			DriverName:                "godatabrickssqlconnector", //important. Do not change
			DriverVersion:             "0.9.0",
			ThriftProtocol:            "binary",
			ThriftTransport:           "http",
			ThriftProtocolVersion:     cli_service.TProtocolVersion_SPARK_CLI_SERVICE_PROTOCOL_V8,
			ThriftDebugClientProtocol: false,
		}

		cfg_copy := cfg.DeepCopy()
		if !reflect.DeepEqual(cfg, cfg_copy) {
			t.Errorf("DeepCopy() = %v, want %v", cfg_copy, cfg)
		}
	})
}

func TestConfig_WithDefaults(t *testing.T) {
	t.Run("default values for polling configuration", func(t *testing.T) {
		cfg := WithDefaults()

		if cfg.PollInterval != 1*time.Second {
			t.Errorf("PollInterval = %v, want %v", cfg.PollInterval, 1*time.Second)
		}
		if cfg.MaxPollInterval != 60*time.Second {
			t.Errorf("MaxPollInterval = %v, want %v", cfg.MaxPollInterval, 60*time.Second)
		}
		if cfg.PollBackoffMultiplier != 2.0 {
			t.Errorf("PollBackoffMultiplier = %v, want %v", cfg.PollBackoffMultiplier, 2.0)
		}
	})

	t.Run("default execution protocol is thrift", func(t *testing.T) {
		cfg := WithDefaults()

		if cfg.ExecutionProtocol != "thrift" {
			t.Errorf("ExecutionProtocol = %v, want %v", cfg.ExecutionProtocol, "thrift")
		}
	})
}
