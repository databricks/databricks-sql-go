package config

import (
	"crypto/tls"
	"fmt"
	"net/url"

	"github.com/databricks/databricks-sql-go/internal/cli_service"
)

type Config struct {
	Host          string // from databricks UI
	Port          int    // from databricks UI
	Catalog       *string
	Schema        *string
	AccessToken   string      // from databricks UI
	TLSConfig     *tls.Config // nil disables TLS. Is it needed?
	Protocol      string      // defaults to https. From databricks UI
	HTTPPath      string      // from databricks UI
	Authenticator string      //TODO for oauth

	RunAsync       bool // TODO
	MaxRows        int  // TODO
	TimeoutSeconds int  // There are several timeouts that can be possibly configurable
	UserAgentEntry string
	Thrift         *ThriftConfig
	DriverName     string
	DriverVersion  string
}

// Thrift config sets several low level configurations. Change with care.
type ThriftConfig struct {
	Protocol            string
	Transport           string
	ProtocolVersion     cli_service.TProtocolVersion
	DebugClientProtocol bool
}

func strPtr(s string) *string {
	return &s
}

func WithDefaults() *Config {
	return &Config{
		Port:     443,
		MaxRows:  10000,
		Catalog:  strPtr("default"),
		Protocol: "https",
		RunAsync: true,
		Thrift: &ThriftConfig{
			Protocol:        "binary",
			Transport:       "http",
			ProtocolVersion: cli_service.TProtocolVersion_SPARK_CLI_SERVICE_PROTOCOL_V6,
		},
		DriverName:    "godatabrickssqlconnector", //important. Do not change
		DriverVersion: "0.9.0",
	}
}

func ParseURI(uri string) (*Config, error) {
	parsedURL, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}
	cfg := &Config{}
	cfg.Host = parsedURL.Hostname()
	// cfg.Port =  parsedURL.Port()
	// userinfo := parsedURL.User.Password()
	// cfg.AccessToken = userinfo
	return cfg, fmt.Errorf("not implemented")
}

func (c *Config) ToEndpointURL() string {
	endpointUrl := fmt.Sprintf("%s://%s:%s@%s:%d%s", c.Protocol, "token", url.QueryEscape(c.AccessToken), c.Host, c.Port, c.HTTPPath)
	return endpointUrl
}

func (c *Config) DeepCopy() *Config {
	if c == nil {
		return nil
	}

	return &Config{
		Host:           c.Host,
		Port:           c.Port,
		Catalog:        c.Catalog,
		Schema:         c.Schema,
		AccessToken:    c.AccessToken,
		TLSConfig:      c.TLSConfig.Clone(),
		Protocol:       c.Protocol,
		HTTPPath:       c.HTTPPath,
		Authenticator:  c.Authenticator,
		RunAsync:       c.RunAsync,
		MaxRows:        c.MaxRows,
		TimeoutSeconds: c.TimeoutSeconds,
		UserAgentEntry: c.UserAgentEntry,
		Thrift: &ThriftConfig{
			Protocol:            c.Thrift.Protocol,
			Transport:           c.Thrift.Transport,
			ProtocolVersion:     c.Thrift.ProtocolVersion,
			DebugClientProtocol: c.Thrift.DebugClientProtocol,
		},
		DriverName:    c.DriverName,
		DriverVersion: c.DriverVersion,
	}
}
