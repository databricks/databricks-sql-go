package dbsql

import (
	"crypto/tls"
	"fmt"
	"net/url"
	"time"

	"github.com/databricks/databricks-sql-go/internal/cli_service"
)

type config struct {
	Host           string // from databricks UI
	Port           int    // from databricks UI
	Catalog        string //??
	Database       string
	AccessToken    string      // from databricks UI
	TLSConfig      *tls.Config // nil disables TLS. Is it needed?
	ConnectTimeout time.Duration
	Protocol       string // defaults to https. From databricks UI
	HTTPPath       string // from databricks UI
	Authenticator  string //TODO for oauth

	RunAsync       bool // TODO
	MaxRows        int  // TODO
	TimeoutSeconds int  // There are several timeouts that can be possibly configurable
	UserAgentEntry string
	Thrift         *thriftConfig
}

// Thrift config sets several low level configurations. Change with care.
type thriftConfig struct {
	Protocol            string
	Transport           string
	ProtocolVersion     cli_service.TProtocolVersion
	DebugClientProtocol bool
}

func newConfigWithDefaults() *config {
	return &config{
		Port:     443,
		MaxRows:  10000,
		Database: "default",
		Protocol: "https",
		Thrift: &thriftConfig{
			Protocol:        "binary",
			Transport:       "http",
			ProtocolVersion: cli_service.TProtocolVersion_SPARK_CLI_SERVICE_PROTOCOL_V6,
		},
	}
}

func parseURI(uri string) (*config, error) {
	parsedURL, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}
	cfg := &config{}
	cfg.Host = parsedURL.Hostname()
	// cfg.Port =  parsedURL.Port()
	// userinfo := parsedURL.User.Password()
	// cfg.AccessToken = userinfo
	return cfg, ErrNotImplemented
}

func (c *config) ToEndpointURL() string {
	endpointUrl := fmt.Sprintf("%s://%s:%s@%s:%d%s", c.Protocol, "token", url.QueryEscape(c.AccessToken), c.Host, c.Port, c.HTTPPath)
	return endpointUrl
}

func (c *config) DeepCopy() *config {
	if c == nil {
		return nil
	}

	return &config{
		Host:           c.Host,
		Port:           c.Port,
		Catalog:        c.Catalog,
		Database:       c.Database,
		AccessToken:    c.AccessToken,
		TLSConfig:      c.TLSConfig.Clone(),
		ConnectTimeout: c.ConnectTimeout,
		Protocol:       c.Protocol,
		HTTPPath:       c.HTTPPath,
		Authenticator:  c.Authenticator,
		RunAsync:       c.RunAsync,
		MaxRows:        c.MaxRows,
		TimeoutSeconds: c.TimeoutSeconds,
		UserAgentEntry: c.UserAgentEntry,
		Thrift: &thriftConfig{
			Protocol:            c.Thrift.Protocol,
			Transport:           c.Thrift.Transport,
			ProtocolVersion:     c.Thrift.ProtocolVersion,
			DebugClientProtocol: c.Thrift.DebugClientProtocol,
		},
	}
}
