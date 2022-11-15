package config

import (
	"crypto/tls"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/pkg/errors"
)

// Driver Configurations
// Only UserConfig are currently exposed to users
type Config struct {
	UserConfig
	TLSConfig     *tls.Config // nil disables TLS
	Authenticator string      //TODO for oauth

	RunAsync                  bool // TODO
	PollInterval              time.Duration
	DefaultTimeout            time.Duration
	CanUseMultipleCatalogs    bool
	DriverName                string
	DriverVersion             string
	ThriftProtocol            string
	ThriftTransport           string
	ThriftProtocolVersion     cli_service.TProtocolVersion
	ThriftDebugClientProtocol bool
}

func (c *Config) ToEndpointURL() string {
	var userInfo string
	if c.AccessToken != "" {
		userInfo = fmt.Sprintf("%s:%s@", "token", url.QueryEscape(c.AccessToken))
	}
	endpointUrl := fmt.Sprintf("%s://%s%s:%d%s", c.Protocol, userInfo, c.Host, c.Port, c.HTTPPath)
	return endpointUrl
}

func (c *Config) DeepCopy() *Config {
	if c == nil {
		return nil
	}

	return &Config{
		UserConfig:     c.UserConfig.DeepCopy(),
		TLSConfig:      c.TLSConfig.Clone(),
		Authenticator:  c.Authenticator,
		RunAsync:       c.RunAsync,
		PollInterval:   c.PollInterval,
		DefaultTimeout: c.DefaultTimeout,

		CanUseMultipleCatalogs:    c.CanUseMultipleCatalogs,
		ThriftProtocol:            c.ThriftProtocol,
		ThriftTransport:           c.ThriftTransport,
		ThriftProtocolVersion:     c.ThriftProtocolVersion,
		ThriftDebugClientProtocol: c.ThriftDebugClientProtocol,
		DriverName:                c.DriverName,
		DriverVersion:             c.DriverVersion,
	}
}

// UserConfig is the set of configurations exposed to users
type UserConfig struct {
	Protocol            string
	Host                string // from databricks UI
	Port                int    // from databricks UI
	HTTPPath            string // from databricks UI
	Catalog             string
	Schema              string
	AccessToken         string // from databricks UI
	MaxRows             int    // TODO
	QueryTimeoutSeconds int    // There are several timeouts that can be possibly configurable
	UserAgentEntry      string
	Location            *time.Location
	SessionParams       map[string]string
}

func (ucfg UserConfig) DeepCopy() UserConfig {
	sessionParams := make(map[string]string)
	for k, v := range ucfg.SessionParams {
		sessionParams[k] = v
	}
	return UserConfig{
		Protocol:            ucfg.Protocol,
		Host:                ucfg.Host,
		Port:                ucfg.Port,
		HTTPPath:            ucfg.HTTPPath,
		Catalog:             ucfg.Catalog,
		Schema:              ucfg.Schema,
		AccessToken:         ucfg.AccessToken,
		MaxRows:             ucfg.MaxRows,
		QueryTimeoutSeconds: ucfg.QueryTimeoutSeconds,
		UserAgentEntry:      ucfg.UserAgentEntry,
		SessionParams:       sessionParams,
	}
}

func (ucfg UserConfig) FillDefaults() UserConfig {
	if ucfg.MaxRows == 0 {
		ucfg.MaxRows = 10000
	}
	if ucfg.Protocol == "" {
		ucfg.Protocol = "https"
	}
	ucfg.SessionParams = make(map[string]string)
	return ucfg
}

func WithDefaults() *Config {
	return &Config{
		UserConfig:             UserConfig{}.FillDefaults(),
		RunAsync:               true,
		CanUseMultipleCatalogs: true,
		PollInterval:           1 * time.Second,
		DefaultTimeout:         60 * time.Second,
		ThriftProtocol:         "binary",
		ThriftTransport:        "http",
		ThriftProtocolVersion:  cli_service.TProtocolVersion_SPARK_CLI_SERVICE_PROTOCOL_V6,
		DriverName:             "godatabrickssqlconnector", //important. Do not change
		DriverVersion:          "0.9.0",
		TLSConfig:              &tls.Config{MinVersion: tls.VersionTLS12},
	}

}

func ParseDSN(dsn string) (UserConfig, error) {
	fullDSN := dsn
	if !strings.HasPrefix(dsn, "https://") && !strings.HasPrefix(dsn, "http://") {
		fullDSN = "https://" + dsn
	}
	parsedURL, err := url.Parse(fullDSN)
	if err != nil {
		return UserConfig{}, errors.Wrap(err, "invalid DSN: invalid format")
	}
	ucfg := UserConfig{}
	ucfg.Protocol = parsedURL.Scheme
	ucfg.Host = parsedURL.Hostname()
	port, err := strconv.Atoi(parsedURL.Port())
	if err != nil {
		return UserConfig{}, errors.Wrap(err, "invalid DSN: invalid DSN port")
	}
	ucfg.Port = port
	name := parsedURL.User.Username()
	if name == "token" {
		pass, ok := parsedURL.User.Password()
		if ok {
			ucfg.AccessToken = pass
		} else {
			return UserConfig{}, errors.New("invalid DSN: token not set")
		}
	} else {
		if name != "" {
			return UserConfig{}, errors.New("invalid DSN: basic auth not enabled")
		}
	}
	ucfg.HTTPPath = parsedURL.Path
	params := parsedURL.Query()
	maxRowsStr := params.Get("maxRows")
	if maxRowsStr != "" {
		maxRows, err := strconv.Atoi(maxRowsStr)
		if err != nil {
			return UserConfig{}, errors.Wrap(err, "invalid DSN: maxRows param is not an integer")
		}
		ucfg.MaxRows = maxRows
	}
	params.Del("maxRows")

	timeoutStr := params.Get("timeout")
	if timeoutStr != "" {
		timeout, err := strconv.Atoi(timeoutStr)
		if err != nil {
			return UserConfig{}, errors.Wrap(err, "invalid DSN: timeout param is not an integer")
		}
		ucfg.QueryTimeoutSeconds = timeout
	}
	params.Del("timeout")
	if params.Has("catalog") {
		ucfg.Catalog = params.Get("catalog")
		params.Del("catalog")
	}
	if params.Has("schema") {
		ucfg.Schema = params.Get("schema")
		params.Del("schema")
	}
	if params.Has("timezone") {
		tz := params.Get("timezone")
		ucfg.Location, err = time.LoadLocation(tz)
	}
	if len(params) > 0 {
		sessionParams := make(map[string]string)
		for k := range params {
			sessionParams[k] = params.Get(k)
		}
		ucfg.SessionParams = sessionParams

	}

	return ucfg, err
}
