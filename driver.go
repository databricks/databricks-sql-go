package dbsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
)

func init() {
	sql.Register("databricks", &databricksDriver{})
}

const (
	DriverName    = "godatabrickssqlconnector" //important. Do not change
	DriverVersion = "0.9.0"
)

type databricksDriver struct{}

func (d *databricksDriver) Open(uri string) (driver.Conn, error) {
	cfg, err := parseURI(uri)
	if err != nil {
		return nil, err
	}
	c := &connector{
		cfg: cfg,
	}
	return c.Connect(context.Background())
}

func (d *databricksDriver) OpenConnector(uri string) (driver.Connector, error) {
	cfg, err := parseURI(uri)
	return &connector{cfg}, err
}

var _ driver.Driver = (*databricksDriver)(nil)
var _ driver.DriverContext = (*databricksDriver)(nil)

type connOption func(*config)

func NewConnector(options ...connOption) (driver.Connector, error) {
	// config with default options
	cfg := newConfigWithDefaults()

	for _, opt := range options {
		opt(cfg)
	}
	// validate config?

	return &connector{cfg}, nil
}

func WithServerHostname(host string) connOption {
	return func(c *config) {
		c.Host = host
	}
}

func WithPort(port int) connOption {
	return func(c *config) {
		c.Port = port
	}
}

func WithAccessToken(token string) connOption {
	return func(c *config) {
		c.AccessToken = token
	}
}

func WithHTTPPath(path string) connOption {
	return func(c *config) {
		c.HTTPPath = path
	}
}

func WithMaxRows(n int) connOption {
	return func(c *config) {
		if n != 0 {
			c.MaxRows = n
		}
	}
}

// This will add a timeout for the server execution.
// In seconds.
func WithTimeout(n int) connOption {
	return func(c *config) {
		c.TimeoutSeconds = n
	}
}
