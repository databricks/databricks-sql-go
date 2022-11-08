package dbsql

import (
	"context"
	"database/sql/driver"
	"fmt"

	utils "github.com/databricks/databricks-sql-go/internal"
	ts "github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
)

// TODO this is the thrift connector. We could have many implementations
type connector struct {
	cfg *config.Config
}

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {

	tclient, err := client.InitThriftClient(c.cfg)
	if err != nil {
		return nil, fmt.Errorf("databricks: error initializing thrift client. %w", err)
	}
	t := true
	var catalogName ts.TIdentifier
	var schemaName ts.TIdentifier
	if c.cfg.Catalog != "" {
		catalogName = ts.TIdentifier(c.cfg.Catalog)
	}
	if c.cfg.Schema != "" {
		schemaName = ts.TIdentifier(c.cfg.Schema)
	}

	req := ts.TOpenSessionReq{
		ClientProtocol: c.cfg.ThriftProtocolVersion,
		Configuration:  make(map[string]string),
		InitialNamespace: &ts.TNamespace{
			CatalogName: &catalogName,
			SchemaName:  &schemaName,
		},
		CanUseMultipleCatalogs: &t,
	}

	session, err := tclient.OpenSession(ctx, &req)
	if err != nil {
		return nil, err
	}

	fmt.Printf("open session: %s\n", utils.Guid(session.SessionHandle.GetSessionId().GUID))
	fmt.Printf("session config: %v\n", session.Configuration)
	conn := &conn{
		cfg:     c.cfg,
		client:  tclient,
		session: session,
	}
	return conn, nil
}

func (c *connector) Driver() driver.Driver {
	return &databricksDriver{}
}

var _ driver.Connector = (*connector)(nil)

type connOption func(*config.Config)

func NewConnector(options ...connOption) (driver.Connector, error) {
	// config with default options
	cfg := config.WithDefaults()

	for _, opt := range options {
		opt(cfg)
	}
	// validate config?

	return &connector{cfg}, nil
}

func WithServerHostname(host string) connOption {
	return func(c *config.Config) {
		c.Host = host
	}
}

func WithPort(port int) connOption {
	return func(c *config.Config) {
		c.Port = port
	}
}

func WithAccessToken(token string) connOption {
	return func(c *config.Config) {
		c.AccessToken = token
	}
}

func WithHTTPPath(path string) connOption {
	return func(c *config.Config) {
		c.HTTPPath = path
	}
}

func WithMaxRows(n int) connOption {
	return func(c *config.Config) {
		if n != 0 {
			c.MaxRows = n
		}
	}
}

// This will add a timeout for the server execution.
// In seconds.
func WithTimeout(n int) connOption {
	return func(c *config.Config) {
		c.TimeoutSeconds = n
	}
}

func WithInitialNamespace(catalog, schema string) connOption {
	return func(c *config.Config) {
		c.Catalog = catalog
		c.Schema = schema
	}
}

func WithUserAgentEntry(entry string) connOption {
	return func(c *config.Config) {
		c.UserAgentEntry = entry
	}

}
