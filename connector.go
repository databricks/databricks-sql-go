package dbsql

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strings"
	"time"

	"github.com/databricks/databricks-sql-go/driverctx"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/databricks/databricks-sql-go/logger"
)

type connector struct {
	cfg *config.Config
}

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	var catalogName *cli_service.TIdentifier
	var schemaName *cli_service.TIdentifier
	if c.cfg.Catalog != "" {
		catalogName = cli_service.TIdentifierPtr(cli_service.TIdentifier(c.cfg.Catalog))
	}
	if c.cfg.Schema != "" {
		schemaName = cli_service.TIdentifierPtr(cli_service.TIdentifier(c.cfg.Schema))
	}

	tclient, err := client.InitThriftClient(c.cfg)
	if err != nil {
		return nil, wrapErr(err, "error initializing thrift client")
	}

	// we need to ensure that open session will eventually end
	session, err := tclient.OpenSession(ctx, &cli_service.TOpenSessionReq{
		ClientProtocol: c.cfg.ThriftProtocolVersion,
		Configuration:  make(map[string]string),
		InitialNamespace: &cli_service.TNamespace{
			CatalogName: catalogName,
			SchemaName:  schemaName,
		},
		CanUseMultipleCatalogs: &c.cfg.CanUseMultipleCatalogs,
	})

	if err != nil {
		return nil, wrapErrf(err, "error connecting: host=%s port=%d, httpPath=%s", c.cfg.Host, c.cfg.Port, c.cfg.HTTPPath)
	}

	conn := &conn{
		id:      client.SprintGuid(session.SessionHandle.GetSessionId().GUID),
		cfg:     c.cfg,
		client:  tclient,
		session: session,
	}
	log := logger.WithContext(conn.id, driverctx.CorrelationIdFromContext(ctx), "")

	log.Info().Msgf("connect: host=%s port=%d httpPath=%s", c.cfg.Host, c.cfg.Port, c.cfg.HTTPPath)

	for k, v := range c.cfg.SessionParams {
		setStmt := fmt.Sprintf("SET `%s` = `%s`;", k, v)
		_, err := conn.ExecContext(ctx, setStmt, []driver.NamedValue{})
		if err != nil {
			return nil, err
		}
		log.Info().Msgf("set session parameter: param=%s value=%s", k, v)
	}
	return conn, nil
}

func (c *connector) Driver() driver.Driver {
	return &databricksDriver{}
}

var _ driver.Connector = (*connector)(nil)

type connOption func(*config.Config)

// NewConnector creates a connection that can be used with sql.OpenDB().
// This is an easier way to set up the DB instead of having to construct a DSN string.
func NewConnector(options ...connOption) (driver.Connector, error) {
	// config with default options
	cfg := config.WithDefaults()

	for _, opt := range options {
		opt(cfg)
	}

	return &connector{cfg: cfg}, nil
}

// WithServerHostname sets up the server hostname. Mandatory.
func WithServerHostname(host string) connOption {
	return func(c *config.Config) {
		if host == "localhost" {
			c.Protocol = "http"
		}
		c.Host = host
	}
}

// WithPort sets up the server port. Mandatory.
func WithPort(port int) connOption {
	return func(c *config.Config) {
		c.Port = port
	}
}

// WithAccessToken sets up the Personal Access Token. Mandatory for now.
func WithAccessToken(token string) connOption {
	return func(c *config.Config) {
		c.AccessToken = token
	}
}

// WithHTTPPath sets up the endpoint to the warehouse. Mandatory.
func WithHTTPPath(path string) connOption {
	return func(c *config.Config) {
		if !strings.HasPrefix(path, "/") {
			path = "/" + path
		}
		c.HTTPPath = path
	}
}

// WithMaxRows sets up the max rows fetched per request. Default is 10000
func WithMaxRows(n int) connOption {
	return func(c *config.Config) {
		if n != 0 {
			c.MaxRows = n
		}
	}
}

// WithTimeout adds timeout for the server query execution. Default is no timeout.
func WithTimeout(n time.Duration) connOption {
	return func(c *config.Config) {
		c.QueryTimeout = n
	}
}

// Sets the initial catalog name and schema name in the session.
// Use <select * from foo> instead of <select * from catalog.schema.foo>
func WithInitialNamespace(catalog, schema string) connOption {
	return func(c *config.Config) {
		c.Catalog = catalog
		c.Schema = schema
	}
}

// Used to identify partners. Set as a string with format <isv-name+product-name>.
func WithUserAgentEntry(entry string) connOption {
	return func(c *config.Config) {
		c.UserAgentEntry = entry
	}
}

// Sessions params will be set upon opening the session by calling SET function.
// If using connection pool, session params can avoid successive calls of "SET ..."
func WithSessionParams(params map[string]string) connOption {
	return func(c *config.Config) {
		for k, v := range params {
			if strings.ToLower(k) == "timezone" {
				if loc, err := time.LoadLocation(v); err != nil {
					logger.Error().Msgf("timezone %s is not valid", v)
				} else {
					c.Location = loc
				}

			}
		}
		c.SessionParams = params
	}
}
