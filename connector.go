package dbsql

import (
	"context"
	"database/sql/driver"
	"fmt"

	utils "github.com/databricks/databricks-sql-go/internal"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/databricks/databricks-sql-go/internal/sentinel"
	"github.com/databricks/databricks-sql-go/logger"
	"github.com/rs/zerolog/log"
)

type connector struct {
	cfg *config.Config
}

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {

	tclient, err := client.InitThriftClient(c.cfg)
	if err != nil {
		return nil, fmt.Errorf("databricks: error initializing thrift client. %w", err)
	}
	var catalogName *cli_service.TIdentifier
	var schemaName *cli_service.TIdentifier
	if c.cfg.Catalog != "" {
		catalogName = cli_service.TIdentifierPtr(cli_service.TIdentifier(c.cfg.Catalog))
	}
	if c.cfg.Schema != "" {
		schemaName = cli_service.TIdentifierPtr(cli_service.TIdentifier(c.cfg.Schema))
	}

	// we need to ensure that open session will eventually end
	sentinel := sentinel.Sentinel{
		OnDoneFn: func(statusResp any) (any, error) {
			return tclient.OpenSession(ctx, &cli_service.TOpenSessionReq{
				ClientProtocol: c.cfg.ThriftProtocolVersion,
				Configuration:  make(map[string]string),
				InitialNamespace: &cli_service.TNamespace{
					CatalogName: catalogName,
					SchemaName:  schemaName,
				},
				CanUseMultipleCatalogs: &c.cfg.CanUseMultipleCatalogs,
			})
		},
	}
	// default timeout in here in addition to potential context timeout
	_, res, err := sentinel.Watch(ctx, c.cfg.PollInterval, c.cfg.DefaultTimeout)
	if err != nil {
		return nil, err
	}
	session, ok := res.(*cli_service.TOpenSessionResp)
	if !ok {
		return nil, fmt.Errorf("databricks: invalid open session response")
	}
	log.Info().Msgf("open session: %s\n", utils.Guid(session.SessionHandle.GetSessionId().GUID))

	conn := &conn{
		cfg:     c.cfg,
		client:  tclient,
		session: session,
	}
	for k, v := range c.cfg.SessionParams {
		setStmt := fmt.Sprintf("SET `%s` = `%s`;", k, v)
		_, err := conn.ExecContext(ctx, setStmt, []driver.NamedValue{})
		if err != nil {
			return nil, err
		}
		logger.Log.Info().Msgf("session parameters: %s", setStmt)
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
		c.QueryTimeoutSeconds = n
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

// Sessions params will be set upon opening the session
// If using connection pool, session params can avoid successive calls of "SET ..."
func WithSessionParams(params map[string]string) connOption {
	return func(c *config.Config) {
		c.SessionParams = params
	}
}
