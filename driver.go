package dbsql

import (
	"context"
	"database/sql"
	"database/sql/driver"

	"github.com/databricks/databricks-sql-go/internal/config"
	_ "github.com/databricks/databricks-sql-go/logger"
)

func init() {
	sql.Register("databricks", &databricksDriver{})
}

type databricksDriver struct{}

// Open returns a new connection to Databricks database with a DSN string.
// Use sql.Open("databricks", <dsn string>) after importing this driver package.
func (d *databricksDriver) Open(dsn string) (driver.Conn, error) {
	cfg := config.WithDefaults()
	userCfg, err := config.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	cfg.UserConfig = userCfg
	c := &connector{
		cfg: cfg,
	}
	return c.Connect(context.Background())
}

// OpenConnector returns a new Connector.
// Used by sql.DB to obtain a Connector and invoke its Connect method to obtain each needed connection.
func (d *databricksDriver) OpenConnector(dsn string) (driver.Connector, error) {
	cfg := config.WithDefaults()
	ucfg, err := config.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	cfg.UserConfig = ucfg

	return &connector{cfg: cfg}, nil
}

var _ driver.Driver = (*databricksDriver)(nil)
var _ driver.DriverContext = (*databricksDriver)(nil)
