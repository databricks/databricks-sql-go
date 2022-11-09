package dbsql

import (
	"context"
	"database/sql"
	"database/sql/driver"

	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/databricks/databricks-sql-go/logger"
	"github.com/rs/zerolog"
)

func init() {
	sql.Register("databricks", &databricksDriver{})
}

type databricksDriver struct{}

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

func (d *databricksDriver) OpenConnector(dsn string) (driver.Connector, error) {
	cfg := config.WithDefaults()
	ucfg, err := config.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	cfg.UserConfig = ucfg
	return &connector{cfg}, nil
}

var _ driver.Driver = (*databricksDriver)(nil)
var _ driver.DriverContext = (*databricksDriver)(nil)

func GetLogger() zerolog.Logger {
	return logger.Log
}
