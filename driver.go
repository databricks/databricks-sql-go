package dbsql

import (
	"context"
	"database/sql"
	"database/sql/driver"

	"github.com/databricks/databricks-sql-go/internal/config"
)

func init() {
	sql.Register("databricks", &databricksDriver{})
}

type databricksDriver struct{}

func (d *databricksDriver) Open(uri string) (driver.Conn, error) {
	cfg, err := config.ParseURI(uri)
	if err != nil {
		return nil, err
	}
	c := &connector{
		cfg: cfg,
	}
	return c.Connect(context.Background())
}

func (d *databricksDriver) OpenConnector(uri string) (driver.Connector, error) {
	cfg, err := config.ParseURI(uri)
	return &connector{cfg}, err
}

var _ driver.Driver = (*databricksDriver)(nil)
var _ driver.DriverContext = (*databricksDriver)(nil)
