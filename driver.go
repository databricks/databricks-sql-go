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

	return &connector{cfg: cfg}, nil
}

var _ driver.Driver = (*databricksDriver)(nil)
var _ driver.DriverContext = (*databricksDriver)(nil)

// type databricksDB struct {
// 	*sql.DB
// }

// func OpenDB(c driver.Connector) *databricksDB {
// 	db := sql.OpenDB(c)
// 	return &databricksDB{db}
// }

// func (db *databricksDB) QueryContextAsync(ctx context.Context, query string, args ...any) (rows *sql.Rows, queryId string, err error) {
// 	return nil, "", nil
// }

// func (db *databricksDB) ExecContextAsync(ctx context.Context, query string, args ...any) (result sql.Result, queryId string) {
// 	//go do something
// 	return nil, ""
// }

// func (db *databricksDB) CancelQuery(ctx context.Context, queryId string) error {
// 	//go do something
// 	return nil
// }

// func (db *databricksDB) GetQueryStatus(ctx context.Context, queryId string) error {
// 	//go do something
// 	return nil
// }

// func (db *databricksDB) FetchRows(ctx context.Context, queryId string) (rows *sql.Rows, err error) {
// 	//go do something
// 	return nil, nil
// }

// func (db *databricksDB) FetchResult(ctx context.Context, queryId string) (rows sql.Result, err error) {
// 	//go do something
// 	return nil, nil
// }
