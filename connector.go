package dbsql

import (
	"context"
	"database/sql/driver"
	"fmt"

	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/databricks/databricks-sql-go/internal/utils"
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

	req := cli_service.TOpenSessionReq{
		ClientProtocol: c.cfg.Thrift.ProtocolVersion,
		Configuration:  make(map[string]string),
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
