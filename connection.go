package dbsql

import (
	"context"
	"database/sql/driver"
	"fmt"
	"time"

	"github.com/databricks/databricks-sql-go/cli_service"
	"github.com/databricks/databricks-sql-go/internal/utils"
)

type conn struct {
	cfg     *config
	client  *cli_service.TCLIServiceClient
	session *cli_service.TOpenSessionResp
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return nil, ErrNotImplemented
}
func (c *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return &dbsqlStmt{}, ErrNotImplemented
}

func (c *conn) Close() error {
	return ErrNotImplemented
}

func (c *conn) Begin() (driver.Tx, error) {
	return nil, ErrNotImplemented
}
func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return nil, ErrNotImplemented
}

func (c *conn) Ping(ctx context.Context) error {
	return ErrNotImplemented
}

func (c *conn) ResetSession(ctx context.Context) error {
	return ErrNotImplemented
}

func (c *conn) IsValid() bool {
	return true
}

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	req := cli_service.TExecuteStatementReq{
		SessionHandle: c.session.SessionHandle,
		Statement:     query,
		QueryTimeout:  int64(c.cfg.TimeoutSeconds),
	}
	resp, err := c.client.ExecuteStatement(ctx, &req)
	if err != nil {
		return nil, err
	} else {
		fmt.Println(resp)
	}

	return nil, ErrNotImplemented
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	// first we try to get the results synchronously.

	var resultSet *cli_service.TFetchResultsResp

	// at any point in time that the context is done we must cancel and return
	querySentinel := utils.Sentinel{
		ProcessFn: func() (any, error) {
			req := cli_service.TExecuteStatementReq{
				SessionHandle: c.session.SessionHandle,
				Statement:     query,
				RunAsync:      true,
				QueryTimeout:  int64(c.cfg.TimeoutSeconds),
				// this is specific for databricks. It shortcuts server roundtrips
				GetDirectResults: &cli_service.TSparkGetDirectResults{
					MaxRows: int64(c.cfg.MaxRows),
				},
				// CanReadArrowResult_: &t,
				// CanDecompressLZ4Result_: &f,
				// CanDownloadResult_: &t,
			}
			resp, err := c.client.ExecuteStatement(ctx, &req)
			return resp, err
		},
	}

	_, res, err := querySentinel.Watch(ctx, 0, 0)

	if err != nil {
		return nil, err
	}
	exStmtResp := res.(*cli_service.TExecuteStatementResp)
	// hold on to the operation handle
	opHandle := exStmtResp.OperationHandle

	if exStmtResp.DirectResults != nil {
		opStatus := exStmtResp.DirectResults.GetOperationStatus()
		resultSet = exStmtResp.DirectResults.ResultSet
		if opStatus.GetOperationState() == cli_service.TOperationState_RUNNING_STATE {

			// if the query took too long, we'll watch the operation until it completes
			// at any point in time that the context is done we must cancel and return
			pollSentinel := utils.Sentinel{
				ProcessFn: func() (any, error) {
					return nil, err
				},
				StatusFn: func() (utils.Done, error) {
					resp, err := c.client.GetOperationStatus(ctx, &cli_service.TGetOperationStatusReq{
						OperationHandle: opHandle,
					})
					return func() bool {
						return resp.GetOperationState() == cli_service.TOperationState_FINISHED_STATE
					}, err
				},
				CancelFn: func() (any, error) {
					ret, err := c.client.CancelOperation(context.Background(), &cli_service.TCancelOperationReq{
						OperationHandle: opHandle,
					})
					return ret, err
				},
			}
			_, _, err := pollSentinel.Watch(ctx, 100*time.Millisecond, 0)
			if err != nil {
				return nil, err
			}

		}

	} else {
		// what happens here? do normal polling I guess
		panic("direct result was nil")
	}
	rows := rows{
		client:       c.client,
		opHandle:     opHandle,
		pageSize:     int64(c.cfg.MaxRows),
		fetchResults: resultSet,
	}
	return &rows, nil
}

var _ driver.Pinger = (*conn)(nil)
var _ driver.SessionResetter = (*conn)(nil)
var _ driver.Validator = (*conn)(nil)
var _ driver.ExecerContext = (*conn)(nil)
var _ driver.QueryerContext = (*conn)(nil)
var _ driver.ConnPrepareContext = (*conn)(nil)
var _ driver.ConnBeginTx = (*conn)(nil)
