package dbsql

import (
	"context"
	"database/sql/driver"
	"fmt"
	"time"

	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/databricks/databricks-sql-go/internal/sentinel"
	"github.com/databricks/databricks-sql-go/logger"
)

type conn struct {
	cfg     *config.Config
	client  *client.ThriftServiceClient
	session *cli_service.TOpenSessionResp
}

// The driver does not really implements prepared statements.
// Statement ExecContext is the same as connection ExecContext
// Statement QueryContext is the same as connection QueryContext
func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return &stmt{conn: c, query: query}, nil
}

// The driver does not really implements prepared statements.
// Statement ExecContext is the same as connection ExecContext
// Statement QueryContext is the same as connection QueryContext
func (c *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return &stmt{conn: c, query: query}, nil
}

func (c *conn) Close() error {
	sentinel := sentinel.Sentinel{
		OnDoneFn: func(statusResp any) (any, error) {
			return c.client.CloseSession(context.Background(), &cli_service.TCloseSessionReq{
				SessionHandle: c.session.SessionHandle,
			})
		},
	}
	_, _, err := sentinel.Watch(context.Background(), c.cfg.PollInterval, 15*time.Second)

	return err
}

// Not supported in Databricks
func (c *conn) Begin() (driver.Tx, error) {
	return nil, fmt.Errorf("databricks: transactions are not supported")
}

// Not supported in Databricks
func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return nil, fmt.Errorf("databricks: transactions are not supported")
}

func (c *conn) Ping(ctx context.Context) error {
	_, err := c.QueryContext(ctx, "select 1", nil)
	if err != nil {
		logger.Err(err).Msg("ping error")
		return driver.ErrBadConn
	}
	return nil
}

// Implementation of SessionResetter
func (c *conn) ResetSession(ctx context.Context) error {
	// For now our session does not have any important state to reset before re-use
	return nil
}

func (c *conn) IsValid() bool {
	return c.session.GetStatus().StatusCode == cli_service.TStatusCode_SUCCESS_STATUS
}

// ExecContext executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
//
// ExecContext honors the context timeout and return when it is canceled.
func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if len(args) > 0 {
		return nil, fmt.Errorf("databricks: query parameters are not supported")
	}
	_, opStatusResp, err := c.runQuery(ctx, query, args)

	if err != nil {
		return nil, err
	}
	res := result{AffectedRows: opStatusResp.GetNumModifiedRows()}

	return &res, nil
}

// QueryContext executes a query that may return rows, such as a
// SELECT.
//
// QueryContext honors the context timeout and return when it is canceled.
func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if len(args) > 0 {
		return nil, fmt.Errorf("databricks: query parameters are not supported")
	}
	// first we try to get the results synchronously.
	// at any point in time that the context is done we must cancel and return
	exStmtResp, _, err := c.runQuery(ctx, query, args)

	if err != nil {
		return nil, err
	}
	// hold on to the operation handle
	opHandle := exStmtResp.OperationHandle

	rows := rows{
		client:   c.client,
		opHandle: opHandle,
		pageSize: int64(c.cfg.MaxRows),
	}

	if exStmtResp.DirectResults != nil {
		// return results
		rows.fetchResults = exStmtResp.DirectResults.ResultSet
		rows.fetchResultsMetadata = exStmtResp.DirectResults.ResultSetMetadata

	}
	return &rows, nil

}

func (c *conn) runQuery(ctx context.Context, query string, args []driver.NamedValue) (*cli_service.TExecuteStatementResp, *cli_service.TGetOperationStatusResp, error) {
	// first we try to get the results synchronously.
	// at any point in time that the context is done we must cancel and return
	exStmtResp, err := c.executeStatement(ctx, query, args)

	if err != nil {
		return nil, nil, err
	}
	// hold on to the operation handle
	opHandle := exStmtResp.OperationHandle

	if exStmtResp.DirectResults != nil {
		opStatus := exStmtResp.DirectResults.GetOperationStatus()

		switch opStatus.GetOperationState() {
		// terminal states
		// good
		case cli_service.TOperationState_FINISHED_STATE:
			// return results
			return exStmtResp, opStatus, nil
		// bad
		case cli_service.TOperationState_CANCELED_STATE, cli_service.TOperationState_CLOSED_STATE, cli_service.TOperationState_ERROR_STATE, cli_service.TOperationState_TIMEDOUT_STATE:
			// do we need to close the operation in these cases?
			logger.Error().Msg(opStatus.GetErrorMessage())
			logger.Debug().Msgf("bad state: %s", opStatus.GetOperationState())

			return exStmtResp, opStatus, fmt.Errorf(opStatus.GetDisplayMessage())
		// live states
		case cli_service.TOperationState_INITIALIZED_STATE, cli_service.TOperationState_PENDING_STATE, cli_service.TOperationState_RUNNING_STATE:
			statusResp, err := c.pollOperation(ctx, opHandle)
			if err != nil {
				return nil, statusResp, err
			}
			switch statusResp.GetOperationState() {
			// terminal states
			// good
			case cli_service.TOperationState_FINISHED_STATE:
				// return handle to fetch results later
				return exStmtResp, opStatus, nil
			// bad
			case cli_service.TOperationState_CANCELED_STATE, cli_service.TOperationState_CLOSED_STATE, cli_service.TOperationState_ERROR_STATE, cli_service.TOperationState_TIMEDOUT_STATE:
				logger.Debug().Msgf("bad state: %s", statusResp.GetOperationState())
				logger.Error().Msg(statusResp.GetErrorMessage())
				return exStmtResp, opStatus, fmt.Errorf(statusResp.GetDisplayMessage())
				// live states
			default:
				logger.Debug().Msgf("bad state: %s", statusResp.GetOperationState())
				logger.Error().Msg(statusResp.GetErrorMessage())
				return exStmtResp, opStatus, fmt.Errorf("invalid operation state. This should not have happened")
			}
		// weird states
		default:
			logger.Debug().Msgf("bad state: %s", opStatus.GetOperationState())
			logger.Error().Msg(opStatus.GetErrorMessage())
			return exStmtResp, opStatus, fmt.Errorf("invalid operation state. This should not have happened")
		}

	} else {
		statusResp, err := c.pollOperation(ctx, opHandle)
		if err != nil {
			return exStmtResp, statusResp, err
		}
		switch statusResp.GetOperationState() {
		// terminal states
		// good
		case cli_service.TOperationState_FINISHED_STATE:
			// return handle to fetch results later
			return exStmtResp, statusResp, nil
		// bad
		case cli_service.TOperationState_CANCELED_STATE, cli_service.TOperationState_CLOSED_STATE, cli_service.TOperationState_ERROR_STATE, cli_service.TOperationState_TIMEDOUT_STATE:
			logger.Debug().Msgf("bad state: %s", statusResp.GetOperationState())
			logger.Error().Msg(statusResp.GetErrorMessage())
			return exStmtResp, statusResp, fmt.Errorf(statusResp.GetDisplayMessage())
			// live states
		default:
			logger.Debug().Msgf("bad state: %s", statusResp.GetOperationState())
			logger.Error().Msg(statusResp.GetErrorMessage())
			return exStmtResp, statusResp, fmt.Errorf("invalid operation state. This should not have happened")
		}
	}
}

func (c *conn) executeStatement(ctx context.Context, query string, args []driver.NamedValue) (*cli_service.TExecuteStatementResp, error) {
	sentinel := sentinel.Sentinel{
		OnDoneFn: func(statusResp any) (any, error) {
			req := cli_service.TExecuteStatementReq{
				SessionHandle: c.session.SessionHandle,
				Statement:     query,
				RunAsync:      c.cfg.RunAsync,
				QueryTimeout:  int64(c.cfg.QueryTimeoutSeconds),
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
	_, res, err := sentinel.Watch(ctx, c.cfg.PollInterval, 0)
	if err != nil {
		return nil, err
	}
	exStmtResp, ok := res.(*cli_service.TExecuteStatementResp)
	if !ok {
		return nil, fmt.Errorf("databricks: invalid execute statement response")
	}
	return exStmtResp, err
}

func (c *conn) pollOperation(ctx context.Context, opHandle *cli_service.TOperationHandle) (*cli_service.TGetOperationStatusResp, error) {
	var statusResp *cli_service.TGetOperationStatusResp
	pollSentinel := sentinel.Sentinel{
		OnDoneFn: func(statusResp any) (any, error) {
			return statusResp, nil
		},
		StatusFn: func() (sentinel.Done, any, error) {
			var err error
			logger.Debug().Msg("databricks: polling status")
			statusResp, err = c.client.GetOperationStatus(context.Background(), &cli_service.TGetOperationStatusReq{
				OperationHandle: opHandle,
			})
			return func() bool {
				// which other states?
				switch statusResp.GetOperationState() {
				case cli_service.TOperationState_INITIALIZED_STATE, cli_service.TOperationState_PENDING_STATE, cli_service.TOperationState_RUNNING_STATE:
					return false
				default:
					logger.Debug().Msg("databricks: polling done")
					return true
				}
			}, statusResp, err
		},
		OnCancelFn: func() (any, error) {
			logger.Debug().Msgf("databricks: canceling operation %s", opHandle.OperationId)
			ret, err := c.client.CancelOperation(context.Background(), &ts.TCancelOperationReq{
				OperationHandle: opHandle,
			})
			return ret, err
		},
	}
	_, resp, err := pollSentinel.Watch(ctx, c.cfg.PollInterval, 0)
	if err != nil {
		return nil, err
	}
	statusResp, ok := resp.(*cli_service.TGetOperationStatusResp)
	if !ok {
		return nil, fmt.Errorf("could not read operation status")
	}
	return statusResp, err
}

var _ driver.Conn = (*conn)(nil)
var _ driver.Pinger = (*conn)(nil)
var _ driver.SessionResetter = (*conn)(nil)
var _ driver.Validator = (*conn)(nil)
var _ driver.ExecerContext = (*conn)(nil)
var _ driver.QueryerContext = (*conn)(nil)
var _ driver.ConnPrepareContext = (*conn)(nil)
var _ driver.ConnBeginTx = (*conn)(nil)
