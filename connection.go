package dbsql

import (
	"context"
	"database/sql/driver"
	"fmt"

	"github.com/rs/zerolog/log"

	ts "github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/databricks/databricks-sql-go/internal/sentinel"
)

type conn struct {
	cfg     *config.Config
	client  *client.ThriftServiceClient
	session *ts.TOpenSessionResp
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return nil, ErrNotImplemented
}

func (c *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return &dbsqlStmt{}, ErrNotImplemented
}

func (c *conn) Close() error {
	_, err := c.client.CloseSession(context.Background(), &ts.TCloseSessionReq{
		SessionHandle: c.session.SessionHandle,
	})

	return err
}

func (c *conn) Begin() (driver.Tx, error) {
	return nil, ErrNotImplemented
}

func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return nil, ErrNotImplemented
}

func (c *conn) Ping(ctx context.Context) error {
	_, err := c.QueryContext(ctx, "select 1", nil)
	if err != nil {
		log.Err(err).Msg("ping error")
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
	return c.session.GetStatus().StatusCode == ts.TStatusCode_SUCCESS_STATUS
}

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	req := ts.TExecuteStatementReq{
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
	// at any point in time that the context is done we must cancel and return
	exStmtResp, err := c.executeStatement(ctx, query, args)

	if err != nil {
		return nil, err
	}
	// hold on to the operation handle
	opHandle := exStmtResp.OperationHandle

	if exStmtResp.DirectResults != nil {
		opStatus := exStmtResp.DirectResults.GetOperationStatus()

		switch opStatus.GetOperationState() {
		// terminal states
		// good
		case ts.TOperationState_FINISHED_STATE:
			// return results
			rows := rows{
				client:               c.client,
				opHandle:             opHandle,
				pageSize:             int64(c.cfg.MaxRows),
				fetchResults:         exStmtResp.DirectResults.ResultSet,
				fetchResultsMetadata: exStmtResp.DirectResults.ResultSetMetadata,
			}
			return &rows, nil
		// bad
		case ts.TOperationState_CANCELED_STATE, ts.TOperationState_CLOSED_STATE, ts.TOperationState_ERROR_STATE, ts.TOperationState_TIMEDOUT_STATE:
			// do we need to close the operation in these cases?
			log.Info().Msgf("bad state: %s", opStatus.GetOperationState())
			log.Info().Msg(opStatus.GetErrorMessage())

			return nil, fmt.Errorf(opStatus.GetErrorMessage())
		// live states
		case ts.TOperationState_INITIALIZED_STATE, ts.TOperationState_PENDING_STATE, ts.TOperationState_RUNNING_STATE:
			statusResp, err := c.pollOperation(ctx, opHandle)
			if err != nil {
				return nil, err
			}
			switch statusResp.GetOperationState() {
			// terminal states
			// good
			case ts.TOperationState_FINISHED_STATE:
				// return handle to fetch results later
				rows := rows{
					client:   c.client,
					opHandle: opHandle,
					pageSize: int64(c.cfg.MaxRows),
				}
				return &rows, nil
			// bad
			case ts.TOperationState_CANCELED_STATE, ts.TOperationState_CLOSED_STATE, ts.TOperationState_ERROR_STATE, ts.TOperationState_TIMEDOUT_STATE:
				log.Info().Msgf("bad state: %s", statusResp.GetOperationState())
				log.Info().Msg(statusResp.GetErrorMessage())
				return nil, fmt.Errorf(statusResp.GetErrorMessage())
				// live states
			default:
				log.Info().Msgf("bad state: %s", statusResp.GetOperationState())
				log.Info().Msg(statusResp.GetErrorMessage())
				return nil, fmt.Errorf("invalid operation state. This should not have happened")
			}
		// weird states
		default:
			log.Info().Msgf("bad state: %s", opStatus.GetOperationState())
			log.Info().Msg(opStatus.GetErrorMessage())
			return nil, fmt.Errorf("invalid operation state. This should not have happened")
		}

	} else {
		statusResp, err := c.pollOperation(ctx, opHandle)
		if err != nil {
			return nil, err
		}
		switch statusResp.GetOperationState() {
		// terminal states
		// good
		case ts.TOperationState_FINISHED_STATE:
			// return handle to fetch results later
			rows := rows{
				client:   c.client,
				opHandle: opHandle,
				pageSize: int64(c.cfg.MaxRows),
			}
			return &rows, nil
		// bad
		case ts.TOperationState_CANCELED_STATE, ts.TOperationState_CLOSED_STATE, ts.TOperationState_ERROR_STATE, ts.TOperationState_TIMEDOUT_STATE:
			log.Info().Msgf("bad state: %s", statusResp.GetOperationState())
			log.Info().Msg(statusResp.GetErrorMessage())
			return nil, fmt.Errorf(statusResp.GetErrorMessage())
			// live states
		default:
			log.Info().Msgf("bad state: %s", statusResp.GetOperationState())
			log.Info().Msg(statusResp.GetErrorMessage())
			return nil, fmt.Errorf("invalid operation state. This should not have happened")
		}
	}
}

func (c *conn) executeStatement(ctx context.Context, query string, args []driver.NamedValue) (*ts.TExecuteStatementResp, error) {
	sentinel := sentinel.Sentinel{
		OnDoneFn: func(statusResp any) (any, error) {
			req := ts.TExecuteStatementReq{
				SessionHandle: c.session.SessionHandle,
				Statement:     query,
				RunAsync:      c.cfg.RunAsync,
				QueryTimeout:  int64(c.cfg.TimeoutSeconds),
				// this is specific for databricks. It shortcuts server roundtrips
				GetDirectResults: &ts.TSparkGetDirectResults{
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
	exStmtResp, ok := res.(*ts.TExecuteStatementResp)
	if !ok {
		return nil, fmt.Errorf("databricks: invalid execute statement response")
	}
	return exStmtResp, err
}

func (c *conn) pollOperation(ctx context.Context, opHandle *ts.TOperationHandle) (*ts.TGetOperationStatusResp, error) {
	var statusResp *ts.TGetOperationStatusResp
	pollSentinel := sentinel.Sentinel{
		OnDoneFn: func(statusResp any) (any, error) {
			return statusResp, nil
		},
		StatusFn: func() (sentinel.Done, any, error) {
			var err error
			statusResp, err = c.client.GetOperationStatus(context.Background(), &ts.TGetOperationStatusReq{
				OperationHandle: opHandle,
			})
			return func() bool {
				// which other states?
				switch statusResp.GetOperationState() {
				case ts.TOperationState_INITIALIZED_STATE, ts.TOperationState_PENDING_STATE, ts.TOperationState_RUNNING_STATE:
					return false
				default:
					return true
				}
			}, statusResp, err
		},
		OnCancelFn: func() (any, error) {
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
	statusResp, ok := resp.(*ts.TGetOperationStatusResp)
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
