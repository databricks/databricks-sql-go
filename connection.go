package dbsql

import (
	"context"
	"database/sql/driver"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/databricks/databricks-sql-go/internal/sentinel"
)

type conn struct {
	cfg     *config.Config
	client  *client.ThriftServiceClient
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
		case cli_service.TOperationState_FINISHED_STATE:
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
		case cli_service.TOperationState_CANCELED_STATE, cli_service.TOperationState_CLOSED_STATE, cli_service.TOperationState_ERROR_STATE, cli_service.TOperationState_TIMEDOUT_STATE:
			// do we need to close the operation in these cases?
			log.Info().Msgf("bad state: %s", opStatus.GetOperationState())
			log.Info().Msg(opStatus.GetDiagnosticInfo())
			log.Info().Msg(opStatus.GetErrorMessage())
			// log.Debug().Msg(fmt.Sprint(c.transport.response.StatusCode))
			// log.Debug().Msg(c.transport.response.Header.Get("X-Databricks-Org-Id"))
			// log.Debug().Msg(c.transport.response.Header.Get("x-databricks-error-or-redirect-message"))
			// log.Debug().Msg(c.transport.response.Header.Get("x-thriftserver-error-message"))
			// log.Debug().Msg(c.transport.response.Header.Get("x-databricks-reason-phrase"))
			return nil, fmt.Errorf(opStatus.GetErrorMessage())
		// live states
		case cli_service.TOperationState_INITIALIZED_STATE, cli_service.TOperationState_PENDING_STATE, cli_service.TOperationState_RUNNING_STATE:
			statusResp, err := c.pollOperation(ctx, opHandle)
			if err != nil {
				return nil, err
			}
			switch statusResp.GetOperationState() {
			// terminal states
			// good
			case cli_service.TOperationState_FINISHED_STATE:
				// return handle to fetch results later
				rows := rows{
					client:   c.client,
					opHandle: opHandle,
					pageSize: int64(c.cfg.MaxRows),
				}
				return &rows, nil
			// bad
			case cli_service.TOperationState_CANCELED_STATE, cli_service.TOperationState_CLOSED_STATE, cli_service.TOperationState_ERROR_STATE, cli_service.TOperationState_TIMEDOUT_STATE:
				log.Info().Msgf("bad state: %s", statusResp.GetOperationState())
				log.Info().Msg(statusResp.GetDiagnosticInfo())
				log.Info().Msg(statusResp.GetErrorMessage())
				return nil, fmt.Errorf(statusResp.GetErrorMessage())
				// live states
			default:
				log.Info().Msgf("bad state: %s", statusResp.GetOperationState())
				log.Info().Msg(statusResp.GetDiagnosticInfo())
				log.Info().Msg(statusResp.GetErrorMessage())
				return nil, fmt.Errorf("invalid operation state. This should not have happened")
			}
		// weird states
		default:
			log.Info().Msgf("bad state: %s", opStatus.GetOperationState())
			log.Info().Msg(opStatus.GetDiagnosticInfo())
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
		case cli_service.TOperationState_FINISHED_STATE:
			// return handle to fetch results later
			rows := rows{
				client:   c.client,
				opHandle: opHandle,
				pageSize: int64(c.cfg.MaxRows),
			}
			return &rows, nil
		// bad
		case cli_service.TOperationState_CANCELED_STATE, cli_service.TOperationState_CLOSED_STATE, cli_service.TOperationState_ERROR_STATE, cli_service.TOperationState_TIMEDOUT_STATE:
			log.Info().Msgf("bad state: %s", statusResp.GetOperationState())
			log.Info().Msg(statusResp.GetDiagnosticInfo())
			log.Info().Msg(statusResp.GetErrorMessage())
			return nil, fmt.Errorf(statusResp.GetErrorMessage())
			// live states
		default:
			log.Info().Msgf("bad state: %s", statusResp.GetOperationState())
			log.Info().Msg(statusResp.GetDiagnosticInfo())
			log.Info().Msg(statusResp.GetErrorMessage())
			return nil, fmt.Errorf("invalid operation state. This should not have happened")
		}
	}
}

func (c *conn) executeStatement(ctx context.Context, query string, args []driver.NamedValue) (*cli_service.TExecuteStatementResp, error) {
	sentinel := sentinel.Sentinel{
		OnDoneFn: func(statusResp any) (any, error) {
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
	_, res, err := sentinel.Watch(ctx, 0, 0)
	exStmtResp := res.(*cli_service.TExecuteStatementResp)
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
			statusResp, err = c.client.GetOperationStatus(ctx, &cli_service.TGetOperationStatusReq{
				OperationHandle: opHandle,
			})
			return func() bool {
				// which other states?
				switch statusResp.GetOperationState() {
				case cli_service.TOperationState_INITIALIZED_STATE, cli_service.TOperationState_PENDING_STATE, cli_service.TOperationState_RUNNING_STATE:
					return false
				default:
					return true
				}
			}, statusResp, err
		},
		OnCancelFn: func() (any, error) {
			ret, err := c.client.CancelOperation(context.Background(), &cli_service.TCancelOperationReq{
				OperationHandle: opHandle,
			})
			return ret, err
		},
	}
	_, resp, err := pollSentinel.Watch(ctx, 100*time.Millisecond, 0)
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
