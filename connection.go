package dbsql

import (
	"context"
	"database/sql/driver"

	"github.com/databricks/databricks-sql-go/driverctx"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/databricks/databricks-sql-go/internal/sentinel"
	"github.com/databricks/databricks-sql-go/logger"
	"github.com/pkg/errors"
)

type conn struct {
	id      string
	cfg     *config.Config
	client  client.DatabricksClient
	session *client.OpenSessionResp
}

// Prepare prepares a statement with the query bound to this connection.
func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return &stmt{conn: c, query: query}, nil
}

// PrepareContext prepares a statement with the query bound to this connection.
// Currently, PrepareContext does not use context and is functionally equivalent to Prepare.
func (c *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return &stmt{conn: c, query: query}, nil
}

// Close closes the session.
// sql package maintains a free pool of connections and only calls Close when there's a surplus of idle connections.
func (c *conn) Close() error {
	log := logger.WithContext(c.id, "", "")
	ctx := driverctx.NewContextWithConnId(context.Background(), c.id)

	_, err := c.client.CloseSession(ctx, &client.CloseSessionReq{
		SessionHandle: c.session.SessionHandle,
	})

	if err != nil {
		log.Err(err).Msg("databricks: failed to close connection")
		return wrapErr(err, "failed to close connection")
	}
	return nil
}

// Not supported in Databricks.
func (c *conn) Begin() (driver.Tx, error) {
	return nil, errors.New(ErrTransactionsNotSupported)
}

// Not supported in Databricks.
func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return nil, errors.New(ErrTransactionsNotSupported)
}

// Ping attempts to verify that the server is accessible.
// Returns ErrBadConn if ping fails and consequently DB.Ping will remove the conn from the pool.
func (c *conn) Ping(ctx context.Context) error {
	log := logger.WithContext(c.id, driverctx.CorrelationIdFromContext(ctx), "")
	ctx = driverctx.NewContextWithConnId(ctx, c.id)
	ctx1, cancel := context.WithTimeout(ctx, c.cfg.PingTimeout)
	defer cancel()
	_, err := c.QueryContext(ctx1, "select 1", nil)
	if err != nil {
		log.Err(err).Msg("databricks: failed to ping")
		return driver.ErrBadConn
	}
	return nil
}

// ResetSession is called prior to executing a query on the connection.
// The session with this driver does not have any important state to reset before re-use.
func (c *conn) ResetSession(ctx context.Context) error {
	return nil
}

// IsValid signals whether a connection is valid or if it should be discarded.
func (c *conn) IsValid() bool {
	return c.session.Status.StatusCode == cli_service.TStatusCode_SUCCESS_STATUS.String() // FIXME: need to standardize it
}

// ExecContext executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
//
// ExecContext honors the context timeout and return when it is canceled.
// Statement ExecContext is the same as connection ExecContext
func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	corrId := driverctx.CorrelationIdFromContext(ctx)
	log := logger.WithContext(c.id, corrId, "")
	msg, start := logger.Track("ExecContext")
	defer log.Duration(msg, start)

	ctx = driverctx.NewContextWithConnId(ctx, c.id)
	if len(args) > 0 {
		return nil, errors.New(ErrParametersNotSupported)
	}
	exStmtResp, opStatusResp, err := c.runQuery(ctx, query, args)

	if exStmtResp != nil && exStmtResp.ExecutionHandle != nil {
		// we have an operation id so update the logger
		log = logger.WithContext(c.id, corrId, exStmtResp.ExecutionHandle.Id())

		// since we have an operation handle we can close the operation if necessary
		alreadyClosed := exStmtResp.Result != nil && exStmtResp.IsClosed
		newCtx := driverctx.NewContextWithCorrelationId(driverctx.NewContextWithConnId(context.Background(), c.id), corrId)
		if !alreadyClosed && (opStatusResp == nil || opStatusResp.ExecutionState != cli_service.TOperationState_CLOSED_STATE.String()) {
			_, err1 := c.client.CloseExecution(newCtx, &client.CloseExecutionReq{
				ExecutionHandle: exStmtResp.ExecutionHandle,
			})
			if err1 != nil {
				log.Err(err1).Msg("databricks: failed to close operation after executing statement")
			}
		}
	}
	if err != nil {
		log.Err(err).Msgf("databricks: failed to execute query: query %s", query)
		return nil, wrapErrf(err, "failed to execute query")
	}

	res := result{AffectedRows: opStatusResp.NumModifiedRows}

	return &res, nil
}

// QueryContext executes a query that may return rows, such as a
// SELECT.
//
// QueryContext honors the context timeout and return when it is canceled.
// Statement QueryContext is the same as connection QueryContext
func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	corrId := driverctx.CorrelationIdFromContext(ctx)
	log := logger.WithContext(c.id, corrId, "")
	msg, start := log.Track("QueryContext")

	ctx = driverctx.NewContextWithConnId(ctx, c.id)
	if len(args) > 0 {
		return nil, errors.New(ErrParametersNotSupported)
	}
	// first we try to get the results synchronously.
	// at any point in time that the context is done we must cancel and return
	exStmtResp, _, err := c.runQuery(ctx, query, args)

	if exStmtResp != nil && exStmtResp.ExecutionHandle != nil {
		log = logger.WithContext(c.id, driverctx.CorrelationIdFromContext(ctx), exStmtResp.ExecutionHandle.Id())
	}
	defer log.Duration(msg, start)

	if err != nil {
		log.Err(err).Msg("databricks: failed to run query") // To log query we need to redact credentials
		return nil, wrapErrf(err, "failed to run query")
	}
	// hold on to the operation handle
	opHandle := exStmtResp.ExecutionHandle

	rows := NewRows(c.id, corrId, c.client, opHandle, int64(c.cfg.MaxRows), c.cfg.Location, exStmtResp)

	return rows, nil

}

func (c *conn) runQuery(ctx context.Context, query string, args []driver.NamedValue) (*client.ExecuteStatementResp, *client.ExecutionStatus, error) {
	log := logger.WithContext(c.id, driverctx.CorrelationIdFromContext(ctx), "")
	// first we try to get the results synchronously.
	// at any point in time that the context is done we must cancel and return
	exStmtResp, err := c.executeStatement(ctx, query, args)

	if err != nil {
		return exStmtResp, nil, err
	}
	opHandle := exStmtResp.ExecutionHandle
	if opHandle != nil {
		log = logger.WithContext(
			c.id,
			driverctx.CorrelationIdFromContext(ctx), opHandle.Id(),
		)
	}

	if exStmtResp.Result != nil {
		opStatus := &exStmtResp.ExecutionStatus

		switch opStatus.ExecutionState {
		// terminal states
		// good
		case cli_service.TOperationState_FINISHED_STATE.String():
			return exStmtResp, opStatus, nil
		// bad
		case cli_service.TOperationState_CANCELED_STATE.String(),
			cli_service.TOperationState_CLOSED_STATE.String(),
			cli_service.TOperationState_ERROR_STATE.String(),
			cli_service.TOperationState_TIMEDOUT_STATE.String():
			logBadQueryState(log, opStatus)
			return exStmtResp, opStatus, errors.New(opStatus.Error.Message)
		// live states
		case cli_service.TOperationState_INITIALIZED_STATE.String(),
			cli_service.TOperationState_PENDING_STATE.String(),
			cli_service.TOperationState_RUNNING_STATE.String():
			statusResp, err := c.pollOperation(ctx, opHandle)
			if err != nil {
				return exStmtResp, statusResp, err
			}
			switch statusResp.ExecutionState {
			// terminal states
			// good
			case cli_service.TOperationState_FINISHED_STATE.String():
				return exStmtResp, statusResp, nil
			// bad
			case cli_service.TOperationState_CANCELED_STATE.String(),
				cli_service.TOperationState_CLOSED_STATE.String(),
				cli_service.TOperationState_ERROR_STATE.String(),
				cli_service.TOperationState_TIMEDOUT_STATE.String():
				logBadQueryState(log, statusResp)
				return exStmtResp, statusResp, errors.New(statusResp.Error.Message)
				// live states
			default:
				logBadQueryState(log, statusResp)
				return exStmtResp, statusResp, errors.New("invalid operation state. This should not have happened")
			}
		// weird states
		default:
			logBadQueryState(log, opStatus)
			return exStmtResp, opStatus, errors.New("invalid operation state. This should not have happened")
		}

	} else {
		statusResp, err := c.pollOperation(ctx, opHandle)
		if err != nil {
			return exStmtResp, statusResp, err
		}
		switch statusResp.ExecutionState {
		// terminal states
		// good
		case cli_service.TOperationState_FINISHED_STATE.String():
			return exStmtResp, statusResp, nil
		// bad
		case cli_service.TOperationState_CANCELED_STATE.String(),
			cli_service.TOperationState_CLOSED_STATE.String(),
			cli_service.TOperationState_ERROR_STATE.String(),
			cli_service.TOperationState_TIMEDOUT_STATE.String():
			logBadQueryState(log, statusResp)
			return exStmtResp, statusResp, errors.New(statusResp.Error.Message)
			// live states
		default:
			logBadQueryState(log, statusResp)
			return exStmtResp, statusResp, errors.New("invalid operation state. This should not have happened")
		}
	}
}

func logBadQueryState(log *logger.DBSQLLogger, opStatus *client.ExecutionStatus) {
	log.Error().Msgf("databricks: query state: %s", opStatus.ExecutionState)
	log.Error().Msg(opStatus.Error.Message)
}

func (c *conn) executeStatement(ctx context.Context, query string, args []driver.NamedValue) (*client.ExecuteStatementResp, error) {
	corrId := driverctx.CorrelationIdFromContext(ctx)
	log := logger.WithContext(c.id, corrId, "")

	ctx = driverctx.NewContextWithConnId(ctx, c.id)
	resp, err := c.client.ExecuteStatement(ctx, &client.ExecuteStatementReq{
		SessionHandle: c.session.SessionHandle,
		Statement:     query,
	})

	var shouldCancel = func(resp *client.ExecuteStatementResp) bool {
		if resp == nil {
			return false
		}
		hasHandle := resp.ExecutionHandle != nil
		// isOpen := resp.DirectResults != nil && resp.DirectResults.CloseOperation == nil
		return hasHandle && !resp.IsClosed
	}

	select {
	default:
	case <-ctx.Done():
		newCtx := driverctx.NewContextWithCorrelationId(driverctx.NewContextWithConnId(context.Background(), c.id), corrId)
		// in case context is done, we need to cancel the operation if necessary
		if err == nil && shouldCancel(resp) {
			log.Debug().Msg("databricks: canceling query")
			_, err1 := c.client.CancelExecution(newCtx, &client.CancelExecutionReq{
				ExecutionHandle: resp.ExecutionHandle,
			})

			if err1 != nil {
				log.Err(err).Msgf("databricks: cancel failed")
			} else {
				log.Debug().Msgf("databricks: cancel success")
			}

		} else {
			log.Debug().Msg("databricks: query did not need cancellation")
		}
		return nil, ctx.Err()
	}

	return resp, err
}

func (c *conn) pollOperation(ctx context.Context, opHandle client.Handle) (*client.ExecutionStatus, error) {
	corrId := driverctx.CorrelationIdFromContext(ctx)
	log := logger.WithContext(c.id, corrId, opHandle.Id())
	ctx = driverctx.NewContextWithConnId(ctx, c.id)
	newCtx := driverctx.NewContextWithCorrelationId(driverctx.NewContextWithConnId(context.Background(), c.id), corrId)
	pollSentinel := sentinel.Sentinel{
		OnDoneFn: func(statusResp any) (any, error) {
			return statusResp, nil
		},
		StatusFn: func() (sentinel.Done, any, error) {
			log.Debug().Msg("databricks: polling status")
			statusResp, err := c.client.GetExecutionStatus(newCtx, &client.GetExecutionStatusReq{
				ExecutionHandle: opHandle,
			})
			if statusResp != nil {
				log.Debug().Msgf("databricks: execution state %s", statusResp.ExecutionState)
			}
			return func() bool {
				if err != nil {
					return true
				}
				switch statusResp.ExecutionState {
				case cli_service.TOperationState_INITIALIZED_STATE.String(),
					cli_service.TOperationState_PENDING_STATE.String(),
					cli_service.TOperationState_RUNNING_STATE.String():
					return false
				default:
					log.Debug().Msg("databricks: polling done")
					return true
				}
			}, statusResp, err
		},
		OnCancelFn: func() (any, error) {
			log.Debug().Msg("databricks: canceling query")
			ret, err := c.client.CancelExecution(newCtx, &client.CancelExecutionReq{
				ExecutionHandle: opHandle,
			})
			return ret, err
		},
	}
	var statusResp *client.GetExecutionStatusResp
	_, resp, err := pollSentinel.Watch(ctx, c.cfg.PollInterval, 0)
	if err != nil {
		return nil, wrapErr(err, "failed to poll query state")
	}
	statusResp, ok := resp.(*client.GetExecutionStatusResp)
	if !ok {
		return nil, errors.New("could not read query status")
	}
	return &statusResp.ExecutionStatus, nil
}

var _ driver.Conn = (*conn)(nil)
var _ driver.Pinger = (*conn)(nil)
var _ driver.SessionResetter = (*conn)(nil)
var _ driver.Validator = (*conn)(nil)
var _ driver.ExecerContext = (*conn)(nil)
var _ driver.QueryerContext = (*conn)(nil)
var _ driver.ConnPrepareContext = (*conn)(nil)
var _ driver.ConnBeginTx = (*conn)(nil)
