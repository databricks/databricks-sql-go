package dbsql

import (
	"context"
	"database/sql/driver"
	"time"

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
	client  cli_service.TCLIService
	session *cli_service.TOpenSessionResp
}

// The driver does not really implement prepared statements.
func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return &stmt{conn: c, query: query}, nil
}

// The driver does not really implement prepared statements.
func (c *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return &stmt{conn: c, query: query}, nil
}

func (c *conn) Close() error {
	log := logger.WithContext(c.id, "", "")
	ctx := driverctx.NewContextWithConnId(context.Background(), c.id)
	sentinel := sentinel.Sentinel{
		OnDoneFn: func(statusResp any) (any, error) {
			return c.client.CloseSession(ctx, &cli_service.TCloseSessionReq{
				SessionHandle: c.session.SessionHandle,
			})
		},
	}
	_, _, err := sentinel.Watch(ctx, c.cfg.PollInterval, 15*time.Second)
	if err != nil {
		log.Err(err).Msg("databricks: failed to close connection")
		return wrapErr(err, "failed to close connection")
	}
	return nil
}

// Not supported in Databricks
func (c *conn) Begin() (driver.Tx, error) {
	return nil, errors.New(ErrTransactionsNotSupported)
}

// Not supported in Databricks
func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return nil, errors.New(ErrTransactionsNotSupported)
}

func (c *conn) Ping(ctx context.Context) error {
	log := logger.WithContext(c.id, driverctx.CorrelationIdFromContext(ctx), "")
	ctx = driverctx.NewContextWithConnId(ctx, c.id)
	ctx1, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	_, err := c.QueryContext(ctx1, "select 1", nil)
	if err != nil {
		log.Err(err).Msg("databricks: failed to ping")
		return wrapErr(err, "failed to ping")
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
// Statement ExecContext is the same as connection ExecContext
func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	log := logger.WithContext(c.id, driverctx.CorrelationIdFromContext(ctx), "")
	defer log.Duration(logger.Track("ExecContext"))
	ctx = driverctx.NewContextWithConnId(ctx, c.id)
	if len(args) > 0 {
		return nil, errors.New(ErrParametersNotSupported)
	}
	exStmtResp, opStatusResp, err := c.runQuery(ctx, query, args)

	if exStmtResp != nil && exStmtResp.OperationHandle != nil {
		log = logger.WithContext(c.id, driverctx.CorrelationIdFromContext(ctx), client.SprintGuid(exStmtResp.OperationHandle.OperationId.GUID))
	}

	if err != nil {
		log.Err(err).Msgf("databricks: failed to execute query: query %s", query)
		return nil, wrapErrf(err, "failed to execute query")
	}
	res := result{AffectedRows: opStatusResp.GetNumModifiedRows()}

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

	if exStmtResp != nil && exStmtResp.OperationHandle != nil {
		log = logger.WithContext(c.id, driverctx.CorrelationIdFromContext(ctx), client.SprintGuid(exStmtResp.OperationHandle.OperationId.GUID))
	}
	defer log.Duration(msg, start)

	if err != nil {
		log.Err(err).Msgf("databricks: failed to run query: query %s", query)
		return nil, wrapErrf(err, "failed to run query")
	}
	// hold on to the operation handle
	opHandle := exStmtResp.OperationHandle

	rows := rows{
		connId:        c.id,
		correlationId: corrId,
		client:        c.client,
		opHandle:      opHandle,
		pageSize:      int64(c.cfg.MaxRows),
		location:      c.cfg.Location,
	}

	if exStmtResp.DirectResults != nil {
		// return results
		rows.fetchResults = exStmtResp.DirectResults.ResultSet
		rows.fetchResultsMetadata = exStmtResp.DirectResults.ResultSetMetadata

	}
	return &rows, nil

}

func (c *conn) runQuery(ctx context.Context, query string, args []driver.NamedValue) (*cli_service.TExecuteStatementResp, *cli_service.TGetOperationStatusResp, error) {
	log := logger.WithContext(c.id, driverctx.CorrelationIdFromContext(ctx), "")
	// first we try to get the results synchronously.
	// at any point in time that the context is done we must cancel and return
	exStmtResp, err := c.executeStatement(ctx, query, args)

	if err != nil {
		return exStmtResp, nil, err
	}
	// hold on to the operation handle
	opHandle := exStmtResp.OperationHandle
	if opHandle != nil && opHandle.OperationId != nil {
		log = logger.WithContext(c.id, driverctx.CorrelationIdFromContext(ctx), client.SprintGuid(opHandle.OperationId.GUID))
	}

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
			logBadQueryState(log, opStatus)
			return exStmtResp, opStatus, errors.New(opStatus.GetDisplayMessage())
		// live states
		case cli_service.TOperationState_INITIALIZED_STATE, cli_service.TOperationState_PENDING_STATE, cli_service.TOperationState_RUNNING_STATE:
			statusResp, err := c.pollOperation(ctx, opHandle)
			if err != nil {
				return exStmtResp, statusResp, err
			}
			switch statusResp.GetOperationState() {
			// terminal states
			// good
			case cli_service.TOperationState_FINISHED_STATE:
				// return handle to fetch results later
				return exStmtResp, opStatus, nil
			// bad
			case cli_service.TOperationState_CANCELED_STATE, cli_service.TOperationState_CLOSED_STATE, cli_service.TOperationState_ERROR_STATE, cli_service.TOperationState_TIMEDOUT_STATE:
				logBadQueryState(log, statusResp)
				return exStmtResp, opStatus, errors.New(statusResp.GetDisplayMessage())
				// live states
			default:
				logBadQueryState(log, statusResp)
				return exStmtResp, opStatus, errors.New("invalid operation state. This should not have happened")
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
		switch statusResp.GetOperationState() {
		// terminal states
		// good
		case cli_service.TOperationState_FINISHED_STATE:
			// return handle to fetch results later
			return exStmtResp, statusResp, nil
		// bad
		case cli_service.TOperationState_CANCELED_STATE, cli_service.TOperationState_CLOSED_STATE, cli_service.TOperationState_ERROR_STATE, cli_service.TOperationState_TIMEDOUT_STATE:
			logBadQueryState(log, statusResp)
			return exStmtResp, statusResp, errors.New(statusResp.GetDisplayMessage())
			// live states
		default:
			logBadQueryState(log, statusResp)
			return exStmtResp, statusResp, errors.New("invalid operation state. This should not have happened")
		}
	}
}

func logBadQueryState(log *logger.DBSQLLogger, opStatus *cli_service.TGetOperationStatusResp) {
	log.Error().Msgf("databricks: query state: %s", opStatus.GetOperationState())
	log.Error().Msg(opStatus.GetErrorMessage())
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
			ctx = driverctx.NewContextWithConnId(ctx, c.id)
			resp, err := c.client.ExecuteStatement(ctx, &req)
			return resp, wrapErr(err, "failed to execute statement")
		},
	}
	_, res, err := sentinel.Watch(ctx, c.cfg.PollInterval, 0)
	if err != nil {
		return nil, err
	}
	exStmtResp, ok := res.(*cli_service.TExecuteStatementResp)
	if !ok {
		return exStmtResp, errors.New("databricks: invalid execute statement response")
	}
	return exStmtResp, err
}

func (c *conn) pollOperation(ctx context.Context, opHandle *cli_service.TOperationHandle) (*cli_service.TGetOperationStatusResp, error) {
	corrId := driverctx.CorrelationIdFromContext(ctx)
	log := logger.WithContext(c.id, corrId, client.SprintGuid(opHandle.OperationId.GUID))
	var statusResp *cli_service.TGetOperationStatusResp
	ctx = driverctx.NewContextWithConnId(ctx, c.id)
	newCtx := driverctx.NewContextWithCorrelationId(driverctx.NewContextWithConnId(context.Background(), c.id), corrId)
	pollSentinel := sentinel.Sentinel{
		OnDoneFn: func(statusResp any) (any, error) {
			return statusResp, nil
		},
		StatusFn: func() (sentinel.Done, any, error) {
			var err error
			log.Debug().Msg("databricks: polling status")
			statusResp, err = c.client.GetOperationStatus(newCtx, &cli_service.TGetOperationStatusReq{
				OperationHandle: opHandle,
			})
			log.Debug().Msgf("databricks: status %s", statusResp.GetOperationState().String())
			return func() bool {
				// which other states?
				switch statusResp.GetOperationState() {
				case cli_service.TOperationState_INITIALIZED_STATE, cli_service.TOperationState_PENDING_STATE, cli_service.TOperationState_RUNNING_STATE:
					return false
				default:
					log.Debug().Msg("databricks: polling done")
					return true
				}
			}, statusResp, err
		},
		OnCancelFn: func() (any, error) {
			log.Debug().Msg("databricks: canceling query")
			ret, err := c.client.CancelOperation(newCtx, &cli_service.TCancelOperationReq{
				OperationHandle: opHandle,
			})
			return ret, err
		},
	}
	_, resp, err := pollSentinel.Watch(ctx, c.cfg.PollInterval, 0)
	if err != nil {
		return nil, wrapErr(err, "failed to poll query state")
	}
	statusResp, ok := resp.(*cli_service.TGetOperationStatusResp)
	if !ok {
		return nil, errors.New("could not read query status")
	}
	return statusResp, nil
}

var _ driver.Conn = (*conn)(nil)
var _ driver.Pinger = (*conn)(nil)
var _ driver.SessionResetter = (*conn)(nil)
var _ driver.Validator = (*conn)(nil)
var _ driver.ExecerContext = (*conn)(nil)
var _ driver.QueryerContext = (*conn)(nil)
var _ driver.ConnPrepareContext = (*conn)(nil)
var _ driver.ConnBeginTx = (*conn)(nil)
