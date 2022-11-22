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

type Conn struct {
	id        string
	cfg       *config.Config
	client    cli_service.TCLIService
	session   *cli_service.TOpenSessionResp
	execution *Execution
}

// The driver does not really implement prepared statements.
func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return &stmt{conn: c, query: query}, nil
}

// The driver does not really implement prepared statements.
func (c *Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return &stmt{conn: c, query: query}, nil
}

func (c *Conn) Close() error {
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
func (c *Conn) Begin() (driver.Tx, error) {
	return nil, errors.New(ErrTransactionsNotSupported)
}

// Not supported in Databricks
func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return nil, errors.New(ErrTransactionsNotSupported)
}

func (c *Conn) Ping(ctx context.Context) error {
	log := logger.WithContext(c.id, driverctx.CorrelationIdFromContext(ctx), "")
	ctx = driverctx.NewContextWithConnId(ctx, c.id)
	ctx1, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	_, err := c.QueryContext(ctx1, "select 1", nil)
	if err != nil {
		log.Err(err).Msg("databricks: failed to ping")
		return driver.ErrBadConn
	}
	return nil
}

// Implementation of SessionResetter
func (c *Conn) ResetSession(ctx context.Context) error {
	// For now our session does not have any important state to reset before re-use
	c.execution = nil
	return nil
}

func (c *Conn) IsValid() bool {
	return c.session.GetStatus().StatusCode == cli_service.TStatusCode_SUCCESS_STATUS
}

// ExecContext executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
//
// ExecContext honors the context timeout and return when it is canceled.
// Statement ExecContext is the same as connection ExecContext
func (c *Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	log := logger.WithContext(c.id, driverctx.CorrelationIdFromContext(ctx), "")
	msg, start := logger.Track("ExecContext")
	ctx = driverctx.NewContextWithConnId(ctx, c.id)
	if len(args) > 0 {
		return nil, errors.New(ErrParametersNotSupported)
	}
	exStmtResp, opStatusResp, err := c.runQuery(ctx, query, args)

	if exStmtResp != nil && exStmtResp.OperationHandle != nil {
		log = logger.WithContext(c.id, driverctx.CorrelationIdFromContext(ctx), client.SprintGuid(exStmtResp.OperationHandle.OperationId.GUID))
	}
	defer log.Duration(msg, start)

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
func (c *Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	corrId := driverctx.CorrelationIdFromContext(ctx)
	log := logger.WithContext(c.id, corrId, "")
	msg, start := log.Track("QueryContext")

	ctx = driverctx.NewContextWithConnId(ctx, c.id)
	if len(args) > 0 {
		return nil, errors.New(ErrParametersNotSupported)
	}
	if query == "" && c.execution != nil {
		opHandle := &cli_service.TOperationHandle{
			OperationId: &cli_service.THandleIdentifier{
				GUID:   client.DecodeGuid(c.execution.Id),
				Secret: c.execution.Secret,
			},
			HasResultSet:  c.execution.HasResultSet,
			OperationType: cli_service.TOperationType_EXECUTE_STATEMENT,
		}
		rows := rows{
			connId:        c.id,
			correlationId: corrId,
			client:        c.client,
			opHandle:      opHandle,
			pageSize:      int64(c.cfg.MaxRows),
			location:      c.cfg.Location,
		}
		return &rows, nil
	} else {

		// first we try to get the results synchronously.
		// at any point in time that the context is done we must cancel and return
		exStmtResp, opStatus, err := c.runQuery(ctx, query, args)

		execId := ""
		execStatus := "UNKNOWN"

		if opStatus != nil {
			execStatus = opStatus.GetOperationState().String()
		}
		// hold on to the operation handle
		opHandle := exStmtResp.OperationHandle

		execId = client.SprintGuid(exStmtResp.OperationHandle.OperationId.GUID)
		log = logger.WithContext(c.id, driverctx.CorrelationIdFromContext(ctx), execId)

		defer log.Duration(msg, start)

		if err != nil {
			log.Err(err).Msgf("databricks: failed to run query: query %s", query)
			return nil, wrapErrf(err, "failed to run query")
		}

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

		execPtr := execFromContext(ctx)
		*execPtr = Execution{
			Id:           execId,
			Status:       execStatus,
			Secret:       opHandle.OperationId.Secret,
			HasResultSet: opHandle.HasResultSet,
		}
		return &rows, nil
	}

}

func (c *Conn) runQuery(ctx context.Context, query string, args []driver.NamedValue) (*cli_service.TExecuteStatementResp, *cli_service.TGetOperationStatusResp, error) {

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
			if !c.cfg.RunAsync {

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
			} else {
				return exStmtResp, opStatus, nil
			}
		// weird states
		default:
			logBadQueryState(log, opStatus)
			return exStmtResp, opStatus, errors.New("invalid operation state. This should not have happened")
		}

	} else {
		if !c.cfg.RunAsync {
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
		} else {
			return exStmtResp, nil, nil
		}
	}
}

func logBadQueryState(log *logger.DBSQLLogger, opStatus *cli_service.TGetOperationStatusResp) {
	log.Error().Msgf("databricks: query state: %s", opStatus.GetOperationState())
	log.Error().Msg(opStatus.GetErrorMessage())
}

func (c *Conn) executeStatement(ctx context.Context, query string, args []driver.NamedValue) (*cli_service.TExecuteStatementResp, error) {
	corrId := driverctx.CorrelationIdFromContext(ctx)
	log := logger.WithContext(c.id, corrId, "")
	sentinel := sentinel.Sentinel{
		OnDoneFn: func(statusResp any) (any, error) {
			req := cli_service.TExecuteStatementReq{
				SessionHandle: c.session.SessionHandle,
				Statement:     query,
				RunAsync:      true,
				QueryTimeout:  int64(c.cfg.QueryTimeout / time.Second),
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
		OnCancelFn: func() (any, error) {
			log.Warn().Msg("databricks: execute statement canceled while creation operation")
			return nil, nil
		},
	}
	_, res, err := sentinel.Watch(ctx, c.cfg.PollInterval, c.cfg.QueryTimeout)
	if err != nil {
		return nil, err
	}
	exStmtResp, ok := res.(*cli_service.TExecuteStatementResp)
	if !ok {
		return exStmtResp, errors.New("databricks: invalid execute statement response")
	}
	return exStmtResp, err
}

func (c *Conn) pollOperation(ctx context.Context, opHandle *cli_service.TOperationHandle) (*cli_service.TGetOperationStatusResp, error) {
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
			if statusResp != nil && statusResp.OperationState != nil {
				log.Debug().Msgf("databricks: status %s", statusResp.GetOperationState().String())
			}
			return func() bool {
				// which other states?
				if err != nil {
					return true
				}
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

func (c *Conn) cancelOperation(ctx context.Context, execution Execution) error {
	req := cli_service.TCancelOperationReq{
		OperationHandle: &cli_service.TOperationHandle{
			OperationId: &cli_service.THandleIdentifier{
				GUID:   client.DecodeGuid(execution.Id),
				Secret: execution.Secret,
			},
			OperationType: cli_service.TOperationType_EXECUTE_STATEMENT,
			HasResultSet:  execution.HasResultSet,
		},
	}
	_, err := c.client.CancelOperation(ctx, &req)
	return err
}

func (c *Conn) getOperationStatus(ctx context.Context, execution Execution) (Execution, error) {
	statusResp, err := c.client.GetOperationStatus(ctx, &cli_service.TGetOperationStatusReq{
		OperationHandle: &cli_service.TOperationHandle{
			OperationId: &cli_service.THandleIdentifier{
				GUID:   client.DecodeGuid(execution.Id),
				Secret: execution.Secret,
			},
			OperationType: cli_service.TOperationType_EXECUTE_STATEMENT,
			HasResultSet:  execution.HasResultSet,
		},
	})
	if err != nil {
		return execution, err
	}
	exRet := Execution{
		Status:       statusResp.GetOperationState().String(),
		Id:           execution.Id,
		Secret:       execution.Secret,
		HasResultSet: execution.HasResultSet,
	}
	return exRet, nil
}

func (c *Conn) CheckNamedValue(nv *driver.NamedValue) error {
	ex, ok := nv.Value.(Execution)
	if ok {
		c.execution = &ex
		return driver.ErrRemoveArgument
	}
	return nil
}

var _ driver.Conn = (*Conn)(nil)
var _ driver.Pinger = (*Conn)(nil)
var _ driver.SessionResetter = (*Conn)(nil)
var _ driver.Validator = (*Conn)(nil)
var _ driver.ExecerContext = (*Conn)(nil)
var _ driver.QueryerContext = (*Conn)(nil)
var _ driver.ConnPrepareContext = (*Conn)(nil)
var _ driver.ConnBeginTx = (*Conn)(nil)
