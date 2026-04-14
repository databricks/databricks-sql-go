package dbsql

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/databricks/databricks-sql-go/driverctx"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	context2 "github.com/databricks/databricks-sql-go/internal/compat/context"
	"github.com/databricks/databricks-sql-go/internal/config"
	dbsqlerrint "github.com/databricks/databricks-sql-go/internal/errors"
	"github.com/databricks/databricks-sql-go/internal/rows"
	"github.com/databricks/databricks-sql-go/internal/sentinel"
	"github.com/databricks/databricks-sql-go/internal/thrift_protocol"
	"github.com/databricks/databricks-sql-go/logger"
	"github.com/databricks/databricks-sql-go/telemetry"
	"github.com/pkg/errors"
)

type conn struct {
	id        string
	cfg       *config.Config
	client    cli_service.TCLIService
	session   *cli_service.TOpenSessionResp
	telemetry *telemetry.Interceptor // Optional telemetry interceptor
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

	// Time CloseSession so we can record DELETE_SESSION before flushing telemetry
	closeStart := time.Now()
	_, err := c.client.CloseSession(ctx, &cli_service.TCloseSessionReq{
		SessionHandle: c.session.SessionHandle,
	})

	// Record DELETE_SESSION regardless of error (matches JDBC), then flush and release
	if c.telemetry != nil {
		c.telemetry.RecordOperation(ctx, c.id, "", telemetry.OperationTypeDeleteSession, time.Since(closeStart).Milliseconds(), err)
		_ = c.telemetry.Close(ctx)
		telemetry.ReleaseForConnection(c.cfg.Host)
	}

	if err != nil {
		log.Err(err).Msg("databricks: failed to close connection")
		return dbsqlerrint.NewBadConnectionError(err)
	}
	return nil
}

// Not supported in Databricks.
func (c *conn) Begin() (driver.Tx, error) {
	return nil, dbsqlerrint.NewDriverError(context.TODO(), dbsqlerr.ErrNotImplemented, nil)
}

// Not supported in Databricks.
func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return nil, dbsqlerrint.NewDriverError(context.TODO(), dbsqlerr.ErrNotImplemented, nil)
}

// Ping attempts to verify that the server is accessible.
// Returns ErrBadConn if ping fails and consequently DB.Ping will remove the conn from the pool.
func (c *conn) Ping(ctx context.Context) error {
	ctx = driverctx.NewContextWithConnId(ctx, c.id)
	log, _ := client.LoggerAndContext(ctx, nil)
	log.Debug().Msg("databricks: pinging")

	ctx1, cancel := context.WithTimeout(ctx, c.cfg.PingTimeout)
	defer cancel()
	rows, err := c.QueryContext(ctx1, "select 1", nil)
	if err != nil {
		log.Err(err).Msg("databricks: failed to ping")
		return dbsqlerrint.NewBadConnectionError(err)
	}
	defer rows.Close() //nolint:errcheck

	log.Debug().Msg("databricks: ping successful")
	return nil
}

// ResetSession is called prior to executing a query on the connection.
// The session with this driver does not have any important state to reset before re-use.
func (c *conn) ResetSession(ctx context.Context) error {
	return nil
}

// IsValid signals whether a connection is valid or if it should be discarded.
func (c *conn) IsValid() bool {
	return c.session.GetStatus().StatusCode == cli_service.TStatusCode_SUCCESS_STATUS
}

// ExecContext executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
//
// ExecContext honors the context timeout and return when it is canceled.
// Statement ExecContext is the same as connection ExecContext
func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	ctx = driverctx.NewContextWithConnId(ctx, c.id)
	log, _ := client.LoggerAndContext(ctx, nil)
	msg, start := logger.Track("ExecContext")
	defer log.Duration(msg, start)

	corrId := driverctx.CorrelationIdFromContext(ctx)

	// Capture execution start time for telemetry before running the query
	executeStart := time.Now()
	exStmtResp, opStatusResp, err := c.runQuery(ctx, query, args)
	log, ctx = client.LoggerAndContext(ctx, exStmtResp)

	// Telemetry: set up metric context BEFORE staging operation so that the
	// staging op's telemetryUpdate callback can attach tags to the metric context.
	var statementID string
	var closeOpErr error // Track CloseOperation errors for telemetry
	if c.telemetry != nil && exStmtResp != nil && exStmtResp.OperationHandle != nil && exStmtResp.OperationHandle.OperationId != nil {
		statementID = client.SprintGuid(exStmtResp.OperationHandle.OperationId.GUID)
		ctx = c.telemetry.BeforeExecuteWithTime(ctx, c.id, statementID, executeStart)
		c.telemetry.AddTag(ctx, telemetry.TagOperationType, telemetry.OperationTypeExecuteStatement)
	}

	stagingErr := c.execStagingOperation(exStmtResp, ctx)

	if c.telemetry != nil && statementID != "" {
		defer func() {
			finalErr := err
			if stagingErr != nil {
				finalErr = stagingErr
			}
			// Include CloseOperation error in telemetry if it occurred
			if closeOpErr != nil && finalErr == nil {
				finalErr = closeOpErr
			}
			c.telemetry.AfterExecute(ctx, finalErr)
			c.telemetry.CompleteStatement(ctx, statementID, finalErr != nil)
		}()
	}

	if exStmtResp != nil && exStmtResp.OperationHandle != nil {
		// since we have an operation handle we can close the operation if necessary
		alreadyClosed := exStmtResp.DirectResults != nil && exStmtResp.DirectResults.CloseOperation != nil
		newCtx := driverctx.NewContextWithCorrelationId(driverctx.NewContextWithConnId(context.Background(), c.id), corrId)
		if !alreadyClosed && (opStatusResp == nil || opStatusResp.GetOperationState() != cli_service.TOperationState_CLOSED_STATE) {
			closeOpStart := time.Now()
			_, err1 := c.client.CloseOperation(newCtx, &cli_service.TCloseOperationReq{
				OperationHandle: exStmtResp.OperationHandle,
			})
			if c.telemetry != nil {
				c.telemetry.RecordOperation(ctx, c.id, statementID, telemetry.OperationTypeCloseStatement, time.Since(closeOpStart).Milliseconds(), err1)
			}
			if err1 != nil {
				log.Err(err1).Msg("databricks: failed to close operation after executing statement")
				closeOpErr = err1 // Capture for telemetry
			}
		}
	}

	if err != nil {
		log.Err(err).Msgf("databricks: failed to execute query: query %s", query)
		return nil, dbsqlerrint.NewExecutionError(ctx, dbsqlerr.ErrQueryExecution, err, opStatusResp)
	}

	if stagingErr != nil {
		log.Err(stagingErr).Msgf("databricks: failed to execute query: query %s", query)
		return nil, dbsqlerrint.NewExecutionError(ctx, dbsqlerr.ErrQueryExecution, stagingErr, opStatusResp)
	}

	res := result{AffectedRows: opStatusResp.GetNumModifiedRows()}

	return &res, nil
}

// chunkTimingAccumulator aggregates per-chunk fetch latencies for telemetry.
// It tracks the initial, slowest, and cumulative latencies, plus the number
// of CloudFetch file downloads. All fields should be accessed under the
// serialization provided by database/sql's closemu (see QueryContext).
type chunkTimingAccumulator struct {
	initialMs  int64
	slowestMs  int64
	sumMs      int64
	initialSet bool
	// cloudFetchFileCount counts individual S3 files downloaded via CloudFetch.
	// Used to set chunk_total_present correctly for both bulk and paginated CloudFetch:
	//   - paginated CF (1 link/FetchResults): file count == page count == correct total
	//   - bulk CF (all links in DirectResults): file count == actual S3 downloads
	// For inline ArrowBatch results this stays 0 and chunk_total_present falls back to chunkCount.
	cloudFetchFileCount int
}

// record accumulates a single chunk or download latency. Returns true if
// the latency was positive and tags should be updated; false otherwise.
func (a *chunkTimingAccumulator) record(latencyMs int64) bool {
	if latencyMs <= 0 {
		return false
	}
	if !a.initialSet {
		a.initialMs = latencyMs
		a.initialSet = true
	}
	if latencyMs > a.slowestMs {
		a.slowestMs = latencyMs
	}
	a.sumMs += latencyMs
	return true
}

// applyTags writes the current timing state to the telemetry context.
func (a *chunkTimingAccumulator) applyTags(ctx context.Context, interceptor *telemetry.Interceptor) {
	interceptor.AddTag(ctx, telemetry.TagChunkInitialLatencyMs, a.initialMs)
	interceptor.AddTag(ctx, telemetry.TagChunkSlowestLatencyMs, a.slowestMs)
	interceptor.AddTag(ctx, telemetry.TagChunkSumLatencyMs, a.sumMs)
}

// QueryContext executes a query that may return rows, such as a
// SELECT.
//
// QueryContext honors the context timeout and return when it is canceled.
// Statement QueryContext is the same as connection QueryContext
func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	ctx = driverctx.NewContextWithConnId(ctx, c.id)
	log, _ := client.LoggerAndContext(ctx, nil)
	msg, start := log.Track("QueryContext")

	// first we try to get the results synchronously.
	// at any point in time that the context is done we must cancel and return

	// Capture execution start time for telemetry before running the query
	executeStart := time.Now()
	exStmtResp, opStatusResp, err := c.runQuery(ctx, query, args)
	log, ctx = client.LoggerAndContext(ctx, exStmtResp)
	defer log.Duration(msg, start)

	// Telemetry: set up metric context for the statement.
	// BeforeExecuteWithTime anchors startTime to before runQuery() ran.
	var statementID string
	if c.telemetry != nil && exStmtResp != nil && exStmtResp.OperationHandle != nil && exStmtResp.OperationHandle.OperationId != nil {
		statementID = client.SprintGuid(exStmtResp.OperationHandle.OperationId.GUID)
		ctx = c.telemetry.BeforeExecuteWithTime(ctx, c.id, statementID, executeStart)
		c.telemetry.AddTag(ctx, telemetry.TagOperationType, telemetry.OperationTypeExecuteStatement)
	}

	if err != nil {
		// Error path: finalize and emit the EXECUTE_STATEMENT metric immediately —
		// there are no rows to iterate so the metric is complete right now.
		if c.telemetry != nil && statementID != "" {
			c.telemetry.AfterExecute(ctx, err)
			c.telemetry.CompleteStatement(ctx, statementID, true)
		}
		log.Err(err).Msg("databricks: failed to run query") // To log query we need to redact credentials
		return nil, dbsqlerrint.NewExecutionError(ctx, dbsqlerr.ErrQueryExecution, err, opStatusResp)
	}

	// Success path: freeze execute latency NOW (before row iteration inflates time.Since).
	// AfterExecute/CompleteStatement are called from closeCallback after all chunks
	// are fetched, so the final metric carries complete chunk timing data.
	if c.telemetry != nil && statementID != "" {
		c.telemetry.FinalizeLatency(ctx)
	}

	// chunkTimingAccumulator aggregates per-chunk fetch latencies across all
	// fetchResultPage calls. These fields are safe without a mutex because they
	// are only mutated from callbacks serialized by database/sql's closemu lock:
	// telemetryUpdate and cloudFetchCallback run inside rows.Next() (which
	// holds closemu.RLock), and closeCallback runs inside rows.Close() (which
	// holds closemu.Lock). This ensures mutual exclusion even when Close() is
	// called from database/sql's awaitDone goroutine on context cancellation.
	var timing chunkTimingAccumulator

	// Telemetry callback invoked after each result page is fetched.
	telemetryUpdate := func(chunkCount int, bytesDownloaded int64, chunkIndex int, chunkLatencyMs int64, _ int32) {
		if c.telemetry == nil {
			return
		}
		c.telemetry.AddTag(ctx, telemetry.TagResultChunkCount, chunkCount)
		c.telemetry.AddTag(ctx, telemetry.TagResultBytesDownloaded, bytesDownloaded)

		// Aggregate per-chunk fetch latencies (skip direct results where latency is 0).
		if timing.record(chunkLatencyMs) {
			timing.applyTags(ctx, c.telemetry)
		}
		// chunk_total_present is set definitively in closeCallback once all pages are known.
	}

	// cloudFetchCallback is invoked per S3 file download for CloudFetch result sets.
	// It aggregates individual file download times into the same initial/slowest/sum vars
	// used for inline chunk timing, matching JDBC's per-chunk HTTP GET timing model.
	// For inline (non-CloudFetch) result sets this is never called.
	var cloudFetchCallback func(downloadMs int64)
	if c.telemetry != nil {
		cloudFetchCallback = func(downloadMs int64) {
			timing.cloudFetchFileCount++ // always count files for chunk_total_present, even sub-ms downloads
			if timing.record(downloadMs) {
				timing.applyTags(ctx, c.telemetry)
			}
		}
	}

	// closeCallback is invoked from rows.Close() after all rows have been consumed.
	// At that point chunk timing is fully accumulated in ctx tags, so we finalize
	// EXECUTE_STATEMENT here rather than at QueryContext return time.
	//
	// We capture a detached context (background with telemetry values) so that
	// telemetry is still recorded even if the caller's ctx has been cancelled
	// (e.g. query timeout or context.WithCancel from database/sql's awaitDone).
	var closeCallback func(latencyMs int64, chunkCount int, iterErr error, closeErr error)
	if c.telemetry != nil && statementID != "" {
		interceptor := c.telemetry
		connID := c.id
		stmtID := statementID
		// Detach from caller's context so cancellation doesn't lose telemetry.
		telemetryCtx := context2.WithoutCancel(ctx)
		closeCallback = func(latencyMs int64, chunkCount int, iterErr error, closeErr error) {
			// Set chunk_total_present to the definitive total now that all iteration is done.
			// For CloudFetch, use cloudFetchFileCount (actual S3 downloads) — this handles
			// both paginated CF (1 link/page, so file count == page count) and bulk CF
			// (all links in DirectResults, so file count == total S3 files).
			// For inline ArrowBatch, cloudFetchFileCount is 0; fall back to chunkCount.
			if timing.cloudFetchFileCount > 0 {
				interceptor.AddTag(telemetryCtx, telemetry.TagChunkTotalPresent, timing.cloudFetchFileCount)
			} else if chunkCount > 0 {
				interceptor.AddTag(telemetryCtx, telemetry.TagChunkTotalPresent, chunkCount)
			}
			// EXECUTE_STATEMENT uses the iteration error (row consumption failure)
			// to correctly report whether the statement succeeded or failed.
			interceptor.AfterExecute(telemetryCtx, iterErr)
			interceptor.CompleteStatement(telemetryCtx, stmtID, iterErr != nil)
			// CLOSE_STATEMENT uses the actual CloseOperation RPC error.
			interceptor.RecordOperation(telemetryCtx, connID, stmtID, telemetry.OperationTypeCloseStatement, latencyMs, closeErr)
		}
	} else if c.telemetry != nil {
		interceptor := c.telemetry
		connID := c.id
		telemetryCtx := context2.WithoutCancel(ctx)
		closeCallback = func(latencyMs int64, _ int, _ error, closeErr error) {
			interceptor.RecordOperation(telemetryCtx, connID, "", telemetry.OperationTypeCloseStatement, latencyMs, closeErr)
		}
	}

	rows, err := rows.NewRows(ctx, exStmtResp.OperationHandle, c.client, c.cfg, exStmtResp.DirectResults, &rows.TelemetryCallbacks{
		OnChunkFetched:   telemetryUpdate,
		OnClose:          closeCallback,
		OnCloudFetchFile: cloudFetchCallback,
	})
	return rows, err

}

func (c *conn) runQuery(ctx context.Context, query string, args []driver.NamedValue) (*cli_service.TExecuteStatementResp, *cli_service.TGetOperationStatusResp, error) {
	// first we try to get the results synchronously.
	// at any point in time that the context is done we must cancel and return
	exStmtResp, err := c.executeStatement(ctx, query, args)
	var log *logger.DBSQLLogger
	log, ctx = client.LoggerAndContext(ctx, exStmtResp)

	if err != nil {
		return exStmtResp, nil, err
	}

	opHandle := exStmtResp.OperationHandle

	if exStmtResp.DirectResults != nil {
		opStatus := exStmtResp.DirectResults.GetOperationStatus()

		switch opStatus.GetOperationState() {
		// terminal states
		// good
		case cli_service.TOperationState_FINISHED_STATE:
			return exStmtResp, opStatus, nil
		// bad
		case cli_service.TOperationState_CANCELED_STATE,
			cli_service.TOperationState_CLOSED_STATE,
			cli_service.TOperationState_ERROR_STATE,
			cli_service.TOperationState_TIMEDOUT_STATE:
			logBadQueryState(log, opStatus)
			return exStmtResp, opStatus, unexpectedOperationState(opStatus)
		// live states
		case cli_service.TOperationState_INITIALIZED_STATE,
			cli_service.TOperationState_PENDING_STATE,
			cli_service.TOperationState_RUNNING_STATE:
			statusResp, err := c.pollOperation(ctx, opHandle)
			if err != nil {
				return exStmtResp, statusResp, err
			}
			switch statusResp.GetOperationState() {
			// terminal states
			// good
			case cli_service.TOperationState_FINISHED_STATE:
				return exStmtResp, statusResp, nil
			// bad
			case cli_service.TOperationState_CANCELED_STATE,
				cli_service.TOperationState_CLOSED_STATE,
				cli_service.TOperationState_ERROR_STATE,
				cli_service.TOperationState_TIMEDOUT_STATE:
				logBadQueryState(log, statusResp)
				return exStmtResp, statusResp, unexpectedOperationState(statusResp)
				// live states
			default:
				logBadQueryState(log, statusResp)
				return exStmtResp, statusResp, invalidOperationState(ctx, statusResp)
			}
		// weird states
		default:
			logBadQueryState(log, opStatus)
			return exStmtResp, opStatus, invalidOperationState(ctx, opStatus)
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
			return exStmtResp, statusResp, nil
		// bad
		case cli_service.TOperationState_CANCELED_STATE,
			cli_service.TOperationState_CLOSED_STATE,
			cli_service.TOperationState_ERROR_STATE,
			cli_service.TOperationState_TIMEDOUT_STATE:
			logBadQueryState(log, statusResp)
			return exStmtResp, statusResp, unexpectedOperationState(statusResp)
			// live states
		default:
			logBadQueryState(log, statusResp)
			return exStmtResp, statusResp, invalidOperationState(ctx, statusResp)
		}
	}
}

func logBadQueryState(log *logger.DBSQLLogger, opStatus *cli_service.TGetOperationStatusResp) {
	log.Error().Msgf("databricks: query state: %s", opStatus.GetOperationState())
	log.Error().Msg(opStatus.GetDisplayMessage())
	log.Debug().Msg(opStatus.GetDiagnosticInfo())
}

func unexpectedOperationState(opStatus *cli_service.TGetOperationStatusResp) error {
	return errors.WithMessage(errors.New(opStatus.GetDisplayMessage()), dbsqlerr.ErrUnexpectedOperationState(opStatus.GetOperationState().String()))
}

func invalidOperationState(ctx context.Context, opStatus *cli_service.TGetOperationStatusResp) error {
	return dbsqlerrint.NewDriverError(ctx, dbsqlerr.ErrInvalidOperationState(opStatus.GetOperationState().String()), nil)
}

func (c *conn) executeStatement(ctx context.Context, query string, args []driver.NamedValue) (*cli_service.TExecuteStatementResp, error) {
	ctx = driverctx.NewContextWithConnId(ctx, c.id)

	parameters, err := convertNamedValuesToSparkParams(args)
	if err != nil {
		return nil, err
	}

	req := cli_service.TExecuteStatementReq{
		SessionHandle: c.session.SessionHandle,
		Statement:     query,
		RunAsync:      true,
		QueryTimeout:  int64(c.cfg.QueryTimeout / time.Second),
	}

	// Check protocol version for feature support
	serverProtocolVersion := c.session.ServerProtocolVersion

	// Add direct results if supported
	if thrift_protocol.SupportsDirectResults(serverProtocolVersion) {
		req.GetDirectResults = &cli_service.TSparkGetDirectResults{
			MaxRows: int64(c.cfg.MaxRows),
		}
	}

	// Add LZ4 compression if supported and enabled
	if thrift_protocol.SupportsLz4Compression(serverProtocolVersion) && c.cfg.UseLz4Compression {
		req.CanDecompressLZ4Result_ = &c.cfg.UseLz4Compression
	}

	// Add cloud fetch if supported and enabled
	if thrift_protocol.SupportsCloudFetch(serverProtocolVersion) && c.cfg.UseCloudFetch {
		req.CanDownloadResult_ = &c.cfg.UseCloudFetch
	}

	// Add Arrow support if supported and enabled
	if thrift_protocol.SupportsArrow(serverProtocolVersion) && c.cfg.UseArrowBatches {
		req.CanReadArrowResult_ = &c.cfg.UseArrowBatches
		req.UseArrowNativeTypes = &cli_service.TSparkArrowTypes{
			DecimalAsArrow:       &c.cfg.UseArrowNativeDecimal,
			TimestampAsArrow:     &c.cfg.UseArrowNativeTimestamp,
			ComplexTypesAsArrow:  &c.cfg.UseArrowNativeComplexTypes,
			IntervalTypesAsArrow: &c.cfg.UseArrowNativeIntervalTypes,
		}
	}

	// Add parameters if supported and provided
	if thrift_protocol.SupportsParameterizedQueries(serverProtocolVersion) && len(parameters) > 0 {
		req.Parameters = parameters
	}

	// Add per-statement query tags if provided via context
	if queryTags := driverctx.QueryTagsFromContext(ctx); len(queryTags) > 0 {
		serialized := SerializeQueryTags(queryTags)
		if serialized != "" {
			if req.ConfOverlay == nil {
				req.ConfOverlay = make(map[string]string)
			}
			req.ConfOverlay["query_tags"] = serialized
		}
	}

	resp, err := c.client.ExecuteStatement(ctx, &req)
	var log *logger.DBSQLLogger
	log, ctx = client.LoggerAndContext(ctx, resp)

	var shouldCancel = func(resp *cli_service.TExecuteStatementResp) bool {
		if resp == nil {
			return false
		}
		hasHandle := resp.OperationHandle != nil
		isOpen := resp.DirectResults == nil || resp.DirectResults.CloseOperation == nil
		return hasHandle && isOpen
	}

	select {
	default:
		// Non-blocking check: continue if context not done
	case <-ctx.Done():
		newCtx := driverctx.NewContextFromBackground(ctx)
		// in case context is done, we need to cancel the operation if necessary
		if err == nil && shouldCancel(resp) {
			log.Debug().Msg("databricks: canceling query")
			_, err1 := c.client.CancelOperation(newCtx, &cli_service.TCancelOperationReq{
				OperationHandle: resp.GetOperationHandle(),
			})

			if err1 != nil {
				log.Err(err1).Msgf("databricks: cancel failed")
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

func (c *conn) pollOperation(ctx context.Context, opHandle *cli_service.TOperationHandle) (*cli_service.TGetOperationStatusResp, error) {
	corrId := driverctx.CorrelationIdFromContext(ctx)
	log := logger.WithContext(c.id, corrId, client.SprintGuid(opHandle.OperationId.GUID))
	var statusResp *cli_service.TGetOperationStatusResp
	ctx = driverctx.NewContextWithConnId(ctx, c.id)
	newCtx := context2.WithoutCancel(ctx)
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
				if err != nil {
					return true
				}
				switch statusResp.GetOperationState() {
				case cli_service.TOperationState_INITIALIZED_STATE,
					cli_service.TOperationState_PENDING_STATE,
					cli_service.TOperationState_RUNNING_STATE:
					return false
				default:
					log.Debug().Msg("databricks: polling done")
					return true
				}
			}, statusResp, err
		},
		OnCancelFn: func() (any, error) {
			log.Debug().Msg("databricks: sentinel canceling query")
			ret, err := c.client.CancelOperation(newCtx, &cli_service.TCancelOperationReq{
				OperationHandle: opHandle,
			})
			return ret, err
		},
	}
	status, resp, err := pollSentinel.Watch(ctx, c.cfg.PollInterval, 0)
	if err != nil {
		log.Err(err).Msg("error polling operation status")
		if status == sentinel.WatchTimeout {
			err = dbsqlerrint.NewRequestError(ctx, dbsqlerr.ErrSentinelTimeout, err)
		}
		return nil, err
	}

	statusResp, ok := resp.(*cli_service.TGetOperationStatusResp)
	if !ok {
		return nil, dbsqlerrint.NewDriverError(ctx, dbsqlerr.ErrReadQueryStatus, nil)
	}
	return statusResp, nil
}

func (c *conn) CheckNamedValue(nv *driver.NamedValue) error {
	var err error
	if parameter, ok := nv.Value.(Parameter); ok {
		nv.Name = parameter.Name
		parameter.Value, err = driver.DefaultParameterConverter.ConvertValue(parameter.Value)
		return err
	}

	nv.Value, err = driver.DefaultParameterConverter.ConvertValue(nv.Value)
	return err
}

var _ driver.Conn = (*conn)(nil)
var _ driver.Pinger = (*conn)(nil)
var _ driver.SessionResetter = (*conn)(nil)
var _ driver.Validator = (*conn)(nil)
var _ driver.ExecerContext = (*conn)(nil)
var _ driver.QueryerContext = (*conn)(nil)
var _ driver.ConnPrepareContext = (*conn)(nil)
var _ driver.ConnBeginTx = (*conn)(nil)
var _ driver.NamedValueChecker = (*conn)(nil)

func Succeeded(response *http.Response) bool {
	if response.StatusCode == 200 || response.StatusCode == 201 || response.StatusCode == 202 || response.StatusCode == 204 {
		return true
	}
	return false
}

func (c *conn) handleStagingPut(ctx context.Context, presignedUrl string, headers map[string]string, localFile string) dbsqlerr.DBError {
	if localFile == "" {
		return dbsqlerrint.NewDriverError(ctx, "cannot perform PUT without specifying a local_file", nil)
	}
	client := &http.Client{}

	dat, err := os.Open(localFile) //nolint:gosec // localFile is provided by the application, not user input
	if err != nil {
		return dbsqlerrint.NewDriverError(ctx, "error reading local file", err)
	}
	defer dat.Close() //nolint:errcheck

	info, err := dat.Stat()
	if err != nil {
		return dbsqlerrint.NewDriverError(ctx, "error reading local file info", err)
	}

	req, _ := http.NewRequest("PUT", presignedUrl, dat)
	req.ContentLength = info.Size() // backend actually requires content length to be known

	for k, v := range headers {
		req.Header.Set(k, v)
	}
	res, err := client.Do(req)
	if err != nil {
		return dbsqlerrint.NewDriverError(ctx, "error sending http request", err)
	}
	defer res.Body.Close() //nolint:errcheck
	content, err := io.ReadAll(res.Body)

	if err != nil || !Succeeded(res) {
		return dbsqlerrint.NewDriverError(ctx, fmt.Sprintf("staging operation over HTTP was unsuccessful: %d-%s", res.StatusCode, content), nil)
	}
	return nil

}

func (c *conn) handleStagingGet(ctx context.Context, presignedUrl string, headers map[string]string, localFile string) dbsqlerr.DBError {
	if localFile == "" {
		return dbsqlerrint.NewDriverError(ctx, "cannot perform GET without specifying a local_file", nil)
	}
	client := &http.Client{}
	req, _ := http.NewRequest("GET", presignedUrl, nil)

	for k, v := range headers {
		req.Header.Set(k, v)
	}
	res, err := client.Do(req)
	if err != nil {
		return dbsqlerrint.NewDriverError(ctx, "error sending http request", err)
	}
	defer res.Body.Close() //nolint:errcheck
	content, err := io.ReadAll(res.Body)

	if err != nil || !Succeeded(res) {
		return dbsqlerrint.NewDriverError(ctx, fmt.Sprintf("staging operation over HTTP was unsuccessful: %d-%s", res.StatusCode, content), nil)
	}

	err = os.WriteFile(localFile, content, 0644) //nolint:gosec
	if err != nil {
		return dbsqlerrint.NewDriverError(ctx, "error writing local file", err)
	}
	return nil
}

func (c *conn) handleStagingRemove(ctx context.Context, presignedUrl string, headers map[string]string) dbsqlerr.DBError {
	client := &http.Client{}
	req, _ := http.NewRequest("DELETE", presignedUrl, nil)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	res, err := client.Do(req)
	if err != nil {
		return dbsqlerrint.NewDriverError(ctx, "error sending http request", err)
	}
	defer res.Body.Close() //nolint:errcheck
	content, err := io.ReadAll(res.Body)

	if err != nil || !Succeeded(res) {
		return dbsqlerrint.NewDriverError(ctx, fmt.Sprintf("staging operation over HTTP was unsuccessful: %d-%s, nil", res.StatusCode, content), nil)
	}

	return nil
}

func localPathIsAllowed(stagingAllowedLocalPaths []string, localFile string) bool {
	for i := range stagingAllowedLocalPaths {
		// Convert both filepaths to absolute paths to avoid potential issues.
		//
		path, err := filepath.Abs(stagingAllowedLocalPaths[i])
		if err != nil {
			return false
		}
		localFile, err := filepath.Abs(localFile)
		if err != nil {
			return false
		}
		relativePath, err := filepath.Rel(path, localFile)
		if err != nil {
			return false
		}
		if !strings.Contains(relativePath, "../") {
			return true
		}
	}
	return false
}

func (c *conn) execStagingOperation(
	exStmtResp *cli_service.TExecuteStatementResp,
	ctx context.Context) dbsqlerr.DBError {

	if exStmtResp == nil || exStmtResp.OperationHandle == nil {
		return nil
	}

	var row driver.Rows
	var err error

	var isStagingOperation bool
	if exStmtResp.DirectResults != nil && exStmtResp.DirectResults.ResultSetMetadata != nil {
		isStagingOperation = exStmtResp.DirectResults.ResultSetMetadata.IsStagingOperation != nil && *exStmtResp.DirectResults.ResultSetMetadata.IsStagingOperation
	} else {
		req := cli_service.TGetResultSetMetadataReq{
			OperationHandle: exStmtResp.OperationHandle,
		}
		resp, err := c.client.GetResultSetMetadata(ctx, &req)
		if err != nil {
			return dbsqlerrint.NewDriverError(ctx, "error performing staging operation", err)
		}
		isStagingOperation = resp.IsStagingOperation != nil && *resp.IsStagingOperation
	}

	if !isStagingOperation {
		return nil
	}

	if len(driverctx.StagingPathsFromContext(ctx)) != 0 {
		// Telemetry callback for staging operation row fetching (chunk timing not tracked for staging ops).
		telemetryUpdate := func(chunkCount int, bytesDownloaded int64, chunkIndex int, chunkLatencyMs int64, totalChunksPresent int32) {
			if c.telemetry != nil {
				c.telemetry.AddTag(ctx, telemetry.TagResultChunkCount, chunkCount)
				c.telemetry.AddTag(ctx, telemetry.TagResultBytesDownloaded, bytesDownloaded)
			}
		}
		row, err = rows.NewRows(ctx, exStmtResp.OperationHandle, c.client, c.cfg, exStmtResp.DirectResults, &rows.TelemetryCallbacks{
			OnChunkFetched: telemetryUpdate,
		})
		if err != nil {
			return dbsqlerrint.NewDriverError(ctx, "error reading row.", err)
		}
		defer row.Close() //nolint:errcheck

	} else {
		return dbsqlerrint.NewDriverError(ctx, "staging ctx must be provided.", nil)
	}

	var sqlRow []driver.Value
	colNames := row.Columns()
	sqlRow = make([]driver.Value, len(colNames))
	err = row.Next(sqlRow)
	if err != nil {
		return dbsqlerrint.NewDriverError(ctx, "error fetching staging operation results", err)
	}
	stringValues := make([]string, 4)
	for i, val := range sqlRow { // this will either be 3 (remove op) or 4 (put/get) elements
		if s, ok := val.(string); ok {
			stringValues[i] = s
		} else {
			return dbsqlerrint.NewDriverError(ctx, "received unexpected response from the server.", nil)
		}
	}
	operation := stringValues[0]
	presignedUrl := stringValues[1]
	headersByteArr := []byte(stringValues[2])
	var headers map[string]string
	if err := json.Unmarshal(headersByteArr, &headers); err != nil {
		return dbsqlerrint.NewDriverError(ctx, "error parsing server response.", nil)
	}
	localFile := stringValues[3]
	stagingAllowedLocalPaths := driverctx.StagingPathsFromContext(ctx)
	switch operation {
	case "PUT":
		if localPathIsAllowed(stagingAllowedLocalPaths, localFile) {
			return c.handleStagingPut(ctx, presignedUrl, headers, localFile)
		} else {
			return dbsqlerrint.NewDriverError(ctx, "local file operations are restricted to paths within the configured stagingAllowedLocalPath", nil)
		}
	case "GET":
		if localPathIsAllowed(stagingAllowedLocalPaths, localFile) {
			return c.handleStagingGet(ctx, presignedUrl, headers, localFile)
		} else {
			return dbsqlerrint.NewDriverError(ctx, "local file operations are restricted to paths within the configured stagingAllowedLocalPath", nil)
		}
	case "REMOVE":
		return c.handleStagingRemove(ctx, presignedUrl, headers)
	default:
		return dbsqlerrint.NewDriverError(ctx, fmt.Sprintf("operation %s is not supported. Supported operations are GET, PUT, and REMOVE", operation), nil)
	}
}
