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
	"github.com/databricks/databricks-sql-go/internal/backend"
	"github.com/databricks/databricks-sql-go/internal/client"
	context2 "github.com/databricks/databricks-sql-go/internal/compat/context"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/databricks/databricks-sql-go/internal/debuglog"
	dbsqlerrint "github.com/databricks/databricks-sql-go/internal/errors"
	"github.com/databricks/databricks-sql-go/internal/retry"
	"github.com/databricks/databricks-sql-go/internal/rows"
	"github.com/databricks/databricks-sql-go/logger"
	"github.com/databricks/databricks-sql-go/telemetry"
)

type conn struct {
	id        string
	cfg       *config.Config
	backend   backend.Backend
	telemetry *telemetry.Interceptor // Optional telemetry interceptor
}

// tagStatementClosed returns a telemetry-only copy of a close-RPC error tagged
// statement_closed (nil stays nil). The raw error still flows to the caller.
func tagStatementClosed(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	return dbsqlerrint.NewRequestError(ctx, "close statement request error", err).
		WithCategory(dbsqlerrint.CategoryStatementClosed)
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
	err := c.backend.CloseSession(ctx)

	// Record DELETE_SESSION regardless of error (matches JDBC), then flush and release
	if c.telemetry != nil {
		// Tag a telemetry-only copy; err stays untagged so it is still returned
		// as a driver.ErrBadConn below (database/sql needs that for pool eviction).
		telErr := err
		if err != nil {
			telErr = dbsqlerrint.NewRequestError(ctx, "close session request error", err).
				WithCategory(dbsqlerrint.CategorySessionClosed)
		}
		c.telemetry.RecordOperation(ctx, c.id, "", telemetry.OperationTypeDeleteSession, time.Since(closeStart).Milliseconds(), telErr)
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
	return c.backend.SessionValid()
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
	defer debuglog.Track(ctx, "conn.ExecContext", "sql.len=%d args=%d", len(query), len(args))()

	corrId := driverctx.CorrelationIdFromContext(ctx)

	// Capture execution start time for telemetry before running the query
	executeStart := time.Now()
	op, err := c.runQuery(ctx, query, args)
	// A nil op means execution never reached the backend (parameter conversion
	// failed): no server statement to close or measure. Return the already-wrapped
	// error directly.
	if op == nil {
		log.Err(err).Msgf("databricks: failed to execute query: query %s", query)
		return nil, err
	}
	// Attach the statement id to the context queryId and refresh the logger so
	// subsequent lines carry it.
	ctx = enrichQueryId(ctx, op.StatementID())
	log = logger.WithContext(driverctx.ConnIdFromContext(ctx), corrId, driverctx.QueryIdFromContext(ctx))

	// Telemetry: set up metric context BEFORE staging operation so that the
	// staging op's telemetryUpdate callback can attach tags to the metric context.
	var statementID string
	var closeOpErr error // Track CloseOperation errors for telemetry
	if c.telemetry != nil && op.StatementID() != "" {
		statementID = op.StatementID()
		ctx = c.telemetry.BeforeExecuteWithTime(ctx, c.id, statementID, executeStart)
		c.telemetry.AddTag(ctx, telemetry.TagOperationType, telemetry.OperationTypeExecuteStatement)
	}

	stagingErr := c.execStagingOperation(op, ctx)

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

	// Close the server operation if one is still open. The backend decides
	// whether a close RPC is actually needed and reports closed=true only when it
	// issued one, so CLOSE_STATEMENT telemetry is recorded only for a real close.
	newCtx := driverctx.NewContextWithCorrelationId(driverctx.NewContextWithConnId(context.Background(), c.id), corrId)
	closeOpStart := time.Now()
	closed, err1 := op.Close(newCtx)
	if closed {
		if c.telemetry != nil {
			c.telemetry.RecordOperation(ctx, c.id, statementID, telemetry.OperationTypeCloseStatement, time.Since(closeOpStart).Milliseconds(), tagStatementClosed(ctx, err1))
		}
		if err1 != nil {
			log.Err(err1).Msg("databricks: failed to close operation after executing statement")
			closeOpErr = err1 // Capture for telemetry
		}
	}

	if err != nil {
		log.Err(err).Msgf("databricks: failed to execute query: query %s", query)
		return nil, op.ExecutionError(ctx, err)
	}

	if stagingErr != nil {
		log.Err(stagingErr).Msgf("databricks: failed to execute query: query %s", query)
		return nil, op.ExecutionError(ctx, stagingErr)
	}

	res := result{AffectedRows: op.AffectedRows()}

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
	defer debuglog.Track(ctx, "conn.QueryContext", "sql.len=%d args=%d", len(query), len(args))()

	// first we try to get the results synchronously.
	// at any point in time that the context is done we must cancel and return

	corrId := driverctx.CorrelationIdFromContext(ctx)

	// Capture execution start time for telemetry before running the query
	executeStart := time.Now()
	op, err := c.runQuery(ctx, query, args)
	// A nil op means execution never reached the backend (parameter conversion
	// failed): no statement id, no telemetry to emit. Log and return the
	// already-wrapped error directly.
	if op == nil {
		log.Err(err).Msg("databricks: failed to run query") // To log query we need to redact credentials
		log.Duration(msg, start)
		return nil, err
	}
	// Attach the statement id to the context queryId and refresh the logger so
	// subsequent lines carry it.
	ctx = enrichQueryId(ctx, op.StatementID())
	log = logger.WithContext(driverctx.ConnIdFromContext(ctx), corrId, driverctx.QueryIdFromContext(ctx))
	defer log.Duration(msg, start)

	// Telemetry: set up metric context for the statement.
	// BeforeExecuteWithTime anchors startTime to before runQuery() ran.
	var statementID string
	if c.telemetry != nil && op.StatementID() != "" {
		statementID = op.StatementID()
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
		return nil, op.ExecutionError(ctx, err)
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

	// Detach from caller's context so that telemetry tag writes and flushes
	// survive context cancellation (e.g. query timeout, database/sql awaitDone).
	// All three callbacks (telemetryUpdate, cloudFetchCallback, closeCallback)
	// use this detached context uniformly.
	telemetryCtx := context2.WithoutCancel(ctx)

	// Telemetry callback invoked after each result page is fetched.
	telemetryUpdate := func(chunkCount int, bytesDownloaded int64, chunkIndex int, chunkLatencyMs int64, _ int32) {
		if c.telemetry == nil {
			return
		}
		c.telemetry.AddTag(telemetryCtx, telemetry.TagChunkCount, chunkCount)
		c.telemetry.AddTag(telemetryCtx, telemetry.TagBytesDownloaded, bytesDownloaded)

		// Aggregate per-chunk fetch latencies (skip direct results where latency is 0).
		if timing.record(chunkLatencyMs) {
			timing.applyTags(telemetryCtx, c.telemetry)
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
				timing.applyTags(telemetryCtx, c.telemetry)
			}
		}
	}

	// closeCallback is invoked from rows.Close() after all rows have been consumed.
	// At that point chunk timing is fully accumulated in telemetryCtx tags, so we
	// finalize EXECUTE_STATEMENT here rather than at QueryContext return time.
	var closeCallback func(latencyMs int64, chunkCount int, iterErr error, closeErr error)
	if c.telemetry != nil && statementID != "" {
		interceptor := c.telemetry
		connID := c.id
		stmtID := statementID
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
			interceptor.RecordOperation(telemetryCtx, connID, stmtID, telemetry.OperationTypeCloseStatement, latencyMs, tagStatementClosed(telemetryCtx, closeErr))
		}
	} else if c.telemetry != nil {
		interceptor := c.telemetry
		connID := c.id
		closeCallback = func(latencyMs int64, _ int, _ error, closeErr error) {
			interceptor.RecordOperation(telemetryCtx, connID, "", telemetry.OperationTypeCloseStatement, latencyMs, tagStatementClosed(telemetryCtx, closeErr))
		}
	}

	rows, err := op.Results(ctx, &rows.TelemetryCallbacks{
		OnChunkFetched:   telemetryUpdate,
		OnClose:          closeCallback,
		OnCloudFetchFile: cloudFetchCallback,
	})
	if err != nil {
		// Results failed AFTER a successful server execute (e.g. result-schema
		// fetch / Arrow import). The completing EXECUTE_STATEMENT telemetry
		// (AfterExecute/CompleteStatement) lives in closeCallback, which fires
		// from Rows.Close() — but no Rows is armed on this path (the kernel
		// returns nil rows, and the Thrift rows we'd get here is discarded on
		// error), so the callback never runs. Finalize the metric now, mirroring
		// the execute-error path above; otherwise the statement stays
		// BeforeExecute'd + FinalizeLatency'd but never completed — a dangling
		// metric for a query that ran server-side. Return nil rows so the
		// discarded Rows can never later fire closeCallback and double-count.
		//
		// Use telemetryCtx (detached from the caller's cancellation, line 313),
		// not the raw ctx, exactly as the three callbacks above do: a
		// Results-after-execute failure is commonly triggered by caller
		// cancellation/timeout, and the export path threads this ctx down to the
		// metric's HTTP request — a cancelled ctx would abort the export and drop
		// the very completing metric this branch exists to emit.
		if c.telemetry != nil && statementID != "" {
			c.telemetry.AfterExecute(telemetryCtx, err)
			c.telemetry.CompleteStatement(telemetryCtx, statementID, true)
		}
		return nil, err
	}
	return rows, nil

}

// runQuery converts the caller's parameters and executes the statement through
// the backend. It returns a nil Operation only when execution never reached the
// backend — parameter conversion failed before any server statement existed — in
// which case the wrapped error is returned directly. Once the backend is called,
// backend.Execute guarantees a non-nil Operation, so callers guard only for the
// pre-backend nil.
func (c *conn) runQuery(ctx context.Context, query string, args []driver.NamedValue) (backend.Operation, error) {
	params, err := convertNamedValuesToParams(args)
	if err != nil {
		return nil, dbsqlerrint.NewExecutionError(ctx, dbsqlerr.ErrQueryExecution, err, nil)
	}
	return c.backend.Execute(ctx, backend.ExecRequest{Query: query, Params: params})
}

// enrichQueryId sets the operation's statement id as the context queryId. A
// caller-set queryId is never overwritten. When the context has no queryId,
// NewContextWithQueryId is always called with the derived id — even if it is
// empty — so any registered QueryIdCallback fires.
func enrichQueryId(ctx context.Context, statementID string) context.Context {
	if driverctx.QueryIdFromContext(ctx) != "" {
		return ctx
	}
	return driverctx.NewContextWithQueryId(ctx, statementID)
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
	return statusInSuccessRange(response.StatusCode)
}

// statusInSuccessRange returns true for the 2xx status codes the staging
// HTTP path treats as success: 200 OK / 201 Created / 202 Accepted / 204
// No Content. Exposed separately from Succeeded so handlers can extend the
// accept set (e.g. REMOVE accepts 404 for idempotent-delete semantics).
func statusInSuccessRange(status int) bool {
	return status == 200 || status == 201 || status == 202 || status == 204
}

func (c *conn) handleStagingPut(ctx context.Context, presignedUrl string, headers map[string]string, localFile string) dbsqlerr.DBError {
	if localFile == "" {
		return dbsqlerrint.NewDriverError(ctx, "cannot perform PUT without specifying a local_file", nil)
	}

	dat, err := os.Open(localFile) //nolint:gosec // localFile is provided by the application, not user input
	if err != nil {
		return dbsqlerrint.NewDriverError(ctx, "error reading local file", err)
	}
	defer dat.Close() //nolint:errcheck

	info, err := dat.Stat()
	if err != nil {
		return dbsqlerrint.NewDriverError(ctx, "error reading local file info", err)
	}
	size := info.Size()

	// Each retry attempt needs a fresh request because http.Client.Do consumes
	// the request body. Rewind the *os.File between attempts so the server
	// receives the full payload on every retry, not just attempt 1.
	//
	// Wrap the file in io.NopCloser so http.Client.Do can't close it — by
	// default it closes any body that implements io.Closer, which would break
	// the Seek on the next retry. The outer defer dat.Close() above owns the
	// file's lifecycle.
	reqFactory := func(attempt int) (*http.Request, error) {
		if attempt > 0 {
			if _, seekErr := dat.Seek(0, io.SeekStart); seekErr != nil {
				return nil, seekErr
			}
		}
		req, reqErr := http.NewRequestWithContext(ctx, http.MethodPut, presignedUrl, io.NopCloser(dat))
		if reqErr != nil {
			return nil, reqErr
		}
		req.ContentLength = size // backend actually requires content length to be known
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		return req, nil
	}

	if _, err := c.doStagingRequestWithRetry(ctx, reqFactory); err != nil {
		return err
	}
	return nil
}

func (c *conn) handleStagingGet(ctx context.Context, presignedUrl string, headers map[string]string, localFile string) dbsqlerr.DBError {
	if localFile == "" {
		return dbsqlerrint.NewDriverError(ctx, "cannot perform GET without specifying a local_file", nil)
	}

	reqFactory := func(_ int) (*http.Request, error) {
		req, reqErr := http.NewRequestWithContext(ctx, http.MethodGet, presignedUrl, nil)
		if reqErr != nil {
			return nil, reqErr
		}
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		return req, nil
	}

	content, err := c.doStagingRequestWithRetry(ctx, reqFactory)
	if err != nil {
		return err
	}
	if writeErr := os.WriteFile(localFile, content, 0644); writeErr != nil { //nolint:gosec
		return dbsqlerrint.NewDriverError(ctx, "error writing local file", writeErr)
	}
	return nil
}

func (c *conn) handleStagingRemove(ctx context.Context, presignedUrl string, headers map[string]string) dbsqlerr.DBError {
	reqFactory := func(_ int) (*http.Request, error) {
		req, reqErr := http.NewRequestWithContext(ctx, http.MethodDelete, presignedUrl, nil)
		if reqErr != nil {
			return nil, reqErr
		}
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		return req, nil
	}

	// Treat 404 as success on REMOVE: DELETE is idempotent, and a 404 means
	// the object is already absent — which is the post-condition the caller
	// asked for. This also avoids spurious failures when a successful DELETE
	// returns a transient 5xx mid-response and the retry sees 404 from the
	// server having already applied the original request.
	acceptStatus := func(status int) bool {
		return statusInSuccessRange(status) || status == http.StatusNotFound
	}

	if _, err := c.doStagingRequestWithRetryAccept(ctx, reqFactory, acceptStatus); err != nil {
		return err
	}
	return nil
}

// maxStagingErrorBodyBytes bounds the response body bytes included in
// terminal staging error messages. Proxies and misconfigured backends can
// return multi-MB error bodies; truncating keeps the driver error readable
// without dropping the typical S3 XML error code that fits well under 512B.
const maxStagingErrorBodyBytes = 512

// doStagingRequestWithRetry executes a staging HTTP request with retry on
// transient object-storage failures (ES-1911239). Wraps
// doStagingRequestWithRetryAccept with the default success predicate (2xx
// from statusInSuccessRange / Succeeded).
func (c *conn) doStagingRequestWithRetry(ctx context.Context, reqFactory func(attempt int) (*http.Request, error)) ([]byte, dbsqlerr.DBError) {
	return c.doStagingRequestWithRetryAccept(ctx, reqFactory, statusInSuccessRange)
}

// doStagingRequestWithRetryAccept is the generalized staging retry helper
// used by all three handleStaging* methods. Mirrors the CloudFetch retry
// path (ES-1892645) in semantics — same retryable status set, same
// exponential-backoff-with-jitter schedule, same RetryMax/RetryWaitMin/
// RetryWaitMax config knobs — so behavior is consistent across the driver's
// two object-storage code paths.
//
// reqFactory must return a fresh *http.Request on each call. Attempt 0 is
// the initial request; attempt N>0 is a retry. The PUT path uses this to
// rewind the file body between attempts; other staging paths just construct
// a new request each time.
//
// acceptStatus reports whether a given HTTP status code should be treated
// as success. Most handlers pass statusInSuccessRange. The REMOVE handler
// extends this to also accept 404 (idempotent-delete semantics).
//
// On success returns the response body bytes. On terminal failure (non-
// retryable status, exhausted retries, or context cancellation) returns a
// dbsqlerr.DBError describing the final state.
func (c *conn) doStagingRequestWithRetryAccept(
	ctx context.Context,
	reqFactory func(attempt int) (*http.Request, error),
	acceptStatus func(status int) bool,
) ([]byte, dbsqlerr.DBError) {
	retryMax := c.cfg.RetryMax
	if retryMax < 0 {
		retryMax = 0
	}
	client := &http.Client{}

	var (
		lastErr        error
		lastStatus     int
		lastBody       []byte
		lastRetryAfter string
	)

	for attempt := 0; attempt <= retryMax; attempt++ {
		if attempt > 0 {
			wait := retry.Backoff(attempt, c.cfg.RetryWaitMin, c.cfg.RetryWaitMax, lastRetryAfter)
			logger.Debug().Msgf(
				"staging: retrying HTTP request (attempt %d/%d) in %v; lastStatus=%d lastErr=%v",
				attempt, retryMax, wait, lastStatus, lastErr,
			)
			t := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				if !t.Stop() {
					<-t.C
				}
				return nil, dbsqlerrint.NewDriverError(ctx, "staging operation cancelled during retry backoff", ctx.Err())
			case <-t.C:
			}
		}

		req, reqErr := reqFactory(attempt)
		if reqErr != nil {
			return nil, dbsqlerrint.NewDriverError(ctx, "error building staging http request", reqErr)
		}

		res, err := client.Do(req)
		if err != nil {
			// Caller cancellation is terminal; otherwise treat transport
			// errors (TCP RST, TLS timeout, etc.) as transient.
			if ctx.Err() != nil {
				return nil, dbsqlerrint.NewDriverError(ctx, "error sending http request", ctx.Err())
			}
			lastErr = err
			lastStatus = 0
			lastRetryAfter = ""
			continue
		}

		body, readErr := io.ReadAll(res.Body)
		res.Body.Close() //nolint:errcheck,gosec // G104: close after drain

		if readErr != nil {
			if ctx.Err() != nil {
				return nil, dbsqlerrint.NewDriverError(ctx, "error reading http response", ctx.Err())
			}
			lastErr = readErr
			lastStatus = 0
			lastRetryAfter = ""
			continue
		}

		if acceptStatus(res.StatusCode) {
			return body, nil
		}

		lastStatus = res.StatusCode
		lastErr = nil
		lastBody = body
		lastRetryAfter = res.Header.Get("Retry-After")

		if !retry.IsRetryableStatus(res.StatusCode) {
			return nil, dbsqlerrint.NewDriverError(ctx, fmt.Sprintf("staging operation over HTTP was unsuccessful: %d-%s", res.StatusCode, truncateErrorBody(body)), nil)
		}
	}

	if lastStatus != 0 {
		// lastErr is nil here by construction: the HTTP-status branch above
		// explicitly clears it on every iteration. The status code and body
		// are captured in msg, so there's no underlying error to wrap.
		return nil, dbsqlerrint.NewDriverError(ctx, fmt.Sprintf("staging operation over HTTP was unsuccessful: %d-%s (after %d retries)", lastStatus, truncateErrorBody(lastBody), retryMax), nil)
	}
	return nil, dbsqlerrint.NewDriverError(ctx, fmt.Sprintf("staging operation HTTP request failed: %v (after %d retries)", lastErr, retryMax), lastErr)
}

// truncateErrorBody caps b at maxStagingErrorBodyBytes for inclusion in error
// messages, appending an indicator when truncation occurred.
func truncateErrorBody(b []byte) string {
	if len(b) <= maxStagingErrorBodyBytes {
		return string(b)
	}
	return fmt.Sprintf("%s... (%d bytes total, truncated)", b[:maxStagingErrorBodyBytes], len(b))
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
	op backend.Operation,
	ctx context.Context) dbsqlerr.DBError {

	defer debuglog.Track(ctx, "conn.execStagingOperation", "stmt=%s", op.StatementID())()

	var row driver.Rows
	var err error

	isStagingOperation, stagingErr := op.IsStaging(ctx)
	if stagingErr != nil {
		return dbsqlerrint.NewDriverError(ctx, "error performing staging operation", stagingErr)
	}

	if !isStagingOperation {
		return nil
	}

	if len(driverctx.StagingPathsFromContext(ctx)) != 0 {
		// Telemetry callback for staging operation row fetching (chunk timing not tracked for staging ops).
		telemetryUpdate := func(chunkCount int, bytesDownloaded int64, chunkIndex int, chunkLatencyMs int64, totalChunksPresent int32) {
			if c.telemetry != nil {
				c.telemetry.AddTag(ctx, telemetry.TagChunkCount, chunkCount)
				c.telemetry.AddTag(ctx, telemetry.TagBytesDownloaded, bytesDownloaded)
			}
		}
		row, err = op.Results(ctx, &rows.TelemetryCallbacks{
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
		return dbsqlerrint.NewDriverError(ctx, fmt.Sprintf("operation %s is not supported. Supported operations are GET, PUT, and REMOVE", operation), nil).WithCategory(dbsqlerrint.CategoryUnsupportedOperation)
	}
}
