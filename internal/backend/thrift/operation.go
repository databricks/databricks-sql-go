package thrift

import (
	"context"
	"database/sql/driver"

	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/databricks/databricks-sql-go/internal/backend"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/debuglog"
	dbsqlerrint "github.com/databricks/databricks-sql-go/internal/errors"
	dbsqlrows "github.com/databricks/databricks-sql-go/internal/rows"
)

// thriftOperation is the Thrift implementation of backend.Operation. It carries
// the execute response (holding the operation handle and optional direct
// results) and the terminal status response. Its constructor does no I/O; the
// RPCs (result fetch, staging-metadata lookup, close) happen in the methods.
type thriftOperation struct {
	backend      *Backend
	exStmtResp   *cli_service.TExecuteStatementResp
	opStatusResp *cli_service.TGetOperationStatusResp
	// statementID caches the formatted operation GUID (SprintGuid allocates and
	// StatementID is read several times per query). An Operation is used by one
	// goroutine at a time (pool discipline), so a plain memo needs no lock.
	statementID    string
	statementIDSet bool
	// closed records whether Close has already issued the CloseOperation RPC on
	// this Operation, so a second call is a no-op per the backend.Operation
	// idempotency contract.
	closed bool
}

var _ backend.Operation = (*thriftOperation)(nil)

// hasHandle reports whether the server returned an operation handle. Every
// accessor tolerates its absence so the non-nil-Operation contract holds even on
// the pre-handle error path.
func (o *thriftOperation) hasHandle() bool {
	return o.exStmtResp != nil && o.exStmtResp.OperationHandle != nil
}

// StatementID is the formatted operation id, or "" when no handle exists.
// Computed once and cached.
func (o *thriftOperation) StatementID() string {
	if o.statementIDSet {
		return o.statementID
	}
	if o.hasHandle() && o.exStmtResp.OperationHandle.OperationId != nil {
		o.statementID = client.SprintGuid(o.exStmtResp.OperationHandle.OperationId.GUID)
	}
	o.statementIDSet = true
	return o.statementID
}

// AffectedRows is the modified-row count for ExecContext, from the terminal
// status response.
func (o *thriftOperation) AffectedRows() int64 {
	return o.opStatusResp.GetNumModifiedRows()
}

// Results builds the driver.Rows for the operation's result set by delegating to
// the rows layer with the Thrift handle, client, and direct results. The caller
// supplies the telemetry callbacks (full for a query, chunk-timing-only for a
// staging read).
func (o *thriftOperation) Results(ctx context.Context, callbacks *dbsqlrows.TelemetryCallbacks) (driver.Rows, error) {
	defer debuglog.Track(ctx, "thrift.Operation.Results", "stmt=%s", o.StatementID())()

	var directResults *cli_service.TSparkDirectResults
	var opHandle *cli_service.TOperationHandle
	if o.exStmtResp != nil {
		directResults = o.exStmtResp.DirectResults
		opHandle = o.exStmtResp.OperationHandle
	}
	return dbsqlrows.NewRows(ctx, opHandle, o.backend.client, o.backend.cfg, directResults, callbacks)
}

// IsStaging reports whether this operation is a staging operation. It reuses the
// result-set metadata from direct results when present, otherwise issues the
// GetResultSetMetadata RPC — the exact branch conn.execStagingOperation used.
// Returns false (no error) when there is no handle. The raw RPC error is returned
// unwrapped; the caller owns the "error performing staging operation" wrapping.
func (o *thriftOperation) IsStaging(ctx context.Context) (bool, error) {
	defer debuglog.Track(ctx, "thrift.Operation.IsStaging", "stmt=%s", o.StatementID())()

	if !o.hasHandle() {
		return false, nil
	}
	if o.exStmtResp.DirectResults != nil && o.exStmtResp.DirectResults.ResultSetMetadata != nil {
		md := o.exStmtResp.DirectResults.ResultSetMetadata
		return md.IsStagingOperation != nil && *md.IsStagingOperation, nil
	}

	debuglog.Logf(ctx, "thrift.Operation.IsStaging", "GetResultSetMetadata")
	resp, err := o.backend.client.GetResultSetMetadata(ctx, &cli_service.TGetResultSetMetadataReq{
		OperationHandle: o.exStmtResp.OperationHandle,
	})
	if err != nil {
		return false, err
	}
	return resp.IsStagingOperation != nil && *resp.IsStagingOperation, nil
}

// Close best-effort closes the server operation. It skips the CloseOperation RPC
// when there is no handle, when direct results already closed it, when the
// operation is already in the CLOSED state, or when Close has already been
// invoked on this Operation; closed reports whether the RPC was actually sent,
// so the caller records CLOSE_STATEMENT telemetry only in that case.
func (o *thriftOperation) Close(ctx context.Context) (bool, error) {
	defer debuglog.Track(ctx, "thrift.Operation.Close", "stmt=%s", o.StatementID())()

	if o.closed {
		return false, nil
	}
	if !o.hasHandle() {
		return false, nil
	}
	alreadyClosed := o.exStmtResp.DirectResults != nil && o.exStmtResp.DirectResults.CloseOperation != nil
	if alreadyClosed {
		return false, nil
	}
	if o.opStatusResp != nil && o.opStatusResp.GetOperationState() == cli_service.TOperationState_CLOSED_STATE {
		return false, nil
	}

	debuglog.Logf(ctx, "thrift.Operation.Close", "CloseOperation")
	_, err := o.backend.client.CloseOperation(ctx, &cli_service.TCloseOperationReq{
		OperationHandle: o.exStmtResp.OperationHandle,
	})
	o.closed = true
	if err != nil {
		return true, err
	}
	return true, nil
}

// ExecutionError wraps cause as the driver's execution error, attaching this
// operation's terminal sqlstate from the status response. Returns nil when cause
// is nil.
func (o *thriftOperation) ExecutionError(ctx context.Context, cause error) error {
	if cause == nil {
		return nil
	}
	return dbsqlerrint.NewExecutionError(ctx, dbsqlerr.ErrQueryExecution, cause, o.opStatusResp)
}
