// Package thrift is the Thrift/HiveServer2 implementation of backend.Backend.
// It owns the generated TCLIService client, the open-session response, and the
// execute/poll/close/session RPCs, so that internal/cli_service is imported only
// here and not by the rest of the driver.
//
// Steps are instrumented via internal/debuglog (gated, ordered, timed,
// function-tagged) so a failing or slow step is visible across the
// execute/poll/fetch flow.
package thrift

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/databricks/databricks-sql-go/driverctx"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/databricks/databricks-sql-go/internal/backend"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	context2 "github.com/databricks/databricks-sql-go/internal/compat/context"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/databricks/databricks-sql-go/internal/debuglog"
	dbsqlerrint "github.com/databricks/databricks-sql-go/internal/errors"
	"github.com/databricks/databricks-sql-go/internal/querytags"
	"github.com/databricks/databricks-sql-go/internal/sentinel"
	"github.com/databricks/databricks-sql-go/internal/thrift_protocol"
	"github.com/databricks/databricks-sql-go/logger"
	"github.com/pkg/errors"
)

// Backend is the Thrift implementation of backend.Backend. One Backend backs one
// conn and is used by a single goroutine at a time (database/sql pool
// discipline), so it holds no locks.
type Backend struct {
	cfg     *config.Config
	client  cli_service.TCLIService
	session *cli_service.TOpenSessionResp
	// sessionID is the formatted session GUID, computed once in OpenSession and
	// reused. Formatting it (SprintGuid) allocates, and it is read on every query
	// (connId enrichment, logging, telemetry), so it is cached rather than
	// recomputed per call.
	sessionID string
}

var _ backend.Backend = (*Backend)(nil)

// New builds a Thrift backend from the driver config and the shared HTTP client.
// The Thrift client is constructed here; the session is not opened until
// OpenSession. ctx carries the caller's correlation id onto a client-init error.
func New(ctx context.Context, cfg *config.Config, httpClient *http.Client) (*Backend, error) {
	defer debuglog.Track(ctx, "thrift.New", "host=%s", cfg.Host)()

	tclient, err := client.InitThriftClient(cfg, httpClient)
	if err != nil {
		debuglog.Logf(ctx, "thrift.New", "InitThriftClient failed: %v", err)
		return nil, dbsqlerrint.NewDriverError(ctx, dbsqlerr.ErrThriftClient, err)
	}
	return &Backend{cfg: cfg, client: tclient}, nil
}

// OpenSession opens the server-side Thrift session, wiring the session params
// and initial namespace (catalog/schema) from the config.
func (b *Backend) OpenSession(ctx context.Context) error {
	defer debuglog.Track(ctx, "thrift.Backend.OpenSession", "host=%s", b.cfg.Host)()

	var catalogName *cli_service.TIdentifier
	var schemaName *cli_service.TIdentifier
	if b.cfg.Catalog != "" {
		catalogName = cli_service.TIdentifierPtr(cli_service.TIdentifier(b.cfg.Catalog))
	}
	if b.cfg.Schema != "" {
		schemaName = cli_service.TIdentifierPtr(cli_service.TIdentifier(b.cfg.Schema))
	}

	// EffectiveSessionParams folds any option-derived confs (e.g. metric-view
	// metadata) into the user's SessionParams, backend-neutrally, so the kernel
	// backend sends the identical confs without duplicating the derivation.
	sessionParams := b.cfg.EffectiveSessionParams()

	protocolVersion := int64(b.cfg.ThriftProtocolVersion)

	debuglog.Logf(ctx, "thrift.Backend.OpenSession", "sending OpenSession protocolVersion=0x%X catalog=%q schema=%q", protocolVersion, b.cfg.Catalog, b.cfg.Schema)
	session, err := b.client.OpenSession(ctx, &cli_service.TOpenSessionReq{
		ClientProtocolI64: &protocolVersion,
		Configuration:     sessionParams,
		InitialNamespace: &cli_service.TNamespace{
			CatalogName: catalogName,
			SchemaName:  schemaName,
		},
		CanUseMultipleCatalogs: &b.cfg.CanUseMultipleCatalogs,
	})
	if err != nil {
		debuglog.Logf(ctx, "thrift.Backend.OpenSession", "OpenSession failed: %v", err)
		return dbsqlerrint.NewRequestError(ctx, fmt.Sprintf("error connecting: host=%s port=%d, httpPath=%s", b.cfg.Host, b.cfg.Port, b.cfg.HTTPPath), err)
	}
	b.session = session
	if session.SessionHandle != nil {
		b.sessionID = client.SprintGuid(session.SessionHandle.GetSessionId().GUID)
	}
	debuglog.Logf(ctx, "thrift.Backend.OpenSession", "session opened id=%s serverProtocol=0x%X", b.sessionID, session.ServerProtocolVersion)
	return nil
}

// SessionID returns the formatted server session id (conn.id's value), computed
// once in OpenSession. Empty until OpenSession succeeds.
func (b *Backend) SessionID() string {
	return b.sessionID
}

// ServerProtocolVersion returns the negotiated Thrift protocol version for the
// open session, for the connector's connect log. It is Thrift-specific and so is
// not part of the neutral backend.Backend interface; the connector reads it off
// the concrete *Backend it constructed. Returns 0 before OpenSession.
func (b *Backend) ServerProtocolVersion() cli_service.TProtocolVersion {
	if b.session == nil {
		return 0
	}
	return b.session.ServerProtocolVersion
}

// SessionValid backs conn.IsValid: the session is usable while its open-status
// code is SUCCESS. No I/O — matches today's behavior exactly.
func (b *Backend) SessionValid() bool {
	if b.session == nil {
		return false
	}
	return b.session.GetStatus().StatusCode == cli_service.TStatusCode_SUCCESS_STATUS
}

// CloseSession closes the server-side session, returning the raw RPC error. The
// caller owns the DELETE_SESSION telemetry timing and the error classification.
func (b *Backend) CloseSession(ctx context.Context) error {
	defer debuglog.Track(ctx, "thrift.Backend.CloseSession", "id=%s", b.SessionID())()

	if b.session == nil {
		return nil
	}
	_, err := b.client.CloseSession(ctx, &cli_service.TCloseSessionReq{
		SessionHandle: b.session.SessionHandle,
	})
	if err != nil {
		debuglog.Logf(ctx, "thrift.Backend.CloseSession", "CloseSession failed: %v", err)
	}
	return err
}

// Execute runs the statement to a terminal state and returns a thriftOperation
// carrying the execute and status responses. Per the backend.Backend contract
// the returned Operation is non-nil even on error.
func (b *Backend) Execute(ctx context.Context, req backend.ExecRequest) (backend.Operation, error) {
	defer debuglog.Track(ctx, "thrift.Backend.Execute", "sql.len=%d params=%d", len(req.Query), len(req.Params))()

	exStmtResp, opStatusResp, err := b.runQuery(ctx, req)
	op := &thriftOperation{
		backend:      b,
		exStmtResp:   exStmtResp,
		opStatusResp: opStatusResp,
	}
	if err != nil {
		debuglog.Logf(ctx, "thrift.Backend.Execute", "runQuery error: %v", err)
	}
	return op, err
}

// runQuery executes the statement, then drives the operation to a terminal state
// via direct results or by polling.
func (b *Backend) runQuery(ctx context.Context, req backend.ExecRequest) (*cli_service.TExecuteStatementResp, *cli_service.TGetOperationStatusResp, error) {
	defer debuglog.Track(ctx, "thrift.Backend.runQuery", "")()

	exStmtResp, err := b.executeStatement(ctx, req)
	var log *logger.DBSQLLogger
	log, ctx = client.LoggerAndContext(ctx, exStmtResp)

	if err != nil {
		return exStmtResp, nil, err
	}

	opHandle := exStmtResp.OperationHandle

	if exStmtResp.DirectResults != nil {
		opStatus := exStmtResp.DirectResults.GetOperationStatus()
		debuglog.Logf(ctx, "thrift.Backend.runQuery", "direct results present, state=%s", opStatus.GetOperationState())

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
			statusResp, err := b.pollOperation(ctx, opHandle)
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
		statusResp, err := b.pollOperation(ctx, opHandle)
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

// executeStatement issues the ExecuteStatement RPC, gating each wire option
// (direct results, LZ4, CloudFetch, Arrow, parameters, query tags) on server
// protocol support, and cancels the operation if the context is done.
func (b *Backend) executeStatement(ctx context.Context, req backend.ExecRequest) (*cli_service.TExecuteStatementResp, error) {
	ctx = driverctx.NewContextWithConnId(ctx, b.SessionID())
	defer debuglog.Track(ctx, "thrift.Backend.executeStatement", "")()

	parameters := toSparkParameters(req.Params)

	thriftReq := cli_service.TExecuteStatementReq{
		SessionHandle: b.session.SessionHandle,
		Statement:     req.Query,
		RunAsync:      true,
		QueryTimeout:  int64(b.cfg.QueryTimeout / time.Second),
	}

	// Check protocol version for feature support
	serverProtocolVersion := b.session.ServerProtocolVersion

	// Add direct results if supported
	if thrift_protocol.SupportsDirectResults(serverProtocolVersion) {
		thriftReq.GetDirectResults = &cli_service.TSparkGetDirectResults{
			MaxRows: int64(b.cfg.MaxRows),
		}
	}

	// Add LZ4 compression if supported and enabled
	if thrift_protocol.SupportsLz4Compression(serverProtocolVersion) && b.cfg.UseLz4Compression {
		thriftReq.CanDecompressLZ4Result_ = &b.cfg.UseLz4Compression
	}

	// Add cloud fetch if supported and enabled
	if thrift_protocol.SupportsCloudFetch(serverProtocolVersion) && b.cfg.UseCloudFetch {
		thriftReq.CanDownloadResult_ = &b.cfg.UseCloudFetch
	}

	// Add Arrow support if supported and enabled
	if thrift_protocol.SupportsArrow(serverProtocolVersion) && b.cfg.UseArrowBatches {
		thriftReq.CanReadArrowResult_ = &b.cfg.UseArrowBatches
		thriftReq.UseArrowNativeTypes = &cli_service.TSparkArrowTypes{
			DecimalAsArrow:       &b.cfg.UseArrowNativeDecimal,
			TimestampAsArrow:     &b.cfg.UseArrowNativeTimestamp,
			ComplexTypesAsArrow:  &b.cfg.UseArrowNativeComplexTypes,
			IntervalTypesAsArrow: &b.cfg.UseArrowNativeIntervalTypes,
		}
	}

	// Add parameters if supported and provided
	if thrift_protocol.SupportsParameterizedQueries(serverProtocolVersion) && len(parameters) > 0 {
		thriftReq.Parameters = parameters
	}

	// Add per-statement query tags if provided via context
	if queryTags := driverctx.QueryTagsFromContext(ctx); len(queryTags) > 0 {
		serialized := querytags.Serialize(queryTags)
		if serialized != "" {
			if thriftReq.ConfOverlay == nil {
				thriftReq.ConfOverlay = make(map[string]string)
			}
			thriftReq.ConfOverlay["query_tags"] = serialized
		}
	}

	debuglog.Logf(ctx, "thrift.Backend.executeStatement", "sending ExecuteStatement runAsync=true directResults=%t params=%d", thriftReq.GetDirectResults != nil, len(parameters))
	resp, err := b.client.ExecuteStatement(ctx, &thriftReq)
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
			debuglog.Logf(newCtx, "thrift.Backend.executeStatement", "context done, canceling query")
			log.Debug().Msg("databricks: canceling query")
			_, err1 := b.client.CancelOperation(newCtx, &cli_service.TCancelOperationReq{
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

// pollOperation polls the operation status until it reaches a terminal state,
// cancelling the operation if the context is done (via the sentinel poll loop).
func (b *Backend) pollOperation(ctx context.Context, opHandle *cli_service.TOperationHandle) (*cli_service.TGetOperationStatusResp, error) {
	corrId := driverctx.CorrelationIdFromContext(ctx)
	opID := client.SprintGuid(opHandle.OperationId.GUID)
	log := logger.WithContext(b.SessionID(), corrId, opID)
	defer debuglog.Track(ctx, "thrift.Backend.pollOperation", "op=%s", opID)()

	var statusResp *cli_service.TGetOperationStatusResp
	ctx = driverctx.NewContextWithConnId(ctx, b.SessionID())
	newCtx := context2.WithoutCancel(ctx)
	pollSentinel := sentinel.Sentinel{
		OnDoneFn: func(statusResp any) (any, error) {
			return statusResp, nil
		},
		StatusFn: func() (sentinel.Done, any, error) {
			var err error
			log.Debug().Msg("databricks: polling status")
			debuglog.Logf(ctx, "thrift.Backend.pollOperation", "GetOperationStatus")
			statusResp, err = b.client.GetOperationStatus(newCtx, &cli_service.TGetOperationStatusReq{
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
			debuglog.Logf(ctx, "thrift.Backend.pollOperation", "sentinel canceling query")
			ret, err := b.client.CancelOperation(newCtx, &cli_service.TCancelOperationReq{
				OperationHandle: opHandle,
			})
			return ret, err
		},
	}
	status, resp, err := pollSentinel.Watch(ctx, b.cfg.PollInterval, 0)
	if err != nil {
		log.Err(err).Msg("error polling operation status")
		if status == sentinel.WatchTimeout {
			// Unreachable today (production Watch uses timeout=0); tagged so it
			// classifies correctly if a nonzero poll timeout is ever enabled.
			err = dbsqlerrint.NewRequestError(ctx, dbsqlerr.ErrSentinelTimeout, err).WithCategory(dbsqlerrint.CategoryStatementTimeout)
		}
		return nil, err
	}

	statusResp, ok := resp.(*cli_service.TGetOperationStatusResp)
	if !ok {
		return nil, dbsqlerrint.NewDriverError(ctx, dbsqlerr.ErrReadQueryStatus, nil)
	}
	return statusResp, nil
}

// --- operation-state helpers ---

func unexpectedOperationState(opStatus *cli_service.TGetOperationStatusResp) error {
	return errors.WithMessage(errors.New(opStatus.GetDisplayMessage()), dbsqlerr.ErrUnexpectedOperationState(opStatus.GetOperationState().String()))
}

func invalidOperationState(ctx context.Context, opStatus *cli_service.TGetOperationStatusResp) error {
	return dbsqlerrint.NewDriverError(ctx, dbsqlerr.ErrInvalidOperationState(opStatus.GetOperationState().String()), nil)
}

func logBadQueryState(log *logger.DBSQLLogger, opStatus *cli_service.TGetOperationStatusResp) {
	log.Error().Msgf("databricks: query state: %s", opStatus.GetOperationState())
	log.Error().Msg(opStatus.GetDisplayMessage())
	log.Debug().Msg(opStatus.GetDiagnosticInfo())
}
