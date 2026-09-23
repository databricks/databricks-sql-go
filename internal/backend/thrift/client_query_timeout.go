package thrift

import (
	"context"
	"time"

	"github.com/databricks/databricks-sql-go/driverctx"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	context2 "github.com/databricks/databricks-sql-go/internal/compat/context"
	dbsqlerrint "github.com/databricks/databricks-sql-go/internal/errors"
	"github.com/databricks/databricks-sql-go/internal/sentinel"
	"github.com/databricks/databricks-sql-go/logger"
)

const (
	clientQueryTimeoutCleanupBudget    = 30 * time.Second
	clientQueryStatusRPCGrace          = 5 * time.Second
	maxConcurrentClientTimeoutCleanups = 64
	unlimitedClientQueryTimeout        = time.Duration(1<<63 - 1)
)

type clientQueryTimeoutError struct{}

func (*clientQueryTimeoutError) Error() string { return "Client query timeout expired" }

func (*clientQueryTimeoutError) Category() dbsqlerrint.ErrorCategory {
	return dbsqlerrint.CategoryStatementTimeout
}

var errClientQueryTimeout = &clientQueryTimeoutError{}

func clientQueryTimeoutStatus() *cli_service.TGetOperationStatusResp {
	state := cli_service.TOperationState_TIMEDOUT_STATE
	sqlState := "HYT00"
	message := errClientQueryTimeout.Error()
	return &cli_service.TGetOperationStatusResp{
		OperationState: &state,
		SqlState:       &sqlState,
		DisplayMessage: &message,
	}
}

func isTerminalOperationStatus(status *cli_service.TGetOperationStatusResp) bool {
	if status == nil || status.OperationState == nil {
		return false
	}
	switch status.GetOperationState() {
	case cli_service.TOperationState_FINISHED_STATE,
		cli_service.TOperationState_CANCELED_STATE,
		cli_service.TOperationState_CLOSED_STATE,
		cli_service.TOperationState_ERROR_STATE,
		cli_service.TOperationState_TIMEDOUT_STATE:
		return true
	default:
		return false
	}
}

func isLiveOperationStatus(status *cli_service.TGetOperationStatusResp) bool {
	if status == nil {
		return false
	}
	switch status.GetOperationState() {
	case cli_service.TOperationState_INITIALIZED_STATE,
		cli_service.TOperationState_PENDING_STATE,
		cli_service.TOperationState_RUNNING_STATE:
		return true
	default:
		return false
	}
}

func operationID(opHandle *cli_service.TOperationHandle) string {
	if opHandle == nil || opHandle.OperationId == nil {
		return ""
	}
	return client.SprintGuid(opHandle.OperationId.GUID)
}

func operationNeedsCleanup(resp *cli_service.TExecuteStatementResp) bool {
	if resp == nil || resp.OperationHandle == nil {
		return false
	}
	return resp.DirectResults == nil || resp.DirectResults.CloseOperation == nil
}

// finiteClientQueryTimeout snapshots and rounds a validated connector option.
// nil and both public unlimited sentinels preserve the legacy Thrift path.
func finiteClientQueryTimeout(configured *time.Duration) *time.Duration {
	if configured == nil || *configured == 0 || *configured == unlimitedClientQueryTimeout {
		return nil
	}
	timeout := *configured
	if remainder := timeout % time.Millisecond; remainder != 0 {
		timeout += time.Millisecond - remainder
	}
	return &timeout
}

func clientDeadlineExpired(deadline *time.Time) bool {
	return deadline != nil && !time.Now().Before(*deadline)
}

func clientStatusGraceExpired(deadline *time.Time) bool {
	return deadline != nil && time.Now().After(deadline.Add(clientQueryStatusRPCGrace))
}

func (b *Backend) startClientTimeoutCleanup(ctx context.Context, opHandle *cli_service.TOperationHandle) {
	if opHandle == nil {
		return
	}
	log := logger.WithContext(b.SessionID(), driverctx.CorrelationIdFromContext(ctx), operationID(opHandle))
	select {
	case b.timeoutCleanupSlots <- struct{}{}:
	default:
		log.Warn().Msg("databricks: client query-timeout cleanup saturated")
		return
	}
	go func() {
		defer func() { <-b.timeoutCleanupSlots }()
		cleanupCtx, cancel := context.WithTimeout(driverctx.NewContextFromBackground(ctx), clientQueryTimeoutCleanupBudget)
		defer cancel()

		cleanupClient, err := b.newClient()
		if err != nil {
			log.Err(err).Msg("databricks: could not create timeout cleanup client")
			return
		}
		if _, err := cleanupClient.CancelOperation(cleanupCtx, &cli_service.TCancelOperationReq{OperationHandle: opHandle}); err != nil {
			log.Err(err).Msg("databricks: timeout cancel failed")
		}
		if _, err := cleanupClient.CloseOperation(cleanupCtx, &cli_service.TCloseOperationReq{OperationHandle: opHandle}); err != nil {
			log.Err(err).Msg("databricks: timeout close failed")
		}
	}()
}

func (b *Backend) pollOperationWithClientDeadline(
	ctx context.Context,
	opHandle *cli_service.TOperationHandle,
	clientDeadline *time.Time,
) (*cli_service.TGetOperationStatusResp, error) {
	if clientDeadline == nil {
		return b.pollOperation(ctx, opHandle)
	}

	interval := b.cfg.PollInterval
	if interval == 0 {
		interval = sentinel.DEFAULT_INTERVAL
	}
	for {
		if ctx.Err() != nil {
			return b.pollOperation(ctx, opHandle)
		}
		remaining := time.Until(*clientDeadline)
		if remaining <= 0 {
			b.startClientTimeoutCleanup(ctx, opHandle)
			return clientQueryTimeoutStatus(), errClientQueryTimeout
		}
		wait := interval
		if remaining < wait {
			wait = remaining
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return b.pollOperation(ctx, opHandle)
		case <-timer.C:
		}

		if clientDeadlineExpired(clientDeadline) {
			b.startClientTimeoutCleanup(ctx, opHandle)
			return clientQueryTimeoutStatus(), errClientQueryTimeout
		}

		pollCtx, cancel := context.WithDeadline(context2.WithoutCancel(ctx), clientDeadline.Add(clientQueryStatusRPCGrace))
		statusResp, err := b.client.GetOperationStatus(pollCtx, &cli_service.TGetOperationStatusReq{OperationHandle: opHandle})
		cancel()
		if err == nil && isTerminalOperationStatus(statusResp) &&
			!clientStatusGraceExpired(clientDeadline) {
			return statusResp, nil
		}
		if ctx.Err() != nil && clientDeadlineExpired(clientDeadline) {
			return b.pollOperation(ctx, opHandle)
		}
		if clientDeadlineExpired(clientDeadline) {
			b.startClientTimeoutCleanup(ctx, opHandle)
			return clientQueryTimeoutStatus(), errClientQueryTimeout
		}
		if err != nil {
			return statusResp, err
		}
		if !isLiveOperationStatus(statusResp) {
			return statusResp, nil
		}
		if ctx.Err() != nil {
			return b.pollOperation(ctx, opHandle)
		}
	}
}
