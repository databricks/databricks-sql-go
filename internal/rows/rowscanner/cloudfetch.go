package rowscanner

import (
	"context"
	"errors"

	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	dbsqlerrint "github.com/databricks/databricks-sql-go/internal/errors"
)

type cloudFetchRefresher interface {
	RefetchLinks(context.Context, *cli_service.TSparkArrowResultLink) ([]*cli_service.TSparkArrowResultLink, error)
	CanRefetchLinks() bool
}

type cloudFetchRefresherKey struct{}

func WithCloudFetchLinkRefresher(ctx context.Context, pages ResultPageIterator) context.Context {
	if refresher, ok := pages.(cloudFetchRefresher); ok {
		return context.WithValue(ctx, cloudFetchRefresherKey{}, refresher)
	}
	return ctx
}

func CloudFetchLinkRefresherFromContext(ctx context.Context) func(context.Context, *cli_service.TSparkArrowResultLink) ([]*cli_service.TSparkArrowResultLink, error) {
	if refresher, ok := ctx.Value(cloudFetchRefresherKey{}).(cloudFetchRefresher); ok && refresher.CanRefetchLinks() {
		return refresher.RefetchLinks
	}
	return nil
}

func CanRefetchCloudFetchLinks(ctx context.Context) bool {
	refresher, ok := ctx.Value(cloudFetchRefresherKey{}).(cloudFetchRefresher)
	return ok && refresher.CanRefetchLinks()
}

func (rpf *resultPageIterator) CanRefetchLinks() bool {
	rpf.operationMu.Lock()
	defer rpf.operationMu.Unlock()
	return !rpf.closedOnServer && rpf.fetchContext().Err() == nil
}

func (rpf *resultPageIterator) RefetchLinks(ctx context.Context, expired *cli_service.TSparkArrowResultLink) ([]*cli_service.TSparkArrowResultLink, error) {
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(rpf.fetchContext(), cancel)
	defer stop()
	defer cancel()

	rpf.operationMu.Lock()
	defer rpf.operationMu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if rpf.closedOnServer {
		return nil, errors.New(dbsqlerr.ErrLinkExpired)
	}

	rpf.fetchPositionChanged = true
	response, err := rpf.client.FetchResults(ctx, &cli_service.TFetchResultsReq{
		OperationHandle: rpf.opHandle,
		StartRowOffset:  &expired.StartRowOffset,
		MaxRows:         expired.RowCount,
	})
	if err != nil {
		return nil, dbsqlerrint.NewRequestError(ctx, dbsqlerr.ErrLinkExpired, err)
	}
	if response != nil && response.Results != nil && len(response.Results.ResultLinks) > 0 {
		return response.Results.ResultLinks, nil
	}
	return nil, errors.New(dbsqlerr.ErrLinkExpired)
}

func (rpf *resultPageIterator) fetchPage(request *cli_service.TFetchResultsReq, startRowOffset int64) (*cli_service.TFetchResultsResp, error) {
	rpf.operationMu.Lock()
	defer rpf.operationMu.Unlock()
	ctx := rpf.fetchContext()
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	if rpf.fetchPositionChanged {
		request.StartRowOffset = &startRowOffset
	}
	response, err := rpf.client.FetchResults(ctx, request)
	if err == nil {
		rpf.fetchPositionChanged = false
	}
	return response, err
}

func (rpf *resultPageIterator) fetchContext() context.Context {
	if rpf.fetchCtx != nil {
		return rpf.fetchCtx
	}
	return rpf.ctx
}
