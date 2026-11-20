package rowscanner

import (
	"context"
	"errors"

	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	dbsqlerrint "github.com/databricks/databricks-sql-go/internal/errors"
)

type cloudFetchRefresher interface {
	RefetchLink(context.Context, *cli_service.TSparkArrowResultLink) (*cli_service.TSparkArrowResultLink, error)
}

type cloudFetchRefresherKey struct{}

func WithCloudFetchLinkRefresher(ctx context.Context, pages ResultPageIterator) context.Context {
	if refresher, ok := pages.(cloudFetchRefresher); ok {
		return context.WithValue(ctx, cloudFetchRefresherKey{}, refresher)
	}
	return ctx
}

func CloudFetchLinkRefresherFromContext(ctx context.Context) func(context.Context, *cli_service.TSparkArrowResultLink) (*cli_service.TSparkArrowResultLink, error) {
	if refresher, ok := ctx.Value(cloudFetchRefresherKey{}).(cloudFetchRefresher); ok {
		return refresher.RefetchLink
	}
	return nil
}

func (rpf *resultPageIterator) RefetchLink(ctx context.Context, expired *cli_service.TSparkArrowResultLink) (*cli_service.TSparkArrowResultLink, error) {
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(rpf.fetchCtx, cancel)
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
	if response != nil && response.Results != nil {
		for _, link := range response.Results.ResultLinks {
			if link != nil && link.StartRowOffset == expired.StartRowOffset && link.RowCount == expired.RowCount {
				return link, nil
			}
		}
	}
	return nil, errors.New(dbsqlerr.ErrLinkExpired)
}

func (rpf *resultPageIterator) fetchPage(request *cli_service.TFetchResultsReq, startRowOffset int64) (*cli_service.TFetchResultsResp, error) {
	rpf.operationMu.Lock()
	defer rpf.operationMu.Unlock()
	ctx := rpf.fetchCtx
	if ctx == nil {
		ctx = rpf.ctx
	}
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
