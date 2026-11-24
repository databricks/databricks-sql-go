package rowscanner

import (
	"context"
	"errors"
	"testing"

	"github.com/databricks/databricks-sql-go/v2/internal/cli_service"
	"github.com/databricks/databricks-sql-go/v2/internal/client"
	dbsqlerrint "github.com/databricks/databricks-sql-go/v2/internal/errors"
	dbsqllog "github.com/databricks/databricks-sql-go/v2/logger"
	"github.com/stretchr/testify/require"
)

func TestCloudFetchRefetchRestoresFetchPosition(t *testing.T) {
	for _, outcome := range []string{"success", "rpc failure", "empty response"} {
		t.Run(outcome, func(t *testing.T) {
			calls := 0
			cli := &client.TestClient{FnFetchResults: func(_ context.Context, request *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
				calls++
				switch calls {
				case 1:
					require.Equal(t, int64(0), *request.StartRowOffset)
					require.Equal(t, int64(9), request.MaxRows)
					if outcome == "rpc failure" {
						return nil, errors.New("refetch failed")
					}
					if outcome == "empty response" {
						return &cli_service.TFetchResultsResp{}, nil
					}
				case 2, 3:
					require.Equal(t, int64(9), *request.StartRowOffset)
					if calls == 2 {
						return nil, errors.New("restore failed")
					}
				default:
					require.Nil(t, request.StartRowOffset, "successful restore must clear the override")
				}
				return &cli_service.TFetchResultsResp{Results: &cli_service.TRowSet{ResultLinks: []*cli_service.TSparkArrowResultLink{{StartRowOffset: 0, RowCount: 3}}}}, nil
			}}
			iterator := NewResultPageIterator(context.Background(), NewDelimiter(0, 9), 10, nil, nil, false, cli, dbsqllog.Logger).(*resultPageIterator)
			defer iterator.fetchCancel()
			_, err := iterator.RefetchLinks(context.Background(), &cli_service.TSparkArrowResultLink{StartRowOffset: 0, RowCount: 3})
			if outcome == "success" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.True(t, iterator.fetchPositionChanged)
			_, err = iterator.fetchPage(&cli_service.TFetchResultsReq{}, 9)
			require.Error(t, err)
			require.True(t, iterator.fetchPositionChanged)
			_, err = iterator.fetchPage(&cli_service.TFetchResultsReq{}, 9)
			require.NoError(t, err)
			require.False(t, iterator.fetchPositionChanged)
			_, err = iterator.fetchPage(&cli_service.TFetchResultsReq{}, 18)
			require.NoError(t, err)
			require.Equal(t, 4, calls)
		})
	}
}

func TestPageFetchCloseCancellationCategory(t *testing.T) {
	iterator := NewResultPageIterator(context.Background(), NewDelimiter(0, 0), 10, nil, nil, false, &client.TestClient{}, dbsqllog.Logger).(*resultPageIterator)
	defer iterator.fetchCancel()
	iterator.fetchCancel()
	require.NoError(t, iterator.ctx.Err())
	_, err := iterator.getNextPage()
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, dbsqlerrint.CategoryFromError(err))
	_, err = iterator.RefetchLinks(context.Background(), &cli_service.TSparkArrowResultLink{RowCount: 3})
	require.ErrorIs(t, err, context.Canceled)
}
