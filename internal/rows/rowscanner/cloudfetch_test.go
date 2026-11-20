package rowscanner

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	dbsqlerrint "github.com/databricks/databricks-sql-go/internal/errors"
	dbsqllog "github.com/databricks/databricks-sql-go/logger"
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

func TestCloudFetchCloseCancelsRefetch(t *testing.T) {
	started := make(chan struct{})
	finished := make(chan error, 1)
	closes := 0
	cli := &client.TestClient{
		FnFetchResults: func(ctx context.Context, _ *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
			close(started)
			<-ctx.Done()
			return nil, ctx.Err()
		},
		FnCloseOperation: func(ctx context.Context, _ *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
			closes++
			return &cli_service.TCloseOperationResp{}, ctx.Err()
		},
	}
	iterator := NewResultPageIterator(context.Background(), NewDelimiter(0, 3), 10, nil, nil, false, cli, dbsqllog.Logger).(*resultPageIterator)
	go func() {
		_, err := iterator.RefetchLinks(context.Background(), &cli_service.TSparkArrowResultLink{RowCount: 3})
		finished <- err
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("refetch did not start")
	}
	require.NoError(t, iterator.Close())
	select {
	case err := <-finished:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("close did not cancel refetch")
	}
	require.NoError(t, iterator.Close())
	require.Equal(t, 1, closes)
}

func TestPageFetchCloseCancellationCategory(t *testing.T) {
	iterator := NewResultPageIterator(context.Background(), NewDelimiter(0, 0), 10, nil, nil, false, &client.TestClient{}, dbsqllog.Logger).(*resultPageIterator)
	defer iterator.fetchCancel()
	iterator.fetchCancel()
	require.NoError(t, iterator.ctx.Err())
	_, err := iterator.getNextPage()
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, dbsqlerrint.CategoryFromError(err))
}
