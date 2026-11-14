package rows

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	dbsqlrows "github.com/databricks/databricks-sql-go/rows"
	"github.com/stretchr/testify/require"
)

func BenchmarkArrowNoDirect(b *testing.B) {
	data, err := os.ReadFile("testdata/multipleFetch/FetchResults1.json")
	require.NoError(b, err)
	var page cli_service.TFetchResultsResp
	require.NoError(b, json.Unmarshal(data, &page))
	page.HasMoreRows = new(bool)
	cli := &client.TestClient{
		FnFetchResults: func(context.Context, *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
			return &page, nil
		},
		FnGetResultSetMetadata: func(context.Context, *cli_service.TGetResultSetMetadataReq) (*cli_service.TGetResultSetMetadataResp, error) {
			return page.ResultSetMetadata, nil
		},
		FnCloseOperation: func(context.Context, *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
			return &cli_service.TCloseOperationResp{}, nil
		},
	}
	cfg := config.WithDefaults()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		result, rowsErr := NewRows(ctx, nil, cli, cfg, nil, nil)
		require.NoError(b, rowsErr)
		iterator, err := result.(dbsqlrows.Rows).GetArrowIPCStreams(ctx)
		require.NoError(b, err)
		// Fetch before SchemaBytes so main and the fix perform equivalent work.
		var count int64
		for {
			stream, err := iterator.Next()
			if err == io.EOF {
				break
			}
			require.NoError(b, err)
			reader, err := ipc.NewReader(stream)
			require.NoError(b, err)
			for reader.Next() {
				count += reader.RecordBatch().NumRows()
			}
			require.NoError(b, reader.Err())
			reader.Release()
		}
		require.EqualValues(b, 7311, count)
		schema, err := iterator.SchemaBytes()
		require.NoError(b, err)
		reader, err := ipc.NewReader(bytes.NewReader(schema))
		require.NoError(b, err)
		reader.Release()
		iterator.Close()
		require.NoError(b, result.Close())
	}
}
