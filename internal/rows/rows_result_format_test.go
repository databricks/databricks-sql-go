package rows

import (
	"context"
	"database/sql/driver"
	"io"
	"testing"

	"github.com/databricks/databricks-sql-go/driverctx"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExecutionResultFormat(t *testing.T) {
	t.Parallel()
	format := func(v cli_service.TSparkRowSetType) *cli_service.TGetResultSetMetadataResp {
		return &cli_service.TGetResultSetMetadataResp{ResultFormat: &v}
	}
	for name, tc := range map[string]struct {
		md   *cli_service.TGetResultSetMetadataResp
		want string
	}{
		"nil metadata":    {nil, ""},
		"format unset":    {&cli_service.TGetResultSetMetadataResp{}, ""},
		"arrow based":     {format(cli_service.TSparkRowSetType_ARROW_BASED_SET), "INLINE_ARROW"},
		"url based":       {format(cli_service.TSparkRowSetType_URL_BASED_SET), "EXTERNAL_LINKS"},
		"column based":    {format(cli_service.TSparkRowSetType_COLUMN_BASED_SET), "COLUMNAR_INLINE"},
		"row based":       {format(cli_service.TSparkRowSetType_ROW_BASED_SET), ""},
		"unknown ordinal": {format(cli_service.TSparkRowSetType(99)), ""},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, ExecutionResultFormat(tc.md))
		})
	}
}

func TestRows_ResultFormatCallback(t *testing.T) {
	t.Parallel()
	ctx := driverctx.NewContextWithConnId(context.Background(), "connId")
	columnar := cli_service.TSparkRowSetType_COLUMN_BASED_SET
	arrowBased := cli_service.TSparkRowSetType_ARROW_BASED_SET
	schema := &cli_service.TTableSchema{Columns: []*cli_service.TColumnDesc{{
		ColumnName: "n",
		TypeDesc: &cli_service.TTypeDesc{Types: []*cli_service.TTypeEntry{{
			PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{Type: cli_service.TTypeId_INT_TYPE},
		}}},
	}}}

	// recorder captures callback order so the format is known to precede OnClose,
	// which finalizes the statement metric.
	recorder := func(events *[]string) *TelemetryCallbacks {
		return &TelemetryCallbacks{
			OnResultFormat: func(format string) { *events = append(*events, "format:"+format) },
			OnClose:        func(int64, int, error, error) { *events = append(*events, "close") },
		}
	}

	t.Run("direct results metadata is reported before OnClose", func(t *testing.T) {
		t.Parallel()
		var events []string
		var metadataCalls int
		cli := &client.TestClient{FnGetResultSetMetadata: func(context.Context, *cli_service.TGetResultSetMetadataReq) (*cli_service.TGetResultSetMetadataResp, error) {
			metadataCalls++
			return nil, io.ErrUnexpectedEOF
		}}
		dr := &cli_service.TSparkDirectResults{
			ResultSetMetadata: &cli_service.TGetResultSetMetadataResp{Schema: schema, ResultFormat: &arrowBased},
			CloseOperation:    &cli_service.TCloseOperationResp{},
		}
		r, err := NewRows(ctx, nil, cli, config.WithDefaults(), dr, recorder(&events))
		require.Nil(t, err)
		require.NoError(t, r.Close())
		assert.Equal(t, []string{"format:INLINE_ARROW", "close"}, events)
		assert.Zero(t, metadataCalls)
	})

	t.Run("format fetched with the first page is reported", func(t *testing.T) {
		t.Parallel()
		var events []string
		noMore := false
		cli := &client.TestClient{
			FnGetResultSetMetadata: func(context.Context, *cli_service.TGetResultSetMetadataReq) (*cli_service.TGetResultSetMetadataResp, error) {
				return &cli_service.TGetResultSetMetadataResp{Schema: schema, ResultFormat: &columnar}, nil
			},
			FnFetchResults: func(context.Context, *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
				return &cli_service.TFetchResultsResp{HasMoreRows: &noMore, Results: &cli_service.TRowSet{Columns: []*cli_service.TColumn{{
					I32Val: &cli_service.TI32Column{Values: []int32{1}, Nulls: []byte{0}},
				}}}}, nil
			},
		}
		r, err := NewRows(ctx, nil, cli, config.WithDefaults(), nil, recorder(&events))
		require.Nil(t, err)
		dest := make([]driver.Value, 1)
		require.NoError(t, r.Next(dest))
		require.NoError(t, r.Close())
		assert.Equal(t, []string{"format:COLUMNAR_INLINE", "close"}, events)
	})

	t.Run("unknown format is not reported and not fetched", func(t *testing.T) {
		t.Parallel()
		var events []string
		var metadataCalls int
		cli := &client.TestClient{
			FnGetResultSetMetadata: func(context.Context, *cli_service.TGetResultSetMetadataReq) (*cli_service.TGetResultSetMetadataResp, error) {
				metadataCalls++
				return &cli_service.TGetResultSetMetadataResp{Schema: schema, ResultFormat: &columnar}, nil
			},
			FnCloseOperation: func(context.Context, *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
				return &cli_service.TCloseOperationResp{}, nil
			},
		}
		r, err := NewRows(ctx, &cli_service.TOperationHandle{OperationId: &cli_service.THandleIdentifier{GUID: []byte{1}}}, cli, config.WithDefaults(), nil, recorder(&events))
		require.Nil(t, err)
		require.NoError(t, r.Close())
		assert.Equal(t, []string{"close"}, events)
		assert.Zero(t, metadataCalls)
	})
}
