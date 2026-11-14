package arrowbased

import (
	"bytes"
	"context"
	"database/sql/driver"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/config"
	dbsqlerrint "github.com/databricks/databricks-sql-go/internal/errors"
	"github.com/stretchr/testify/require"
)

func TestScalarCloudInitializationFailureDoesNotRetry(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()
	cfg := config.WithDefaults()
	scanner, err := NewArrowRowScanner(&cli_service.TGetResultSetMetadataResp{}, &cli_service.TRowSet{
		ResultLinks: []*cli_service.TSparkArrowResultLink{{FileLink: server.URL, RowCount: 1, ExpiryTime: time.Now().Add(time.Hour).Unix()}},
	}, cfg, nil, context.Background(), nil)
	require.NoError(t, err)
	defer scanner.Close()
	first := scanner.ScanRow(make([]driver.Value, 1), 0)
	require.Error(t, first)
	count := requests.Load()
	require.Positive(t, count)
	for range 3 {
		require.Same(t, first, scanner.ScanRow(make([]driver.Value, 1), 0))
	}
	require.Equal(t, count, requests.Load(), "later row scans must not repeat failed downloads")
	iterator, iteratorErr := scanner.GetArrowIPCStreams(context.Background(), *cfg, nil)
	require.NoError(t, iteratorErr)
	defer iterator.Close()
	_, iteratorErr = iterator.SchemaBytes()
	require.Error(t, iteratorErr)
	require.Greater(t, requests.Load(), count, "a new Arrow iterator may retry initialization")
}

func TestDiscoveredSchemaFieldCountMismatch(t *testing.T) {
	for _, cloud := range []bool{false, true} {
		for _, thriftCount := range []int{1, 3} {
			t.Run("cloud="+strconv.FormatBool(cloud)+"/columns="+strconv.Itoa(thriftCount), func(t *testing.T) {
				fields := []arrow.Field{{Name: "a", Type: arrow.PrimitiveTypes.Int64}, {Name: "b", Type: arrow.PrimitiveTypes.Int64}}
				builder := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema(fields, nil))
				defer builder.Release()
				for i := range fields {
					builder.Field(i).(*array.Int64Builder).Append(1)
				}
				record := builder.NewRecordBatch()
				defer record.Release()
				var payload bytes.Buffer
				writer := ipc.NewWriter(&payload, ipc.WithSchema(record.Schema()))
				require.NoError(t, writer.Write(record))
				require.NoError(t, writer.Close())
				metadata := &cli_service.TGetResultSetMetadataResp{Schema: &cli_service.TTableSchema{}}
				for range thriftCount {
					metadata.Schema.Columns = append(metadata.Schema.Columns, &cli_service.TColumnDesc{ColumnName: "value", TypeDesc: &cli_service.TTypeDesc{Types: []*cli_service.TTypeEntry{{PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{Type: cli_service.TTypeId_BIGINT_TYPE}}}}})
				}
				rowSet := &cli_service.TRowSet{ArrowBatches: []*cli_service.TSparkArrowBatch{{Batch: payload.Bytes(), RowCount: 1}}}
				if cloud {
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(payload.Bytes()) }))
					defer server.Close()
					rowSet.ArrowBatches = nil
					rowSet.ResultLinks = []*cli_service.TSparkArrowResultLink{{FileLink: server.URL, RowCount: 1, ExpiryTime: time.Now().Add(time.Hour).Unix()}}
				}
				require.NotPanics(t, func() {
					scanner, err := NewArrowRowScanner(metadata, rowSet, config.WithDefaults(), nil, context.Background(), nil)
					if scanner != nil {
						defer scanner.Close()
						err = scanner.ScanRow(make([]driver.Value, thriftCount), 0)
					}
					require.ErrorContains(t, err, "field count")
					require.Equal(t, dbsqlerrint.CategoryArrowSchemaParsing, dbsqlerrint.CategoryFromError(err))
				})
			})
		}
	}
}
