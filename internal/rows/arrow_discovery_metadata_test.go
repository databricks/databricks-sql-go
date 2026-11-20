package rows

import (
	"bytes"
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/databricks/databricks-sql-go/internal/rows/rowscanner"
	dbsqlrows "github.com/databricks/databricks-sql-go/rows"
	"github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/require"
)

func TestArrowDiscoveredColumnMetadata(test *testing.T) {
	for _, empty := range []bool{false, true} {
		for _, cloud := range []bool{false, true} {
			if empty && cloud {
				continue
			}
			for _, accessor := range []string{"next", "ipc", "batches"} {
				test.Run(fmt.Sprintf("empty=%t/cloud=%t/%s", empty, cloud, accessor), func(test *testing.T) {
					schema := arrow.NewSchema([]arrow.Field{
						{Name: "variant", Type: arrow.BinaryTypes.String, Nullable: true, Metadata: arrow.MetadataFrom(map[string]string{"Spark:DataType:SqlName": "VARIANT"})},
						{Name: "clock", Type: arrow.FixedWidthTypes.Time64ns, Nullable: true},
						{Name: "stamp", Type: &arrow.TimestampType{Unit: arrow.Microsecond}, Nullable: true},
						{Name: "items", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64), Nullable: true},
					}, nil)
					builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
					defer builder.Release()
					var count int64
					if !empty {
						for _, field := range builder.Fields() {
							field.AppendNull()
						}
						count = 1
					}
					record := builder.NewRecordBatch()
					defer record.Release()
					var serialized bytes.Buffer
					writer := ipc.NewWriter(&serialized, ipc.WithSchema(schema))
					require.NoError(test, writer.Write(record))
					require.NoError(test, writer.Close())
					format := cli_service.TSparkRowSetType_ARROW_BASED_SET
					metadata := &cli_service.TGetResultSetMetadataResp{Schema: &cli_service.TTableSchema{}, ResultFormat: &format}
					for index, typeID := range []cli_service.TTypeId{cli_service.TTypeId_STRING_TYPE, cli_service.TTypeId_STRING_TYPE, cli_service.TTypeId_TIMESTAMP_TYPE, cli_service.TTypeId_ARRAY_TYPE} {
						column := decimalMetadataColumn(typeID, nil, nil)
						column.ColumnName = schema.Field(index).Name
						metadata.Schema.Columns = append(metadata.Schema.Columns, column)
					}
					page := &cli_service.TFetchResultsResp{ResultSetMetadata: metadata, HasMoreRows: new(bool), Results: &cli_service.TRowSet{ArrowBatches: []*cli_service.TSparkArrowBatch{{Batch: serialized.Bytes(), RowCount: count}}}}
					if cloud {
						server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) { _, _ = response.Write(serialized.Bytes()) }))
						defer server.Close()
						format = cli_service.TSparkRowSetType_URL_BASED_SET
						page.Results = &cli_service.TRowSet{ResultLinks: []*cli_service.TSparkArrowResultLink{{FileLink: server.URL, RowCount: 1, ExpiryTime: time.Now().Add(time.Hour).Unix()}}}
					}
					fetches, metadataCalls := 0, 0
					cli := &client.TestClient{
						FnFetchResults: func(context.Context, *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
							fetches++
							return page, nil
						},
						FnGetResultSetMetadata: func(context.Context, *cli_service.TGetResultSetMetadataReq) (*cli_service.TGetResultSetMetadataResp, error) {
							metadataCalls++
							return metadata, nil
						},
						FnCloseOperation: func(context.Context, *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
							return &cli_service.TCloseOperationResp{}, nil
						},
					}
					cfg := config.WithDefaults()
					cfg.UseArrowNativeComplexTypes = true
					result, rowsErr := NewRows(context.Background(), nil, cli, cfg, nil, nil)
					require.NoError(test, rowsErr)
					defer func() { require.NoError(test, result.Close()) }()
					columnRows := result.(*rows)
					require.Equal(test, "STRING", columnRows.ColumnTypeDatabaseTypeName(0))
					require.Equal(test, "STRING", columnRows.ColumnTypeDatabaseTypeName(1))
					require.Equal(test, "TIMESTAMP", columnRows.ColumnTypeDatabaseTypeName(2))
					if accessor == "next" {
						err := result.Next(make([]driver.Value, 4))
						if empty {
							require.ErrorIs(test, err, io.EOF)
						} else {
							require.NoError(test, err)
						}
					} else {
						streams, rows := readReviewArrowResults(test, result.(dbsqlrows.Rows), accessor, func(actual *arrow.Schema) { require.True(test, schema.Equal(actual)) })
						require.Equal(test, count, rows)
						if empty {
							require.Zero(test, streams)
						}
					}
					require.Equal(test, "VARIANT", columnRows.ColumnTypeDatabaseTypeName(0))
					require.Equal(test, "TIME", columnRows.ColumnTypeDatabaseTypeName(1))
					require.Equal(test, "TIMESTAMP_NTZ", columnRows.ColumnTypeDatabaseTypeName(2))
					require.Equal(test, scanTypeDateTime, columnRows.ColumnTypeScanType(1))
					_, hasLength := columnRows.ColumnTypeLength(1)
					require.False(test, hasLength)
					require.Equal(test, 1, fetches)
					require.Equal(test, 1, metadataCalls)
				})
			}
		}
	}
}

func TestArrowMetadataRetryCountsPageOnce(test *testing.T) {
	var page cli_service.TFetchResultsResp
	loadTestData(test, "multipleFetch/FetchResults1.json", &page)
	metadata := page.ResultSetMetadata
	page.ResultSetMetadata, page.HasMoreRows = nil, new(bool)
	fetches, metadataCalls := 0, 0
	unavailable := errors.New("temporary metadata failure")
	cli := &client.TestClient{
		FnFetchResults: func(context.Context, *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
			fetches++
			return &page, nil
		},
		FnGetResultSetMetadata: func(context.Context, *cli_service.TGetResultSetMetadataReq) (*cli_service.TGetResultSetMetadataResp, error) {
			metadataCalls++
			if metadataCalls < 3 {
				return nil, unavailable
			}
			return metadata, nil
		},
		FnCloseOperation: func(context.Context, *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
			return &cli_service.TCloseOperationResp{}, nil
		},
	}
	var indexes []int
	result, rowsErr := NewRows(context.Background(), nil, cli, config.WithDefaults(), nil, &TelemetryCallbacks{OnChunkFetched: func(count int, size int64, index int, latency int64, total int32) { indexes = append(indexes, index) }})
	require.NoError(test, rowsErr)
	defer func() { require.NoError(test, result.Close()) }()
	iterator, err := result.(dbsqlrows.Rows).GetArrowIPCStreams(context.Background())
	require.NoError(test, err)
	_, err = iterator.SchemaBytes()
	require.ErrorIs(test, err, unavailable)
	iterator.Close()
	destination := make([]driver.Value, len(metadata.Schema.Columns))
	require.ErrorIs(test, result.Next(destination), unavailable)
	require.NoError(test, result.Next(destination))
	require.Equal(test, []int{0}, indexes)
	require.Equal(test, 1, fetches)
	require.Equal(test, 3, metadataCalls)
}

func TestEmptyArrowPayloadDoesNotFetchMetadataOnNext(test *testing.T) {
	var payload bytes.Buffer
	writer := ipc.NewWriter(&payload, ipc.WithSchema(arrow.NewSchema(nil, nil)))
	require.NoError(test, writer.Close())
	metadataCalls := 0
	cli := &client.TestClient{
		FnFetchResults: func(context.Context, *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
			return &cli_service.TFetchResultsResp{HasMoreRows: new(bool), Results: &cli_service.TRowSet{ArrowBatches: []*cli_service.TSparkArrowBatch{{Batch: payload.Bytes()}}}}, nil
		},
		FnGetResultSetMetadata: func(context.Context, *cli_service.TGetResultSetMetadataReq) (*cli_service.TGetResultSetMetadataResp, error) {
			metadataCalls++
			return nil, errors.New("operation already closed")
		},
		FnCloseOperation: func(context.Context, *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
			return &cli_service.TCloseOperationResp{}, nil
		},
	}
	result, rowsErr := NewRows(context.Background(), nil, cli, config.WithDefaults(), nil, nil)
	require.NoError(test, rowsErr)
	defer func() { require.NoError(test, result.Close()) }()
	require.ErrorIs(test, result.Next(nil), io.EOF)
	require.Zero(test, metadataCalls)
}

func TestScalarEmbeddedSchemaPages(test *testing.T) {
	for _, compressed := range []bool{false, true} {
		for _, direct := range []bool{false, true} {
			test.Run(fmt.Sprintf("lz4=%t/direct=%t", compressed, direct), func(test *testing.T) {
				var first, second cli_service.TFetchResultsResp
				loadTestData(test, "multipleFetch/FetchResults1.json", &first)
				loadTestData(test, "multipleFetch/FetchResults1.json", &second)
				count := rowscanner.CountRows(first.Results)
				second.Results.StartRowOffset = count
				second.HasMoreRows = new(bool)
				for _, page := range []*cli_service.TFetchResultsResp{&first, &second} {
					for index, batch := range page.Results.ArrowBatches {
						payload := batch.Batch
						if index%2 == 0 {
							payload = bytes.Join([][]byte{page.ResultSetMetadata.ArrowSchema, payload}, nil)
						}
						if compressed {
							var output bytes.Buffer
							compressor := lz4.NewWriter(&output)
							_, err := compressor.Write(payload)
							require.NoError(test, err)
							require.NoError(test, compressor.Close())
							payload = output.Bytes()
						}
						batch.Batch = payload
					}
					page.ResultSetMetadata.ArrowSchema = nil
				}
				fetches := 0
				cli := &client.TestClient{
					FnFetchResults: func(context.Context, *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
						fetches++
						if !direct && fetches == 1 {
							return &first, nil
						}
						return &second, nil
					},
					FnCloseOperation: func(context.Context, *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
						return &cli_service.TCloseOperationResp{}, nil
					},
				}
				var initial *cli_service.TSparkDirectResults
				if direct {
					initial = &cli_service.TSparkDirectResults{ResultSetMetadata: first.ResultSetMetadata, ResultSet: &first}
				}
				cfg := config.WithDefaults()
				cfg.UseLz4Compression = compressed
				result, rowsErr := NewRows(context.Background(), nil, cli, cfg, initial, nil)
				require.NoError(test, rowsErr)
				defer func() { require.NoError(test, result.Close()) }()
				destination := make([]driver.Value, len(first.ResultSetMetadata.Schema.Columns))
				var actual int64
				for {
					err := result.Next(destination)
					if err == io.EOF {
						break
					}
					require.NoError(test, err)
					actual++
				}
				require.Equal(test, count*2, actual)
				if direct {
					require.Equal(test, 1, fetches)
				} else {
					require.Equal(test, 2, fetches)
				}
			})
		}
	}
}
