package rows

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	dbsqlrows "github.com/databricks/databricks-sql-go/rows"
	"github.com/pierrec/lz4/v4"
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

func BenchmarkArrowDirectInline(b *testing.B) {
	for _, batchRows := range []int{20, 500} {
		for _, compressed := range []bool{false, true} {
			for _, accessor := range []string{"next", "ipc"} {
				b.Run(fmt.Sprintf("%s/rowsPerBatch=%d/lz4=%t", accessor, batchRows, compressed), func(b *testing.B) {
					schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "name", Type: arrow.BinaryTypes.String}}, nil)
					ids := array.NewInt64Builder(memory.DefaultAllocator)
					names := array.NewStringBuilder(memory.DefaultAllocator)
					defer ids.Release()
					defer names.Release()
					for i := range batchRows {
						ids.Append(int64(i))
						names.Append("benchmark value")
					}
					idValues, nameValues := ids.NewArray(), names.NewArray()
					defer idValues.Release()
					defer nameValues.Release()
					record := array.NewRecordBatch(schema, []arrow.Array{idValues, nameValues}, int64(batchRows))
					defer record.Release()
					var serializedSchema, serializedRecord bytes.Buffer
					writer := ipc.NewWriter(&serializedSchema, ipc.WithSchema(schema))
					require.NoError(b, writer.Close())
					schemaBytes := serializedSchema.Bytes()[:serializedSchema.Len()-8]
					writer = ipc.NewWriter(&serializedRecord, ipc.WithSchema(schema))
					require.NoError(b, writer.Write(record))
					require.NoError(b, writer.Close())
					payload := serializedRecord.Bytes()[len(schemaBytes):]
					if compressed {
						var output bytes.Buffer
						compressor := lz4.NewWriter(&output)
						require.NoError(b, compressor.Apply(lz4.BlockSizeOption(lz4.Block64Kb)))
						_, err := compressor.Write(payload)
						require.NoError(b, err)
						require.NoError(b, compressor.Close())
						payload = output.Bytes()
					}
					var columns []*cli_service.TColumnDesc
					for i, typeID := range []cli_service.TTypeId{cli_service.TTypeId_BIGINT_TYPE, cli_service.TTypeId_STRING_TYPE} {
						columns = append(columns, &cli_service.TColumnDesc{ColumnName: schema.Field(i).Name, TypeDesc: &cli_service.TTypeDesc{Types: []*cli_service.TTypeEntry{{PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{Type: typeID}}}}})
					}
					var batches []*cli_service.TSparkArrowBatch
					for range 10000 / batchRows {
						batches = append(batches, &cli_service.TSparkArrowBatch{Batch: payload, RowCount: int64(batchRows)})
					}
					format := cli_service.TSparkRowSetType_ARROW_BASED_SET
					metadata := &cli_service.TGetResultSetMetadataResp{ArrowSchema: schemaBytes, Schema: &cli_service.TTableSchema{Columns: columns}, ResultFormat: &format}
					direct := &cli_service.TSparkDirectResults{ResultSetMetadata: metadata, ResultSet: &cli_service.TFetchResultsResp{Results: &cli_service.TRowSet{ArrowBatches: batches}, HasMoreRows: new(bool)}, CloseOperation: &cli_service.TCloseOperationResp{}}
					cfg := config.WithDefaults()
					cfg.UseLz4Compression = compressed
					ctx := context.Background()
					cli := &client.TestClient{}
					b.ReportAllocs()
					b.ResetTimer()
					for b.Loop() {
						result, rowsErr := NewRows(ctx, nil, cli, cfg, direct, nil)
						if rowsErr != nil {
							b.Fatal(rowsErr)
						}
						var count int64
						if accessor == "next" {
							destination := make([]driver.Value, 2)
							for {
								err := result.Next(destination)
								if err == io.EOF {
									break
								}
								if err != nil {
									b.Fatal(err)
								}
								count++
							}
						} else {
							iterator, err := result.(dbsqlrows.Rows).GetArrowIPCStreams(ctx)
							if err != nil {
								b.Fatal(err)
							}
							for {
								stream, err := iterator.Next()
								if err == io.EOF {
									break
								}
								if err != nil {
									b.Fatal(err)
								}
								reader, err := ipc.NewReader(stream)
								if err != nil {
									b.Fatal(err)
								}
								for reader.Next() {
									count += reader.RecordBatch().NumRows()
								}
								if reader.Err() != nil {
									b.Fatal(reader.Err())
								}
								reader.Release()
							}
							iterator.Close()
						}
						if count != 10000 {
							b.Fatalf("row count: %d", count)
						}
						if err := result.Close(); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}
	}
}
