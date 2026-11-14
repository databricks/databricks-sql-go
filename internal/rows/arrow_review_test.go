package rows

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/databricks/databricks-sql-go/internal/rows/rowscanner"
	dbsqlrows "github.com/databricks/databricks-sql-go/rows"
	"github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/require"
)

func readReviewArrowResults(t *testing.T, result dbsqlrows.Rows, accessor string, check func(*arrow.Schema)) (int, int64) {
	t.Helper()
	var streams int
	var rows int64
	if accessor == "ipc" {
		iterator, err := result.GetArrowIPCStreams(context.Background())
		require.NoError(t, err)
		defer iterator.Close()
		schema, err := iterator.SchemaBytes()
		require.NoError(t, err)
		reader, err := ipc.NewReader(bytes.NewReader(schema))
		require.NoError(t, err)
		check(reader.Schema())
		reader.Release()
		for {
			stream, err := iterator.Next()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			streams++
			reader, err := ipc.NewReader(stream)
			require.NoError(t, err)
			check(reader.Schema())
			for reader.Next() {
				rows += reader.RecordBatch().NumRows()
			}
			require.NoError(t, reader.Err())
			reader.Release()
		}
	} else {
		iterator, err := result.GetArrowBatches(context.Background())
		require.NoError(t, err)
		defer iterator.Close()
		schema, err := iterator.Schema()
		require.NoError(t, err)
		check(schema)
		for {
			record, err := iterator.Next()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			streams++
			check(record.Schema())
			rows += record.NumRows()
			record.Release()
		}
	}
	return streams, rows
}

func TestArrowTrailingEmptyPage(t *testing.T) {
	for _, accessor := range []string{"ipc", "batches"} {
		for _, direct := range []bool{false, true} {
			t.Run(accessor+"/direct="+strconv.FormatBool(direct), func(t *testing.T) {
				var page cli_service.TFetchResultsResp
				loadTestData(t, "multipleFetch/FetchResults1.json", &page)
				count := rowscanner.CountRows(page.Results)
				last := &cli_service.TFetchResultsResp{HasMoreRows: new(bool), ResultSetMetadata: page.ResultSetMetadata,
					Results: &cli_service.TRowSet{ArrowBatches: []*cli_service.TSparkArrowBatch{{Batch: page.ResultSetMetadata.ArrowSchema}}}}
				var initial *cli_service.TSparkDirectResults
				if direct {
					initial = &cli_service.TSparkDirectResults{ResultSetMetadata: page.ResultSetMetadata, ResultSet: &page}
					last.Results.StartRowOffset = count
				}
				cli := &client.TestClient{
					FnGetResultSetMetadata: func(context.Context, *cli_service.TGetResultSetMetadataReq) (*cli_service.TGetResultSetMetadataResp, error) {
						return page.ResultSetMetadata, nil
					},
					FnFetchResults: func(context.Context, *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
						return last, nil
					},
					FnCloseOperation: func(context.Context, *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
						return &cli_service.TCloseOperationResp{}, nil
					},
				}
				result, err := NewRows(context.Background(), nil, cli, config.WithDefaults(), initial, nil)
				require.NoError(t, err)
				defer func() { require.NoError(t, result.Close()) }()
				chunks, rows := readReviewArrowResults(t, result.(dbsqlrows.Rows), accessor, func(*arrow.Schema) {})
				if direct {
					require.Equal(t, len(page.Results.ArrowBatches), chunks)
					require.Equal(t, count, rows)
				} else {
					require.Zero(t, chunks)
					require.Zero(t, rows)
				}
			})
		}
	}
}

func TestArrowCloudSchemaAliases(t *testing.T) {
	for _, accessor := range []string{"ipc", "batches"} {
		for _, direct := range []bool{false, true} {
			t.Run(accessor+"/direct="+strconv.FormatBool(direct), func(t *testing.T) {
				var page cli_service.TFetchResultsResp
				loadTestData(t, "multipleFetch/FetchResults1.json", &page)
				serverSchema, err := ipc.NewReader(bytes.NewReader(page.ResultSetMetadata.ArrowSchema))
				require.NoError(t, err)
				defer serverSchema.Release()
				files := map[string][]byte{}
				var downloads atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if downloads.Add(1) == 1 {
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					_, _ = w.Write(files[r.URL.Path])
				}))
				defer server.Close()
				var links []*cli_service.TSparkArrowResultLink
				var count int64
				for i, batch := range page.Results.ArrowBatches {
					path := fmt.Sprintf("/%d", i)
					files[path] = bytes.Join([][]byte{page.ResultSetMetadata.ArrowSchema, batch.Batch}, nil)
					links = append(links, &cli_service.TSparkArrowResultLink{FileLink: server.URL + path, StartRowOffset: count, RowCount: batch.RowCount, ExpiryTime: time.Now().Add(time.Hour).Unix()})
					count += batch.RowCount
				}
				for i, col := range page.ResultSetMetadata.Schema.Columns {
					col.ColumnName = fmt.Sprintf("alias_%d", i)
				}
				page.ResultSetMetadata.ArrowSchema = nil
				format := cli_service.TSparkRowSetType_URL_BASED_SET
				page.ResultSetMetadata.ResultFormat = &format
				// Split links across pages to exercise schema reuse as well as first-file replay.
				firstCount := links[0].RowCount
				page.Results = &cli_service.TRowSet{ResultLinks: links[:1]}
				last := &cli_service.TFetchResultsResp{HasMoreRows: new(bool), ResultSetMetadata: page.ResultSetMetadata, Results: &cli_service.TRowSet{StartRowOffset: firstCount, ResultLinks: links[1:]}}
				fetches := 0
				cli := &client.TestClient{
					FnFetchResults: func(context.Context, *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
						fetches++
						if !direct && fetches == 1 {
							return &page, nil
						}
						return last, nil
					},
					FnCloseOperation: func(context.Context, *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
						return &cli_service.TCloseOperationResp{}, nil
					},
				}
				var initial *cli_service.TSparkDirectResults
				if direct {
					initial = &cli_service.TSparkDirectResults{ResultSet: &page, ResultSetMetadata: page.ResultSetMetadata}
				}
				result, err := NewRows(context.Background(), nil, cli, config.WithDefaults(), initial, nil)
				require.NoError(t, err)
				defer func() { require.NoError(t, result.Close()) }()
				require.Zero(t, downloads.Load(), "QueryContext must not download CloudFetch files")
				if accessor == "ipc" {
					iterator, err := result.(dbsqlrows.Rows).GetArrowIPCStreams(context.Background())
					require.NoError(t, err)
					require.Zero(t, downloads.Load(), "getter must remain lazy")
					require.Zero(t, fetches)
					_, err = iterator.SchemaBytes()
					require.Error(t, err)
					iterator.Close()
				} else {
					iterator, err := result.(dbsqlrows.Rows).GetArrowBatches(context.Background())
					require.NoError(t, err)
					require.Zero(t, downloads.Load(), "getter must remain lazy")
					require.Zero(t, fetches)
					_, err = iterator.Schema()
					require.Error(t, err)
					iterator.Close()
				}
				_, rows := readReviewArrowResults(t, result.(dbsqlrows.Rows), accessor, func(schema *arrow.Schema) {
					for i, field := range schema.Fields() {
						require.Equal(t, fmt.Sprintf("alias_%d", i), field.Name)
						require.True(t, arrow.TypeEqual(serverSchema.Schema().Field(i).Type, field.Type))
						require.Equal(t, serverSchema.Schema().Field(i).Nullable, field.Nullable)
						require.True(t, serverSchema.Schema().Field(i).Metadata.Equal(field.Metadata))
					}
				})
				require.Equal(t, count, rows)
			})
		}
	}
}

func TestArrowEmptyNativeSchemaError(t *testing.T) {
	for _, accessor := range []string{"ipc", "batches"} {
		t.Run(accessor, func(t *testing.T) {
			metadata := &cli_service.TGetResultSetMetadataResp{Schema: &cli_service.TTableSchema{Columns: []*cli_service.TColumnDesc{{ColumnName: "items", TypeDesc: &cli_service.TTypeDesc{Types: []*cli_service.TTypeEntry{{PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{Type: cli_service.TTypeId_ARRAY_TYPE}}}}}}}}
			cli := &client.TestClient{
				FnFetchResults: func(context.Context, *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
					return &cli_service.TFetchResultsResp{ResultSetMetadata: metadata, Results: &cli_service.TRowSet{ArrowBatches: []*cli_service.TSparkArrowBatch{}}, HasMoreRows: new(bool)}, nil
				},
				FnCloseOperation: func(context.Context, *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
					return &cli_service.TCloseOperationResp{}, nil
				},
			}
			result, err := NewRows(context.Background(), nil, cli, config.WithDefaults(), nil, nil)
			require.NoError(t, err)
			defer func() { require.NoError(t, result.Close()) }()
			if accessor == "ipc" {
				iterator, err := result.(dbsqlrows.Rows).GetArrowIPCStreams(context.Background())
				require.NoError(t, err)
				defer iterator.Close()
				_, err = iterator.SchemaBytes()
				require.ErrorContains(t, err, "ARRAY require arrow schema")
				_, err = iterator.Next()
				require.ErrorIs(t, err, io.EOF)
			} else {
				iterator, err := result.(dbsqlrows.Rows).GetArrowBatches(context.Background())
				require.NoError(t, err)
				defer iterator.Close()
				_, err = iterator.Schema()
				require.ErrorContains(t, err, "ARRAY require arrow schema")
				_, err = iterator.Next()
				require.ErrorIs(t, err, io.EOF)
			}
		})
	}
}

func TestArrowLateNTZMetadata(t *testing.T) {
	metadata := &cli_service.TGetResultSetMetadataResp{Schema: &cli_service.TTableSchema{Columns: []*cli_service.TColumnDesc{{ColumnName: "ts", TypeDesc: &cli_service.TTypeDesc{Types: []*cli_service.TTypeEntry{{PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{Type: cli_service.TTypeId_TIMESTAMP_TYPE}}}}}}}}
	r := &rows{client: &client.TestClient{}, resultSetMetadata: metadata, schema: metadata.Schema, config: config.WithDefaults()}
	require.Equal(t, "TIMESTAMP", r.ColumnTypeDatabaseTypeName(0))
	var serialized bytes.Buffer
	w := ipc.NewWriter(&serialized, ipc.WithSchema(arrow.NewSchema([]arrow.Field{{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond}}}, nil)))
	require.NoError(t, w.Close())
	r.useServerArrowSchema(&cli_service.TGetResultSetMetadataResp{ArrowSchema: serialized.Bytes()})
	require.Equal(t, "TIMESTAMP_NTZ", r.ColumnTypeDatabaseTypeName(0))
}

func TestArrowMetadataRetryKeepsFirstPage(t *testing.T) {
	for _, accessor := range []string{"ipc", "batches"} {
		t.Run(accessor, func(t *testing.T) {
			var page cli_service.TFetchResultsResp
			loadTestData(t, "multipleFetch/FetchResults1.json", &page)
			metadata := page.ResultSetMetadata
			page.ResultSetMetadata, page.HasMoreRows = nil, new(bool)
			fetches, metadataCalls := 0, 0
			unavailable := errors.New("metadata temporarily unavailable")
			cli := &client.TestClient{
				FnFetchResults: func(context.Context, *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
					fetches++
					return &page, nil
				},
				FnGetResultSetMetadata: func(context.Context, *cli_service.TGetResultSetMetadataReq) (*cli_service.TGetResultSetMetadataResp, error) {
					metadataCalls++
					if metadataCalls == 1 {
						return nil, unavailable
					}
					return metadata, nil
				},
				FnCloseOperation: func(context.Context, *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
					return &cli_service.TCloseOperationResp{}, nil
				},
			}
			result, err := NewRows(context.Background(), nil, cli, config.WithDefaults(), nil, nil)
			require.NoError(t, err)
			defer func() { require.NoError(t, result.Close()) }()
			if accessor == "ipc" {
				iterator, err := result.(dbsqlrows.Rows).GetArrowIPCStreams(context.Background())
				require.NoError(t, err)
				_, err = iterator.SchemaBytes()
				require.ErrorIs(t, err, unavailable)
				iterator.Close()
			} else {
				iterator, err := result.(dbsqlrows.Rows).GetArrowBatches(context.Background())
				require.NoError(t, err)
				_, err = iterator.Schema()
				require.ErrorIs(t, err, unavailable)
				iterator.Close()
			}
			_, count := readReviewArrowResults(t, result.(dbsqlrows.Rows), accessor, func(*arrow.Schema) {})
			require.EqualValues(t, 7311, count)
			require.Equal(t, 1, fetches)
			require.Equal(t, 2, metadataCalls)
		})
	}
}

func TestArrowEmbeddedSchemaPages(t *testing.T) {
	for _, accessor := range []string{"ipc", "batches"} {
		for _, compressed := range []bool{false, true} {
			t.Run(accessor+"/lz4="+strconv.FormatBool(compressed), func(t *testing.T) {
				var first, second cli_service.TFetchResultsResp
				loadTestData(t, "multipleFetch/FetchResults1.json", &first)
				loadTestData(t, "multipleFetch/FetchResults1.json", &second)
				count := rowscanner.CountRows(first.Results)
				second.Results.StartRowOffset = count
				second.HasMoreRows = new(bool)
				for _, page := range []*cli_service.TFetchResultsResp{&first, &second} {
					for i, batch := range page.Results.ArrowBatches {
						payload := batch.Batch
						// Alternate complete streams and bare record messages on each page.
						if i%2 == 0 {
							payload = bytes.Join([][]byte{page.ResultSetMetadata.ArrowSchema, payload}, nil)
						}
						if compressed {
							var output bytes.Buffer
							writer := lz4.NewWriter(&output)
							_, err := writer.Write(payload)
							require.NoError(t, err)
							require.NoError(t, writer.Close())
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
						if fetches == 1 {
							return &first, nil
						}
						return &second, nil
					},
					FnCloseOperation: func(context.Context, *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
						return &cli_service.TCloseOperationResp{}, nil
					},
				}
				cfg := config.WithDefaults()
				cfg.UseLz4Compression = compressed
				result, err := NewRows(context.Background(), nil, cli, cfg, nil, nil)
				require.NoError(t, err)
				defer func() { require.NoError(t, result.Close()) }()
				_, rows := readReviewArrowResults(t, result.(dbsqlrows.Rows), accessor, func(*arrow.Schema) {})
				require.Equal(t, 2*count, rows)
			})
		}
	}
}
