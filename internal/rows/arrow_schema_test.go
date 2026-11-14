package rows

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	dbsqlrows "github.com/databricks/databricks-sql-go/rows"
	"github.com/stretchr/testify/require"
)

func TestArrowSchemaBeforeFetching(t *testing.T) {
	for _, accessor := range []string{"ipc", "batches"} {
		for _, empty := range []bool{false, true} {
			for _, schemaShape := range []string{"server", "fetch", "inline", "cloud", "nil", "empty"} {
				t.Run(accessor+"/"+schemaShape+"/empty="+strconv.FormatBool(empty), func(t *testing.T) {
					var page cli_service.TFetchResultsResp
					loadTestData(t, "multipleFetch/FetchResults1.json", &page)
					serverBytes := page.ResultSetMetadata.ArrowSchema
					serverReader, err := ipc.NewReader(bytes.NewReader(serverBytes))
					require.NoError(t, err)
					defer serverReader.Release()
					if empty {
						page.Results.ArrowBatches = nil
					}
					metadata := *page.ResultSetMetadata
					if schemaShape != "server" {
						metadata.ArrowSchema = nil
					}
					if schemaShape != "server" && schemaShape != "fetch" {
						page.ResultSetMetadata.ArrowSchema = nil
					}
					if schemaShape == "empty" {
						metadata.ArrowSchema = []byte{}
						page.ResultSetMetadata.ArrowSchema = []byte{}
					}
					var downloads atomic.Int32
					expectedDownloads := int32(0)
					if schemaShape == "inline" || schemaShape == "cloud" {
						payload := append([]byte{}, serverBytes...)
						if !empty {
							payload = append(payload, page.Results.ArrowBatches[0].Batch...)
						}
						if schemaShape == "inline" {
							if empty {
								page.Results.ArrowBatches = []*cli_service.TSparkArrowBatch{{Batch: payload}}
							} else {
								page.Results.ArrowBatches[0].Batch = payload
							}
						} else {
							files := make(map[string][]byte)
							server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
								downloads.Add(1)
								_, _ = writer.Write(files[request.URL.Path])
							}))
							defer server.Close()
							batches := page.Results.ArrowBatches
							if empty {
								batches = []*cli_service.TSparkArrowBatch{{}}
							}
							var offset int64
							for index, batch := range batches {
								path := "/" + strconv.Itoa(index)
								files[path] = bytes.Join([][]byte{serverBytes, batch.Batch}, nil)
								page.Results.ResultLinks = append(page.Results.ResultLinks, &cli_service.TSparkArrowResultLink{FileLink: server.URL + path, StartRowOffset: offset, RowCount: batch.RowCount, ExpiryTime: time.Now().Add(time.Hour).Unix()})
								offset += batch.RowCount
								expectedDownloads++
							}
							page.Results.ArrowBatches = nil
							format := cli_service.TSparkRowSetType_URL_BASED_SET
							page.ResultSetMetadata.ResultFormat = &format
							metadata.ResultFormat = &format
						}
					}
					if schemaShape != "nil" && schemaShape != "empty" && (!empty || schemaShape == "server" || schemaShape == "fetch") {
						columns := make([]*cli_service.TColumnDesc, len(metadata.Schema.Columns))
						for i, column := range metadata.Schema.Columns {
							columns[i] = &cli_service.TColumnDesc{ColumnName: column.ColumnName, TypeDesc: &cli_service.TTypeDesc{Types: []*cli_service.TTypeEntry{{PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{Type: cli_service.TTypeId(999)}}}}}
						}
						metadata.Schema = &cli_service.TTableSchema{Columns: columns}
					}
					page.HasMoreRows = new(bool)
					metadataCalls, fetchCalls := 0, 0
					fakeClient := &client.TestClient{
						FnGetResultSetMetadata: func(context.Context, *cli_service.TGetResultSetMetadataReq) (*cli_service.TGetResultSetMetadataResp, error) {
							metadataCalls++
							return &metadata, nil
						},
						FnFetchResults: func(context.Context, *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
							fetchCalls++
							return &page, nil
						},
						FnCloseOperation: func(context.Context, *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
							return &cli_service.TCloseOperationResp{}, nil
						},
					}
					result, err := NewRows(context.Background(), nil, fakeClient, config.WithDefaults(), nil, nil)
					require.NoError(t, err)
					defer func() { require.NoError(t, result.Close()) }()
					var rowCount int64
					if accessor == "ipc" {
						iterator, err := result.(dbsqlrows.Rows).GetArrowIPCStreams(context.Background())
						require.NoError(t, err)
						defer iterator.Close()
						schemaBytes, err := iterator.SchemaBytes()
						require.NoError(t, err)
						require.NotEmpty(t, schemaBytes)
						schemaReader, err := ipc.NewReader(bytes.NewReader(schemaBytes))
						require.NoError(t, err)
						defer schemaReader.Release()
						assertArrowSchemaTypes(t, serverReader.Schema(), schemaReader.Schema())
						for {
							stream, err := iterator.Next()
							if err == io.EOF {
								break
							}
							require.NoError(t, err)
							reader, err := ipc.NewReader(stream)
							require.NoError(t, err)
							require.True(t, schemaReader.Schema().Equal(reader.Schema()))
							for reader.Next() {
								rowCount += reader.RecordBatch().NumRows()
							}
							require.NoError(t, reader.Err())
							reader.Release()
						}
					} else {
						iterator, err := result.(dbsqlrows.Rows).GetArrowBatches(context.Background())
						require.NoError(t, err)
						defer iterator.Close()
						schema, err := iterator.Schema()
						require.NoError(t, err)
						assertArrowSchemaTypes(t, serverReader.Schema(), schema)
						for {
							record, err := iterator.Next()
							if err == io.EOF {
								break
							}
							require.NoError(t, err)
							require.True(t, schema.Equal(record.Schema()))
							rowCount += record.NumRows()
							record.Release()
						}
					}
					require.Zero(t, metadataCalls, "FetchResults metadata avoids another RPC")
					require.Equal(t, 1, fetchCalls)
					if empty {
						require.Zero(t, rowCount)
					} else {
						require.EqualValues(t, 7311, rowCount)
					}
					if schemaShape == "cloud" {
						if empty {
							expectedDownloads = 0
						}
						require.Equal(t, expectedDownloads, downloads.Load(), "schema discovery must reuse the downloaded stream")
					}
				})
			}
		}
	}
}

func assertArrowSchemaTypes(t *testing.T, expected, actual *arrow.Schema) {
	t.Helper()
	require.Equal(t, expected.NumFields(), actual.NumFields())
	for index, field := range expected.Fields() {
		require.Equal(t, field.Name, actual.Field(index).Name)
		require.True(t, arrow.TypeEqual(field.Type, actual.Field(index).Type), "column %s: server=%s fallback=%s", field.Name, field.Type, actual.Field(index).Type)
		require.Equal(t, field.Nullable, actual.Field(index).Nullable, "column %s", field.Name)
	}
}

func TestArrowSchemaMetadataError(t *testing.T) {
	metadataErr := errors.New("metadata unavailable")
	for _, accessor := range []string{"ipc", "batches"} {
		t.Run(accessor, func(t *testing.T) {
			fakeClient := &client.TestClient{
				FnGetResultSetMetadata: func(context.Context, *cli_service.TGetResultSetMetadataReq) (*cli_service.TGetResultSetMetadataResp, error) {
					return nil, metadataErr
				},
				FnFetchResults: func(context.Context, *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
					return &cli_service.TFetchResultsResp{Results: &cli_service.TRowSet{}, HasMoreRows: new(bool)}, nil
				},
				FnCloseOperation: func(context.Context, *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
					return &cli_service.TCloseOperationResp{}, nil
				},
			}
			result, err := NewRows(context.Background(), nil, fakeClient, config.WithDefaults(), nil, nil)
			require.NoError(t, err)
			defer func() { require.NoError(t, result.Close()) }()
			if accessor == "ipc" {
				iterator, err := result.(dbsqlrows.Rows).GetArrowIPCStreams(context.Background())
				require.NoError(t, err)
				defer iterator.Close()
				_, err = iterator.SchemaBytes()
				require.ErrorIs(t, err, metadataErr)
			} else {
				iterator, err := result.(dbsqlrows.Rows).GetArrowBatches(context.Background())
				require.NoError(t, err)
				defer iterator.Close()
				_, err = iterator.Schema()
				require.ErrorIs(t, err, metadataErr)
			}
		})
	}
}

func TestArrowSchemaInitialFetchError(t *testing.T) {
	for _, accessor := range []string{"ipc", "batches"} {
		t.Run(accessor, func(t *testing.T) {
			fetchErr := errors.New("first page unavailable")
			fakeClient := &client.TestClient{
				FnGetResultSetMetadata: func(context.Context, *cli_service.TGetResultSetMetadataReq) (*cli_service.TGetResultSetMetadataResp, error) {
					return &cli_service.TGetResultSetMetadataResp{Schema: &cli_service.TTableSchema{}}, nil
				},
				FnFetchResults: func(context.Context, *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
					return nil, fetchErr
				},
				FnCloseOperation: func(context.Context, *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
					return &cli_service.TCloseOperationResp{}, nil
				},
			}
			result, err := NewRows(context.Background(), nil, fakeClient, config.WithDefaults(), nil, nil)
			require.NoError(t, err)
			defer func() { require.NoError(t, result.Close()) }()
			if accessor == "ipc" {
				iterator, err := result.(dbsqlrows.Rows).GetArrowIPCStreams(context.Background())
				require.NoError(t, err)
				defer iterator.Close()
				_, err = iterator.SchemaBytes()
				require.ErrorIs(t, err, fetchErr)
			} else {
				iterator, err := result.(dbsqlrows.Rows).GetArrowBatches(context.Background())
				require.NoError(t, err)
				defer iterator.Close()
				_, err = iterator.Schema()
				require.ErrorIs(t, err, fetchErr)
			}
		})
	}
}

func BenchmarkArrowSchemaDirectResults(bench *testing.B) {
	data, err := os.ReadFile("testdata/directResultsMultipleFetch/ExecuteStatement.json")
	require.NoError(bench, err)
	var response cli_service.TExecuteStatementResp
	require.NoError(bench, json.Unmarshal(data, &response))
	cfg := config.WithDefaults()
	fakeClient := &client.TestClient{
		FnCloseOperation: func(context.Context, *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
			return &cli_service.TCloseOperationResp{}, nil
		},
	}
	ctx := context.Background()
	bench.ReportAllocs()
	bench.ResetTimer()
	for bench.Loop() {
		result, rowsErr := NewRows(ctx, nil, fakeClient, cfg, response.DirectResults, nil)
		require.NoError(bench, rowsErr)
		iterator, err := result.(dbsqlrows.Rows).GetArrowIPCStreams(ctx)
		require.NoError(bench, err)
		schemaBytes, err := iterator.SchemaBytes()
		require.NoError(bench, err)
		require.NotEmpty(bench, schemaBytes)
		iterator.Close()
		require.NoError(bench, result.Close())
	}
}
