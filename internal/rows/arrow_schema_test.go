package rows

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/databricks/databricks-sql-go/v2/internal/cli_service"
	"github.com/databricks/databricks-sql-go/v2/internal/client"
	"github.com/databricks/databricks-sql-go/v2/internal/config"
	dbsqlerrint "github.com/databricks/databricks-sql-go/v2/internal/errors"
	dbsqlrows "github.com/databricks/databricks-sql-go/v2/rows"
	"github.com/stretchr/testify/require"
)

type schemaTestIterator struct {
	schema  func() (*arrow.Schema, error)
	next    func() (int64, *arrow.Schema, error)
	hasNext func() bool
	close   func()
}

func newSchemaTestIterator(t *testing.T, result dbsqlrows.Rows, accessor string) schemaTestIterator {
	t.Helper()
	if accessor == "batches" {
		iterator, err := result.GetArrowBatches(context.Background())
		require.NoError(t, err)
		t.Cleanup(iterator.Close)
		return schemaTestIterator{iterator.Schema, func() (int64, *arrow.Schema, error) {
			record, err := iterator.Next()
			if err != nil {
				return 0, nil, err
			}
			defer record.Release()
			return record.NumRows(), record.Schema(), nil
		}, iterator.HasNext, iterator.Close}
	}
	iterator, err := result.GetArrowIPCStreams(context.Background())
	require.NoError(t, err)
	t.Cleanup(iterator.Close)
	return schemaTestIterator{func() (*arrow.Schema, error) {
		serialized, err := iterator.SchemaBytes()
		if err != nil || len(serialized) == 0 {
			return nil, err
		}
		reader, err := ipc.NewReader(bytes.NewReader(serialized))
		if err != nil {
			return nil, err
		}
		defer reader.Release()
		return reader.Schema(), nil
	}, func() (int64, *arrow.Schema, error) {
		stream, err := iterator.Next()
		if err != nil {
			return 0, nil, err
		}
		reader, err := ipc.NewReader(stream)
		if err != nil {
			return 0, nil, err
		}
		defer reader.Release()
		var count int64
		for reader.Next() {
			count += reader.RecordBatch().NumRows()
		}
		return count, reader.Schema(), reader.Err()
	}, iterator.HasNext, iterator.Close}
}

func newSchemaTestRows(t *testing.T, cli *client.TestClient, direct *cli_service.TSparkDirectResults, callbacks *TelemetryCallbacks) *rows {
	t.Helper()
	result, err := NewRows(context.Background(), &cli_service.TOperationHandle{OperationId: &cli_service.THandleIdentifier{GUID: make([]byte, 16)}}, cli, config.WithDefaults(), direct, callbacks)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, result.Close()) })
	return result.(*rows)
}

func schemaTestPage(t *testing.T, count int64, timestamp ...bool) (*cli_service.TFetchResultsResp, []byte, *arrow.Schema) {
	t.Helper()
	fieldType := arrow.PrimitiveTypes.Int64
	thriftType := cli_service.TTypeId_BIGINT_TYPE
	if len(timestamp) > 0 && timestamp[0] {
		fieldType = &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "Etc/UTC"}
		thriftType = cli_service.TTypeId_TIMESTAMP_TYPE
	}
	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: fieldType}}, nil)
	builder := array.NewBuilder(memory.DefaultAllocator, fieldType)
	defer builder.Release()
	for range count {
		builder.AppendEmptyValue()
	}
	values := builder.NewArray()
	defer values.Release()
	record := array.NewRecordBatch(schema, []arrow.Array{values}, count)
	defer record.Release()
	var payload bytes.Buffer
	writer := ipc.NewWriter(&payload, ipc.WithSchema(schema))
	require.NoError(t, writer.Write(record))
	require.NoError(t, writer.Close())
	stream := bytes.NewReader(payload.Bytes())
	messages := ipc.NewMessageReader(stream)
	_, err := messages.Message()
	require.NoError(t, err)
	messages.Release()
	schemaEnd := payload.Len() - stream.Len()
	format := cli_service.TSparkRowSetType_ARROW_BASED_SET
	return &cli_service.TFetchResultsResp{
		ResultSetMetadata: &cli_service.TGetResultSetMetadataResp{
			ArrowSchema: payload.Bytes()[:schemaEnd], ResultFormat: &format,
			Schema: &cli_service.TTableSchema{Columns: []*cli_service.TColumnDesc{decimalMetadataColumn(thriftType, nil, nil)}},
		},
		Results:     &cli_service.TRowSet{ArrowBatches: []*cli_service.TSparkArrowBatch{{Batch: payload.Bytes()[schemaEnd:], RowCount: count}}},
		HasMoreRows: new(bool),
	}, payload.Bytes(), schema
}

func TestArrowSchemaBeforeFetching(t *testing.T) {
	for _, accessor := range []string{"ipc", "batches"} {
		for _, testSource := range []string{"rpc", "page", "direct", "status", "columns", "rpc-next", "page-next"} {
			source := strings.TrimSuffix(testSource, "-next")
			nextFirst := source != testSource
			shapes := []string{"server", "nil", "empty"}
			switch source {
			case "direct":
				shapes = []string{"server", "nil"}
			case "status", "columns":
				shapes = []string{"server"}
			}
			for _, shape := range shapes {
				for _, cloud := range []bool{false, true} {
					for _, count := range []int64{0, 1} {
						if nextFirst && (cloud || count == 0 || shape == "server") {
							continue
						}
						t.Run(fmt.Sprintf("%s/%s/%s/cloud=%t/rows=%d", accessor, testSource, shape, cloud, count), func(t *testing.T) {
							inferredCloud := cloud && shape != "server" && source != "direct"
							page, payload, expected := schemaTestPage(t, count, inferredCloud)
							metadataSchema := expected
							if inferredCloud {
								fields := expected.Fields()
								fields[0].Type = &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
								metadataSchema = arrow.NewSchema(fields, nil)
							}
							page.ResultSetMetadata.Schema.Columns[0].ColumnName = "value"
							var downloads atomic.Int32
							server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
								downloads.Add(1)
								_, _ = response.Write(payload)
							}))
							defer server.Close()
							if cloud && count > 0 {
								*page.ResultSetMetadata.ResultFormat = cli_service.TSparkRowSetType_URL_BASED_SET
								page.Results = &cli_service.TRowSet{}
								if count > 0 {
									page.Results.ResultLinks = []*cli_service.TSparkArrowResultLink{{FileLink: server.URL, RowCount: count, ExpiryTime: time.Now().Add(time.Hour).Unix()}}
								}
							}
							switch shape {
							case "nil":
								page.ResultSetMetadata.ArrowSchema = nil
							case "empty":
								page.ResultSetMetadata.ArrowSchema = []byte{}
							}
							last, _, _ := schemaTestPage(t, 0)
							last.Results.StartRowOffset = count
							metadataCalls, fetches := 0, 0
							cli := &client.TestClient{
								FnGetResultSetMetadata: func(ctx context.Context, request *cli_service.TGetResultSetMetadataReq) (*cli_service.TGetResultSetMetadataResp, error) {
									metadataCalls++
									require.NotNil(t, request.OperationHandle)
									require.NoError(t, ctx.Err())
									return page.ResultSetMetadata, nil
								},
								FnFetchResults: func(context.Context, *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
									fetches++
									if source == "direct" {
										return last, nil
									}
									return page, nil
								},
								FnCloseOperation: func(context.Context, *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
									return &cli_service.TCloseOperationResp{}, nil
								},
							}
							var direct *cli_service.TSparkDirectResults
							switch source {
							case "direct":
								page.HasMoreRows = new(true)
								direct = &cli_service.TSparkDirectResults{ResultSetMetadata: page.ResultSetMetadata, ResultSet: page}
							case "status":
								direct = &cli_service.TSparkDirectResults{}
							}
							result := newSchemaTestRows(t, cli, direct, nil)
							if source == "columns" {
								require.Equal(t, []string{"value"}, result.Columns())
							}
							if source == "page" {
								result.ResultPageIterator.HasNext()
							}
							iterator := newSchemaTestIterator(t, result, accessor)
							var total int64
							if nextFirst {
								fetched, actual, err := iterator.next()
								require.NoError(t, err)
								require.True(t, expected.Equal(actual))
								total += fetched
							}
							before := fetches
							for range 2 {
								actual, err := iterator.schema()
								require.NoError(t, err)
								require.True(t, metadataSchema.Equal(actual), "expected=%s actual=%s", metadataSchema, actual)
							}
							require.Equal(t, before, fetches)
							require.Zero(t, downloads.Load())
							wantCalls := 0
							if (source == "rpc" && !nextFirst) || source == "status" || source == "columns" {
								wantCalls = 1
							}
							require.Equal(t, wantCalls, metadataCalls)
							for {
								fetched, actual, err := iterator.next()
								if err == io.EOF {
									break
								}
								require.NoError(t, err)
								require.True(t, expected.Equal(actual))
								total += fetched
							}
							require.Equal(t, count, total)
							_, err := iterator.schema()
							require.NoError(t, err)
							require.Equal(t, wantCalls, metadataCalls)
						})
					}
				}
			}
		}
	}
}

func TestArrowCloudFetchDirectThriftSchemaAcrossPages(t *testing.T) {
	page, payload, _ := schemaTestPage(t, 1)
	page.ResultSetMetadata.ArrowSchema = nil
	page.ResultSetMetadata.Schema.Columns[0].ColumnName = "renamed"
	*page.ResultSetMetadata.ResultFormat = cli_service.TSparkRowSetType_URL_BASED_SET
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		_, _ = response.Write(payload)
	}))
	defer server.Close()
	page.HasMoreRows = new(true)
	page.Results = &cli_service.TRowSet{ResultLinks: []*cli_service.TSparkArrowResultLink{{
		FileLink: server.URL, RowCount: 1, ExpiryTime: time.Now().Add(time.Hour).Unix(),
	}}}
	last := &cli_service.TFetchResultsResp{
		ResultSetMetadata: page.ResultSetMetadata,
		Results: &cli_service.TRowSet{StartRowOffset: 1, ResultLinks: []*cli_service.TSparkArrowResultLink{{
			FileLink: server.URL, StartRowOffset: 1, RowCount: 1, ExpiryTime: time.Now().Add(time.Hour).Unix(),
		}}},
		HasMoreRows: new(false),
	}
	cli := &client.TestClient{
		FnFetchResults: func(context.Context, *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
			return last, nil
		},
		FnCloseOperation: func(context.Context, *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
			return &cli_service.TCloseOperationResp{}, nil
		},
	}
	result := newSchemaTestRows(t, cli, &cli_service.TSparkDirectResults{ResultSetMetadata: page.ResultSetMetadata, ResultSet: page}, nil)
	iterator := newSchemaTestIterator(t, result, "batches")
	for range 2 {
		count, schema, err := iterator.next()
		require.NoError(t, err)
		require.EqualValues(t, 1, count)
		require.Equal(t, "renamed", schema.Field(0).Name)
	}
	_, _, err := iterator.next()
	require.ErrorIs(t, err, io.EOF)
}

func TestArrowSchemaErrors(t *testing.T) {
	for _, accessor := range []string{"ipc", "batches"} {
		for _, scenario := range []string{"request", "canceled", "conversion-empty", "conversion-rows", "fetch", "column-format"} {
			t.Run(accessor+"/"+scenario, func(t *testing.T) {
				page, _, _ := schemaTestPage(t, 1)
				cause := errors.New("request failed")
				if scenario == "column-format" {
					*page.ResultSetMetadata.ResultFormat = cli_service.TSparkRowSetType_COLUMN_BASED_SET
					page.ResultSetMetadata.ArrowSchema = nil
				}
				conversion := scenario == "conversion-empty" || scenario == "conversion-rows"
				if conversion {
					page.ResultSetMetadata.ArrowSchema = nil
					page.ResultSetMetadata.Schema.Columns[0] = decimalMetadataColumn(cli_service.TTypeId_ARRAY_TYPE, nil, nil)
					if scenario == "conversion-empty" {
						page.Results.ArrowBatches = nil
					}
				}
				closes := 0
				cli := &client.TestClient{
					FnGetResultSetMetadata: func(ctx context.Context, _ *cli_service.TGetResultSetMetadataReq) (*cli_service.TGetResultSetMetadataResp, error) {
						require.Equal(t, scenario == "canceled", ctx.Err() != nil)
						if scenario == "request" || scenario == "canceled" {
							return nil, cause
						}
						return page.ResultSetMetadata, nil
					},
					FnFetchResults: func(context.Context, *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
						if scenario == "fetch" {
							return nil, cause
						}
						return page, nil
					},
					FnCloseOperation: func(context.Context, *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
						closes++
						return &cli_service.TCloseOperationResp{}, nil
					},
				}
				result := newSchemaTestRows(t, cli, nil, nil)
				iterator := newSchemaTestIterator(t, result, accessor)
				if scenario == "canceled" {
					result.resultsCancel()
				}
				_, err := iterator.schema()
				require.Zero(t, closes)
				switch {
				case scenario == "column-format":
					require.ErrorContains(t, err, "result set is not in arrow format")
				case conversion:
					require.ErrorContains(t, err, "ARRAY require arrow schema")
					_, _, err = iterator.next()
					if scenario == "conversion-empty" {
						require.ErrorIs(t, err, io.EOF)
					} else {
						require.ErrorContains(t, err, "ARRAY require arrow schema")
					}
				case scenario == "fetch":
					require.NoError(t, err)
					_, _, err = iterator.next()
					require.ErrorIs(t, err, cause)
					_, _, err = iterator.next()
					require.ErrorIs(t, err, io.EOF)
				default:
					require.ErrorIs(t, err, cause)
					require.ErrorContains(t, err, errRowsMetadataFetchFailed)
					category := dbsqlerrint.CategoryResultSet
					if scenario == "canceled" {
						category = ""
					}
					require.Equal(t, category, dbsqlerrint.CategoryFromError(err))
				}
				iterator.close()
				require.NoError(t, result.Close())
				require.Equal(t, 1, closes)
			})
		}
	}
}

func TestArrowSchemaErrorPreservesPendingFetchError(t *testing.T) {
	for _, accessor := range []string{"ipc", "batches"} {
		t.Run(accessor, func(t *testing.T) {
			page, _, _ := schemaTestPage(t, 1)
			page.ResultSetMetadata.ArrowSchema = nil
			page.ResultSetMetadata.Schema.Columns[0] = decimalMetadataColumn(cli_service.TTypeId_ARRAY_TYPE, nil, nil)
			page.HasMoreRows = new(true)
			fetches := 0
			cause := errors.New("pending page failure")
			cli := &client.TestClient{
				FnGetResultSetMetadata: func(context.Context, *cli_service.TGetResultSetMetadataReq) (*cli_service.TGetResultSetMetadataResp, error) {
					t.Fatal("cached page metadata must be reused")
					return nil, nil
				},
				FnFetchResults: func(context.Context, *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
					fetches++
					if fetches == 1 {
						return page, nil
					}
					return nil, cause
				},
			}
			result := newSchemaTestRows(t, cli, nil, nil)
			_, err := result.ResultPageIterator.Next()
			require.NoError(t, err)
			iterator := newSchemaTestIterator(t, result, accessor)
			require.True(t, iterator.hasNext())
			_, err = iterator.schema()
			require.ErrorContains(t, err, "ARRAY require arrow schema")
			_, _, err = iterator.next()
			require.ErrorIs(t, err, cause)
			_, _, err = iterator.next()
			require.ErrorIs(t, err, io.EOF)
		})
	}
}

func TestArrowSchemaMetadataStaysLocal(t *testing.T) {
	page, _, _ := schemaTestPage(t, 0)
	started, release, finished := make(chan struct{}), make(chan struct{}, 1), make(chan struct{})
	cli := &client.TestClient{FnGetResultSetMetadata: func(context.Context, *cli_service.TGetResultSetMetadataReq) (*cli_service.TGetResultSetMetadataResp, error) {
		close(started)
		select {
		case <-release:
			return page.ResultSetMetadata, nil
		case <-time.After(5 * time.Second):
			return nil, context.DeadlineExceeded
		}
	}, FnCloseOperation: func(context.Context, *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
		return &cli_service.TCloseOperationResp{}, nil
	}}
	result := newSchemaTestRows(t, cli, nil, &TelemetryCallbacks{OnResultFormat: func(string) {}})
	iterator := newSchemaTestIterator(t, result, "ipc")
	var schemaErr error
	go func() {
		_, schemaErr = iterator.schema()
		close(finished)
	}()
	t.Cleanup(func() {
		close(release)
		select {
		case <-finished:
		case <-time.After(5 * time.Second):
			t.Error("schema goroutine did not stop")
		}
	})
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("metadata RPC did not start")
	}
	require.NoError(t, result.Close())
	release <- struct{}{}
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		require.Empty(t, result.ResultFormat())
		select {
		case <-finished:
			require.NoError(t, schemaErr)
			require.Nil(t, result.resultSetMetadata)
			require.Nil(t, result.schema)
			return
		case <-deadline.C:
			t.Fatal("schema load did not finish")
		default:
		}
	}
}

func TestArrowCloseBeforeFirstUse(t *testing.T) {
	for _, accessor := range []string{"ipc", "batches"} {
		for _, direct := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/direct=%t", accessor, direct), func(t *testing.T) {
				calls, closes := 0, 0
				cli := &client.TestClient{
					FnGetResultSetMetadata: func(context.Context, *cli_service.TGetResultSetMetadataReq) (*cli_service.TGetResultSetMetadataResp, error) {
						calls++
						return nil, errors.New("RPC after Close")
					},
					FnFetchResults: func(context.Context, *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
						calls++
						return nil, errors.New("fetch after Close")
					},
					FnCloseOperation: func(context.Context, *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
						closes++
						return &cli_service.TCloseOperationResp{}, nil
					},
				}
				var initial *cli_service.TSparkDirectResults
				if direct {
					page, _, _ := schemaTestPage(t, 1)
					initial = &cli_service.TSparkDirectResults{ResultSetMetadata: page.ResultSetMetadata, ResultSet: page}
				}
				result := newSchemaTestRows(t, cli, initial, nil)
				iterator := newSchemaTestIterator(t, result, accessor)
				iterator.close()
				iterator.close()
				_, schemaErr := iterator.schema()
				if !direct && accessor == "batches" {
					require.ErrorContains(t, schemaErr, "no schema available")
				} else {
					require.NoError(t, schemaErr)
				}
				require.False(t, iterator.hasNext())
				_, _, err := iterator.next()
				require.ErrorIs(t, err, io.EOF)
				require.NoError(t, result.Close())
				require.Zero(t, calls)
				require.Equal(t, 1, closes)
			})
		}
	}
}
