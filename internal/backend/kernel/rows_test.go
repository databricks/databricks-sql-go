//go:build cgo && databricks_kernel

package kernel

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/base64"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/databricks/databricks-sql-go/v2/internal/backend"
	dbsqlrows "github.com/databricks/databricks-sql-go/v2/internal/rows"
	"github.com/stretchr/testify/require"
)

func batchTestIPC(test testing.TB, batches int) []byte {
	test.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "boolean", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "float", Type: arrow.PrimitiveTypes.Float32, Nullable: true},
		{Name: "double", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)
	builder := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer builder.Release()
	builder.Field(0).(*array.BooleanBuilder).AppendValues([]bool{true, false, false}, []bool{true, true, false})
	builder.Field(1).(*array.Float32Builder).AppendValues([]float32{1.25, -2.5, 0}, []bool{true, true, false})
	builder.Field(2).(*array.Float64Builder).AppendValues([]float64{math.Inf(1), math.SmallestNonzeroFloat64, 0}, []bool{true, true, false})
	record := builder.NewRecordBatch()
	defer record.Release()
	var buffer bytes.Buffer
	writer := ipc.NewWriter(&buffer, ipc.WithSchema(schema))
	for batch := 0; batch < batches; batch++ {
		require.NoError(test, writer.Write(record))
	}
	require.NoError(test, writer.Close())
	return buffer.Bytes()
}

func openBatchTestRows(test testing.TB, payload []byte, nextChunk http.HandlerFunc, callbacks *dbsqlrows.TelemetryCallbacks) *kernelRows {
	test.Helper()
	reader, err := ipc.NewReader(bytes.NewReader(payload))
	require.NoError(test, err)
	rowCount := int64(0)
	for reader.Next() {
		rowCount += reader.RecordBatch().NumRows()
	}
	require.NoError(test, reader.Err())
	reader.Release()
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		time.Sleep(time.Millisecond)
		writer.Header().Set("Content-Type", "application/json")
		switch {
		case request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/sessions"):
			_, _ = io.WriteString(writer, `{"session_id":"batch-session"}`)
		case request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/statements"):
			chunks := 1
			result := map[string]any{"chunk_index": 0, "row_offset": 0, "row_count": rowCount, "attachment": base64.StdEncoding.EncodeToString(payload)}
			if nextChunk != nil {
				chunks = 2
				result["next_chunk_index"] = 1
			}
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"statement_id": "batch-statement", "status": map[string]any{"state": "SUCCEEDED"},
				"manifest": map[string]any{
					"format": "ARROW_STREAM", "total_chunk_count": chunks, "total_row_count": rowCount * int64(chunks),
					"schema": map[string]any{"column_count": 3, "columns": []map[string]any{
						{"name": "boolean", "type_name": "BOOLEAN", "type_text": "BOOLEAN", "position": 0},
						{"name": "float", "type_name": "FLOAT", "type_text": "FLOAT", "position": 1},
						{"name": "double", "type_name": "DOUBLE", "type_text": "DOUBLE", "position": 2},
					}},
				}, "result": result,
			})
		case request.Method == http.MethodGet && strings.HasSuffix(request.URL.Path, "/result/chunks/1") && nextChunk != nil:
			nextChunk(writer, request)
		default:
			_, _ = io.WriteString(writer, `{}`)
		}
	}))
	test.Cleanup(server.Close)
	enabled := false
	kernelBackend := New(Config{
		Host: server.URL, HTTPPath: "/sql/1.0/warehouses/batches", Auth: Auth{Mode: AuthPAT, Token: "batch-test-token"},
		TLSSkipVerify: true, Retry: &RetryConfig{MinWait: time.Millisecond, MaxWait: time.Millisecond}, RequestTimeout: 5 * time.Second,
		Telemetry: &TelemetryConfig{Enabled: &enabled},
	})
	require.NoError(test, kernelBackend.OpenSession(context.Background()))
	test.Cleanup(func() { require.NoError(test, kernelBackend.CloseSession(context.Background())) })
	operation, err := kernelBackend.Execute(context.Background(), backend.ExecRequest{Query: "SELECT boolean, float, double FROM batches"})
	require.NoError(test, err)
	test.Cleanup(func() { _, closeErr := operation.Close(context.Background()); require.NoError(test, closeErr) })
	rows, err := operation.Results(context.Background(), callbacks)
	require.NoError(test, err)
	test.Cleanup(func() { require.NoError(test, rows.Close()) })
	return rows.(*kernelRows)
}

func TestKernelRowsBufferedBatches(test *testing.T) {
	var chunks []int
	var closedChunks int
	rows := openBatchTestRows(test, batchTestIPC(test, 3), nil, &dbsqlrows.TelemetryCallbacks{
		OnChunkFetched: func(count int, _ int64, _ int, _ int64, _ int32) { chunks = append(chunks, count) },
		OnClose: func(_ int64, count int, iterationErr, closeErr error) {
			closedChunks = count
			require.NoError(test, iterationErr)
			require.NoError(test, closeErr)
		},
	})
	want := [][]driver.Value{{true, float32(1.25), math.Inf(1)}, {false, float32(-2.5), math.SmallestNonzeroFloat64}, {nil, nil, nil}}
	values := make([]driver.Value, 3)
	for batch := 0; batch < 3; batch++ {
		for _, expected := range want {
			require.NoError(test, rows.Next(values))
			require.Equal(test, expected, values)
		}
	}
	require.ErrorIs(test, rows.Next(values), io.EOF)
	require.True(test, rows.eof)
	require.Nil(test, rows.cur)
	require.ErrorIs(test, rows.Next(values), io.EOF)
	require.Equal(test, []int{1, 2, 3}, chunks)
	require.NoError(test, rows.Close())
	require.NoError(test, rows.Close())
	require.Equal(test, 3, closedChunks)
}

func TestKernelRowsPendingBatch(test *testing.T) {
	for _, scenario := range []struct {
		name   string
		fail   bool
		cancel bool
	}{{name: "batch"}, {name: "error", fail: true}, {name: "cancel", cancel: true}} {
		test.Run(scenario.name, func(test *testing.T) {
			payload := batchTestIPC(test, 1)
			entered := make(chan struct{})
			release := make(chan struct{})
			done := make(chan struct{})
			started := false
			defer func() {
				close(release)
				if started {
					select {
					case <-done:
					case <-time.After(6 * time.Second):
						test.Error("fetch goroutine did not exit")
					}
				}
			}()
			rows := openBatchTestRows(test, payload, func(writer http.ResponseWriter, request *http.Request) {
				close(entered)
				select {
				case <-release:
				case <-request.Context().Done():
					return
				}
				if scenario.fail {
					writer.WriteHeader(http.StatusBadRequest)
					_, _ = io.WriteString(writer, `{"error_code":"BAD_REQUEST","message":"Invalid SessionHandle: batch-session"}`)
					return
				}
				_ = json.NewEncoder(writer).Encode(map[string]any{"chunk_index": 1, "row_offset": 3, "row_count": 3, "attachment": base64.StdEncoding.EncodeToString(payload)})
			}, nil)
			for row := 0; row < 3; row++ {
				require.NoError(test, rows.Next(make([]driver.Value, 3)))
			}
			finished := make(chan error, 1)
			started = true
			go func() {
				defer close(done)
				finished <- rows.nextBatch()
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				test.Fatal("pending fetch never requested the next chunk")
			}
			select {
			case err := <-finished:
				test.Fatalf("pending fetch returned before provider progress: %v", err)
			case <-time.After(20 * time.Millisecond):
			}
			if scenario.cancel {
				rows.cancel()
			}
			release <- struct{}{}
			select {
			case err := <-finished:
				if scenario.fail {
					require.ErrorContains(test, err, "kernel: next_batch:")
					require.ErrorContains(test, err, "Invalid SessionHandle")
					require.NotErrorIs(test, err, driver.ErrBadConn)
					require.False(test, rows.op.backend.SessionValid())
				} else {
					require.NoError(test, err)
					require.EqualValues(test, 3, rows.cur.NumRows())
				}
			case <-time.After(5 * time.Second):
				test.Fatal("blocking fallback did not finish")
			}
			if scenario.cancel {
				require.ErrorIs(test, rows.nextBatch(), context.Canceled)
			}
		})
	}
}

func TestKernelRowsTryNextError(test *testing.T) {
	rows := &kernelRows{op: &kernelOp{backend: &KernelBackend{}}}
	err := rows.nextBatch()
	require.ErrorContains(test, err, "kernel: next_batch:")
	require.ErrorContains(test, err, "try_next_batch: pointers must not be null")
	require.NotErrorIs(test, err, driver.ErrBadConn)
	require.False(test, rows.eof)
}

func TestKernelRowsCancelBeforeBufferedBatch(test *testing.T) {
	rows := openBatchTestRows(test, batchTestIPC(test, 3), nil, nil)
	require.NoError(test, rows.nextBatch())
	current := rows.cur
	rows.cancel()
	require.ErrorIs(test, rows.nextBatch(), context.Canceled)
	require.Same(test, current, rows.cur)
	require.Equal(test, 1, rows.chunkCount)
	require.NoError(test, rows.Close())
	require.Nil(test, rows.cur)
	require.ErrorIs(test, rows.Next(make([]driver.Value, 3)), io.EOF)
}

func BenchmarkKernelRowsBuffered(benchmark *testing.B) {
	const batches = 4096
	payload := batchTestIPC(benchmark, batches)
	benchmark.ReportAllocs()
	benchmark.ResetTimer()
	for iteration := 0; iteration < benchmark.N; iteration++ {
		benchmark.StopTimer()
		rows := openBatchTestRows(benchmark, payload, nil, nil)
		benchmark.StartTimer()
		for batch := 0; batch < batches; batch++ {
			require.NoError(benchmark, rows.nextBatch())
		}
		require.ErrorIs(benchmark, rows.nextBatch(), io.EOF)
		benchmark.StopTimer()
		require.NoError(benchmark, rows.Close())
	}
	benchmark.ReportMetric(float64(benchmark.Elapsed().Nanoseconds())/float64(benchmark.N*batches), "ns/batch")
}
