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

func batchTestIPC(t testing.TB, batches int) []byte {
	t.Helper()
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
		require.NoError(t, writer.Write(record))
	}
	require.NoError(t, writer.Close())
	return buffer.Bytes()
}

func openBatchTestRows(t testing.TB, payload []byte, nextChunk http.HandlerFunc, callbacks *dbsqlrows.TelemetryCallbacks) *kernelRows {
	t.Helper()
	reader, err := ipc.NewReader(bytes.NewReader(payload))
	require.NoError(t, err)
	rowCount := int64(0)
	for reader.Next() {
		rowCount += reader.RecordBatch().NumRows()
	}
	require.NoError(t, reader.Err())
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
	t.Cleanup(server.Close)
	enabled := false
	kernelBackend := New(Config{
		Host: server.URL, HTTPPath: "/sql/1.0/warehouses/batches", Auth: Auth{Mode: AuthPAT, Token: "batch-test-token"},
		TLSSkipVerify: true, Retry: &RetryConfig{MinWait: time.Millisecond, MaxWait: time.Millisecond}, RequestTimeout: 5 * time.Second,
		Telemetry: &TelemetryConfig{Enabled: &enabled},
	})
	require.NoError(t, kernelBackend.OpenSession(context.Background()))
	t.Cleanup(func() { require.NoError(t, kernelBackend.CloseSession(context.Background())) })
	operation, err := kernelBackend.Execute(context.Background(), backend.ExecRequest{Query: "SELECT boolean, float, double FROM batches"})
	require.NoError(t, err)
	t.Cleanup(func() { _, closeErr := operation.Close(context.Background()); require.NoError(t, closeErr) })
	rows, err := operation.Results(context.Background(), callbacks)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rows.Close()) })
	return rows.(*kernelRows)
}

func TestKernelRowsBufferedBatches(t *testing.T) {
	var chunks []int
	var closedChunks int
	rows := openBatchTestRows(t, batchTestIPC(t, 3), nil, &dbsqlrows.TelemetryCallbacks{
		OnChunkFetched: func(count int, _ int64, _ int, _ int64, _ int32) { chunks = append(chunks, count) },
		OnClose: func(_ int64, count int, iterationErr, closeErr error) {
			closedChunks = count
			require.NoError(t, iterationErr)
			require.NoError(t, closeErr)
		},
	})
	want := [][]driver.Value{{true, float32(1.25), math.Inf(1)}, {false, float32(-2.5), math.SmallestNonzeroFloat64}, {nil, nil, nil}}
	values := make([]driver.Value, 3)
	for batch := 0; batch < 3; batch++ {
		for _, expected := range want {
			require.NoError(t, rows.Next(values))
			require.Equal(t, expected, values)
		}
	}
	require.ErrorIs(t, rows.Next(values), io.EOF)
	require.True(t, rows.eof)
	require.ErrorIs(t, rows.Next(values), io.EOF)
	require.Equal(t, []int{1, 2, 3}, chunks)
	require.NoError(t, rows.Close())
	require.NoError(t, rows.Close())
	require.Equal(t, 3, closedChunks)
}

func TestKernelRowsPendingBatch(t *testing.T) {
	for _, scenario := range []struct {
		name   string
		fail   bool
		cancel bool
	}{{name: "batch"}, {name: "error", fail: true}, {name: "cancel", cancel: true}} {
		t.Run(scenario.name, func(t *testing.T) {
			payload := batchTestIPC(t, 1)
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
						t.Error("fetch goroutine did not exit")
					}
				}
			}()
			rows := openBatchTestRows(t, payload, func(writer http.ResponseWriter, request *http.Request) {
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
				require.NoError(t, rows.Next(make([]driver.Value, 3)))
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
				t.Fatal("pending fetch never requested the next chunk")
			}
			select {
			case err := <-finished:
				t.Fatalf("pending fetch returned before provider progress: %v", err)
			case <-time.After(20 * time.Millisecond):
			}
			if scenario.cancel {
				rows.cancel()
			}
			release <- struct{}{}
			select {
			case err := <-finished:
				if scenario.fail {
					require.ErrorContains(t, err, "kernel: next_batch:")
					require.ErrorContains(t, err, "Invalid SessionHandle")
					require.NotErrorIs(t, err, driver.ErrBadConn)
					require.False(t, rows.op.backend.SessionValid())
				} else {
					require.NoError(t, err)
					require.EqualValues(t, 3, rows.cur.NumRows())
				}
			case <-time.After(5 * time.Second):
				t.Fatal("blocking fallback did not finish")
			}
			if scenario.cancel {
				require.ErrorIs(t, rows.nextBatch(), context.Canceled)
			}
		})
	}
}

func TestKernelRowsTryNextError(t *testing.T) {
	rows := &kernelRows{op: &kernelOp{backend: &KernelBackend{}}}
	err := rows.nextBatch()
	require.ErrorContains(t, err, "kernel: next_batch:")
	require.ErrorContains(t, err, "try_next_batch: pointers must not be null")
	require.NotErrorIs(t, err, driver.ErrBadConn)
	require.False(t, rows.eof)
}

func TestKernelRowsCancelBeforeBufferedBatch(t *testing.T) {
	rows := openBatchTestRows(t, batchTestIPC(t, 3), nil, nil)
	require.NoError(t, rows.nextBatch())
	current := rows.cur
	rows.cancel()
	require.ErrorIs(t, rows.nextBatch(), context.Canceled)
	require.Same(t, current, rows.cur)
	require.Equal(t, 1, rows.chunkCount)
	require.NoError(t, rows.Close())
	require.Nil(t, rows.cur)
	require.ErrorIs(t, rows.Next(make([]driver.Value, 3)), io.EOF)
}

func BenchmarkKernelRowsBuffered(b *testing.B) {
	const batches = 4096
	payload := batchTestIPC(b, batches)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		b.StopTimer()
		rows := openBatchTestRows(b, payload, nil, nil)
		b.StartTimer()
		for batch := 0; batch < batches; batch++ {
			require.NoError(b, rows.nextBatch())
		}
		require.ErrorIs(b, rows.nextBatch(), io.EOF)
		b.StopTimer()
		require.NoError(b, rows.Close())
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*batches), "ns/batch")
}
