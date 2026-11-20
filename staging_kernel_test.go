//go:build cgo && databricks_kernel

package dbsql

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"encoding/json"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/databricks/databricks-sql-go/driverctx"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/databricks/databricks-sql-go/internal/backend"
	"github.com/databricks/databricks-sql-go/internal/backend/kernel"
	"github.com/stretchr/testify/require"
)

type cancelOnCompletionErrorContext struct {
	context.Context
	cancel   context.CancelFunc
	errCalls int
}

func (c *cancelOnCompletionErrorContext) Err() error {
	c.errCalls++
	// CompleteStaging checks Err before completion and again after a native failure.
	if c.errCalls == 2 {
		c.cancel()
	}
	return c.Context.Err()
}

func TestKernelVolumeCompletionControlsSessionValidity(t *testing.T) {
	for _, tc := range []struct {
		name, operation, local, errorCode                             string
		status                                                        int
		instruction, cancelAfterError, inFlight, success, invalidRoot bool
	}{
		{name: "instruction401", instruction: true, errorCode: "SESSION_EXPIRED"},
		{name: "storage401", status: 401, errorCode: "VOLUME_STORAGE_HTTP_ERROR"},
		{name: "storageBody", errorCode: "VOLUME_STORAGE_NETWORK_ERROR"},
		{name: "instruction401Cancelled", instruction: true, cancelAfterError: true, errorCode: "SESSION_EXPIRED"},
		{name: "getSuccess", success: true},
		{name: "removeNotFound", operation: "REMOVE", status: 404, success: true},
		{name: "inFlightCancel", inFlight: true},
		{name: "streamPut", operation: "PUT", local: "__input_stream__"},
		{name: "invalidUTF8Root", invalidRoot: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			directory := t.TempDir()
			destination := filepath.Join(directory, "download.bin")
			var result map[string]any
			storageStarted := make(chan struct{})
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.Method == "POST" && strings.HasSuffix(r.URL.Path, "/sessions"):
					_, _ = io.WriteString(w, `{"session_id":"volume-session"}`)
				case r.Method == "POST" && r.URL.Path == "/api/2.0/sql/statements":
					_ = json.NewEncoder(w).Encode(result)
				case r.URL.Path == "/api/2.0/sql/statements/volume-stmt/result/chunks/1":
					w.WriteHeader(401)
					_, _ = io.WriteString(w, `{"error_code":"SESSION_EXPIRED","message":"instruction session expired","sql_state":"08003"}`)
				case r.URL.Path == "/object":
					if tc.inFlight {
						close(storageStarted)
						<-r.Context().Done()
					} else if tc.status != 0 {
						w.WriteHeader(tc.status)
					} else if tc.success {
						_, _ = io.WriteString(w, "downloaded")
					} else {
						w.Header().Set("Content-Length", "20")
						_, _ = io.WriteString(w, "short")
					}
				default:
					_, _ = io.WriteString(w, `{}`)
				}
			}))
			defer server.Close()
			names := []string{"operation", "presignedUrl", "headers", "localFile"}
			operation, local := tc.operation, destination
			if operation == "" {
				operation = "GET"
			}
			if tc.local != "" {
				local = tc.local
			}
			values := []string{operation, server.URL + "/object", "{}", local}
			fields, columns := make([]arrow.Field, 4), make([]arrow.Array, 4)
			manifestColumns := make([]map[string]any, 4)
			for i, name := range names {
				fields[i] = arrow.Field{Name: name, Type: arrow.BinaryTypes.String, Nullable: true}
				builder := array.NewStringBuilder(memory.DefaultAllocator)
				builder.Append(values[i])
				columns[i] = builder.NewStringArray()
				builder.Release()
				defer columns[i].Release()
				manifestColumns[i] = map[string]any{"name": name, "type_name": "STRING", "type_text": "STRING", "position": i}
			}
			schema := arrow.NewSchema(fields, nil)
			record := array.NewRecordBatch(schema, columns, 1)
			defer record.Release()
			var attachment bytes.Buffer
			writer := ipc.NewWriter(&attachment, ipc.WithSchema(schema))
			require.NoError(t, writer.Write(record))
			require.NoError(t, writer.Close())
			chunk := map[string]any{"chunk_index": 0, "row_offset": 0, "row_count": 1, "attachment": base64.StdEncoding.EncodeToString(attachment.Bytes())}
			chunks := 1
			if tc.instruction {
				chunk["next_chunk_index"], chunks = 1, 2
			}
			result = map[string]any{
				"statement_id": "volume-stmt", "status": map[string]any{"state": "CLOSED"},
				"manifest": map[string]any{"format": "ARROW_STREAM", "is_volume_operation": true, "schema": map[string]any{"column_count": 4, "columns": manifestColumns}, "total_row_count": 1, "total_chunk_count": chunks},
				"result":   chunk,
			}
			connector, err := NewConnector(WithServerHostname(server.URL), WithHTTPPath("/sql/1.0/warehouses/test"),
				WithAccessToken("test-token"), WithUseKernel(true), WithSkipTLSHostVerify(), WithRetries(-1, time.Millisecond, time.Millisecond))
			require.NoError(t, err)
			db := sql.OpenDB(connector)
			defer db.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			connection, err := db.Conn(ctx)
			require.NoError(t, err)
			defer func() {
				cancel()
				server.CloseClientConnections()
				go connection.Close()
			}()
			var execResult sql.Result
			if tc.cancelAfterError {
				err = connection.Raw(func(raw any) error {
					operation, err := raw.(*conn).backend.Execute(ctx, backend.ExecRequest{Query: "GET instruction"})
					if err != nil {
						return err
					}
					defer operation.Close(ctx)
					cancelContext, cancelError := context.WithCancel(ctx)
					defer cancelError()
					stagingContext := driverctx.NewContextWithStagingInfo(&cancelOnCompletionErrorContext{Context: cancelContext, cancel: cancelError}, []string{directory})
					return operation.(backend.StagingCompleter).CompleteStaging(stagingContext)
				})
				require.ErrorIs(t, err, context.Canceled)
			} else {
				roots := []string{directory}
				if tc.invalidRoot {
					roots = []string{directory + string([]byte{0xff})}
				}
				if tc.inFlight {
					go func() { <-storageStarted; cancel() }()
				}
				type executionResult struct {
					result sql.Result
					err    error
				}
				done := make(chan executionResult, 1)
				go func() {
					result, err := connection.ExecContext(driverctx.NewContextWithStagingInfo(ctx, roots), "Volume instruction")
					done <- executionResult{result, err}
				}()
				select {
				case completed := <-done:
					execResult, err = completed.result, completed.err
				case <-time.After(10 * time.Second):
					cancel()
					t.Fatal("ExecContext did not return")
				}
			}
			if tc.success {
				require.NoError(t, err)
				if tc.operation != "REMOVE" {
					n, _ := execResult.RowsAffected()
					require.Zero(t, n)
					content, readErr := fs.ReadFile(os.DirFS(directory), "download.bin")
					require.NoError(t, readErr)
					require.Equal(t, "downloaded", string(content))
				}
			} else if tc.errorCode == "" {
				require.ErrorIs(t, err, dbsqlerr.DriverError)
				if tc.inFlight {
					require.ErrorIs(t, err, context.Canceled)
				}
			} else {
				require.Error(t, err)
				require.NotErrorIs(t, err, driver.ErrBadConn)
				var detail *kernel.KernelError
				require.ErrorAs(t, err, &detail)
				require.Equal(t, tc.errorCode, detail.ErrorCode)
			}
			require.NoError(t, connection.Raw(func(raw any) error {
				require.Equal(t, !tc.instruction, raw.(driver.Validator).IsValid())
				return nil
			}))
		})
	}
}
