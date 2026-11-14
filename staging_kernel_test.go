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
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/databricks/databricks-sql-go/driverctx"
	"github.com/databricks/databricks-sql-go/internal/backend/kernel"
	"github.com/stretchr/testify/require"
)

func TestKernelVolumeFailureOriginControlsSessionValidity(t *testing.T) {
	for _, mode := range []string{"instruction401", "storage401", "storageBody"} {
		t.Run(mode, func(t *testing.T) {
			directory := t.TempDir()
			destination := filepath.Join(directory, "download.bin")
			require.NoError(t, os.WriteFile(destination, []byte("old bytes"), 0600))
			var executions, storage atomic.Int64
			var result map[string]any
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.Method == "POST" && strings.HasSuffix(r.URL.Path, "/sessions"):
					_, _ = io.WriteString(w, `{"session_id":"volume-session"}`)
				case r.Method == "POST" && r.URL.Path == "/api/2.0/sql/statements":
					executions.Add(1)
					_ = json.NewEncoder(w).Encode(result)
				case r.URL.Path == "/api/2.0/sql/statements/volume-stmt/result/chunks/1":
					w.WriteHeader(401)
					_, _ = io.WriteString(w, `{"error_code":"SESSION_EXPIRED","message":"instruction session expired","sql_state":"08003"}`)
				case r.URL.Path == "/object":
					storage.Add(1)
					if mode == "storage401" {
						w.WriteHeader(401)
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
			values := []string{"GET", server.URL + "/object", "{}", destination}
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
			if mode == "instruction401" {
				chunk["next_chunk_index"], chunks = 1, 2
			}
			result = map[string]any{
				"statement_id": "volume-stmt", "status": map[string]any{"state": "SUCCEEDED"},
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
			defer connection.Close()
			_, err = connection.ExecContext(driverctx.NewContextWithStagingInfo(ctx, []string{directory}), "GET instruction")
			require.Error(t, err)
			require.NotErrorIs(t, err, driver.ErrBadConn)
			var detail *kernel.KernelError
			require.ErrorAs(t, err, &detail)
			wantCode := map[string]string{"instruction401": "SESSION_EXPIRED", "storage401": "VOLUME_STORAGE_HTTP_ERROR", "storageBody": "VOLUME_STORAGE_NETWORK_ERROR"}[mode]
			require.Equal(t, wantCode, detail.ErrorCode)
			require.NoError(t, connection.Raw(func(raw any) error {
				require.Equal(t, mode != "instruction401", raw.(driver.Validator).IsValid())
				return nil
			}))
			data, err := os.ReadFile(destination)
			require.NoError(t, err)
			require.Equal(t, "old bytes", string(data))
			require.Equal(t, int64(1), executions.Load())
			wantStorage := int64(1)
			if mode == "instruction401" {
				wantStorage = 0
			}
			require.Equal(t, wantStorage, storage.Load())
		})
	}
}
