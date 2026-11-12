//go:build cgo && databricks_kernel

package kernel

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/ipc"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/databricks/databricks-sql-go/driverctx"
	"github.com/databricks/databricks-sql-go/internal/backend"
)

func TestExecuteCancelsFileTransfer(t *testing.T) {
	for _, verb := range []string{"PUT", "GET"} {
		t.Run(verb, func(t *testing.T) {
			directory := t.TempDir()
			local := filepath.Join(directory, "file")
			original := bytes.Repeat([]byte("a"), 8*1024*1024)
			if err := os.WriteFile(local, original, 0600); err != nil {
				t.Fatal(err)
			}
			started := make(chan struct{})
			release := make(chan struct{})
			var instruction []byte
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.URL.Path == "/storage":
					if verb == "PUT" {
						if _, err := io.CopyN(io.Discard, r.Body, 1); err != nil {
							return
						}
					} else {
						_, _ = w.Write([]byte("partial"))
						w.(http.Flusher).Flush()
					}
					close(started)
					select {
					case <-r.Context().Done():
					case <-release:
					}
				case r.Method == "POST" && r.URL.Path == "/api/2.0/sql/statements":
					_, _ = w.Write(instruction)
				case strings.HasSuffix(r.URL.Path, "/sessions"):
					_, _ = io.WriteString(w, `{"session_id":"session","warehouse_id":"test"}`)
				default:
					_, _ = io.WriteString(w, `{}`)
				}
			}))
			defer server.Close()
			instruction = fileTransferInstruction(t, verb, server.URL+"/storage", local)
			k := New(Config{Host: server.URL, WarehouseID: "test", Auth: Auth{Mode: AuthPAT, Token: "test"}})
			if err := k.OpenSession(context.Background()); err != nil {
				t.Fatal(err)
			}
			defer k.CloseSession(context.Background())
			raw, err := k.Execute(context.Background(), backend.ExecRequest{Query: "get '/Volumes/c/s/v/file' to '" + local + "'"})
			if err != nil {
				t.Fatalf("raw execution without staging context: %v", err)
			}
			if _, err := raw.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			select {
			case <-started:
				t.Fatal("raw execution started a file transfer")
			default:
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx = driverctx.NewContextWithStagingInfo(ctx, []string{directory})
			finished := make(chan error, 1)
			stopped := make(chan struct{})
			query := "PUT '" + local + "' INTO '/Volumes/c/s/v/file' OVERWRITE"
			if verb == "GET" {
				query = "GET '/Volumes/c/s/v/file' TO '" + local + "'"
			}
			go func() {
				defer close(stopped)
				op, err := k.Execute(ctx, backend.ExecRequest{Query: query, TransferFiles: true})
				if op != nil {
					_, _ = op.Close(context.Background())
				}
				finished <- err
			}()
			defer func() {
				close(release)
				<-stopped
			}()
			select {
			case <-started:
			case err := <-finished:
				t.Fatalf("execute returned before transfer: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("transfer did not start")
			}
			cancel()
			select {
			case err := <-finished:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("execute error = %v, want context.Canceled", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("cancelled execute is still waiting for the transfer")
			}
			if verb == "GET" {
				data, err := os.ReadFile(local)
				if err != nil || !bytes.Equal(data, original) {
					t.Fatalf("cancelled GET changed the destination: %v", err)
				}
			}
		})
	}
}

func fileTransferInstruction(t *testing.T, verb, url, local string) []byte {
	t.Helper()
	names := []string{"operation", "url", "headers", "local"}
	fields := make([]arrow.Field, len(names))
	columns := make([]map[string]any, len(names))
	for i, name := range names {
		fields[i] = arrow.Field{Name: name, Type: arrow.BinaryTypes.String}
		columns[i] = map[string]any{"name": name, "type_name": "STRING", "type_text": "STRING", "position": i}
	}
	schema := arrow.NewSchema(fields, nil)
	builder := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer builder.Release()
	for i, value := range []string{verb, url, "{}", local} {
		builder.Field(i).(*array.StringBuilder).Append(value)
	}
	record := builder.NewRecord()
	defer record.Release()
	var buffer bytes.Buffer
	writer := ipc.NewWriter(&buffer, ipc.WithSchema(schema))
	if err := writer.Write(record); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(map[string]any{
		"statement_id": "transfer",
		"status":       map[string]any{"state": "SUCCEEDED"},
		"manifest": map[string]any{
			"format": "ARROW_STREAM", "is_volume_operation": true,
			"schema":            map[string]any{"column_count": len(names), "columns": columns},
			"total_chunk_count": 1, "total_row_count": 1, "total_byte_count": buffer.Len(), "truncated": false,
		},
		"result": map[string]any{
			"chunk_index": 0, "row_offset": 0, "row_count": 1, "byte_count": buffer.Len(),
			"attachment": base64.StdEncoding.EncodeToString(buffer.Bytes()),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return body
}
