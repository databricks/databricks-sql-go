package dbsql

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	thriftbackend "github.com/databricks/databricks-sql-go/internal/backend/thrift"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/databricks/databricks-sql-go/internal/featureflags"
	dbsqlrows "github.com/databricks/databricks-sql-go/rows"
	"github.com/databricks/databricks-sql-go/telemetry"
)

// telemetryCapture records the frontend logs POSTed to /telemetry-ext by the
// real telemetry interceptor, aggregator and exporter.
type telemetryCapture struct {
	mu   sync.Mutex
	logs []telemetry.TelemetryFrontendLog
}

func (c *telemetryCapture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	var req telemetry.TelemetryRequest
	if r.URL.Path == "/telemetry-ext" && json.Unmarshal(body, &req) == nil {
		c.mu.Lock()
		for _, raw := range req.ProtoLogs {
			var l telemetry.TelemetryFrontendLog
			if json.Unmarshal([]byte(raw), &l) == nil {
				c.logs = append(c.logs, l)
			}
		}
		c.mu.Unlock()
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"errors":[],"numSuccess":1}`))
}

// executeEvents returns the exported EXECUTE_STATEMENT events for statementID.
func (c *telemetryCapture) executeEvents(statementID string) []*telemetry.TelemetryEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []*telemetry.TelemetryEvent
	for _, l := range c.logs {
		ev := l.Entry.SQLDriverLog
		if ev == nil || ev.SQLStatementID != statementID || ev.SQLOperation == nil ||
			ev.SQLOperation.OperationDetail == nil ||
			ev.SQLOperation.OperationDetail.OperationType != telemetry.OperationTypeExecuteStatement {
			continue
		}
		out = append(out, ev)
	}
	return out
}

// newTelemetryTestConn backs a conn with cli and a real telemetry interceptor
// exporting to a local capture server.
func newTelemetryTestConn(t *testing.T, cli *client.TestClient) (*conn, *telemetryCapture) {
	t.Helper()
	capture := &telemetryCapture{}
	srv := httptest.NewServer(capture)
	t.Cleanup(srv.Close)

	cfg := config.WithDefaults()
	cfg.Host = srv.URL
	cfg.UseLz4Compression = false
	cfg.MaxDownloadThreads = 1

	interceptor := telemetry.InitializeForConnection(context.Background(), telemetry.TelemetryInitOptions{
		FeatureFlags:    featureflags.Request{Host: srv.URL, DriverVersion: "test", UserAgent: "test", HTTPClient: srv.Client()},
		EnableTelemetry: config.NewConfigValue(true),
	})
	require.NotNil(t, interceptor)

	if cli.FnCloseSession == nil {
		cli.FnCloseSession = func(context.Context, *cli_service.TCloseSessionReq) (*cli_service.TCloseSessionResp, error) {
			return &cli_service.TCloseSessionResp{Status: &cli_service.TStatus{StatusCode: cli_service.TStatusCode_SUCCESS_STATUS}}, nil
		}
	}
	be := thriftbackend.NewForTest(cli, getTestSession(), cfg)
	return &conn{id: be.SessionID(), cfg: cfg, backend: be, telemetry: interceptor}, capture
}

// closeAndAwaitExecuteEvent closes the conn (flushing telemetry) and returns
// the single EXECUTE_STATEMENT event exported for the test statement. Every
// Thrift statement, failed or not, is classified SQL (as on the kernel).
func closeAndAwaitExecuteEvent(t *testing.T, c *conn, capture *telemetryCapture) *telemetry.TelemetryEvent {
	t.Helper()
	stmtID := client.SprintGuid(telemetryTestOpHandle().OperationId.GUID)
	_ = c.Close()
	require.Eventually(t, func() bool { return len(capture.executeEvents(stmtID)) > 0 },
		5*time.Second, 10*time.Millisecond, "EXECUTE_STATEMENT event exported")
	// Let any concurrent flush land so a duplicate emission is caught.
	time.Sleep(100 * time.Millisecond)
	events := capture.executeEvents(stmtID)
	require.Len(t, events, 1, "exactly one EXECUTE_STATEMENT event per statement")
	assert.Equal(t, "SQL", events[0].SQLOperation.StatementType, "statement_type")
	return events[0]
}

func telemetryTestOpHandle() *cli_service.TOperationHandle {
	return &cli_service.TOperationHandle{OperationId: &cli_service.THandleIdentifier{
		GUID:   []byte{9, 8, 7, 6, 5, 4, 3, 2, 1, 0, 1, 2, 3, 4, 5, 6},
		Secret: []byte("s"),
	}}
}

func telemetryTestSchema() *cli_service.TTableSchema {
	return &cli_service.TTableSchema{Columns: []*cli_service.TColumnDesc{{
		ColumnName: "n",
		Position:   1,
		TypeDesc: &cli_service.TTypeDesc{Types: []*cli_service.TTypeEntry{{
			PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{Type: cli_service.TTypeId_INT_TYPE},
		}}},
	}}}
}

// telemetryTestArrow returns the Arrow schema message, the record batch that
// follows it (as carried inline by Thrift), and the full IPC stream (as served
// by a CloudFetch link) for a one-row INT column.
func telemetryTestArrow(t *testing.T) (schemaBytes, batchBytes, stream []byte) {
	t.Helper()
	sch := arrow.NewSchema([]arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int32}}, nil)

	var schemaOnly bytes.Buffer
	require.NoError(t, ipc.NewWriter(&schemaOnly, ipc.WithSchema(sch)).Close())

	b := array.NewRecordBuilder(memory.NewGoAllocator(), sch)
	defer b.Release()
	b.Field(0).(*array.Int32Builder).AppendValues([]int32{1}, nil)
	rec := b.NewRecordBatch()
	defer rec.Release()

	var full bytes.Buffer
	w := ipc.NewWriter(&full, ipc.WithSchema(sch))
	require.NoError(t, w.Write(rec))
	require.NoError(t, w.Close())

	n := schemaOnly.Len() - 8 // drop the end-of-stream marker
	return full.Bytes()[:n], full.Bytes()[n:], full.Bytes()
}

func rowSetType(v cli_service.TSparkRowSetType) *cli_service.TSparkRowSetType { return &v }

func finishedStatus() *cli_service.TGetOperationStatusResp {
	return &cli_service.TGetOperationStatusResp{
		Status:          &cli_service.TStatus{StatusCode: cli_service.TStatusCode_SUCCESS_STATUS},
		OperationState:  cli_service.TOperationStatePtr(cli_service.TOperationState_FINISHED_STATE),
		NumModifiedRows: thrift.Int64Ptr(1),
	}
}

// directResultsClient answers ExecuteStatement with complete, server-closed
// direct results.
func directResultsClient(md *cli_service.TGetResultSetMetadataResp, rs *cli_service.TRowSet, metadataCalls *int32) *client.TestClient {
	return &client.TestClient{
		FnExecuteStatement: func(context.Context, *cli_service.TExecuteStatementReq) (*cli_service.TExecuteStatementResp, error) {
			dr := &cli_service.TSparkDirectResults{
				OperationStatus:   finishedStatus(),
				ResultSetMetadata: md,
				CloseOperation:    &cli_service.TCloseOperationResp{},
			}
			if rs != nil {
				dr.ResultSet = &cli_service.TFetchResultsResp{Results: rs, HasMoreRows: thrift.BoolPtr(false)}
			}
			return &cli_service.TExecuteStatementResp{
				Status:          &cli_service.TStatus{StatusCode: cli_service.TStatusCode_SUCCESS_STATUS},
				OperationHandle: telemetryTestOpHandle(),
				DirectResults:   dr,
			}, nil
		},
		FnGetResultSetMetadata: func(context.Context, *cli_service.TGetResultSetMetadataReq) (*cli_service.TGetResultSetMetadataResp, error) {
			atomic.AddInt32(metadataCalls, 1)
			return md, nil
		},
	}
}

// pollingClient answers ExecuteStatement without direct results, so result
// metadata is only available through GetResultSetMetadata.
func pollingClient(md *cli_service.TGetResultSetMetadataResp, rs *cli_service.TRowSet, metadataCalls *int32) *client.TestClient {
	return &client.TestClient{
		FnExecuteStatement: func(context.Context, *cli_service.TExecuteStatementReq) (*cli_service.TExecuteStatementResp, error) {
			return &cli_service.TExecuteStatementResp{
				Status:          &cli_service.TStatus{StatusCode: cli_service.TStatusCode_SUCCESS_STATUS},
				OperationHandle: telemetryTestOpHandle(),
			}, nil
		},
		FnGetOperationStatus: func(context.Context, *cli_service.TGetOperationStatusReq) (*cli_service.TGetOperationStatusResp, error) {
			return finishedStatus(), nil
		},
		FnGetResultSetMetadata: func(context.Context, *cli_service.TGetResultSetMetadataReq) (*cli_service.TGetResultSetMetadataResp, error) {
			atomic.AddInt32(metadataCalls, 1)
			return md, nil
		},
		FnFetchResults: func(context.Context, *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
			return &cli_service.TFetchResultsResp{
				Status:            &cli_service.TStatus{StatusCode: cli_service.TStatusCode_SUCCESS_STATUS},
				Results:           rs,
				HasMoreRows:       thrift.BoolPtr(false),
				ResultSetMetadata: md,
			}, nil
		},
		FnCloseOperation: func(context.Context, *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
			return &cli_service.TCloseOperationResp{Status: &cli_service.TStatus{StatusCode: cli_service.TStatusCode_SUCCESS_STATUS}}, nil
		},
	}
}

// drainRows reads every row the way database/sql does (Columns, then Next to
// io.EOF) and closes the rows.
func drainRows(t *testing.T, r driver.Rows) int {
	t.Helper()
	dest := make([]driver.Value, len(r.Columns()))
	n := 0
	for {
		err := r.Next(dest)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		n++
	}
	require.NoError(t, r.Close())
	return n
}

func TestConn_QueryContextTelemetryExecutionResult(t *testing.T) {
	// Arrow v12 caches the shared Timestamp_us zone without a lock (see #465).
	// Warm it while tests still run serially so the parallel Arrow cases below
	// don't race on it.
	_, err := arrow.FixedWidthTypes.Timestamp_us.(*arrow.TimestampType).GetToTimeFunc()
	require.NoError(t, err)
	t.Parallel()
	schemaBytes, batchBytes, stream := telemetryTestArrow(t)

	t.Run("inline Arrow direct results report INLINE_ARROW", func(t *testing.T) {
		t.Parallel()
		var metadataCalls int32
		md := &cli_service.TGetResultSetMetadataResp{
			Schema: telemetryTestSchema(), ArrowSchema: schemaBytes,
			ResultFormat: rowSetType(cli_service.TSparkRowSetType_ARROW_BASED_SET),
		}
		rs := &cli_service.TRowSet{ArrowBatches: []*cli_service.TSparkArrowBatch{{Batch: batchBytes, RowCount: 1}}}
		c, capture := newTelemetryTestConn(t, directResultsClient(md, rs, &metadataCalls))

		r, err := c.QueryContext(context.Background(), "SELECT 1 AS n", nil)
		require.NoError(t, err)
		assert.Equal(t, 1, drainRows(t, r))

		ev := closeAndAwaitExecuteEvent(t, c, capture)
		assert.Equal(t, "INLINE_ARROW", ev.SQLOperation.ExecutionResult)
		assert.Nil(t, ev.ErrorInfo)
		assert.Zero(t, atomic.LoadInt32(&metadataCalls), "direct results metadata is reused, no extra RPC")
	})

	t.Run("CloudFetch direct results report EXTERNAL_LINKS", func(t *testing.T) {
		t.Parallel()
		cf := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write(stream)
		}))
		t.Cleanup(cf.Close)

		var metadataCalls int32
		md := &cli_service.TGetResultSetMetadataResp{
			Schema: telemetryTestSchema(), ArrowSchema: schemaBytes,
			ResultFormat: rowSetType(cli_service.TSparkRowSetType_URL_BASED_SET),
		}
		rs := &cli_service.TRowSet{ResultLinks: []*cli_service.TSparkArrowResultLink{{
			FileLink: cf.URL, ExpiryTime: time.Now().Add(10 * time.Minute).Unix(), RowCount: 1,
		}}}
		c, capture := newTelemetryTestConn(t, directResultsClient(md, rs, &metadataCalls))

		r, err := c.QueryContext(context.Background(), "SELECT * FROM big", nil)
		require.NoError(t, err)
		assert.Equal(t, 1, drainRows(t, r))

		ev := closeAndAwaitExecuteEvent(t, c, capture)
		assert.Equal(t, "EXTERNAL_LINKS", ev.SQLOperation.ExecutionResult)
		assert.NotNil(t, ev.SQLOperation.ChunkDetails, "CloudFetch chunk details still reported")
		assert.Zero(t, atomic.LoadInt32(&metadataCalls))
	})

	t.Run("format known only after the first fetch", func(t *testing.T) {
		t.Parallel()
		var metadataCalls int32
		md := &cli_service.TGetResultSetMetadataResp{
			Schema:       telemetryTestSchema(),
			ResultFormat: rowSetType(cli_service.TSparkRowSetType_COLUMN_BASED_SET),
		}
		rs := &cli_service.TRowSet{Columns: []*cli_service.TColumn{{
			I32Val: &cli_service.TI32Column{Values: []int32{1}, Nulls: []byte{0}},
		}}}
		c, capture := newTelemetryTestConn(t, pollingClient(md, rs, &metadataCalls))

		r, err := c.QueryContext(context.Background(), "SELECT 1 AS n", nil)
		require.NoError(t, err)
		assert.Equal(t, 1, drainRows(t, r))

		ev := closeAndAwaitExecuteEvent(t, c, capture)
		assert.Equal(t, "COLUMNAR_INLINE", ev.SQLOperation.ExecutionResult)
		assert.Equal(t, int32(1), atomic.LoadInt32(&metadataCalls), "no metadata RPC beyond the one rows already issue")
	})

	// Arrow schema initialization fetches metadata once; telemetry reuses it.
	for _, api := range []struct {
		name    string
		consume func(t *testing.T, r dbsqlrows.Rows)
	}{
		{"GetArrowBatches", func(t *testing.T, r dbsqlrows.Rows) {
			it, err := r.GetArrowBatches(context.Background())
			require.NoError(t, err)
			defer it.Close()
			n := int64(0)
			for it.HasNext() {
				rec, err := it.Next()
				require.NoError(t, err)
				n += rec.NumRows()
				rec.Release()
			}
			assert.Equal(t, int64(1), n)
		}},
		{"GetArrowIPCStreams", func(t *testing.T, r dbsqlrows.Rows) {
			it, err := r.GetArrowIPCStreams(context.Background())
			require.NoError(t, err)
			defer it.Close()
			streams := 0
			for it.HasNext() {
				rd, err := it.Next()
				require.NoError(t, err)
				_, err = io.ReadAll(rd)
				require.NoError(t, err)
				streams++
			}
			assert.Equal(t, 1, streams)
		}},
	} {
		t.Run(api.name+" reports the result format", func(t *testing.T) {
			t.Parallel()
			var metadataCalls int32
			md := &cli_service.TGetResultSetMetadataResp{
				Schema: telemetryTestSchema(), ArrowSchema: schemaBytes,
				ResultFormat: rowSetType(cli_service.TSparkRowSetType_ARROW_BASED_SET),
			}
			rs := &cli_service.TRowSet{ArrowBatches: []*cli_service.TSparkArrowBatch{{Batch: batchBytes, RowCount: 1}}}
			c, capture := newTelemetryTestConn(t, pollingClient(md, rs, &metadataCalls))

			r, err := c.QueryContext(context.Background(), "SELECT 1 AS n", nil)
			require.NoError(t, err)
			api.consume(t, r.(dbsqlrows.Rows))
			require.NoError(t, r.Close())

			ev := closeAndAwaitExecuteEvent(t, c, capture)
			assert.Equal(t, "INLINE_ARROW", ev.SQLOperation.ExecutionResult)
			assert.Nil(t, ev.ErrorInfo)
			assert.Equal(t, int32(1), atomic.LoadInt32(&metadataCalls), "no metadata RPC beyond schema initialization")
		})
	}

	t.Run("unknown format is omitted without a metadata RPC", func(t *testing.T) {
		t.Parallel()
		var metadataCalls int32
		md := &cli_service.TGetResultSetMetadataResp{
			Schema:       telemetryTestSchema(),
			ResultFormat: rowSetType(cli_service.TSparkRowSetType_COLUMN_BASED_SET),
		}
		c, capture := newTelemetryTestConn(t, pollingClient(md, nil, &metadataCalls))

		r, err := c.QueryContext(context.Background(), "SELECT 1 AS n", nil)
		require.NoError(t, err)
		require.NoError(t, r.Close()) // closed before the result set was read

		ev := closeAndAwaitExecuteEvent(t, c, capture)
		assert.Empty(t, ev.SQLOperation.ExecutionResult)
		assert.Zero(t, atomic.LoadInt32(&metadataCalls), "telemetry must not fetch metadata on its own")
	})

	t.Run("execution failure reports no format", func(t *testing.T) {
		t.Parallel()
		var metadataCalls int32
		cli := pollingClient(nil, nil, &metadataCalls)
		cli.FnGetOperationStatus = func(context.Context, *cli_service.TGetOperationStatusReq) (*cli_service.TGetOperationStatusResp, error) {
			return &cli_service.TGetOperationStatusResp{
				Status:         &cli_service.TStatus{StatusCode: cli_service.TStatusCode_SUCCESS_STATUS},
				OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_ERROR_STATE),
				ErrorMessage:   thrift.StringPtr("boom"),
			}, nil
		}
		c, capture := newTelemetryTestConn(t, cli)

		_, err := c.QueryContext(context.Background(), "SELECT broken", nil)
		require.Error(t, err)

		ev := closeAndAwaitExecuteEvent(t, c, capture)
		assert.Empty(t, ev.SQLOperation.ExecutionResult)
		assert.NotNil(t, ev.ErrorInfo)
	})

	t.Run("fetch failure keeps the received format", func(t *testing.T) {
		t.Parallel()
		var metadataCalls int32
		md := &cli_service.TGetResultSetMetadataResp{
			Schema:       telemetryTestSchema(),
			ResultFormat: rowSetType(cli_service.TSparkRowSetType_COLUMN_BASED_SET),
		}
		cli := pollingClient(md, nil, &metadataCalls)
		cli.FnFetchResults = func(context.Context, *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
			return nil, io.ErrUnexpectedEOF
		}
		c, capture := newTelemetryTestConn(t, cli)

		r, err := c.QueryContext(context.Background(), "SELECT 1 AS n", nil)
		require.NoError(t, err)
		dest := make([]driver.Value, len(r.Columns())) // reads the metadata
		require.Error(t, r.Next(dest))
		_ = r.Close()

		ev := closeAndAwaitExecuteEvent(t, c, capture)
		assert.Equal(t, "COLUMNAR_INLINE", ev.SQLOperation.ExecutionResult)
		assert.NotNil(t, ev.ErrorInfo)
	})

	// Results either fails (the conn reports the format the discarded rows
	// received) or succeeds (only Rows.Close reports it); either way it is
	// reported once. The metadata comes inline in the direct results or from
	// the RPC the rows already issue when the direct results omit it.
	for _, tc := range []struct {
		name          string
		arrowSchema   []byte
		setupFails    bool
		inlineMD      bool
		metadataCalls int32
	}{
		{"direct results format reported once by Rows.Close", schemaBytes, false, true, 0},
		{"result setup failure keeps the received format", []byte("not an arrow schema"), true, true, 0},
		{"fetched metadata format reported once by Rows.Close", schemaBytes, false, false, 1},
		{"result setup failure keeps the fetched format", []byte("not an arrow schema"), true, false, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var metadataCalls int32
			md := &cli_service.TGetResultSetMetadataResp{
				Schema: telemetryTestSchema(), ArrowSchema: tc.arrowSchema,
				ResultFormat: rowSetType(cli_service.TSparkRowSetType_ARROW_BASED_SET),
			}
			rs := &cli_service.TRowSet{ArrowBatches: []*cli_service.TSparkArrowBatch{{Batch: batchBytes, RowCount: 1}}}
			cli := directResultsClient(md, rs, &metadataCalls)
			if !tc.inlineMD {
				cli = directResultsClient(nil, rs, &metadataCalls)
				cli.FnGetResultSetMetadata = func(context.Context, *cli_service.TGetResultSetMetadataReq) (*cli_service.TGetResultSetMetadataResp, error) {
					atomic.AddInt32(&metadataCalls, 1)
					return md, nil
				}
			}
			c, capture := newTelemetryTestConn(t, cli)

			r, err := c.QueryContext(context.Background(), "SELECT 1 AS n", nil)
			if tc.setupFails {
				require.Error(t, err)
				require.Nil(t, r)
			} else {
				require.NoError(t, err)
				assert.Equal(t, 1, drainRows(t, r))
			}

			ev := closeAndAwaitExecuteEvent(t, c, capture)
			assert.Equal(t, "INLINE_ARROW", ev.SQLOperation.ExecutionResult)
			assert.Equal(t, tc.setupFails, ev.ErrorInfo != nil, "error_info")
			assert.Equal(t, tc.metadataCalls, atomic.LoadInt32(&metadataCalls), "no metadata RPC added for telemetry")
		})
	}
}

func TestConn_ExecContextTelemetryExecutionResult(t *testing.T) {
	t.Parallel()
	notStaging := false

	for _, tc := range []struct {
		name          string
		direct        bool
		format        cli_service.TSparkRowSetType
		want          string
		metadataCalls int32
	}{
		{"direct results metadata", true, cli_service.TSparkRowSetType_ARROW_BASED_SET, "INLINE_ARROW", 0},
		{"metadata from the staging check RPC", false, cli_service.TSparkRowSetType_ARROW_BASED_SET, "INLINE_ARROW", 1},
		{"columnar result", false, cli_service.TSparkRowSetType_COLUMN_BASED_SET, "COLUMNAR_INLINE", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var metadataCalls int32
			md := &cli_service.TGetResultSetMetadataResp{
				Schema: telemetryTestSchema(), IsStagingOperation: &notStaging,
				ResultFormat: rowSetType(tc.format),
			}
			cli := pollingClient(md, nil, &metadataCalls)
			if tc.direct {
				cli = directResultsClient(md, nil, &metadataCalls)
			}
			c, capture := newTelemetryTestConn(t, cli)

			res, err := c.ExecContext(context.Background(), "INSERT INTO t VALUES (1)", nil)
			require.NoError(t, err)
			n, _ := res.RowsAffected()
			assert.Equal(t, int64(1), n)

			ev := closeAndAwaitExecuteEvent(t, c, capture)
			assert.Equal(t, tc.want, ev.SQLOperation.ExecutionResult)
			assert.Equal(t, tc.metadataCalls, atomic.LoadInt32(&metadataCalls), "no metadata RPC added for telemetry")
		})
	}

	t.Run("execution failure reports no format", func(t *testing.T) {
		t.Parallel()
		var metadataCalls int32
		md := &cli_service.TGetResultSetMetadataResp{
			IsStagingOperation: &notStaging,
			ResultFormat:       rowSetType(cli_service.TSparkRowSetType_ARROW_BASED_SET),
		}
		cli := pollingClient(md, nil, &metadataCalls)
		cli.FnGetOperationStatus = func(context.Context, *cli_service.TGetOperationStatusReq) (*cli_service.TGetOperationStatusResp, error) {
			return &cli_service.TGetOperationStatusResp{
				Status:         &cli_service.TStatus{StatusCode: cli_service.TStatusCode_SUCCESS_STATUS},
				OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_ERROR_STATE),
				ErrorMessage:   thrift.StringPtr("boom"),
			}, nil
		}
		c, capture := newTelemetryTestConn(t, cli)

		_, err := c.ExecContext(context.Background(), "INSERT INTO broken VALUES (1)", nil)
		require.Error(t, err)

		ev := closeAndAwaitExecuteEvent(t, c, capture)
		assert.Empty(t, ev.SQLOperation.ExecutionResult)
		assert.NotNil(t, ev.ErrorInfo)
	})

	// Like JDBC, the format describes a result the server already returned, so
	// a failure after the metadata was read keeps it.
	t.Run("staging failure keeps the received format", func(t *testing.T) {
		t.Parallel()
		var metadataCalls int32
		staging := true
		md := &cli_service.TGetResultSetMetadataResp{
			Schema: telemetryTestSchema(), IsStagingOperation: &staging,
			ResultFormat: rowSetType(cli_service.TSparkRowSetType_ARROW_BASED_SET),
		}
		c, capture := newTelemetryTestConn(t, directResultsClient(md, nil, &metadataCalls))

		// No staging paths in the context, so the staging step fails.
		_, err := c.ExecContext(context.Background(), "PUT '/tmp/x' INTO '/Volumes/a/b/c/x'", nil)
		require.Error(t, err)

		ev := closeAndAwaitExecuteEvent(t, c, capture)
		assert.Equal(t, "INLINE_ARROW", ev.SQLOperation.ExecutionResult)
		assert.NotNil(t, ev.ErrorInfo)
	})
}
