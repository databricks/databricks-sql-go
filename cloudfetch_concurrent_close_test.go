package dbsql

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/stretchr/testify/require"
)

// TestCloudFetchRowsCloseFromAnotherGoroutine closes sql.Rows from a second
// goroutine while Next is blocked on a CloudFetch download. The CloudFetch
// iterator is single-owner: database/sql serializes driver Next and Close
// (Rows.closemu), so Close waits for the in-flight download and then cancels
// the queued ones. Run with -race.
func TestCloudFetchRowsCloseFromAnotherGoroutine(t *testing.T) {
	const (
		rowsPerFile = 100
		nFiles      = 8
		threads     = 4
	)

	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	var schemaBuf bytes.Buffer
	require.NoError(t, ipc.NewWriter(&schemaBuf, ipc.WithSchema(schema)).Close())
	fileBytes := func(start int64) []byte {
		b := array.NewInt64Builder(memory.DefaultAllocator)
		defer b.Release()
		for i := int64(0); i < rowsPerFile; i++ {
			b.Append(start + i)
		}
		col := b.NewArray()
		defer col.Release()
		rec := array.NewRecordBatch(schema, []arrow.Array{col}, rowsPerFile)
		defer rec.Release()
		var buf bytes.Buffer
		w := ipc.NewWriter(&buf, ipc.WithSchema(schema))
		require.NoError(t, w.Write(rec))
		require.NoError(t, w.Close())
		return buf.Bytes()
	}

	// File 0 is served at once, file 1 waits for releaseFile1, and the rest
	// block until their request is cancelled. stop unblocks handlers if the
	// test fails, so files.Close does not hang.
	var started atomic.Int64
	releaseFile1 := make(chan struct{})
	stop := make(chan struct{})
	files := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started.Add(1)
		idx, _ := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/"))
		if idx > 0 {
			gate := releaseFile1
			if idx > 1 {
				gate = nil
			}
			select {
			case <-gate:
			case <-r.Context().Done():
				return
			case <-stop:
				return
			}
		}
		_, _ = w.Write(fileBytes(int64(idx * rowsPerFile)))
	}))
	defer files.Close()

	links := make([]*cli_service.TSparkArrowResultLink, nFiles)
	for i := range links {
		links[i] = &cli_service.TSparkArrowResultLink{
			FileLink:       fmt.Sprintf("%s/%d", files.URL, i),
			ExpiryTime:     time.Now().Add(10 * time.Minute).Unix(),
			StartRowOffset: int64(i * rowsPerFile),
			RowCount:       rowsPerFile,
		}
	}

	state := &callState{}
	loadTestData(t, "OpenSessionSuccess.json", &state.openSessionResp)
	loadTestData(t, "CloseSessionSuccess.json", &state.closeSessionResp)
	loadTestData(t, "CloseOperationSuccess.json", &state.closeOperationResp)
	loadTestData(t, "ExecuteStatement1.json", &state.executeStatementResp)
	urlBased := cli_service.TSparkRowSetType_URL_BASED_SET
	direct := state.executeStatementResp.DirectResults
	direct.CloseOperation = nil
	direct.ResultSetMetadata.ResultFormat = &urlBased
	direct.ResultSetMetadata.ArrowSchema = schemaBuf.Bytes()
	direct.ResultSetMetadata.Schema.Columns = []*cli_service.TColumnDesc{{
		ColumnName: "id",
		TypeDesc: &cli_service.TTypeDesc{Types: []*cli_service.TTypeEntry{{
			PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{Type: cli_service.TTypeId_BIGINT_TYPE},
		}}},
		Position: 1,
	}}
	direct.ResultSet.Results = &cli_service.TRowSet{ResultLinks: links}

	thriftServer := getServer(state)
	defer thriftServer.Close()
	u, err := url.Parse(thriftServer.URL)
	require.NoError(t, err)
	port, err := strconv.Atoi(u.Port())
	require.NoError(t, err)
	connector, err := NewConnector(
		WithServerHostname("localhost"),
		WithPort(port),
		WithHTTPPath("/"),
		WithMaxDownloadThreads(threads),
	)
	require.NoError(t, err)
	db := sql.OpenDB(connector)
	defer db.Close() //nolint:errcheck

	rows, err := db.QueryContext(context.Background(), "SELECT id")
	require.NoError(t, err)
	defer func() {
		// Release blocked downloads before Close waits for Next.
		close(stop)
		_ = rows.Close()
	}()

	var consumed atomic.Int64
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for rows.Next() {
			var id int64
			if rows.Scan(&id) == nil {
				consumed.Add(1)
			}
		}
	}()

	// After draining file 0 the reader refills the queue (threads+1 requests)
	// inside driver Next, then blocks there on file 1.
	require.Eventually(t, func() bool {
		return consumed.Load() == rowsPerFile && started.Load() == threads+1
	}, 5*time.Second, 5*time.Millisecond)

	closeDone := make(chan error, 1)
	go func() { closeDone <- rows.Close() }()
	select {
	case err := <-closeDone:
		t.Fatalf("rows.Close returned (err=%v) while driver Next was in progress; the CloudFetch iterator relies on database/sql serializing them", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(releaseFile1)
	select {
	case err := <-closeDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("rows.Close did not return after the in-flight download completed")
	}
	select {
	case <-readerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("reader did not finish after rows.Close")
	}

	require.Eventually(t, func() bool {
		buf := make([]byte, 1<<20)
		return !strings.Contains(string(buf[:runtime.Stack(buf, true)]), "cloudFetchDownloadTask).Run")
	}, 5*time.Second, 20*time.Millisecond, "CloudFetch download goroutines leaked after rows.Close")
}
