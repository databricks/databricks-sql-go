package arrowbased

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/pkg/errors"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/ipc"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestCloudFetchIterator(t *testing.T) {

	var handler func(w http.ResponseWriter, r *http.Request)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler(w, r)
	}))
	defer server.Close()

	writeTruncatedOK := func(t *testing.T, w http.ResponseWriter, body []byte) {
		t.Helper()
		hj, ok := w.(http.Hijacker)
		if !ok {
			t.Errorf("ResponseWriter does not support Hijacker")
			return
		}
		conn, bufrw, err := hj.Hijack()
		if err != nil {
			t.Errorf("hijack failed: %v", err)
			return
		}
		_, _ = fmt.Fprintf(bufrw, "HTTP/1.1 200 OK\r\nContent-Length: 1000000\r\nConnection: close\r\n\r\n")
		if len(body) > 0 {
			_, _ = bufrw.Write(body)
		}
		_ = bufrw.Flush()
		_ = conn.Close()
	}

	t.Run("should fetch all the links", func(t *testing.T) {
		cloudFetchHeaders := map[string]string{
			"foo": "bar",
		}

		handler = func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			for name, value := range cloudFetchHeaders {
				if values, ok := r.Header[name]; ok {
					if values[0] != value {
						panic(errors.New("Missing auth headers"))
					}
				}
			}
			_, err := w.Write(generateMockArrowBytes(generateArrowRecord()))
			if err != nil {
				panic(err)
			}
		}

		startRowOffset := int64(100)

		links := []*cli_service.TSparkArrowResultLink{
			{
				FileLink:       server.URL,
				ExpiryTime:     time.Now().Add(10 * time.Minute).Unix(),
				StartRowOffset: startRowOffset,
				RowCount:       1,
				HttpHeaders:    cloudFetchHeaders,
			},
			{
				FileLink:       server.URL,
				ExpiryTime:     time.Now().Add(10 * time.Minute).Unix(),
				StartRowOffset: startRowOffset + 1,
				RowCount:       1,
				HttpHeaders:    cloudFetchHeaders,
			},
		}

		cfg := config.WithDefaults()
		cfg.UseLz4Compression = false
		cfg.MaxDownloadThreads = 1

		bi, err := NewCloudBatchIterator(
			context.Background(),
			links,
			startRowOffset,
			nil,
			cfg,
			nil,
		)
		if err != nil {
			panic(err)
		}

		// Access the internal structure through the wrapper
		wrapper, ok := bi.(*batchIterator)
		assert.True(t, ok)
		cbi, ok := wrapper.ipcIterator.(*cloudIPCStreamIterator)
		assert.True(t, ok)

		assert.True(t, bi.HasNext())
		assert.Equal(t, cbi.pendingLinks.Len(), len(links))
		assert.Equal(t, cbi.downloadTasks.Len(), 0)

		// get first link - should succeed
		sab1, err2 := bi.Next()
		if err2 != nil {
			panic(err2)
		}

		assert.Equal(t, cbi.pendingLinks.Len(), len(links)-1)
		assert.Equal(t, cbi.downloadTasks.Len(), 0)
		assert.Equal(t, sab1.Start(), startRowOffset)

		// get second link - should succeed
		sab2, err3 := bi.Next()
		if err3 != nil {
			panic(err3)
		}

		assert.Equal(t, cbi.pendingLinks.Len(), len(links)-2)
		assert.Equal(t, cbi.downloadTasks.Len(), 0)
		assert.Equal(t, sab2.Start(), startRowOffset+sab1.Count())

		// all links downloaded, should be no more data
		assert.False(t, bi.HasNext())
	})

	t.Run("should fail on expired link", func(t *testing.T) {
		handler = func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, err := w.Write(generateMockArrowBytes(generateArrowRecord()))
			if err != nil {
				panic(err)
			}
		}

		startRowOffset := int64(100)

		links := []*cli_service.TSparkArrowResultLink{
			{
				FileLink:       server.URL,
				ExpiryTime:     time.Now().Add(10 * time.Minute).Unix(),
				StartRowOffset: startRowOffset,
				RowCount:       1,
			},
			{
				FileLink:       server.URL,
				ExpiryTime:     time.Now().Add(-10 * time.Minute).Unix(), // expired link
				StartRowOffset: startRowOffset + 1,
				RowCount:       1,
			},
		}

		cfg := config.WithDefaults()
		cfg.UseLz4Compression = false
		cfg.MaxDownloadThreads = 1

		bi, err := NewCloudBatchIterator(
			context.Background(),
			links,
			startRowOffset,
			nil,
			cfg,
			nil,
		)
		if err != nil {
			panic(err)
		}

		// Access the internal structure through the wrapper
		wrapper, ok := bi.(*batchIterator)
		assert.True(t, ok)
		cbi, ok := wrapper.ipcIterator.(*cloudIPCStreamIterator)
		assert.True(t, ok)

		assert.True(t, bi.HasNext())
		assert.Equal(t, cbi.pendingLinks.Len(), len(links))
		assert.Equal(t, cbi.downloadTasks.Len(), 0)

		// get first link - should succeed
		sab1, err2 := bi.Next()
		if err2 != nil {
			panic(err2)
		}

		assert.Equal(t, cbi.pendingLinks.Len(), len(links)-1)
		assert.Equal(t, cbi.downloadTasks.Len(), 0)
		assert.Equal(t, sab1.Start(), startRowOffset)

		// get second link - should fail
		_, err3 := bi.Next()
		assert.NotNil(t, err3)
		assert.ErrorContains(t, err3, dbsqlerr.ErrLinkExpired)
	})

	t.Run("should fail on HTTP errors", func(t *testing.T) {
		startRowOffset := int64(100)

		links := []*cli_service.TSparkArrowResultLink{
			{
				FileLink:       server.URL,
				ExpiryTime:     time.Now().Add(10 * time.Minute).Unix(),
				StartRowOffset: startRowOffset,
				RowCount:       1,
			},
			{
				FileLink:       server.URL,
				ExpiryTime:     time.Now().Add(10 * time.Minute).Unix(),
				StartRowOffset: startRowOffset + 1,
				RowCount:       1,
			},
		}

		cfg := config.WithDefaults()
		cfg.UseLz4Compression = false
		cfg.MaxDownloadThreads = 1

		bi, err := NewCloudBatchIterator(
			context.Background(),
			links,
			startRowOffset,
			nil,
			cfg,
			nil,
		)
		if err != nil {
			panic(err)
		}

		// Access the internal structure through the wrapper
		wrapper, ok := bi.(*batchIterator)
		assert.True(t, ok)
		cbi, ok := wrapper.ipcIterator.(*cloudIPCStreamIterator)
		assert.True(t, ok)

		assert.True(t, bi.HasNext())
		assert.Equal(t, cbi.pendingLinks.Len(), len(links))
		assert.Equal(t, cbi.downloadTasks.Len(), 0)

		// set handler for the first link, which returns some data
		handler = func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, err := w.Write(generateMockArrowBytes(generateArrowRecord()))
			if err != nil {
				panic(err)
			}
		}

		// get first link - should succeed
		sab1, err2 := bi.Next()
		if err2 != nil {
			panic(err2)
		}

		assert.Equal(t, cbi.pendingLinks.Len(), len(links)-1)
		assert.Equal(t, cbi.downloadTasks.Len(), 0)
		assert.Equal(t, sab1.Start(), startRowOffset)

		// set handler for the first link, which fails with some non-retryable HTTP error
		handler = func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}

		// get second link - should fail
		_, err3 := bi.Next()
		assert.NotNil(t, err3)
		assert.ErrorContains(t, err3, fmt.Sprintf("%s %d", "HTTP error", http.StatusNotFound))
	})

	t.Run("should use custom HTTPClient when provided", func(t *testing.T) {
		handler = func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, err := w.Write(generateMockArrowBytes(generateArrowRecord()))
			if err != nil {
				panic(err)
			}
		}

		startRowOffset := int64(100)
		customHTTPClient := &http.Client{
			Transport: &http.Transport{MaxIdleConns: 10},
		}

		cfg := config.WithDefaults()
		cfg.UseLz4Compression = false
		cfg.MaxDownloadThreads = 1
		cfg.HTTPClient = customHTTPClient

		bi, err := NewCloudBatchIterator(
			context.Background(),
			[]*cli_service.TSparkArrowResultLink{{
				FileLink:       server.URL,
				ExpiryTime:     time.Now().Add(10 * time.Minute).Unix(),
				StartRowOffset: startRowOffset,
				RowCount:       1,
			}},
			startRowOffset,
			nil,
			cfg,
			nil,
		)
		assert.Nil(t, err)

		cbi := bi.(*batchIterator).ipcIterator.(*cloudIPCStreamIterator)
		assert.Equal(t, customHTTPClient, cbi.httpClient)

		// Verify fetch works
		sab, nextErr := bi.Next()
		assert.Nil(t, nextErr)
		assert.NotNil(t, sab)
	})

	t.Run("should fallback to http.DefaultClient when HTTPClient is nil", func(t *testing.T) {
		handler = func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, err := w.Write(generateMockArrowBytes(generateArrowRecord()))
			if err != nil {
				panic(err)
			}
		}

		startRowOffset := int64(100)
		cfg := config.WithDefaults()
		cfg.UseLz4Compression = false
		cfg.MaxDownloadThreads = 1
		// Explicitly set HTTPClient to nil to verify fallback behavior
		cfg.HTTPClient = nil

		bi, err := NewCloudBatchIterator(
			context.Background(),
			[]*cli_service.TSparkArrowResultLink{{
				FileLink:       server.URL,
				ExpiryTime:     time.Now().Add(10 * time.Minute).Unix(),
				StartRowOffset: startRowOffset,
				RowCount:       1,
			}},
			startRowOffset,
			nil,
			cfg,
			nil,
		)
		assert.Nil(t, err)

		cbi := bi.(*batchIterator).ipcIterator.(*cloudIPCStreamIterator)
		assert.Equal(t, http.DefaultClient, cbi.httpClient)

		// Verify fetch works with default client
		sab, nextErr := bi.Next()
		assert.Nil(t, nextErr)
		assert.NotNil(t, sab)
	})

	t.Run("should retry transient HTTP 503 and eventually succeed", func(t *testing.T) {
		var attempts int32
		handler = func(w http.ResponseWriter, r *http.Request) {
			n := atomic.AddInt32(&attempts, 1)
			if n < 3 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, err := w.Write(generateMockArrowBytes(generateArrowRecord()))
			if err != nil {
				panic(err)
			}
		}

		startRowOffset := int64(100)
		cfg := config.WithDefaults()
		cfg.UseLz4Compression = false
		cfg.MaxDownloadThreads = 1
		cfg.RetryMax = 4
		cfg.RetryWaitMin = 1 * time.Millisecond
		cfg.RetryWaitMax = 5 * time.Millisecond

		bi, err := NewCloudBatchIterator(
			context.Background(),
			[]*cli_service.TSparkArrowResultLink{{
				FileLink:       server.URL,
				ExpiryTime:     time.Now().Add(10 * time.Minute).Unix(),
				StartRowOffset: startRowOffset,
				RowCount:       1,
			}},
			startRowOffset,
			nil,
			cfg,
			nil,
		)
		assert.Nil(t, err)

		sab, nextErr := bi.Next()
		assert.Nil(t, nextErr)
		assert.NotNil(t, sab)
		assert.Equal(t, int32(3), atomic.LoadInt32(&attempts), "expected 2 retries before success")
	})

	t.Run("should retry mid-stream body read failures (200 OK then connection drop)", func(t *testing.T) {
		var attempts int32
		realBody := generateMockArrowBytes(generateArrowRecord())
		handler = func(w http.ResponseWriter, r *http.Request) {
			n := atomic.AddInt32(&attempts, 1)
			if n == 1 {
				writeTruncatedOK(t, w, []byte("partial"))
				return
			}
			w.WriteHeader(http.StatusOK)
			if _, err := w.Write(realBody); err != nil {
				panic(err)
			}
		}

		startRowOffset := int64(100)
		cfg := config.WithDefaults()
		cfg.UseLz4Compression = false
		cfg.MaxDownloadThreads = 1
		cfg.RetryMax = 4
		cfg.RetryWaitMin = 1 * time.Millisecond
		cfg.RetryWaitMax = 5 * time.Millisecond

		bi, err := NewCloudBatchIterator(
			context.Background(),
			[]*cli_service.TSparkArrowResultLink{{
				FileLink:       server.URL,
				ExpiryTime:     time.Now().Add(10 * time.Minute).Unix(),
				StartRowOffset: startRowOffset,
				RowCount:       1,
			}},
			startRowOffset,
			nil,
			cfg,
			nil,
		)
		assert.Nil(t, err)

		sab, nextErr := bi.Next()
		assert.Nil(t, nextErr)
		assert.NotNil(t, sab)
		assert.Equal(t, int32(2), atomic.LoadInt32(&attempts), "expected first attempt to fail mid-stream, second to succeed")
	})

	t.Run("should fail after exhausting retries on persistent body-read failures", func(t *testing.T) {
		var attempts int32
		handler = func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&attempts, 1)
			writeTruncatedOK(t, w, nil)
		}

		startRowOffset := int64(100)
		cfg := config.WithDefaults()
		cfg.UseLz4Compression = false
		cfg.MaxDownloadThreads = 1
		cfg.RetryMax = 2
		cfg.RetryWaitMin = 1 * time.Millisecond
		cfg.RetryWaitMax = 5 * time.Millisecond

		bi, err := NewCloudBatchIterator(
			context.Background(),
			[]*cli_service.TSparkArrowResultLink{{
				FileLink:       server.URL,
				ExpiryTime:     time.Now().Add(10 * time.Minute).Unix(),
				StartRowOffset: startRowOffset,
				RowCount:       1,
			}},
			startRowOffset,
			nil,
			cfg,
			nil,
		)
		assert.Nil(t, err)

		_, nextErr := bi.Next()
		assert.NotNil(t, nextErr)
		assert.ErrorContains(t, nextErr, "after 2 retries")
		// initial attempt + RetryMax retries
		assert.Equal(t, int32(3), atomic.LoadInt32(&attempts))
	})

	t.Run("should retry transient HTTP 500 and eventually succeed", func(t *testing.T) {
		var attempts int32
		handler = func(w http.ResponseWriter, r *http.Request) {
			n := atomic.AddInt32(&attempts, 1)
			if n < 2 {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, err := w.Write(generateMockArrowBytes(generateArrowRecord()))
			if err != nil {
				panic(err)
			}
		}

		startRowOffset := int64(100)
		cfg := config.WithDefaults()
		cfg.UseLz4Compression = false
		cfg.MaxDownloadThreads = 1
		cfg.RetryMax = 4
		cfg.RetryWaitMin = 1 * time.Millisecond
		cfg.RetryWaitMax = 5 * time.Millisecond

		bi, err := NewCloudBatchIterator(
			context.Background(),
			[]*cli_service.TSparkArrowResultLink{{
				FileLink:       server.URL,
				ExpiryTime:     time.Now().Add(10 * time.Minute).Unix(),
				StartRowOffset: startRowOffset,
				RowCount:       1,
			}},
			startRowOffset,
			nil,
			cfg,
			nil,
		)
		assert.Nil(t, err)

		sab, nextErr := bi.Next()
		assert.Nil(t, nextErr)
		assert.NotNil(t, sab)
		assert.Equal(t, int32(2), atomic.LoadInt32(&attempts))
	})

	t.Run("should fail after exhausting retries on persistent 503", func(t *testing.T) {
		var attempts int32
		handler = func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&attempts, 1)
			w.WriteHeader(http.StatusServiceUnavailable)
		}

		startRowOffset := int64(100)
		cfg := config.WithDefaults()
		cfg.UseLz4Compression = false
		cfg.MaxDownloadThreads = 1
		cfg.RetryMax = 2
		cfg.RetryWaitMin = 1 * time.Millisecond
		cfg.RetryWaitMax = 5 * time.Millisecond

		bi, err := NewCloudBatchIterator(
			context.Background(),
			[]*cli_service.TSparkArrowResultLink{{
				FileLink:       server.URL,
				ExpiryTime:     time.Now().Add(10 * time.Minute).Unix(),
				StartRowOffset: startRowOffset,
				RowCount:       1,
			}},
			startRowOffset,
			nil,
			cfg,
			nil,
		)
		assert.Nil(t, err)

		_, nextErr := bi.Next()
		assert.NotNil(t, nextErr)
		assert.ErrorContains(t, nextErr, fmt.Sprintf("HTTP error %d", http.StatusServiceUnavailable))
		assert.ErrorContains(t, nextErr, "after 2 retries")
		// initial attempt + RetryMax retries
		assert.Equal(t, int32(3), atomic.LoadInt32(&attempts))
	})

	t.Run("should not retry on non-retryable status (403)", func(t *testing.T) {
		var attempts int32
		handler = func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&attempts, 1)
			w.WriteHeader(http.StatusForbidden)
		}

		startRowOffset := int64(100)
		cfg := config.WithDefaults()
		cfg.UseLz4Compression = false
		cfg.MaxDownloadThreads = 1
		cfg.RetryMax = 5
		cfg.RetryWaitMin = 1 * time.Millisecond
		cfg.RetryWaitMax = 5 * time.Millisecond

		bi, err := NewCloudBatchIterator(
			context.Background(),
			[]*cli_service.TSparkArrowResultLink{{
				FileLink:       server.URL,
				ExpiryTime:     time.Now().Add(10 * time.Minute).Unix(),
				StartRowOffset: startRowOffset,
				RowCount:       1,
			}},
			startRowOffset,
			nil,
			cfg,
			nil,
		)
		assert.Nil(t, err)

		_, nextErr := bi.Next()
		assert.NotNil(t, nextErr)
		assert.ErrorContains(t, nextErr, fmt.Sprintf("HTTP error %d", http.StatusForbidden))
		assert.NotContains(t, nextErr.Error(), "after")
		assert.Equal(t, int32(1), atomic.LoadInt32(&attempts), "non-retryable status must fail on first attempt")
	})

	t.Run("should detect link expiry between retries", func(t *testing.T) {
		var attempts int32
		handler = func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&attempts, 1)
			w.Header().Set("Retry-After", "3")
			w.WriteHeader(http.StatusServiceUnavailable)
		}

		startRowOffset := int64(100)
		cfg := config.WithDefaults()
		cfg.UseLz4Compression = false
		cfg.MaxDownloadThreads = 1
		cfg.RetryMax = 5
		cfg.RetryWaitMin = 1 * time.Millisecond
		cfg.RetryWaitMax = 3 * time.Second
		expiryTime := time.Now().Unix() + 2

		bi, err := NewCloudBatchIterator(
			context.Background(),
			[]*cli_service.TSparkArrowResultLink{{
				FileLink:       server.URL,
				ExpiryTime:     expiryTime,
				StartRowOffset: startRowOffset,
				RowCount:       1,
			}},
			startRowOffset,
			nil,
			cfg,
			nil,
		)
		assert.Nil(t, err)

		_, nextErr := bi.Next()
		assert.NotNil(t, nextErr)
		assert.ErrorContains(t, nextErr, dbsqlerr.ErrLinkExpired)
		// The retry sleeps past expiry, then short-circuits before another GET.
		assert.Equal(t, int32(1), atomic.LoadInt32(&attempts))
	})

	t.Run("should respect context cancellation during backoff", func(t *testing.T) {
		var attempts int32
		handler = func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&attempts, 1)
			w.WriteHeader(http.StatusServiceUnavailable)
		}

		startRowOffset := int64(100)
		cfg := config.WithDefaults()
		cfg.UseLz4Compression = false
		cfg.MaxDownloadThreads = 1
		cfg.RetryMax = 5
		cfg.RetryWaitMin = 500 * time.Millisecond
		cfg.RetryWaitMax = 1 * time.Second

		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(50 * time.Millisecond)
			cancel()
		}()

		bi, err := NewCloudBatchIterator(
			ctx,
			[]*cli_service.TSparkArrowResultLink{{
				FileLink:       server.URL,
				ExpiryTime:     time.Now().Add(10 * time.Minute).Unix(),
				StartRowOffset: startRowOffset,
				RowCount:       1,
			}},
			startRowOffset,
			nil,
			cfg,
			nil,
		)
		assert.Nil(t, err)

		started := time.Now()
		_, nextErr := bi.Next()
		elapsed := time.Since(started)

		assert.NotNil(t, nextErr)
		// Cancellation should land well before all retries would otherwise complete
		// (5 * 500ms+ = 2.5s+ minimum without cancel).
		assert.Less(t, elapsed, 1*time.Second, "context cancel should abort retry backoff promptly")
	})
}

func TestCloudFetchSchemaOverride(t *testing.T) {
	// Reproduces ES-1804970: When the server result cache serves Arrow IPC files
	// from a prior query, the embedded schema has stale column names. The
	// authoritative schema from GetResultSetMetadata must override them.

	// IPC data has columns ["id", "name"] (stale, from cached query)
	staleRecord := generateArrowRecord()
	staleIPCBytes := generateMockArrowBytes(staleRecord)

	// Authoritative schema has columns ["x", "y"] (correct, from GetResultSetMetadata)
	correctFields := []arrow.Field{
		{Name: "x", Type: arrow.PrimitiveTypes.Int32},
		{Name: "y", Type: arrow.BinaryTypes.String},
	}
	correctSchema := arrow.NewSchema(correctFields, nil)
	var schemaBuf bytes.Buffer
	schemaWriter := ipc.NewWriter(&schemaBuf, ipc.WithSchema(correctSchema))
	if err := schemaWriter.Close(); err != nil {
		t.Fatal(err)
	}
	correctSchemaBytes := schemaBuf.Bytes()

	// Serve stale IPC data via mock HTTP
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, err := w.Write(staleIPCBytes)
		if err != nil {
			panic(err)
		}
	}))
	defer server.Close()

	t.Run("should override stale column names with authoritative schema", func(t *testing.T) {
		links := []*cli_service.TSparkArrowResultLink{
			{
				FileLink:       server.URL,
				ExpiryTime:     time.Now().Add(10 * time.Minute).Unix(),
				StartRowOffset: 0,
				RowCount:       3,
			},
		}

		cfg := config.WithDefaults()
		cfg.UseLz4Compression = false
		cfg.MaxDownloadThreads = 1

		bi, err := NewCloudBatchIterator(
			context.Background(),
			links,
			0,
			correctSchemaBytes,
			cfg,
			nil,
		)
		assert.Nil(t, err)

		batch, batchErr := bi.Next()
		assert.Nil(t, batchErr)
		assert.NotNil(t, batch)

		rec, recErr := batch.Next()
		assert.Nil(t, recErr)
		assert.NotNil(t, rec)

		// The record schema must use the authoritative names, not the stale ones
		assert.Equal(t, "x", rec.Schema().Field(0).Name)
		assert.Equal(t, "y", rec.Schema().Field(1).Name)

		// Data must be preserved
		assert.Equal(t, int64(3), rec.NumRows())
		assert.Equal(t, 2, len(rec.Schema().Fields()))

		rec.Release()
	})

	t.Run("should pass through unchanged when no override schema provided", func(t *testing.T) {
		links := []*cli_service.TSparkArrowResultLink{
			{
				FileLink:       server.URL,
				ExpiryTime:     time.Now().Add(10 * time.Minute).Unix(),
				StartRowOffset: 0,
				RowCount:       3,
			},
		}

		cfg := config.WithDefaults()
		cfg.UseLz4Compression = false
		cfg.MaxDownloadThreads = 1

		bi, err := NewCloudBatchIterator(
			context.Background(),
			links,
			0,
			nil,
			cfg,
			nil,
		)
		assert.Nil(t, err)

		batch, batchErr := bi.Next()
		assert.Nil(t, batchErr)

		rec, recErr := batch.Next()
		assert.Nil(t, recErr)

		// Without override, the original (stale) column names are preserved
		assert.Equal(t, "id", rec.Schema().Field(0).Name)
		assert.Equal(t, "name", rec.Schema().Field(1).Name)

		rec.Release()
	})
}

// TestCloudFetchIterator_OnFileDownloaded_CallbackInvokedWithPositiveDuration verifies
// that the onFileDownloaded telemetry callback is called once per downloaded S3 file with
// a positive downloadMs value.
//
// This covers the CloudFetch timing fix where per-S3-file download durations are measured
// and reported as initial_chunk_latency_ms / slowest_chunk_latency_ms / sum_chunks_download_time_ms
// in the telemetry payload.
func TestCloudFetchIterator_OnFileDownloaded_CallbackInvokedWithPositiveDuration(t *testing.T) {
	// Serve real arrow bytes so the iterator can parse them successfully.
	arrowBytes := generateMockArrowBytes(generateArrowRecord())

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Add a tiny sleep so the measured download time is reliably > 0ms.
		time.Sleep(2 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(arrowBytes)
	}))
	defer server.Close()

	startRowOffset := int64(0)
	links := []*cli_service.TSparkArrowResultLink{
		{
			FileLink:       server.URL,
			ExpiryTime:     time.Now().Add(10 * time.Minute).Unix(),
			StartRowOffset: startRowOffset,
			RowCount:       1,
		},
		{
			FileLink:       server.URL,
			ExpiryTime:     time.Now().Add(10 * time.Minute).Unix(),
			StartRowOffset: startRowOffset + 1,
			RowCount:       1,
		},
	}

	cfg := config.WithDefaults()
	cfg.UseLz4Compression = false
	cfg.MaxDownloadThreads = 1

	var callbackMu sync.Mutex
	var downloadDurations []int64

	onFileDownloaded := func(downloadMs int64) {
		callbackMu.Lock()
		downloadDurations = append(downloadDurations, downloadMs)
		callbackMu.Unlock()
	}

	bi, err := NewCloudBatchIterator(context.Background(), links, startRowOffset, nil, cfg, onFileDownloaded)
	assert.Nil(t, err)

	// Consume all batches to trigger the downloads.
	for bi.HasNext() {
		_, nextErr := bi.Next()
		assert.Nil(t, nextErr)
	}

	callbackMu.Lock()
	durations := make([]int64, len(downloadDurations))
	copy(durations, downloadDurations)
	callbackMu.Unlock()

	// Callback must be invoked once per link.
	assert.Equal(t, len(links), len(durations),
		"onFileDownloaded must be called once per downloaded file")

	// Each reported duration must be positive (the server adds a 2ms delay).
	for i, d := range durations {
		assert.Greater(t, d, int64(0),
			"onFileDownloaded must report positive downloadMs for file %d, got %d", i, d)
	}
}

// TestCloudFetchIterator_OnFileDownloaded_NilCallbackDoesNotPanic verifies that passing
// nil for onFileDownloaded (non-telemetry paths) does not cause a panic during iteration.
func TestCloudFetchIterator_OnFileDownloaded_NilCallbackDoesNotPanic(t *testing.T) {
	arrowBytes := generateMockArrowBytes(generateArrowRecord())

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(arrowBytes)
	}))
	defer server.Close()

	startRowOffset := int64(0)
	links := []*cli_service.TSparkArrowResultLink{
		{
			FileLink:       server.URL,
			ExpiryTime:     time.Now().Add(10 * time.Minute).Unix(),
			StartRowOffset: startRowOffset,
			RowCount:       1,
		},
	}

	cfg := config.WithDefaults()
	cfg.UseLz4Compression = false
	cfg.MaxDownloadThreads = 1

	// nil callback — must not panic
	bi, err := NewCloudBatchIterator(context.Background(), links, startRowOffset, nil, cfg, nil)
	assert.Nil(t, err)

	assert.NotPanics(t, func() {
		for bi.HasNext() {
			_, _ = bi.Next()
		}
	}, "nil onFileDownloaded must not cause a panic")
}

func generateArrowRecord() arrow.Record {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())

	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}
	schema := arrow.NewSchema(fields, nil)

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	builder.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3}, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues([]string{"one", "two", "three"}, nil)

	record := builder.NewRecord()

	return record
}

func generateMockArrowBytes(record arrow.Record) []byte {

	defer record.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(record.Schema()))
	if err := w.Write(record); err != nil {
		return nil
	}
	if err := w.Write(record); err != nil {
		return nil
	}
	if err := w.Close(); err != nil {
		return nil
	}
	return buf.Bytes()
}

// TestCloudFetchIterator_CloseReleasesInFlightDownloads reproduces issue #356:
// when the consumer closes the iterator while downloads are still in flight,
// goroutines that completed their HTTP fetch get permanently blocked sending
// to the unbuffered resultChan. They retain the downloaded buffers (Arrow
// allocations in earlier versions, raw bytes in current code) until process
// exit, producing a heap plateau that only releases on restart.
//
// The test schedules many concurrent downloads, lets them complete, and then
// closes the iterator without consuming the queued results. After Close
// returns, no cloudFetchDownloadTask goroutines must remain.
func TestCloudFetchIterator_CloseReleasesInFlightDownloads(t *testing.T) {
	arrowBytes := generateMockArrowBytes(generateArrowRecord())

	// Track in-flight downloads. The server signals each request as it starts
	// and waits on a release channel so the test can hold downloads in the
	// queued-but-not-yet-consumed state before closing the iterator.
	var inFlight atomic.Int64
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inFlight.Add(1)
		<-release
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(arrowBytes)
	}))
	defer server.Close()

	const nLinks = 20
	links := make([]*cli_service.TSparkArrowResultLink, nLinks)
	for i := range links {
		links[i] = &cli_service.TSparkArrowResultLink{
			FileLink:       server.URL,
			ExpiryTime:     time.Now().Add(10 * time.Minute).Unix(),
			StartRowOffset: int64(i),
			RowCount:       1,
		}
	}

	cfg := config.WithDefaults()
	cfg.UseLz4Compression = false
	cfg.MaxDownloadThreads = 10

	bi, err := NewCloudBatchIterator(context.Background(), links, 0, nil, cfg, nil)
	assert.Nil(t, err)

	// Kick off the first batch download. The iterator schedules
	// MaxDownloadThreads concurrent fetches behind the scenes.
	go func() { _, _ = bi.Next() }()

	// Wait for all MaxDownloadThreads goroutines to be blocked inside the
	// server handler (they've issued the GET and are waiting for the body).
	assert.Eventually(t, func() bool {
		return inFlight.Load() == int64(cfg.MaxDownloadThreads)
	}, 5*time.Second, 10*time.Millisecond, "expected %d in-flight downloads", cfg.MaxDownloadThreads)

	// Release the downloads so each goroutine finishes its HTTP read and
	// attempts to send its result on the unbuffered resultChan. Only the
	// first task's result will be read (by the Next() call above); the rest
	// will be queued, blocked on the send.
	close(release)

	// Give the goroutines time to finish their HTTP work and reach the
	// channel send.
	time.Sleep(200 * time.Millisecond)

	// Close the iterator without consuming the remaining batches.
	bi.Close()

	// After Close, every cloudFetchDownloadTask goroutine must exit. We don't
	// compare against the total goroutine count because httptest keeps
	// persistent server/transport goroutines around — we look only for our
	// own download goroutines.
	assert.Eventually(t, func() bool {
		return countDownloadTaskGoroutines() == 0
	}, 5*time.Second, 50*time.Millisecond,
		"cloudFetchDownloadTask goroutines leaked after Close: have %d",
		countDownloadTaskGoroutines())
}

// TestCloudFetchIterator_CloseReleasesAfterRetry is the retry-path counterpart
// to TestCloudFetchIterator_CloseReleasesInFlightDownloads. PR #355 added
// HTTP retry to fetchBatchBytes, materially lengthening the window during
// which a download task can produce a result after the iterator has been
// closed. The result-send must therefore go through cft.sendResult so the
// ctx.Done arm fires and the goroutine exits; a regression that routes the
// final result back through `cft.resultChan <-` (e.g. a sloppy merge of
// #355 onto pre-#357 main) blocks forever and pins the buffered Arrow body
// in the heap.
//
// The server flaps once (503 then 200) to force the retry path so the task
// produces its result after at least one backoff. The first task's result
// is consumed by the foreground Next(); the remaining MaxDownloadThreads-1
// tasks have queued results that nobody is reading. bi.Close() must release
// them.
func TestCloudFetchIterator_CloseReleasesAfterRetry(t *testing.T) {
	arrowBytes := generateMockArrowBytes(generateArrowRecord())

	// Each link is requested at most twice: first attempt returns 503,
	// second returns the body. Tracked per-link via a map keyed on the
	// link's StartRowOffset so MaxDownloadThreads parallel requests stay
	// independent.
	var mu sync.Mutex
	attempts := map[string]int{}
	var inFlightSecond atomic.Int64
	release := make(chan struct{})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Use the query string injected per-link below as the key.
		key := r.URL.RawQuery
		mu.Lock()
		attempts[key]++
		n := attempts[key]
		mu.Unlock()

		if n == 1 {
			// First attempt: serve a retryable 503 so the task enters the
			// retry/backoff path.
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		// Second attempt: block until the test releases, then return
		// success. The block lets us observe all MaxDownloadThreads tasks
		// having made it past the retry and parked on the channel send.
		inFlightSecond.Add(1)
		<-release
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(arrowBytes)
	}))
	defer server.Close()

	const nLinks = 20
	links := make([]*cli_service.TSparkArrowResultLink, nLinks)
	for i := range links {
		links[i] = &cli_service.TSparkArrowResultLink{
			// Unique query string per link so the server's per-link
			// attempt counter doesn't conflate parallel requests.
			FileLink:       fmt.Sprintf("%s/?id=%d", server.URL, i),
			ExpiryTime:     time.Now().Add(10 * time.Minute).Unix(),
			StartRowOffset: int64(i),
			RowCount:       1,
		}
	}

	cfg := config.WithDefaults()
	cfg.UseLz4Compression = false
	cfg.MaxDownloadThreads = 10
	// Backoff is observable but short — the test should still complete in
	// well under a second.
	cfg.RetryMax = 3
	cfg.RetryWaitMin = 10 * time.Millisecond
	cfg.RetryWaitMax = 50 * time.Millisecond

	bi, err := NewCloudBatchIterator(context.Background(), links, 0, nil, cfg, nil)
	assert.Nil(t, err)

	// One foreground reader. It drains exactly one batch and then exits;
	// the remaining MaxDownloadThreads-1 in-flight tasks have nobody reading.
	go func() { _, _ = bi.Next() }()

	// Wait for every concurrent task to have completed its 503 retry, slept
	// the backoff, and reached its second-attempt handler (where it's
	// parked waiting for the release channel).
	assert.Eventually(t, func() bool {
		return inFlightSecond.Load() == int64(cfg.MaxDownloadThreads)
	}, 5*time.Second, 10*time.Millisecond,
		"expected %d second-attempt requests, got %d",
		cfg.MaxDownloadThreads, inFlightSecond.Load())

	// Release: every task now finishes its successful HTTP read and
	// attempts to send its result. The foreground Next() consumes one;
	// the rest are queued in cloudIPCStreamIterator.downloadTasks and the
	// goroutines park on resultChan (via sendResult).
	close(release)

	// Give the goroutines time to reach the channel send.
	time.Sleep(200 * time.Millisecond)

	// Close without draining. cft.ctx is cancelled for every queued task,
	// so each parked sendResult must unblock via its ctx.Done arm.
	bi.Close()

	// All cloudFetchDownloadTask.Run goroutines must exit.
	assert.Eventually(t, func() bool {
		return countDownloadTaskGoroutines() == 0
	}, 5*time.Second, 50*time.Millisecond,
		"cloudFetchDownloadTask goroutines leaked after Close on retry path: have %d",
		countDownloadTaskGoroutines())
}

// countDownloadTaskGoroutines returns the number of live goroutines whose
// stack includes cloudFetchDownloadTask.Run. Used to detect the leak in
// issue #356.
func countDownloadTaskGoroutines() int {
	buf := make([]byte, 64*1024)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	return strings.Count(string(buf), "cloudFetchDownloadTask).Run")
}

// fakePositionedIPCIterator is a test IPCStreamIterator that also implements
// positionedIPCStreamIterator, so it exercises the CloudFetch row-count-capping
// path through NewBatchIterator without real CloudFetch downloads.
type fakePositionedIPCIterator struct {
	data           []byte
	startRowOffset int64
	expectedRows   int64
	consumed       bool
}

var _ IPCStreamIterator = (*fakePositionedIPCIterator)(nil)
var _ positionedIPCStreamIterator = (*fakePositionedIPCIterator)(nil)

func (f *fakePositionedIPCIterator) Next() (io.Reader, error) {
	r, _, _, err := f.NextWithMetadata()
	return r, err
}
func (f *fakePositionedIPCIterator) NextWithMetadata() (io.Reader, int64, int64, error) {
	if f.consumed {
		return nil, 0, 0, io.EOF
	}
	f.consumed = true
	return bytes.NewReader(f.data), f.startRowOffset, f.expectedRows, nil
}
func (f *fakePositionedIPCIterator) HasNext() bool { return !f.consumed }
func (f *fakePositionedIPCIterator) Close()        {}

// fakePlainIPCIterator implements only IPCStreamIterator (the inline/local
// shape) so the cap must never apply to it.
type fakePlainIPCIterator struct {
	data     []byte
	consumed bool
}

var _ IPCStreamIterator = (*fakePlainIPCIterator)(nil)

func (f *fakePlainIPCIterator) Next() (io.Reader, error) {
	if f.consumed {
		return nil, io.EOF
	}
	f.consumed = true
	return bytes.NewReader(f.data), nil
}
func (f *fakePlainIPCIterator) HasNext() bool { return !f.consumed }
func (f *fakePlainIPCIterator) Close()        {}

// TestBatchIterator_RowCountCap covers the batchIterator -> limitArrowRecords
// integration (#371 review F1/F6). generateMockArrowBytes writes the 3-row
// record twice, so each stream decodes to 6 rows.
func TestBatchIterator_RowCountCap(t *testing.T) {
	const decoded = 6
	const startOffset int64 = 100

	t.Run("positioned: caps padding rows down to RowCount", func(t *testing.T) {
		it := &fakePositionedIPCIterator{data: generateMockArrowBytes(generateArrowRecord()), startRowOffset: startOffset, expectedRows: 4}
		bi := NewBatchIterator(it, startOffset)
		batch, err := bi.Next()
		assert.NoError(t, err)
		defer batch.Close()
		assert.Equal(t, int64(4), batch.Count(), "batch should be capped to RowCount")
		assert.Equal(t, startOffset, batch.Start(), "batch must anchor at the server offset")
	})

	t.Run("positioned: exact boundary keeps all rows", func(t *testing.T) {
		it := &fakePositionedIPCIterator{data: generateMockArrowBytes(generateArrowRecord()), startRowOffset: startOffset, expectedRows: decoded}
		bi := NewBatchIterator(it, startOffset)
		batch, err := bi.Next()
		assert.NoError(t, err)
		defer batch.Close()
		assert.Equal(t, int64(decoded), batch.Count())
	})

	t.Run("positioned: RowCount larger than decoded keeps all rows", func(t *testing.T) {
		it := &fakePositionedIPCIterator{data: generateMockArrowBytes(generateArrowRecord()), startRowOffset: startOffset, expectedRows: 100}
		bi := NewBatchIterator(it, startOffset)
		batch, err := bi.Next()
		assert.NoError(t, err)
		defer batch.Close()
		assert.Equal(t, int64(decoded), batch.Count())
	})

	t.Run("positioned: RowCount==0 is NOT trusted, keeps all rows (F1)", func(t *testing.T) {
		it := &fakePositionedIPCIterator{data: generateMockArrowBytes(generateArrowRecord()), startRowOffset: startOffset, expectedRows: 0}
		bi := NewBatchIterator(it, startOffset)
		batch, err := bi.Next()
		assert.NoError(t, err)
		defer batch.Close()
		assert.Equal(t, int64(decoded), batch.Count(), "RowCount==0 must not silently drop the batch")
	})

	t.Run("plain/inline iterator is never capped", func(t *testing.T) {
		it := &fakePlainIPCIterator{data: generateMockArrowBytes(generateArrowRecord())}
		bi := NewBatchIterator(it, startOffset)
		batch, err := bi.Next()
		assert.NoError(t, err)
		defer batch.Close()
		assert.Equal(t, int64(decoded), batch.Count(), "inline path must return all decoded rows")
		assert.Equal(t, startOffset, batch.Start())
	})
}
