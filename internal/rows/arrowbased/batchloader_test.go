package arrowbased

import (
	"bytes"
	"context"
	"fmt"
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

	// ES-1892645: Cloud Fetch must retry transient S3 errors. Without retry,
	// a single 503 SlowDown (which AWS guarantees will occur with non-trivial
	// frequency on large result sets) aborts the entire query. The downstream
	// customer hit this on queries with 3,800-6,000 result files.

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
		// First attempt sees a not-yet-expired link, gets 503, sleeps. The
		// retry sleep (≥ retryWaitMin/2 = 1s with equal jitter) crosses a
		// Unix-second boundary, so the second iteration finds the link
		// expired and short-circuits. We expect exactly one HTTP attempt
		// followed by ErrLinkExpired — not all retries exhausted.
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
		// waitMin=2s → equal jitter gives sleep ∈ [1s, 2s), guaranteed to
		// cross at least one Unix-second tick.
		cfg.RetryWaitMin = 2 * time.Second
		cfg.RetryWaitMax = 4 * time.Second

		bi, err := NewCloudBatchIterator(
			context.Background(),
			[]*cli_service.TSparkArrowResultLink{{
				FileLink:       server.URL,
				ExpiryTime:     time.Now().Unix(), // floor(now); expires within the next second
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
		// Only the first attempt should have hit the server.
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

func TestCloudFetchBackoff(t *testing.T) {
	t.Run("retry-after integer seconds is honored", func(t *testing.T) {
		got := cloudFetchBackoff(1, 100*time.Millisecond, 60*time.Second, "2")
		assert.Equal(t, 2*time.Second, got)
	})

	t.Run("retry-after is capped at waitMax", func(t *testing.T) {
		got := cloudFetchBackoff(1, 100*time.Millisecond, 1*time.Second, "100")
		assert.Equal(t, 1*time.Second, got)
	})

	t.Run("retry-after http-date is ignored, falls back to exponential", func(t *testing.T) {
		minWait := 100 * time.Millisecond
		got := cloudFetchBackoff(1, minWait, 10*time.Second, "Tue, 15 Nov 1994 08:12:31 GMT")
		// attempt=1 base = minWait; equal jitter in [minWait/2, minWait]
		assert.GreaterOrEqual(t, got, minWait/2)
		assert.LessOrEqual(t, got, minWait)
	})

	t.Run("exponential is capped at waitMax", func(t *testing.T) {
		maxWait := 200 * time.Millisecond
		// 100ms * 2^9 = 51200ms, capped at 200ms; equal jitter -> [100ms, 200ms]
		for i := 0; i < 50; i++ {
			got := cloudFetchBackoff(10, 100*time.Millisecond, maxWait, "")
			assert.GreaterOrEqual(t, got, maxWait/2)
			assert.LessOrEqual(t, got, maxWait)
		}
	})

	t.Run("base grows exponentially with attempt", func(t *testing.T) {
		minWait, maxWait := 100*time.Millisecond, 10*time.Second
		// attempt=1 -> base 100ms,  jitter [50ms,100ms]
		// attempt=3 -> base 400ms,  jitter [200ms,400ms]
		for i := 0; i < 50; i++ {
			got1 := cloudFetchBackoff(1, minWait, maxWait, "")
			got3 := cloudFetchBackoff(3, minWait, maxWait, "")
			assert.GreaterOrEqual(t, got1, 50*time.Millisecond)
			assert.LessOrEqual(t, got1, 100*time.Millisecond)
			assert.GreaterOrEqual(t, got3, 200*time.Millisecond)
			assert.LessOrEqual(t, got3, 400*time.Millisecond)
		}
	})

	t.Run("zero waitMin returns zero", func(t *testing.T) {
		got := cloudFetchBackoff(1, 0, 0, "")
		assert.Equal(t, time.Duration(0), got)
	})
}

func TestCloudFetchRetryableStatus(t *testing.T) {
	retryable := []int{408, 429, 500, 502, 503, 504}
	notRetryable := []int{200, 201, 301, 302, 400, 401, 403, 404, 409, 410, 501}

	for _, s := range retryable {
		assert.True(t, isCloudFetchRetryableStatus(s), "%d should be retryable", s)
	}
	for _, s := range notRetryable {
		assert.False(t, isCloudFetchRetryableStatus(s), "%d should not be retryable", s)
	}
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
