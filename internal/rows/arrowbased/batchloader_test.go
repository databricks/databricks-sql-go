package arrowbased

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
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
			cfg,
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
			cfg,
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
			cfg,
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

	t.Run("should use custom Transport when provided", func(t *testing.T) {
		handler = func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write(generateMockArrowBytes(generateArrowRecord()))
		}

		startRowOffset := int64(100)
		customTransport := &http.Transport{MaxIdleConns: 10}

		cfg := config.WithDefaults()
		cfg.UseLz4Compression = false
		cfg.MaxDownloadThreads = 1
		cfg.UserConfig.Transport = customTransport

		bi, err := NewCloudBatchIterator(
			context.Background(),
			[]*cli_service.TSparkArrowResultLink{{
				FileLink:       server.URL,
				ExpiryTime:     time.Now().Add(10 * time.Minute).Unix(),
				StartRowOffset: startRowOffset,
				RowCount:       1,
			}},
			startRowOffset,
			cfg,
		)
		assert.Nil(t, err)

		cbi := bi.(*batchIterator).ipcIterator.(*cloudIPCStreamIterator)
		assert.NotNil(t, cbi.httpClient)
		assert.Equal(t, customTransport, cbi.httpClient.Transport)

		// Verify fetch works
		sab, nextErr := bi.Next()
		assert.Nil(t, nextErr)
		assert.NotNil(t, sab)
	})

	t.Run("should fallback to http.DefaultClient when Transport is nil", func(t *testing.T) {
		handler = func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write(generateMockArrowBytes(generateArrowRecord()))
		}

		startRowOffset := int64(100)
		cfg := config.WithDefaults()
		cfg.UseLz4Compression = false
		cfg.MaxDownloadThreads = 1

		bi, err := NewCloudBatchIterator(
			context.Background(),
			[]*cli_service.TSparkArrowResultLink{{
				FileLink:       server.URL,
				ExpiryTime:     time.Now().Add(10 * time.Minute).Unix(),
				StartRowOffset: startRowOffset,
				RowCount:       1,
			}},
			startRowOffset,
			cfg,
		)
		assert.Nil(t, err)

		cbi := bi.(*batchIterator).ipcIterator.(*cloudIPCStreamIterator)
		assert.Equal(t, http.DefaultClient, cbi.httpClient)

		// Verify fetch works with default client
		sab, nextErr := bi.Next()
		assert.Nil(t, nextErr)
		assert.NotNil(t, sab)
	})
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
