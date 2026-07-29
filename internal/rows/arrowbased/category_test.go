package arrowbased

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/config"
	dbsqlerrint "github.com/databricks/databricks-sql-go/internal/errors"
	"github.com/stretchr/testify/assert"
)

// A CloudFetch download failure must surface from the batch iterator carrying
// CategoryChunkDownload, on the same object path the telemetry hook reads.
func TestCloudFetchDownloadErrorCarriesCategory(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
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

	bi, err := NewCloudBatchIterator(context.Background(), links, startRowOffset, nil, cfg, nil)
	assert.Nil(t, err)

	_, nextErr := bi.Next()
	assert.NotNil(t, nextErr)
	assert.Equal(t, dbsqlerrint.CategoryChunkDownload, dbsqlerrint.CategoryFromError(nextErr),
		"download failure must carry chunk_download_error so telemetry classifies it correctly")
}

// Retry-exhausted (persistent retryable status) and transport-error download
// failures also carry CategoryChunkDownload — the two branches after the retry
// loop, distinct from the non-retryable-status branch above.
func TestCloudFetchDownloadErrorCarriesCategory_RetryBranches(t *testing.T) {
	link := func(url string) []*cli_service.TSparkArrowResultLink {
		return []*cli_service.TSparkArrowResultLink{{
			FileLink:       url,
			ExpiryTime:     time.Now().Add(10 * time.Minute).Unix(),
			StartRowOffset: 0,
			RowCount:       1,
		}}
	}
	cfg := config.WithDefaults()
	cfg.UseLz4Compression = false
	cfg.MaxDownloadThreads = 1
	cfg.RetryMax = 1
	cfg.RetryWaitMin = 1 * time.Millisecond
	cfg.RetryWaitMax = 5 * time.Millisecond

	t.Run("retries exhausted on persistent 503", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
		defer server.Close()

		bi, err := NewCloudBatchIterator(context.Background(), link(server.URL), 0, nil, cfg, nil)
		assert.Nil(t, err)
		_, nextErr := bi.Next()
		assert.NotNil(t, nextErr)
		assert.Equal(t, dbsqlerrint.CategoryChunkDownload, dbsqlerrint.CategoryFromError(nextErr))
	})

	t.Run("transport error (unreachable server)", func(t *testing.T) {
		// Start then immediately close the server so the connection is refused,
		// driving the transport-error branch (lastErr set, lastStatus 0).
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
		url := server.URL
		server.Close()

		bi, err := NewCloudBatchIterator(context.Background(), link(url), 0, nil, cfg, nil)
		assert.Nil(t, err)
		_, nextErr := bi.Next()
		assert.NotNil(t, nextErr)
		assert.Equal(t, dbsqlerrint.CategoryChunkDownload, dbsqlerrint.CategoryFromError(nextErr))
	})
}

// A CloudFetch payload that fails LZ4 decompression must surface carrying
// CategoryDecompression, on the same object path the telemetry hook reads.
func TestCloudFetchDecompressionErrorCarriesCategory(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Not valid LZ4 — lz4.NewReader will fail to decompress this.
		_, _ = w.Write([]byte("this is not lz4 compressed data"))
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
	cfg.UseLz4Compression = true
	cfg.MaxDownloadThreads = 1

	bi, err := NewCloudBatchIterator(context.Background(), links, startRowOffset, nil, cfg, nil)
	assert.Nil(t, err)

	_, nextErr := bi.Next()
	assert.NotNil(t, nextErr)
	assert.Equal(t, dbsqlerrint.CategoryDecompression, dbsqlerrint.CategoryFromError(nextErr))
	// loadBatchFor (arrowRows.go) uses a direct, non-walking assertion; the
	// outermost error must itself be a DBError or the tag is re-wrapped away.
	_, ok := nextErr.(dbsqlerr.DBError)
	assert.True(t, ok, "outermost error must be a DBError to survive loadBatchFor's direct assertion")
}

// A schema-parse failure while building the row scanner must carry
// CategoryArrowSchemaParsing. Covers the schema-convert branch (broken decimal
// type) and the IPC-read branch (invalid pre-supplied arrow schema bytes).
func TestArrowSchemaParseErrorCarriesCategory(t *testing.T) {
	t.Run("schema convert failure", func(t *testing.T) {
		rowSet := &cli_service.TRowSet{ArrowBatches: []*cli_service.TSparkArrowBatch{{RowCount: 2}}}
		schema := getAllTypesSchema()
		// Break the decimal column so tTableSchemaToArrowSchema fails.
		schema.Columns[13].TypeDesc.Types[0].PrimitiveEntry.TypeQualifiers = nil
		metadataResp := getMetadataResp(schema)

		cfg := config.Config{}
		cfg.UseArrowBatches = true
		cfg.ArrowConfig.UseArrowNativeDecimal = true

		_, err := NewArrowRowScanner(metadataResp, rowSet, &cfg, nil, context.Background(), nil)
		assert.NotNil(t, err)
		assert.Equal(t, dbsqlerrint.CategoryArrowSchemaParsing, dbsqlerrint.CategoryFromError(err))
	})

	t.Run("invalid IPC arrow schema bytes", func(t *testing.T) {
		rowSet := &cli_service.TRowSet{ArrowBatches: []*cli_service.TSparkArrowBatch{{RowCount: 2}}}
		metadataResp := getMetadataResp(getAllTypesSchema())
		// A non-nil but invalid ArrowSchema forces the ipc.NewReader branch to fail.
		metadataResp.ArrowSchema = []byte("not a valid arrow ipc stream")

		_, err := NewArrowRowScanner(metadataResp, rowSet, &config.Config{}, nil, context.Background(), nil)
		assert.NotNil(t, err)
		assert.Equal(t, dbsqlerrint.CategoryArrowSchemaParsing, dbsqlerrint.CategoryFromError(err))
	})
}
