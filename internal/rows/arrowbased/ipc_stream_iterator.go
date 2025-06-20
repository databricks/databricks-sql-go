package arrowbased

import (
	"bytes"
	"context"
	"io"

	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/databricks/databricks-sql-go/internal/rows/rowscanner"
	dbsqlrows "github.com/databricks/databricks-sql-go/rows"
	"github.com/pierrec/lz4/v4"
)

// ipcStreamIterator provides access to raw Arrow IPC streams without deserialization
type ipcStreamIterator struct {
	ctx                context.Context
	resultPageIterator rowscanner.ResultPageIterator
	currentBatches     []*cli_service.TSparkArrowBatch
	currentIndex       int
	arrowSchemaBytes   []byte
	useLz4             bool
	hasMorePages       bool
}

// NewIPCStreamIterator creates an iterator that returns raw IPC streams
func NewIPCStreamIterator(
	ctx context.Context,
	resultPageIterator rowscanner.ResultPageIterator,
	initialRowSet *cli_service.TRowSet,
	schemaBytes []byte,
	cfg *config.Config,
) (dbsqlrows.IPCStreamIterator, error) {
	var useLz4 bool
	if cfg != nil {
		useLz4 = cfg.UseLz4Compression
	}

	var batches []*cli_service.TSparkArrowBatch
	if initialRowSet != nil {
		batches = initialRowSet.ArrowBatches
	}

	return &ipcStreamIterator{
		ctx:                ctx,
		resultPageIterator: resultPageIterator,
		currentBatches:     batches,
		currentIndex:       0,
		arrowSchemaBytes:   schemaBytes,
		useLz4:             useLz4,
		hasMorePages:       resultPageIterator != nil && resultPageIterator.HasNext(),
	}, nil
}

// NextIPCStream returns the next Arrow batch as a raw IPC stream
func (it *ipcStreamIterator) NextIPCStream() (io.Reader, error) {
	// Check if we need to load more batches from the next page
	if it.currentIndex >= len(it.currentBatches) {
		if !it.hasMorePages || it.resultPageIterator == nil {
			return nil, io.EOF
		}

		// Fetch next page
		fetchResult, err := it.resultPageIterator.Next()
		if err != nil {
			return nil, err
		}

		if fetchResult == nil || fetchResult.Results == nil || fetchResult.Results.ArrowBatches == nil {
			return nil, io.EOF
		}

		it.currentBatches = fetchResult.Results.ArrowBatches
		it.currentIndex = 0
		it.hasMorePages = it.resultPageIterator.HasNext()

		// If no batches in this page, recurse to try next page
		if len(it.currentBatches) == 0 {
			return it.NextIPCStream()
		}
	}

	batch := it.currentBatches[it.currentIndex]
	it.currentIndex++

	// Create reader for the batch data
	var batchReader io.Reader = bytes.NewReader(batch.Batch)

	// Handle LZ4 decompression if needed
	if it.useLz4 {
		batchReader = lz4.NewReader(batchReader)
	}

	// Combine schema and batch data into a complete IPC stream
	// Arrow IPC format expects: [Schema][Batch1][Batch2]...
	return io.MultiReader(
		bytes.NewReader(it.arrowSchemaBytes),
		batchReader,
	), nil
}

// HasNext returns true if there are more batches
func (it *ipcStreamIterator) HasNext() bool {
	return it.currentIndex < len(it.currentBatches) || it.hasMorePages
}

// Close releases any resources
func (it *ipcStreamIterator) Close() {
	// Nothing to close for this implementation
}

// GetSchemaBytes returns the Arrow schema in IPC format
func (it *ipcStreamIterator) GetSchemaBytes() ([]byte, error) {
	return it.arrowSchemaBytes, nil
}
