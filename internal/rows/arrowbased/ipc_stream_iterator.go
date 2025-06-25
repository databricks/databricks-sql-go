package arrowbased

import (
	"bytes"
	"io"

	"github.com/databricks/databricks-sql-go/internal/config"
	dbsqlrows "github.com/databricks/databricks-sql-go/rows"
	"github.com/pierrec/lz4/v4"
)

// ipcStreamIterator provides access to raw Arrow IPC streams without deserialization
type ipcStreamIterator struct {
	rawBatchIterator RawBatchIterator
	arrowSchemaBytes []byte
	useLz4           bool
}

// NewIPCStreamIterator creates an iterator that returns raw IPC streams
func NewIPCStreamIterator(
	rawIterator RawBatchIterator,
	schemaBytes []byte,
	cfg *config.Config,
) dbsqlrows.IPCStreamIterator {
	var useLz4 bool
	if cfg != nil {
		useLz4 = cfg.UseLz4Compression
	}

	return &ipcStreamIterator{
		rawBatchIterator: rawIterator,
		arrowSchemaBytes: schemaBytes,
		useLz4:           useLz4,
	}
}

// NextIPCStream returns the next Arrow batch as a raw IPC stream
func (it *ipcStreamIterator) NextIPCStream() (io.Reader, error) {
	rawBatch, err := it.rawBatchIterator.Next()
	if err != nil {
		return nil, err
	}

	// Create reader for the batch data
	var batchReader io.Reader = bytes.NewReader(rawBatch.Batch)

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
	return it.rawBatchIterator.HasNext()
}

// Close releases any resources
func (it *ipcStreamIterator) Close() {
	it.rawBatchIterator.Close()
}

// GetSchemaBytes returns the Arrow schema in IPC format
func (it *ipcStreamIterator) GetSchemaBytes() ([]byte, error) {
	return it.arrowSchemaBytes, nil
}
