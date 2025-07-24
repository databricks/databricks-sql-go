package rows

import (
	"context"
	"io"

	"github.com/apache/arrow/go/v12/arrow"
)

type Rows interface {
	GetArrowBatches(context.Context) (ArrowBatchIterator, error)
	GetArrowIPCStreams(context.Context) (ArrowIPCStreamIterator, error)
}

type ArrowBatchIterator interface {
	// Retrieve the next arrow.Record.
	// Will return io.EOF if there are no more records
	Next() (arrow.Record, error)

	// Return true if the iterator contains more batches, false otherwise.
	HasNext() bool

	// Release any resources in use by the iterator.
	Close()

	// Return the schema of the records.
	Schema() (*arrow.Schema, error)
}

type ArrowIPCStreamIterator interface {
	// Retrieve the next Arrow IPC stream as an io.Reader.
	// Will return io.EOF if there are no more streams
	Next() (io.Reader, error)

	// Return true if the iterator contains more streams, false otherwise.
	HasNext() bool

	// Release any resources in use by the iterator.
	Close()

	// Return the Arrow schema bytes for the streams.
	SchemaBytes() ([]byte, error)
}
