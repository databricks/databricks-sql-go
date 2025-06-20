package rows

import (
	"io"
)

// IPCStreamIterator provides access to raw Arrow IPC streams
type IPCStreamIterator interface {
	// GetNextIPCStream returns the next Arrow batch as an IPC stream reader
	// Returns io.EOF when no more batches are available
	NextIPCStream() (io.Reader, error)

	// HasNext returns true if there are more batches
	HasNext() bool

	// Close releases any resources
	Close()

	// GetSchemaBytes returns the Arrow schema in IPC format
	GetSchemaBytes() ([]byte, error)
}

// Extension to existing Rows interface
type RowsWithIPCStream interface {
	Rows
	// GetIPCStreams returns an iterator for raw Arrow IPC streams
	GetIPCStreams() (IPCStreamIterator, error)
}
