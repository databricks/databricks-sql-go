package arrowbased

import (
	"context"
	"io"

	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/config"
	dbsqlerrint "github.com/databricks/databricks-sql-go/internal/errors"
	"github.com/databricks/databricks-sql-go/internal/rows/rowscanner"
	"github.com/databricks/databricks-sql-go/rows"
)

// NewArrowIPCStreamIterator creates a new iterator for Arrow IPC streams
func NewArrowIPCStreamIterator(ctx context.Context, rpi rowscanner.ResultPageIterator, ipcIterator IPCStreamIterator, arrowSchemaBytes []byte, cfg config.Config) rows.ArrowIPCStreamIterator {
	return &arrowIPCStreamIterator{
		cfg:                cfg,
		ipcStreamIterator:  ipcIterator,
		resultPageIterator: rpi,
		ctx:                ctx,
		arrowSchemaBytes:   arrowSchemaBytes,
	}
}

// arrowIPCStreamIterator implements rows.ArrowIPCStreamIterator
type arrowIPCStreamIterator struct {
	ctx                context.Context
	cfg                config.Config
	ipcStreamIterator  IPCStreamIterator
	resultPageIterator rowscanner.ResultPageIterator
	isFinished         bool
	arrowSchemaBytes   []byte
}

var _ rows.ArrowIPCStreamIterator = (*arrowIPCStreamIterator)(nil)

// Next retrieves the next Arrow IPC stream
func (ri *arrowIPCStreamIterator) Next() (io.Reader, error) {
	if !ri.HasNext() {
		return nil, io.EOF
	}

	if ri.ipcStreamIterator != nil && ri.ipcStreamIterator.HasNext() {
		return ri.ipcStreamIterator.Next()
	}

	// If there is no iterator, or we have exhausted the current iterator, try to load more data
	if err := ri.fetchNextData(); err != nil {
		return nil, err
	}

	// Try again after fetching new data
	if ri.ipcStreamIterator != nil && ri.ipcStreamIterator.HasNext() {
		return ri.ipcStreamIterator.Next()
	}

	return nil, io.EOF
}

// HasNext returns true if there are more streams available
func (ri *arrowIPCStreamIterator) HasNext() bool {
	if ri.isFinished {
		return false
	}

	if ri.ipcStreamIterator != nil && ri.ipcStreamIterator.HasNext() {
		return true
	}

	if ri.resultPageIterator == nil || !ri.resultPageIterator.HasNext() {
		return false
	}

	return true
}

// Close releases resources
func (ri *arrowIPCStreamIterator) Close() {
	if ri.ipcStreamIterator != nil {
		ri.ipcStreamIterator.Close()
		ri.ipcStreamIterator = nil
	}
	ri.isFinished = true
}

// SchemaBytes returns the Arrow schema bytes
func (ri *arrowIPCStreamIterator) SchemaBytes() ([]byte, error) {
	return ri.arrowSchemaBytes, nil
}

// fetchNextData loads the next page of data
func (ri *arrowIPCStreamIterator) fetchNextData() error {
	if ri.isFinished {
		return io.EOF
	}

	// First close any existing iterator
	if ri.ipcStreamIterator != nil {
		ri.ipcStreamIterator.Close()
		ri.ipcStreamIterator = nil
	}

	if ri.resultPageIterator == nil || !ri.resultPageIterator.HasNext() {
		ri.isFinished = true
		return io.EOF
	}

	// Get the next page of the result set
	resp, err := ri.resultPageIterator.Next()
	if err != nil {
		ri.isFinished = true
		return err
	}

	// Check the result format
	resultFormat := resp.ResultSetMetadata.GetResultFormat()
	if resultFormat != cli_service.TSparkRowSetType_ARROW_BASED_SET && resultFormat != cli_service.TSparkRowSetType_URL_BASED_SET {
		return dbsqlerrint.NewDriverError(ri.ctx, errArrowRowsNotArrowFormat, nil)
	}

	// Update schema if this is the first fetch
	if ri.arrowSchemaBytes == nil && resp.ResultSetMetadata != nil && resp.ResultSetMetadata.ArrowSchema != nil {
		ri.arrowSchemaBytes = resp.ResultSetMetadata.ArrowSchema
	}

	// Create new iterator from the fetched data
	bi, err := ri.newIPCStreamIterator(resp)
	if err != nil {
		ri.isFinished = true
		return err
	}

	ri.ipcStreamIterator = bi
	return nil
}

// Create a new IPC stream iterator from a page of the result set
func (ri *arrowIPCStreamIterator) newIPCStreamIterator(fr *cli_service.TFetchResultsResp) (IPCStreamIterator, error) {
	rowSet := fr.Results
	if len(rowSet.ResultLinks) > 0 {
		return NewCloudIPCStreamIterator(ri.ctx, rowSet.ResultLinks, rowSet.StartRowOffset, &ri.cfg)
	} else {
		return NewLocalIPCStreamIterator(ri.ctx, rowSet.ArrowBatches, rowSet.StartRowOffset, ri.arrowSchemaBytes, &ri.cfg)
	}
}
