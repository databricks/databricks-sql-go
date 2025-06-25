package arrowbased

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/ipc"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/databricks/databricks-sql-go/internal/rows/rowscanner"
	"github.com/databricks/databricks-sql-go/rows"
)

func NewArrowRecordIterator(ctx context.Context, rpi rowscanner.ResultPageIterator, bi BatchIterator, arrowSchemaBytes []byte, cfg config.Config) rows.ArrowBatchIterator {
	ari := arrowRecordIterator{
		cfg:              cfg,
		ctx:              ctx,
		arrowSchemaBytes: arrowSchemaBytes,
	}

	if bi != nil && rpi != nil {
		// Both initial batch iterator and result page iterator
		// Extract the raw iterator from the initial batch iterator and create a composite
		if batchIter, ok := bi.(*batchIterator); ok {
			pagedRaw := &pagedRawBatchIterator{
				ctx:                ctx,
				resultPageIterator: rpi,
				cfg:                &cfg,
				startRowOffset:     0,
			}
			compositeRaw := NewInitialThenPagedRawIterator(batchIter.rawIterator, pagedRaw)
			ari.batchIterator = NewBatchIterator(compositeRaw, arrowSchemaBytes, &cfg)
		} else {
			// Fallback: use initial batch iterator, ignore pagination for now
			ari.batchIterator = bi
		}
	} else if bi != nil {
		// Only initial batch iterator
		ari.batchIterator = bi
	} else if rpi != nil {
		// Only result page iterator
		pagedRawIter := &pagedRawBatchIterator{
			ctx:                ctx,
			resultPageIterator: rpi,
			cfg:                &cfg,
			startRowOffset:     0,
		}
		ari.batchIterator = NewBatchIterator(pagedRawIter, arrowSchemaBytes, &cfg)
	}

	return &ari
}

// A type implemented DBSQLArrowBatchIterator
type arrowRecordIterator struct {
	ctx              context.Context
	cfg              config.Config
	batchIterator    BatchIterator
	currentBatch     SparkArrowBatch
	isFinished       bool
	arrowSchemaBytes []byte
	arrowSchema      *arrow.Schema
}

var _ rows.ArrowBatchIterator = (*arrowRecordIterator)(nil)

// Retrieve the next arrow record
func (ri *arrowRecordIterator) Next() (arrow.Record, error) {
	if !ri.HasNext() {
		// returning EOF indicates that there are no more records to iterate
		return nil, io.EOF
	}

	// make sure we have the current batch
	err := ri.getCurrentBatch()
	if err != nil {
		return nil, err
	}

	// return next record in current batch
	r, err := ri.currentBatch.Next()

	ri.checkFinished()

	return r, err
}

// Indicate whether there are any more records available
func (ri *arrowRecordIterator) HasNext() bool {
	ri.checkFinished()
	return !ri.isFinished
}

// Free any resources associated with this iterator
func (ri *arrowRecordIterator) Close() {
	if !ri.isFinished {
		ri.isFinished = true
		if ri.currentBatch != nil {
			ri.currentBatch.Close()
		}

		if ri.batchIterator != nil {
			ri.batchIterator.Close()
		}
	}
}

func (ri *arrowRecordIterator) checkFinished() {
	finished := ri.isFinished ||
		((ri.currentBatch == nil || !ri.currentBatch.HasNext()) &&
			(ri.batchIterator == nil || !ri.batchIterator.HasNext()))

	if finished {
		// Reached end of result set so Close
		ri.Close()
	}
}

// Update the current batch if necessary
func (ri *arrowRecordIterator) getCurrentBatch() error {
	// only need to update if no current batch or current batch has no more records
	if ri.currentBatch == nil || !ri.currentBatch.HasNext() {
		// release current batch
		if ri.currentBatch != nil {
			ri.currentBatch.Close()
		}

		// Get next batch from batch iterator
		if ri.batchIterator == nil {
			return io.EOF
		}

		var err error
		ri.currentBatch, err = ri.batchIterator.Next()
		if err != nil {
			return err
		}

		// Update schema bytes if we don't have them yet and the batch iterator got them
		if ri.arrowSchemaBytes == nil {
			if batchIter, ok := ri.batchIterator.(*batchIterator); ok {
				if pagedIter, ok := batchIter.rawIterator.(*pagedRawBatchIterator); ok {
					if schemaBytes := pagedIter.GetSchemaBytes(); schemaBytes != nil {
						ri.arrowSchemaBytes = schemaBytes
					}
				}
			}
		}
	}

	return nil
}

// Return the schema of the records.
func (ri *arrowRecordIterator) Schema() (*arrow.Schema, error) {
	// Return cached schema if available
	if ri.arrowSchema != nil {
		return ri.arrowSchema, nil
	}

	// Try to get schema bytes if not already available
	if ri.arrowSchemaBytes == nil {
		if ri.HasNext() {
			if err := ri.getCurrentBatch(); err != nil {
				return nil, err
			}
		}

		// If still no schema bytes, we can't create a schema
		if ri.arrowSchemaBytes == nil {
			return nil, fmt.Errorf("no schema available")
		}
	}

	// Convert schema bytes to Arrow schema
	reader, err := ipc.NewReader(bytes.NewReader(ri.arrowSchemaBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create Arrow IPC reader: %w", err)
	}
	defer reader.Release()

	// Cache and return the schema
	ri.arrowSchema = reader.Schema()
	return ri.arrowSchema, nil
}

// InitialThenPagedRawIterator handles initial raw iterator first, then paged raw iterator
type InitialThenPagedRawIterator struct {
	InitialRaw RawBatchIterator
	PagedRaw   RawBatchIterator
}

// NewInitialThenPagedRawIterator creates a composite iterator
func NewInitialThenPagedRawIterator(initial, paged RawBatchIterator) RawBatchIterator {
	return &InitialThenPagedRawIterator{
		InitialRaw: initial,
		PagedRaw:   paged,
	}
}

func (i *InitialThenPagedRawIterator) Next() (*cli_service.TSparkArrowBatch, error) {
	if i.InitialRaw != nil && i.InitialRaw.HasNext() {
		return i.InitialRaw.Next()
	}
	if i.PagedRaw != nil {
		return i.PagedRaw.Next()
	}
	return nil, io.EOF
}

func (i *InitialThenPagedRawIterator) HasNext() bool {
	return (i.InitialRaw != nil && i.InitialRaw.HasNext()) ||
		(i.PagedRaw != nil && i.PagedRaw.HasNext())
}

func (i *InitialThenPagedRawIterator) Close() {
	if i.InitialRaw != nil {
		i.InitialRaw.Close()
	}
	if i.PagedRaw != nil {
		i.PagedRaw.Close()
	}
}

func (i *InitialThenPagedRawIterator) GetStartRowOffset() int64 {
	if i.InitialRaw != nil && i.InitialRaw.HasNext() {
		return i.InitialRaw.GetStartRowOffset()
	}
	if i.PagedRaw != nil {
		return i.PagedRaw.GetStartRowOffset()
	}
	return 0
}
