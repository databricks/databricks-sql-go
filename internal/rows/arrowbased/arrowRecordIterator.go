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
	dbsqlerr "github.com/databricks/databricks-sql-go/internal/errors"
	"github.com/databricks/databricks-sql-go/internal/rows/rowscanner"
	"github.com/databricks/databricks-sql-go/rows"
)

func NewArrowRecordIterator(ctx context.Context, rpi rowscanner.ResultPageIterator, bi BatchIterator, arrowSchemaBytes []byte, cfg config.Config) rows.ArrowBatchIterator {
	ari := arrowRecordIterator{
		cfg:                cfg,
		batchIterator:      bi,
		resultPageIterator: rpi,
		ctx:                ctx,
		arrowSchemaBytes:   arrowSchemaBytes,
	}

	return &ari

}

// A type implemented DBSQLArrowBatchIterator
type arrowRecordIterator struct {
	ctx                context.Context
	cfg                config.Config
	batchIterator      BatchIterator
	resultPageIterator rowscanner.ResultPageIterator
	currentBatch       SparkArrowBatch
	isFinished         bool
	arrowSchemaBytes   []byte
	arrowSchema        *arrow.Schema
	// err holds a page-fetch error detected while advancing the result page
	// iterator. It must be surfaced through Next() rather than being masked as
	// a normal end-of-results (io.EOF). See issue #507. Once captured the
	// iterator is finished, so checkFinished never re-captures it from the
	// result page iterator.
	err error
}

var _ rows.ArrowBatchIterator = (*arrowRecordIterator)(nil)

// Retrieve the next arrow record
func (ri *arrowRecordIterator) Next() (arrow.Record, error) {
	if !ri.HasNext() {
		// returning EOF indicates that there are no more records to iterate
		return nil, io.EOF
	}

	// If advancing the result page failed, surface that error rather than
	// masking a failed page fetch as a normal end-of-results. See #507. The
	// error is reported once and then the iterator is finished.
	if err := ri.takePendingErr(); err != nil {
		return nil, err
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

// takePendingErr returns the pending page-fetch error, if any, and marks it as
// reported so it is surfaced exactly once; the iterator is then finished.
func (ri *arrowRecordIterator) takePendingErr() error {
	if ri.err == nil {
		return nil
	}
	err := ri.err
	ri.err = nil
	ri.Close()
	return err
}

// Indicate whether there are any more records available
func (ri *arrowRecordIterator) HasNext() bool {
	ri.checkFinished()
	// A pending page-fetch error still needs to be reported via Next(), so the
	// iterator is not considered exhausted until that error is consumed.
	return !ri.isFinished || ri.err != nil
}

// Free any resources associated with this iterator. An explicit Close also
// discards any pending, not-yet-reported page-fetch error so a closed iterator
// reports HasNext()==false and Next()==io.EOF.
func (ri *arrowRecordIterator) Close() {
	ri.err = nil
	ri.release()
}

// release frees the iterator's resources and marks it finished without
// touching any pending page-fetch error, so that error can still be surfaced
// via Next() when the end of the result set is reached internally.
func (ri *arrowRecordIterator) release() {
	if !ri.isFinished {
		ri.isFinished = true
		if ri.currentBatch != nil {
			ri.currentBatch.Close()
		}

		if ri.batchIterator != nil {
			ri.batchIterator.Close()
		}

		if ri.resultPageIterator != nil {
			ri.resultPageIterator.Close() //nolint:errcheck,gosec // G104: close in cleanup
		}
	}
}

func (ri *arrowRecordIterator) checkFinished() {
	// Once finished (including after an explicit Close()), never touch the
	// result page iterator again: its HasNext() would issue a FetchResults
	// round-trip against an operation that may already be closed.
	if ri.isFinished {
		return
	}

	noLocalData := (ri.currentBatch == nil || !ri.currentBatch.HasNext()) &&
		(ri.batchIterator == nil || !ri.batchIterator.HasNext())

	// resultPageIterator.HasNext() issues a real FetchResults round-trip, so it
	// is evaluated lazily — only once all local data is drained — to avoid
	// eagerly prefetching the next page while the current one is still in use.
	noMoreData := noLocalData &&
		(ri.resultPageIterator == nil || !ri.resultPageIterator.HasNext())

	// If the result page iterator reported no more pages because a page fetch
	// failed (rather than because the result set was fully consumed), hold on
	// to that error so Next() can surface it instead of io.EOF. See #507.
	if noMoreData && ri.resultPageIterator != nil && ri.err == nil {
		ri.err = ri.resultPageIterator.Err()
	}

	if noMoreData {
		// Reached end of result set so release resources, keeping any
		// pending page-fetch error for Next() to report.
		ri.release()
	}
}

// Update the current batch if necessary
func (ri *arrowRecordIterator) getCurrentBatch() error {

	// only need to update if no current batch or current batch has no more records
	if ri.currentBatch == nil || !ri.currentBatch.HasNext() {

		// ensure up to date batch iterator
		err := ri.getBatchIterator()
		if err != nil {
			return err
		}

		// release current batch
		if ri.currentBatch != nil {
			ri.currentBatch.Close()
		}

		// Get next batch from batch iterator
		ri.currentBatch, err = ri.batchIterator.Next()
		if err != nil {
			return err
		}
	}

	return nil
}

// Update batch iterator if necessary
func (ri *arrowRecordIterator) getBatchIterator() error {
	// only need to update if there is no batch iterator or the
	// batch iterator has no more batches
	if ri.batchIterator == nil || !ri.batchIterator.HasNext() {
		if ri.batchIterator != nil {
			// release any resources held by the current batch iterator
			ri.batchIterator.Close()
			ri.batchIterator = nil
		}

		// Get the next page of the result set
		resp, err := ri.resultPageIterator.Next()
		if err != nil {
			return err
		}

		// Check the result format
		resultFormat := resp.ResultSetMetadata.GetResultFormat()
		if resultFormat != cli_service.TSparkRowSetType_ARROW_BASED_SET && resultFormat != cli_service.TSparkRowSetType_URL_BASED_SET {
			return dbsqlerr.NewDriverError(ri.ctx, errArrowRowsNotArrowFormat, nil)
		}

		if ri.arrowSchemaBytes == nil {
			ri.arrowSchemaBytes = resp.ResultSetMetadata.ArrowSchema
		}

		// Create a new batch iterator for the batches in the result page
		bi, err := ri.newBatchIterator(resp)
		if err != nil {
			return err
		}

		ri.batchIterator = bi
	}

	return nil
}

// Create a new batch iterator from a page of the result set
func (ri *arrowRecordIterator) newBatchIterator(fr *cli_service.TFetchResultsResp) (BatchIterator, error) {
	rowSet := fr.Results
	if len(rowSet.ResultLinks) > 0 {
		return NewCloudBatchIterator(ri.ctx, rowSet.ResultLinks, rowSet.StartRowOffset, ri.arrowSchemaBytes, &ri.cfg, nil)
	} else {
		return NewLocalBatchIterator(ri.ctx, rowSet.ArrowBatches, rowSet.StartRowOffset, ri.arrowSchemaBytes, &ri.cfg)
	}
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
			// A pending page-fetch error is reported here (once) rather than
			// via getCurrentBatch, so a later Next() does not repeat it.
			if err := ri.takePendingErr(); err != nil {
				return nil, err
			}
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
