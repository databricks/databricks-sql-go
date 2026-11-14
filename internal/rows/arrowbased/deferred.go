package arrowbased

import (
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	dbsqlrows "github.com/databricks/databricks-sql-go/rows"
)

type deferredIterator[T interface {
	HasNext() bool
	Close()
}] struct {
	iterator   T
	initialize func() (T, error)
	loaded     bool
	closed     bool
	err        error
}

func (d *deferredIterator[T]) load() error {
	if d.err != nil {
		return d.err
	}
	if d.loaded {
		return nil
	}
	if d.closed {
		return io.EOF
	}
	iterator, err := d.initialize()
	if err != nil {
		d.err, d.initialize = err, nil
		return err
	}
	d.iterator, d.loaded, d.initialize = iterator, true, nil
	return nil
}

func (d *deferredIterator[T]) HasNext() bool {
	if d.closed {
		return false
	}
	if d.err != nil {
		return true
	}
	if err := d.load(); err != nil {
		d.err = err
		return true
	}
	return d.iterator.HasNext()
}

func (d *deferredIterator[T]) Close() {
	if !d.closed && d.loaded {
		d.iterator.Close()
	}
	d.closed, d.initialize = true, nil
}

func (d *deferredIterator[T]) nextError() error {
	if d.closed {
		return io.EOF
	}
	if d.err != nil {
		err := d.err
		d.Close()
		return err
	}
	if err := d.load(); err != nil {
		d.Close()
		return err
	}
	return nil
}

// DeferArrowBatches keeps result I/O in iteration and schema access, not the getter.
func DeferArrowBatches(initialize func() (dbsqlrows.ArrowBatchIterator, error)) dbsqlrows.ArrowBatchIterator {
	return &deferredBatches{deferredIterator[dbsqlrows.ArrowBatchIterator]{initialize: initialize}}
}

type deferredBatches struct {
	deferredIterator[dbsqlrows.ArrowBatchIterator]
}

func (d *deferredBatches) Next() (arrow.RecordBatch, error) {
	if err := d.nextError(); err != nil {
		return nil, err
	}
	return d.iterator.Next()
}

func (d *deferredBatches) Schema() (*arrow.Schema, error) {
	if err := d.load(); err != nil {
		return nil, err
	}
	return d.iterator.Schema()
}

// DeferArrowIPCStreams keeps result I/O in iteration and schema access, not the getter.
func DeferArrowIPCStreams(initialize func() (dbsqlrows.ArrowIPCStreamIterator, error)) dbsqlrows.ArrowIPCStreamIterator {
	return &deferredStreams{deferredIterator[dbsqlrows.ArrowIPCStreamIterator]{initialize: initialize}}
}

type deferredStreams struct {
	deferredIterator[dbsqlrows.ArrowIPCStreamIterator]
}

func (d *deferredStreams) Next() (io.Reader, error) {
	if err := d.nextError(); err != nil {
		return nil, err
	}
	return d.iterator.Next()
}

func (d *deferredStreams) SchemaBytes() ([]byte, error) {
	if err := d.load(); err != nil {
		return nil, err
	}
	return d.iterator.SchemaBytes()
}
