package arrowbased

import (
	"bufio"
	"bytes"
	"context"
	"github.com/databricks/databricks-sql-go/driverctx"
	"github.com/pierrec/lz4/v4"
	"io"

	"net/http"

	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/ipc"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	dbsqlerrint "github.com/databricks/databricks-sql-go/internal/errors"
	"github.com/databricks/databricks-sql-go/internal/fetcher"
)

const (
	UseLz4CompressionKey string = "useLz4Compression"
)

type cloudURL struct {
	*cli_service.TSparkArrowResultLink
}

func (cu *cloudURL) Fetch(ctx context.Context) ([]*sparkArrowBatch, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", cu.FileLink, nil)
	if err != nil {
		return nil, err
	}

	client := http.DefaultClient
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()

	var arrowSchema *arrow.Schema
	var arrowBatches []*sparkArrowBatch

	rdr, err := getArrowReader(ctx, res.Body)

	if err != nil {
		return nil, err
	}

	startRow := cu.StartRowOffset

	for rdr.Next() {
		r := rdr.Record()
		r.Retain()
		if arrowSchema == nil {
			arrowSchema = r.Schema()
		}

		var output bytes.Buffer
		w := ipc.NewWriter(&output, ipc.WithSchema(r.Schema()))

		err := w.Write(r)
		if err != nil {
			panic(err)
		}
		err = w.Close()
		if err != nil {
			panic(err)
		}

		recordBytes := output.Bytes()

		arrowBatches = append(arrowBatches, &sparkArrowBatch{
			arrowRecordBytes: recordBytes,
			hasSchema:        true,
			rowCount:         r.NumRows(),
			startRow:         startRow,
			endRow:           startRow + r.NumRows() - 1,
		})

		startRow = startRow + r.NumRows()

		r.Release()
	}

	if rdr.Err() != nil {
		panic(rdr.Err())
	}
	rdr.Release()

	return arrowBatches, nil
}

func getArrowReader(ctx context.Context, rd io.Reader) (*ipc.Reader, error) {
	if driverctx.UseLz4CompressionFromContext(ctx) {
		return ipc.NewReader(lz4.NewReader(rd))
	}
	return ipc.NewReader(bufio.NewReader(rd))
}

func getArrowBatch(ctx context.Context, src []byte) ([]byte, error) {
	if driverctx.UseLz4CompressionFromContext(ctx) {
		srcBuffer := bytes.NewBuffer(src)
		dstBuffer := bytes.NewBuffer(nil)

		r := lz4.NewReader(srcBuffer)
		_, err := io.Copy(dstBuffer, r)
		if err != nil {
			return nil, err
		}

		return dstBuffer.Bytes(), nil
	}
	return src, nil
}

var _ fetcher.FetchableItems[*sparkArrowBatch] = (*cloudURL)(nil)

type localBatch struct {
	*cli_service.TSparkArrowBatch
	startRow int64
}

var _ fetcher.FetchableItems[*sparkArrowBatch] = (*localBatch)(nil)

func (lb *localBatch) Fetch(ctx context.Context) ([]*sparkArrowBatch, error) {
	arrowBatchBytes, err := getArrowBatch(ctx, lb.Batch)
	if err != nil {
		return nil, err
	}
	batch := &sparkArrowBatch{
		rowCount:         lb.RowCount,
		startRow:         lb.startRow,
		endRow:           lb.startRow + lb.RowCount - 1,
		arrowRecordBytes: arrowBatchBytes,
	}

	return []*sparkArrowBatch{batch}, nil
}

type BatchLoader interface {
	GetBatchFor(recordNum int64) (*sparkArrowBatch, dbsqlerr.DBError)
}

type batchLoader[T interface {
	Fetch(ctx context.Context) ([]*sparkArrowBatch, error)
}] struct {
	fetcher.Fetcher[*sparkArrowBatch]
	arrowBatches []*sparkArrowBatch
	ctx          context.Context
}

func NewCloudBatchLoader(ctx context.Context, files []*cli_service.TSparkArrowResultLink) (*batchLoader[*cloudURL], dbsqlerr.DBError) {
	inputChan := make(chan fetcher.FetchableItems[*sparkArrowBatch], len(files))

	for i := range files {
		li := &cloudURL{TSparkArrowResultLink: files[i]}
		inputChan <- li
	}

	// make sure to close input channel or fetcher will block waiting for more inputs
	close(inputChan)

	f, _ := fetcher.NewConcurrentFetcher[*cloudURL](ctx, 3, inputChan)
	cbl := &batchLoader[*cloudURL]{
		Fetcher: f,
		ctx:     ctx,
	}

	return cbl, nil
}

func NewLocalBatchLoader(ctx context.Context, batches []*cli_service.TSparkArrowBatch) (*batchLoader[*localBatch], dbsqlerr.DBError) {
	var startRow int64
	inputChan := make(chan fetcher.FetchableItems[*sparkArrowBatch], len(batches))
	for i := range batches {
		b := batches[i]
		if b != nil {
			li := &localBatch{TSparkArrowBatch: b, startRow: startRow}
			inputChan <- li
			startRow = startRow + b.RowCount
		}
	}
	close(inputChan)

	f, _ := fetcher.NewConcurrentFetcher[*localBatch](ctx, 3, inputChan)
	cbl := &batchLoader[*localBatch]{
		Fetcher: f,
		ctx:     ctx,
	}

	return cbl, nil
}

func (cbl *batchLoader[T]) GetBatchFor(recordNum int64) (*sparkArrowBatch, dbsqlerr.DBError) {

	for i := range cbl.arrowBatches {
		if cbl.arrowBatches[i].startRow <= recordNum && cbl.arrowBatches[i].endRow >= recordNum {
			return cbl.arrowBatches[i], nil
		}
	}

	batchChan, _, err := cbl.Start()
	if err != nil {
		return nil, dbsqlerrint.NewDriverError(cbl.ctx, errArrowRowsInvalidRowIndex(recordNum), err)
	}

	for {
		batch, ok := <-batchChan
		if !ok {
			err := cbl.Err()
			if err != nil {
				return nil, dbsqlerrint.NewDriverError(cbl.ctx, errArrowRowsInvalidRowIndex(recordNum), err)
			}
			break
		}

		cbl.arrowBatches = append(cbl.arrowBatches, batch)
		if batch.contains(recordNum) {
			return batch, nil
		}
	}

	return nil, dbsqlerrint.NewDriverError(cbl.ctx, errArrowRowsInvalidRowIndex(recordNum), err)
}
