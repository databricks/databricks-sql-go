package arrowbased

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"time"

	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/databricks/databricks-sql-go/internal/rows/rowscanner"
	"github.com/pierrec/lz4/v4"
	"github.com/pkg/errors"

	"net/http"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/ipc"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	dbsqlerrint "github.com/databricks/databricks-sql-go/internal/errors"
	"github.com/databricks/databricks-sql-go/internal/fetcher"
)

type BatchLoader interface {
	GetBatchFor(recordNum int64) (*sparkArrowBatch, dbsqlerr.DBError)
}

func NewCloudBatchLoader(ctx context.Context, files []*cli_service.TSparkArrowResultLink, cfg *config.Config) (*batchLoader[*cloudURL], dbsqlerr.DBError) {

	if cfg == nil {
		cfg = config.WithDefaults()
	}

	inputChan := make(chan fetcher.FetchableItems[*sparkArrowBatch], len(files))

	for i := range files {
		li := &cloudURL{
			TSparkArrowResultLink: files[i],
			minTimeToExpiry:       cfg.MinTimeToExpiry,
			useLz4Compression:     cfg.UseLz4Compression,
		}
		inputChan <- li
	}

	// make sure to close input channel or fetcher will block waiting for more inputs
	close(inputChan)

	f, _ := fetcher.NewConcurrentFetcher[*cloudURL](ctx, cfg.MaxDownloadThreads, cfg.MaxFilesInMemory, inputChan)
	cbl := &batchLoader[*cloudURL]{
		Fetcher: f,
		ctx:     ctx,
	}

	return cbl, nil
}

func NewLocalBatchLoader(ctx context.Context, batches []*cli_service.TSparkArrowBatch, cfg *config.Config) (*batchLoader[*localBatch], dbsqlerr.DBError) {

	if cfg == nil {
		cfg = config.WithDefaults()
	}

	var startRow int64
	inputChan := make(chan fetcher.FetchableItems[*sparkArrowBatch], len(batches))
	for i := range batches {
		b := batches[i]
		if b != nil {
			li := &localBatch{
				TSparkArrowBatch:  b,
				startRow:          startRow,
				useLz4Compression: cfg.UseLz4Compression,
			}
			inputChan <- li
			startRow = startRow + b.RowCount
		}
	}
	close(inputChan)

	f, _ := fetcher.NewConcurrentFetcher[*localBatch](ctx, cfg.MaxDownloadThreads, cfg.MaxFilesInMemory, inputChan)
	cbl := &batchLoader[*localBatch]{
		Fetcher: f,
		ctx:     ctx,
	}

	return cbl, nil
}

type batchLoader[T interface {
	Fetch(ctx context.Context) ([]*sparkArrowBatch, error)
}] struct {
	fetcher.Fetcher[*sparkArrowBatch]
	arrowBatches []*sparkArrowBatch
	ctx          context.Context
}

var _ BatchLoader = (*batchLoader[*localBatch])(nil)

func (cbl *batchLoader[T]) GetBatchFor(recordNum int64) (*sparkArrowBatch, dbsqlerr.DBError) {

	for i := range cbl.arrowBatches {
		if cbl.arrowBatches[i].Start() <= recordNum && cbl.arrowBatches[i].End() >= recordNum {
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
		if batch.Contains(recordNum) {
			return batch, nil
		}
	}

	return nil, dbsqlerrint.NewDriverError(cbl.ctx, errArrowRowsInvalidRowIndex(recordNum), err)
}

type cloudURL struct {
	*cli_service.TSparkArrowResultLink
	minTimeToExpiry   time.Duration
	useLz4Compression bool
}

func (cu *cloudURL) Fetch(ctx context.Context) ([]*sparkArrowBatch, error) {
	if isLinkExpired(cu.ExpiryTime, cu.minTimeToExpiry) {
		return nil, errors.New(dbsqlerr.ErrLinkExpired)
	}

	req, err := http.NewRequestWithContext(ctx, "GET", cu.FileLink, nil)
	if err != nil {
		return nil, err
	}

	client := http.DefaultClient
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if res.StatusCode != http.StatusOK {
		return nil, dbsqlerrint.NewDriverError(ctx, errArrowRowsCloudFetchDownloadFailure, err)
	}

	defer res.Body.Close()

	var arrowSchema *arrow.Schema
	var arrowBatches []*sparkArrowBatch

	rdr, err := getArrowReader(res.Body, cu.useLz4Compression)

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
			Delimiter:        rowscanner.NewDelimiter(startRow, r.NumRows()),
			arrowRecordBytes: recordBytes,
			hasSchema:        true,
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

func isLinkExpired(expiryTime int64, linkExpiryBuffer time.Duration) bool {
	bufferSecs := int64(linkExpiryBuffer.Seconds())
	return expiryTime-bufferSecs < time.Now().Unix()
}

func getArrowReader(rd io.Reader, useLz4Compression bool) (*ipc.Reader, error) {
	if useLz4Compression {
		return ipc.NewReader(lz4.NewReader(rd))
	}
	return ipc.NewReader(bufio.NewReader(rd))
}

func getArrowBatch(useLz4Compression bool, src []byte) ([]byte, error) {
	if useLz4Compression {
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
	startRow          int64
	useLz4Compression bool
}

var _ fetcher.FetchableItems[*sparkArrowBatch] = (*localBatch)(nil)

func (lb *localBatch) Fetch(ctx context.Context) ([]*sparkArrowBatch, error) {
	arrowBatchBytes, err := getArrowBatch(lb.useLz4Compression, lb.Batch)
	if err != nil {
		return nil, err
	}
	batch := &sparkArrowBatch{
		Delimiter:        rowscanner.NewDelimiter(lb.startRow, lb.RowCount),
		arrowRecordBytes: arrowBatchBytes,
	}

	return []*sparkArrowBatch{batch}, nil
}
