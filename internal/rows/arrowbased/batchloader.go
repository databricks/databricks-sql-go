package arrowbased

import (
	"bytes"
	"context"
	"io"
	"time"

	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/databricks/databricks-sql-go/internal/rows/rowscanner"
	"github.com/pierrec/lz4/v4"
	"github.com/pkg/errors"

	"net/http"

	"github.com/apache/arrow/go/v16/arrow/ipc"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	dbsqlerrint "github.com/databricks/databricks-sql-go/internal/errors"
	"github.com/databricks/databricks-sql-go/internal/fetcher"
	"github.com/databricks/databricks-sql-go/logger"
)

type BatchIterator interface {
	Next() (SparkArrowBatch, error)
	HasNext() bool
	Close()
}

type BatchLoader interface {
	rowscanner.Delimiter
	GetBatchFor(recordNum int64) (SparkArrowBatch, dbsqlerr.DBError)
	Close()
}

func NewBatchIterator(batchLoader BatchLoader) (BatchIterator, dbsqlerr.DBError) {
	bi := &batchIterator{
		nextBatchStart: batchLoader.Start(),
		batchLoader:    batchLoader,
	}

	return bi, nil
}

func NewCloudBatchLoader(ctx context.Context, files []*cli_service.TSparkArrowResultLink, startRowOffset int64, cfg *config.Config) (*batchLoader[*cloudURL], dbsqlerr.DBError) {

	if cfg == nil {
		cfg = config.WithDefaults()
	}

	inputChan := make(chan fetcher.FetchableItems[SparkArrowBatch], len(files))

	var rowCount int64
	for i := range files {
		f := files[i]
		li := &cloudURL{
			// TSparkArrowResultLink: f,
			Delimiter:         rowscanner.NewDelimiter(f.StartRowOffset, f.RowCount),
			fileLink:          f.FileLink,
			expiryTime:        f.ExpiryTime,
			minTimeToExpiry:   cfg.MinTimeToExpiry,
			compressibleBatch: compressibleBatch{useLz4Compression: cfg.UseLz4Compression},
		}
		inputChan <- li

		rowCount += f.RowCount
	}

	// make sure to close input channel or fetcher will block waiting for more inputs
	close(inputChan)

	f, _ := fetcher.NewConcurrentFetcher[*cloudURL](ctx, cfg.MaxDownloadThreads, cfg.MaxFilesInMemory, inputChan)
	cbl := &batchLoader[*cloudURL]{
		Delimiter: rowscanner.NewDelimiter(startRowOffset, rowCount),
		fetcher:   f,
		ctx:       ctx,
	}

	return cbl, nil
}

func NewLocalBatchLoader(ctx context.Context, batches []*cli_service.TSparkArrowBatch, startRowOffset int64, arrowSchemaBytes []byte, cfg *config.Config) (*batchLoader[*localBatch], dbsqlerr.DBError) {

	if cfg == nil {
		cfg = config.WithDefaults()
	}

	var startRow int64 = startRowOffset
	var rowCount int64
	inputChan := make(chan fetcher.FetchableItems[SparkArrowBatch], len(batches))
	for i := range batches {
		b := batches[i]
		if b != nil {
			li := &localBatch{
				Delimiter:         rowscanner.NewDelimiter(startRow, b.RowCount),
				batchBytes:        b.Batch,
				arrowSchemaBytes:  arrowSchemaBytes,
				compressibleBatch: compressibleBatch{useLz4Compression: cfg.UseLz4Compression},
			}
			inputChan <- li
			startRow = startRow + b.RowCount
			rowCount += b.RowCount
		}
	}
	close(inputChan)

	f, _ := fetcher.NewConcurrentFetcher[*localBatch](ctx, cfg.MaxDownloadThreads, cfg.MaxFilesInMemory, inputChan)
	cbl := &batchLoader[*localBatch]{
		Delimiter: rowscanner.NewDelimiter(startRowOffset, rowCount),
		fetcher:   f,
		ctx:       ctx,
	}

	return cbl, nil
}

type batchLoader[T interface {
	Fetch(ctx context.Context) (SparkArrowBatch, error)
}] struct {
	rowscanner.Delimiter
	fetcher      fetcher.Fetcher[SparkArrowBatch]
	arrowBatches []SparkArrowBatch
	ctx          context.Context
}

var _ BatchLoader = (*batchLoader[*localBatch])(nil)

func (cbl *batchLoader[T]) GetBatchFor(rowNumber int64) (SparkArrowBatch, dbsqlerr.DBError) {

	logger.Debug().Msgf("batchLoader.GetBatchFor(%d)", rowNumber)

	for i := range cbl.arrowBatches {
		logger.Debug().Msgf("  trying batch for range [%d..%d]", cbl.arrowBatches[i].Start(), cbl.arrowBatches[i].End())
		if cbl.arrowBatches[i].Contains(rowNumber) {
			logger.Debug().Msgf("  found batch containing the requested row %d", rowNumber)
			return cbl.arrowBatches[i], nil
		}
	}

	logger.Debug().Msgf("  batch not found, trying to download more")

	batchChan, _, err := cbl.fetcher.Start()
	var emptyBatch SparkArrowBatch
	if err != nil {
		logger.Debug().Msgf("  no batch found for row %d", rowNumber)
		return emptyBatch, dbsqlerrint.NewDriverError(cbl.ctx, errArrowRowsInvalidRowNumber(rowNumber), err)
	}

	for {
		batch, ok := <-batchChan
		if !ok {
			err := cbl.fetcher.Err()
			if err != nil {
				logger.Debug().Msgf("  no batch found for row %d", rowNumber)
				return emptyBatch, dbsqlerrint.NewDriverError(cbl.ctx, errArrowRowsInvalidRowNumber(rowNumber), err)
			}
			break
		}

		cbl.arrowBatches = append(cbl.arrowBatches, batch)
		logger.Debug().Msgf("  trying newly downloaded batch for range [%d..%d]", batch.Start(), batch.End())
		if batch.Contains(rowNumber) {
			logger.Debug().Msgf("  found batch containing the requested row %d", rowNumber)
			return batch, nil
		}
	}

	logger.Debug().Msgf("  no batch found for row %d", rowNumber)

	return emptyBatch, dbsqlerrint.NewDriverError(cbl.ctx, errArrowRowsInvalidRowNumber(rowNumber), err)
}

func (cbl *batchLoader[T]) Close() {
	for i := range cbl.arrowBatches {
		cbl.arrowBatches[i].Close()
	}
}

type compressibleBatch struct {
	useLz4Compression bool
}

func (cb compressibleBatch) getReader(r io.Reader) io.Reader {
	if cb.useLz4Compression {
		return lz4.NewReader(r)
	}
	return r
}

type cloudURL struct {
	compressibleBatch
	rowscanner.Delimiter
	fileLink        string
	expiryTime      int64
	minTimeToExpiry time.Duration
}

func (cu *cloudURL) Fetch(ctx context.Context) (SparkArrowBatch, error) {
	var sab SparkArrowBatch

	if isLinkExpired(cu.expiryTime, cu.minTimeToExpiry) {
		return sab, errors.New(dbsqlerr.ErrLinkExpired)
	}

	req, err := http.NewRequestWithContext(ctx, "GET", cu.fileLink, nil)
	if err != nil {
		return sab, err
	}

	client := http.DefaultClient
	res, err := client.Do(req)
	if err != nil {
		return sab, err
	}
	if res.StatusCode != http.StatusOK {
		return sab, dbsqlerrint.NewDriverError(ctx, errArrowRowsCloudFetchDownloadFailure, err)
	}

	defer res.Body.Close()

	r := cu.compressibleBatch.getReader(res.Body)

	records, err := getArrowRecords(r, cu.Start())
	if err != nil {
		return nil, err
	}

	arrowBatch := sparkArrowBatch{
		Delimiter:    rowscanner.NewDelimiter(cu.Start(), cu.Count()),
		arrowRecords: records,
	}

	return &arrowBatch, nil
}

func isLinkExpired(expiryTime int64, linkExpiryBuffer time.Duration) bool {
	bufferSecs := int64(linkExpiryBuffer.Seconds())
	return expiryTime-bufferSecs < time.Now().Unix()
}

var _ fetcher.FetchableItems[SparkArrowBatch] = (*cloudURL)(nil)

type localBatch struct {
	compressibleBatch
	rowscanner.Delimiter
	batchBytes       []byte
	arrowSchemaBytes []byte
}

var _ fetcher.FetchableItems[SparkArrowBatch] = (*localBatch)(nil)

func (lb *localBatch) Fetch(ctx context.Context) (SparkArrowBatch, error) {
	r := lb.compressibleBatch.getReader(bytes.NewReader(lb.batchBytes))
	r = io.MultiReader(bytes.NewReader(lb.arrowSchemaBytes), r)

	records, err := getArrowRecords(r, lb.Start())
	if err != nil {
		return &sparkArrowBatch{}, err
	}

	lb.batchBytes = nil
	batch := sparkArrowBatch{
		Delimiter:    rowscanner.NewDelimiter(lb.Start(), lb.Count()),
		arrowRecords: records,
	}

	return &batch, nil
}

func getArrowRecords(r io.Reader, startRowOffset int64) ([]SparkArrowRecord, error) {
	ipcReader, err := ipc.NewReader(r)
	if err != nil {
		return nil, err
	}

	defer ipcReader.Release()

	startRow := startRowOffset
	var records []SparkArrowRecord
	for ipcReader.Next() {
		r := ipcReader.Record()
		r.Retain()

		sar := sparkArrowRecord{
			Delimiter: rowscanner.NewDelimiter(startRow, r.NumRows()),
			Record:    r,
		}

		records = append(records, &sar)

		startRow += r.NumRows()
	}

	if ipcReader.Err() != nil {
		for i := range records {
			records[i].Release()
		}
		return nil, ipcReader.Err()
	}

	return records, nil
}

type batchIterator struct {
	nextBatchStart int64
	batchLoader    BatchLoader
}

var _ BatchIterator = (*batchIterator)(nil)

func (bi *batchIterator) Next() (SparkArrowBatch, error) {
	if !bi.HasNext() {
		return nil, io.EOF
	}
	if bi != nil && bi.batchLoader != nil {
		batch, err := bi.batchLoader.GetBatchFor(bi.nextBatchStart)
		if batch != nil && err == nil {
			bi.nextBatchStart = batch.Start() + batch.Count()
		}
		return batch, err
	}
	return nil, nil
}

func (bi *batchIterator) HasNext() bool {
	return bi != nil && bi.batchLoader != nil && bi.batchLoader.Contains(bi.nextBatchStart)
}

func (bi *batchIterator) Close() {
	if bi != nil && bi.batchLoader != nil {
		bi.batchLoader.Close()
	}
}
