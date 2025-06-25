package arrowbased

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/databricks/databricks-sql-go/internal/rows/rowscanner"
	"github.com/pierrec/lz4/v4"
	"github.com/pkg/errors"

	"net/http"

	"github.com/apache/arrow/go/v12/arrow/ipc"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	dbsqlerrint "github.com/databricks/databricks-sql-go/internal/errors"
	"github.com/databricks/databricks-sql-go/logger"
)

type RawBatchIterator interface {
	Next() (*cli_service.TSparkArrowBatch, error)
	HasNext() bool
	Close()
	GetStartRowOffset() int64
}

// BatchIterator provides parsed Arrow batches
type BatchIterator interface {
	Next() (SparkArrowBatch, error)
	HasNext() bool
	Close()
}

// batchIterator wraps a RawBatchIterator and parses the raw batches
type batchIterator struct {
	rawIterator      RawBatchIterator
	arrowSchemaBytes []byte
	cfg              *config.Config
}

var _ BatchIterator = (*batchIterator)(nil)

func (bi *batchIterator) Next() (SparkArrowBatch, error) {
	rawBatch, err := bi.rawIterator.Next()
	if err != nil {
		return nil, err
	}

	// GetStartRowOffset returns the start offset of the last returned batch
	startOffset := bi.rawIterator.GetStartRowOffset()

	reader := io.MultiReader(
		bytes.NewReader(bi.arrowSchemaBytes),
		getReader(bytes.NewReader(rawBatch.Batch), bi.cfg.UseLz4Compression),
	)

	records, err := getArrowRecords(reader, startOffset)
	if err != nil {
		return nil, err
	}

	batch := sparkArrowBatch{
		Delimiter:    rowscanner.NewDelimiter(startOffset, rawBatch.RowCount),
		arrowRecords: records,
	}

	return &batch, nil
}

func (bi *batchIterator) HasNext() bool {
	return bi.rawIterator.HasNext()
}

func (bi *batchIterator) Close() {
	bi.rawIterator.Close()
}

// NewBatchIterator creates a BatchIterator from a RawBatchIterator
func NewBatchIterator(
	rawIterator RawBatchIterator,
	arrowSchemaBytes []byte,
	cfg *config.Config,
) BatchIterator {
	return &batchIterator{
		rawIterator:      rawIterator,
		arrowSchemaBytes: arrowSchemaBytes,
		cfg:              cfg,
	}
}

func NewCloudRawBatchIterator(
	ctx context.Context,
	files []*cli_service.TSparkArrowResultLink,
	startRowOffset int64,
	cfg *config.Config,
) (RawBatchIterator, dbsqlerr.DBError) {
	bi := &cloudBatchIterator{
		ctx:              ctx,
		cfg:              cfg,
		currentRowOffset: startRowOffset,
		pendingLinks:     NewQueue[cli_service.TSparkArrowResultLink](),
		downloadTasks:    NewQueue[cloudFetchDownloadTask](),
	}

	for _, link := range files {
		bi.pendingLinks.Enqueue(link)
	}

	return bi, nil
}

func NewLocalRawBatchIterator(
	ctx context.Context,
	batches []*cli_service.TSparkArrowBatch,
	startRowOffset int64,
	arrowSchemaBytes []byte,
	cfg *config.Config,
) (RawBatchIterator, dbsqlerr.DBError) {
	bi := &localBatchIterator{
		cfg:              cfg,
		currentRowOffset: startRowOffset,
		arrowSchemaBytes: arrowSchemaBytes,
		batches:          batches,
		index:            -1,
	}

	return bi, nil
}

type localBatchIterator struct {
	cfg              *config.Config
	currentRowOffset int64  // Tracks the start offset of the last returned batch
	arrowSchemaBytes []byte
	batches          []*cli_service.TSparkArrowBatch
	index            int
}

var _ RawBatchIterator = (*localBatchIterator)(nil)

func (bi *localBatchIterator) Next() (*cli_service.TSparkArrowBatch, error) {
	cnt := len(bi.batches)
	bi.index++
	if bi.index < cnt {
		ab := bi.batches[bi.index]
		// Update offset after returning the batch
		bi.currentRowOffset += ab.RowCount
		return ab, nil
	}

	bi.index = cnt
	return nil, io.EOF
}

func (bi *localBatchIterator) HasNext() bool {
	// `Next()` will first increment an index, and only then return a batch
	// So `HasNext` should check if index can be incremented and still be within array
	return bi.index+1 < len(bi.batches)
}

func (bi *localBatchIterator) Close() {
	bi.index = len(bi.batches)
}

func (bi *localBatchIterator) GetStartRowOffset() int64 {
	// Return the offset of the last returned batch
	if bi.index >= 0 && bi.index < len(bi.batches) {
		// currentRowOffset points to after the last returned batch
		// so subtract the current batch's row count to get its start
		return bi.currentRowOffset - bi.batches[bi.index].RowCount
	}
	return bi.currentRowOffset
}

type cloudBatchIterator struct {
	ctx              context.Context
	cfg              *config.Config
	currentRowOffset int64  // Tracks the offset after the last returned batch
	lastBatchRowCount int64 // Tracks the row count of the last returned batch
	pendingLinks     Queue[cli_service.TSparkArrowResultLink]
	downloadTasks    Queue[cloudFetchDownloadTask]
}

var _ RawBatchIterator = (*cloudBatchIterator)(nil)

func (bi *cloudBatchIterator) Next() (*cli_service.TSparkArrowBatch, error) {
	for (bi.downloadTasks.Len() < bi.cfg.MaxDownloadThreads) && (bi.pendingLinks.Len() > 0) {
		link := bi.pendingLinks.Dequeue()
		logger.Debug().Msgf(
			"CloudFetch: schedule link at offset %d row count %d",
			link.StartRowOffset,
			link.RowCount,
		)

		cancelCtx, cancelFn := context.WithCancel(bi.ctx)
		task := &cloudFetchDownloadTask{
			ctx:               cancelCtx,
			cancel:            cancelFn,
			useLz4Compression: bi.cfg.UseLz4Compression,
			link:              link,
			resultChan:        make(chan cloudFetchDownloadTaskResult),
			minTimeToExpiry:   bi.cfg.MinTimeToExpiry,
		}
		task.Run()
		bi.downloadTasks.Enqueue(task)
	}

	task := bi.downloadTasks.Dequeue()
	if task == nil {
		return nil, io.EOF
	}

	batchData, rowCount, err := task.GetResult()

	// once we've got an errored out task - cancel the remaining ones
	if err != nil {
		bi.Close()
		return nil, err
	}

	// explicitly call cancel function on successfully completed task to avoid context leak
	task.cancel()
	
	// Create TSparkArrowBatch from the downloaded data
	batch := &cli_service.TSparkArrowBatch{
		Batch:    batchData,
		RowCount: rowCount,
	}
	bi.lastBatchRowCount = rowCount
	bi.currentRowOffset += rowCount
	return batch, nil
}

func (bi *cloudBatchIterator) HasNext() bool {
	return (bi.pendingLinks.Len() > 0) || (bi.downloadTasks.Len() > 0)
}

func (bi *cloudBatchIterator) Close() {
	bi.pendingLinks.Clear()
	for bi.downloadTasks.Len() > 0 {
		task := bi.downloadTasks.Dequeue()
		task.cancel()
	}
}

func (bi *cloudBatchIterator) GetStartRowOffset() int64 {
	// Return the start offset of the last returned batch
	return bi.currentRowOffset - bi.lastBatchRowCount
}

type cloudFetchDownloadTaskResult struct {
	batchData []byte
	rowCount  int64
	err       error
}

type cloudFetchDownloadTask struct {
	ctx               context.Context
	cancel            context.CancelFunc
	useLz4Compression bool
	minTimeToExpiry   time.Duration
	link              *cli_service.TSparkArrowResultLink
	resultChan        chan cloudFetchDownloadTaskResult
}

func (cft *cloudFetchDownloadTask) GetResult() ([]byte, int64, error) {
	link := cft.link

	result, ok := <-cft.resultChan
	if ok {
		if result.err != nil {
			logger.Debug().Msgf(
				"CloudFetch: failed to download link at offset %d row count %d, reason: %s",
				link.StartRowOffset,
				link.RowCount,
				result.err.Error(),
			)
			return nil, 0, result.err
		}
		logger.Debug().Msgf(
			"CloudFetch: received data for link at offset %d row count %d",
			link.StartRowOffset,
			link.RowCount,
		)
		return result.batchData, result.rowCount, nil
	}

	// This branch should never be reached. If you see this message - something got really wrong
	logger.Debug().Msgf(
		"CloudFetch: channel was closed before result was received; link at offset %d row count %d",
		link.StartRowOffset,
		link.RowCount,
	)
	return nil, 0, nil
}

func (cft *cloudFetchDownloadTask) Run() {
	go func() {
		defer close(cft.resultChan)

		logger.Debug().Msgf(
			"CloudFetch: start downloading link at offset %d row count %d",
			cft.link.StartRowOffset,
			cft.link.RowCount,
		)
		data, err := fetchBatchBytes(cft.ctx, cft.link, cft.minTimeToExpiry)
		if err != nil {
			cft.resultChan <- cloudFetchDownloadTaskResult{batchData: nil, rowCount: 0, err: err}
			return
		}

		// Read all data into memory
		batchData, err := io.ReadAll(data)
		data.Close() // Close after reading
		if err != nil {
			cft.resultChan <- cloudFetchDownloadTaskResult{batchData: nil, rowCount: 0, err: err}
			return
		}

		logger.Debug().Msgf(
			"CloudFetch: received batch data for link at offset %d row count %d",
			cft.link.StartRowOffset,
			cft.link.RowCount,
		)

		cft.resultChan <- cloudFetchDownloadTaskResult{
			batchData: batchData,
			rowCount:  cft.link.RowCount,
			err:       nil,
		}
	}()
}

func fetchBatchBytes(
	ctx context.Context,
	link *cli_service.TSparkArrowResultLink,
	minTimeToExpiry time.Duration,
) (io.ReadCloser, error) {
	if isLinkExpired(link.ExpiryTime, minTimeToExpiry) {
		return nil, errors.New(dbsqlerr.ErrLinkExpired)
	}

	// TODO: Retry on HTTP errors
	req, err := http.NewRequestWithContext(ctx, "GET", link.FileLink, nil)
	if err != nil {
		return nil, err
	}

	if link.HttpHeaders != nil {
		for key, value := range link.HttpHeaders {
			req.Header.Set(key, value)
		}
	}

	client := http.DefaultClient
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if res.StatusCode != http.StatusOK {
		msg := fmt.Sprintf("%s: %s %d", errArrowRowsCloudFetchDownloadFailure, "HTTP error", res.StatusCode)
		return nil, dbsqlerrint.NewDriverError(ctx, msg, err)
	}

	return res.Body, nil
}

func getReader(r io.Reader, useLz4Compression bool) io.Reader {
	if useLz4Compression {
		return lz4.NewReader(r)
	}
	return r
}

func isLinkExpired(expiryTime int64, linkExpiryBuffer time.Duration) bool {
	bufferSecs := int64(linkExpiryBuffer.Seconds())
	return expiryTime-bufferSecs < time.Now().Unix()
}

// pagedRawBatchIterator wraps a result page iterator and provides raw batches
type pagedRawBatchIterator struct {
	ctx                context.Context
	resultPageIterator rowscanner.ResultPageIterator
	currentIterator    RawBatchIterator
	cfg                *config.Config
	startRowOffset     int64
}

// NewPagedRawBatchIterator creates a raw batch iterator from a result page iterator
func NewPagedRawBatchIterator(
	ctx context.Context,
	resultPageIterator rowscanner.ResultPageIterator,
	cfg *config.Config,
) RawBatchIterator {
	return &pagedRawBatchIterator{
		ctx:                ctx,
		resultPageIterator: resultPageIterator,
		cfg:                cfg,
		startRowOffset:     0,
	}
}

func (pi *pagedRawBatchIterator) Next() (*cli_service.TSparkArrowBatch, error) {
	// If we have a current iterator and it has more batches, use it
	if pi.currentIterator != nil && pi.currentIterator.HasNext() {
		return pi.currentIterator.Next()
	}

	// Need to fetch next page
	if pi.resultPageIterator == nil || !pi.resultPageIterator.HasNext() {
		return nil, io.EOF
	}

	fetchResult, err := pi.resultPageIterator.Next()
	if err != nil {
		return nil, err
	}

	if fetchResult == nil || fetchResult.Results == nil {
		return nil, io.EOF
	}

	rowSet := fetchResult.Results
	
	// Create appropriate iterator based on result type
	if len(rowSet.ResultLinks) > 0 {
		pi.currentIterator, err = NewCloudRawBatchIterator(pi.ctx, rowSet.ResultLinks, rowSet.StartRowOffset, pi.cfg)
	} else if len(rowSet.ArrowBatches) > 0 {
		// Note: we don't have schema bytes here, but raw iterator doesn't need them
		pi.currentIterator, err = NewLocalRawBatchIterator(pi.ctx, rowSet.ArrowBatches, rowSet.StartRowOffset, nil, pi.cfg)
	} else {
		return nil, io.EOF
	}

	if err != nil {
		return nil, err
	}

	// Now get the first batch from the new iterator
	return pi.currentIterator.Next()
}

func (pi *pagedRawBatchIterator) HasNext() bool {
	return (pi.currentIterator != nil && pi.currentIterator.HasNext()) ||
		(pi.resultPageIterator != nil && pi.resultPageIterator.HasNext())
}

func (pi *pagedRawBatchIterator) Close() {
	if pi.currentIterator != nil {
		pi.currentIterator.Close()
	}
}

func (pi *pagedRawBatchIterator) GetStartRowOffset() int64 {
	if pi.currentIterator != nil {
		return pi.currentIterator.GetStartRowOffset()
	}
	return pi.startRowOffset
}

// Legacy wrapper functions for backward compatibility with tests
func NewCloudBatchIterator(
	ctx context.Context,
	files []*cli_service.TSparkArrowResultLink,
	startRowOffset int64,
	cfg *config.Config,
) (BatchIterator, dbsqlerr.DBError) {
	// For cloud iterator, we don't have schema bytes at this level
	// The schema will need to be provided when creating the BatchIterator wrapper
	// This is a temporary compatibility function for tests
	return nil, dbsqlerrint.NewDriverError(ctx, "NewCloudBatchIterator is deprecated, use NewCloudRawBatchIterator + NewBatchIterator", nil)
}

func NewLocalBatchIterator(
	ctx context.Context,
	batches []*cli_service.TSparkArrowBatch,
	startRowOffset int64,
	arrowSchemaBytes []byte,
	cfg *config.Config,
) (BatchIterator, dbsqlerr.DBError) {
	rawIterator, err := NewLocalRawBatchIterator(ctx, batches, startRowOffset, arrowSchemaBytes, cfg)
	if err != nil {
		return nil, err
	}
	return NewBatchIterator(rawIterator, arrowSchemaBytes, cfg), nil
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
