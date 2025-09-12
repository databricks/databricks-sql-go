package arrowbased

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/pierrec/lz4/v4"
	"github.com/pkg/errors"

	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/databricks/databricks-sql-go/internal/rows/rowscanner"

	"github.com/apache/arrow/go/v12/arrow/ipc"

	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	dbsqlerrint "github.com/databricks/databricks-sql-go/internal/errors"
	"github.com/databricks/databricks-sql-go/logger"
)

type IPCStreamIterator interface {
	Next() (io.Reader, error)
	HasNext() bool
	Close()
}

func NewCloudIPCStreamIterator(
	ctx context.Context,
	files []*cli_service.TSparkArrowResultLink,
	startRowOffset int64,
	cfg *config.Config,
) (IPCStreamIterator, dbsqlerr.DBError) {
	bi := &cloudIPCStreamIterator{
		ctx:            ctx,
		cfg:            cfg,
		startRowOffset: startRowOffset,
		pendingLinks:   NewQueue[cli_service.TSparkArrowResultLink](),
		downloadTasks:  NewQueue[cloudFetchDownloadTask](),
		results:        make(chan cloudFetchDownloadTaskResult, cfg.MaxDownloadThreads*2),
	}

	for _, link := range files {
		bi.pendingLinks.Enqueue(link)
	}
	go bi.startDownloads()

	return bi, nil
}

// NewCloudBatchIterator creates a cloud-based BatchIterator for backward compatibility
func NewCloudBatchIterator(
	ctx context.Context,
	files []*cli_service.TSparkArrowResultLink,
	startRowOffset int64,
	cfg *config.Config,
) (BatchIterator, dbsqlerr.DBError) {
	ipcIterator, err := NewCloudIPCStreamIterator(ctx, files, startRowOffset, cfg)
	if err != nil {
		return nil, err
	}
	return NewBatchIterator(ipcIterator, startRowOffset), nil
}

func NewLocalIPCStreamIterator(
	ctx context.Context,
	batches []*cli_service.TSparkArrowBatch,
	startRowOffset int64,
	arrowSchemaBytes []byte,
	cfg *config.Config,
) (IPCStreamIterator, dbsqlerr.DBError) {
	bi := &localIPCStreamIterator{
		cfg:              cfg,
		startRowOffset:   startRowOffset,
		arrowSchemaBytes: arrowSchemaBytes,
		batches:          batches,
		index:            -1,
	}

	return bi, nil
}

// NewLocalBatchIterator creates a local BatchIterator for backward compatibility
func NewLocalBatchIterator(
	ctx context.Context,
	batches []*cli_service.TSparkArrowBatch,
	startRowOffset int64,
	arrowSchemaBytes []byte,
	cfg *config.Config,
) (BatchIterator, dbsqlerr.DBError) {
	ipcIterator, err := NewLocalIPCStreamIterator(ctx, batches, startRowOffset, arrowSchemaBytes, cfg)
	if err != nil {
		return nil, err
	}
	return NewBatchIterator(ipcIterator, startRowOffset), nil
}

type localIPCStreamIterator struct {
	cfg              *config.Config
	startRowOffset   int64
	arrowSchemaBytes []byte
	batches          []*cli_service.TSparkArrowBatch
	index            int
}

var _ IPCStreamIterator = (*localIPCStreamIterator)(nil)

func (bi *localIPCStreamIterator) Next() (io.Reader, error) {
	cnt := len(bi.batches)
	bi.index++
	if bi.index < cnt {
		ab := bi.batches[bi.index]

		reader := io.MultiReader(
			bytes.NewReader(bi.arrowSchemaBytes),
			getReader(bytes.NewReader(ab.Batch), bi.cfg.UseLz4Compression),
		)

		return reader, nil
	}

	bi.index = cnt
	return nil, io.EOF
}

func (bi *localIPCStreamIterator) HasNext() bool {
	// `Next()` will first increment an index, and only then return a batch
	// So `HasNext` should check if index can be incremented and still be within array
	return bi.index+1 < len(bi.batches)
}

func (bi *localIPCStreamIterator) Close() {
	bi.index = len(bi.batches)
}

type cloudIPCStreamIterator struct {
	ctx            context.Context
	cfg            *config.Config
	startRowOffset int64
	pendingLinks   Queue[cli_service.TSparkArrowResultLink]
	downloadTasks  Queue[cloudFetchDownloadTask]
	results        chan cloudFetchDownloadTaskResult
	wg             sync.WaitGroup
}

var _ IPCStreamIterator = (*cloudIPCStreamIterator)(nil)

func (bi *cloudIPCStreamIterator) startDownloads() {
	defer func() {
		bi.wg.Wait()
		close(bi.results)
	}()

	// Start tasks while we have capacity and work
	for bi.pendingLinks.Len() > 0 || bi.downloadTasks.Len() > 0 {
		// Fill up to MaxDownloadThreads
		for bi.pendingLinks.Len() > 0 && bi.downloadTasks.Len() < bi.cfg.MaxDownloadThreads {
			link := bi.pendingLinks.Dequeue()

			cancelCtx, cancelFn := context.WithCancel(bi.ctx)
			task := &cloudFetchDownloadTask{
				ctx:               cancelCtx,
				cancel:            cancelFn,
				useLz4Compression: bi.cfg.UseLz4Compression,
				link:              link,
				resultChan:        make(chan cloudFetchDownloadTaskResult),
				minTimeToExpiry:   bi.cfg.MinTimeToExpiry,
			}
			bi.downloadTasks.Enqueue(task)

			bi.wg.Add(1)
			go func(t *cloudFetchDownloadTask) {
				defer bi.wg.Done()
				defer func() {
					if r := recover(); r != nil {
						// Don’t block indefinitely on shutdown
						select {
						case bi.results <- cloudFetchDownloadTaskResult{nil, fmt.Errorf("panic: %v", r)}:
						case <-bi.ctx.Done():
						}
					}
				}()

				// Do the real work inside the goroutine
				t.Run()                   // starts and completes the download
				res, err := t.GetResult() // or blocks until ready

				// Publish result unless shutting down
				select {
				case bi.results <- cloudFetchDownloadTaskResult{res, err}:
				case <-bi.ctx.Done():
				}

				// Mark this task as no longer active
				_ = bi.downloadTasks.Dequeue()
			}(task)
		}

		// Wait for either: context canceled, or a small state change window
		select {
		case <-bi.ctx.Done():
			// Cancel all in-flight tasks and exit
			for bi.downloadTasks.Len() > 0 {
				t := bi.downloadTasks.Dequeue()
				t.cancel()
			}
			return
		default:
			// Yield briefly; avoids tight spin while allowing quick refills
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (bi *cloudIPCStreamIterator) Next() (io.Reader, error) {
	result, ok := <-bi.results
	if !ok {
		// Channel closed, no more results
		return nil, io.EOF
	}
	if result.err != nil {
		bi.Close()
		return nil, result.err
	}
	return result.data, nil
}

func (bi *cloudIPCStreamIterator) HasNext() bool {
	return (bi.pendingLinks.Len() > 0) || (bi.downloadTasks.Len() > 0)
}

func (bi *cloudIPCStreamIterator) Close() {
	bi.pendingLinks.Clear()
	for bi.downloadTasks.Len() > 0 {
		task := bi.downloadTasks.Dequeue()
		task.cancel()
	}
}

type cloudFetchDownloadTaskResult struct {
	data io.Reader
	err  error
}

type cloudFetchDownloadTask struct {
	ctx                context.Context
	cancel             context.CancelFunc
	useLz4Compression  bool
	minTimeToExpiry    time.Duration
	link               *cli_service.TSparkArrowResultLink
	resultChan         chan cloudFetchDownloadTaskResult
	speedThresholdMbps float64
}

func (cft *cloudFetchDownloadTask) GetResult() (io.Reader, error) {
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
			return nil, result.err
		}
		logger.Debug().Msgf(
			"CloudFetch: received data for link at offset %d row count %d",
			link.StartRowOffset,
			link.RowCount,
		)
		return result.data, nil
	}

	// This branch should never be reached. If you see this message - something got really wrong
	logger.Debug().Msgf(
		"CloudFetch: channel was closed before result was received; link at offset %d row count %d",
		link.StartRowOffset,
		link.RowCount,
	)
	return nil, nil
}

func (cft *cloudFetchDownloadTask) Run() {
	go func() {
		defer close(cft.resultChan)

		logger.Debug().Msgf(
			"CloudFetch: start downloading link at offset %d row count %d",
			cft.link.StartRowOffset,
			cft.link.RowCount,
		)
		data, err := fetchBatchBytes(cft.ctx, cft.link, cft.minTimeToExpiry, cft.speedThresholdMbps)
		if err != nil {
			cft.resultChan <- cloudFetchDownloadTaskResult{data: nil, err: err}
			return
		}

		// Read all data into memory before closing
		buf, err := io.ReadAll(getReader(data, cft.useLz4Compression))
		data.Close()
		if err != nil {
			cft.resultChan <- cloudFetchDownloadTaskResult{data: nil, err: err}
			return
		}

		logger.Debug().Msgf(
			"CloudFetch: downloaded data for link at offset %d row count %d",
			cft.link.StartRowOffset,
			cft.link.RowCount,
		)

		cft.resultChan <- cloudFetchDownloadTaskResult{data: bytes.NewReader(buf), err: nil}
	}()
}

// logCloudFetchSpeed calculates and logs download speed metrics
func logCloudFetchSpeed(fullURL string, contentLength int64, duration time.Duration, speedThresholdMbps float64) {
	if contentLength > 0 && duration.Seconds() > 0 {
		// Extract base URL (up to first ?)
		baseURL := fullURL
		if idx := strings.Index(baseURL, "?"); idx != -1 {
			baseURL = baseURL[:idx]
		}

		speedMbps := float64(contentLength) / (1024 * 1024) / duration.Seconds()

		logger.Info().Msgf("CloudFetch: Result File Download speed from cloud storage %s %.4f Mbps", baseURL, speedMbps)

		if speedMbps < speedThresholdMbps {
			logger.Warn().Msgf("CloudFetch: Results download is slower than threshold speed of %.4f Mbps", speedThresholdMbps)
		}
	}
}

func fetchBatchBytes(
	ctx context.Context,
	link *cli_service.TSparkArrowResultLink,
	minTimeToExpiry time.Duration,
	speedThresholdMbps float64,
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

	startTime := time.Now()
	client := http.DefaultClient
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if res.StatusCode != http.StatusOK {
		msg := fmt.Sprintf("%s: %s %d", errArrowRowsCloudFetchDownloadFailure, "HTTP error", res.StatusCode)
		return nil, dbsqlerrint.NewDriverError(ctx, msg, err)
	}

	// Log download speed metrics
	logCloudFetchSpeed(link.FileLink, res.ContentLength, time.Since(startTime), speedThresholdMbps)

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

// BatchIterator is the interface for iterating through Arrow batches
type BatchIterator interface {
	Next() (SparkArrowBatch, error)
	HasNext() bool
	Close()
}

// batchIterator wraps IPCStreamIterator to provide backward compatibility
type batchIterator struct {
	ipcIterator    IPCStreamIterator
	startRowOffset int64
}

// NewBatchIterator creates a BatchIterator from an IPCStreamIterator
func NewBatchIterator(ipcIterator IPCStreamIterator, startRowOffset int64) BatchIterator {
	return &batchIterator{
		ipcIterator:    ipcIterator,
		startRowOffset: startRowOffset,
	}
}

func (bi *batchIterator) Next() (SparkArrowBatch, error) {
	reader, err := bi.ipcIterator.Next()
	if err != nil {
		return nil, err
	}

	records, err := getArrowRecords(reader, bi.startRowOffset)
	if err != nil {
		return nil, err
	}

	// Calculate total rows in this batch
	totalRows := int64(0)
	for _, record := range records {
		totalRows += record.NumRows()
	}

	batch := &sparkArrowBatch{
		Delimiter:    rowscanner.NewDelimiter(bi.startRowOffset, totalRows),
		arrowRecords: records,
	}

	bi.startRowOffset += totalRows
	return batch, nil
}

func (bi *batchIterator) HasNext() bool {
	return bi.ipcIterator.HasNext()
}

func (bi *batchIterator) Close() {
	bi.ipcIterator.Close()
}
