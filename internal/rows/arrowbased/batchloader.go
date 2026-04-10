package arrowbased

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/databricks/databricks-sql-go/internal/rows/rowscanner"
	"github.com/pierrec/lz4/v4"
	"github.com/pkg/errors"

	"net/http"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
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
	httpClient := http.DefaultClient
	if cfg.UserConfig.CloudFetchConfig.HTTPClient != nil {
		httpClient = cfg.UserConfig.CloudFetchConfig.HTTPClient
	}

	bi := &cloudIPCStreamIterator{
		ctx:            ctx,
		cfg:            cfg,
		startRowOffset: startRowOffset,
		pendingLinks:   NewQueue[cli_service.TSparkArrowResultLink](),
		downloadTasks:  NewQueue[cloudFetchDownloadTask](),
		httpClient:     httpClient,
	}

	for _, link := range files {
		bi.pendingLinks.Enqueue(link)
	}

	return bi, nil
}

// NewCloudBatchIterator creates a cloud-based BatchIterator for backward compatibility.
// arrowSchemaBytes is the authoritative schema from GetResultSetMetadata, used to
// override stale column names in cached Arrow IPC files.
func NewCloudBatchIterator(
	ctx context.Context,
	files []*cli_service.TSparkArrowResultLink,
	startRowOffset int64,
	arrowSchemaBytes []byte,
	cfg *config.Config,
) (BatchIterator, dbsqlerr.DBError) {
	ipcIterator, err := NewCloudIPCStreamIterator(ctx, files, startRowOffset, cfg)
	if err != nil {
		return nil, err
	}

	var overrideSchema *arrow.Schema
	if len(arrowSchemaBytes) > 0 {
		var schemaErr error
		overrideSchema, schemaErr = schemaFromIPCBytes(arrowSchemaBytes)
		if schemaErr != nil {
			logger.Warn().Msgf("CloudFetch: failed to parse override schema: %v", schemaErr)
		}
	}

	return &batchIterator{
		ipcIterator:    ipcIterator,
		startRowOffset: startRowOffset,
		overrideSchema: overrideSchema,
	}, nil
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
	httpClient     *http.Client
}

var _ IPCStreamIterator = (*cloudIPCStreamIterator)(nil)

func (bi *cloudIPCStreamIterator) Next() (io.Reader, error) {
	for (bi.downloadTasks.Len() < bi.cfg.MaxDownloadThreads) && (bi.pendingLinks.Len() > 0) {
		link := bi.pendingLinks.Dequeue()
		logger.Debug().Msgf(
			"CloudFetch: schedule link at offset %d row count %d",
			link.StartRowOffset,
			link.RowCount,
		)

		cancelCtx, cancelFn := context.WithCancel(bi.ctx)
		task := &cloudFetchDownloadTask{
			ctx:                cancelCtx,
			cancel:             cancelFn,
			useLz4Compression:  bi.cfg.UseLz4Compression,
			link:               link,
			resultChan:         make(chan cloudFetchDownloadTaskResult),
			minTimeToExpiry:    bi.cfg.MinTimeToExpiry,
			speedThresholdMbps: bi.cfg.CloudFetchSpeedThresholdMbps,
			httpClient:         bi.httpClient,
		}
		task.Run()
		bi.downloadTasks.Enqueue(task)
	}

	task := bi.downloadTasks.Dequeue()
	if task == nil {
		return nil, io.EOF
	}

	data, err := task.GetResult()

	// once we've got an errored out task - cancel the remaining ones
	if err != nil {
		bi.Close()
		return nil, err
	}

	// explicitly call cancel function on successfully completed task to avoid context leak
	task.cancel()
	return data, nil
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
	httpClient         *http.Client
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
		data, err := fetchBatchBytes(cft.ctx, cft.link, cft.minTimeToExpiry, cft.speedThresholdMbps, cft.httpClient)
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
	httpClient *http.Client,
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
	res, err := httpClient.Do(req)
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
	overrideSchema *arrow.Schema // authoritative schema to fix stale CloudFetch column names
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

	// When using CloudFetch, cached Arrow IPC files may contain stale column
	// names from a previous query. Replace the embedded schema with the
	// authoritative schema from GetResultSetMetadata.
	if bi.overrideSchema != nil && len(records) > 0 && len(bi.overrideSchema.Fields()) == len(records[0].Columns()) {
		for i, rec := range records {
			sar, ok := rec.(*sparkArrowRecord)
			if !ok {
				continue
			}
			corrected := array.NewRecord(bi.overrideSchema, sar.Columns(), sar.NumRows())
			sar.Release()
			records[i] = &sparkArrowRecord{
				Delimiter: sar.Delimiter,
				Record:    corrected,
			}
		}
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

// schemaFromIPCBytes parses Arrow schema bytes (IPC format) into an *arrow.Schema.
func schemaFromIPCBytes(schemaBytes []byte) (*arrow.Schema, error) {
	reader, err := ipc.NewReader(bytes.NewReader(schemaBytes))
	if err != nil {
		return nil, err
	}
	defer reader.Release()
	return reader.Schema(), nil
}
