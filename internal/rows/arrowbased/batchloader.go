package arrowbased

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"strconv"
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
	onFileDownloaded func(downloadMs int64),
) (IPCStreamIterator, dbsqlerr.DBError) {
	httpClient := http.DefaultClient
	if cfg.HTTPClient != nil {
		httpClient = cfg.HTTPClient
	}

	bi := &cloudIPCStreamIterator{
		ctx:              ctx,
		cfg:              cfg,
		startRowOffset:   startRowOffset,
		pendingLinks:     NewQueue[cli_service.TSparkArrowResultLink](),
		downloadTasks:    NewQueue[cloudFetchDownloadTask](),
		httpClient:       httpClient,
		onFileDownloaded: onFileDownloaded,
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
	onFileDownloaded func(downloadMs int64),
) (BatchIterator, dbsqlerr.DBError) {
	ipcIterator, err := NewCloudIPCStreamIterator(ctx, files, startRowOffset, cfg, onFileDownloaded)
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
	ctx              context.Context
	cfg              *config.Config
	startRowOffset   int64
	pendingLinks     Queue[cli_service.TSparkArrowResultLink]
	downloadTasks    Queue[cloudFetchDownloadTask]
	httpClient       *http.Client
	onFileDownloaded func(downloadMs int64) // nil for non-telemetry paths
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

		cancelCtx, cancelFn := context.WithCancel(bi.ctx) //nolint:gosec // cancelFn stored in task and called on completion
		task := &cloudFetchDownloadTask{
			ctx:                cancelCtx,
			cancel:             cancelFn,
			useLz4Compression:  bi.cfg.UseLz4Compression,
			link:               link,
			resultChan:         make(chan cloudFetchDownloadTaskResult),
			minTimeToExpiry:    bi.cfg.MinTimeToExpiry,
			speedThresholdMbps: bi.cfg.CloudFetchSpeedThresholdMbps,
			httpClient:         bi.httpClient,
			retryMax:           bi.cfg.RetryMax,
			retryWaitMin:       bi.cfg.RetryWaitMin,
			retryWaitMax:       bi.cfg.RetryWaitMax,
		}
		task.Run()
		bi.downloadTasks.Enqueue(task)
	}

	task := bi.downloadTasks.Dequeue()
	if task == nil {
		return nil, io.EOF
	}

	data, downloadMs, err := task.GetResult()

	// once we've got an errored out task - cancel the remaining ones
	if err != nil {
		bi.Close()
		return nil, err
	}

	// explicitly call cancel function on successfully completed task to avoid context leak
	task.cancel()

	// Notify telemetry with per-file download time (matches JDBC's per-chunk HTTP GET timing).
	// Always invoke for successfully completed downloads so the caller can count files;
	// sub-millisecond downloads report downloadMs=0 and the caller decides whether to
	// include them in timing aggregation.
	if bi.onFileDownloaded != nil {
		bi.onFileDownloaded(downloadMs)
	}

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
	data       io.Reader
	err        error
	downloadMs int64 // wall-clock time for HTTP GET + decompression
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
	retryMax           int
	retryWaitMin       time.Duration
	retryWaitMax       time.Duration
}

func (cft *cloudFetchDownloadTask) GetResult() (io.Reader, int64, error) {
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
		return result.data, result.downloadMs, nil
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
		downloadStart := time.Now()
		// fetchBatchBytes now buffers the body in memory and retries on
		// mid-stream failures, so the returned reader is always a
		// *bytes.Reader and needs no Close.
		data, err := fetchBatchBytes(
			cft.ctx,
			cft.link,
			cft.minTimeToExpiry,
			cft.speedThresholdMbps,
			cft.httpClient,
			cft.retryMax,
			cft.retryWaitMin,
			cft.retryWaitMax,
		)
		if err != nil {
			cft.sendResult(cloudFetchDownloadTaskResult{data: nil, err: err})
			return
		}

		// Decompression sits outside the retry loop on purpose: a malformed
		// LZ4 frame is data corruption, not a transient network condition,
		// and won't recover on retry.
		buf, err := io.ReadAll(getReader(data, cft.useLz4Compression))
		downloadMs := time.Since(downloadStart).Milliseconds()
		if err != nil {
			cft.sendResult(cloudFetchDownloadTaskResult{data: nil, err: err})
			return
		}

		logger.Debug().Msgf(
			"CloudFetch: downloaded data for link at offset %d row count %d",
			cft.link.StartRowOffset,
			cft.link.RowCount,
		)

		cft.sendResult(cloudFetchDownloadTaskResult{data: bytes.NewReader(buf), err: nil, downloadMs: downloadMs})
	}()
}

// sendResult delivers the download result to the consumer, but drops it if the
// task's context has already been cancelled. Without this guard, a goroutine
// that finishes its work after the iterator is closed blocks forever on the
// unbuffered resultChan and pins the downloaded buffer in the heap (issue #356).
func (cft *cloudFetchDownloadTask) sendResult(result cloudFetchDownloadTaskResult) {
	select {
	case cft.resultChan <- result:
	case <-cft.ctx.Done():
	}
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

// fetchBatchBytes downloads a single Cloud Fetch result link from object
// storage and returns the raw (still-compressed, if any) response body
// buffered in memory. Both connection-time failures and mid-stream body-read
// failures are retried up to retryMax times with exponential backoff and
// equal jitter, alongside HTTP 408/429/500/502/503/504. The body read
// happens inside the retry loop on purpose: with multi-MB S3 objects, a TCP
// RST or truncated response surfaces as an io.ReadAll error *after* the
// 200 OK headers have already arrived, and that's exactly the failure mode
// the customer hits at scale. Decompression and IPC parsing are left to the
// caller — those errors aren't transient, so retrying them is wasted work.
//
// Link expiry is rechecked after each backoff: a long retry chain may outlive
// a presigned URL, and continuing past expiry is guaranteed to fail.
func fetchBatchBytes(
	ctx context.Context,
	link *cli_service.TSparkArrowResultLink,
	minTimeToExpiry time.Duration,
	speedThresholdMbps float64,
	httpClient *http.Client,
	retryMax int,
	retryWaitMin time.Duration,
	retryWaitMax time.Duration,
) (io.Reader, error) {
	if retryMax < 0 {
		retryMax = 0
	}

	var (
		lastErr        error
		lastStatus     int
		lastRetryAfter string
	)

	for attempt := 0; attempt <= retryMax; attempt++ {
		if attempt > 0 {
			wait := cloudFetchBackoff(attempt, retryWaitMin, retryWaitMax, lastRetryAfter)
			logger.Debug().Msgf(
				"CloudFetch: retrying download of link at offset %d (attempt %d/%d) in %v; lastStatus=%d lastErr=%v",
				link.StartRowOffset, attempt, retryMax, wait, lastStatus, lastErr,
			)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(wait):
			}
		}

		// Check link expiry *after* backoff: a long retry chain may outlive a
		// presigned URL, and there's no point spending another HTTP attempt
		// (or another retry) on a link we know will be rejected.
		if isLinkExpired(link.ExpiryTime, minTimeToExpiry) {
			return nil, errors.New(dbsqlerr.ErrLinkExpired)
		}

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
			// Caller cancellation is terminal; otherwise treat transport errors
			// (TCP RST, TLS timeout, etc.) as transient.
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			lastErr = err
			lastStatus = 0
			lastRetryAfter = ""
			continue
		}

		if res.StatusCode == http.StatusOK {
			// Drain the full body inside the retry loop so a mid-stream
			// failure (TCP RST, S3 cutting the connection partway through a
			// multi-MB object, server-claimed Content-Length not delivered)
			// is retried just like a header-time error.
			buf, readErr := io.ReadAll(res.Body)
			res.Body.Close() //nolint:errcheck,gosec // G104: close after drain
			if readErr != nil {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				lastErr = readErr
				lastStatus = 0
				lastRetryAfter = ""
				continue
			}
			logCloudFetchSpeed(link.FileLink, int64(len(buf)), time.Since(startTime), speedThresholdMbps)
			return bytes.NewReader(buf), nil
		}

		// Drain and close so the underlying connection can be reused.
		_, _ = io.Copy(io.Discard, res.Body)
		res.Body.Close() //nolint:errcheck,gosec // G104: closing after drain

		lastStatus = res.StatusCode
		lastErr = nil
		lastRetryAfter = ""
		if res.Header != nil {
			lastRetryAfter = res.Header.Get("Retry-After")
		}

		if !isCloudFetchRetryableStatus(res.StatusCode) {
			msg := fmt.Sprintf("%s: %s %d", errArrowRowsCloudFetchDownloadFailure, "HTTP error", res.StatusCode)
			return nil, dbsqlerrint.NewDriverError(ctx, msg, nil)
		}
	}

	if lastStatus != 0 {
		msg := fmt.Sprintf("%s: %s %d (after %d retries)", errArrowRowsCloudFetchDownloadFailure, "HTTP error", lastStatus, retryMax)
		return nil, dbsqlerrint.NewDriverError(ctx, msg, nil)
	}
	msg := fmt.Sprintf("%s: %v (after %d retries)", errArrowRowsCloudFetchDownloadFailure, lastErr, retryMax)
	return nil, dbsqlerrint.NewDriverError(ctx, msg, lastErr)
}

// cloudFetchRetryableStatuses lists HTTP status codes from object storage that
// indicate transient conditions and warrant a retry. Mirrors AWS S3 guidance
// for SlowDown (503) / InternalError (500) plus the general 408/429/502/504.
var cloudFetchRetryableStatuses = map[int]struct{}{
	http.StatusRequestTimeout:      {}, // 408
	http.StatusTooManyRequests:     {}, // 429
	http.StatusInternalServerError: {}, // 500
	http.StatusBadGateway:          {}, // 502
	http.StatusServiceUnavailable:  {}, // 503
	http.StatusGatewayTimeout:      {}, // 504
}

func isCloudFetchRetryableStatus(status int) bool {
	_, ok := cloudFetchRetryableStatuses[status]
	return ok
}

// cloudFetchBackoff returns the wait before retry attempt N (1-based). The
// base delay is exponential — waitMin * 2^(attempt-1) capped at waitMax — with
// equal jitter applied: the actual sleep is uniformly distributed in
// [base/2, base]. Equal jitter (rather than no jitter) is used to spread
// synchronized retries across the up-to-MaxDownloadThreads concurrent
// downloads, which would otherwise hammer the storage endpoint in lockstep
// after a region-wide blip. If the server returned a parseable integer
// Retry-After header, that value (in seconds) is honored instead, capped at
// waitMax. HTTP-date Retry-After values are ignored — same as the Thrift
// client's backoff.
func cloudFetchBackoff(attempt int, waitMin, waitMax time.Duration, retryAfter string) time.Duration {
	if retryAfter != "" {
		if secs, err := strconv.ParseInt(retryAfter, 10, 64); err == nil && secs >= 0 {
			d := time.Duration(secs) * time.Second
			if d > waitMax {
				return waitMax
			}
			return d
		}
	}

	expo := float64(waitMin) * math.Pow(2, float64(attempt-1))
	if expo > float64(waitMax) || math.IsInf(expo, 0) {
		expo = float64(waitMax)
	}
	base := time.Duration(expo)
	if base <= 0 {
		return 0
	}
	half := base / 2
	if half <= 0 {
		return base
	}
	return half + time.Duration(rand.Int63n(int64(half))) //nolint:gosec // G404: jitter only, non-cryptographic
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
