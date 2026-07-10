package arrowbased

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/databricks/databricks-sql-go/internal/retry"
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

// positionedIPCStreamIterator is an optional extension of IPCStreamIterator for
// streams that carry server-declared positioning metadata alongside each IPC
// payload. It is implemented ONLY by the CloudFetch iterator: CloudFetch result
// links carry an authoritative StartRowOffset and RowCount, and the Arrow IPC
// files they point at may be padded with extra rows beyond RowCount. batchIterator
// uses this metadata to (a) anchor each batch at its true stream offset and
// (b) cap the decoded records to RowCount so padding rows are not surfaced as
// real data (see limitArrowRecords).
//
// The inline/local Arrow path intentionally does NOT implement this: those
// batches are returned verbatim by the server with no padding, and their
// per-batch RowCount has historically been untrusted, so capping there would
// risk silently dropping rows. NextWithMetadata returns expectedRows < 0 to mean
// "row count unknown — do not cap".
type positionedIPCStreamIterator interface {
	// NextWithMetadata returns the next IPC payload along with its absolute
	// stream start offset and the server-declared row count (expectedRows). An
	// expectedRows < 0 means the count is unknown and no capping should occur.
	NextWithMetadata() (reader io.Reader, startRowOffset int64, expectedRows int64, err error)
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
var _ positionedIPCStreamIterator = (*cloudIPCStreamIterator)(nil)

func (bi *cloudIPCStreamIterator) Next() (io.Reader, error) {
	reader, _, _, err := bi.NextWithMetadata()
	return reader, err
}

// NextWithMetadata returns the next downloaded CloudFetch IPC payload together
// with the link's authoritative StartRowOffset and RowCount. The Arrow file may
// contain padding rows beyond RowCount; the caller caps to RowCount.
func (bi *cloudIPCStreamIterator) NextWithMetadata() (io.Reader, int64, int64, error) {
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
		return nil, 0, 0, io.EOF
	}

	data, downloadMs, err := task.GetResult()

	// once we've got an errored out task - cancel the remaining ones
	if err != nil {
		bi.Close()
		return nil, 0, 0, err
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

	return data, task.link.StartRowOffset, task.link.RowCount, nil
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
		rawBody, err := fetchBatchBytes(
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

		buf := rawBody
		if cft.useLz4Compression {
			// Decompression sits outside the retry loop: malformed LZ4 is data
			// corruption, not a transient network condition.
			buf, err = io.ReadAll(lz4.NewReader(bytes.NewReader(rawBody)))
			if err != nil {
				cft.sendResult(cloudFetchDownloadTaskResult{data: nil, err: err})
				return
			}
		}
		downloadMs := time.Since(downloadStart).Milliseconds()

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

// fetchBatchBytes downloads a single CloudFetch result link and returns the
// raw response body, still compressed if the server used LZ4. Connection-time
// failures, retryable HTTP statuses, and mid-stream body read failures are
// retried up to retryMax times with exponential backoff and equal jitter.
// Decompression and IPC parsing stay with the caller because those failures are
// not transient network conditions.
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
) ([]byte, error) {
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
			wait := retry.Backoff(attempt, retryWaitMin, retryWaitMax, lastRetryAfter)
			logger.Debug().Msgf(
				"CloudFetch: retrying download of link at offset %d (attempt %d/%d) in %v; lastStatus=%d lastErr=%v",
				link.StartRowOffset, attempt, retryMax, wait, lastStatus, lastErr,
			)
			t := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				if !t.Stop() {
					<-t.C
				}
				return nil, ctx.Err()
			case <-t.C:
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
			// Read the full body inside the retry loop so truncated 200 OK
			// responses are retried just like header-time failures.
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
			return buf, nil
		}

		// Drain and close so the underlying connection can be reused.
		_, _ = io.Copy(io.Discard, res.Body)
		res.Body.Close() //nolint:errcheck,gosec // G104: closing after drain

		lastStatus = res.StatusCode
		lastErr = nil
		lastRetryAfter = res.Header.Get("Retry-After")

		if !retry.IsRetryableStatus(res.StatusCode) {
			msg := fmt.Sprintf("%s: %s %d", errArrowRowsCloudFetchDownloadFailure, "HTTP error", res.StatusCode)
			return nil, dbsqlerrint.NewDriverError(ctx, msg, nil)
		}
	}

	if lastStatus != 0 {
		// lastErr is nil here by construction: the HTTP-status branch above
		// explicitly clears it on every iteration. The status code is captured
		// in msg, so there's no underlying error to wrap.
		return nil, dbsqlerrint.NewDriverError(ctx, fmt.Sprintf("%s: %s %d (after %d retries)", errArrowRowsCloudFetchDownloadFailure, "HTTP error", lastStatus, retryMax), nil)
	}
	msg := fmt.Sprintf("%s: %v (after %d retries)", errArrowRowsCloudFetchDownloadFailure, lastErr, retryMax)
	return nil, dbsqlerrint.NewDriverError(ctx, msg, lastErr)
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
	// startRowOffset is the absolute offset of this batch within the result
	// stream. For positioned (CloudFetch) streams it comes from the server's
	// link metadata; otherwise we track it locally by accumulating decoded rows.
	startRowOffset := bi.startRowOffset
	// expectedRows is the server-declared row count for this batch. A value < 0
	// means "unknown" and disables capping (the inline/local path).
	expectedRows := int64(-1)
	var reader io.Reader
	var err error
	if positionedIterator, ok := bi.ipcIterator.(positionedIPCStreamIterator); ok {
		reader, startRowOffset, expectedRows, err = positionedIterator.NextWithMetadata()
	} else {
		reader, err = bi.ipcIterator.Next()
	}
	if err != nil {
		return nil, err
	}

	records, err := getArrowRecords(reader, startRowOffset)
	if err != nil {
		return nil, err
	}

	// Cap the decoded records to the server-declared row count, dropping the
	// padding rows some CloudFetch Arrow files carry beyond their link's
	// RowCount. Only cap when the count is strictly positive: expectedRows == 0
	// with decoded rows is treated as "untrustworthy, do not cap" rather than
	// silently dropping the whole batch (see #371 review F1).
	if expectedRows > 0 {
		records = limitArrowRecords(records, expectedRows)
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
		Delimiter:    rowscanner.NewDelimiter(startRowOffset, totalRows),
		arrowRecords: records,
	}

	// Advance the local offset for the next non-positioned batch. Positioned
	// streams overwrite startRowOffset from server metadata on the next call.
	bi.startRowOffset = startRowOffset + totalRows
	return batch, nil
}

// limitArrowRecords caps a decoded batch to expectedRows, releasing any records
// (and the tail of a partially-kept record) that fall beyond the server-declared
// count. It is the mechanism that strips CloudFetch Arrow padding rows.
//
// Contract:
//   - Callers must only invoke this when expectedRows is trustworthy and the
//     batch may be over-long; expectedRows < 0 is treated as "unknown" and the
//     records are returned unchanged.
//   - When a record straddles the boundary it is sliced with NewSlice(0, remaining):
//     the slice bounds are record-relative (0-based within the record), while the
//     Delimiter's start is the ABSOLUTE stream offset of the record. Keep these two
//     distinct — do not pass the absolute start as a slice index.
func limitArrowRecords(records []SparkArrowRecord, expectedRows int64) []SparkArrowRecord {
	if expectedRows < 0 {
		return records
	}

	remaining := expectedRows
	limited := records[:0]
	for _, record := range records {
		if remaining <= 0 {
			record.Release()
			continue
		}

		if record.NumRows() <= remaining {
			limited = append(limited, record)
			remaining -= record.NumRows()
			continue
		}

		start := record.Start()
		sliced := record.NewSlice(0, remaining)
		record.Release()
		if sliced != nil {
			limited = append(limited, &sparkArrowRecord{
				Delimiter: rowscanner.NewDelimiter(start, sliced.NumRows()),
				Record:    sliced,
			})
		}
		remaining = 0
	}

	return limited
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
