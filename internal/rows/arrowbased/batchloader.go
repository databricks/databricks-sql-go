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

	"github.com/apache/arrow/go/v12/arrow/ipc"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	dbsqlerrint "github.com/databricks/databricks-sql-go/internal/errors"
	"github.com/databricks/databricks-sql-go/logger"
)

type BatchIterator interface {
	Next() (SparkArrowBatch, error)
	HasNext() bool
	Close()
}

func NewCloudBatchIterator(
	ctx context.Context,
	files []*cli_service.TSparkArrowResultLink,
	startRowOffset int64,
	cfg *config.Config,
) (BatchIterator, dbsqlerr.DBError) {
	bi := &cloudBatchIterator{
		ctx:            ctx,
		cfg:            cfg,
		startRowOffset: startRowOffset,
		pendingLinks:   NewQueue[cli_service.TSparkArrowResultLink](),
		downloadTasks:  NewQueue[cloudFetchDownloadTask](),
	}

	for _, link := range files {
		bi.pendingLinks.Enqueue(link)
	}

	return bi, nil
}

func NewLocalBatchIterator(
	ctx context.Context,
	batches []*cli_service.TSparkArrowBatch,
	startRowOffset int64,
	arrowSchemaBytes []byte,
	cfg *config.Config,
) (BatchIterator, dbsqlerr.DBError) {
	bi := &localBatchIterator{
		cfg:              cfg,
		startRowOffset:   startRowOffset,
		arrowSchemaBytes: arrowSchemaBytes,
		batches:          batches,
		index:            -1,
	}

	return bi, nil
}

type localBatchIterator struct {
	cfg              *config.Config
	startRowOffset   int64
	arrowSchemaBytes []byte
	batches          []*cli_service.TSparkArrowBatch
	index            int
}

var _ BatchIterator = (*localBatchIterator)(nil)

func (bi *localBatchIterator) Next() (SparkArrowBatch, error) {
	cnt := len(bi.batches)
	bi.index++
	if bi.index < cnt {
		ab := bi.batches[bi.index]

		reader := io.MultiReader(
			bytes.NewReader(bi.arrowSchemaBytes),
			getReader(bytes.NewReader(ab.Batch), bi.cfg.UseLz4Compression),
		)

		records, err := getArrowRecords(reader, bi.startRowOffset)
		if err != nil {
			return &sparkArrowBatch{}, err
		}

		batch := sparkArrowBatch{
			Delimiter:    rowscanner.NewDelimiter(bi.startRowOffset, ab.RowCount),
			arrowRecords: records,
		}

		bi.startRowOffset += ab.RowCount // advance to beginning of the next batch

		return &batch, nil
	}

	bi.index = cnt
	return nil, io.EOF
}

func (bi *localBatchIterator) HasNext() bool {
	return bi.index < len(bi.batches)
}

func (bi *localBatchIterator) Close() {
	bi.index = len(bi.batches)
}

type cloudBatchIterator struct {
	ctx            context.Context
	cfg            *config.Config
	startRowOffset int64
	pendingLinks   Queue[cli_service.TSparkArrowResultLink]
	downloadTasks  Queue[cloudFetchDownloadTask]
}

var _ BatchIterator = (*cloudBatchIterator)(nil)

func (bi *cloudBatchIterator) Next() (SparkArrowBatch, error) {
	for (bi.downloadTasks.Len() < bi.cfg.MaxDownloadThreads) && (bi.pendingLinks.Len() > 0) {
		link := bi.pendingLinks.Dequeue()
		logger.Debug().Msgf(
			"CloudFetch: schedule link at offset %d row count %d",
			link.StartRowOffset,
			link.RowCount,
		)
		task := &cloudFetchDownloadTask{
			ctx:               bi.ctx,
			useLz4Compression: bi.cfg.UseLz4Compression,
			link:              link,
			resultChan:        make(chan SparkArrowBatch),
			errorChan:         make(chan error),
			minTimeToExpiry:   bi.cfg.MinTimeToExpiry,
		}
		task.Run()
		bi.downloadTasks.Enqueue(task)
	}

	task := bi.downloadTasks.Dequeue()
	if task == nil {
		return nil, io.EOF
	}

	return task.GetResult()
}

func (bi *cloudBatchIterator) HasNext() bool {
	return (bi.pendingLinks.Len() > 0) || (bi.downloadTasks.Len() > 0)
}

func (bi *cloudBatchIterator) Close() {
	bi.pendingLinks.Clear() // Clear the list
	// TODO: Cancel all download tasks
	bi.downloadTasks.Clear() // Clear the list
}

type cloudFetchDownloadTask struct {
	ctx               context.Context
	useLz4Compression bool
	minTimeToExpiry   time.Duration
	link              *cli_service.TSparkArrowResultLink
	resultChan        chan SparkArrowBatch
	errorChan         chan error
}

func (cft *cloudFetchDownloadTask) GetResult() (SparkArrowBatch, error) {
	link := cft.link

	select {
	case batch, ok := <-cft.resultChan:
		if ok {
			logger.Debug().Msgf(
				"CloudFetch: received data for link at offset %d row count %d",
				link.StartRowOffset,
				link.RowCount,
			)
			return batch, nil
		}
	case err, ok := <-cft.errorChan:
		if ok {
			logger.Debug().Msgf(
				"CloudFetch: failed to download link at offset %d row count %d",
				link.StartRowOffset,
				link.RowCount,
			)
			return nil, err
		}
	}

	logger.Debug().Msgf(
		"CloudFetch: this should never happen; link at offset %d row count %d",
		link.StartRowOffset,
		link.RowCount,
	)
	return nil, nil // TODO: ???
}

func (cft *cloudFetchDownloadTask) Run() {
	go func() {
		link := cft.link

		logger.Debug().Msgf(
			"CloudFetch: start downloading link at offset %d row count %d",
			link.StartRowOffset,
			link.RowCount,
		)
		data, err := cft.fetchBatchBytes()
		if err != nil {
			cft.errorChan <- err
			return
		}

		// TODO: error handling?
		defer data.Close()

		reader := getReader(data, cft.useLz4Compression)

		records, err := getArrowRecords(reader, cft.link.StartRowOffset)
		if err != nil {
			cft.errorChan <- err
			return
		}

		batch := sparkArrowBatch{
			Delimiter:    rowscanner.NewDelimiter(cft.link.StartRowOffset, cft.link.RowCount),
			arrowRecords: records,
		}
		cft.resultChan <- &batch
	}()
}

func (cft *cloudFetchDownloadTask) fetchBatchBytes() (io.ReadCloser, error) {
	if isLinkExpired(cft.link.ExpiryTime, cft.minTimeToExpiry) {
		return nil, errors.New(dbsqlerr.ErrLinkExpired)
	}

	// TODO: Retry on HTTP errors
	req, err := http.NewRequestWithContext(cft.ctx, "GET", cft.link.FileLink, nil)
	if err != nil {
		return nil, err
	}

	client := http.DefaultClient
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if res.StatusCode != http.StatusOK {
		return nil, dbsqlerrint.NewDriverError(cft.ctx, errArrowRowsCloudFetchDownloadFailure, err)
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
