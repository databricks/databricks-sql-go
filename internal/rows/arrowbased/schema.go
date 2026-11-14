package arrowbased

import (
	"bytes"
	"context"
	"io"

	"github.com/apache/arrow-go/v18/arrow/ipc"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/config"
	dbsqlerrint "github.com/databricks/databricks-sql-go/internal/errors"
)

func schemaFromFirstBatch(ctx context.Context, rowSet *cli_service.TRowSet, cfg *config.Config, onDownload func(int64)) ([]byte, IPCStreamIterator, dbsqlerr.DBError) {
	var stream io.Reader
	var cloud IPCStreamIterator
	var offset, count int64
	if len(rowSet.ResultLinks) > 0 {
		var err dbsqlerr.DBError
		cloud, err = NewCloudIPCStreamIterator(ctx, rowSet.ResultLinks, rowSet.StartRowOffset, cfg, onDownload)
		if err != nil {
			return nil, nil, err
		}
		var fetchErr error
		stream, offset, count, fetchErr = cloud.(positionedIPCStreamIterator).NextWithMetadata()
		if fetchErr != nil {
			cloud.Close()
			return nil, nil, dbsqlerrint.NewDriverError(ctx, errArrowRowsUnableToReadBatch, fetchErr)
		}
	} else if len(rowSet.ArrowBatches) > 0 && len(rowSet.ArrowBatches[0].Batch) > 0 {
		compressed := cfg != nil && cfg.UseLz4Compression
		stream = getReader(bytes.NewReader(rowSet.ArrowBatches[0].Batch), compressed)
	} else {
		return nil, nil, nil
	}
	var prefix bytes.Buffer
	messages := ipc.NewMessageReader(io.TeeReader(stream, &prefix))
	message, err := messages.Message()
	defer messages.Release()
	if err != nil {
		if cloud != nil {
			cloud.Close()
			return nil, nil, dbsqlerrint.NewDriverError(ctx, errArrowRowsUnableToReadBatch, err).WithCategory(dbsqlerrint.CategoryArrowSchemaParsing)
		}
		return nil, nil, nil
	}
	var schemaBytes []byte
	if message.Type() == ipc.MessageSchema {
		schemaBytes = prefix.Bytes()
	}
	if cloud == nil {
		return schemaBytes, nil, nil
	}
	return schemaBytes, &prefetchedIPCStreamIterator{
		source: cloud,
		first:  io.MultiReader(bytes.NewReader(prefix.Bytes()), stream), offset: offset, count: count,
	}, nil
}

type prefetchedIPCStreamIterator struct {
	source IPCStreamIterator
	first  io.Reader
	offset int64
	count  int64
}

func (iterator *prefetchedIPCStreamIterator) Next() (io.Reader, error) {
	stream, _, _, err := iterator.NextWithMetadata()
	return stream, err
}

func (iterator *prefetchedIPCStreamIterator) NextWithMetadata() (io.Reader, int64, int64, error) {
	if iterator.first != nil {
		stream := iterator.first
		iterator.first = nil
		return stream, iterator.offset, iterator.count, nil
	}
	return iterator.source.(positionedIPCStreamIterator).NextWithMetadata()
}

func (iterator *prefetchedIPCStreamIterator) HasNext() bool {
	return iterator.first != nil || iterator.source.HasNext()
}

func (iterator *prefetchedIPCStreamIterator) Close() {
	iterator.first = nil
	iterator.source.Close()
}
