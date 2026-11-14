package arrowbased

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
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
	schemaBytes, replay, err := peekIPCSchema(stream)
	if err != nil {
		if cloud != nil {
			cloud.Close()
			return nil, nil, dbsqlerrint.NewDriverError(ctx, errArrowRowsUnableToReadBatch, err).WithCategory(dbsqlerrint.CategoryArrowSchemaParsing)
		}
		return nil, nil, nil
	}
	if cloud == nil {
		return schemaBytes, nil, nil
	}
	return schemaBytes, &prefetchedIPCStreamIterator{
		source: cloud,
		first:  replay, offset: offset, count: count,
	}, nil
}

// Inspect only message metadata; record bodies remain unread, including under LZ4.
func peekIPCSchema(stream io.Reader) ([]byte, io.Reader, error) {
	var prefix bytes.Buffer
	reader := io.TeeReader(stream, &prefix)
	var word [4]byte
	if _, err := io.ReadFull(reader, word[:]); err != nil {
		return nil, nil, err
	}
	length := binary.LittleEndian.Uint32(word[:])
	if length == 0xffffffff {
		if _, err := io.ReadFull(reader, word[:]); err != nil {
			return nil, nil, err
		}
		length = binary.LittleEndian.Uint32(word[:])
	}
	if length == 0 {
		return nil, io.MultiReader(bytes.NewReader(prefix.Bytes()), stream), nil
	}
	headerSize := prefix.Len()
	if _, err := io.CopyN(&prefix, stream, int64(length)); err != nil {
		return nil, nil, err
	}
	isSchema, err := ipcMetadataIsSchema(prefix.Bytes()[headerSize:])
	if err != nil {
		return nil, nil, err
	}
	var schema []byte
	if isSchema {
		schema = prefix.Bytes()
	}
	return schema, io.MultiReader(bytes.NewReader(prefix.Bytes()), stream), nil
}

func ipcMetadataIsSchema(metadata []byte) (isSchema bool, err error) {
	if len(metadata) < 4 {
		return false, io.ErrUnexpectedEOF
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			isSchema, err = false, fmt.Errorf("databricks: invalid Arrow message metadata: %v", recovered)
		}
	}()
	metaBuffer, bodyBuffer := memory.NewBufferBytes(metadata), memory.NewBufferBytes(nil)
	defer metaBuffer.Release()
	defer bodyBuffer.Release()
	message := ipc.NewMessage(metaBuffer, bodyBuffer)
	defer message.Release()
	return message.Type() == ipc.MessageSchema, nil
}

func hasIPCSchema(payload []byte) (bool, error) {
	if len(payload) < 4 {
		return false, io.ErrUnexpectedEOF
	}
	length, offset := binary.LittleEndian.Uint32(payload[:4]), 4
	if length == 0xffffffff {
		if len(payload) < 8 {
			return false, io.ErrUnexpectedEOF
		}
		length, offset = binary.LittleEndian.Uint32(payload[4:8]), 8
	}
	if length == 0 {
		return false, nil
	}
	if int64(length) > int64(len(payload)-offset) {
		return false, io.ErrUnexpectedEOF
	}
	return ipcMetadataIsSchema(payload[offset : offset+int(length)])
}

type prefetchedIPCStreamIterator struct {
	source IPCStreamIterator
	first  io.Reader
	offset int64
	count  int64
}

// schemaIPCStreamIterator replaces cached IPC headers without decoding record data.
type schemaIPCStreamIterator struct {
	source            IPCStreamIterator
	schema            []byte
	replacementSchema *arrow.Schema
}

func (iterator *schemaIPCStreamIterator) Next() (io.Reader, error) {
	stream, err := iterator.source.Next()
	if err != nil || len(iterator.schema) == 0 {
		return stream, err
	}
	reader, err := ipc.NewReader(stream)
	if err != nil {
		return nil, cloudArrowSchemaError(err)
	}
	defer reader.Release()
	if iterator.replacementSchema == nil {
		iterator.replacementSchema, err = schemaFromIPCBytes(iterator.schema)
		if err != nil {
			return nil, cloudArrowSchemaError(fmt.Errorf("invalid replacement schema: %w", err))
		}
	}
	if reader.Schema().NumFields() != iterator.replacementSchema.NumFields() {
		return nil, cloudArrowSchemaError(fmt.Errorf("field count differs: file=%d metadata=%d", reader.Schema().NumFields(), iterator.replacementSchema.NumFields()))
	}
	for i := range reader.Schema().NumFields() {
		if !arrow.TypeEqual(reader.Schema().Field(i).Type, iterator.replacementSchema.Field(i).Type) {
			return nil, cloudArrowSchemaError(fmt.Errorf("field type differs at index %d", i))
		}
	}
	return io.MultiReader(bytes.NewReader(iterator.schema), stream), nil
}

func cloudArrowSchemaError(cause error) dbsqlerr.DBError {
	// A malformed header must not expose EOF as iterator exhaustion.
	return dbsqlerrint.NewDriverError(context.Background(), fmt.Sprintf("invalid CloudFetch Arrow schema: %v", cause), nil).WithCategory(dbsqlerrint.CategoryArrowSchemaParsing)
}

func (iterator *schemaIPCStreamIterator) HasNext() bool { return iterator.source.HasNext() }
func (iterator *schemaIPCStreamIterator) Close()        { iterator.source.Close() }

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
