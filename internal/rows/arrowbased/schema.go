package arrowbased

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
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
	ctx               context.Context
	schema            []byte
	replacementSchema *arrow.Schema
	validatedHeader   []byte
}

func (iterator *schemaIPCStreamIterator) Next() (io.Reader, error) {
	stream, err := iterator.source.Next()
	if err != nil || len(iterator.schema) == 0 {
		return stream, err
	}
	replacement, err := ipcSchemaMessage(iterator.schema)
	if err != nil {
		return nil, iterator.schemaError(fmt.Errorf("invalid replacement schema: %w", err))
	}
	var header []byte
	if downloaded, ok := stream.(*cloudIPCReader); ok {
		header, err = ipcSchemaMessage(downloaded.payload)
	} else {
		header, _, err = peekIPCSchema(stream)
	}
	if err != nil {
		return nil, iterator.schemaError(err)
	}
	if len(header) == 0 {
		return nil, iterator.schemaError(io.ErrUnexpectedEOF)
	}
	matchesReplacement := bytes.Equal(header, replacement)
	if !matchesReplacement && !bytes.Equal(header, iterator.validatedHeader) {
		if err := iterator.validateHeader(header, replacement); err != nil {
			return nil, iterator.schemaError(err)
		}
		iterator.validatedHeader = append(iterator.validatedHeader[:0], header...)
	}
	if downloaded, ok := stream.(*cloudIPCReader); ok {
		if !matchesReplacement {
			downloaded.prefix = replacement
			downloaded.payload = downloaded.payload[len(header):]
		}
		return downloaded, nil
	}
	return io.MultiReader(bytes.NewReader(replacement), stream), nil
}

func (iterator *schemaIPCStreamIterator) validateHeader(header, replacement []byte) error {
	fileSchema, err := schemaFromIPCBytes(header)
	if err != nil {
		return err
	}
	if iterator.replacementSchema == nil {
		iterator.replacementSchema, err = schemaFromIPCBytes(replacement)
		if err != nil {
			return fmt.Errorf("invalid replacement schema: %w", err)
		}
	}
	if fileSchema.NumFields() != iterator.replacementSchema.NumFields() {
		return fmt.Errorf("field count differs: file=%d metadata=%d", fileSchema.NumFields(), iterator.replacementSchema.NumFields())
	}
	for index := range fileSchema.NumFields() {
		if !arrow.TypeEqual(fileSchema.Field(index).Type, iterator.replacementSchema.Field(index).Type) {
			return fmt.Errorf("field type differs at index %d", index)
		}
	}
	return nil
}

func (iterator *schemaIPCStreamIterator) schemaError(cause error) dbsqlerr.DBError {
	iterator.Close()
	if errors.Is(cause, io.EOF) {
		cause = io.ErrUnexpectedEOF
	}
	return dbsqlerrint.NewDriverError(iterator.ctx, "invalid CloudFetch Arrow schema", cause).WithCategory(dbsqlerrint.CategoryArrowSchemaParsing)
}

func ipcSchemaMessage(payload []byte) ([]byte, error) {
	if len(payload) < 4 {
		return nil, io.ErrUnexpectedEOF
	}
	length, offset := binary.LittleEndian.Uint32(payload[:4]), 4
	if length == 0xffffffff {
		if len(payload) < 8 {
			return nil, io.ErrUnexpectedEOF
		}
		length, offset = binary.LittleEndian.Uint32(payload[4:8]), 8
	}
	if length == 0 || int64(length) > int64(len(payload)-offset) {
		return nil, io.ErrUnexpectedEOF
	}
	return payload[:offset+int(length)], nil
}

type cloudIPCReader struct {
	prefix  []byte
	payload []byte
}

func (reader *cloudIPCReader) Read(destination []byte) (int, error) {
	if len(destination) == 0 {
		return 0, nil
	}
	count := copy(destination, reader.prefix)
	reader.prefix = reader.prefix[count:]
	if count == 0 {
		count = copy(destination, reader.payload)
		reader.payload = reader.payload[count:]
		if count == 0 {
			return 0, io.EOF
		}
	}
	return count, nil
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
