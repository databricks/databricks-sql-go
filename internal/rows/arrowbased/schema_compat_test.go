package arrowbased

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/config"
	dbsqlerrint "github.com/databricks/databricks-sql-go/internal/errors"
	"github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/require"
)

func TestCloudIPCUnchangedNamesPreserveTypes(test *testing.T) {
	for _, types := range [][2]arrow.DataType{
		{arrow.StructOf(arrow.Field{Name: "old", Type: arrow.PrimitiveTypes.Int64}), arrow.StructOf(arrow.Field{Name: "new", Type: arrow.PrimitiveTypes.Int64})},
		{arrow.StructOf(arrow.Field{Name: "child", Type: arrow.PrimitiveTypes.Int64}), arrow.StructOf(arrow.Field{Name: "child", Type: arrow.PrimitiveTypes.Int64, Nullable: true})},
		{arrow.ListOfField(arrow.Field{Name: "item", Type: arrow.PrimitiveTypes.Int64}), arrow.ListOf(arrow.PrimitiveTypes.Int64)},
		{&arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "Etc/UTC"}},
		{&arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "Asia/Tokyo"}},
	} {
		test.Run(fmt.Sprintf("%s/%s", types[0], types[1]), func(test *testing.T) {
			serialize := func(dataType arrow.DataType) []byte {
				var output bytes.Buffer
				writer := ipc.NewWriter(&output, ipc.WithSchema(arrow.NewSchema([]arrow.Field{{Name: "value", Type: dataType}}, nil)))
				require.NoError(test, writer.Close())
				return output.Bytes()
			}
			payload := serialize(types[0])
			iterator := &schemaIPCStreamIterator{source: &repeatedCloudStream{payload: payload}, ctx: context.Background(), schema: serialize(types[1])}
			defer iterator.Close()
			for range 2 {
				stream, err := iterator.Next()
				require.NoError(test, err)
				actual, err := io.ReadAll(stream)
				require.NoError(test, err)
				require.Equal(test, payload, actual)
			}
		})
	}
}

func TestCloudSchemaDiscoveryTruncationIsNotEOF(test *testing.T) {
	for _, payload := range [][]byte{nil, {0xff, 0xff, 0xff, 0xff}, {0xff, 0xff, 0xff, 0xff, 24, 0, 0, 0}} {
		server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) { _, _ = response.Write(payload) }))
		rowSet := &cli_service.TRowSet{ResultLinks: []*cli_service.TSparkArrowResultLink{{FileLink: server.URL, RowCount: 1, ExpiryTime: time.Now().Add(time.Hour).Unix()}}}
		_, streams, err := schemaFromFirstBatch(context.Background(), rowSet, config.WithDefaults(), nil)
		require.ErrorIs(test, err, io.ErrUnexpectedEOF)
		require.NotErrorIs(test, err, io.EOF)
		require.Nil(test, streams)
		require.Equal(test, dbsqlerrint.CategoryArrowSchemaParsing, dbsqlerrint.CategoryFromError(err))
		server.Close()
	}
}

func TestInlineEmptyLZ4BatchKeepsFollowingStreams(test *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.Append(1)
	values := builder.NewArray()
	defer values.Release()
	record := array.NewRecordBatch(schema, []arrow.Array{values}, 1)
	defer record.Release()
	var payload, serializedSchema bytes.Buffer
	writer := ipc.NewWriter(&payload, ipc.WithSchema(schema))
	require.NoError(test, writer.Write(record))
	require.NoError(test, writer.Close())
	writer = ipc.NewWriter(&serializedSchema, ipc.WithSchema(schema))
	require.NoError(test, writer.Close())
	var batches []*cli_service.TSparkArrowBatch
	for _, data := range [][]byte{payload.Bytes(), nil, payload.Bytes()} {
		var compressed bytes.Buffer
		compressor := lz4.NewWriter(&compressed)
		_, err := compressor.Write(data)
		require.NoError(test, err)
		require.NoError(test, compressor.Close())
		batches = append(batches, &cli_service.TSparkArrowBatch{Batch: compressed.Bytes()})
	}
	cfg := config.WithDefaults()
	cfg.UseLz4Compression = true
	iterator, err := NewLocalIPCStreamIterator(context.Background(), batches, 0, serializedSchema.Bytes()[:serializedSchema.Len()-8], cfg, true)
	require.NoError(test, err)
	defer iterator.Close()
	var rows int64
	for range batches {
		stream, err := iterator.Next()
		require.NoError(test, err)
		reader, err := ipc.NewReader(stream)
		require.NoError(test, err)
		for reader.Next() {
			rows += reader.RecordBatch().NumRows()
		}
		require.NoError(test, reader.Err())
		reader.Release()
	}
	require.EqualValues(test, 2, rows)
}
