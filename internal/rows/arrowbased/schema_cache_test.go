package arrowbased

import (
	"bytes"
	"context"
	"io"
	"testing"
	"testing/iotest"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/databricks/databricks-sql-go/driverctx"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/stretchr/testify/require"
)

type repeatedCloudStream struct {
	payload []byte
	reader  cloudIPCReader
	closed  bool
}

func (source *repeatedCloudStream) Next() (io.Reader, error) {
	if source.closed {
		return nil, io.EOF
	}
	source.reader = cloudIPCReader{payload: source.payload}
	source.reader.body.Reset(source.payload)
	return &source.reader, nil
}

func (source *repeatedCloudStream) HasNext() bool { return !source.closed }
func (source *repeatedCloudStream) Close()        { source.closed = true }

func TestCloudIPCSchemaCache(test *testing.T) {
	serialize := func(name string, dataType arrow.DataType) []byte {
		var output bytes.Buffer
		writer := ipc.NewWriter(&output, ipc.WithSchema(arrow.NewSchema([]arrow.Field{{Name: name, Type: dataType}}, nil)))
		require.NoError(test, writer.Close())
		return output.Bytes()
	}
	metadata := serialize("authoritative", arrow.PrimitiveTypes.Int64)
	replacement, err := schemaFromIPCBytes(metadata)
	require.NoError(test, err)
	for _, name := range []string{"authoritative", "cached"} {
		test.Run(name, func(test *testing.T) {
			payload := serialize(name, arrow.PrimitiveTypes.Int64)
			source := &repeatedCloudStream{payload: payload}
			iterator := newArrowIPCStreamIterator(context.Background(), nil, nil, metadata, *config.WithDefaults())
			iterator.cloudStreams.replacementSchema = replacement
			firstPage := iterator.withCloudSchema(source)
			stream, err := firstPage.Next()
			require.NoError(test, err)
			actual, err := io.ReadAll(stream)
			require.NoError(test, err)
			require.Equal(test, metadata, actual)
			require.Same(test, replacement, iterator.cloudStreams.replacementSchema)
			if name == "authoritative" {
				require.Empty(test, iterator.cloudStreams.validatedHeader)
			} else {
				header, err := ipcSchemaMessage(payload)
				require.NoError(test, err)
				require.Equal(test, header, iterator.cloudStreams.validatedHeader)
			}
			allocations := testing.AllocsPerRun(100, func() {
				_, nextErr := firstPage.Next()
				if nextErr != nil {
					test.Fatal(nextErr)
				}
			})
			require.Zero(test, allocations, "matching file headers must not allocate or decode IPC")
			cachedHeader := iterator.cloudStreams.validatedHeader
			firstPage.Close()
			secondPage := iterator.withCloudSchema(&repeatedCloudStream{payload: payload})
			require.Same(test, firstPage, secondPage)
			_, err = secondPage.Next()
			require.NoError(test, err)
			if len(cachedHeader) > 0 {
				require.Same(test, &cachedHeader[0], &iterator.cloudStreams.validatedHeader[0])
			}
			badPage := iterator.withCloudSchema(&repeatedCloudStream{payload: serialize("cached", arrow.PrimitiveTypes.Float64)})
			_, err = badPage.Next()
			require.ErrorContains(test, err, "field type differs")
			require.False(test, badPage.HasNext())
		})
	}
}

func TestCloudIPCReader(test *testing.T) {
	for _, prefix := range [][]byte{nil, []byte("schema")} {
		for _, payload := range [][]byte{nil, []byte("record data")} {
			reader := &cloudIPCReader{}
			reader.prefix.Reset(prefix)
			reader.body.Reset(payload)
			require.NoError(test, iotest.TestReader(reader, bytes.Join([][]byte{prefix, payload}, nil)))
			_, err := reader.Seek(0, io.SeekStart)
			require.NoError(test, err)
			var output bytes.Buffer
			count, err := io.Copy(&output, reader)
			require.NoError(test, err)
			require.EqualValues(test, len(prefix)+len(payload), count)
			require.True(test, bytes.Equal(bytes.Join([][]byte{prefix, payload}, nil), output.Bytes()))
			require.Zero(test, reader.Len())
		}
	}
}

func TestCloudIPCHeaderErrorContextAndCause(test *testing.T) {
	ctx := driverctx.NewContextWithConnId(context.Background(), "connection")
	ctx = driverctx.NewContextWithCorrelationId(ctx, "correlation")
	var output bytes.Buffer
	writer := ipc.NewWriter(&output, ipc.WithSchema(arrow.NewSchema(nil, nil)))
	require.NoError(test, writer.Close())
	for _, downloaded := range []bool{false, true} {
		var source IPCStreamIterator
		if downloaded {
			source = &repeatedCloudStream{payload: []byte{0xff}}
		} else {
			var err dbsqlerr.DBError
			source, err = NewLocalIPCStreamIterator(ctx, []*cli_service.TSparkArrowBatch{{Batch: []byte{0xff}}}, 0, nil, config.WithDefaults())
			require.NoError(test, err)
		}
		iterator := &schemaIPCStreamIterator{ctx: ctx, source: source, schema: output.Bytes()}
		_, err := iterator.Next()
		require.ErrorIs(test, err, io.ErrUnexpectedEOF)
		var driverError dbsqlerr.DBError
		require.ErrorAs(test, err, &driverError)
		require.Equal(test, "connection", driverError.ConnectionId())
		require.Equal(test, "correlation", driverError.CorrelationId())
		require.False(test, source.HasNext())
		_, err = iterator.Next()
		require.ErrorIs(test, err, io.EOF)
	}
}
