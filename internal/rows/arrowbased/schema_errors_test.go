package arrowbased

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/config"
	dbsqlrows "github.com/databricks/databricks-sql-go/rows"
	"github.com/stretchr/testify/require"
)

func TestArrowScannerFailureReturnsNilInterface(t *testing.T) {
	metadata := &cli_service.TGetResultSetMetadataResp{ArrowSchema: []byte("invalid schema")}
	scanner, err := NewArrowRowScanner(metadata, &cli_service.TRowSet{}, config.WithDefaults(), nil, context.Background(), nil)
	require.Error(t, err)
	require.True(t, scanner == nil, "constructor errors must return a nil interface")
}

func TestMalformedIPCSchemaProbe(t *testing.T) {
	for _, payload := range [][]byte{
		{0xff, 0xff, 0xff, 0xff, 4, 0, 0, 0, 0xff, 0xff, 0xff, 0x7f},
		{0xff, 0xff, 0xff, 0xff, 1, 0, 0, 0, 0},
		{0xff, 0xff, 0xff, 0xff},
	} {
		require.NotPanics(t, func() {
			_, err := hasIPCSchema(payload)
			require.Error(t, err)
			_, _, err = peekIPCSchema(bytes.NewReader(payload))
			require.Error(t, err)
			iterator, err := NewLocalIPCStreamIterator(context.Background(), []*cli_service.TSparkArrowBatch{{Batch: payload}}, 0, nil, config.WithDefaults(), true)
			require.NoError(t, err)
			defer iterator.Close()
			_, err = iterator.Next()
			require.Error(t, err)
		})
	}
}

func TestLocalStreamWithServerSchemaDoesNotProbe(t *testing.T) {
	// Invalid record data still belongs to the consuming IPC reader, as on main.
	iterator, iteratorErr := NewLocalIPCStreamIterator(context.Background(), []*cli_service.TSparkArrowBatch{{Batch: []byte("invalid record")}}, 0, []byte("server schema"), config.WithDefaults())
	require.NoError(t, iteratorErr)
	defer iterator.Close()
	stream, err := iterator.Next()
	require.NoError(t, err)
	payload, err := io.ReadAll(stream)
	require.NoError(t, err)
	require.Equal(t, "server schemainvalid record", string(payload))
}

func TestCloudIPCHeaderValidation(t *testing.T) {
	serialize := func(fields []arrow.Field) []byte {
		var output bytes.Buffer
		writer := ipc.NewWriter(&output, ipc.WithSchema(arrow.NewSchema(fields, nil)))
		require.NoError(t, writer.Close())
		return output.Bytes()
	}
	schema := serialize([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}})
	for _, payload := range [][]byte{{0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0}, serialize(nil)} {
		source, sourceErr := NewLocalIPCStreamIterator(context.Background(), []*cli_service.TSparkArrowBatch{{Batch: payload}, {Batch: schema}}, 0, nil, config.WithDefaults())
		require.NoError(t, sourceErr)
		iterator := &schemaIPCStreamIterator{source: source, schema: schema}
		defer iterator.Close()
		_, err := iterator.Next()
		require.Error(t, err)
		require.False(t, errors.Is(err, io.EOF), "invalid files must not look like end of results")
		require.Contains(t, err.Error(), "CloudFetch Arrow schema")
		require.True(t, iterator.HasNext())
		stream, err := iterator.Next()
		require.NoError(t, err)
		reader, err := ipc.NewReader(stream)
		require.NoError(t, err)
		require.Equal(t, "id", reader.Schema().Field(0).Name)
		reader.Release()
	}
}

func TestDeferredSchemaInitializationErrorIsStable(t *testing.T) {
	wanted := errors.New("initialization failed")
	for _, accessor := range []string{"ipc", "batches"} {
		t.Run(accessor, func(t *testing.T) {
			calls := 0
			var schema func() error
			var next func() error
			var hasNext func() bool
			var closeIterator func()
			if accessor == "ipc" {
				iterator := DeferArrowIPCStreams(func() (dbsqlrows.ArrowIPCStreamIterator, error) { calls++; return nil, wanted })
				schema = func() error { _, err := iterator.SchemaBytes(); return err }
				next = func() error { _, err := iterator.Next(); return err }
				hasNext, closeIterator = iterator.HasNext, iterator.Close
			} else {
				iterator := DeferArrowBatches(func() (dbsqlrows.ArrowBatchIterator, error) { calls++; return nil, wanted })
				schema = func() error { _, err := iterator.Schema(); return err }
				next = func() error { _, err := iterator.Next(); return err }
				hasNext, closeIterator = iterator.HasNext, iterator.Close
			}
			defer closeIterator()
			require.ErrorIs(t, schema(), wanted)
			require.ErrorIs(t, schema(), wanted)
			require.True(t, hasNext())
			require.ErrorIs(t, next(), wanted)
			require.False(t, hasNext())
			require.ErrorIs(t, schema(), wanted)
			require.ErrorIs(t, next(), io.EOF)
			require.Equal(t, 1, calls)
		})
	}
}
