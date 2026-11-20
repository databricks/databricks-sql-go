package rows

import (
	"bytes"
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	dbsqllog "github.com/databricks/databricks-sql-go/logger"
	dbsqlrows "github.com/databricks/databricks-sql-go/rows"
	"github.com/stretchr/testify/require"
)

func TestArrowSpliceStateAfterValidationError(test *testing.T) {
	var page cli_service.TFetchResultsResp
	loadTestData(test, "multipleFetch/FetchResults1.json", &page)
	reader, err := ipc.NewReader(bytes.NewReader(page.ResultSetMetadata.ArrowSchema))
	require.NoError(test, err)
	fields := reader.Schema().Fields()
	reader.Release()
	expectedName := fields[0].Name
	fields[0].Name = "cached_name"
	serialize := func(fields []arrow.Field) []byte {
		var output bytes.Buffer
		writer := ipc.NewWriter(&output, ipc.WithSchema(arrow.NewSchema(fields, nil)))
		require.NoError(test, writer.Close())
		return output.Bytes()[:output.Len()-8]
	}
	payload := bytes.Join([][]byte{serialize(fields), page.Results.ArrowBatches[0].Batch}, nil)
	badPayload := serialize(fields[:len(fields)-1])
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/bad" {
			_, _ = response.Write(badPayload)
		} else {
			_, _ = response.Write(payload)
		}
	}))
	defer server.Close()
	var links []*cli_service.TSparkArrowResultLink
	for index, path := range []string{"/a", "/bad", "/c"} {
		links = append(links, &cli_service.TSparkArrowResultLink{FileLink: server.URL + path, RowCount: 1, StartRowOffset: int64(index), ExpiryTime: time.Now().Add(time.Hour).Unix()})
	}
	format := cli_service.TSparkRowSetType_URL_BASED_SET
	page.ResultSetMetadata.ResultFormat = &format
	hasMore := true
	page.HasMoreRows = &hasMore
	page.Results = &cli_service.TRowSet{ResultLinks: links[:2]}
	last := &cli_service.TFetchResultsResp{HasMoreRows: new(bool), ResultSetMetadata: page.ResultSetMetadata, Results: &cli_service.TRowSet{StartRowOffset: 2, ResultLinks: links[2:]}}
	cli := &client.TestClient{
		FnFetchResults: func(context.Context, *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
			return last, nil
		},
		FnCloseOperation: func(context.Context, *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
			return &cli_service.TCloseOperationResp{}, nil
		},
	}
	cfg := config.WithDefaults()
	cfg.MaxDownloadThreads = 1
	result, rowsErr := NewRows(context.Background(), nil, cli, cfg, &cli_service.TSparkDirectResults{ResultSet: &page, ResultSetMetadata: page.ResultSetMetadata}, nil)
	require.NoError(test, rowsErr)
	defer func() { require.NoError(test, result.Close()) }()
	iterator, err := result.(dbsqlrows.Rows).GetArrowIPCStreams(context.Background())
	require.NoError(test, err)
	defer iterator.Close()
	for index := range 3 {
		stream, err := iterator.Next()
		if index == 1 {
			require.ErrorContains(test, err, "field count")
			continue
		}
		require.NoError(test, err)
		reader, err := ipc.NewReader(stream)
		require.NoError(test, err)
		name := reader.Schema().Field(0).Name
		reader.Release()
		require.Equal(test, expectedName, name)
	}
	_, err = iterator.Next()
	require.ErrorIs(test, err, io.EOF)
}

func TestEmptyComplexArrowResultKeepsEOFWithoutErrorLogs(test *testing.T) {
	var output bytes.Buffer
	dbsqllog.SetLogOutput(&output)
	defer dbsqllog.SetLogOutput(os.Stderr)
	for _, typeID := range []cli_service.TTypeId{cli_service.TTypeId_ARRAY_TYPE, cli_service.TTypeId_MAP_TYPE, cli_service.TTypeId_STRUCT_TYPE} {
		for _, payload := range [][]byte{nil, {0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0}} {
			for _, accessor := range []string{"ipc", "batches", "next"} {
				test.Run(fmt.Sprintf("%s/payload=%d/%s", typeID, len(payload), accessor), func(test *testing.T) {
					output.Reset()
					format := cli_service.TSparkRowSetType_ARROW_BASED_SET
					metadata := &cli_service.TGetResultSetMetadataResp{ResultFormat: &format, Schema: &cli_service.TTableSchema{Columns: []*cli_service.TColumnDesc{decimalMetadataColumn(typeID, nil, nil)}}}
					page := &cli_service.TFetchResultsResp{ResultSetMetadata: metadata, HasMoreRows: new(bool), Results: &cli_service.TRowSet{ArrowBatches: []*cli_service.TSparkArrowBatch{{Batch: payload}}}}
					fetches := 0
					cli := &client.TestClient{
						FnFetchResults: func(context.Context, *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
							fetches++
							return page, nil
						},
						FnCloseOperation: func(context.Context, *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
							return &cli_service.TCloseOperationResp{}, nil
						},
					}
					cfg := config.WithDefaults()
					cfg.UseArrowNativeComplexTypes = true
					result, rowsErr := NewRows(context.Background(), nil, cli, cfg, nil, nil)
					require.NoError(test, rowsErr)
					defer func() { require.NoError(test, result.Close()) }()
					switch accessor {
					case "ipc":
						iterator, err := result.(dbsqlrows.Rows).GetArrowIPCStreams(context.Background())
						require.NoError(test, err)
						defer iterator.Close()
						require.False(test, iterator.HasNext())
						for range 3 {
							_, err = iterator.Next()
							require.ErrorIs(test, err, io.EOF)
						}
						_, err = iterator.SchemaBytes()
						require.Error(test, err)
					case "batches":
						iterator, err := result.(dbsqlrows.Rows).GetArrowBatches(context.Background())
						require.NoError(test, err)
						defer iterator.Close()
						require.False(test, iterator.HasNext())
						for range 3 {
							_, err = iterator.Next()
							require.ErrorIs(test, err, io.EOF)
						}
						_, err = iterator.Schema()
						require.Error(test, err)
					case "next":
						for range 3 {
							require.ErrorIs(test, result.Next(make([]driver.Value, 1)), io.EOF)
						}
					}
					require.Equal(test, 1, fetches)
					require.NotContains(test, output.String(), `"level":"error"`)
				})
			}
		}
	}
}
