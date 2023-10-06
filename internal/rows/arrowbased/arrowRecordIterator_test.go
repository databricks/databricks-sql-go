package arrowbased

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/databricks/databricks-sql-go/internal/rows/rowscanner"
	dbsqllog "github.com/databricks/databricks-sql-go/logger"
	"github.com/stretchr/testify/assert"
)

func TestArrowRecordIterator(t *testing.T) {

	t.Run("with direct results", func(t *testing.T) {
		executeStatementResp := cli_service.TExecuteStatementResp{}
		loadTestData2(t, "directResultsMultipleFetch/ExecuteStatement.json", &executeStatementResp)

		fetchResp1 := cli_service.TFetchResultsResp{}
		loadTestData2(t, "directResultsMultipleFetch/FetchResults1.json", &fetchResp1)

		fetchResp2 := cli_service.TFetchResultsResp{}
		loadTestData2(t, "directResultsMultipleFetch/FetchResults2.json", &fetchResp2)

		var fetchesInfo []fetchResultsInfo

		client := getSimpleClient(&fetchesInfo, []cli_service.TFetchResultsResp{fetchResp1, fetchResp2})
		logger := dbsqllog.WithContext("connectionId", "correlationId", "")
		rpi := rowscanner.NewResultPageIterator(
			rowscanner.NewDelimiter(0, 7311),
			5000,
			nil,
			false,
			client,
			"connectionId",
			"correlationId",
			logger)

		bl, err := NewLocalBatchLoader(
			context.Background(),
			executeStatementResp.DirectResults.ResultSet.Results.ArrowBatches,
			0,
			executeStatementResp.DirectResults.ResultSetMetadata.ArrowSchema,
			nil,
		)
		assert.Nil(t, err)

		bi, err := NewBatchIterator(bl)
		assert.Nil(t, err)

		cfg := *config.WithDefaults()
		rs := NewArrowRecordIterator(context.Background(), rpi, bi, executeStatementResp.DirectResults.ResultSetMetadata.ArrowSchema, cfg)
		defer rs.Close()

		hasNext := rs.HasNext()
		assert.True(t, hasNext)
		r, err2 := rs.Next()
		assert.Nil(t, err2)
		assert.Equal(t, executeStatementResp.DirectResults.ResultSet.Results.ArrowBatches[0].RowCount, r.NumRows())
		r.Release()

		hasNext = rs.HasNext()
		assert.True(t, hasNext)
		r2, err2 := rs.Next()
		assert.Nil(t, err2)
		assert.Equal(t, executeStatementResp.DirectResults.ResultSet.Results.ArrowBatches[1].RowCount, r2.NumRows())
		r2.Release()

		hasNext = rs.HasNext()
		assert.True(t, hasNext)
		r3, err2 := rs.Next()
		assert.Nil(t, err2)
		assert.Equal(t, fetchResp1.Results.ArrowBatches[0].RowCount, r3.NumRows())
		r3.Release()

		hasNext = rs.HasNext()
		assert.True(t, hasNext)
		r4, err2 := rs.Next()
		assert.Nil(t, err2)
		assert.Equal(t, fetchResp1.Results.ArrowBatches[1].RowCount, r4.NumRows())
		r4.Release()

		hasNext = rs.HasNext()
		assert.True(t, hasNext)
		r5, err2 := rs.Next()
		assert.Nil(t, err2)
		assert.Equal(t, fetchResp2.Results.ArrowBatches[0].RowCount, r5.NumRows())
		r5.Release()

		hasNext = rs.HasNext()
		assert.True(t, hasNext)
		r6, err2 := rs.Next()
		assert.Nil(t, err2)
		assert.Equal(t, fetchResp2.Results.ArrowBatches[1].RowCount, r6.NumRows())
		r6.Release()

		hasNext = rs.HasNext()
		assert.False(t, hasNext)
		r7, err2 := rs.Next()
		assert.Nil(t, r7)
		assert.ErrorContains(t, err2, io.EOF.Error())
	})

	t.Run("no direct results", func(t *testing.T) {

		fetchResp1 := cli_service.TFetchResultsResp{}
		loadTestData2(t, "multipleFetch/FetchResults1.json", &fetchResp1)

		fetchResp2 := cli_service.TFetchResultsResp{}
		loadTestData2(t, "multipleFetch/FetchResults2.json", &fetchResp2)

		fetchResp3 := cli_service.TFetchResultsResp{}
		loadTestData2(t, "multipleFetch/FetchResults3.json", &fetchResp3)

		var fetchesInfo []fetchResultsInfo

		client := getSimpleClient(&fetchesInfo, []cli_service.TFetchResultsResp{fetchResp1, fetchResp2, fetchResp3})
		logger := dbsqllog.WithContext("connectionId", "correlationId", "")
		rpi := rowscanner.NewResultPageIterator(
			rowscanner.NewDelimiter(0, 0),
			5000,
			nil,
			false,
			client,
			"connectionId",
			"correlationId",
			logger)

		cfg := *config.WithDefaults()
		rs := NewArrowRecordIterator(context.Background(), rpi, nil, nil, cfg)
		defer rs.Close()

		hasNext := rs.HasNext()
		assert.True(t, hasNext)
		r, err2 := rs.Next()
		assert.Nil(t, err2)
		assert.Equal(t, fetchResp1.Results.ArrowBatches[0].RowCount, r.NumRows())
		r.Release()

		hasNext = rs.HasNext()
		assert.True(t, hasNext)
		r2, err2 := rs.Next()
		assert.Nil(t, err2)
		assert.Equal(t, fetchResp1.Results.ArrowBatches[1].RowCount, r2.NumRows())
		r2.Release()

		hasNext = rs.HasNext()
		assert.True(t, hasNext)
		r3, err2 := rs.Next()
		assert.Nil(t, err2)
		assert.Equal(t, fetchResp2.Results.ArrowBatches[0].RowCount, r3.NumRows())
		r3.Release()

		hasNext = rs.HasNext()
		assert.True(t, hasNext)
		r4, err2 := rs.Next()
		assert.Nil(t, err2)
		assert.Equal(t, fetchResp2.Results.ArrowBatches[1].RowCount, r4.NumRows())
		r4.Release()

		hasNext = rs.HasNext()
		assert.True(t, hasNext)
		r5, err2 := rs.Next()
		assert.Nil(t, err2)
		assert.Equal(t, fetchResp3.Results.ArrowBatches[0].RowCount, r5.NumRows())
		r5.Release()

		hasNext = rs.HasNext()
		assert.True(t, hasNext)
		r6, err2 := rs.Next()
		assert.Nil(t, err2)
		assert.Equal(t, fetchResp3.Results.ArrowBatches[1].RowCount, r6.NumRows())
		r6.Release()
	})
}

type fetchResultsInfo struct {
	direction      cli_service.TFetchOrientation
	resultStartRec int
}

func getSimpleClient(fetches *[]fetchResultsInfo, fetchResults []cli_service.TFetchResultsResp) cli_service.TCLIService {
	var resultIndex int

	fetchResultsFn := func(ctx context.Context, req *cli_service.TFetchResultsReq) (_r *cli_service.TFetchResultsResp, _err error) {

		p := fetchResults[resultIndex]

		*fetches = append(*fetches, fetchResultsInfo{direction: req.Orientation, resultStartRec: int(p.Results.StartRowOffset)})
		resultIndex++
		return &p, nil
	}

	client := &client.TestClient{
		FnFetchResults: fetchResultsFn,
	}

	return client
}

func loadTestData2(t *testing.T, name string, v any) {
	if f, err := os.ReadFile(fmt.Sprintf("../testdata/%s", name)); err != nil {
		t.Errorf("could not read data from: %s", name)
	} else {
		if err := json.Unmarshal(f, v); err != nil {
			t.Errorf("could not load data from: %s", name)
		}
	}
}
