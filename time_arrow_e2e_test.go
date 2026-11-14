//go:build e2e

package dbsql

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/stretchr/testify/require"

	"github.com/databricks/databricks-sql-go/internal/backend"
	thriftbackend "github.com/databricks/databricks-sql-go/internal/backend/thrift"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	dbsqlrows "github.com/databricks/databricks-sql-go/rows"
)

func TestE2ETimeArrowResults(t *testing.T) {
	for _, test := range []struct {
		name   string
		rows   int
		format cli_service.TSparkRowSetType
	}{
		{"inline", 3, cli_service.TSparkRowSetType_ARROW_BASED_SET},
		{"CloudFetch", 250000, cli_service.TSparkRowSetType_URL_BASED_SET},
	} {
		t.Run(test.name, func(t *testing.T) {
			host, path, token := readPecoTestingCreds(t)
			require.NotEmpty(t, host)
			require.NotEmpty(t, path)
			require.NotEmpty(t, token)
			connectorValue, err := NewConnector(WithServerHostname(host), WithHTTPPath(path), WithAccessToken(token),
				WithSessionParams(map[string]string{"timezone": "America/Los_Angeles"}))
			require.NoError(t, err)
			capture := &timeWireClient{}
			connectorValue.(*connector).thriftBackendFactory = func(ctx context.Context, cfg *config.Config, httpClient *http.Client) (backend.Backend, error) {
				service, err := client.InitThriftClient(cfg, httpClient)
				if err != nil {
					return nil, err
				}
				capture.TCLIService = service
				return thriftbackend.NewForTest(capture, nil, cfg), nil
			}
			db := sql.OpenDB(connectorValue)
			db.SetMaxOpenConns(1)
			defer db.Close() //nolint:errcheck
			assertTimeArrowResults(t, db, test.rows)
			capture.mu.Lock()
			defer capture.mu.Unlock()
			require.NotEmpty(t, capture.metadata)
			for _, metadata := range capture.metadata {
				require.Equal(t, test.format, metadata.GetResultFormat())
				require.NotEmpty(t, metadata.ArrowSchema)
				reader, err := ipc.NewReader(bytes.NewReader(metadata.ArrowSchema))
				require.NoError(t, err)
				require.Equal(t, arrow.FixedWidthTypes.Time64ns, reader.Schema().Field(1).Type)
				reader.Release()
				typeID := metadata.Schema.Columns[1].TypeDesc.Types[0].PrimitiveEntry.Type
				t.Logf("TIME wire TTypeId=%s (%d), Arrow=time64[ns], result format=%s", typeID, typeID, metadata.GetResultFormat())
				require.Equal(t, cli_service.TTypeId_STRING_TYPE, typeID)
			}
			require.Equal(t, test.format == cli_service.TSparkRowSetType_URL_BASED_SET, capture.links)
		})
	}
}

type timeWireClient struct {
	cli_service.TCLIService
	mu       sync.Mutex
	metadata []*cli_service.TGetResultSetMetadataResp
	links    bool
}

func (capture *timeWireClient) observe(metadata *cli_service.TGetResultSetMetadataResp, result *cli_service.TFetchResultsResp) {
	capture.mu.Lock()
	defer capture.mu.Unlock()
	if metadata != nil {
		capture.metadata = append(capture.metadata, metadata)
	}
	if result != nil {
		if result.ResultSetMetadata != nil {
			capture.metadata = append(capture.metadata, result.ResultSetMetadata)
		}
		if result.Results != nil && len(result.Results.ResultLinks) > 0 {
			capture.links = true
		}
	}
}

func (capture *timeWireClient) ExecuteStatement(ctx context.Context, req *cli_service.TExecuteStatementReq) (*cli_service.TExecuteStatementResp, error) {
	response, err := capture.TCLIService.ExecuteStatement(ctx, req)
	if response != nil && response.DirectResults != nil {
		capture.observe(response.DirectResults.ResultSetMetadata, response.DirectResults.ResultSet)
	}
	return response, err
}

func (capture *timeWireClient) GetResultSetMetadata(ctx context.Context, req *cli_service.TGetResultSetMetadataReq) (*cli_service.TGetResultSetMetadataResp, error) {
	response, err := capture.TCLIService.GetResultSetMetadata(ctx, req)
	capture.observe(response, nil)
	return response, err
}

func (capture *timeWireClient) FetchResults(ctx context.Context, req *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
	response, err := capture.TCLIService.FetchResults(ctx, req)
	capture.observe(nil, response)
	return response, err
}

func assertTimeArrowResults(t *testing.T, db *sql.DB, wantRows int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	query := "SELECT id, CASE WHEN id % 3 = 0 THEN CAST(NULL AS TIME) WHEN id % 3 = 1 THEN TIME '00:00:00' ELSE TIME '23:59:59.123456' END AS clock, array(TIME '09:15:00.123456', CAST(NULL AS TIME)) AS clocks, named_struct('clock', TIME '09:15:00.123456', 'missing', CAST(NULL AS TIME)) AS structure, sha2(CAST(id AS STRING), 512) AS payload FROM range(?)"
	rows, err := db.QueryContext(ctx, query, int64(wantRows))
	require.NoError(t, err)
	defer rows.Close() //nolint:errcheck
	columns, err := rows.ColumnTypes()
	require.NoError(t, err)
	require.Equal(t, "TIME", columns[1].DatabaseTypeName())
	count := 0
	for rows.Next() {
		var id int64
		var clock sql.NullTime
		var clocks, structure, payload string
		require.NoError(t, rows.Scan(&id, &clock, &clocks, &structure, &payload))
		require.Equal(t, id%3 != 0, clock.Valid)
		if clock.Valid {
			want := time.Time{}
			if id%3 == 2 {
				want = time.Date(1, time.January, 1, 23, 59, 59, 123456000, time.UTC)
			}
			require.Equal(t, want, clock.Time)
		}
		if count == 0 {
			require.JSONEq(t, `["09:15:00.123456",null]`, clocks)
			require.JSONEq(t, `{"clock":"09:15:00.123456","missing":null}`, structure)
		}
		require.Len(t, payload, 128)
		count++
	}
	require.NoError(t, rows.Err())
	require.Equal(t, wantRows, count)
	require.NoError(t, rows.Close())

	connection, err := db.Conn(ctx)
	require.NoError(t, err)
	defer connection.Close() //nolint:errcheck
	require.NoError(t, connection.Raw(func(raw any) error {
		result, err := raw.(driver.QueryerContext).QueryContext(ctx, query, []driver.NamedValue{{Ordinal: 1, Value: int64(wantRows)}})
		require.NoError(t, err)
		defer result.Close() //nolint:errcheck
		batches, err := result.(dbsqlrows.Rows).GetArrowBatches(ctx)
		require.NoError(t, err)
		defer batches.Close()
		schema, err := batches.Schema()
		require.NoError(t, err)
		require.Equal(t, arrow.FixedWidthTypes.Time64ns, schema.Field(1).Type)
		var batchRows int64
		for batches.HasNext() {
			batch, err := batches.Next()
			require.NoError(t, err)
			batchRows += batch.NumRows()
			batch.Release()
		}
		require.Equal(t, int64(wantRows), batchRows)
		t.Logf("verified time64[ns] schema and %d rows through database/sql and Arrow batches", wantRows)
		return nil
	}))
}
