//go:build e2e

package dbsql

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/databricks/databricks-sql-go/internal/backend"
	thriftbackend "github.com/databricks/databricks-sql-go/internal/backend/thrift"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
)

// variantCloudFetchRows is large enough for the server to return the result
// through CloudFetch (URL_BASED_SET), matching TestE2ETimeArrowResults.
const variantCloudFetchRows = 250000

// TestE2EVariantTypeName runs against the live warehouse on the Thrift backend: the
// VARIANT column reports "VARIANT" while its value stays JSON text with the STRING scan
// type and length. It also captures the Thrift responses and checks that every result
// schema the server sends, inline or CloudFetch, tags the column with
// Spark:DataType:SqlName=VARIANT, the metadata the driver reads for the type name.
func TestE2EVariantTypeName(t *testing.T) {
	host, httpPath, token := pecoTestingCreds(t)
	connectorValue, err := NewConnector(
		WithServerHostname(host),
		WithPort(443),
		WithHTTPPath(httpPath),
		WithAccessToken(token),
	)
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
	// One connection, so the capture wraps a single Thrift client.
	db.SetMaxOpenConns(1)
	defer db.Close() //nolint:errcheck

	assertVariantColumnTypes(t, db)

	capture.mu.Lock()
	defer capture.mu.Unlock()
	require.NotEmpty(t, capture.metadata)
	formats := map[cli_service.TSparkRowSetType]bool{}
	for _, metadata := range capture.metadata {
		formats[metadata.GetResultFormat()] = true
		require.NotEmpty(t, metadata.ArrowSchema, "result format %s", metadata.GetResultFormat())
		reader, err := ipc.NewReader(bytes.NewReader(metadata.ArrowSchema))
		require.NoError(t, err)
		field := reader.Schema().Field(0)
		reader.Release()
		sqlName, _ := field.Metadata.GetValue("Spark:DataType:SqlName")
		t.Logf("VARIANT wire Arrow=%s, SqlName=%q, result format=%s", field.Type, sqlName, metadata.GetResultFormat())
		require.Equal(t, "VARIANT", sqlName, "Arrow schema tag, result format %s", metadata.GetResultFormat())
		require.Equal(t, arrow.BinaryTypes.String, field.Type, "result format %s", metadata.GetResultFormat())
	}
	require.True(t, formats[cli_service.TSparkRowSetType_URL_BASED_SET], "a CloudFetch result was returned")
	require.True(t, capture.links, "CloudFetch result links were returned")
}

// assertVariantColumnTypes checks a VARIANT column against a plain STRING column in
// the same result, for a populated row, a CloudFetch-sized result, and empty and
// NULL-only results, whose schemas the server or kernel may build differently. The
// kernel e2e test shares it.
func assertVariantColumnTypes(t *testing.T, db *sql.DB) {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), `SELECT parse_json('{"a":1}') AS v, 'x' AS s`)
	require.NoError(t, err)
	defer rows.Close() //nolint:errcheck

	assertVariantAndStringColumns(t, rows)
	require.True(t, rows.Next(), "expected one row")
	var v, s string
	require.NoError(t, rows.Scan(&v, &s))
	assert.JSONEq(t, `{"a":1}`, v)
	assert.Equal(t, "x", s)

	t.Run("CloudFetch", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		query := fmt.Sprintf(`SELECT parse_json(to_json(named_struct('id', id))) AS v, sha2(CAST(id AS STRING), 512) AS s FROM range(%d)`, variantCloudFetchRows)
		rows, err := db.QueryContext(ctx, query)
		require.NoError(t, err)
		defer rows.Close() //nolint:errcheck

		assertVariantAndStringColumns(t, rows)
		count := 0
		for rows.Next() {
			var v, s string
			require.NoError(t, rows.Scan(&v, &s))
			require.True(t, json.Valid([]byte(v)), "VARIANT value %q", v)
			require.Len(t, s, 128)
			count++
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, variantCloudFetchRows, count)
	})

	for _, tc := range []struct {
		name  string
		query string
		rows  int
	}{
		{"empty range", `SELECT parse_json('{"a":1}') AS v, 'x' AS s FROM range(0)`, 0},
		{"empty filter", `SELECT parse_json('{"a":1}') AS v, 'x' AS s FROM range(10) WHERE id > 100`, 0},
		{"NULL only", `SELECT CAST(NULL AS VARIANT) AS v, CAST(NULL AS STRING) AS s FROM range(3)`, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows, err := db.QueryContext(context.Background(), tc.query)
			require.NoError(t, err)
			defer rows.Close() //nolint:errcheck

			assertVariantAndStringColumns(t, rows)
			count := 0
			for rows.Next() {
				var v, s sql.NullString
				require.NoError(t, rows.Scan(&v, &s))
				assert.False(t, v.Valid, "VARIANT value")
				assert.False(t, s.Valid, "STRING value")
				count++
			}
			require.NoError(t, rows.Err())
			assert.Equal(t, tc.rows, count)
		})
	}
}

// assertVariantAndStringColumns checks that column 0 reports VARIANT and column 1
// STRING, with the same scan type and length.
func assertVariantAndStringColumns(t *testing.T, rows *sql.Rows) {
	t.Helper()
	cts, err := rows.ColumnTypes()
	require.NoError(t, err)
	require.Len(t, cts, 2)
	assert.Equal(t, "VARIANT", cts[0].DatabaseTypeName(), "VARIANT column")
	assert.Equal(t, "STRING", cts[1].DatabaseTypeName(), "plain string column")
	assert.Equal(t, cts[1].ScanType(), cts[0].ScanType(), "VARIANT scan type")
	variantLen, variantHasLen := cts[0].Length()
	stringLen, stringHasLen := cts[1].Length()
	assert.Equal(t, stringLen, variantLen, "VARIANT length")
	assert.Equal(t, stringHasLen, variantHasLen, "VARIANT length presence")
}
