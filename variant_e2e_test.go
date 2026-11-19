//go:build e2e

package dbsql

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestE2EVariantTypeName runs against the live warehouse on the Thrift backend: the
// VARIANT column reports "VARIANT" while its value stays JSON text with the STRING scan
// type and length.
func TestE2EVariantTypeName(t *testing.T) {
	host, httpPath, token := pecoTestingCreds(t)
	connector, err := NewConnector(
		WithServerHostname(host),
		WithPort(443),
		WithHTTPPath(httpPath),
		WithAccessToken(token),
	)
	require.NoError(t, err)
	db := sql.OpenDB(connector)
	defer db.Close() //nolint:errcheck

	assertVariantColumnTypes(t, db)
}

// assertVariantColumnTypes checks a VARIANT column against a plain STRING column in
// the same result, for a populated row and for empty and NULL-only results, whose
// schemas the server or kernel may build differently. The kernel e2e test shares it.
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
