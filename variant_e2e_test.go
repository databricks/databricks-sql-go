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
// the same result; the kernel e2e test shares it.
func assertVariantColumnTypes(t *testing.T, db *sql.DB) {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), `SELECT parse_json('{"a":1}') AS v, 'x' AS s`)
	require.NoError(t, err)
	defer rows.Close() //nolint:errcheck

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

	require.True(t, rows.Next(), "expected one row")
	var v, s string
	require.NoError(t, rows.Scan(&v, &s))
	assert.JSONEq(t, `{"a":1}`, v)
	assert.Equal(t, "x", s)
}
