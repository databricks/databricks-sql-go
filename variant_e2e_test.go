//go:build e2e

package dbsql

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestE2EVariantTypeName runs against the live warehouse on the Thrift backend. With
// WithEnableVariantTypeName the VARIANT column reports "VARIANT"; by default it reports
// "STRING". The value is JSON text either way.
func TestE2EVariantTypeName(t *testing.T) {
	host, httpPath, token := pecoTestingCreds(t)
	const query = `SELECT parse_json('{"a":1}') AS v, 'x' AS s`

	for _, tc := range []struct {
		name     string
		opts     []ConnOption
		wantType string
	}{
		{name: "default", opts: nil, wantType: "STRING"},
		{name: "enabled", opts: []ConnOption{WithEnableVariantTypeName(true)}, wantType: "VARIANT"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := append([]ConnOption{
				WithServerHostname(host),
				WithPort(443),
				WithHTTPPath(httpPath),
				WithAccessToken(token),
			}, tc.opts...)
			connector, err := NewConnector(opts...)
			require.NoError(t, err)

			db := sql.OpenDB(connector)
			defer db.Close() //nolint:errcheck

			rows, err := db.QueryContext(context.Background(), query)
			require.NoError(t, err)
			defer rows.Close() //nolint:errcheck

			cts, err := rows.ColumnTypes()
			require.NoError(t, err)
			require.Len(t, cts, 2)
			assert.Equal(t, tc.wantType, cts[0].DatabaseTypeName(), "VARIANT column")
			assert.Equal(t, "STRING", cts[1].DatabaseTypeName(), "plain string column")

			require.True(t, rows.Next(), "expected one row")
			var v, s string
			require.NoError(t, rows.Scan(&v, &s))
			assert.JSONEq(t, `{"a":1}`, v)
			assert.Equal(t, "x", s)
		})
	}
}
