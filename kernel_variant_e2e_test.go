//go:build cgo && databricks_kernel && e2e

package dbsql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestKernelE2EVariantTypeName runs against the live warehouse on the kernel backend.
// It mirrors TestE2EVariantTypeName: the VARIANT column reports "VARIANT" only with
// WithEnableVariantTypeName, and "STRING" by default.
func TestKernelE2EVariantTypeName(t *testing.T) {
	// Reyden cannot decode VARIANT over inline Arrow (see TestKernelE2EDataTypes).
	skipOnReyden(t, "VARIANT over inline Arrow is not decodable on the reyden reference engine (reyden-sea gap; dbsql-sea/Thrift read it fine)")
	const query = `SELECT parse_json('{"a":1}') AS v, 'x' AS s`

	for _, tc := range []struct {
		name     string
		enabled  bool
		wantType string
	}{
		{name: "default", enabled: false, wantType: "STRING"},
		{name: "enabled", enabled: true, wantType: "VARIANT"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := kernelTestDBWith(t, WithEnableVariantTypeName(tc.enabled))
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
