//go:build e2e

package dbsql

import (
	"context"
	"database/sql"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestE2ETimeResults(t *testing.T) {
	db := timeTestDB(t, WithSessionParams(map[string]string{"timezone": "America/Los_Angeles"}))
	defer db.Close() //nolint:errcheck
	assertTimeResults(t, db)
}

func timeTestDB(t *testing.T, extra ...ConnOption) *sql.DB {
	t.Helper()
	host, httpPath, token := readPecoTestingCreds(t)
	require.NotEmpty(t, host, "live TIME tests require DATABRICKS_PECOTESTING_SERVER_HOSTNAME")
	require.NotEmpty(t, httpPath, "live TIME tests require DATABRICKS_PECOTESTING_HTTP_PATH2")
	require.NotEmpty(t, token, "live TIME tests require DATABRICKS_PECOTESTING_TOKEN or its configured fallback")
	options := append([]ConnOption{WithServerHostname(host), WithHTTPPath(httpPath), WithAccessToken(token)}, extra...)
	connector, err := NewConnector(options...)
	require.NoError(t, err)
	return sql.OpenDB(connector)
}

func assertTimeResults(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	rows, err := db.QueryContext(ctx, "SELECT clock AS typed, clock AS dynamic, clock AS text, CAST(NULL AS TIME) AS missing FROM (SELECT TIME '09:15:00.123456' AS clock)")
	if serverTimeUnavailable(err) {
		t.Skip("live TIME verification pending: this workspace has not enabled the TIME beta")
	}
	require.NoError(t, err)
	defer rows.Close() //nolint:errcheck
	columns, err := rows.ColumnTypes()
	require.NoError(t, err)
	for _, column := range columns {
		require.Equal(t, "TIME", column.DatabaseTypeName())
		require.Equal(t, reflect.TypeOf(time.Time{}), column.ScanType())
	}
	if !rows.Next() {
		t.Fatalf("TIME result unavailable: %v", rows.Err())
	}
	var typed time.Time
	var dynamic any
	var text string
	var missing sql.NullTime
	require.NoError(t, rows.Scan(&typed, &dynamic, &text, &missing))
	want := time.Date(1, time.January, 1, 9, 15, 0, 123456000, time.UTC)
	require.Equal(t, want, typed)
	require.Equal(t, want, dynamic)
	require.Equal(t, "0001-01-01T09:15:00.123456Z", text)
	require.False(t, missing.Valid)
	require.False(t, rows.Next())
	require.NoError(t, rows.Err())

	var list, structure, mapping string
	err = db.QueryRowContext(ctx, "SELECT array(TIME '09:15:00.123456', CAST(NULL AS TIME)), named_struct('clock', TIME '09:15:00.123456', 'missing', CAST(NULL AS TIME)), map(TIME '09:15:00.123456', TIME '09:15:00.123456')").Scan(&list, &structure, &mapping)
	require.NoError(t, err)
	require.JSONEq(t, `["09:15:00.123456",null]`, list)
	require.JSONEq(t, `{"clock":"09:15:00.123456","missing":null}`, structure)
	require.JSONEq(t, `{"09:15:00.123456":"09:15:00.123456"}`, mapping)
}
