//go:build cgo && databricks_kernel && e2e

package dbsql

import "testing"

func TestKernelE2ETimeResults(t *testing.T) {
	options := []ConnOption{WithUseKernel(true), WithSessionParams(map[string]string{"timezone": "Asia/Tokyo"})}
	if isReydenLeg() {
		options = append(options, WithHTTPPath(reydenHTTPPath()))
	}
	db := timeTestDB(t, options...)
	defer db.Close() //nolint:errcheck
	assertTimeResults(t, db)
}
