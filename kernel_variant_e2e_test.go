//go:build cgo && databricks_kernel && e2e

package dbsql

import "testing"

// TestKernelE2EVariantTypeName runs against the live warehouse on the kernel backend
// and mirrors TestE2EVariantTypeName.
func TestKernelE2EVariantTypeName(t *testing.T) {
	// Reyden cannot decode VARIANT over inline Arrow (see TestKernelE2EDataTypes).
	skipOnReyden(t, "VARIANT over inline Arrow is not decodable on the reyden reference engine (reyden-sea gap; dbsql-sea/Thrift read it fine)")
	db := kernelTestDB(t)
	defer db.Close() //nolint:errcheck

	assertVariantColumnTypes(t, db)
}
