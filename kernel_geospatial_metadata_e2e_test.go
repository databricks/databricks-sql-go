//go:build cgo && databricks_kernel && e2e

package dbsql

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKernelE2EGeospatialTypeNames(t *testing.T) {
	for _, native := range []bool{false, true} {
		t.Run(fmt.Sprintf("native=%t", native), func(t *testing.T) {
			db := kernelTestDBWith(t, WithEnableGeoSpatialSupport(native))
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			assertGeospatialColumnTypes(t, db, native)
		})
	}
}
