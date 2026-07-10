package thrift

import (
	"testing"

	"github.com/databricks/databricks-sql-go/internal/backend"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Tests the stage-2 wire mapping in isolation: backend.Param -> TSparkParameter.
// Type inference and value stringification (stage 1) live in the dbsql package
// and are tested there; this covers only the mechanical shape mapping — named vs
// positional, the nil-Value -> SQL NULL case, and pointer independence across
// entries. strPtr is the shared helper from backend_test.go.
func TestToSparkParameters(t *testing.T) {
	t.Run("empty input yields nil", func(t *testing.T) {
		assert.Nil(t, toSparkParameters(nil))
		assert.Nil(t, toSparkParameters([]backend.Param{}))
	})

	t.Run("named parameter maps name, type and value", func(t *testing.T) {
		out := toSparkParameters([]backend.Param{{Name: "p1", Type: "BIGINT", Value: strPtr("5")}})
		require.Len(t, out, 1)
		require.NotNil(t, out[0].Name)
		assert.Equal(t, "p1", *out[0].Name)
		require.NotNil(t, out[0].Type)
		assert.Equal(t, "BIGINT", *out[0].Type)
		require.NotNil(t, out[0].Value)
		require.NotNil(t, out[0].Value.StringValue)
		assert.Equal(t, "5", *out[0].Value.StringValue)
	})

	t.Run("positional parameter has nil name", func(t *testing.T) {
		out := toSparkParameters([]backend.Param{{Name: "", Type: "STRING", Value: strPtr("x")}})
		require.Len(t, out, 1)
		assert.Nil(t, out[0].Name, "empty Name must map to a nil TSparkParameter.Name (positional)")
		assert.Equal(t, "STRING", *out[0].Type)
		assert.Equal(t, "x", *out[0].Value.StringValue)
	})

	t.Run("nil Value maps to SQL NULL (nil TSparkParameterValue)", func(t *testing.T) {
		out := toSparkParameters([]backend.Param{{Name: "n", Type: "VOID", Value: nil}})
		require.Len(t, out, 1)
		assert.Nil(t, out[0].Value, "nil Param.Value must produce a nil TSparkParameter.Value")
		require.NotNil(t, out[0].Type)
		assert.Equal(t, "VOID", *out[0].Type)
	})

	t.Run("multiple params keep independent value pointers", func(t *testing.T) {
		// Guards the loop-variable aliasing fix: each entry must point at its own
		// copy of the value/type, not a shared final iteration's slot.
		out := toSparkParameters([]backend.Param{
			{Name: "a", Type: "INT", Value: strPtr("1")},
			{Name: "b", Type: "INT", Value: strPtr("2")},
			{Name: "c", Type: "INT", Value: strPtr("3")},
		})
		require.Len(t, out, 3)
		assert.Equal(t, "1", *out[0].Value.StringValue)
		assert.Equal(t, "2", *out[1].Value.StringValue)
		assert.Equal(t, "3", *out[2].Value.StringValue)
		assert.Equal(t, "a", *out[0].Name)
		assert.Equal(t, "b", *out[1].Name)
		assert.Equal(t, "c", *out[2].Name)
		// Distinct backing pointers, not aliases of one slot.
		assert.NotSame(t, out[0].Value.StringValue, out[1].Value.StringValue)
		assert.NotSame(t, out[0].Name, out[1].Name)
	})
}
