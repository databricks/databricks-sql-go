package arrowbased

import (
	"testing"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/decimal128"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDecimal128ContainerValue verifies that the native Arrow decimal path
// decodes DECIMAL columns losslessly as strings, preserving full precision and
// scale, and that nulls are surfaced as nil. This is the value-level coverage
// for the path exercised when UseArrowNativeDecimal is enabled (the boundary
// test in TestArrowRowScanner only proves ScanRow no longer blocks decimals).
func TestDecimal128ContainerValue(t *testing.T) {
	const precision, scale int32 = 38, 18

	// A 38-digit, scale-18 value that float64 (≈15-17 significant digits)
	// cannot represent exactly — the regression guard for lossy conversion.
	const highPrecision = "12345678901234567890.123456789012345678"

	// Note: ToString(scale) always renders to the column's full declared scale
	// (18 here), so values are normalized — e.g. "3.30" -> "3.300000000000000000".
	// This is lossless and deterministic, unlike a float64 conversion.
	cases := []struct {
		name     string
		input    string // empty string => null
		expected any
	}{
		{name: "high precision preserved", input: highPrecision, expected: highPrecision},
		{name: "simple value normalized to scale", input: "3.30", expected: "3.300000000000000000"},
		{name: "negative value", input: "-0.000000000000000001", expected: "-0.000000000000000001"},
		{name: "zero", input: "0.000000000000000000", expected: "0.000000000000000000"},
		{name: "null", input: "", expected: nil},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mem := memory.NewGoAllocator()
			dt := &arrow.Decimal128Type{Precision: precision, Scale: scale}
			b := array.NewDecimal128Builder(mem, dt)
			defer b.Release()

			if tc.input == "" {
				b.AppendNull()
			} else {
				num, err := decimal128.FromString(tc.input, precision, scale)
				require.NoError(t, err)
				b.Append(num)
			}

			arr := b.NewDecimal128Array()
			defer arr.Release()

			container := &decimal128Container{scale: scale}
			err := container.SetValueArray(arr.Data())
			require.NoError(t, err)

			assert.Equal(t, tc.input == "", container.IsNull(0))

			got, err := container.Value(0)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, got)
		})
	}
}
