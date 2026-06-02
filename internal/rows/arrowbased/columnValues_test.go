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

// TestDecimal128NestedInListPreservesPrecision guards the DEFAULT path: complex
// types are native by default (UseArrowNativeComplexTypes=true), so a decimal
// nested inside an ARRAY/STRUCT is decoded by decimal128Container even when the
// top-level UseArrowNativeDecimal flag is off. Before the lossless-string fix
// this path went through ToFloat64 and silently corrupted high-precision values
// (and rendered SQL NULL as the number 0). This test pins the lossless behavior.
func TestDecimal128NestedInListPreservesPrecision(t *testing.T) {
	const precision, scale int32 = 38, 18
	const highPrecision = "12345678901234567890.123456789012345678"

	mem := memory.NewGoAllocator()
	elemType := &arrow.Decimal128Type{Precision: precision, Scale: scale}
	lb := array.NewListBuilder(mem, elemType)
	defer lb.Release()
	vb := lb.ValueBuilder().(*array.Decimal128Builder)

	// One list: [highPrecision, NULL, "3.300000000000000000"]
	lb.Append(true)
	for _, s := range []string{highPrecision, "", "3.30"} {
		if s == "" {
			vb.AppendNull()
			continue
		}
		num, err := decimal128.FromString(s, precision, scale)
		require.NoError(t, err)
		vb.Append(num)
	}

	listArr := lb.NewListArray()
	defer listArr.Release()

	lvc := &listValueContainer{
		listArray:     listArr,
		listArrayType: arrow.ListOf(elemType),
		values:        &decimal128Container{scale: scale},
	}
	require.NoError(t, lvc.values.SetValueArray(listArr.ListValues().Data()))

	got, err := lvc.Value(0)
	require.NoError(t, err)

	// Decimals are rendered as lossless JSON strings; NULL stays null (not 0).
	expected := `["12345678901234567890.123456789012345678",null,"3.300000000000000000"]`
	assert.Equal(t, expected, got)
}
