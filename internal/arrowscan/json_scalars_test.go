package arrowscan

import (
	"strings"
	"testing"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
)

type nestedScalarJSONFixture struct {
	name  string
	dtype arrow.DataType
	input string
	want  string
}

func nestedScalarJSONFixtures() []nestedScalarJSONFixture {
	return []nestedScalarJSONFixture{
		{"boolean", arrow.ListOf(arrow.FixedWidthTypes.Boolean), `[true,false,null]`, `[true,false,null]`},
		{"int8", arrow.ListOf(arrow.PrimitiveTypes.Int8), `[-128,-1,0,1,127,null]`, `[-128,-1,0,1,127,null]`},
		{"int16", arrow.ListOf(arrow.PrimitiveTypes.Int16), `[-32768,-1,0,1,32767,null]`, `[-32768,-1,0,1,32767,null]`},
		{"int32", arrow.ListOf(arrow.PrimitiveTypes.Int32), `[-2147483648,-1,0,1,2147483647,null]`, `[-2147483648,-1,0,1,2147483647,null]`},
		{"int64", arrow.ListOf(arrow.PrimitiveTypes.Int64), `[-9223372036854775808,-1,0,1,9223372036854775807,null]`, `[-9223372036854775808,-1,0,1,9223372036854775807,null]`},
		{
			"map_boolean", arrow.MapOf(arrow.BinaryTypes.String, arrow.FixedWidthTypes.Boolean),
			`[{"key":"t","value":true},{"key":"f","value":false},{"key":"n","value":null}]`,
			`{"t":true,"f":false,"n":null}`,
		},
		{
			"map_int64", arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int64),
			`[{"key":"quote\"&<>","value":-9223372036854775808},{"key":"max","value":9223372036854775807},{"key":"n","value":null}]`,
			`{"quote\"\u0026\u003c\u003e":-9223372036854775808,"max":9223372036854775807,"n":null}`,
		},
		{
			"struct", arrow.StructOf(
				arrow.Field{Name: "flag", Type: arrow.FixedWidthTypes.Boolean},
				arrow.Field{Name: "tiny", Type: arrow.PrimitiveTypes.Int8},
				arrow.Field{Name: "small", Type: arrow.PrimitiveTypes.Int16},
				arrow.Field{Name: "int", Type: arrow.PrimitiveTypes.Int32},
				arrow.Field{Name: "big", Type: arrow.PrimitiveTypes.Int64},
				arrow.Field{Name: "missing", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
				arrow.Field{Name: "s", Type: arrow.BinaryTypes.String},
			),
			`{"flag":false,"tiny":-128,"small":-32768,"int":-2147483648,"big":-9223372036854775808,"missing":null,"s":"quote\"&<>"}`,
			`{"flag":false,"tiny":-128,"small":-32768,"int":-2147483648,"big":-9223372036854775808,"missing":null,"s":"quote\"\u0026\u003c\u003e"}`,
		},
	}
}

func TestScanCellNestedScalarJSON(t *testing.T) {
	for _, tc := range nestedScalarJSONFixtures() {
		t.Run(tc.name, func(t *testing.T) {
			input := "[" + tc.input + ",null," + tc.input + "]"
			full, _, err := array.FromJSON(memory.DefaultAllocator, tc.dtype, strings.NewReader(input), array.WithUseNumber())
			if err != nil {
				t.Fatal(err)
			}
			defer full.Release()
			sliced := array.NewSlice(full, 1, 3)
			defer sliced.Release()
			for _, cache := range []*StructKeyCache{nil, NewStructKeyCache()} {
				for _, col := range []arrow.Array{full, sliced} {
					for row := 0; row < col.Len(); row++ {
						var want any = tc.want
						if row+col.Data().Offset() == 1 {
							want = nil
						}
						got, err := ScanCellCached(col, row, nil, cache)
						if err != nil {
							t.Fatal(err)
						}
						if got != want {
							t.Errorf("offset %d row %d: got %v, want %v", col.Data().Offset(), row, got, want)
						}
					}
				}
			}
		})
	}
}

func BenchmarkScanCellNestedScalarJSON(b *testing.B) {
	for _, tc := range nestedScalarJSONFixtures() {
		b.Run(tc.name, func(b *testing.B) {
			col, _, err := array.FromJSON(memory.DefaultAllocator, tc.dtype, strings.NewReader("["+tc.input+"]"), array.WithUseNumber())
			if err != nil {
				b.Fatal(err)
			}
			defer col.Release()
			cache := NewStructKeyCache()
			b.ReportAllocs()
			for b.Loop() {
				if _, err := ScanCellCached(col, 0, nil, cache); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
