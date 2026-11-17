package arrowbased

import (
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/databricks/databricks-sql-go/internal/arrowscan"
)

func TestNonFiniteFloatRendering(t *testing.T) {
	for _, dt := range []arrow.DataType{arrow.PrimitiveTypes.Float32, arrow.PrimitiveTypes.Float64} {
		t.Run(dt.Name(), func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer pool.AssertSize(t, 0)
			values := []float64{1.5, math.NaN(), math.Inf(1), math.Inf(-1)}
			names := []string{"finite", "nan", "positive", "negative"}
			appendValue := func(b array.Builder, value float64) {
				switch b := b.(type) {
				case *array.Float32Builder:
					b.Append(float32(value))
				case *array.Float64Builder:
					b.Append(value)
				default:
					t.Fatalf("unexpected float builder %T", b)
				}
			}
			appendListValues := func(b array.Builder) {
				for _, value := range values {
					appendValue(b, value)
				}
				b.AppendNull()
			}
			t.Run("top_level", func(t *testing.T) {
				b := array.NewBuilder(pool, dt)
				defer b.Release()
				appendListValues(b)
				arr := b.NewArray()
				defer arr.Release()
				holder, err := (&arrowValueContainerMaker{}).makeColumnValueContainer(dt, time.UTC, nil, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer holder.Release()
				if err := holder.SetValueArray(arr.Data()); err != nil {
					t.Fatal(err)
				}
				for row, want := range values {
					thrift, thriftErr := holder.Value(row)
					kernel, kernelErr := arrowscan.ScanCell(arr, row, time.UTC)
					if thriftErr != nil || kernelErr != nil {
						t.Fatalf("row %d: thrift error %v, kernel error %v", row, thriftErr, kernelErr)
					}
					for _, value := range []any{thrift, kernel} {
						var got float64
						switch value := value.(type) {
						case float32:
							if dt.ID() != arrow.FLOAT32 {
								t.Fatalf("unexpected float32 for %s", dt)
							}
							got = float64(value)
						case float64:
							if dt.ID() != arrow.FLOAT64 {
								t.Fatalf("unexpected float64 for %s", dt)
							}
							got = value
						default:
							t.Fatalf("top-level value has non-numeric type %T", value)
						}
						if got != want && !(math.IsNaN(got) && math.IsNaN(want)) {
							t.Errorf("row %d: got %v, want %v", row, got, want)
						}
					}
				}
				kernelNull, err := arrowscan.ScanCell(arr, len(values), time.UTC)
				if !holder.IsNull(len(values)) || kernelNull != nil || err != nil {
					t.Fatalf("top-level NULL changed: kernel %v, %v", kernelNull, err)
				}
			})
			const listJSON = `[1.5,"NaN","Infinity","-Infinity",null]`
			cases := []struct {
				name  string
				build func() arrow.Array
				want  string
			}{
				{"array", func() arrow.Array {
					b := array.NewListBuilder(pool, dt)
					defer b.Release()
					b.Append(true)
					appendListValues(b.ValueBuilder())
					return b.NewArray()
				}, listJSON},
				{"map_values", func() arrow.Array {
					b := array.NewMapBuilder(pool, arrow.BinaryTypes.String, dt, false)
					defer b.Release()
					b.Append(true)
					b.KeyBuilder().(*array.StringBuilder).AppendValues(append(names, "null"), nil)
					appendListValues(b.ItemBuilder())
					return b.NewArray()
				}, `{"finite":1.5,"nan":"NaN","positive":"Infinity","negative":"-Infinity","null":null}`},
				{"map_keys", func() arrow.Array {
					b := array.NewMapBuilder(pool, dt, arrow.BinaryTypes.String, false)
					defer b.Release()
					b.Append(true)
					for _, value := range values {
						appendValue(b.KeyBuilder(), value)
					}
					b.ItemBuilder().(*array.StringBuilder).AppendValues(names, nil)
					return b.NewArray()
				}, `{"1.5":"finite","NaN":"nan","+Inf":"positive","-Inf":"negative"}`},
				{"struct", func() arrow.Array {
					fields := make([]arrow.Field, 0, len(names)+1)
					for _, name := range append(names, "null") {
						fields = append(fields, arrow.Field{Name: name, Type: dt, Nullable: true})
					}
					b := array.NewStructBuilder(pool, arrow.StructOf(fields...))
					defer b.Release()
					b.Append(true)
					for i, value := range values {
						appendValue(b.FieldBuilder(i), value)
					}
					b.FieldBuilder(len(values)).AppendNull()
					return b.NewArray()
				}, `{"finite":1.5,"nan":"NaN","positive":"Infinity","negative":"-Infinity","null":null}`},
				{"nested", func() arrow.Array {
					b := array.NewStructBuilder(pool, arrow.StructOf(arrow.Field{
						Name: "values", Type: arrow.ListOf(dt), Nullable: true,
					}))
					defer b.Release()
					b.Append(true)
					list := b.FieldBuilder(0).(*array.ListBuilder)
					list.Append(true)
					appendListValues(list.ValueBuilder())
					return b.NewArray()
				}, `{"values":` + listJSON + `}`},
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					arr := tc.build()
					defer arr.Release()
					holder, err := (&arrowValueContainerMaker{}).makeColumnValueContainer(arr.DataType(), time.UTC, nil, nil)
					if err != nil {
						t.Fatal(err)
					}
					defer holder.Release()
					if err := holder.SetValueArray(arr.Data()); err != nil {
						t.Fatal(err)
					}
					thrift, thriftErr := holder.Value(0)
					kernel, kernelErr := arrowscan.ScanCell(arr, 0, time.UTC)
					for _, result := range []struct {
						backend string
						value   any
						err     error
					}{{"thrift", thrift, thriftErr}, {"kernel", kernel, kernelErr}} {
						if result.err != nil || result.value != tc.want {
							t.Errorf("%s: got %v, %v; want %s", result.backend, result.value, result.err, tc.want)
						} else if !json.Valid([]byte(result.value.(string))) {
							t.Errorf("%s: invalid JSON: %s", result.backend, result.value)
						}
					}
				})
			}
		})
	}
}
