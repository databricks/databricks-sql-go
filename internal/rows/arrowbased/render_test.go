package arrowbased

import (
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestComplexRenderingGolden(t *testing.T) {
	cases := []struct {
		name     string
		dataType arrow.DataType
		input    string
		golden   []string
	}{
		{"null", arrow.Null, `[null,null]`, []string{"null", "null"}},
		{"bool", arrow.FixedWidthTypes.Boolean, `[true,null,false]`, []string{"true", "null", "false"}},
		{"int8", arrow.PrimitiveTypes.Int8, `[-128,null,127]`, []string{"-128", "null", "127"}},
		{"int16", arrow.PrimitiveTypes.Int16, `[-32768,null,32767]`, []string{"-32768", "null", "32767"}},
		{"int32", arrow.PrimitiveTypes.Int32, `[-2147483648,null,2147483647]`, []string{"-2147483648", "null", "2147483647"}},
		{"int64", arrow.PrimitiveTypes.Int64, `[-9223372036854775808,null,9007199254740993]`, []string{"-9223372036854775808", "null", "9007199254740993"}},
		{"float32", arrow.PrimitiveTypes.Float32, `[0.1,null,-2.5]`, []string{"0.1", "null", "-2.5"}},
		{"float64", arrow.PrimitiveTypes.Float64, `[0.30000000000000004,null,1e21]`, []string{"0.30000000000000004", "null", "1e+21"}},
		{"string", arrow.BinaryTypes.String, `["a\"\\\n\t",null,"<>&\u2028日本語"]`, []string{`"a\"\\\n\t"`, "null", `"\u003c\u003e\u0026\u2028日本語"`}},
		{"binary", arrow.BinaryTypes.Binary, `["YWJj",null,""]`, []string{`"YWJj"`, "null", `""`}},
		{"decimal", &arrow.Decimal128Type{Precision: 38, Scale: 2}, `["123456789012345678901234567890.12",null,"-0.01"]`, []string{"123456789012345678901234567890.12", "null", "-0.01"}},
		{"date", arrow.FixedWidthTypes.Date32, `["2026-11-26",null,"1969-12-31"]`, []string{`"2026-11-26 00:00:00 +0000 UTC"`, "null", `"1969-12-31 00:00:00 +0000 UTC"`}},
		{"time", arrow.FixedWidthTypes.Time64ns, `["12:34:56.123456789",null,"00:00:00"]`, []string{`"12:34:56.123456789"`, "null", `"00:00:00"`}},
		{"timestamp", &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, `[0,null,1234567]`, []string{`"1970-01-01 00:00:00 +0000 UTC"`, "null", `"1970-01-01 00:00:01.234567 +0000 UTC"`}},
		{"timestamp_ntz", &arrow.TimestampType{Unit: arrow.Microsecond}, `[0,null,-1]`, []string{`"1970-01-01 00:00:00 +0000 UTC"`, "null", `"1969-12-31 23:59:59.999999 +0000 UTC"`}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			var inputs []json.RawMessage
			require.NoError(t, json.Unmarshal([]byte(test.input), &inputs))
			fields := make([]arrow.Field, len(inputs))
			structInput := make(map[string]json.RawMessage, len(inputs))
			structGolden := make([]string, len(inputs))
			mapInputs := make([]string, len(inputs))
			mapGolden := make([]string, len(inputs))
			for index, input := range inputs {
				name := "f\"\\\n<>&日本語" + strconv.Itoa(index)
				fields[index] = arrow.Field{Name: name, Type: test.dataType, Nullable: true}
				structInput[name] = input
				key, err := json.Marshal(name)
				require.NoError(t, err)
				structGolden[index] = string(key) + ":" + test.golden[index]
				mapInputs[index] = fmt.Sprintf(`{"key":%d,"value":%s}`, index, input)
				mapGolden[index] = fmt.Sprintf(`"%d":%s`, index, test.golden[index])
			}
			structJSON, err := json.Marshal(structInput)
			require.NoError(t, err)
			listType := arrow.ListOf(test.dataType)
			mapType := arrow.MapOf(arrow.PrimitiveTypes.Int64, test.dataType)
			structType := arrow.StructOf(fields...)
			listGolden := "[" + strings.Join(test.golden, ",") + "]"
			objectGolden := "{" + strings.Join(structGolden, ",") + "}"
			mapJSON := "[" + strings.Join(mapInputs, ",") + "]"
			mapResult := "{" + strings.Join(mapGolden, ",") + "}"
			type renderShape struct {
				name     string
				dataType arrow.DataType
				input    string
				golden   string
			}
			shapes := []renderShape{
				{"list", listType, test.input, listGolden},
				{"map", mapType, mapJSON, mapResult},
				{"struct", structType, string(structJSON), objectGolden},
				{"empty_list", listType, "[]", "[]"},
				{"empty_map", mapType, "[]", "{}"},
				{"empty_struct", arrow.StructOf(), "{}", "{}"},
				{"list_list", arrow.ListOf(listType), "[[]," + test.input + ",null]", "[[]," + listGolden + ",null]"},
				{"list_struct", arrow.ListOf(structType), "[" + string(structJSON) + ",null]", "[" + objectGolden + ",null]"},
				{"map_map", arrow.MapOf(arrow.PrimitiveTypes.Int64, mapType), `[{"key":7,"value":` + mapJSON + `}]`, `{"7":` + mapResult + `}`},
				{"map_list", arrow.MapOf(arrow.PrimitiveTypes.Int64, listType), `[{"key":7,"value":` + test.input + `},{"key":8,"value":null}]`, `{"7":` + listGolden + `,"8":null}`},
				{"struct_map_list", arrow.StructOf(arrow.Field{Name: "items", Type: arrow.MapOf(arrow.BinaryTypes.String, listType)}), `{"items":[{"key":"a\"<","value":` + test.input + `}]}`, `{"items":{"a\"\u003c":` + listGolden + `}}`},
			}
			listKey, err := json.Marshal(listGolden)
			require.NoError(t, err)
			structKey, err := json.Marshal(objectGolden)
			require.NoError(t, err)
			listKeys := arrow.MapOf(listType, arrow.BinaryTypes.String)
			structKeys := arrow.MapOf(structType, arrow.BinaryTypes.String)
			listKeysInput, listKeysGolden := `[{"key":`+test.input+`,"value":"value"}]`, "{"+string(listKey)+`:"value"}`
			structKeysInput, structKeysGolden := `[{"key":`+string(structJSON)+`,"value":"value"}]`, "{"+string(structKey)+`:"value"}`
			shapes = append(shapes,
				renderShape{"list_keys", listKeys, listKeysInput, listKeysGolden},
				renderShape{"struct_keys", structKeys, structKeysInput, structKeysGolden},
				renderShape{"list_list_keys", arrow.ListOf(listKeys), "[" + listKeysInput + ",null]", "[" + listKeysGolden + ",null]"},
				renderShape{"list_struct_keys", arrow.ListOf(structKeys), "[" + structKeysInput + ",null]", "[" + structKeysGolden + ",null]"},
			)
			if test.name != "null" {
				key := test.golden[0]
				if !strings.HasPrefix(key, `"`) {
					key = `"` + key + `"`
				}
				shapes = append(shapes, renderShape{"map_keys", arrow.MapOf(test.dataType, arrow.BinaryTypes.String), `[{"key":` + string(inputs[0]) + `,"value":"value"}]`, "{" + key + `:"value"}`})
			}
			for _, shape := range shapes {
				t.Run(shape.name, func(t *testing.T) {
					pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
					defer pool.AssertSize(t, 0)
					builder := array.NewBuilder(pool, shape.dataType)
					defer builder.Release()
					require.NoError(t, builder.UnmarshalJSON([]byte("["+shape.input+",null,"+shape.input+"]")))
					values := builder.NewArray()
					defer values.Release()
					sliced := array.NewSlice(values, 1, 3)
					defer sliced.Release()
					for _, values := range []arrow.Array{values, sliced} {
						holder, err := (&arrowValueContainerMaker{}).makeColumnValueContainer(values.DataType(), time.UTC, func(timestamp arrow.Timestamp) time.Time {
							return timestamp.ToTime(arrow.Microsecond)
						}, nil)
						require.NoError(t, err)
						defer holder.Release()
						require.NoError(t, holder.SetValueArray(values.Data()))
						for row := 0; row < values.Len(); row++ {
							if holder.IsNull(row) {
								continue
							}
							got, err := holder.Value(row)
							require.NoError(t, err)
							require.Equal(t, shape.golden, got)
						}
						got, err := holder.Value(values.Len())
						require.NoError(t, err)
						require.Nil(t, got)
					}
				})
			}
		})
	}
}

func TestComplexRenderingAllocations(t *testing.T) {
	const width = 5000
	const runs = 3
	const maxBytesPerOp = uint64(16 << 20)
	for _, shape := range []string{"array", "map", "struct", "struct_array"} {
		t.Run(shape, func(t *testing.T) {
			values := buildWideRenderValues(t, shape, width)
			defer values.Release()
			holder, err := (&arrowValueContainerMaker{}).makeColumnValueContainer(values.DataType(), time.UTC, nil, nil)
			require.NoError(t, err)
			defer holder.Release()
			require.NoError(t, holder.SetValueArray(values.Data()))
			_, err = holder.Value(0)
			require.NoError(t, err)

			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			for iteration := 0; iteration < runs; iteration++ {
				if _, err := holder.Value(0); err != nil {
					t.Fatal(err)
				}
			}
			runtime.ReadMemStats(&after)
			bytesPerOp := (after.TotalAlloc - before.TotalAlloc) / runs
			t.Logf("%d bytes/op (limit %d)", bytesPerOp, maxBytesPerOp)
			require.Less(t, bytesPerOp, maxBytesPerOp)
		})
	}
}

func TestEmptyComplexRenderingAllocations(test *testing.T) {
	for _, shape := range []string{"array", "map", "struct"} {
		test.Run(shape, func(test *testing.T) {
			values := buildWideRenderValues(test, shape, 0)
			defer values.Release()
			holder, err := (&arrowValueContainerMaker{}).makeColumnValueContainer(values.DataType(), time.UTC, nil, nil)
			require.NoError(test, err)
			defer holder.Release()
			require.NoError(test, holder.SetValueArray(values.Data()))
			allocations := testing.AllocsPerRun(100, func() {
				if _, err := holder.Value(0); err != nil {
					test.Fatal(err)
				}
			})
			require.LessOrEqual(test, allocations, float64(2))
		})
	}
}

func TestComplexRenderingScratch(test *testing.T) {
	for _, shape := range []string{"array", "map", "struct", "struct_array"} {
		test.Run(shape, func(test *testing.T) {
			values := buildWideRenderValues(test, shape, 64)
			defer values.Release()
			holder, err := (&arrowValueContainerMaker{}).makeColumnValueContainer(values.DataType(), time.UTC, nil, nil)
			require.NoError(test, err)
			scratch := renderScratch(holder)
			defer func() {
				holder.Release()
				require.Nil(test, *scratch)
			}()
			*scratch = []byte("previous batch")
			require.NoError(test, holder.SetValueArray(values.Data()))
			require.Nil(test, *scratch)
			first, err := holder.Value(0)
			require.NoError(test, err)
			expected := strings.Clone(first.(string))
			require.NotEmpty(test, *scratch)
			bufferStart := &(*scratch)[0]
			second, err := holder.Value(0)
			require.NoError(test, err)
			require.Equal(test, expected, second)
			require.Same(test, bufferStart, &(*scratch)[0])
			for index := range *scratch {
				(*scratch)[index] = 'x'
			}
			require.Equal(test, expected, first)
			third, err := holder.Value(0)
			require.NoError(test, err)
			require.Equal(test, expected, third)
			if shape == "struct_array" {
				child := holder.(*structValueContainer).fieldValues[0].(*listValueContainer)
				require.Nil(test, child.scratch)
			}
		})
	}
}

func TestComplexRenderingScratchBound(test *testing.T) {
	const largeLen = 4 << 20
	large := `"` + strings.Repeat("a", largeLen) + `"`
	listType := arrow.ListOf(arrow.BinaryTypes.String)
	shapes := []struct {
		name     string
		dataType arrow.DataType
		input    string
		small    string
	}{
		{"array", listType, `[[` + large + `],["b"]]`, `["b"]`},
		{"map", arrow.MapOf(arrow.PrimitiveTypes.Int64, arrow.BinaryTypes.String), `[[{"key":1,"value":` + large + `}],[{"key":2,"value":"b"}]]`, `{"2":"b"}`},
		{"struct", arrow.StructOf(arrow.Field{Name: "s", Type: arrow.BinaryTypes.String}), `[{"s":` + large + `},{"s":"b"}]`, `{"s":"b"}`},
		{"struct_array", arrow.StructOf(arrow.Field{Name: "items", Type: listType}), `[{"items":[` + large + `]},{"items":["b"]}]`, `{"items":["b"]}`},
	}
	for _, shape := range shapes {
		test.Run(shape.name, func(test *testing.T) {
			holder := newJSONHolder(test, shape.dataType, shape.input)
			scratch := renderScratch(holder)
			got, err := holder.Value(1)
			require.NoError(test, err)
			require.Equal(test, shape.small, got)
			bufferStart := &(*scratch)[0]

			before := liveHeap()
			got, err = holder.Value(0)
			require.NoError(test, err)
			require.Greater(test, len(got.(string)), largeLen)
			got, err = holder.Value(1)
			require.NoError(test, err)
			require.Equal(test, shape.small, got)
			after := liveHeap()
			require.Less(test, after, before+heapGuard)
			require.LessOrEqual(test, cap(*scratch), 64<<10)
			require.Same(test, bufferStart, &(*scratch)[0])
		})
	}
	// About 57 KB of output, but grown element by element into a 72 KiB buffer.
	test.Run("near_bound", func(test *testing.T) {
		values := buildWideRenderValues(test, "array", 3400)
		defer values.Release()
		holder, err := (&arrowValueContainerMaker{}).makeColumnValueContainer(values.DataType(), time.UTC, nil, nil)
		require.NoError(test, err)
		defer holder.Release()
		require.NoError(test, holder.SetValueArray(values.Data()))
		grown, err := holder.(complexValueAppender).appendValue(nil, 0)
		require.NoError(test, err)
		require.Greater(test, cap(grown), 64<<10)
		got, err := holder.Value(0)
		require.NoError(test, err)
		require.Less(test, len(got.(string)), 64<<10)
		scratch := *renderScratch(holder)
		require.True(test, scratch == nil, "retained a %d-byte buffer", cap(scratch))
	})
}

func TestComplexRenderingSmallAfterLarge(test *testing.T) {
	input := `[["` + strings.Repeat("a", 1<<20) + `"],["0123456789ab"]]`
	for _, sequence := range []string{"LS", "LLS", "LLSLSLS", strings.Repeat("L", 20) + "S"} {
		test.Run(sequence, func(test *testing.T) {
			holder := newJSONHolder(test, arrow.ListOf(arrow.BinaryTypes.String), input)
			for step, kind := range sequence {
				if kind == 'L' {
					_, err := holder.Value(0)
					require.NoError(test, err)
					continue
				}
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				got, err := holder.Value(1)
				runtime.ReadMemStats(&after)
				require.NoError(test, err)
				require.Equal(test, `["0123456789ab"]`, got)
				require.Less(test, after.TotalAlloc-before.TotalAlloc, uint64(4<<10), "step %d", step)
			}
		})
	}
}

func TestComplexRenderingComplexKeyScratch(test *testing.T) {
	const keyLen = 50 << 10
	keysType := arrow.MapOf(arrow.ListOf(arrow.BinaryTypes.String), arrow.BinaryTypes.String)
	row := func(key string) string { return `[{"key":["` + key + `"],"value":"v"}]` }
	large, small := row(strings.Repeat("a", keyLen)), row("b")
	shapes := []struct {
		name     string
		dataType arrow.DataType
		input    string
		golden   string
	}{
		{"map", keysType, "[" + large + "," + small + "]", `{"[\"b\"]":"v"}`},
		{"list_map", arrow.ListOf(keysType), "[[" + large + "],[" + small + "]]", `[{"[\"b\"]":"v"}]`},
	}
	for _, shape := range shapes {
		test.Run(shape.name, func(test *testing.T) {
			holder := newJSONHolder(test, shape.dataType, shape.input)
			container, ok := holder.(*mapValueContainer)
			if !ok {
				container = holder.(*listValueContainer).values.(*mapValueContainer)
			}

			before := liveHeap()
			got, err := holder.Value(0)
			require.NoError(test, err)
			require.Greater(test, len(got.(string)), keyLen)
			for range 3 {
				got, err = holder.Value(1)
				require.NoError(test, err)
				require.Equal(test, shape.golden, got)
			}
			after := liveHeap()
			require.Less(test, after, before+heapGuard)
			require.LessOrEqual(test, cap(*renderScratch(holder)), 64<<10)
			require.Nil(test, *renderScratch(container.keys))
			if !ok {
				require.Nil(test, *renderScratch(container))
			}
		})
	}
}

// heapGuard loosely bounds live heap growth; exact bounds are asserted on capacities.
const heapGuard = 96 << 10

// liveHeap runs two GC cycles so sync.Pool victims (encoding/json buffers) are freed.
func liveHeap() uint64 {
	runtime.GC()
	runtime.GC()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return stats.HeapAlloc
}

type failingValues struct {
	columnValues
	row int
}

func (values failingValues) Value(row int) (any, error) {
	if row == values.row {
		return nil, errors.New("render failure")
	}
	return values.columnValues.Value(row)
}

func TestComplexRenderingAfterError(test *testing.T) {
	listType := arrow.ListOf(arrow.PrimitiveTypes.Int64)
	scalarMap := arrow.MapOf(arrow.PrimitiveTypes.Int64, arrow.PrimitiveTypes.Int64)
	scalarMapInput := `[[{"key":1,"value":10},{"key":2,"value":20}],[{"key":3,"value":30}]]`
	fail := func(values *columnValues) { *values = failingValues{*values, 1} }
	shapes := []struct {
		name     string
		dataType arrow.DataType
		input    string
		inject   func(columnValues)
	}{
		{"array", listType, `[[1,2],[3]]`, func(holder columnValues) {
			fail(&holder.(*listValueContainer).values)
		}},
		{"struct_array", arrow.StructOf(arrow.Field{Name: "items", Type: listType}), `[{"items":[1,2]},{"items":[3]}]`, func(holder columnValues) {
			fail(&holder.(*structValueContainer).fieldValues[0].(*listValueContainer).values)
		}},
		{"map_key", scalarMap, scalarMapInput, func(holder columnValues) {
			fail(&holder.(*mapValueContainer).keys)
		}},
		{"map_complex_key", arrow.MapOf(listType, arrow.PrimitiveTypes.Int64), `[[{"key":[1,2],"value":10}],[{"key":[3],"value":30}]]`, func(holder columnValues) {
			fail(&holder.(*mapValueContainer).keys.(*listValueContainer).values)
		}},
		{"map_value", scalarMap, scalarMapInput, func(holder columnValues) {
			fail(&holder.(*mapValueContainer).values)
		}},
	}
	for _, shape := range shapes {
		test.Run(shape.name, func(test *testing.T) {
			want, err := newJSONHolder(test, shape.dataType, shape.input).Value(1)
			require.NoError(test, err)
			holder := newJSONHolder(test, shape.dataType, shape.input)
			shape.inject(holder)
			for range 2 {
				got, err := holder.Value(0)
				require.EqualError(test, err, "render failure")
				require.Nil(test, got)
				got, err = holder.Value(1)
				require.NoError(test, err)
				require.Equal(test, want, got)
			}
		})
	}
}

func newJSONHolder(test *testing.T, dataType arrow.DataType, input string) columnValues {
	test.Helper()
	builder := array.NewBuilder(memory.NewGoAllocator(), dataType)
	defer builder.Release()
	require.NoError(test, builder.UnmarshalJSON([]byte(input)))
	values := builder.NewArray()
	test.Cleanup(values.Release)
	holder, err := (&arrowValueContainerMaker{}).makeColumnValueContainer(values.DataType(), time.UTC, nil, nil)
	require.NoError(test, err)
	test.Cleanup(holder.Release)
	require.NoError(test, holder.SetValueArray(values.Data()))
	return holder
}

func renderScratch(holder columnValues) *[]byte {
	switch container := holder.(type) {
	case *listValueContainer:
		return &container.scratch
	case *mapValueContainer:
		return &container.scratch
	case *structValueContainer:
		return &container.scratch
	}
	return nil
}

func buildWideRenderValues(t testing.TB, shape string, width int) arrow.Array {
	t.Helper()
	pool := memory.NewGoAllocator()
	var builder array.Builder
	switch shape {
	case "array":
		builder = array.NewListBuilder(pool, arrow.PrimitiveTypes.Float64)
	case "map":
		builder = array.NewMapBuilder(pool, arrow.PrimitiveTypes.Int64, arrow.PrimitiveTypes.Float64, false)
	case "struct":
		fields := make([]arrow.Field, width)
		for index := range fields {
			fields[index] = arrow.Field{Name: "f" + strconv.Itoa(index), Type: arrow.PrimitiveTypes.Float64}
		}
		builder = array.NewStructBuilder(pool, arrow.StructOf(fields...))
	case "struct_array":
		values := buildWideRenderValues(t, "array", width)
		defer values.Release()
		nested, err := array.NewStructArray([]arrow.Array{values}, []string{"items"})
		require.NoError(t, err)
		return nested
	default:
		t.Fatalf("unknown render shape %q", shape)
	}
	defer builder.Release()
	switch builder := builder.(type) {
	case *array.ListBuilder:
		builder.Append(true)
	case *array.MapBuilder:
		builder.Append(true)
	case *array.StructBuilder:
		builder.Append(true)
	}
	for index := 0; index < width; index++ {
		value := float64(index)/7 - 0.5
		switch builder := builder.(type) {
		case *array.ListBuilder:
			builder.ValueBuilder().(*array.Float64Builder).Append(value)
		case *array.MapBuilder:
			builder.KeyBuilder().(*array.Int64Builder).Append(int64(index))
			builder.ItemBuilder().(*array.Float64Builder).Append(value)
		case *array.StructBuilder:
			builder.FieldBuilder(index).(*array.Float64Builder).Append(value)
		}
	}
	return builder.NewArray()
}
