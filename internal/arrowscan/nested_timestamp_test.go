package arrowscan

import (
	"bytes"
	"os"
	"testing"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/ipc"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/stretchr/testify/require"
)

func typeName(f arrow.Field) string {
	v, _ := f.Metadata.GetValue(databricksTypeNameMetadataKey)
	return v
}

// sameLayout compares types ignoring field metadata. arrow.TypeEqual cannot be
// used directly: arrow v12 compares maps with reflect.DeepEqual.
func sameLayout(a, b arrow.DataType) bool {
	am, aok := a.(*arrow.MapType)
	bm, bok := b.(*arrow.MapType)
	if aok || bok {
		return aok && bok && am.KeysSorted == bm.KeysSorted &&
			am.ItemField().Nullable == bm.ItemField().Nullable &&
			sameLayout(am.KeyType(), bm.KeyType()) && sameLayout(am.ItemType(), bm.ItemType())
	}
	an, aok := a.(arrow.NestedType)
	bn, bok := b.(arrow.NestedType)
	if !aok || !bok {
		return arrow.TypeEqual(a, b)
	}
	if a.ID() != b.ID() || len(an.Fields()) != len(bn.Fields()) {
		return false
	}
	for i, af := range an.Fields() {
		bf := bn.Fields()[i]
		if af.Name != bf.Name || af.Nullable != bf.Nullable || !sameLayout(af.Type, bf.Type) {
			return false
		}
	}
	return true
}

// withType re-types col's top-level array data, as the C Data import does from
// the labelled schema (arrow's FixedSizeListBuilder drops element metadata).
func withType(col arrow.Array, dt arrow.DataType) arrow.Array {
	d := col.Data()
	nd := array.NewData(dt, d.Len(), d.Buffers(), d.Children(), d.NullN(), d.Offset())
	defer nd.Release()
	return array.MakeFromData(nd)
}

// TestWithNestedTimestampTypesKernelSchema labels every nested timestamp leaf of
// a live kernel schema from its column's Spark JSON type, without changing the
// physical layout or the top-level fields' metadata.
func TestWithNestedTimestampTypesKernelSchema(t *testing.T) {
	data, err := os.ReadFile("testdata/timestamp_ntz_nested_kernel.arrow")
	require.NoError(t, err)
	reader, err := ipc.NewReader(bytes.NewReader(data))
	require.NoError(t, err)
	defer reader.Release()
	in := reader.Schema()

	out := WithNestedTimestampTypes(in)
	require.Len(t, out.Fields(), len(in.Fields()))
	for i := range in.Fields() {
		require.True(t, sameLayout(in.Field(i).Type, out.Field(i).Type), "column %d layout changed", i)
		require.Equal(t, in.Field(i).Metadata, out.Field(i).Metadata)
		require.Equal(t, in.Field(i).Name, out.Field(i).Name)
	}

	arr := out.Field(0).Type.(*arrow.ListType)
	require.Equal(t, "TIMESTAMP_NTZ", typeName(arr.ElemField()))

	m := out.Field(1).Type.(*arrow.MapType)
	require.Equal(t, "", typeName(m.KeyField()))
	require.Equal(t, "TIMESTAMP_NTZ", typeName(m.ItemField()))
	inMap := in.Field(1).Type.(*arrow.MapType)
	require.Equal(t, inMap.ItemField().Nullable, m.ItemField().Nullable)
	require.Equal(t, inMap.KeysSorted, m.KeysSorted)

	st := out.Field(2).Type.(*arrow.StructType)
	require.Equal(t, "", typeName(st.Field(0)))
	require.Equal(t, "TIMESTAMP_NTZ", typeName(st.Field(1)))
	require.Equal(t, "TIMESTAMP", typeName(st.Field(2)))
	require.Equal(t, "TIMESTAMP_NTZ", typeName(st.Field(3).Type.(*arrow.StructType).Field(0)))

	require.Equal(t, "TIMESTAMP_NTZ", typeName(out.Field(3).Type.(*arrow.MapType).KeyField()))

	elem := out.Field(4).Type.(*arrow.ListType).Elem().(*arrow.StructType)
	require.Equal(t, "TIMESTAMP_NTZ", typeName(elem.Field(0)))
	require.Equal(t, "TIMESTAMP", typeName(elem.Field(1)))
}

// TestWithNestedTimestampTypesScan renders nested NTZ leaves with their wall
// clock under a non-UTC location once labelled, for each container shape.
func TestWithNestedTimestampTypesScan(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("tz database unavailable: %v", err)
	}
	wall := arrow.Timestamp(time.Date(2024, time.June, 15, 9, 15, 0, 123456000, time.UTC).UnixMicro())
	tsUTC := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	const ntz = `"2024-06-15 09:15:00.123456 +0000 UTC"`
	const shifted = `"2024-06-15 05:15:00.123456 -0400 EDT"`
	jsonMeta := func(js string) arrow.Metadata {
		return arrow.NewMetadata([]string{sparkJSONTypeMetadataKey}, []string{js})
	}
	for _, tc := range []struct {
		name   string
		field  arrow.Field
		append func(b array.Builder)
		want   string
	}{
		{
			name:  "large list",
			field: arrow.Field{Name: "c", Type: arrow.LargeListOf(tsUTC), Metadata: jsonMeta(`{"type":"array","elementType":"timestamp_ntz","containsNull":true}`)},
			append: func(b array.Builder) {
				lb := b.(*array.LargeListBuilder)
				lb.Append(true)
				lb.ValueBuilder().(*array.TimestampBuilder).Append(wall)
			},
			want: `[` + ntz + `]`,
		},
		{
			name:  "fixed size list",
			field: arrow.Field{Name: "c", Type: arrow.FixedSizeListOf(1, tsUTC), Metadata: jsonMeta(`{"type":"array","elementType":"timestamp_ntz","containsNull":true}`)},
			append: func(b array.Builder) {
				lb := b.(*array.FixedSizeListBuilder)
				lb.Append(true)
				lb.ValueBuilder().(*array.TimestampBuilder).Append(wall)
			},
			want: `[` + ntz + `]`,
		},
		{
			name:  "map of array",
			field: arrow.Field{Name: "c", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.ListOf(tsUTC)), Metadata: jsonMeta(`{"type":"map","keyType":"string","valueType":{"type":"array","elementType":"timestamp_ntz","containsNull":true},"valueContainsNull":true}`)},
			append: func(b array.Builder) {
				mb := b.(*array.MapBuilder)
				mb.Append(true)
				mb.KeyBuilder().(*array.StringBuilder).Append("k")
				ib := mb.ItemBuilder().(*array.ListBuilder)
				ib.Append(true)
				ib.ValueBuilder().(*array.TimestampBuilder).Append(wall)
			},
			want: `{"k":[` + ntz + `]}`,
		},
		{
			name:  "no json type keeps shifting",
			field: arrow.Field{Name: "c", Type: arrow.ListOf(tsUTC)},
			append: func(b array.Builder) {
				lb := b.(*array.ListBuilder)
				lb.Append(true)
				lb.ValueBuilder().(*array.TimestampBuilder).Append(wall)
			},
			want: `[` + shifted + `]`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := WithNestedTimestampTypes(arrow.NewSchema([]arrow.Field{tc.field}, nil)).Field(0)
			b := array.NewBuilder(memory.NewGoAllocator(), f.Type)
			defer b.Release()
			tc.append(b)
			built := b.NewArray()
			defer built.Release()
			col := withType(built, f.Type)
			defer col.Release()
			got, err := ScanCellCached(col, 0, loc, NewStructKeyCache())
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

// TestWithNestedTimestampTypesUnchanged leaves a column alone when its JSON type
// is missing, malformed or does not match its Arrow shape, and never overrides
// a child's own logical-type metadata.
func TestWithNestedTimestampTypesUnchanged(t *testing.T) {
	tsUTC := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	jsonMeta := func(js string) arrow.Metadata {
		return arrow.NewMetadata([]string{sparkJSONTypeMetadataKey}, []string{js})
	}
	labelled := arrow.Field{Name: "e", Type: tsUTC, Nullable: true,
		Metadata: arrow.NewMetadata([]string{databricksTypeNameMetadataKey}, []string{"TIMESTAMP"})}
	for name, f := range map[string]arrow.Field{
		"scalar timestamp":   {Name: "c", Type: tsUTC, Metadata: jsonMeta(`"timestamp_ntz"`)},
		"no timestamps":      {Name: "c", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64), Metadata: jsonMeta(`{"type":"array","elementType":"long"}`)},
		"no json type":       {Name: "c", Type: arrow.ListOf(tsUTC)},
		"malformed json":     {Name: "c", Type: arrow.ListOf(tsUTC), Metadata: jsonMeta(`{"type":"array",`)},
		"missing element":    {Name: "c", Type: arrow.ListOf(tsUTC), Metadata: jsonMeta(`{"type":"array"}`)},
		"shape mismatch":     {Name: "c", Type: arrow.ListOf(tsUTC), Metadata: jsonMeta(`{"type":"struct","fields":[]}`)},
		"field count":        {Name: "c", Type: arrow.StructOf(arrow.Field{Name: "a", Type: tsUTC}), Metadata: jsonMeta(`{"type":"struct","fields":[]}`)},
		"non-timestamp leaf": {Name: "c", Type: arrow.ListOf(tsUTC), Metadata: jsonMeta(`{"type":"array","elementType":"string"}`)},
		"child metadata":     {Name: "c", Type: arrow.ListOfField(labelled), Metadata: jsonMeta(`{"type":"array","elementType":"timestamp_ntz"}`)},
		"bare array":         {Name: "c", Type: arrow.ListOf(tsUTC), Metadata: jsonMeta(`"array"`)},
		"bare map":           {Name: "c", Type: arrow.MapOf(arrow.BinaryTypes.String, tsUTC), Metadata: jsonMeta(`"map"`)},
		"bare nested array": {Name: "c", Type: arrow.StructOf(arrow.Field{Name: "a", Type: arrow.ListOf(tsUTC)}),
			Metadata: jsonMeta(`{"type":"struct","fields":[{"name":"a","type":"array"}]}`)},
		"map without value": {Name: "c", Type: arrow.MapOf(arrow.BinaryTypes.String, tsUTC), Metadata: jsonMeta(`{"type":"map","keyType":"string"}`)},
		"one subtree mismatched": {Name: "c",
			Type:     arrow.StructOf(arrow.Field{Name: "a", Type: tsUTC}, arrow.Field{Name: "b", Type: arrow.ListOf(tsUTC)}),
			Metadata: jsonMeta(`{"type":"struct","fields":[{"name":"a","type":"timestamp_ntz"},{"name":"b","type":"string"}]}`)},
	} {
		t.Run(name, func(t *testing.T) {
			in := arrow.NewSchema([]arrow.Field{f}, nil)
			require.Same(t, in, WithNestedTimestampTypes(in))
		})
	}
}

// TestWithNestedTimestampTypesBlankLabel replaces a blank existing type name
// rather than adding a second key that metadata lookups would never reach, and
// relabelling a labelled schema is a no-op.
func TestWithNestedTimestampTypesBlankLabel(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("tz database unavailable: %v", err)
	}
	tsUTC := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	for _, blank := range []string{"", "  "} {
		elem := arrow.Field{Name: "e", Type: tsUTC, Nullable: true,
			Metadata: arrow.NewMetadata([]string{"other", databricksTypeNameMetadataKey}, []string{"kept", blank})}
		in := arrow.NewSchema([]arrow.Field{{Name: "c", Type: arrow.ListOfField(elem),
			Metadata: arrow.NewMetadata([]string{sparkJSONTypeMetadataKey}, []string{`{"type":"array","elementType":"timestamp_ntz"}`})}}, nil)

		out := WithNestedTimestampTypes(in)
		md := out.Field(0).Type.(*arrow.ListType).ElemField().Metadata
		require.Equal(t, arrow.NewMetadata([]string{databricksTypeNameMetadataKey, "other"}, []string{"TIMESTAMP_NTZ", "kept"}), md)
		require.Same(t, out, WithNestedTimestampTypes(out))

		b := array.NewBuilder(memory.NewGoAllocator(), out.Field(0).Type).(*array.ListBuilder)
		b.Append(true)
		b.ValueBuilder().(*array.TimestampBuilder).Append(arrow.Timestamp(time.Date(2024, time.June, 15, 9, 15, 0, 123456000, time.UTC).UnixMicro()))
		col := b.NewArray()
		b.Release()
		got, err := ScanCellCached(col, 0, loc, NewStructKeyCache())
		col.Release()
		require.NoError(t, err)
		require.Equal(t, `["2024-06-15 09:15:00.123456 +0000 UTC"]`, got)
	}
}

// TestWithNestedTimestampTypesIgnoresTimestampFreeMismatch still labels a
// timestamp when a timestamp-free sibling (e.g. a binary geospatial struct)
// differs from its JSON type.
func TestWithNestedTimestampTypesIgnoresTimestampFreeMismatch(t *testing.T) {
	geo := arrow.StructOf(arrow.Field{Name: "srid", Type: arrow.PrimitiveTypes.Int32}, arrow.Field{Name: "wkb", Type: arrow.BinaryTypes.Binary})
	tsUTC := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	in := arrow.NewSchema([]arrow.Field{{Name: "c",
		Type: arrow.StructOf(arrow.Field{Name: "g", Type: geo}, arrow.Field{Name: "n", Type: tsUTC}),
		Metadata: arrow.NewMetadata([]string{sparkJSONTypeMetadataKey},
			[]string{`{"type":"struct","fields":[{"name":"g","type":"geometry(4326)"},{"name":"n","type":"timestamp_ntz"}]}`})}}, nil)

	st := WithNestedTimestampTypes(in).Field(0).Type.(*arrow.StructType)
	require.Equal(t, "", typeName(st.Field(0)))
	require.Equal(t, "TIMESTAMP_NTZ", typeName(st.Field(1)))
}
