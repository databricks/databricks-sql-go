package kernel

import (
	"database/sql/driver"
	"encoding/binary"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/decimal128"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/databricks/databricks-sql-go/internal/arrowscan"
	"github.com/stretchr/testify/require"
)

type poisoningAllocator struct {
	memory.Allocator
	frees int
}

func (a *poisoningAllocator) Free(b []byte) {
	for i := range b {
		b[i] = 0xAA
	}
	a.frees++
	a.Allocator.Free(b)
}

func TestPreparedBatchRetainedStrings(t *testing.T) {
	for _, dt := range []arrow.DataType{arrow.BinaryTypes.String, arrow.BinaryTypes.LargeString} {
		for _, tc := range []struct {
			name       string
			start, end int64
			allNull    bool
		}{
			{name: "full", end: 7},
			{name: "sliced", start: 1, end: 6},
			{name: "empty_slice", start: 2, end: 2},
			{name: "all_null", end: 7, allNull: true},
		} {
			t.Run(dt.Name()+"/"+tc.name, func(t *testing.T) {
				poison := &poisoningAllocator{Allocator: memory.NewGoAllocator()}
				mem := memory.NewCheckedAllocator(poison)
				defer mem.AssertSize(t, 0)
				var retained, want []driver.Value
				for batch := 0; batch < 2; batch++ {
					b := array.NewBuilder(mem, dt)
					values := []string{"excluded prefix", "alpha", "", "ignored", "日本語", strings.Repeat("x", 4096), "excluded suffix"}
					valid := []bool{true, true, true, false, true, true, true}
					if tc.allNull {
						valid = make([]bool, len(values))
					}
					switch b := b.(type) {
					case *array.StringBuilder:
						b.AppendValues(values, valid)
					case *array.LargeStringBuilder:
						b.AppendValues(values, valid)
					}
					full := b.NewArray()
					b.Release()
					col := array.NewSlice(full, tc.start, tc.end)
					full.Release()
					schema := arrow.NewSchema([]arrow.Field{{Name: "s", Type: dt}}, nil)
					rec := array.NewRecord(schema, []arrow.Array{col}, int64(col.Len()))
					col.Release()
					columns := prepareBatch(rec, time.UTC, []bool{false}, []bool{false}, false, arrowscan.NewStructKeyCache())
					var borrowed string
					for row := 0; row < int(rec.NumRows()); row++ {
						v, err := columns[0](row)
						require.NoError(t, err)
						retained = append(retained, v)
						i := int(tc.start) + row
						if valid[i] {
							want = append(want, values[i])
							if values[i] != "" {
								raw, err := arrowscan.ScanCell(rec.Column(0), row, time.UTC)
								require.NoError(t, err)
								borrowed = raw.(string)
							}
						} else {
							want = append(want, nil)
						}
					}
					rec.Release()
					if borrowed != "" {
						require.Equal(t, byte(0xAA), borrowed[0], "release must overwrite the original string storage")
					}
					runtime.GC()
					require.Equal(t, want, retained)
				}
				require.Positive(t, poison.frees)
			})
		}
	}
}

func TestPreparedBatchRenderedValuesOwnMemory(t *testing.T) {
	poison := &poisoningAllocator{Allocator: memory.NewGoAllocator()}
	mem := memory.NewCheckedAllocator(poison)
	defer mem.AssertSize(t, 0)
	geoType := arrow.StructOf(arrow.Field{Name: "srid", Type: arrow.PrimitiveTypes.Int32}, arrow.Field{Name: "wkb", Type: arrow.BinaryTypes.Binary})
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "list", Type: arrow.ListOf(arrow.BinaryTypes.String)},
		{Name: "map", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String)},
		{Name: "struct", Type: arrow.StructOf(arrow.Field{Name: "s", Type: arrow.BinaryTypes.String})},
		{Name: "decimal", Type: &arrow.Decimal128Type{Precision: 7, Scale: 2}},
		{Name: "duration", Type: &arrow.DurationType{Unit: arrow.Microsecond}},
		{Name: "months", Type: arrow.FixedWidthTypes.MonthInterval},
		{Name: "geo", Type: geoType},
	}, nil)
	b := array.NewRecordBuilder(mem, schema)
	l := b.Field(0).(*array.ListBuilder)
	l.Append(true)
	l.ValueBuilder().(*array.StringBuilder).Append("list value")
	m := b.Field(1).(*array.MapBuilder)
	m.Append(true)
	m.KeyBuilder().(*array.StringBuilder).Append("map key")
	m.ItemBuilder().(*array.StringBuilder).Append("map value")
	s := b.Field(2).(*array.StructBuilder)
	s.Append(true)
	s.FieldBuilder(0).(*array.StringBuilder).Append("struct value")
	b.Field(3).(*array.Decimal128Builder).Append(decimal128.FromI64(1234))
	b.Field(4).(*array.DurationBuilder).Append(arrow.Duration(86400000000))
	b.Field(5).(*array.MonthIntervalBuilder).Append(14)
	g := b.Field(6).(*array.StructBuilder)
	g.Append(true)
	g.FieldBuilder(0).(*array.Int32Builder).Append(4326)
	wkb := make([]byte, 21)
	wkb[0] = 1
	binary.LittleEndian.PutUint32(wkb[1:], 1)
	g.FieldBuilder(1).(*array.BinaryBuilder).Append(wkb)
	rec := b.NewRecord()
	b.Release()
	geo := make([]bool, rec.NumCols())
	geo[6] = true
	columns := prepareBatch(rec, time.UTC, make([]bool, rec.NumCols()), geo, false, arrowscan.NewStructKeyCache())
	got := make([]driver.Value, len(columns))
	for i, scan := range columns {
		v, err := scan(0)
		require.NoError(t, err)
		got[i] = v
	}
	rec.Release()
	runtime.GC()
	wantEWKB := make([]byte, 25)
	wantEWKB[0] = 1
	binary.LittleEndian.PutUint32(wantEWKB[1:], 0x20000001)
	binary.LittleEndian.PutUint32(wantEWKB[5:], 4326)
	require.Equal(t, []driver.Value{`["list value"]`, `{"map key":"map value"}`, `{"s":"struct value"}`, "12.34", "1 00:00:00.000000000", "1-2", wantEWKB}, got)
}

func TestPreparedBatchScanOptions(t *testing.T) {
	loc := time.FixedZone("session", -7*3600)
	tsType := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "zoned", Type: tsType},
		{Name: "ntz", Type: tsType},
		{Name: "decimal", Type: &arrow.Decimal128Type{Precision: 7, Scale: 2}},
		{Name: "invalid_geo", Type: arrow.BinaryTypes.String},
		{Name: "unsupported", Type: arrow.FixedWidthTypes.Time32s},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	for i := 0; i < 2; i++ {
		b.Field(i).(*array.TimestampBuilder).Append(123456789)
	}
	b.Field(2).(*array.Decimal128Builder).Append(decimal128.FromI64(1234))
	b.Field(3).(*array.StringBuilder).Append("invalid")
	b.Field(4).(*array.Time32Builder).Append(42)
	for _, f := range b.Fields() {
		f.AppendNull()
	}
	rec := b.NewRecord()
	defer rec.Release()
	ntz := []bool{false, true, false, false, false}
	geo := []bool{false, false, false, true, false}
	for _, decimalAsFloat := range []bool{false, true} {
		columns := prepareBatch(rec, loc, ntz, geo, decimalAsFloat, arrowscan.NewStructKeyCache())
		for col, scan := range columns {
			columnLoc := loc
			if ntz[col] {
				columnLoc = time.UTC
			}
			for row := 0; row < int(rec.NumRows()); row++ {
				want, wantErr := arrowscan.ScanCellCachedWithOptions(rec.Column(col), row, columnLoc, nil, arrowscan.ScanOptions{
					DecimalAsFloat: decimalAsFloat, GeospatialBinary: geo[col],
				})
				got, err := scan(row)
				require.Equal(t, want, got)
				if wantErr != nil {
					require.EqualError(t, err, wantErr.Error())
				} else {
					require.NoError(t, err)
				}
			}
		}
	}
}
