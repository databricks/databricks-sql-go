package arrowscan

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestBoundCellParity(t *testing.T) {
	fixtures := []struct {
		dataType arrow.DataType
		values   []string
	}{
		{arrow.FixedWidthTypes.Boolean, []string{"true", "false"}},
		{arrow.PrimitiveTypes.Int8, []string{"-128", "127", "0"}},
		{arrow.PrimitiveTypes.Int16, []string{"-32768", "32767", "0"}},
		{arrow.PrimitiveTypes.Int32, []string{"-2147483648", "2147483647", "12345"}},
		{arrow.PrimitiveTypes.Int64, []string{"-9223372036854775808", "9223372036854775807", "12345"}},
		{arrow.PrimitiveTypes.Uint8, []string{"0", "255"}},
		{arrow.PrimitiveTypes.Uint16, []string{"0", "65535"}},
		{arrow.PrimitiveTypes.Uint32, []string{"0", "4294967295"}},
		{arrow.PrimitiveTypes.Uint64, []string{"0", "18446744073709551615"}},
		{arrow.PrimitiveTypes.Float32, []string{"0.1", "-0.0", `"NaN"`, `"+Inf"`, `"-Inf"`}},
		{arrow.PrimitiveTypes.Float64, []string{"0.1", "-0.0", `"NaN"`, `"+Inf"`, `"-Inf"`}},
		{arrow.BinaryTypes.String, []string{`""`, `"héllo\u0000世界"`, `"not a short string"`}},
		{arrow.BinaryTypes.LargeString, []string{`""`, `"héllo\u0000世界"`}},
		{arrow.BinaryTypes.Binary, []string{`""`, `"AAEC/w=="`}},
		{arrow.FixedWidthTypes.Date32, []string{`"0001-01-01"`, `"1970-01-01"`, `"9999-12-31"`}},
		{arrow.FixedWidthTypes.Date64, []string{`"0001-01-01"`, `"1970-01-01"`, `"9999-12-31"`}},
		{arrow.Null, []string{"null"}},
		{&arrow.Decimal128Type{Precision: 38, Scale: 2}, []string{`"-12345.67"`, `"999999999999999999999999999999999999.99"`}},
		{&arrow.Decimal128Type{Precision: 10, Scale: -2}, []string{`"12300"`, `"-45600"`}},
		{&arrow.Time32Type{Unit: arrow.Second}, []string{`"00:00:00"`, `"23:59:59"`}},
		{&arrow.Time64Type{Unit: arrow.Nanosecond}, []string{`"00:00:00"`, `"23:59:59.123456789"`}},
		{&arrow.DurationType{Unit: arrow.Microsecond}, []string{"-90061500000", "90061500000"}},
		{arrow.FixedWidthTypes.MonthInterval, []string{`{"months":-25}`, `{"months":25}`}},
		{arrow.FixedWidthTypes.DayTimeInterval, []string{`{"days":-1,"milliseconds":-1234}`, `{"days":2,"milliseconds":1234}`}},
		{arrow.ListOf(arrow.PrimitiveTypes.Int32), []string{"[1,null,3]", "[]"}},
		{arrow.LargeListOf(arrow.BinaryTypes.String), []string{`["x",null]`, "[]"}},
		{arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Int32), []string{"[1,null]", "[2,3]"}},
		{arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32), []string{`[{"key":"x","value":1},{"key":"y","value":null}]`, "[]"}},
		{arrow.StructOf(arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Float32, Nullable: true}), []string{`{"a":0.1}`, `{"a":null}`}},
		{&arrow.FixedSizeBinaryType{ByteWidth: 1}, []string{`"AA=="`, `"AQ=="`}},
	}
	for _, unit := range []arrow.TimeUnit{arrow.Second, arrow.Millisecond, arrow.Microsecond, arrow.Nanosecond} {
		for _, zone := range []string{"", "UTC", "America/Los_Angeles"} {
			fixtures = append(fixtures, struct {
				dataType arrow.DataType
				values   []string
			}{&arrow.TimestampType{Unit: unit, TimeZone: zone}, []string{"-123456789", "0", "123456789"}})
		}
	}
	for _, fixture := range fixtures {
		t.Run(fixture.dataType.String(), func(t *testing.T) {
			for _, nullMode := range []string{"none", "some", "all"} {
				t.Run(nullMode, func(t *testing.T) {
					values := make([]string, 40)
					for row := range values {
						values[row] = fixture.values[row%len(fixture.values)]
						if nullMode == "all" || nullMode == "some" && row%7 == 0 {
							values[row] = "null"
						}
					}
					column, _, err := array.FromJSON(memory.DefaultAllocator, fixture.dataType, strings.NewReader("["+strings.Join(values, ",")+"]"))
					if err != nil {
						t.Fatal(err)
					}
					defer column.Release()
					if nullMode == "none" && column.NullN() == 0 && len(column.Data().Buffers()) > 0 {
						data := column.Data()
						buffers := append([]*memory.Buffer(nil), data.Buffers()...)
						buffers[0] = nil
						withoutBitmap := array.NewData(data.DataType(), data.Len(), buffers, data.Children(), 0, data.Offset())
						noNulls := array.MakeFromData(withoutBitmap)
						withoutBitmap.Release()
						checkBoundParity(t, noNulls)
						noNulls.Release()
					}
					for _, offset := range []int64{0, 1, 7, 9, 40} {
						sliced := array.NewSlice(column, offset, 40)
						checkBoundParity(t, sliced)
						sliced.Release()
					}
				})
			}
		})
	}
}

func TestBoundGeospatialParity(t *testing.T) {
	dataType := arrow.StructOf(
		arrow.Field{Name: "srid", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		arrow.Field{Name: "wkb", Type: arrow.BinaryTypes.Binary, Nullable: true},
	)
	builder := array.NewStructBuilder(memory.DefaultAllocator, dataType)
	defer builder.Release()
	for _, srid := range []int32{4326, 3857} {
		builder.Append(true)
		builder.FieldBuilder(0).(*array.Int32Builder).Append(srid)
		builder.FieldBuilder(1).(*array.BinaryBuilder).Append([]byte{1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 0, 64})
	}
	builder.AppendNull()
	column := builder.NewArray()
	defer column.Release()
	checkBoundParity(t, column)
}

func checkBoundParity(t *testing.T, column arrow.Array) {
	t.Helper()
	for _, location := range []*time.Location{nil, time.UTC, time.FixedZone("session", -7*3600)} {
		for _, options := range []ScanOptions{{}, {DecimalAsFloat: true}, {GeospatialBinary: true}, {DecimalAsFloat: true, GeospatialBinary: true}} {
			keys := NewStructKeyCache()
			read := BindCellScanner(column, location, keys, options)
			for row := 0; row < column.Len(); row++ {
				want, wantErr := ScanCellCachedWithOptions(column, row, location, keys, options)
				got, gotErr := read(row)
				if fmt.Sprint(gotErr) != fmt.Sprint(wantErr) {
					t.Fatalf("row %d options %+v: error %v, want %v", row, options, gotErr, wantErr)
				}
				equal := reflect.DeepEqual(got, want)
				switch value := want.(type) {
				case float32:
					actual, ok := got.(float32)
					equal = ok && math.Float32bits(actual) == math.Float32bits(value)
				case float64:
					actual, ok := got.(float64)
					equal = ok && math.Float64bits(actual) == math.Float64bits(value)
				}
				if !equal {
					t.Fatalf("row %d options %+v: %T(%#v), want %T(%#v)", row, options, got, got, want, want)
				}
			}
		}
	}
}

func TestBoundStringOutlivesArray(t *testing.T) {
	const text = "a string retained after the Rust-owned Arrow batch is released"
	pool := overwriteOnFreeAllocator{memory.NewGoAllocator()}
	for _, nulls := range []bool{false, true} {
		builder := array.NewStringBuilder(pool)
		builder.Append(text)
		if nulls {
			builder.AppendNull()
		}
		column := builder.NewArray()
		builder.Release()
		read := BindCellScanner(column, nil, nil, ScanOptions{})
		got, err := read(0)
		column.Release()
		if err != nil || got != text {
			t.Fatalf("got %q, %v after releasing batch", got, err)
		}
	}
}

func TestBoundTimestampExtremes(t *testing.T) {
	for _, unit := range []arrow.TimeUnit{arrow.Second, arrow.Millisecond, arrow.Microsecond} {
		builder := array.NewTimestampBuilder(memory.DefaultAllocator, &arrow.TimestampType{Unit: unit, TimeZone: "UTC"})
		for _, instant := range []time.Time{
			time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC),
		} {
			value := instant.Unix()
			if unit == arrow.Millisecond {
				value = instant.UnixMilli()
			}
			if unit == arrow.Microsecond {
				value = instant.UnixMicro()
			}
			builder.Append(arrow.Timestamp(value))
		}
		column := builder.NewArray()
		builder.Release()
		checkBoundParity(t, column)
		column.Release()
	}
}

func TestBoundEmptyColumns(t *testing.T) {
	for _, dataType := range []arrow.DataType{
		arrow.FixedWidthTypes.Boolean, arrow.PrimitiveTypes.Int8,
		arrow.PrimitiveTypes.Int16, arrow.PrimitiveTypes.Int32,
		arrow.PrimitiveTypes.Int64, arrow.PrimitiveTypes.Float32,
		arrow.PrimitiveTypes.Float64, arrow.BinaryTypes.String, arrow.BinaryTypes.Binary,
		arrow.FixedWidthTypes.Date32, arrow.FixedWidthTypes.Date64,
		&arrow.TimestampType{Unit: arrow.Microsecond},
	} {
		builder := array.NewBuilder(memory.DefaultAllocator, dataType)
		column := builder.NewArray()
		builder.Release()
		checkBoundParity(t, column)
		column.Release()
	}
}
