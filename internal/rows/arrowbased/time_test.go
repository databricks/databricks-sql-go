package arrowbased

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/databricks/databricks-sql-go/internal/arrowscan"
)

func TestTimeColumns(t *testing.T) {
	for _, test := range []struct {
		dataType arrow.DataType
		fraction string
		nanos    int
	}{
		{arrow.FixedWidthTypes.Time32s, "", 0},
		{arrow.FixedWidthTypes.Time32ms, ".123", 123000000},
		{arrow.FixedWidthTypes.Time64us, ".123456", 123456000},
		{arrow.FixedWidthTypes.Time64ns, ".123456", 123456000},
		{arrow.FixedWidthTypes.Time64ns, ".123456789", 123456789},
	} {
		t.Run(test.dataType.String()+test.fraction, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer pool.AssertSize(t, 0)
			builder := array.NewBuilder(pool, test.dataType)
			defer builder.Release()
			for _, clock := range []string{"09:15:00" + test.fraction, "00:00:00", "23:59:59" + test.fraction} {
				require.NoError(t, builder.AppendValueFromString(clock))
			}
			builder.AppendNull()
			values := builder.NewArray()
			defer values.Release()
			expected := []any{
				time.Date(1, time.January, 1, 9, 15, 0, test.nanos, time.UTC),
				time.Time{},
				time.Date(1, time.January, 1, 23, 59, 59, test.nanos, time.UTC),
				nil,
			}
			sliced := array.NewSlice(values, 1, 4)
			defer sliced.Release()
			for _, location := range []*time.Location{nil, time.UTC, time.FixedZone("west", -7*3600), time.FixedZone("east", 9*3600)} {
				holder, err := (&arrowValueContainerMaker{}).makeColumnValueContainer(test.dataType, location, nil, nil)
				require.NoError(t, err)
				containers := &rowValues{columnValueHolders: []columnValues{holder}}
				for _, batch := range []struct {
					values   arrow.Array
					expected []any
				}{{values, expected}, {sliced, expected[1:]}} {
					require.NoError(t, containers.SetColumnValues(0, batch.values.Data()))
					for row, want := range batch.expected {
						if want == nil {
							require.True(t, holder.IsNull(row))
						} else {
							got, err := holder.Value(row)
							require.NoError(t, err)
							require.Equal(t, want, got)
						}
						got, err := arrowscan.ScanCell(batch.values, row, location)
						require.NoError(t, err)
						require.Equal(t, want, got)
					}
				}
				containers.Close()
			}
			info := arrowscan.ColumnTypeInfoFor(test.dataType)
			require.Equal(t, "TIME", info.DatabaseTypeName)
			require.Equal(t, reflect.TypeOf(time.Time{}), info.ScanType)

			clock := "09:15:00" + test.fraction
			for _, nested := range []struct {
				dataType arrow.DataType
				input    string
				expected []string
			}{
				{arrow.ListOf(test.dataType), fmt.Sprintf(`[[%q,null],null]`, clock), []string{fmt.Sprintf(`[%q,null]`, clock), ""}},
				{arrow.StructOf(arrow.Field{Name: "clock", Type: test.dataType, Nullable: true}), fmt.Sprintf(`[{"clock":%q},{"clock":null}]`, clock), []string{fmt.Sprintf(`{"clock":%q}`, clock), `{"clock":null}`}},
				{arrow.MapOf(arrow.BinaryTypes.String, test.dataType), fmt.Sprintf(`[[{"key":"clock","value":%q},{"key":"missing","value":null}],null]`, clock), []string{fmt.Sprintf(`{"clock":%q,"missing":null}`, clock), ""}},
				{arrow.MapOf(test.dataType, arrow.BinaryTypes.String), fmt.Sprintf(`[[{"key":%q,"value":"clock"}],null]`, clock), []string{fmt.Sprintf(`{%q:"clock"}`, clock), ""}},
			} {
				t.Run(nested.dataType.String(), func(t *testing.T) {
					builder := array.NewBuilder(pool, nested.dataType)
					defer builder.Release()
					require.NoError(t, builder.UnmarshalJSON([]byte(nested.input)))
					values := builder.NewArray()
					defer values.Release()
					holder, err := (&arrowValueContainerMaker{}).makeColumnValueContainer(nested.dataType, time.Local, nil, nil)
					require.NoError(t, err)
					defer holder.Release()
					require.NoError(t, holder.SetValueArray(values.Data()))
					for row := range values.Len() {
						got, err := arrowscan.ScanCell(values, row, time.Local)
						require.NoError(t, err)
						if holder.IsNull(row) {
							require.Nil(t, got)
						} else {
							want, err := holder.Value(row)
							require.NoError(t, err)
							require.JSONEq(t, nested.expected[row], got.(string))
							require.Equal(t, want, got)
						}
					}
				})
			}
		})
	}
}

func TestNestedTimeFractionalZeros(t *testing.T) {
	for _, test := range []struct {
		dataType arrow.DataType
		clock    string
		expected string
	}{
		{arrow.FixedWidthTypes.Time32s, "10:30:00", "10:30:00"},
		{arrow.FixedWidthTypes.Time32ms, "09:15:30.500", "09:15:30.5"},
		{arrow.FixedWidthTypes.Time64us, "06:30:45.887000", "06:30:45.887"},
		{arrow.FixedWidthTypes.Time64ns, "09:15:30.500000000", "09:15:30.5"},
	} {
		t.Run(test.dataType.String(), func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer pool.AssertSize(t, 0)
			for _, nested := range []struct {
				dataType arrow.DataType
				input    string
				expected string
			}{
				{arrow.ListOf(test.dataType), fmt.Sprintf(`[[%q]]`, test.clock), fmt.Sprintf(`[%q]`, test.expected)},
				{arrow.StructOf(arrow.Field{Name: "clock", Type: test.dataType}), fmt.Sprintf(`[{"clock":%q}]`, test.clock), fmt.Sprintf(`{"clock":%q}`, test.expected)},
			} {
				builder := array.NewBuilder(pool, nested.dataType)
				defer builder.Release()
				require.NoError(t, builder.UnmarshalJSON([]byte(nested.input)))
				values := builder.NewArray()
				defer values.Release()
				holder, err := (&arrowValueContainerMaker{}).makeColumnValueContainer(nested.dataType, time.UTC, nil, nil)
				require.NoError(t, err)
				defer holder.Release()
				require.NoError(t, holder.SetValueArray(values.Data()))
				thrift, err := holder.Value(0)
				require.NoError(t, err)
				require.Equal(t, nested.expected, thrift)
				kernel, err := arrowscan.ScanCell(values, 0, nil)
				require.NoError(t, err)
				require.Equal(t, nested.expected, kernel)
			}
		})
	}
}
