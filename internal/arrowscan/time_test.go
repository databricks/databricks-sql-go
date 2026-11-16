package arrowscan

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestScanTimeUnits(t *testing.T) {
	for _, test := range []struct {
		dataType arrow.DataType
		clock    string
		nanos    int
	}{
		{arrow.FixedWidthTypes.Time32s, "12:34:56", 0},
		{arrow.FixedWidthTypes.Time32ms, "12:34:56.789", 789000000},
		{arrow.FixedWidthTypes.Time64us, "12:34:56.789123", 789123000},
		{arrow.FixedWidthTypes.Time64ns, "12:34:56.789123456", 789123456},
	} {
		t.Run(test.dataType.String(), func(t *testing.T) {
			builder := array.NewBuilder(memory.DefaultAllocator, test.dataType)
			defer builder.Release()
			require.NoError(t, builder.AppendValueFromString(test.clock))
			builder.AppendNull()
			values := builder.NewArray()
			defer values.Release()
			got, err := ScanCell(values, 0, time.FixedZone("east", 9*3600))
			require.NoError(t, err)
			require.Equal(t, time.Date(1, time.January, 1, 12, 34, 56, test.nanos, time.UTC), got)
			got, err = ScanCell(values, 1, nil)
			require.NoError(t, err)
			require.Nil(t, got)
		})
	}
}

func TestTimeNullOnlyMetadata(t *testing.T) {
	for _, dataType := range []arrow.DataType{arrow.FixedWidthTypes.Time32s, arrow.FixedWidthTypes.Time32ms, arrow.FixedWidthTypes.Time64us, arrow.FixedWidthTypes.Time64ns} {
		t.Run(dataType.String(), func(t *testing.T) {
			builder := array.NewBuilder(memory.DefaultAllocator, dataType)
			defer builder.Release()
			builder.AppendNull()
			values := builder.NewArray()
			defer values.Release()
			got, err := ScanCell(values, 0, nil)
			require.NoError(t, err)
			require.Nil(t, got)
			metadata := ColumnTypeInfoFor(dataType)
			require.Equal(t, "TIME", metadata.DatabaseTypeName)
			require.Equal(t, reflect.TypeOf(time.Time{}), metadata.ScanType)
			require.Zero(t, metadata.Length)
			require.False(t, metadata.HasLength)
		})
	}
}

func TestTimeManifestFieldMetadata(t *testing.T) {
	type metadataCase struct {
		name       string
		kernelName string
		sparkName  string
		wantTime   bool
	}
	cases := []metadataCase{
		{"manifest", "TIME", "TIME(6)", true},
		{"kernel only", "TIME", "", true},
		{"case and whitespace", " time ", "", true},
		{"blank kernel name", " ", "TIME", true},
		{"ordinary string", "STRING", "STRING", false},
		{"no metadata", "", "", false},
		{"timestamp", "", "TIMESTAMP", false},
		{"similar name", "", "TIMEZONE", false},
		{"unsupported precision", "", "TIME(7)", false},
		{"kernel name takes precedence", "STRING", "TIME(6)", false},
	}
	for precision := range 7 {
		name := fmt.Sprintf("TIME(%d)", precision)
		cases = append(cases, metadataCase{name, "", name, true})
	}
	for _, dataType := range []arrow.DataType{arrow.BinaryTypes.String, arrow.BinaryTypes.LargeString} {
		for _, test := range cases {
			t.Run(dataType.String()+"/"+test.name, func(t *testing.T) {
				metadata := make(map[string]string)
				if test.kernelName != "" {
					metadata[databricksTypeNameMetadataKey] = test.kernelName
				}
				if test.sparkName != "" {
					metadata[sparkSQLNameMetadataKey] = test.sparkName
				}
				field := arrow.Field{Name: "clock", Type: dataType, Nullable: true, Metadata: arrow.MetadataFrom(metadata)}
				want := ColumnTypeInfoFor(dataType)
				if test.wantTime {
					want = ColumnTypeInfoFor(arrow.FixedWidthTypes.Time64ns)
				}
				got := ColumnTypeInfoForField(field)
				require.Equal(t, want.DatabaseTypeName, got.DatabaseTypeName)
				require.Equal(t, want.ScanType, got.ScanType)
				require.Equal(t, want.Length, got.Length)
				require.Equal(t, want.HasLength, got.HasLength)
			})
		}
	}
}
