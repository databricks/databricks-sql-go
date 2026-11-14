package arrowscan

import (
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
