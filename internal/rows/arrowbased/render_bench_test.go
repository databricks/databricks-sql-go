package arrowbased

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/databricks/databricks-sql-go/v2/internal/arrowscan"
)

// BenchmarkRenderNested renders nested columns to their JSON strings through
// both backends: the Thrift containers in this package and the kernel's
// arrowscan. Each op renders every row of the column.
func BenchmarkRenderNested(b *testing.B) {
	const rows = 1000
	pool := memory.NewGoAllocator()
	ascii := []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta"}
	intl := []string{"café crème", "naïve façade", "Größe", "日本語のテキスト", "Привет мир", "señal ñandú", "東京都", "Ελληνικά"}

	shapes := []struct {
		name  string
		build func() arrow.Array
	}{
		{"map_ascii", func() arrow.Array {
			mb := array.NewMapBuilder(pool, arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int64, false)
			kb, ib := mb.KeyBuilder().(*array.StringBuilder), mb.ItemBuilder().(*array.Int64Builder)
			for i := 0; i < rows; i++ {
				mb.Append(true)
				for j := 0; j < 3; j++ {
					kb.Append(ascii[(i+j)%len(ascii)] + strconv.Itoa(j))
					ib.Append(int64(i * j))
				}
			}
			return mb.NewArray()
		}},
		{"map_utf8", func() arrow.Array {
			mb := array.NewMapBuilder(pool, arrow.BinaryTypes.String, arrow.BinaryTypes.String, false)
			kb, ib := mb.KeyBuilder().(*array.StringBuilder), mb.ItemBuilder().(*array.StringBuilder)
			for i := 0; i < rows; i++ {
				mb.Append(true)
				for j := 0; j < 3; j++ {
					kb.Append(ascii[(i+j)%len(ascii)] + strconv.Itoa(j))
					ib.Append(intl[(i+j)%len(intl)])
				}
			}
			return mb.NewArray()
		}},
		{"array_double", func() arrow.Array {
			lb := array.NewListBuilder(pool, arrow.PrimitiveTypes.Float64)
			vb := lb.ValueBuilder().(*array.Float64Builder)
			for i := 0; i < rows; i++ {
				lb.Append(true)
				for j := 0; j < 5; j++ {
					vb.Append(float64(i*5+j) / 7)
				}
			}
			return lb.NewArray()
		}},
		{"array_string", func() arrow.Array {
			lb := array.NewListBuilder(pool, arrow.BinaryTypes.String)
			vb := lb.ValueBuilder().(*array.StringBuilder)
			for i := 0; i < rows; i++ {
				lb.Append(true)
				for j := 0; j < 5; j++ {
					vb.Append(ascii[(i+j)%len(ascii)] + strconv.Itoa(i))
				}
			}
			return lb.NewArray()
		}},
		{"array_binary", func() arrow.Array {
			lb := array.NewListBuilder(pool, arrow.BinaryTypes.Binary)
			vb := lb.ValueBuilder().(*array.BinaryBuilder)
			for i := 0; i < rows; i++ {
				lb.Append(true)
				for j := 0; j < 5; j++ {
					vb.Append([]byte(ascii[(i+j)%len(ascii)]))
				}
			}
			return lb.NewArray()
		}},
		{"struct", func() arrow.Array {
			sb := array.NewStructBuilder(pool, arrow.StructOf(
				arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
				arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: "score", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
			))
			for i := 0; i < rows; i++ {
				sb.Append(true)
				sb.FieldBuilder(0).(*array.Int64Builder).Append(int64(i))
				sb.FieldBuilder(1).(*array.StringBuilder).Append(ascii[i%len(ascii)])
				sb.FieldBuilder(2).(*array.Float64Builder).Append(float64(i) / 3)
			}
			return sb.NewArray()
		}},
	}

	for _, s := range shapes {
		arr := s.build()
		b.Run(s.name+"/thrift", func(b *testing.B) {
			holder, err := (&arrowValueContainerMaker{}).makeColumnValueContainer(arr.DataType(), time.UTC, func(ts arrow.Timestamp) time.Time {
				return ts.ToTime(arrow.Microsecond)
			}, nil)
			if err != nil {
				b.Fatal(err)
			}
			if err := holder.SetValueArray(arr.Data()); err != nil {
				b.Fatal(err)
			}
			defer holder.Release()
			b.ReportAllocs()
			for n := 0; n < b.N; n++ {
				for row := 0; row < rows; row++ {
					if _, err := holder.Value(row); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
		b.Run(s.name+"/kernel", func(b *testing.B) {
			keys := arrowscan.NewStructKeyCache()
			b.ReportAllocs()
			for n := 0; n < b.N; n++ {
				for row := 0; row < rows; row++ {
					if _, err := arrowscan.ScanCellCached(arr, row, time.UTC, keys); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
		arr.Release()
	}
}

func BenchmarkRenderWideArray(b *testing.B) {
	benchmarkRenderWide(b, "array")
}

func BenchmarkRenderWideMap(b *testing.B) {
	benchmarkRenderWide(b, "map")
}

func BenchmarkRenderWideStruct(b *testing.B) {
	benchmarkRenderWide(b, "struct")
}

func BenchmarkRenderWideNested(b *testing.B) {
	benchmarkRenderWide(b, "struct_array")
}

func benchmarkRenderWide(b *testing.B, shape string) {
	for _, width := range []int{0, 1, 2, 5, 50, 500, 1536, 5000} {
		values := buildWideRenderValues(b, shape, width)
		b.Run("thrift/"+strconv.Itoa(width), func(b *testing.B) {
			holder, err := (&arrowValueContainerMaker{}).makeColumnValueContainer(values.DataType(), time.UTC, nil, nil)
			if err != nil {
				b.Fatal(err)
			}
			defer holder.Release()
			if err := holder.SetValueArray(values.Data()); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				if _, err := holder.Value(0); err != nil {
					b.Fatal(err)
				}
			}
		})
		values.Release()
	}
}

// BenchmarkRenderSmallAfterLarge times only a 16-byte ARRAY<STRING> value that
// follows a large one in the same column; run it with a fixed -benchtime=Nx.
func BenchmarkRenderSmallAfterLarge(b *testing.B) {
	for _, size := range []int{100 << 10, 1 << 20, 4 << 20} {
		builder := array.NewListBuilder(memory.NewGoAllocator(), arrow.BinaryTypes.String)
		elements := builder.ValueBuilder().(*array.StringBuilder)
		builder.Append(true)
		elements.Append(strings.Repeat("a", size))
		builder.Append(true)
		elements.Append("0123456789ab")
		values := builder.NewArray()
		builder.Release()
		b.Run("thrift/"+strconv.Itoa(size>>10)+"KiB", func(b *testing.B) {
			holder, err := (&arrowValueContainerMaker{}).makeColumnValueContainer(values.DataType(), time.UTC, nil, nil)
			if err != nil {
				b.Fatal(err)
			}
			defer holder.Release()
			if err := holder.SetValueArray(values.Data()); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				b.StopTimer()
				if _, err := holder.Value(0); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				if _, err := holder.Value(1); err != nil {
					b.Fatal(err)
				}
			}
		})
		values.Release()
	}
}
