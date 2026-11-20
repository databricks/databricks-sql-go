package arrowscan

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/require"
)

func TestGeospatialTypeNamePreservesSRID(t *testing.T) {
	for _, sqlName := range []string{"GEOMETRY(0)", "GEOGRAPHY(4326)", "GEOMETRY(3857)", "GEOMETRY(ANY)"} {
		for _, key := range []string{"databricks.type_text", sparkSQLNameMetadataKey} {
			for _, physical := range []arrow.DataType{arrow.BinaryTypes.String, arrow.BinaryTypes.LargeString,
				arrow.StructOf(arrow.Field{Name: "srid", Type: arrow.PrimitiveTypes.Int32}, arrow.Field{Name: "wkb", Type: arrow.BinaryTypes.Binary})} {
				t.Run(sqlName+"/"+key+"/"+physical.Name(), func(t *testing.T) {
					field := arrow.Field{Type: physical, Metadata: arrow.MetadataFrom(map[string]string{
						key: sqlName, databricksTypeNameMetadataKey: "GEOMETRY",
					})}
					info := ColumnTypeInfoForField(field)
					require.Equal(t, sqlName, info.DatabaseTypeName)
					before := ColumnTypeInfoFor(physical)
					require.Equal(t, before.ScanType, info.ScanType)
					require.Equal(t, before.Length, info.Length)
					require.Equal(t, before.HasLength, info.HasLength)
					require.Equal(t, physical.ID() == arrow.STRUCT, IsBinaryGeospatialField(field))
				})
			}
		}
	}
	child := arrow.Field{Type: arrow.BinaryTypes.String, Metadata: arrow.MetadataFrom(map[string]string{sparkSQLNameMetadataKey: "GEOMETRY(0)"})}
	for _, physical := range []arrow.DataType{arrow.StructOf(child), arrow.ListOfField(child)} {
		field := arrow.Field{Type: physical}
		require.Equal(t, ColumnTypeInfoFor(physical), ColumnTypeInfoForField(field))
		require.False(t, IsBinaryGeospatialField(field))
	}
}

func TestLogicalGeospatialTypeMetadata(t *testing.T) {
	for _, test := range []struct {
		name     string
		metadata map[string]string
		want     string
	}{
		{"case and whitespace", map[string]string{sparkSQLNameMetadataKey: " geometry(3857) "}, "GEOMETRY(3857)"},
		{"kernel family only", map[string]string{databricksTypeNameMetadataKey: "GEOGRAPHY"}, "GEOGRAPHY"},
		{"blank full name", map[string]string{"databricks.type_text": " ", sparkSQLNameMetadataKey: "GEOMETRY(0)"}, "GEOMETRY(0)"},
		{"manifest before Spark name", map[string]string{"databricks.type_text": "GEOMETRY(ANY)", sparkSQLNameMetadataKey: "GEOMETRY(0)"}, "GEOMETRY(ANY)"},
		{"no metadata", nil, ""},
		{"unrelated string", map[string]string{sparkSQLNameMetadataKey: "STRING"}, ""},
		{"nested geo", map[string]string{sparkSQLNameMetadataKey: "STRUCT<g:GEOMETRY(0)>"}, ""},
		{"prefix", map[string]string{sparkSQLNameMetadataKey: "GEOMETRY_OTHER(0)"}, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			name, ok := LogicalGeospatialType(arrow.Field{Type: arrow.BinaryTypes.String, Metadata: arrow.MetadataFrom(test.metadata)})
			require.Equal(t, test.want, name)
			require.Equal(t, test.want != "", ok)
		})
	}
}
