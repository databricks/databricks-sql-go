package rows

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"testing"

	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/stretchr/testify/require"
)

func decimalMetadataColumn(typeID cli_service.TTypeId, precision, scale *int32) *cli_service.TColumnDesc {
	return &cli_service.TColumnDesc{TypeDesc: &cli_service.TTypeDesc{Types: []*cli_service.TTypeEntry{{
		PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{
			Type: typeID,
			TypeQualifiers: &cli_service.TTypeQualifiers{Qualifiers: map[string]*cli_service.TTypeQualifierValue{
				"precision": {I32Value: precision}, "scale": {I32Value: scale},
			}},
		},
	}}}}
}

func TestDecimalMetadataThriftResults(t *testing.T) {
	for _, rowCount := range []int{0, 1} {
		for _, direct := range []bool{false, true} {
			t.Run(fmt.Sprintf("rows=%d/direct=%t", rowCount, direct), func(t *testing.T) {
				pairs := [][2]int32{{10, 2}, {38, 0}, {38, 38}}
				schema := &cli_service.TTableSchema{}
				result := &cli_service.TRowSet{}
				for _, pair := range pairs {
					precision, scale := pair[0], pair[1]
					schema.Columns = append(schema.Columns, decimalMetadataColumn(cli_service.TTypeId_DECIMAL_TYPE, &precision, &scale))
					values := []string{}
					if rowCount > 0 {
						values = append(values, "1")
					}
					result.Columns = append(result.Columns, &cli_service.TColumn{StringVal: &cli_service.TStringColumn{Values: values, Nulls: []byte{0}}})
				}
				metadata := &cli_service.TGetResultSetMetadataResp{Schema: schema}
				hasMore := false
				fetch := &cli_service.TFetchResultsResp{Results: result, HasMoreRows: &hasMore}
				metadataCalls := 0
				testClient := &client.TestClient{
					FnCloseOperation: func(context.Context, *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
						return &cli_service.TCloseOperationResp{}, nil
					},
					FnGetResultSetMetadata: func(context.Context, *cli_service.TGetResultSetMetadataReq) (*cli_service.TGetResultSetMetadataResp, error) {
						metadataCalls++
						return metadata, nil
					},
					FnFetchResults: func(context.Context, *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
						return fetch, nil
					},
				}
				var directResults *cli_service.TSparkDirectResults
				if direct {
					directResults = &cli_service.TSparkDirectResults{ResultSetMetadata: metadata, ResultSet: fetch}
				}
				resultRows, err := NewRows(context.Background(), nil, testClient, config.WithDefaults(), directResults, nil)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, resultRows.Close()) })
				decimalRows, supported := resultRows.(driver.RowsColumnTypePrecisionScale)
				require.True(t, supported, "rows must expose declared DECIMAL qualifiers")
				check := func() {
					for index, pair := range pairs {
						precision, scale, ok := decimalRows.ColumnTypePrecisionScale(index)
						require.True(t, ok)
						require.Equal(t, int64(pair[0]), precision)
						require.Equal(t, int64(pair[1]), scale)
					}
				}
				check()
				values := make([]driver.Value, len(pairs))
				for range rowCount {
					require.NoError(t, resultRows.Next(values))
				}
				require.ErrorIs(t, resultRows.Next(values), io.EOF)
				check()
				if direct {
					require.Zero(t, metadataCalls)
				} else {
					require.Equal(t, 1, metadataCalls)
				}
			})
		}
	}
}

func TestDecimalMetadataThriftUnavailable(t *testing.T) {
	precision, scale := int32(10), int32(2)
	noQualifiers := decimalMetadataColumn(cli_service.TTypeId_DECIMAL_TYPE, nil, nil)
	noQualifiers.TypeDesc.Types[0].PrimitiveEntry.TypeQualifiers = nil
	emptyQualifiers := decimalMetadataColumn(cli_service.TTypeId_DECIMAL_TYPE, nil, nil)
	emptyQualifiers.TypeDesc.Types[0].PrimitiveEntry.TypeQualifiers.Qualifiers = nil
	for name, column := range map[string]*cli_service.TColumnDesc{
		"integer":           decimalMetadataColumn(cli_service.TTypeId_INT_TYPE, &precision, &scale),
		"string":            decimalMetadataColumn(cli_service.TTypeId_STRING_TYPE, &precision, &scale),
		"float":             decimalMetadataColumn(cli_service.TTypeId_DOUBLE_TYPE, &precision, &scale),
		"nested":            decimalMetadataColumn(cli_service.TTypeId_ARRAY_TYPE, &precision, &scale),
		"missing_precision": decimalMetadataColumn(cli_service.TTypeId_DECIMAL_TYPE, nil, &scale),
		"missing_scale":     decimalMetadataColumn(cli_service.TTypeId_DECIMAL_TYPE, &precision, nil),
		"missing_both":      decimalMetadataColumn(cli_service.TTypeId_DECIMAL_TYPE, nil, nil),
		"no_qualifiers":     noQualifiers,
		"empty_qualifiers":  emptyQualifiers,
	} {
		t.Run(name, func(t *testing.T) {
			resultRows := &rows{client: &client.TestClient{}, schema: &cli_service.TTableSchema{Columns: []*cli_service.TColumnDesc{column}}}
			decimalRows, supported := any(resultRows).(driver.RowsColumnTypePrecisionScale)
			require.True(t, supported)
			for _, index := range []int{-1, 0, 1} {
				precision, scale, ok := decimalRows.ColumnTypePrecisionScale(index)
				require.Equal(t, int64(0), precision)
				require.Equal(t, int64(0), scale)
				require.False(t, ok)
			}
		})
	}
}
