package rows

import (
	"bytes"
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	dbsqlrows "github.com/databricks/databricks-sql-go/rows"
	"github.com/stretchr/testify/require"
)

func TestArrowColumnMetadataWithServerSchema(t *testing.T) {
	for _, direct := range []bool{false, true} {
		for _, nativeDecimal := range []bool{false, true} {
			for _, rowCount := range []int64{0, 1} {
				for _, accessor := range []string{"next", "ipc", "batches"} {
					t.Run(fmt.Sprintf("direct=%t/nativeDecimal=%t/rows=%d/%s", direct, nativeDecimal, rowCount, accessor), func(t *testing.T) {
						cfg := config.WithDefaults()
						cfg.UseArrowNativeDecimal = nativeDecimal
						var decimalType arrow.DataType = arrow.BinaryTypes.String
						if nativeDecimal {
							decimalType = &arrow.Decimal128Type{Precision: 10, Scale: 2}
						}
						arrowSchema := arrow.NewSchema([]arrow.Field{
							{Name: "variant", Type: arrow.BinaryTypes.String, Nullable: true, Metadata: arrow.MetadataFrom(map[string]string{"Spark:DataType:SqlName": "VARIANT"})},
							{Name: "clock", Type: arrow.FixedWidthTypes.Time64ns, Nullable: true},
							{Name: "amount", Type: decimalType, Nullable: true},
						}, nil)
						precision, scale := int32(10), int32(2)
						thriftSchema := &cli_service.TTableSchema{Columns: []*cli_service.TColumnDesc{
							decimalMetadataColumn(cli_service.TTypeId_STRING_TYPE, nil, nil),
							decimalMetadataColumn(cli_service.TTypeId_STRING_TYPE, nil, nil),
							decimalMetadataColumn(cli_service.TTypeId_DECIMAL_TYPE, &precision, &scale),
						}}
						for index, column := range thriftSchema.Columns {
							column.ColumnName = arrowSchema.Field(index).Name
						}
						var serializedSchema, serializedRecord bytes.Buffer
						writer := ipc.NewWriter(&serializedSchema, ipc.WithSchema(arrowSchema))
						require.NoError(t, writer.Close())
						schemaBytes := serializedSchema.Bytes()[:serializedSchema.Len()-8]
						builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
						defer builder.Release()
						if rowCount > 0 {
							for _, field := range builder.Fields() {
								field.AppendEmptyValue()
							}
						}
						record := builder.NewRecordBatch()
						defer record.Release()
						writer = ipc.NewWriter(&serializedRecord, ipc.WithSchema(arrowSchema))
						require.NoError(t, writer.Write(record))
						require.NoError(t, writer.Close())
						format := cli_service.TSparkRowSetType_ARROW_BASED_SET
						metadata := &cli_service.TGetResultSetMetadataResp{Schema: thriftSchema, ArrowSchema: schemaBytes, ResultFormat: &format}
						page := &cli_service.TFetchResultsResp{
							ResultSetMetadata: metadata, HasMoreRows: new(bool),
							Results: &cli_service.TRowSet{ArrowBatches: []*cli_service.TSparkArrowBatch{{Batch: serializedRecord.Bytes()[len(schemaBytes):], RowCount: rowCount}}},
						}
						fetchCalls := 0
						testClient := &client.TestClient{
							FnGetResultSetMetadata: func(context.Context, *cli_service.TGetResultSetMetadataReq) (*cli_service.TGetResultSetMetadataResp, error) {
								initial := *metadata
								initial.ArrowSchema = nil
								return &initial, nil
							},
							FnFetchResults: func(context.Context, *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
								fetchCalls++
								return page, nil
							},
							FnCloseOperation: func(context.Context, *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
								return &cli_service.TCloseOperationResp{}, nil
							},
						}
						var initial *cli_service.TSparkDirectResults
						if direct {
							initial = &cli_service.TSparkDirectResults{ResultSetMetadata: metadata, ResultSet: page}
						}
						result, err := NewRows(context.Background(), nil, testClient, cfg, initial, nil)
						require.NoError(t, err)
						defer func() { require.NoError(t, result.Close()) }()
						columnRows := result.(*rows)
						if !direct {
							require.Equal(t, "STRING", columnRows.ColumnTypeDatabaseTypeName(0))
							require.Equal(t, "STRING", columnRows.ColumnTypeDatabaseTypeName(1))
							require.Equal(t, scanTypeString, columnRows.ColumnTypeScanType(1))
							length, ok := columnRows.ColumnTypeLength(1)
							require.EqualValues(t, math.MaxInt64, length)
							require.True(t, ok)
						}
						if accessor == "next" {
							values := make([]driver.Value, 3)
							err := result.Next(values)
							if rowCount == 0 {
								require.ErrorIs(t, err, io.EOF)
							} else {
								require.NoError(t, err)
								require.ErrorIs(t, result.Next(values), io.EOF)
							}
						} else {
							_, count := readReviewArrowResults(t, result.(dbsqlrows.Rows), accessor, func(schema *arrow.Schema) {
								require.True(t, arrowSchema.Equal(schema), "server schema must win over STRING fallbacks")
							})
							require.Equal(t, rowCount, count)
						}
						require.Equal(t, "VARIANT", columnRows.ColumnTypeDatabaseTypeName(0))
						require.Equal(t, scanTypeString, columnRows.ColumnTypeScanType(0))
						length, ok := columnRows.ColumnTypeLength(0)
						require.EqualValues(t, math.MaxInt64, length)
						require.True(t, ok)
						require.Equal(t, "TIME", columnRows.ColumnTypeDatabaseTypeName(1))
						require.Equal(t, scanTypeDateTime, columnRows.ColumnTypeScanType(1))
						length, ok = columnRows.ColumnTypeLength(1)
						require.Zero(t, length)
						require.False(t, ok)
						require.Equal(t, "DECIMAL", columnRows.ColumnTypeDatabaseTypeName(2))
						gotPrecision, gotScale, ok := columnRows.ColumnTypePrecisionScale(2)
						require.EqualValues(t, precision, gotPrecision)
						require.EqualValues(t, scale, gotScale)
						require.True(t, ok)
						if direct {
							require.Zero(t, fetchCalls)
						} else {
							require.Equal(t, 1, fetchCalls)
						}
					})
				}
			}
		}
	}
}
