package dbsql

import (
	"database/sql/driver"
	"strconv"
	"testing"
	"time"

	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/stretchr/testify/assert"
)

func TestParameter_Inference(t *testing.T) {
	t.Run("Should infer types correctly", func(t *testing.T) {
		values := [5]driver.NamedValue{{Name: "", Value: float32(5.1)}, {Name: "", Value: time.Now()}, {Name: "", Value: int64(5)}, {Name: "", Value: true}, {Name: "", Value: DBSqlParam{Value: "6.2", Type: Decimal}}}
		parameters := convertNamedValuesToSparkParams(values[:])
		assert.Equal(t, strconv.FormatFloat(float64(5.1), 'f', -1, 64), *parameters[0].Value.StringValue)
		assert.NotNil(t, parameters[1].Value.StringValue)
		assert.Equal(t, string("TIMESTAMP"), *parameters[1].Type)
		assert.Equal(t, &cli_service.TSparkParameterValue{StringValue: strPtr("5")}, parameters[2].Value)
		assert.Equal(t, string("true"), *parameters[3].Value.StringValue)
		assert.Equal(t, string("DECIMAL"), *parameters[4].Type)
		assert.Equal(t, string("6.2"), *parameters[4].Value.StringValue)
	})
}
func TestParameters_Names(t *testing.T) {
	t.Run("Should infer types correctly", func(t *testing.T) {
		values := [2]driver.NamedValue{{Name: "1", Value: int(26)}, {Name: "", Value: DBSqlParam{Name: "2", Type: Decimal, Value: "6.2"}}}
		parameters := convertNamedValuesToSparkParams(values[:])
		assert.Equal(t, string("1"), *parameters[0].Name)
		assert.Equal(t, cli_service.TSparkParameterValue{StringValue: strPtr("26")}, *parameters[0].Value)
		assert.Equal(t, string("2"), *parameters[1].Name)
		assert.Equal(t, cli_service.TSparkParameterValue{StringValue: strPtr("6.2")}, *parameters[1].Value)
		assert.Equal(t, string("DECIMAL"), *parameters[1].Type)
	})
}
