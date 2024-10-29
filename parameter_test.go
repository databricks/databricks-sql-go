package dbsql

import (
	"database/sql/driver"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"
	"time"

	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/stretchr/testify/assert"
)

func TestParameter_Inference(t *testing.T) {
	t.Run("Should infer types correctly", func(t *testing.T) {
		values := [7]driver.NamedValue{
			{Name: "", Value: float32(5.1)},
			{Name: "", Value: time.Now()},
			{Name: "", Value: int64(5)},
			{Name: "", Value: true},
			{Name: "", Value: Parameter{Value: "6.2", Type: SqlDecimal}},
			{Name: "", Value: nil},
			{Name: "", Value: Parameter{Value: float64Ptr(6.2), Type: SqlUnknown}},
		}
		parameters, _ := convertNamedValuesToSparkParams(values[:])
		assert.Equal(t, strconv.FormatFloat(float64(5.1), 'f', -1, 64), *parameters[0].Value.StringValue)
		assert.NotNil(t, parameters[1].Value.StringValue)
		assert.Equal(t, string("TIMESTAMP"), *parameters[1].Type)
		assert.Equal(t, &cli_service.TSparkParameterValue{StringValue: strPtr("5")}, parameters[2].Value)
		assert.Equal(t, string("true"), *parameters[3].Value.StringValue)
		assert.Equal(t, string("DECIMAL(2,1)"), *parameters[4].Type)
		assert.Equal(t, string("6.2"), *parameters[4].Value.StringValue)
		assert.Equal(t, string("VOID"), *parameters[5].Type)
		assert.Nil(t, parameters[5].Value)
		assert.Equal(t, &cli_service.TSparkParameterValue{StringValue: strPtr("6.2")}, parameters[6].Value)
	})
}

func TestParameters_ConvertToSpark(t *testing.T) {
	t.Run("Should convert names parameters", func(t *testing.T) {
		values := [2]driver.NamedValue{
			{Name: "1", Value: int(26)},
			{Name: "", Value: Parameter{Name: "2", Type: SqlDecimal, Value: "6.2"}},
		}
		parameters, err := convertNamedValuesToSparkParams(values[:])
		require.NoError(t, err)
		require.Equal(t, string("1"), *parameters[0].Name)
		require.Equal(t, cli_service.TSparkParameterValue{StringValue: strPtr("26")}, *parameters[0].Value)
		require.Equal(t, string("2"), *parameters[1].Name)
		require.Equal(t, cli_service.TSparkParameterValue{StringValue: strPtr("6.2")}, *parameters[1].Value)
		require.Equal(t, string("DECIMAL(2,1)"), *parameters[1].Type)
	})

	t.Run("Should convert positional parameters", func(t *testing.T) {
		values := [2]driver.NamedValue{
			{Value: int(26)},
			{Name: "", Value: Parameter{Type: SqlDecimal, Value: "6.2"}},
		}
		parameters, err := convertNamedValuesToSparkParams(values[:])
		require.NoError(t, err)
		require.Nil(t, parameters[0].Name)
		require.Equal(t, cli_service.TSparkParameterValue{StringValue: strPtr("26")}, *parameters[0].Value)
		require.Nil(t, parameters[1].Name)
		require.Equal(t, cli_service.TSparkParameterValue{StringValue: strPtr("6.2")}, *parameters[1].Value)
		require.Equal(t, string("DECIMAL(2,1)"), *parameters[1].Type)
	})

	t.Run("Should error out when named and positional parameters are mixed", func(t *testing.T) {
		values := [4]driver.NamedValue{
			{Name: "a", Value: int(26)},
			{Name: "", Value: Parameter{Type: SqlDecimal, Value: "6.2"}},
			{Value: "test"},
			{Name: "b", Value: Parameter{Type: SqlDouble, Value: 123.456}},
		}
		_, err := convertNamedValuesToSparkParams(values[:])
		require.Error(t, err)
		require.Equal(t, err.Error(), dbsqlerr.ErrMixedNamedAndPositionalParameters)
	})
}
