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
			{Name: "", Value: Parameter{Value: float64Ptr(6.2), Type: SqlUnkown}},
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

func TestParameter_BigInt(t *testing.T) {
	t.Run("Should infer int64 as BIGINT", func(t *testing.T) {
		maxInt64 := int64(9223372036854775807)
		values := []driver.NamedValue{
			{Value: maxInt64},
		}
		parameters, err := convertNamedValuesToSparkParams(values)
		require.NoError(t, err)
		require.Equal(t, "BIGINT", *parameters[0].Type)
		require.Equal(t, "9223372036854775807", *parameters[0].Value.StringValue)
	})

	t.Run("Should infer uint64 as BIGINT", func(t *testing.T) {
		largeUint64 := uint64(0x123456789ABCDEF0)
		values := []driver.NamedValue{
			{Value: largeUint64},
		}
		parameters, err := convertNamedValuesToSparkParams(values)
		require.NoError(t, err)
		require.Equal(t, "BIGINT", *parameters[0].Type)
		require.Equal(t, "1311768467463790320", *parameters[0].Value.StringValue)
	})

	t.Run("Should infer negative int64 as BIGINT", func(t *testing.T) {
		minInt64 := int64(-9223372036854775808)
		values := []driver.NamedValue{
			{Value: minInt64},
		}
		parameters, err := convertNamedValuesToSparkParams(values)
		require.NoError(t, err)
		require.Equal(t, "BIGINT", *parameters[0].Type)
		require.Equal(t, "-9223372036854775808", *parameters[0].Value.StringValue)
	})

	t.Run("Should handle explicit BigInt Parameter with non-string value", func(t *testing.T) {
		values := []driver.NamedValue{
			{Value: Parameter{Type: SqlBigInt, Value: int64(12345)}},
		}
		parameters, err := convertNamedValuesToSparkParams(values)
		require.NoError(t, err)
		require.Equal(t, "BIGINT", *parameters[0].Type)
		require.Equal(t, "12345", *parameters[0].Value.StringValue)
	})

	t.Run("Should preserve int32 as INTEGER", func(t *testing.T) {
		values := []driver.NamedValue{
			{Value: int32(2147483647)},
		}
		parameters, err := convertNamedValuesToSparkParams(values)
		require.NoError(t, err)
		require.Equal(t, "INTEGER", *parameters[0].Type)
		require.Equal(t, "2147483647", *parameters[0].Value.StringValue)
	})
}

func TestParameter_Float(t *testing.T) {
	t.Run("Should infer float64 as DOUBLE", func(t *testing.T) {
		value := float64(3.141592653589793)
		values := []driver.NamedValue{
			{Value: value},
		}
		parameters, err := convertNamedValuesToSparkParams(values)
		require.NoError(t, err)
		require.Equal(t, "DOUBLE", *parameters[0].Type)
		require.Equal(t, "3.141592653589793", *parameters[0].Value.StringValue)
	})

	t.Run("Should infer float32 as FLOAT", func(t *testing.T) {
		value := float32(3.14)
		values := []driver.NamedValue{
			{Value: value},
		}
		parameters, err := convertNamedValuesToSparkParams(values)
		require.NoError(t, err)
		require.Equal(t, "FLOAT", *parameters[0].Type)
		require.Equal(t, "3.14", *parameters[0].Value.StringValue)
	})

	t.Run("Should handle large float64 values", func(t *testing.T) {
		// Value beyond float32 range
		value := float64(1e200)
		values := []driver.NamedValue{
			{Value: value},
		}
		parameters, err := convertNamedValuesToSparkParams(values)
		require.NoError(t, err)
		require.Equal(t, "DOUBLE", *parameters[0].Type)
	})

	t.Run("Should handle small float64 values", func(t *testing.T) {
		// Value below float32 precision
		value := float64(1e-300)
		values := []driver.NamedValue{
			{Value: value},
		}
		parameters, err := convertNamedValuesToSparkParams(values)
		require.NoError(t, err)
		require.Equal(t, "DOUBLE", *parameters[0].Type)
	})

	t.Run("Should handle explicit Double Parameter with non-string value", func(t *testing.T) {
		values := []driver.NamedValue{
			{Value: Parameter{Type: SqlDouble, Value: float64(3.14159)}},
		}
		parameters, err := convertNamedValuesToSparkParams(values)
		require.NoError(t, err)
		require.Equal(t, "DOUBLE", *parameters[0].Type)
		require.Equal(t, "3.14159", *parameters[0].Value.StringValue)
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
