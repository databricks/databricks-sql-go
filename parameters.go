package dbsql

import (
	"database/sql/driver"
	"fmt"
	"strconv"
	"time"

	"github.com/databricks/databricks-sql-go/internal/cli_service"
)

type DbSqlParam struct {
	Name  string
	Type  SqlType
	Value any
}

type SqlType int64

const (
	Void SqlType = iota
	String
	Date
	Timestamp
	Float
	Decimal
	Double
	Integer
	Bigint
	Smallint
	Tinyint
	Boolean
	IntervalMonth
	IntervalDay
)

func (s SqlType) String() string {
	switch s {
	case Void:
		return "VOID"
	case String:
		return "STRING"
	case Date:
		return "DATE"
	case Timestamp:
		return "TIMESTAMP"
	case Float:
		return "FLOAT"
	case Decimal:
		return "DECIMAL"
	case Double:
		return "DOUBLE"
	case Integer:
		return "INTEGER"
	case Bigint:
		return "BIGINT"
	case Smallint:
		return "SMALLINT"
	case Tinyint:
		return "TINYINT"
	case Boolean:
		return "BOOLEAN"
	case IntervalMonth:
		return "INTERVAL MONTH"
	case IntervalDay:
		return "INTERVAL DAY"
	}
	return "unknown"
}

func valuesToDBSQLParams(namedValues []driver.NamedValue) []DbSqlParam {
	var params []DbSqlParam
	for i := range namedValues {
		namedValue := namedValues[i]
		param := *new(DbSqlParam)
		param.Name = namedValue.Name
		param.Value = namedValue.Value
		params = append(params, param)
	}
	return params
}

func inferTypes(params []DbSqlParam) {
	for i := range params {
		param := &params[i]
		switch value := param.Value.(type) {
		case nil:
			param.Type = Void
		case bool:
			param.Value = strconv.FormatBool(value)
			param.Type = Boolean
		case string:
			param.Value = &value
			param.Type = String
		case int:
			param.Value = strconv.Itoa(value)
			param.Type = Integer
		case uint:
			param.Value = strconv.FormatUint(uint64(value), 10)
			param.Type = Integer
		case int8:
			param.Value = strconv.Itoa(int(value))
			param.Type = Integer
		case uint8:
			param.Value = strconv.FormatUint(uint64(value), 10)
			param.Type = Integer
		case int16:
			param.Value = strconv.Itoa(int(value))
			param.Type = Integer
		case uint16:
			param.Value = strconv.FormatUint(uint64(value), 10)
			param.Type = Integer
		case int32:
			param.Value = strconv.Itoa(int(value))
			param.Type = Integer
		case uint32:
			param.Value = strconv.FormatUint(uint64(value), 10)
			param.Type = Integer
		case int64:
			param.Value = strconv.Itoa(int(value))
			param.Type = Integer
		case uint64:
			param.Value = strconv.FormatUint(uint64(value), 10)
			param.Type = Integer
		case float32:
			param.Value = strconv.FormatFloat(float64(value), 'f', -1, 32)
			param.Type = Float
		case time.Time:
			param.Value = value.String()
			param.Type = Timestamp
		case DbSqlParam:
			param.Value = value.Value
			param.Type = value.Type
		default:
			s := fmt.Sprintf("%s", value)
			param.Value = s
			param.Type = String
		}
	}
}
func convertNamedValuesToSparkParams(values []driver.NamedValue) []*cli_service.TSparkParameter {
	var sparkParams []*cli_service.TSparkParameter

	sqlParams := valuesToDBSQLParams(values)
	inferTypes(sqlParams)
	for i := range sqlParams {
		sqlParam := sqlParams[i]
		sparkParamValue := sqlParam.Value.(string)
		sparkParamType := sqlParam.Type.String()
		sparkParam := cli_service.TSparkParameter{Name: &sqlParam.Name, Type: &sparkParamType, Value: &cli_service.TSparkParameterValue{StringValue: &sparkParamValue}}
		sparkParams = append(sparkParams, &sparkParam)
	}
	return sparkParams
}
