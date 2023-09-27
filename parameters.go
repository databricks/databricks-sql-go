package dbsql

import (
	"database/sql/driver"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/databricks/databricks-sql-go/internal/cli_service"
)

type DBSqlParam struct {
	Name  string
	Type  SqlType
	Value any
}

type SqlType int64

const (
	String SqlType = iota
	Date
	Timestamp
	Float
	Decimal
	Double
	Integer
	BigInt
	SmallInt
	TinyInt
	Boolean
	IntervalMonth
	IntervalDay
)

func (s SqlType) String() string {
	switch s {
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
	case BigInt:
		return "BIGINT"
	case SmallInt:
		return "SMALLINT"
	case TinyInt:
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

func valuesToDBSQLParams(namedValues []driver.NamedValue) []DBSqlParam {
	var params []DBSqlParam
	for i := range namedValues {
		namedValue := namedValues[i]
		param := *new(DBSqlParam)
		param.Name = namedValue.Name
		param.Value = namedValue.Value
		params = append(params, param)
	}
	return params
}

func inferTypes(params []DBSqlParam) {
	for i := range params {
		param := &params[i]
		switch value := param.Value.(type) {
		case bool:
			param.Value = strconv.FormatBool(value)
			param.Type = Boolean
		case string:
			param.Value = value
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
		case DBSqlParam:
			param.Name = value.Name
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
		var sparkParamType string
		if sqlParam.Type == Decimal {
			sparkParamType = inferDecimalType(sparkParamValue)
		} else {
			sparkParamType = sqlParam.Type.String()
		}
		sparkParam := cli_service.TSparkParameter{Name: &sqlParam.Name, Type: &sparkParamType, Value: &cli_service.TSparkParameterValue{StringValue: &sparkParamValue}}
		sparkParams = append(sparkParams, &sparkParam)
	}
	return sparkParams
}

func inferDecimalType(d string) (t string) {
	var overall int
	var after int
	if strings.HasPrefix(d, "0.") {
		// Less than one
		overall = len(d) - 2
		after = len(d) - 2
	} else if !strings.Contains(d, ".") {
		// Less than one
		overall = len(d)
		after = 0
	} else {
		components := strings.Split(d, ".")
		overall, after = len(components[0])+len(components[1]), len(components[1])
	}

	return fmt.Sprintf("DECIMAL(%d,%d)", overall, after)
}
