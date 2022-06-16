package dbsql

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"time"
)

const (
	TimeFmt = "2006-01-02T15:04:05.999-07:00"
)

func EscapeArg(arg driver.NamedValue) (string, error) {
	switch v := arg.Value.(type) {
	case int64:
		return fmt.Sprintf("%v", v), nil
	case float64:
		return fmt.Sprintf("%v", v), nil
	case bool:
		return fmt.Sprintf("%v", v), nil
	case string:
		return fmt.Sprintf("'%v'", strings.ReplaceAll(v, "'", "''")), nil
	case time.Time:
		return "'" + v.Format(TimeFmt) + "'", nil
	default:
		return "", fmt.Errorf("unsupported parameter type %T for value %v", v, v)
	}
}
