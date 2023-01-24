package dbsql

import (
	"bytes"
	"database/sql/driver"
	"fmt"
	"strings"
	"time"
)

func EscapeArgs(args []driver.NamedValue) (_ []string, err error) {
	escaped := make([]string, len(args))

	for i, arg := range args {
		escaped[i], err = escapearg(arg.Value)
		if err != nil {
			return nil, err
		}
	}
	return escaped, nil
}

func escapearg(val interface{}) (string, error) {
	if vb, isBytes := val.([]byte); isBytes {
		val = string(vb)
	}
	switch v := val.(type) {
	case string:
		return "'" + strings.ReplaceAll(strings.ReplaceAll(v, "'", "''"), "\\", "\\\\") + "'", nil
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return fmt.Sprintf("%v", v), nil
	case bool:
		return fmt.Sprintf("%v", v), nil
	case nil:
		return "NULL", nil
	case time.Time:
		return "'" + v.Format("2006-01-02 15:04:05") + "'", nil
	case []interface{}:
		var err error
		nested := make([]string, len(v))
		for j, vv := range v {
			nested[j], err = escapearg(vv)
			if err != nil {
				return "", err
			}
		}
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("(%s)", strings.Join(nested, ", ")), nil
	default:
		return "", fmt.Errorf("unsupported type %T", v)
	}
}

func SubstituteArgs(sql string, args []driver.NamedValue) (string, error) {
	escaped, err := EscapeArgs(args)
	if err != nil {
		return "", err
	}

	buf := &bytes.Buffer{}
	i := 0
	for {
		p := strings.Index(sql, "?")
		if p == -1 {
			break
		}

		if len(sql[p:]) > 1 && sql[p:p+2] == "??" { // escape ?? => ?
			buf.WriteString(sql[:p])
			buf.WriteString("?")
			if len(sql[p:]) == 1 {
				break
			}
			sql = sql[p+2:]
		} else {
			buf.WriteString(sql[:p])
			fmt.Fprint(buf, escaped[i])
			i++
			sql = sql[p+1:]
		}
	}

	buf.WriteString(sql)
	return buf.String(), nil
}
