package dbsql

import (
	"database/sql/driver"
	"testing"
	"time"
)

func TestEscaper(t *testing.T) {
	testcases := []struct {
		Value          driver.Value
		ExpectedOutput string
	}{
		{Value: "a'b'c", ExpectedOutput: `'a''b''c'`},
		{Value: int64(1024), ExpectedOutput: `1024`},
		{Value: float64(1024.5), ExpectedOutput: `1024.5`},
		{Value: true, ExpectedOutput: "true"},
		{Value: time.Date(2020, time.April, 11, 21, 34, 01, 0, time.UTC), ExpectedOutput: "'2020-04-11T21:34:01+00:00'"},
	}

	for _, test := range testcases {
		actual, err := EscapeArg(driver.NamedValue{Value: test.Value})
		if err != nil {
			t.Error(err)
		}
		if actual != test.ExpectedOutput {
			t.Errorf("expecting %v, actual value: %v", test.ExpectedOutput, actual)
		}

	}
}
