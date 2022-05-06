package dbsql

import (
	"database/sql/driver"
	"testing"
)

func TestStatement(t *testing.T) {
	tests := []struct {
		stmt   string
		args   []driver.NamedValue
		target string
	}{
		{
			stmt: "@p1 p1",
			args: []driver.NamedValue{
				{Ordinal: 1, Value: "val_1"},
			},
			target: "val_1 p1",
		},
		{
			stmt: "@p1 @p10 @p11 @named @named1 @p1",
			args: []driver.NamedValue{
				{Ordinal: 1, Value: "val_1"},
				{Ordinal: 10, Name: "named", Value: "val_named"},
				{Ordinal: 11, Value: "val_11"},
			},
			target: "val_1 @p10 val_11 val_named @named1 val_1",
		},
	}

	for _, tt := range tests {
		result := statement(tt.stmt, tt.args)

		if result != tt.target {
			t.Fatalf("mismatch for statement: %q\n\ttarget: %q\n\tresult: %q", tt.stmt, tt.target, result)
		}
	}
}
