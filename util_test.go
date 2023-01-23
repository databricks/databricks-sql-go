package dbsql

import (
	"database/sql/driver"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEscapeArgs(t *testing.T) {
	thyme := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	tests := map[string]struct {
		args        []driver.NamedValue
		expected    []string
		expectError bool
	}{
		"strings": {
			args: []driver.NamedValue{{Ordinal: 1, Value: "foo"}, {Ordinal: 2, Value: "bar"}},
			expected: []string{
				"'foo'",
				"'bar'",
			},
		},
		"strings with quotes": {
			args: []driver.NamedValue{{Ordinal: 1, Value: "f'oo"}, {Ordinal: 2, Value: "bar"}},
			expected: []string{
				"'f''oo'",
				"'bar'",
			},
		},
		"lists": {
			args: []driver.NamedValue{{Ordinal: 1, Value: []interface{}{"foo", "bar"}}},
			expected: []string{
				"('foo', 'bar')",
			},
		},
		"time": {
			args: []driver.NamedValue{{Ordinal: 1, Value: thyme}},
			expected: []string{
				"'2020-01-01 00:00:00'",
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			actual, err := EscapeArgs(test.args)
			if test.expectError {
				assert.Error(t, err)
			} else {
				assert.Equal(t, test.expected, actual)
			}
		})
	}
}

func TestSubstituteArgs(t *testing.T) {
	tests := map[string]struct {
		sql         string
		args        []driver.NamedValue
		expected    string
		expectError bool
	}{
		"no args": {
			sql:      "SELECT * FROM foo",
			expected: "SELECT * FROM foo",
		},
		"one arg": {
			sql:      "SELECT * FROM foo WHERE bar = ?",
			args:     []driver.NamedValue{{Ordinal: 1, Value: "baz"}},
			expected: "SELECT * FROM foo WHERE bar = 'baz'",
		},
		"two args": {
			sql:      "SELECT * FROM foo WHERE bar = ? AND baz = ?",
			args:     []driver.NamedValue{{Ordinal: 1, Value: "baz"}, {Ordinal: 2, Value: "qux"}},
			expected: "SELECT * FROM foo WHERE bar = 'baz' AND baz = 'qux'",
		},
		"two args with list": {
			sql:      "SELECT * FROM foo WHERE bar = ? AND baz IN ?",
			args:     []driver.NamedValue{{Ordinal: 1, Value: "baz"}, {Ordinal: 2, Value: []interface{}{"qux", "quux"}}},
			expected: "SELECT * FROM foo WHERE bar = 'baz' AND baz IN ('qux', 'quux')",
		},
		"three args with list and time": {
			sql:      "SELECT * FROM foo WHERE bar = ? AND baz IN ? AND qux = ?",
			args:     []driver.NamedValue{{Ordinal: 1, Value: "baz"}, {Ordinal: 2, Value: []interface{}{"qux", "quux"}}, {Ordinal: 3, Value: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)}},
			expected: "SELECT * FROM foo WHERE bar = 'baz' AND baz IN ('qux', 'quux') AND qux = '2020-01-01 00:00:00'",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			actual, err := SubstituteArgs(test.sql, test.args)
			if test.expectError {
				assert.Error(t, err)
			} else {
				assert.Equal(t, test.expected, actual)
			}
		})
	}
}
