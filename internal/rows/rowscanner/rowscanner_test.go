package rowscanner

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestHandlingDateTime(t *testing.T) {
	t.Run("should do nothing if data is not a date/time", func(t *testing.T) {
		val, err := HandleDateTime("this is not a date", "STRING", "string_col", time.UTC)
		assert.Nil(t, err, "handleDateTime should do nothing if a column is not a date/time")
		assert.Equal(t, "this is not a date", val)
	})

	t.Run("should error on invalid date/time value", func(t *testing.T) {
		_, err := HandleDateTime("this is not a date", "DATE", "date_col", time.UTC)
		assert.NotNil(t, err)
		assert.True(t, strings.HasPrefix(err.Error(), fmt.Sprintf(ErrRowsParseValue, "DATE", "this is not a date", "date_col")))
	})

	t.Run("should parse valid date", func(t *testing.T) {
		dt, err := HandleDateTime("2006-12-22", "DATE", "date_col", time.UTC)
		assert.Nil(t, err)
		assert.Equal(t, time.Date(2006, 12, 22, 0, 0, 0, 0, time.UTC), dt)
	})

	t.Run("should parse valid timestamp", func(t *testing.T) {
		dt, err := HandleDateTime("2006-12-22 17:13:11.000001000", "TIMESTAMP", "timestamp_col", time.UTC)
		assert.Nil(t, err)
		assert.Equal(t, time.Date(2006, 12, 22, 17, 13, 11, 1000, time.UTC), dt)
	})

	t.Run("should parse date with negative year", func(t *testing.T) {
		expectedTime := time.Date(-2006, 12, 22, 0, 0, 0, 0, time.UTC)
		dateStrings := []string{
			"-2006-12-22",
			"\u22122006-12-22",
			"\x2D2006-12-22",
		}

		for _, s := range dateStrings {
			dt, err := HandleDateTime(s, "DATE", "date_col", time.UTC)
			assert.Nil(t, err)
			assert.Equal(t, expectedTime, dt)
		}
	})

	t.Run("should parse timestamp with negative year", func(t *testing.T) {
		expectedTime := time.Date(-2006, 12, 22, 17, 13, 11, 1000, time.UTC)

		timestampStrings := []string{
			"-2006-12-22 17:13:11.000001000",
			"\u22122006-12-22 17:13:11.000001000",
			"\x2D2006-12-22 17:13:11.000001000",
		}

		for _, s := range timestampStrings {
			dt, err := HandleDateTime(s, "TIMESTAMP", "timestamp_col", time.UTC)
			assert.Nil(t, err)
			assert.Equal(t, expectedTime, dt)
		}
	})
}
