package arrowscan

import (
	"database/sql/driver"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
)

func timeOfDayValue(value int64, unit arrow.TimeUnit) driver.Value {
	return timeOfDay(value, unit)
}

func timeOfDay(value int64, unit arrow.TimeUnit) time.Time {
	return time.Time{}.Add(time.Duration(value) * unit.Multiplier())
}
