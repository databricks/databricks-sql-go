package arrowscan

import (
	"database/sql/driver"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
)

func timeOfDayValue(value int64, unit arrow.TimeUnit) driver.Value {
	return timeOfDay(value, unit)
}

// timeOfDay carries TIME on 0001-01-01 UTC. Midnight equals time.Time{} and
// JSON omitzero omits it; detect NULL with sql.NullTime or *time.Time, not IsZero.
func timeOfDay(value int64, unit arrow.TimeUnit) time.Time {
	return time.Time{}.Add(time.Duration(value) * unit.Multiplier())
}
