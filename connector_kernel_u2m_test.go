package dbsql

import (
	"testing"

	"github.com/databricks/databricks-sql-go/internal/config"

	"github.com/stretchr/testify/assert"
)

func TestKernelSkipsDriverTelemetry(t *testing.T) {
	assert.True(
		t,
		skipDriverTelemetry(&config.Config{UserConfig: config.UserConfig{UseKernel: true}}),
		"kernel connections skip driver telemetry",
	)
	assert.False(
		t,
		skipDriverTelemetry(&config.Config{UserConfig: config.UserConfig{UseKernel: false}}),
		"thrift connections keep driver telemetry eligible",
	)
}
