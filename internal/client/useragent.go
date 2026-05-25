package client

import (
	"fmt"

	"github.com/databricks/databricks-sql-go/internal/agent"
	"github.com/databricks/databricks-sql-go/internal/config"
)

// BuildUserAgent constructs the User-Agent header value used by the driver
// for Thrift, telemetry, and feature-flag requests so all traffic from a
// single connection is attributable to the same identifier in access logs.
//
// Format: "<driverName>/<driverVersion>[ (<userAgentEntry>)][ agent/<detectedAgent>]"
func BuildUserAgent(cfg *config.Config) string {
	userAgent := fmt.Sprintf("%s/%s", cfg.DriverName, cfg.DriverVersion)
	if cfg.UserAgentEntry != "" {
		userAgent = fmt.Sprintf("%s/%s (%s)", cfg.DriverName, cfg.DriverVersion, cfg.UserAgentEntry)
	}
	if agentProduct := agent.Detect(); agentProduct != "" {
		userAgent = fmt.Sprintf("%s agent/%s", userAgent, agentProduct)
	}
	return userAgent
}
