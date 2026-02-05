package telemetry

// Tag names for connection metrics
const (
	TagWorkspaceID   = "workspace.id"
	TagSessionID     = "session.id"
	TagDriverVersion = "driver.version"
	TagDriverOS      = "driver.os"
	TagDriverRuntime = "driver.runtime"
	TagServerAddress = "server.address" // Not exported to Databricks
)

// Tag names for statement metrics
const (
	TagStatementID           = "statement.id"
	TagResultFormat          = "result.format"
	TagResultChunkCount      = "result.chunk_count"
	TagResultBytesDownloaded = "result.bytes_downloaded"
	TagCompressionEnabled    = "result.compression_enabled"
	TagPollCount             = "poll.count"
	TagPollLatency           = "poll.latency_ms"
)

// Tag names for operation metrics
const (
	TagOperationType = "operation_type"
)

// Tag names for error metrics
const (
	TagErrorType = "error.type"
	TagErrorCode = "error.code"
)

// Feature flag tags
const (
	TagFeatureCloudFetch    = "feature.cloudfetch"
	TagFeatureLZ4           = "feature.lz4"
	TagFeatureDirectResults = "feature.direct_results"
)

// tagExportScope defines where a tag can be exported.
type tagExportScope int

const (
	exportNone  tagExportScope = 0
	exportLocal                = 1 << iota
	exportDatabricks
	exportAll = exportLocal | exportDatabricks
)

// tagDefinition defines a metric tag and its export scope.
type tagDefinition struct {
	name        string
	exportScope tagExportScope
	description string
	required    bool
}

// connectionTags returns tags allowed for connection events.
func connectionTags() []tagDefinition {
	return []tagDefinition{
		{TagWorkspaceID, exportDatabricks, "Databricks workspace ID", true},
		{TagSessionID, exportDatabricks, "Connection session ID", true},
		{TagDriverVersion, exportAll, "Driver version", false},
		{TagDriverOS, exportAll, "Operating system", false},
		{TagDriverRuntime, exportAll, "Go runtime version", false},
		{TagFeatureCloudFetch, exportDatabricks, "CloudFetch enabled", false},
		{TagFeatureLZ4, exportDatabricks, "LZ4 compression enabled", false},
		{TagServerAddress, exportLocal, "Server address (local only)", false},
	}
}

// statementTags returns tags allowed for statement events.
func statementTags() []tagDefinition {
	return []tagDefinition{
		{TagStatementID, exportDatabricks, "Statement ID", true},
		{TagSessionID, exportDatabricks, "Session ID", true},
		{TagResultFormat, exportDatabricks, "Result format", false},
		{TagResultChunkCount, exportDatabricks, "Chunk count", false},
		{TagResultBytesDownloaded, exportDatabricks, "Bytes downloaded", false},
		{TagCompressionEnabled, exportDatabricks, "Compression enabled", false},
		{TagPollCount, exportDatabricks, "Poll count", false},
		{TagPollLatency, exportDatabricks, "Poll latency", false},
	}
}

// shouldExportToDatabricks returns true if tag should be exported to Databricks.
func shouldExportToDatabricks(metricType, tagName string) bool {
	var tags []tagDefinition
	switch metricType {
	case "connection":
		tags = connectionTags()
	case "statement":
		tags = statementTags()
	default:
		return false
	}

	for _, tag := range tags {
		if tag.name == tagName {
			return tag.exportScope&exportDatabricks != 0
		}
	}
	return false
}
