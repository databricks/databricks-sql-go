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

// Tag names for statement metrics.
// Values must match the keys used in metricContext.tags and read by
// createTelemetryRequest / aggregator — keep them in sync.
const (
	TagStatementID        = "statement.id"
	TagResultFormat       = "result.format"
	TagChunkCount         = "chunk_count"
	TagBytesDownloaded    = "bytes_downloaded"
	TagCompressionEnabled = "result.compression_enabled"
	TagOperationType      = "operation_type"
	TagPollCount          = "poll_count"
	TagPollLatency        = "poll.latency_ms"
)

// Tag names for chunk timing metrics
const (
	TagChunkInitialLatencyMs = "chunk_initial_latency_ms"
	TagChunkSlowestLatencyMs = "chunk_slowest_latency_ms"
	TagChunkSumLatencyMs     = "chunk_sum_latency_ms"
	TagChunkTotalPresent     = "chunk_total_present"
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
