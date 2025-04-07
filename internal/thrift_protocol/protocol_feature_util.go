package thrift_protocol

import "github.com/databricks/databricks-sql-go/internal/cli_service"

// Feature checks
// SupportsDirectResults checks if the server protocol version supports direct results
// Supported in SPARK_CLI_SERVICE_PROTOCOL_V1 and above
func SupportsDirectResults(version cli_service.TProtocolVersion) bool {
	return version >= cli_service.TProtocolVersion_SPARK_CLI_SERVICE_PROTOCOL_V1
}

// SupportsLz4Compression checks if the server protocol version supports LZ4 compression
// Supported in SPARK_CLI_SERVICE_PROTOCOL_V6 and above
func SupportsLz4Compression(version cli_service.TProtocolVersion) bool {
	return version >= cli_service.TProtocolVersion_SPARK_CLI_SERVICE_PROTOCOL_V6
}

// SupportsCloudFetch checks if the server protocol version supports cloud fetch
// Supported in SPARK_CLI_SERVICE_PROTOCOL_V3 and above
func SupportsCloudFetch(version cli_service.TProtocolVersion) bool {
	return version >= cli_service.TProtocolVersion_SPARK_CLI_SERVICE_PROTOCOL_V3
}

// SupportsArrow checks if the server protocol version supports Arrow format
// Supported in SPARK_CLI_SERVICE_PROTOCOL_V5 and above
func SupportsArrow(version cli_service.TProtocolVersion) bool {
	return version >= cli_service.TProtocolVersion_SPARK_CLI_SERVICE_PROTOCOL_V5
}

// SupportsCompressedArrow checks if the server protocol version supports compressed Arrow format
// Supported in SPARK_CLI_SERVICE_PROTOCOL_V6 and above
func SupportsCompressedArrow(version cli_service.TProtocolVersion) bool {
	return version >= cli_service.TProtocolVersion_SPARK_CLI_SERVICE_PROTOCOL_V6
}

// SupportsParameterizedQueries checks if the server protocol version supports parameterized queries
// Supported in SPARK_CLI_SERVICE_PROTOCOL_V8 and above
func SupportsParameterizedQueries(version cli_service.TProtocolVersion) bool {
	return version >= cli_service.TProtocolVersion_SPARK_CLI_SERVICE_PROTOCOL_V8
}

// SupportsMultipleCatalogs checks if the server protocol version supports multiple catalogs
// Supported in SPARK_CLI_SERVICE_PROTOCOL_V4 and above
func SupportsMultipleCatalogs(version cli_service.TProtocolVersion) bool {
	return version >= cli_service.TProtocolVersion_SPARK_CLI_SERVICE_PROTOCOL_V4
}
