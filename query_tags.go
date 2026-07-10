package dbsql

import "github.com/databricks/databricks-sql-go/internal/querytags"

// SerializeQueryTags converts a map of query tags to the wire format string.
// The format is comma-separated key:value pairs (e.g., "team:engineering,app:etl").
//
// Escaping rules (consistent with Python and NodeJS connectors):
//   - Keys: only backslashes are escaped
//   - Values: backslashes, colons, and commas are escaped with a leading backslash
//   - Empty string values result in just the key being emitted (no colon)
//
// Returns empty string if the map is nil or empty.
//
// The implementation lives in internal/querytags so the execution backends can
// share it without importing this package; this remains the public entry point.
func SerializeQueryTags(tags map[string]string) string {
	return querytags.Serialize(tags)
}
