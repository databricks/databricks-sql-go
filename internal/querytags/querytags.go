// Package querytags holds the query-tag wire serialization shared by the public
// dbsql API and the execution backends.
//
// It lives in internal/ so that internal/backend/thrift (and the future kernel
// backend) can serialize per-statement query tags without importing the public
// dbsql package, which would create an import cycle (dbsql -> backend -> dbsql).
// The public dbsql.SerializeQueryTags forwards here so the exported behavior and
// symbol are unchanged.
package querytags

import "strings"

// Serialize converts a map of query tags to the wire format string. The format
// is comma-separated key:value pairs (e.g., "team:engineering,app:etl").
//
// Escaping rules (consistent with the Python and NodeJS connectors):
//   - Keys: only backslashes are escaped
//   - Values: backslashes, colons, and commas are escaped with a leading backslash
//   - Empty string values result in just the key being emitted (no colon)
//
// Returns empty string if the map is nil or empty.
func Serialize(tags map[string]string) string {
	if len(tags) == 0 {
		return ""
	}

	parts := make([]string, 0, len(tags))
	for k, v := range tags {
		escapedKey := strings.ReplaceAll(k, `\`, `\\`)
		if v == "" {
			parts = append(parts, escapedKey)
		} else {
			escapedValue := strings.ReplaceAll(v, `\`, `\\`)
			escapedValue = strings.ReplaceAll(escapedValue, `:`, `\:`)
			escapedValue = strings.ReplaceAll(escapedValue, `,`, `\,`)
			parts = append(parts, escapedKey+":"+escapedValue)
		}
	}
	return strings.Join(parts, ",")
}
