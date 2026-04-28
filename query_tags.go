package dbsql

import "strings"

// SerializeQueryTags converts a map of query tags to the wire format string.
// The format is comma-separated key:value pairs (e.g., "team:engineering,app:etl").
//
// Escaping rules (consistent with Python and NodeJS connectors):
//   - Keys: only backslashes are escaped
//   - Values: backslashes, colons, and commas are escaped with a leading backslash
//   - Empty string values result in just the key being emitted (no colon)
//
// Returns empty string if the map is nil or empty.
func SerializeQueryTags(tags map[string]string) string {
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
