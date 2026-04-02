package dbsql

import (
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSerializeQueryTags(t *testing.T) {
	t.Parallel()

	t.Run("nil input returns empty string", func(t *testing.T) {
		result := SerializeQueryTags(nil)
		assert.Equal(t, "", result)
	})

	t.Run("empty map returns empty string", func(t *testing.T) {
		result := SerializeQueryTags(map[string]string{})
		assert.Equal(t, "", result)
	})

	t.Run("single tag", func(t *testing.T) {
		result := SerializeQueryTags(map[string]string{"team": "engineering"})
		assert.Equal(t, "team:engineering", result)
	})

	t.Run("multiple tags", func(t *testing.T) {
		// Go map iteration order is non-deterministic, so we check the parts
		result := SerializeQueryTags(map[string]string{"team": "engineering", "app": "etl"})
		parts := strings.Split(result, ",")
		sort.Strings(parts)
		assert.Equal(t, []string{"app:etl", "team:engineering"}, parts)
	})

	t.Run("empty value emits key only without colon", func(t *testing.T) {
		result := SerializeQueryTags(map[string]string{"flag": ""})
		assert.Equal(t, "flag", result)
	})

	t.Run("mixed empty and non-empty values", func(t *testing.T) {
		result := SerializeQueryTags(map[string]string{"team": "eng", "flag": "", "app": "etl"})
		parts := strings.Split(result, ",")
		sort.Strings(parts)
		assert.Equal(t, []string{"app:etl", "flag", "team:eng"}, parts)
	})

	t.Run("escape backslash in value", func(t *testing.T) {
		result := SerializeQueryTags(map[string]string{"path": `a\b`})
		assert.Equal(t, `path:a\\b`, result)
	})

	t.Run("escape colon in value", func(t *testing.T) {
		result := SerializeQueryTags(map[string]string{"url": "http://host"})
		assert.Equal(t, `url:http\://host`, result)
	})

	t.Run("escape comma in value", func(t *testing.T) {
		result := SerializeQueryTags(map[string]string{"list": "a,b"})
		assert.Equal(t, `list:a\,b`, result)
	})

	t.Run("escape multiple special chars in value", func(t *testing.T) {
		result := SerializeQueryTags(map[string]string{"val": `a\b:c,d`})
		assert.Equal(t, `val:a\\b\:c\,d`, result)
	})

	t.Run("escape backslash in key", func(t *testing.T) {
		result := SerializeQueryTags(map[string]string{`a\b`: "value"})
		assert.Equal(t, `a\\b:value`, result)
	})

	t.Run("escape backslash in key with empty value", func(t *testing.T) {
		result := SerializeQueryTags(map[string]string{`a\b`: ""})
		assert.Equal(t, `a\\b`, result)
	})

	t.Run("colons and commas in keys are not escaped", func(t *testing.T) {
		result := SerializeQueryTags(map[string]string{"key:name": "value"})
		assert.Equal(t, "key:name:value", result)
	})

	t.Run("all empty values", func(t *testing.T) {
		result := SerializeQueryTags(map[string]string{"key1": "", "key2": "", "key3": ""})
		parts := strings.Split(result, ",")
		sort.Strings(parts)
		assert.Equal(t, []string{"key1", "key2", "key3"}, parts)
	})
}
