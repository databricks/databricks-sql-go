package dbsql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPathAllowed(t *testing.T) {
	t.Run("Should not allow paths that don't share directory", func(t *testing.T) {
		stagingAllowedLocalPath := []string{"/var/www/html"}
		localFile := "/var/www/html/../html1/not_allowed.html"
		assert.False(t, localPathIsAllowed(stagingAllowedLocalPath, localFile))
	})
	t.Run("Should allow multiple specified allowed local staging paths", func(t *testing.T) {
		stagingAllowedLocalPath := []string{"/foo", "/var/www/html"}
		localFile := "/var/www/html/allowed.html"
		assert.True(t, localPathIsAllowed(stagingAllowedLocalPath, localFile))
	})
}
