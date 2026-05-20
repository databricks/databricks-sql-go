package dbsql

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandleStagingRemoveHTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte("Access Denied"))
	}))
	defer server.Close()

	c := &conn{cfg: config.WithDefaults()}
	err := c.handleStagingRemove(context.Background(), server.URL, map[string]string{})
	require.NotNil(t, err)
	assert.NotContains(t, err.Error(), ", nil", "error message should not contain literal \", nil\"")
}

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
