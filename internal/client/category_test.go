package client

import (
	"context"
	"net/http"
	"testing"

	"github.com/databricks/databricks-sql-go/driverctx"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	dbsqlerrint "github.com/databricks/databricks-sql-go/internal/errors"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

// categoryForStatus returns the category errorHandler assigns for a status code.
func categoryForStatus(code int) dbsqlerrint.ErrorCategory {
	var resp *http.Response
	if code != 0 {
		resp = &http.Response{StatusCode: code}
	}
	//nolint:bodyclose // synthetic response, no body
	_, werr := errorHandler(resp, errors.New("boom"), 3)
	return dbsqlerrint.CategoryFromError(werr)
}

// Only a retry-exhausted 429 is tagged rate_limit_exceeded; other statuses aren't.
func TestErrorHandlerRateLimitTag(t *testing.T) {
	assert.Equal(t, dbsqlerrint.CategoryRateLimitExceeded, categoryForStatus(http.StatusTooManyRequests))
	assert.Equal(t, dbsqlerrint.ErrorCategory(""), categoryForStatus(http.StatusServiceUnavailable), "503 is retryable but not rate limiting")
	assert.Equal(t, dbsqlerrint.ErrorCategory(""), categoryForStatus(http.StatusInternalServerError))
	assert.Equal(t, dbsqlerrint.ErrorCategory(""), categoryForStatus(0), "nil response")
}

// The 429 telemetry error must carry the request's connection id, not empty ids.
func TestErrorHandlerRateLimitCarriesIDs(t *testing.T) {
	ctx := driverctx.NewContextWithConnId(context.Background(), "conn-xyz")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://example", nil)
	assert.NoError(t, err)
	resp := &http.Response{StatusCode: http.StatusTooManyRequests, Request: req}

	//nolint:bodyclose // synthetic response, no body
	_, werr := errorHandler(resp, errors.New("boom"), 3)

	var dbErr dbsqlerr.DBError
	assert.True(t, errors.As(werr, &dbErr))
	assert.Equal(t, "conn-xyz", dbErr.ConnectionId(), "429 telemetry error must carry the request's connection id")
}
