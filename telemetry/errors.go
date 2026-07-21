package telemetry

import (
	"errors"
	"strings"

	dbsqlerrint "github.com/databricks/databricks-sql-go/internal/errors"
)

// isTerminalError returns true if error is terminal (non-retryable).
// Terminal errors indicate user errors or permanent failures that won't
// be resolved by retrying the operation.

func isTerminalError(err error) bool {
	if err == nil {
		return false
	}

	// Priority 1: Check HTTP status code if available (most reliable)
	if httpErr, ok := extractHTTPError(err); ok {
		return isTerminalHTTPStatus(httpErr.statusCode)
	}

	// Priority 2: Fall back to error message patterns
	errMsg := strings.ToLower(err.Error())
	terminalPatterns := []string{
		"authentication failed",
		"unauthorized",
		"forbidden",
		"not found",
		"invalid request",
		"syntax error",
		"bad request",
		"invalid parameter",
		"permission denied",
	}

	for _, pattern := range terminalPatterns {
		if strings.Contains(errMsg, pattern) {
			return true
		}
	}

	return false
}

// classifyError classifies an error for telemetry purposes.
// Returns a string representation of the error type.

func classifyError(err error) string {
	if err == nil {
		return ""
	}

	// Prefer a category declared at the error source, else match the message.
	if category := dbsqlerrint.CategoryFromError(err); category != "" {
		return string(category)
	}

	errMsg := strings.ToLower(err.Error())

	// Ordered patterns — first match wins. Emit the shared category constants
	// so the wire strings live in one place.
	patterns := []struct {
		pattern   string
		errorType dbsqlerrint.ErrorCategory
	}{
		{"timeout", dbsqlerrint.CategoryTimeout},
		{"context cancel", dbsqlerrint.CategoryCancelled},
		{"connection", dbsqlerrint.CategoryConnectionError},
		{"authentication", dbsqlerrint.CategoryAuthError},
		{"unauthorized", dbsqlerrint.CategoryAuthError},
		{"forbidden", dbsqlerrint.CategoryPermissionError},
		{"not found", dbsqlerrint.CategoryNotFound},
		{"syntax", dbsqlerrint.CategorySyntaxError},
		{"invalid", dbsqlerrint.CategoryInvalidRequest},
	}

	for _, p := range patterns {
		if strings.Contains(errMsg, p.pattern) {
			return string(p.errorType)
		}
	}

	// Default to generic error
	return string(dbsqlerrint.CategoryGeneric)
}

// httpError represents an HTTP error with status code.

type httpError struct {
	statusCode int
	message    string
}

func (e *httpError) Error() string {
	return e.message
}

// isTerminalHTTPStatus returns true for non-retryable HTTP status codes.

func isTerminalHTTPStatus(status int) bool {
	// 4xx errors (except 429) are terminal
	return status >= 400 && status < 500 && status != 429
}

// extractHTTPError extracts HTTP error information if available.

func extractHTTPError(err error) (*httpError, bool) {
	var httpErr *httpError
	if errors.As(err, &httpErr) {
		return httpErr, true
	}
	return nil, false
}
