package telemetry

import (
	"errors"
	"strings"
)

// isTerminalError returns true if error is terminal (non-retryable).
// Terminal errors indicate user errors or permanent failures that won't
// be resolved by retrying the operation.
func isTerminalError(err error) bool {
	if err == nil {
		return false
	}

	// Check error message patterns for terminal errors
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

	errMsg := strings.ToLower(err.Error())

	// Check for common error patterns
	patterns := map[string]string{
		"timeout":        "timeout",
		"context cancel": "cancelled",
		"connection":     "connection_error",
		"authentication": "auth_error",
		"unauthorized":   "auth_error",
		"forbidden":      "permission_error",
		"not found":      "not_found",
		"syntax":         "syntax_error",
		"invalid":        "invalid_request",
	}

	for pattern, errorType := range patterns {
		if strings.Contains(errMsg, pattern) {
			return errorType
		}
	}

	// Default to generic error
	return "error"
}

// isRetryableError returns true if the error is retryable.
// This is the inverse of isTerminalError.
func isRetryableError(err error) bool {
	return !isTerminalError(err)
}

// httpError represents an HTTP error with status code.
type httpError struct {
	statusCode int
	message    string
}

func (e *httpError) Error() string {
	return e.message
}

// newHTTPError creates a new HTTP error.
func newHTTPError(statusCode int, message string) error {
	return &httpError{
		statusCode: statusCode,
		message:    message,
	}
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
