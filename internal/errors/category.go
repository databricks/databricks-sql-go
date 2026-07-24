package errors

// ErrorCategory is a source-declared classification for a driver error, so
// telemetry can read the category directly instead of inferring it from the
// error message.
type ErrorCategory string

type categorizer interface {
	Category() ErrorCategory
}

// CategoryFromError returns the innermost (deepest) non-empty category in the
// error chain, so a tag on an outer wrapper never masks the more specific one
// at the source. Handles both single-error and tree-shaped (errors.Join /
// multiple %w) Unwrap chains.
func CategoryFromError(err error) ErrorCategory {
	if err == nil {
		return ""
	}
	switch x := err.(type) {
	case interface{ Unwrap() error }:
		if deeper := CategoryFromError(x.Unwrap()); deeper != "" {
			return deeper
		}
	case interface{ Unwrap() []error }:
		for _, e := range x.Unwrap() {
			if deeper := CategoryFromError(e); deeper != "" {
				return deeper
			}
		}
	}
	if c, ok := err.(categorizer); ok {
		return c.Category()
	}
	return ""
}

const (
	CategoryTimeout              ErrorCategory = "timeout"
	CategoryCancelled            ErrorCategory = "cancelled"
	CategoryConnectionError      ErrorCategory = "connection_error"
	CategoryAuthError            ErrorCategory = "auth_error"
	CategoryPermissionError      ErrorCategory = "permission_error"
	CategoryNotFound             ErrorCategory = "not_found"
	CategorySyntaxError          ErrorCategory = "syntax_error"
	CategoryInvalidRequest       ErrorCategory = "invalid_request"
	CategoryGeneric              ErrorCategory = "error"
	CategoryChunkDownload        ErrorCategory = "chunk_download_error"
	CategoryDecompression        ErrorCategory = "decompression_error"
	CategoryArrowSchemaParsing   ErrorCategory = "arrow_schema_parsing_error"
	CategoryResultSet            ErrorCategory = "result_set_error"
	CategoryUnsupportedOperation ErrorCategory = "unsupported_operation"
	CategoryRateLimitExceeded    ErrorCategory = "rate_limit_exceeded"
	CategorySSLHandshake         ErrorCategory = "ssl_handshake_error"
	CategoryStatementTimeout     ErrorCategory = "statement_execution_timeout"
	CategoryExecuteStatement     ErrorCategory = "execute_statement_failed"
	CategoryStatementCancelled   ErrorCategory = "execute_statement_cancelled"
	CategorySessionClosed        ErrorCategory = "session_closed"
	CategoryStatementClosed      ErrorCategory = "statement_closed"
)
