package kernel

import "strings"

// This file is intentionally NOT behind the `cgo && databricks_kernel` build tag.
// Staging detection is pure Go (string inspection of the SQL), so keeping it
// untagged lets its test run under CGO_ENABLED=0.

// isStagingStatement reports whether sql is a Unity Catalog volume staging
// command (PUT / GET / REMOVE). The Thrift backend learns a statement is staging
// from server result metadata (IsStagingOperation); the kernel C ABI exposes no
// such signal, and the kernel path cannot perform the local file transfer these
// commands require. We therefore detect them from the SQL and reject at execute
// time (see Execute) rather than silently returning success with no file moved —
// which is what returning false from IsStaging would do. Case-insensitive; skips
// leading whitespace AND leading SQL comments (`-- ...` and `/* ... */`), so a
// comment-prefixed staging command is still caught rather than slipping through
// as a silent no-op. Errs toward matching only a clear leading keyword — a false
// negative just yields the kernel's normal "unsupported syntax" server error,
// whereas a false positive would reject a legitimate query.
func isStagingStatement(sql string) bool {
	s := stripLeadingSQLComments(sql)
	for _, kw := range []string{"PUT", "GET", "REMOVE"} {
		if len(s) > len(kw) &&
			strings.EqualFold(s[:len(kw)], kw) &&
			isSQLTokenBreak(s[len(kw)]) {
			return true
		}
	}
	return false
}

// stripLeadingSQLComments removes leading whitespace and leading SQL comments
// (both `-- ...` line comments and `/* ... */` block comments), repeatedly, so
// the first real token is exposed. An unterminated block comment consumes the
// rest of the string (yielding ""). Comment bodies are not scanned for nested
// markers — SQL block comments do not nest — and this only touches the LEADING
// run, so a comment later in the statement is untouched.
func stripLeadingSQLComments(sql string) string {
	s := strings.TrimSpace(sql)
	for {
		switch {
		case strings.HasPrefix(s, "--"):
			if i := strings.IndexByte(s, '\n'); i >= 0 {
				s = s[i+1:]
			} else {
				return "" // line comment to end of input
			}
		case strings.HasPrefix(s, "/*"):
			if i := strings.Index(s[2:], "*/"); i >= 0 {
				s = s[2+i+2:]
			} else {
				return "" // unterminated block comment
			}
		default:
			return s
		}
		s = strings.TrimSpace(s)
	}
}

// isSQLTokenBreak reports whether b ends a leading keyword — a space or a quote
// (staging commands take a quoted path, e.g. PUT '<local>' INTO '<volume>'), so
// "PUT '..." matches but an identifier like "PUTS" or "GETTER" does not.
func isSQLTokenBreak(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r' || b == '\'' || b == '"'
}
