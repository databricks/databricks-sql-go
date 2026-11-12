package kernel

import "strings"

// This file is intentionally NOT behind the `cgo && databricks_kernel` build tag.
// Staging detection is pure Go (string inspection of the SQL), so keeping it
// untagged lets its test run under CGO_ENABLED=0.

// isStagingStatement detects PUT, GET, or REMOVE after leading SQL comments,
// ignoring keyword case.
func isStagingStatement(sql string) bool {
	s := stripLeadingSQLComments(sql)
	for _, kw := range []string{"PUT", "GET", "REMOVE"} {
		if len(s) > len(kw) &&
			strings.EqualFold(s[:len(kw)], kw) &&
			(isSQLTokenBreak(s[len(kw)]) || strings.HasPrefix(s[len(kw):], "/*") || strings.HasPrefix(s[len(kw):], "--")) {
			return true
		}
	}
	return false
}

// stripLeadingSQLComments removes leading whitespace and leading SQL comments
// (both `-- ...` line comments and `/* ... */` block comments), repeatedly, so
// the first real token is exposed. An unterminated block comment consumes the
// rest of the string (yielding ""). Nested block comments must be balanced so
// their trailing text cannot hide a staging keyword. Only the leading run is
// removed; comments later in the statement are untouched.
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
			depth, i := 1, 2
			for i < len(s) && depth > 0 {
				switch {
				case strings.HasPrefix(s[i:], "/*"):
					depth++
					i += 2
				case strings.HasPrefix(s[i:], "*/"):
					depth--
					i += 2
				default:
					i++
				}
			}
			if depth != 0 {
				return "" // unterminated block comment
			}
			s = s[i:]
		default:
			return s
		}
		s = strings.TrimSpace(s)
	}
}

// isSQLTokenBreak reports whether b ends a leading keyword, whitespace or a quote
// (staging commands take a quoted path, e.g. PUT '<local>' INTO '<volume>'), so
// "PUT '..." matches but an identifier like "PUTS" or "GETTER" does not.
func isSQLTokenBreak(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r' || b == '\f' || b == '\v' || b == '\'' || b == '"'
}
