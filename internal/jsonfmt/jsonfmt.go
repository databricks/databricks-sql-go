// Package jsonfmt renders scalar values as JSON, byte-identical to
// encoding/json.Marshal, for the nested-type renderers shared by the Thrift
// (arrowbased) and kernel (arrowscan) result paths.
//
// Those renderers marshal one scalar per nested element, so per-call overhead
// dominates. Go 1.27 routes encoding/json through the json/v2 engine by default,
// which made that per-call cost markedly higher; formatting the common scalar
// types directly avoids it. Anything not handled here falls back to json.Marshal,
// so output never diverges from it.
package jsonfmt

import (
	"encoding/json"
	"math"
	"strconv"
	"unicode/utf8"
)

// Append appends the JSON encoding of v to dst, exactly as json.Marshal(v)
// would render it, and returns the extended slice.
func Append(dst []byte, v any) ([]byte, error) {
	switch t := v.(type) {
	case nil:
		return append(dst, "null"...), nil
	case bool:
		return strconv.AppendBool(dst, t), nil
	case int:
		return strconv.AppendInt(dst, int64(t), 10), nil
	case int8:
		return strconv.AppendInt(dst, int64(t), 10), nil
	case int16:
		return strconv.AppendInt(dst, int64(t), 10), nil
	case int32:
		return strconv.AppendInt(dst, int64(t), 10), nil
	case int64:
		return strconv.AppendInt(dst, t, 10), nil
	case uint8:
		return strconv.AppendUint(dst, uint64(t), 10), nil
	case uint16:
		return strconv.AppendUint(dst, uint64(t), 10), nil
	case uint32:
		return strconv.AppendUint(dst, uint64(t), 10), nil
	case uint64:
		return strconv.AppendUint(dst, t, 10), nil
	case float32:
		if !isFinite(float64(t)) {
			break
		}
		return appendFloat(dst, float64(t), 32), nil
	case float64:
		if !isFinite(t) {
			break
		}
		return appendFloat(dst, t, 64), nil
	case string:
		if needsEscape(t) {
			break
		}
		dst = append(dst, '"')
		dst = append(dst, t...)
		return append(dst, '"'), nil
	}
	// NaN/Inf (json.Marshal's error), strings needing escapes, []byte (base64),
	// and any other type.
	b, err := json.Marshal(v)
	if err != nil {
		return dst, err
	}
	if len(dst) == 0 {
		return b, nil // skip copying json.Marshal's fresh slice
	}
	return append(dst, b...), nil
}

func isFinite(f float64) bool {
	return !math.IsInf(f, 0) && !math.IsNaN(f)
}

// appendFloat mirrors encoding/json's ES6-style float formatting: plain decimal
// unless the magnitude is below 1e-6 or at least 1e21, with exponents unpadded.
func appendFloat(dst []byte, f float64, bits int) []byte {
	abs := math.Abs(f)
	format := byte('f')
	// Compare at the value's own width so float32 cutoffs match exactly.
	if abs != 0 {
		if bits == 64 && (abs < 1e-6 || abs >= 1e21) || bits == 32 && (float32(abs) < 1e-6 || float32(abs) >= 1e21) {
			format = 'e'
		}
	}
	dst = strconv.AppendFloat(dst, f, format, -1, bits)
	if format == 'e' {
		// e-09 -> e-9
		n := len(dst)
		if n >= 4 && dst[n-4] == 'e' && dst[n-3] == '-' && dst[n-2] == '0' {
			dst[n-2] = dst[n-1]
			dst = dst[:n-1]
		}
	}
	return dst
}

// needsEscape reports whether json.Marshal would escape any part of s. It copies
// ASCII verbatim except control characters, the quote/backslash and the
// HTML-escaped <, >, &; and valid multi-byte UTF-8 verbatim except U+2028/U+2029.
// Everything else (including invalid UTF-8) takes the fallback.
func needsEscape(s string) bool {
	for i := 0; i < len(s); i++ {
		if !plainASCII[s[i]] {
			if s[i] < utf8.RuneSelf {
				return true
			}
			return needsEscapeUTF8(s[i:])
		}
	}
	return false
}

// needsEscapeUTF8 is needsEscape for the rest of s from its first non-ASCII byte.
func needsEscapeUTF8(s string) bool {
	for i := 0; i < len(s); {
		if c := s[i]; c < utf8.RuneSelf {
			if !plainASCII[c] {
				return true
			}
			i++
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 || r == 0x2028 || r == 0x2029 {
			return true
		}
		i += size
	}
	return false
}

// plainASCII marks the ASCII bytes json.Marshal copies verbatim. A table lookup
// keeps the scan faster than json.Marshal's own on long strings.
var plainASCII = func() (t [256]bool) {
	for c := 0x20; c <= 0x7f; c++ {
		t[c] = true
	}
	for _, c := range `"\<>&` {
		t[c] = false
	}
	return t
}()
