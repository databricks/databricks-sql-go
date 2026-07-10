// Package decimalfmt renders Arrow decimal128 values as exact fixed-point
// strings, shared by the Thrift (arrowbased) and kernel result paths so a
// DECIMAL renders identically regardless of backend. Keeping one implementation
// means a precision fix (e.g. databricks-sql-go#274) lands in both at once.
package decimalfmt

import (
	"math/big"
	"strings"

	"github.com/apache/arrow/go/v12/arrow/decimal128"
)

// ExactString renders an Arrow decimal128 as an exact fixed-point string,
// applying scale by string placement rather than float conversion. This
// preserves precision a float64 would lose beyond ~17 significant digits
// (databricks-sql-go#274). A negative scale is not produced by the server and is
// treated as scale 0 rather than panicking.
func ExactString(n decimal128.Num, scale int32) string {
	unscaled := n.BigInt() // exact signed unscaled integer
	neg := unscaled.Sign() < 0
	digits := new(big.Int).Abs(unscaled).String()

	var b strings.Builder
	if neg {
		b.WriteByte('-')
	}

	if scale <= 0 {
		b.WriteString(digits)
		return b.String()
	}

	s := int(scale)
	if len(digits) <= s {
		// Pad with leading zeros so there are exactly `scale` fractional digits
		// and a single leading integer zero, e.g. 5 with scale 3 -> "0.005".
		b.WriteString("0.")
		b.WriteString(strings.Repeat("0", s-len(digits)))
		b.WriteString(digits)
	} else {
		b.WriteString(digits[:len(digits)-s])
		b.WriteByte('.')
		b.WriteString(digits[len(digits)-s:])
	}
	return b.String()
}
