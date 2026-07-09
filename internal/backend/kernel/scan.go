//go:build cgo && databricks_kernel

package kernel

import (
	"math/big"
	"strings"

	"github.com/apache/arrow/go/v12/arrow/decimal128"
)

// decimal128ToExactString renders an Arrow decimal128 as an exact fixed-point
// string, applying scale by string placement rather than float conversion. This
// matches the Thrift path's default DECIMAL rendering and preserves precision a
// float64 would lose beyond ~17 significant digits (databricks-sql-go#274).
func decimal128ToExactString(n decimal128.Num, scale int32) string {
	unscaled := n.BigInt()
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
