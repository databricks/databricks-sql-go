// Package decimalfmt renders Arrow decimal128 values as exact fixed-point
// strings, shared by the Thrift (arrowbased) and kernel result paths so a
// DECIMAL renders identically regardless of backend. Keeping one implementation
// means a precision fix (e.g. databricks-sql-go#274) lands in both at once.
package decimalfmt

import (
	"math/big"

	"github.com/apache/arrow/go/v12/arrow/decimal128"
)

// decMaxLen bounds the longest exact string a real Databricks DECIMAL produces,
// so ExactString's stack scratch never has to grow onto the heap. A Databricks
// DECIMAL is at most precision 38, i.e. 38 significant digits; the widest render
// is a small magnitude at a large scale — "0." + (scale) fractional digits — plus
// a sign. 48 covers scale up to 45 with the sign and "0." prefix; anything longer
// (only reachable via a synthetic decimal128 scale the server never sends) still
// renders correctly, just with one heap growth in Append.
const decMaxLen = 48

// ExactString renders an Arrow decimal128 as an exact fixed-point string,
// applying scale by digit placement rather than float conversion. This preserves
// precision a float64 would lose beyond ~17 significant digits
// (databricks-sql-go#274). A negative scale is not produced by the server and is
// treated as scale 0 rather than panicking.
//
// It allocates only the returned string (the digit rendering uses a stack
// scratch, no math/big for magnitudes that fit in a uint64 — i.e. every
// DECIMAL(≤18) and most DECIMAL(≤38) values). See Append for the zero-copy form.
func ExactString(n decimal128.Num, scale int32) string {
	var buf [decMaxLen]byte
	return string(Append(buf[:0], n, scale))
}

// Append renders n at the given scale into dst and returns the extended slice,
// exactly as ExactString would but without forcing a string allocation — for
// callers that render many decimals into a reused buffer (e.g. a batch-scoped
// arena). The output is byte-for-byte identical to ExactString.
//
// The magnitude is rendered from the unscaled integer with no heap allocation
// when it fits in a uint64 (the common case: |unscaled| < 2^64 covers every
// DECIMAL(≤18) and every DECIMAL(≤38) value whose magnitude stays under 2^64).
// A true 128-bit magnitude (DECIMAL(20+) with a very large value) falls back to
// math/big, matching the old renderer's output exactly.
func Append(dst []byte, n decimal128.Num, scale int32) []byte {
	neg := n.Sign() < 0
	abs := n
	if neg {
		// Two's-complement negation; for a positive magnitude the high word is 0
		// iff the magnitude fits in a uint64.
		abs = n.Negate()
	}

	if abs.HighBits() == 0 {
		// Fast path: |unscaled| < 2^64. Render the digits right-to-left into a
		// stack scratch (uint64 is at most 20 decimal digits), no math/big.
		var tmp [20]byte
		mag := abs.LowBits()
		i := len(tmp)
		if mag == 0 {
			i--
			tmp[i] = '0'
		} else {
			for mag > 0 {
				i--
				tmp[i] = byte('0' + mag%10)
				mag /= 10
			}
		}
		return appendPlaced(dst, tmp[i:], neg, scale)
	}

	// Slow path: true 128-bit magnitude. Rare (only DECIMAL values ≥ 2^64), so a
	// math/big allocation here is acceptable; the digit placement is identical.
	mag := new(big.Int).Abs(n.BigInt())
	var tmp [40]byte // 2^127 has 39 decimal digits
	return appendPlaced(dst, mag.Append(tmp[:0], 10), neg, scale)
}

// appendPlaced writes the sign and the magnitude digits into dst with the decimal
// point inserted `scale` places from the right. digits is the magnitude's decimal
// ASCII with no leading zeros ("0" for a zero magnitude). This encodes the exact
// placement rules both backends must agree on; the fast and slow paths share it so
// they can never diverge.
func appendPlaced(dst, digits []byte, neg bool, scale int32) []byte {
	if neg {
		dst = append(dst, '-')
	}
	if scale <= 0 {
		return append(dst, digits...)
	}

	s := int(scale)
	nd := len(digits)
	if nd <= s {
		// Fewer digits than the scale: a single leading integer zero, then enough
		// fractional zeros to reach exactly `scale` places, e.g. 5 at scale 3 ->
		// "0.005".
		dst = append(dst, '0', '.')
		for k := 0; k < s-nd; k++ {
			dst = append(dst, '0')
		}
		return append(dst, digits...)
	}
	// Split the digits `scale` places from the right around the point.
	dst = append(dst, digits[:nd-s]...)
	dst = append(dst, '.')
	return append(dst, digits[nd-s:]...)
}
