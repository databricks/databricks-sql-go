package decimalfmt

import (
	"math"
	"math/big"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v12/arrow/decimal128"
)

// ExactString applies scale by string placement, preserving digits a float64
// would lose beyond ~17 significant figures.
func TestExactString(t *testing.T) {
	cases := []struct {
		unscaled int64
		scale    int32
		want     string
	}{
		{12345, 2, "123.45"},
		{5, 3, "0.005"},
		{100, 0, "100"},
		{1999, 2, "19.99"},
		{-12345, 2, "-123.45"},
		{-5, 3, "-0.005"},
		{0, 2, "0.00"},
	}
	for _, c := range cases {
		got := ExactString(decimal128.FromI64(c.unscaled), c.scale)
		if got != c.want {
			t.Errorf("unscaled=%d scale=%d: got %q want %q", c.unscaled, c.scale, got, c.want)
		}
	}
}

// A value beyond float64's exact integer range must survive intact — the whole
// point of formatting from the 128-bit unscaled integer rather than a float.
func TestExactStringHighPrecision(t *testing.T) {
	// 12345678901234567890 (20 digits) — past float64's 2^53 exact-integer limit,
	// and past uint64's max (18446744073709551615), so it also exercises the
	// 128-bit slow path.
	big20, _ := new(big.Int).SetString("12345678901234567890", 10)
	got := ExactString(decimal128.FromBigInt(big20), 4)
	if want := "1234567890123456.7890"; got != want {
		t.Errorf("got %q want %q", got, want)
	}
}

// exactStringOracle is the pre-optimization implementation, kept verbatim as the
// correctness oracle for the alloc-free rewrite. ExactString is on the SHARED
// Thrift + kernel render path, so its output must stay byte-for-byte identical to
// this reference for every input; TestExactStringOracleParity fuzzes the two
// against each other. Do NOT "simplify" this to call ExactString — it exists
// precisely to be an independent second implementation.
func exactStringOracle(n decimal128.Num, scale int32) string {
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

// TestExactStringOracleParity is the byte-parity gate for the shared render path:
// it asserts the alloc-free ExactString reproduces the old implementation
// (exactStringOracle) exactly across a wide grid — both signs, the full scale
// range, the uint64/128-bit boundary, and true 128-bit magnitudes including the
// DECIMAL(38) extreme. A single divergence here would be a silent DECIMAL
// corruption in BOTH backends.
func TestExactStringOracleParity(t *testing.T) {
	// Magnitudes chosen to straddle every branch: zero, small, the uint64 max
	// boundary (2^64-1 and 2^64), the largest 38-digit decimal, and the in-range
	// 128-bit extreme. decimal128.FromBigInt panics above 2^127-1, so that is the
	// largest fixture (a bare -2^127 would panic).
	mags := []string{
		"0",
		"1",
		"5",
		"9",
		"12345",
		"99999999999999999",                      // 17 nines (fits int64)
		"18446744073709551615",                   // 2^64 - 1  (uint64 max, last fast-path magnitude)
		"18446744073709551616",                   // 2^64      (first 128-bit slow-path magnitude)
		"99999999999999999999999999999999999999", // DECIMAL(38) all-nines max
		"170141183460469231731687303715884105727", // 2^127 - 1 (in-range 128-bit extreme)
	}
	scales := []int32{-1, 0, 1, 2, 3, 6, 17, 18, 37, 38}

	for _, m := range mags {
		bi, ok := new(big.Int).SetString(m, 10)
		if !ok {
			t.Fatalf("bad fixture %q", m)
		}
		for _, sign := range []int{1, -1} {
			v := new(big.Int).Set(bi)
			if sign < 0 {
				if v.Sign() == 0 {
					continue // -0 == 0, already covered
				}
				v.Neg(v)
			}
			num := decimal128.FromBigInt(v)
			for _, sc := range scales {
				got := ExactString(num, sc)
				want := exactStringOracle(num, sc)
				if got != want {
					t.Errorf("mag=%s sign=%d scale=%d: got %q want %q", m, sign, sc, got, want)
				}
			}
		}
	}
}

// TestAppendMatchesExactString proves the reused-buffer Append form produces the
// identical bytes to ExactString (and that appending into a non-empty buffer only
// extends it), so a caller can render many decimals into one scratch/arena buffer
// with no behavioral difference.
func TestAppendMatchesExactString(t *testing.T) {
	nums := []decimal128.Num{
		decimal128.FromI64(0),
		decimal128.FromI64(5),
		decimal128.FromI64(-12345),
		decimal128.FromI64(math.MaxInt64),
		decimal128.FromU64(math.MaxUint64),
	}
	big128, _ := new(big.Int).SetString("123456789012345678901234567890", 10)
	nums = append(nums, decimal128.FromBigInt(big128), decimal128.FromBigInt(big128.Neg(big128)))

	for _, n := range nums {
		for _, sc := range []int32{0, 2, 3, 18, 30} {
			want := ExactString(n, sc)
			// Into an empty buffer.
			if got := string(Append(nil, n, sc)); got != want {
				t.Errorf("Append(nil) n=%v scale=%d: got %q want %q", n, sc, got, want)
			}
			// Into a pre-filled buffer: only the suffix must match, and the prefix
			// must be preserved.
			pre := []byte("PFX|")
			out := string(Append(pre, n, sc))
			if !strings.HasPrefix(out, "PFX|") || strings.TrimPrefix(out, "PFX|") != want {
				t.Errorf("Append(pre) n=%v scale=%d: got %q want prefix+%q", n, sc, out, want)
			}
		}
	}
}

// BenchmarkExactString / BenchmarkExactStringSink / BenchmarkAppendReused document
// the alloc profile the fix targets. Run:
//
//	go test -run '^$' -bench 'ExactString|AppendReused' -benchmem ./internal/decimalfmt/
//
// Read the two ExactString benchmarks together: the discard form (result unused)
// lets escape analysis prove the string never leaves the frame and elides its heap
// allocation, reporting 0 B/0 allocs — which flatters but does not reflect
// production, where every caller keeps the result. The Sink form assigns to a
// package-level var so the string escapes, reporting the real 1-alloc-per-cell
// profile the render path actually pays. Both are still a large win over the old
// renderer (which allocated the big.Int scratch on top of the returned string).
// AppendReused is the genuinely zero-alloc form, available to callers that render
// into a reused buffer (no production caller does today).
func BenchmarkExactString(b *testing.B) {
	n := decimal128.FromI64(1999) // DECIMAL(6,2) 19.99 — the common small case
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = ExactString(n, 2)
	}
}

// benchSink keeps the ExactString result escaping so BenchmarkExactStringSink
// measures the real production alloc profile (see the note above).
var benchSink string

func BenchmarkExactStringSink(b *testing.B) {
	n := decimal128.FromI64(1999)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		benchSink = ExactString(n, 2)
	}
}

func BenchmarkAppendReused(b *testing.B) {
	n := decimal128.FromI64(1999)
	buf := make([]byte, 0, decMaxLen)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf = Append(buf[:0], n, 2)
	}
	_ = buf
}
