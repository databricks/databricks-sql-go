package decimalfmt

import (
	"math/big"
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
	// 12345678901234567890 (20 digits) — past float64's 2^53 exact-integer limit.
	big20, _ := new(big.Int).SetString("12345678901234567890", 10)
	got := ExactString(decimal128.FromBigInt(big20), 4)
	if want := "1234567890123456.7890"; got != want {
		t.Errorf("got %q want %q", got, want)
	}
}
