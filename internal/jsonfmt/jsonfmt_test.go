package jsonfmt

import (
	"encoding/json"
	"math"
	"math/rand"
	"strings"
	"testing"
	"unicode/utf8"
)

// assertParity fails unless Marshal(v) matches json.Marshal(v) byte-for-byte,
// including whether it errors.
func assertParity(t *testing.T, v any) {
	t.Helper()
	want, wantErr := json.Marshal(v)
	got, gotErr := Marshal(v)
	if (wantErr != nil) != (gotErr != nil) || wantErr != nil && gotErr.Error() != wantErr.Error() {
		t.Fatalf("%#v: error mismatch: got %v, want %v", v, gotErr, wantErr)
	}
	if wantErr == nil && string(got) != string(want) {
		t.Fatalf("%#v: got %s, want %s", v, got, want)
	}
}

func TestMarshalMatchesEncodingJSON(t *testing.T) {
	values := []any{
		nil, true, false,
		0, -1, math.MaxInt, math.MinInt,
		int8(math.MinInt8), int8(math.MaxInt8), int16(math.MinInt16), int32(math.MinInt32), int64(math.MinInt64), int64(math.MaxInt64),
		uint8(255), uint16(math.MaxUint16), uint32(math.MaxUint32), uint64(math.MaxUint64),
		0.0, math.Copysign(0, -1), 1.0, -1.5, 3.14, 0.1, 1e20, 1e21, 1e-6, 1e-7, 9.999999e-7, 123456789.123456789,
		math.MaxFloat64, math.SmallestNonzeroFloat64, -math.MaxFloat64,
		float32(0), float32(math.Copysign(0, -1)), float32(3.14), float32(0.1), float32(1e21), float32(1e-6), float32(1e-7),
		float32(math.MaxFloat32), float32(math.SmallestNonzeroFloat32),
		math.NaN(), math.Inf(1), math.Inf(-1), float32(math.NaN()), float32(math.Inf(-1)),
		"", "alpha", "with space", "a\"b", `back\slash`, "<tag>", "a&b", "tab\there", "nl\n", "\x00", "\x7f",
		"héllo", "日本語", "  ", "\xff\xfe", "emoji 😀",
		[]byte("abc"), []byte(nil),
	}
	for _, v := range values {
		assertParity(t, v)
	}
}

// Every single-byte string, so the escape set can't drift from json.Marshal's.
func TestMarshalStringBytes(t *testing.T) {
	for c := 0; c < 256; c++ {
		assertParity(t, string([]byte{byte(c)}))
		assertParity(t, "x"+string([]byte{byte(c)})+"y")
	}
}

// Every rune (surrogates encode as U+FFFD), so the UTF-8 fast path can't drift
// from json.Marshal.
func TestMarshalEveryRune(t *testing.T) {
	for r := rune(0); r <= utf8.MaxRune; r++ {
		assertParity(t, "x"+string(r))
	}
}

// Random mixes of ASCII, multi-byte runes, U+2028/U+2029 and raw invalid bytes.
func TestMarshalRandomUnicodeStrings(t *testing.T) {
	r := rand.New(rand.NewSource(3)) //nolint:gosec // G404: deterministic test inputs, not security
	pieces := []string{"a", "Z", " ", "<", "&", "\"", "\\", "\n", "\x7f"}
	for _, cp := range []rune{0xe9, 0xdf, 0x65e5, 0x1f600, 0x2028, 0x2029, 0xa0, 0xfeff, 0xffff} {
		pieces = append(pieces, string(cp))
	}
	for i := 0; i < 100000; i++ {
		var sb strings.Builder
		for j, n := 0, r.Intn(12); j < n; j++ {
			switch r.Intn(4) {
			case 0:
				var raw [1]byte // possibly invalid UTF-8
				_, _ = r.Read(raw[:])
				sb.Write(raw[:])
			case 1:
				sb.WriteRune(r.Int31n(utf8.MaxRune + 1))
			default:
				sb.WriteString(pieces[r.Intn(len(pieces))])
			}
		}
		assertParity(t, sb.String())
	}
}

// Random bit patterns reach exponent cutoffs, subnormals, and rounding cases a
// hand-picked list misses.
func TestMarshalRandomFloats(t *testing.T) {
	r := rand.New(rand.NewSource(1)) //nolint:gosec // G404: deterministic test inputs, not security
	for i := 0; i < 200000; i++ {
		assertParity(t, math.Float64frombits(r.Uint64()))
		assertParity(t, math.Float32frombits(r.Uint32()))
		assertParity(t, r.NormFloat64()*math.Pow(10, float64(r.Intn(50)-25)))
		assertParity(t, float32(r.NormFloat64()*math.Pow(10, float64(r.Intn(30)-15))))
	}
}

func TestAppendKeepsPrefix(t *testing.T) {
	got, err := Append([]byte("["), int32(7))
	if err != nil || string(got) != "[7" {
		t.Fatalf("got %q, %v", got, err)
	}
	got, err = Append([]byte("["), []byte("abc"))
	if err != nil || string(got) != `["YWJj"` {
		t.Fatalf("got %q, %v", got, err)
	}
}

func BenchmarkMarshalScalars(b *testing.B) {
	// The last three take json.Marshal's fallback.
	values := []any{int32(42), int64(-9000), 3.14, 0.30000000000000004, float32(0.5), true, "alpha0", "café crème",
		`a"b`, []byte("abc"), math.NaN()}
	b.Run("jsonfmt", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for _, v := range values {
				_, _ = Marshal(v)
			}
		}
	})
	b.Run("encoding_json", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for _, v := range values {
				_, _ = json.Marshal(v)
			}
		}
	})
}
