package decimalfmt

import (
	"math/big"
	"runtime"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/stretchr/testify/require"
)

func TestArenaExactStringParity(test *testing.T) {
	var arena Arena
	for precision := 1; precision <= 38; precision++ {
		maximum, ok := new(big.Int).SetString(strings.Repeat("9", precision), 10)
		require.True(test, ok)
		for _, magnitude := range []*big.Int{big.NewInt(0), big.NewInt(1), maximum} {
			for _, sign := range []int64{1, -1} {
				number := decimal128.FromBigInt(new(big.Int).Mul(magnitude, big.NewInt(sign)))
				for scale := int32(0); scale <= int32(precision); scale++ {
					require.Equal(test, ExactString(number, scale), arena.ExactString(number, scale), "precision=%d scale=%d number=%v", precision, scale, number)
				}
			}
		}
	}
	for _, text := range []string{"18446744073709551615", "18446744073709551616", "170141183460469231731687303715884105727", "-170141183460469231731687303715884105728"} {
		magnitude, ok := new(big.Int).SetString(text, 10)
		require.True(test, ok)
		var number decimal128.Num
		if text[0] == '-' {
			number = decimal128.New(-1<<63, 0)
		} else {
			number = decimal128.FromBigInt(magnitude)
		}
		for _, scale := range []int32{-1, 0, 1, 38, 64, 40000} {
			require.Equal(test, ExactString(number, scale), arena.ExactString(number, scale))
		}
	}
}

func TestArenaStringsSurviveRolloverAndReset(test *testing.T) {
	var arena Arena
	stringsKept := make([]string, 20000)
	for index := range stringsKept {
		stringsKept[index] = arena.ExactString(decimal128.FromI64(int64(index)-10000), 38)
		require.Equal(test, arenaChunkSize, arena.buffer.Cap())
		require.LessOrEqual(test, arena.buffer.Len(), arenaChunkSize)
	}
	arena.Reset()
	require.Zero(test, arena.buffer.Cap())
	require.Zero(test, arena.buffer.Len())
	for index := 0; index < 20000; index++ {
		_ = arena.ExactString(decimal128.FromI64(-1), 38)
	}
	arena.Reset()
	runtime.GC()
	for index, value := range stringsKept {
		require.Equal(test, ExactString(decimal128.FromI64(int64(index)-10000), 38), value)
	}
}

func TestArenaAmortizesAllocations(test *testing.T) {
	var arena Arena
	allocations := testing.AllocsPerRun(10000, func() {
		benchSink = arena.ExactString(decimal128.FromI64(-12345), 2)
	})
	require.Less(test, allocations, 0.01)
}
