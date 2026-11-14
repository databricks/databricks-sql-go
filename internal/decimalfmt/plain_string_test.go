package decimalfmt

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPlainString(t *testing.T) {
	for _, test := range []struct {
		value string
		scale int32
		want  string
	}{
		{"0", 2, "0.00"},
		{"-0E-10", 2, "0.00"},
		{"0E-18", 18, "0.000000000000000000"},
		{"+0E-38", 38, "0.00000000000000000000000000000000000000"},
		{"1.2300", 2, "1.23"},
		{"1E+7", 2, "10000000.00"},
		{"9007199254740993E-2", 2, "90071992547409.93"},
		{"9.9999999999999999999999999999999999999E+37", 0, "99999999999999999999999999999999999999"},
		{"-9.9999999999999999999999999999999999999E+37", 0, "-99999999999999999999999999999999999999"},
		{"-9999999999999999999999999999999999999.9", 1, "-9999999999999999999999999999999999999.9"},
	} {
		t.Run(test.value, func(t *testing.T) {
			got, err := PlainString(test.value, &test.scale)
			require.NoError(t, err)
			require.Equal(t, test.want, got)
		})
	}
}

func TestPlainStringDoesNotRound(t *testing.T) {
	scale := int32(2)
	for _, value := range []string{"1.231", "-1E-3", "1E+38", "1Einvalid", "invalid.1"} {
		t.Run(value, func(t *testing.T) {
			_, err := PlainString(value, &scale)
			require.Error(t, err)
		})
	}
	_, err := PlainString("1E-2147483647", nil)
	require.Error(t, err)
}
