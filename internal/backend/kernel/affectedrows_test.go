package kernel

import "testing"

func TestNormalizeAffectedRows(t *testing.T) {
	cases := []struct {
		name string
		in   int64
		want int64
	}{
		{"ddl or select sentinel (-1) folds to Thrift's 0", -1, 0},
		{"any negative folds to 0", -42, 0},
		{"zero real count is preserved", 0, 0},
		{"positive DML count passes through", 7, 7},
		{"large DML count passes through", 1_000_000, 1_000_000},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := normalizeAffectedRows(tc.in); got != tc.want {
				t.Errorf("normalizeAffectedRows(%d) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}
