package kernel

import "testing"

func TestNormalizeAffectedRows(t *testing.T) {
	cases := []struct {
		name          string
		in            int64
		reportUnknown bool
		want          int64
	}{
		{"ddl or select sentinel (-1) folds to Thrift's 0", -1, false, 0},
		{"any negative folds to 0", -42, false, 0},
		{"zero real count is preserved", 0, false, 0},
		{"positive DML count passes through", 7, false, 7},
		{"large DML count passes through", 1_000_000, false, 1_000_000},

		{"report unknown: sentinel (-1) stays -1", -1, true, -1},
		{"report unknown: any negative maps to -1", -42, true, -1},
		{"report unknown: zero real count is preserved", 0, true, 0},
		{"report unknown: positive DML count passes through", 7, true, 7},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := normalizeAffectedRows(tc.in, tc.reportUnknown); got != tc.want {
				t.Errorf("normalizeAffectedRows(%d, %v) = %d, want %d", tc.in, tc.reportUnknown, got, tc.want)
			}
		})
	}
}
