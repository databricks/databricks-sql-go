package kernel

import (
	"testing"
	"time"
)

func TestRequestTimeoutMilliseconds(t *testing.T) {
	for _, tc := range []struct {
		name    string
		timeout time.Duration
		want    int64
	}{
		{"zero", 0, 0},
		{"negative", -time.Second, 0},
		{"sub-millisecond", time.Nanosecond, 1},
		{"fractional milliseconds", 1500 * time.Microsecond, 1},
		{"seconds", 12 * time.Second, 12_000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := requestTimeoutMilliseconds(tc.timeout); got != tc.want {
				t.Errorf("requestTimeoutMilliseconds(%v) = %d, want %d", tc.timeout, got, tc.want)
			}
		})
	}
}

func TestEffectiveRequestTimeoutMilliseconds(t *testing.T) {
	for _, tc := range []struct {
		name    string
		timeout time.Duration
		want    int64
	}{
		{"zero uses kernel default", 0, 120_000},
		{"negative uses kernel default", -time.Second, 120_000},
		{"sub-millisecond", time.Nanosecond, 1},
		{"seconds", 12 * time.Second, 12_000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := EffectiveRequestTimeoutMilliseconds(tc.timeout); got != tc.want {
				t.Errorf("EffectiveRequestTimeoutMilliseconds(%v) = %d, want %d", tc.timeout, got, tc.want)
			}
		})
	}
}
