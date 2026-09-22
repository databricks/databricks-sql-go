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

func TestClientQueryTimeoutMilliseconds(t *testing.T) {
	maxFinite := time.Duration(MaxClientQueryTimeoutMilliseconds) * time.Millisecond
	maximumDuration := time.Duration(1<<63 - 1)
	for _, tc := range []struct {
		name    string
		timeout time.Duration
		want    uint64
		wantErr bool
	}{
		{"negative", -time.Nanosecond, 0, true},
		{"zero is unlimited", 0, 0, false},
		{"sub-millisecond rounds up", time.Nanosecond, 1, false},
		{"fractional milliseconds round up", 1500 * time.Microsecond, 2, false},
		{"whole milliseconds", 12 * time.Second, 12_000, false},
		{"largest finite value", maxFinite, MaxClientQueryTimeoutMilliseconds, false},
		{"rounded value above largest finite", maxFinite + time.Nanosecond, 0, true},
		{"maximum duration is unlimited", maximumDuration, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ClientQueryTimeoutMilliseconds(tc.timeout)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ClientQueryTimeoutMilliseconds(%v) error = %v, wantErr %v", tc.timeout, err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("ClientQueryTimeoutMilliseconds(%v) = %d, want %d", tc.timeout, got, tc.want)
			}
		})
	}
}

func TestConfiguredClientQueryTimeoutMillisecondsPreservesPresenceAndSnapshots(t *testing.T) {
	if got, err := configuredClientQueryTimeoutMilliseconds(nil); err != nil || got != nil {
		t.Fatalf("omitted timeout = (%v, %v), want (nil, nil)", got, err)
	}

	configured := time.Duration(0)
	got, err := configuredClientQueryTimeoutMilliseconds(&configured)
	if err != nil || got == nil || *got != 0 {
		t.Fatalf("explicit zero = (%v, %v), want pointer to 0", got, err)
	}
	configured = 5 * time.Second
	if *got != 0 {
		t.Errorf("resolved timeout changed after source mutation: got %d, want 0", *got)
	}
}
