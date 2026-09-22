package querytimeout

import (
	"testing"
	"time"
)

func TestMilliseconds(t *testing.T) {
	tests := []struct {
		name    string
		value   time.Duration
		want    uint64
		wantErr bool
	}{
		{name: "zero is unlimited", value: 0, want: 0},
		{name: "maximum duration is unlimited", value: time.Duration(1<<63 - 1), want: 0},
		{name: "fraction rounds up", value: time.Millisecond + time.Nanosecond, want: 2},
		{name: "negative is invalid", value: -time.Nanosecond, wantErr: true},
		{name: "above maximum is invalid", value: time.Duration(MaxMilliseconds)*time.Millisecond + time.Nanosecond, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := Milliseconds(test.value)
			if (err != nil) != test.wantErr {
				t.Fatalf("Milliseconds(%v) error = %v, wantErr %v", test.value, err, test.wantErr)
			}
			if got != test.want {
				t.Errorf("Milliseconds(%v) = %d, want %d", test.value, got, test.want)
			}
		})
	}
}

func TestFiniteDuration(t *testing.T) {
	for _, value := range []time.Duration{0, time.Duration(1<<63 - 1)} {
		value := value
		if _, finite, err := FiniteDuration(&value); err != nil || finite {
			t.Fatalf("FiniteDuration(%v) = finite %v, error %v; want unlimited", value, finite, err)
		}
	}

	value := time.Millisecond + time.Nanosecond
	got, finite, err := FiniteDuration(&value)
	if err != nil || !finite || got != 2*time.Millisecond {
		t.Fatalf("FiniteDuration(%v) = %v, %v, %v; want 2ms, true, nil", value, got, finite, err)
	}

	value = time.Duration(MaxMilliseconds) * time.Millisecond
	got, finite, err = FiniteDuration(&value)
	if err != nil || !finite || got != value {
		t.Fatalf("FiniteDuration(maximum) = %v, %v, %v; want %v, true, nil", got, finite, err, value)
	}
}
