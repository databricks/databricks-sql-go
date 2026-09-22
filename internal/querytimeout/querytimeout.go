// Package querytimeout owns the backend-neutral client query-timeout contract.
package querytimeout

import (
	"fmt"
	"time"
)

// MaxMilliseconds is the largest finite timeout accepted by the kernel C ABI.
// Thrift uses the same ceiling so WithClientQueryTimeout has one validation and
// rounding contract regardless of the selected backend.
const MaxMilliseconds uint64 = 9_223_372_036_854

// StatusRPCGrace is the extra time an in-flight status RPC may use after the
// execution deadline. Only a terminal response returned during this window wins.
const StatusRPCGrace = 5 * time.Second

const unlimited = time.Duration(1<<63 - 1)

// Milliseconds converts a public duration to the common backend wire value.
// Zero and time.Duration's maximum are unlimited; positive fractional
// milliseconds round up so they cannot accidentally become unlimited.
func Milliseconds(timeout time.Duration) (uint64, error) {
	if timeout < 0 {
		return 0, fmt.Errorf("client query timeout must not be negative")
	}
	if timeout == 0 || timeout == unlimited {
		return 0, nil
	}

	milliseconds := uint64(timeout / time.Millisecond)
	if timeout%time.Millisecond != 0 {
		milliseconds++
	}
	if milliseconds > MaxMilliseconds {
		return 0, fmt.Errorf("client query timeout rounds above the maximum of %d ms", MaxMilliseconds)
	}
	return milliseconds, nil
}

// FiniteDuration snapshots a configured option for the Thrift execution path.
// The bool is false for omission and both unlimited sentinels.
func FiniteDuration(timeout *time.Duration) (time.Duration, bool, error) {
	if timeout == nil {
		return 0, false, nil
	}
	milliseconds, err := Milliseconds(*timeout)
	if err != nil {
		return 0, false, err
	}
	if milliseconds == 0 {
		return 0, false, nil
	}
	rounded := *timeout - *timeout%time.Millisecond
	if *timeout%time.Millisecond != 0 {
		rounded += time.Millisecond
	}
	return rounded, true, nil
}
