package retry

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBackoff(t *testing.T) {
	t.Run("retry-after integer seconds is honored", func(t *testing.T) {
		got := Backoff(1, 100*time.Millisecond, 60*time.Second, "2")
		assert.Equal(t, 2*time.Second, got)
	})

	t.Run("retry-after is capped at waitMax", func(t *testing.T) {
		got := Backoff(1, 100*time.Millisecond, 1*time.Second, "100")
		assert.Equal(t, 1*time.Second, got)
	})

	t.Run("retry-after http-date is ignored, falls back to exponential", func(t *testing.T) {
		minWait := 100 * time.Millisecond
		got := Backoff(1, minWait, 10*time.Second, "Tue, 15 Nov 1994 08:12:31 GMT")
		// attempt=1 base = minWait; equal jitter in [minWait/2, minWait]
		assert.GreaterOrEqual(t, got, minWait/2)
		assert.LessOrEqual(t, got, minWait)
	})

	t.Run("exponential is capped at waitMax", func(t *testing.T) {
		maxWait := 200 * time.Millisecond
		// 100ms * 2^9 = 51200ms, capped at 200ms; equal jitter -> [100ms, 200ms]
		for i := 0; i < 50; i++ {
			got := Backoff(10, 100*time.Millisecond, maxWait, "")
			assert.GreaterOrEqual(t, got, maxWait/2)
			assert.LessOrEqual(t, got, maxWait)
		}
	})

	t.Run("base grows exponentially with attempt", func(t *testing.T) {
		minWait, maxWait := 100*time.Millisecond, 10*time.Second
		// attempt=1 -> base 100ms,  jitter [50ms,100ms]
		// attempt=3 -> base 400ms,  jitter [200ms,400ms]
		for i := 0; i < 50; i++ {
			got1 := Backoff(1, minWait, maxWait, "")
			got3 := Backoff(3, minWait, maxWait, "")
			assert.GreaterOrEqual(t, got1, 50*time.Millisecond)
			assert.LessOrEqual(t, got1, 100*time.Millisecond)
			assert.GreaterOrEqual(t, got3, 200*time.Millisecond)
			assert.LessOrEqual(t, got3, 400*time.Millisecond)
		}
	})

	t.Run("zero waitMin returns zero", func(t *testing.T) {
		got := Backoff(1, 0, 0, "")
		assert.Equal(t, time.Duration(0), got)
	})
}

func TestIsRetryableStatus(t *testing.T) {
	retryable := []int{408, 429, 500, 502, 503, 504}
	notRetryable := []int{200, 201, 301, 302, 400, 401, 403, 404, 409, 410, 501}

	for _, s := range retryable {
		assert.True(t, IsRetryableStatus(s), "%d should be retryable", s)
	}
	for _, s := range notRetryable {
		assert.False(t, IsRetryableStatus(s), "%d should not be retryable", s)
	}
}
