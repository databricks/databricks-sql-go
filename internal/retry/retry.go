// Package retry provides shared HTTP retry/backoff helpers for transient
// object-storage failures (S3 SlowDown, 5xx, etc.). Used by the CloudFetch
// download path and the staging-operation (PUT/GET/REMOVE) handlers so both
// share a single retryable-status set and backoff schedule.
package retry

import (
	"math"
	"math/rand"
	"net/http"
	"strconv"
	"time"
)

// RetryableStatuses lists HTTP status codes from object storage that indicate
// transient conditions and warrant a retry. Mirrors AWS S3 guidance for
// SlowDown (503) / InternalError (500) plus the general 408/429/502/504.
var RetryableStatuses = map[int]struct{}{
	http.StatusRequestTimeout:      {}, // 408
	http.StatusTooManyRequests:     {}, // 429
	http.StatusInternalServerError: {}, // 500
	http.StatusBadGateway:          {}, // 502
	http.StatusServiceUnavailable:  {}, // 503
	http.StatusGatewayTimeout:      {}, // 504
}

// IsRetryableStatus reports whether the given HTTP status code is a transient
// object-storage failure that warrants a retry.
func IsRetryableStatus(status int) bool {
	_, ok := RetryableStatuses[status]
	return ok
}

// Backoff returns the wait before retry attempt N (1-based). The base delay
// is exponential — waitMin * 2^(attempt-1) capped at waitMax — with equal
// jitter applied: the actual sleep is uniformly distributed in
// [base/2, base]. Equal jitter (rather than no jitter) spreads synchronized
// retries across concurrent callers, which would otherwise hammer the storage
// endpoint in lockstep after a region-wide blip. If the server returned a
// parseable integer Retry-After header, that value (in seconds) is honored
// instead, capped at waitMax. HTTP-date Retry-After values are ignored —
// same as the Thrift client's backoff.
func Backoff(attempt int, waitMin, waitMax time.Duration, retryAfter string) time.Duration {
	if retryAfter != "" {
		if secs, err := strconv.ParseInt(retryAfter, 10, 64); err == nil && secs >= 0 {
			d := time.Duration(secs) * time.Second
			if d > waitMax {
				return waitMax
			}
			return d
		}
	}

	expo := float64(waitMin) * math.Pow(2, float64(attempt-1))
	if expo > float64(waitMax) || math.IsInf(expo, 0) {
		expo = float64(waitMax)
	}
	base := time.Duration(expo)
	if base <= 0 {
		return 0
	}
	half := base / 2
	if half <= 0 {
		return base
	}
	return half + time.Duration(rand.Int63n(int64(half))) //nolint:gosec // G404: jitter only, non-cryptographic
}
