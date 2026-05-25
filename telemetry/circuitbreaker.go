package telemetry

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// circuitState represents the state of the circuit breaker.
type circuitState int32

const (
	stateClosed circuitState = iota
	stateOpen
	stateHalfOpen
)

// callResult represents the result of a call (success or failure).
type callResult bool

const (
	callSuccess callResult = true
	callFailure callResult = false
)

// circuitBreaker implements the circuit breaker pattern.
// It protects against failing telemetry endpoints by tracking failures
// using a sliding window and failure rate percentage.
//
// State transitions:
// - Closed → Open: When failure rate exceeds threshold after minimum calls
// - Open → Half-Open: After wait duration
// - Half-Open → Closed: After successful test calls
// - Half-Open → Open: On any failure in half-open state
type circuitBreaker struct {
	mu sync.RWMutex

	state         atomic.Int32 // circuitState
	lastStateTime time.Time

	// Sliding window for tracking calls
	window       []callResult
	windowIndex  int
	windowFilled bool
	totalCalls   int
	failureCount int

	// Half-open state tracking
	halfOpenSuccesses int

	// retryAfterHint, if non-zero, overrides waitDurationInOpenState for
	// the current open-state interval. Populated from server-provided
	// Retry-After headers on 429 responses; cleared when the breaker
	// transitions out of Open (to half-open after the wait elapses, or
	// to closed via half-open success).
	retryAfterHint time.Duration

	config circuitBreakerConfig
}

// circuitBreakerConfig holds circuit breaker configuration.
type circuitBreakerConfig struct {
	failureRateThreshold     int           // Open if failure rate >= this percentage (0-100)
	minimumNumberOfCalls     int           // Minimum calls before evaluating failure rate
	slidingWindowSize        int           // Number of recent calls to track
	waitDurationInOpenState  time.Duration // Wait before transitioning to half-open
	permittedCallsInHalfOpen int           // Number of test calls in half-open state
}

// defaultCircuitBreakerConfig returns default configuration.
//
// Each export call is exactly one HTTP transaction (the exporter sets
// WithSkipTransientRetries so the retryablehttp layer does not loop on
// 429/503), which means each breaker call corresponds to one observed
// outcome from the server.
//
// Detection-time math at default cadence (FlushInterval = 30s in
// config.go): minimumNumberOfCalls=10 means we need ~10 flush intervals
// of data before the failure-rate gate fires — i.e. up to ~5 minutes of
// 100%-failing exports before the breaker opens. That is intentionally
// slow on the trip side so a transient blip on a low-traffic client
// doesn't trigger a 60s blackout; once the breaker is open, the longer
// minimum-evaluation window matters less because the open-state interval
// (60s, or Retry-After if larger) controls the next probe.
//
// slidingWindowSize=30 is deliberately larger than minimumNumberOfCalls:
// once enough calls accumulate, the failure-rate evaluation runs over up
// to the last 30 outcomes, giving recent successes time to offset
// transient failures. The two numbers tune different things —
// minimumNumberOfCalls gates when we start evaluating; slidingWindowSize
// caps how many outcomes participate in that evaluation.
func defaultCircuitBreakerConfig() circuitBreakerConfig {
	return circuitBreakerConfig{
		failureRateThreshold:     50,
		minimumNumberOfCalls:     10,
		slidingWindowSize:        30,
		waitDurationInOpenState:  60 * time.Second,
		permittedCallsInHalfOpen: 3,
	}
}

// newCircuitBreaker creates a new circuit breaker.
func newCircuitBreaker(cfg circuitBreakerConfig) *circuitBreaker {
	cb := &circuitBreaker{
		config:        cfg,
		lastStateTime: time.Now(),
		window:        make([]callResult, cfg.slidingWindowSize),
	}
	cb.state.Store(int32(stateClosed))
	return cb
}

// ErrCircuitOpen is returned when circuit is open.
var ErrCircuitOpen = errors.New("circuit breaker is open")

// execute executes the function if circuit allows.
// Returns ErrCircuitOpen if the circuit is open and the wait duration hasn't elapsed.
func (cb *circuitBreaker) execute(ctx context.Context, fn func() error) error {
	state := circuitState(cb.state.Load())

	switch state {
	case stateOpen:
		// Check if wait duration has passed. If the server hinted a
		// Retry-After on the failure that opened the breaker, honor the
		// larger of (configured wait, server hint).
		cb.mu.Lock()
		wait := cb.config.waitDurationInOpenState
		if cb.retryAfterHint > wait {
			wait = cb.retryAfterHint
		}
		shouldRetry := time.Since(cb.lastStateTime) > wait
		if shouldRetry {
			// The wait window the hint asked for has elapsed — clear it
			// so a later reopen (e.g. half-open failure with no new hint)
			// doesn't extend itself by a stale duration.
			cb.retryAfterHint = 0
			cb.setStateUnlocked(stateHalfOpen)
		}
		cb.mu.Unlock()

		if shouldRetry {
			return cb.tryExecute(ctx, fn)
		}
		return ErrCircuitOpen

	case stateHalfOpen:
		return cb.tryExecute(ctx, fn)

	case stateClosed:
		return cb.tryExecute(ctx, fn)
	}

	return nil
}

// tryExecute attempts to execute the function and updates state.
func (cb *circuitBreaker) tryExecute(ctx context.Context, fn func() error) error {
	err := fn()

	if err != nil {
		cb.recordCall(callFailure)
		return err
	}

	cb.recordCall(callSuccess)
	return nil
}

// recordCall records a call result in the sliding window and evaluates state transitions.
func (cb *circuitBreaker) recordCall(result callResult) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	state := circuitState(cb.state.Load())

	// Handle half-open state specially
	if state == stateHalfOpen {
		if result == callFailure {
			// Any failure in half-open immediately reopens circuit
			cb.resetWindowUnlocked()
			cb.setStateUnlocked(stateOpen)
			return
		}

		cb.halfOpenSuccesses++
		if cb.halfOpenSuccesses >= cb.config.permittedCallsInHalfOpen {
			// Enough successes to close circuit. Drop any stale Retry-After
			// hint so it doesn't extend a future open interval after the
			// server has recovered.
			cb.resetWindowUnlocked()
			cb.retryAfterHint = 0
			cb.setStateUnlocked(stateClosed)
		}
		return
	}

	// Record in sliding window
	// Remove old value from count if window is full
	if cb.windowFilled && cb.window[cb.windowIndex] == callFailure {
		cb.failureCount--
	}

	// Add new value
	cb.window[cb.windowIndex] = result
	if result == callFailure {
		cb.failureCount++
	}

	// Move to next position
	cb.windowIndex = (cb.windowIndex + 1) % cb.config.slidingWindowSize
	if cb.windowIndex == 0 {
		cb.windowFilled = true
	}

	cb.totalCalls++

	// Evaluate if we should open the circuit
	if state == stateClosed {
		cb.evaluateStateUnlocked()
	}
}

// evaluateStateUnlocked checks if the circuit should open based on failure rate.
// Caller must hold cb.mu lock.
func (cb *circuitBreaker) evaluateStateUnlocked() {
	// Need minimum number of calls before evaluating
	windowSize := cb.totalCalls
	if cb.windowFilled {
		windowSize = cb.config.slidingWindowSize
	}

	if windowSize < cb.config.minimumNumberOfCalls {
		return
	}

	// Calculate failure rate
	failureRate := (cb.failureCount * 100) / windowSize

	if failureRate >= cb.config.failureRateThreshold {
		cb.setStateUnlocked(stateOpen)
	}
}

// resetWindowUnlocked clears the sliding window.
// Caller must hold cb.mu lock.
func (cb *circuitBreaker) resetWindowUnlocked() {
	cb.windowIndex = 0
	cb.windowFilled = false
	cb.totalCalls = 0
	cb.failureCount = 0
	cb.halfOpenSuccesses = 0
}

// extendOpenStateAtLeast records a server-provided Retry-After hint. While
// the breaker is open, it will stay open for at least the given duration
// (and at least waitDurationInOpenState). The hint is cleared when the
// breaker transitions out of Open.
//
// Safe to call concurrently from any caller (the exporter parses
// Retry-After on 429 responses and forwards it here).
func (cb *circuitBreaker) extendOpenStateAtLeast(d time.Duration) {
	if d <= 0 {
		return
	}
	cb.mu.Lock()
	if d > cb.retryAfterHint {
		cb.retryAfterHint = d
	}
	cb.mu.Unlock()
}

// setStateUnlocked transitions to a new state without locking.
// Caller must hold cb.mu lock.
func (cb *circuitBreaker) setStateUnlocked(newState circuitState) {
	oldState := circuitState(cb.state.Load())
	if oldState == newState {
		return
	}

	cb.state.Store(int32(newState))
	cb.lastStateTime = time.Now()

	// Log state transition at DEBUG level
	// logger.Debug().Msgf("circuit breaker: %v -> %v", oldState, newState)
}

// getState returns the current state (for testing).
func (cb *circuitBreaker) getState() circuitState {
	return circuitState(cb.state.Load())
}

// getFailureRate returns the current failure rate percentage (for testing).
func (cb *circuitBreaker) getFailureRate() int {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	windowSize := cb.totalCalls
	if cb.windowFilled {
		windowSize = cb.config.slidingWindowSize
	}

	if windowSize == 0 {
		return 0
	}

	return (cb.failureCount * 100) / windowSize
}

// circuitBreakerManager manages circuit breakers per host.
// Each host gets its own circuit breaker to provide isolation.
type circuitBreakerManager struct {
	mu       sync.RWMutex
	breakers map[string]*circuitBreaker
}

var (
	breakerManagerOnce     sync.Once
	breakerManagerInstance *circuitBreakerManager
)

// getCircuitBreakerManager returns the singleton instance.
func getCircuitBreakerManager() *circuitBreakerManager {
	breakerManagerOnce.Do(func() {
		breakerManagerInstance = &circuitBreakerManager{
			breakers: make(map[string]*circuitBreaker),
		}
	})
	return breakerManagerInstance
}

// getCircuitBreaker gets or creates a circuit breaker for the host.
// Thread-safe for concurrent access.
//
// The host is normalized (scheme stripped, lowercased, trailing slash trimmed)
// so DSN variants ("example.com", "example.com/", "https://example.com")
// share a single breaker per logical host instead of fragmenting trip state.
func (m *circuitBreakerManager) getCircuitBreaker(host string) *circuitBreaker {
	key := normalizeHostKey(host)

	m.mu.RLock()
	cb, exists := m.breakers[key]
	m.mu.RUnlock()

	if exists {
		return cb
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check after acquiring write lock
	if cb, exists = m.breakers[key]; exists {
		return cb
	}

	cb = newCircuitBreaker(defaultCircuitBreakerConfig())
	m.breakers[key] = cb
	return cb
}
