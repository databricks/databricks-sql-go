package telemetry

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestCircuitBreaker_InitialState(t *testing.T) {
	cfg := defaultCircuitBreakerConfig()
	cb := newCircuitBreaker(cfg)

	if cb.getState() != stateClosed {
		t.Errorf("Expected initial state to be Closed, got %v", cb.getState())
	}
}

func TestCircuitBreaker_ClosedToOpen_FailureRate(t *testing.T) {
	cfg := circuitBreakerConfig{
		failureRateThreshold:     50, // 50% failure rate
		minimumNumberOfCalls:     10, // Lower minimum for testing
		slidingWindowSize:        20,
		waitDurationInOpenState:  1 * time.Second,
		permittedCallsInHalfOpen: 2,
	}
	cb := newCircuitBreaker(cfg)

	ctx := context.Background()
	failFunc := func() error {
		return errors.New("test error")
	}
	successFunc := func() error {
		return nil
	}

	// Execute 10 calls: 6 failures (60% failure rate) should open circuit
	for i := 0; i < 6; i++ {
		cb.execute(ctx, failFunc)
	}
	for i := 0; i < 4; i++ {
		cb.execute(ctx, successFunc)
	}

	// Circuit should be open (60% > 50% threshold)
	if cb.getState() != stateOpen {
		t.Errorf("Expected state to be Open after 60%% failure rate, got %v (failure rate: %d%%)", cb.getState(), cb.getFailureRate())
	}
}

func TestCircuitBreaker_MinimumCallsRequired(t *testing.T) {
	cfg := circuitBreakerConfig{
		failureRateThreshold:     50,
		minimumNumberOfCalls:     20,
		slidingWindowSize:        30,
		waitDurationInOpenState:  1 * time.Second,
		permittedCallsInHalfOpen: 2,
	}
	cb := newCircuitBreaker(cfg)

	ctx := context.Background()
	failFunc := func() error {
		return errors.New("test error")
	}

	// Execute 10 failures (less than minimum)
	for i := 0; i < 10; i++ {
		cb.execute(ctx, failFunc)
	}

	// Circuit should still be closed (not enough calls)
	if cb.getState() != stateClosed {
		t.Errorf("Expected state to remain Closed with insufficient calls, got %v", cb.getState())
	}

	// Execute 10 more failures (now 20 total, 100% failure rate)
	for i := 0; i < 10; i++ {
		cb.execute(ctx, failFunc)
	}

	// Now circuit should be open
	if cb.getState() != stateOpen {
		t.Errorf("Expected state to be Open after minimum calls with 100%% failure rate, got %v", cb.getState())
	}
}

func TestCircuitBreaker_SlidingWindow(t *testing.T) {
	cfg := circuitBreakerConfig{
		failureRateThreshold:     50,
		minimumNumberOfCalls:     10,
		slidingWindowSize:        10, // Small window for testing
		waitDurationInOpenState:  1 * time.Second,
		permittedCallsInHalfOpen: 2,
	}
	cb := newCircuitBreaker(cfg)

	ctx := context.Background()
	failFunc := func() error {
		return errors.New("test error")
	}
	successFunc := func() error {
		return nil
	}

	// Fill window with 10 failures (100% failure rate)
	for i := 0; i < 10; i++ {
		cb.execute(ctx, failFunc)
	}

	if cb.getState() != stateOpen {
		t.Fatalf("Expected circuit to be Open, got %v", cb.getState())
	}

	// Wait and transition to half-open
	time.Sleep(cfg.waitDurationInOpenState + 50*time.Millisecond)

	// Successful call to move to half-open
	cb.execute(ctx, successFunc)

	if cb.getState() != stateHalfOpen {
		t.Fatalf("Expected state to be HalfOpen, got %v", cb.getState())
	}

	// One more success should close it (2 successes needed)
	cb.execute(ctx, successFunc)

	if cb.getState() != stateClosed {
		t.Errorf("Expected state to be Closed after half-open successes, got %v", cb.getState())
	}

	// Window should be reset - now add 10 successes
	for i := 0; i < 10; i++ {
		cb.execute(ctx, successFunc)
	}

	// Should remain closed (0% failure rate)
	if cb.getState() != stateClosed {
		t.Errorf("Expected state to remain Closed with all successes, got %v", cb.getState())
	}
}

func TestCircuitBreaker_OpenRejectsRequests(t *testing.T) {
	cfg := circuitBreakerConfig{
		failureRateThreshold:     50,
		minimumNumberOfCalls:     10,
		slidingWindowSize:        20,
		waitDurationInOpenState:  1 * time.Hour, // Long wait so it stays open
		permittedCallsInHalfOpen: 2,
	}
	cb := newCircuitBreaker(cfg)

	ctx := context.Background()
	failFunc := func() error {
		return errors.New("test error")
	}

	// Open the circuit (10 failures = 100% failure rate)
	for i := 0; i < 10; i++ {
		cb.execute(ctx, failFunc)
	}

	if cb.getState() != stateOpen {
		t.Fatalf("Expected circuit to be Open, got %v", cb.getState())
	}

	// Circuit is open, should reject immediately
	err := cb.execute(ctx, func() error {
		t.Fatal("Function should not be executed when circuit is open")
		return nil
	})

	if err != ErrCircuitOpen {
		t.Errorf("Expected ErrCircuitOpen, got %v", err)
	}
}

func TestCircuitBreaker_OpenToHalfOpen(t *testing.T) {
	cfg := circuitBreakerConfig{
		failureRateThreshold:     50,
		minimumNumberOfCalls:     10,
		slidingWindowSize:        20,
		waitDurationInOpenState:  100 * time.Millisecond,
		permittedCallsInHalfOpen: 2,
	}
	cb := newCircuitBreaker(cfg)

	ctx := context.Background()
	failFunc := func() error {
		return errors.New("test error")
	}

	// Open the circuit
	for i := 0; i < 10; i++ {
		cb.execute(ctx, failFunc)
	}

	if cb.getState() != stateOpen {
		t.Fatalf("Expected state to be Open, got %v", cb.getState())
	}

	// Wait for wait duration
	time.Sleep(cfg.waitDurationInOpenState + 50*time.Millisecond)

	// Next request should transition to half-open
	successFunc := func() error {
		return nil
	}
	err := cb.execute(ctx, successFunc)
	if err != nil {
		t.Errorf("Expected no error in half-open state, got %v", err)
	}

	// Should be in half-open state
	if cb.getState() != stateHalfOpen {
		t.Errorf("Expected state to be HalfOpen, got %v", cb.getState())
	}
}

func TestCircuitBreaker_HalfOpenToClosed(t *testing.T) {
	cfg := circuitBreakerConfig{
		failureRateThreshold:     50,
		minimumNumberOfCalls:     10,
		slidingWindowSize:        20,
		waitDurationInOpenState:  100 * time.Millisecond,
		permittedCallsInHalfOpen: 3, // Need 3 successes
	}
	cb := newCircuitBreaker(cfg)

	ctx := context.Background()
	failFunc := func() error {
		return errors.New("test error")
	}
	successFunc := func() error {
		return nil
	}

	// Open the circuit
	for i := 0; i < 10; i++ {
		cb.execute(ctx, failFunc)
	}

	// Wait for wait duration
	time.Sleep(cfg.waitDurationInOpenState + 50*time.Millisecond)

	// Execute 3 successes to close
	for i := 0; i < cfg.permittedCallsInHalfOpen; i++ {
		err := cb.execute(ctx, successFunc)
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
	}

	// Circuit should now be closed
	if cb.getState() != stateClosed {
		t.Errorf("Expected state to be Closed after %d successes, got %v", cfg.permittedCallsInHalfOpen, cb.getState())
	}
}

func TestCircuitBreaker_HalfOpenToOpen(t *testing.T) {
	cfg := circuitBreakerConfig{
		failureRateThreshold:     50,
		minimumNumberOfCalls:     10,
		slidingWindowSize:        20,
		waitDurationInOpenState:  100 * time.Millisecond,
		permittedCallsInHalfOpen: 3,
	}
	cb := newCircuitBreaker(cfg)

	ctx := context.Background()
	failFunc := func() error {
		return errors.New("test error")
	}

	// Open the circuit
	for i := 0; i < 10; i++ {
		cb.execute(ctx, failFunc)
	}

	// Wait for wait duration
	time.Sleep(cfg.waitDurationInOpenState + 50*time.Millisecond)

	// First request transitions to half-open, but fails
	err := cb.execute(ctx, failFunc)
	if err == nil {
		t.Errorf("Expected error, got nil")
	}

	// Circuit should immediately reopen on failure in half-open
	if cb.getState() != stateOpen {
		t.Errorf("Expected state to be Open after failure in HalfOpen, got %v", cb.getState())
	}
}

func TestCircuitBreaker_SuccessInClosed(t *testing.T) {
	cfg := defaultCircuitBreakerConfig()
	cb := newCircuitBreaker(cfg)

	ctx := context.Background()
	successFunc := func() error {
		return nil
	}

	// Execute successful requests
	for i := 0; i < 50; i++ {
		err := cb.execute(ctx, successFunc)
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
	}

	// Should remain closed (0% failure rate)
	if cb.getState() != stateClosed {
		t.Errorf("Expected state to remain Closed, got %v", cb.getState())
	}

	// Failure rate should be 0%
	if cb.getFailureRate() != 0 {
		t.Errorf("Expected 0%% failure rate, got %d%%", cb.getFailureRate())
	}
}

func TestCircuitBreaker_ConcurrentAccess(t *testing.T) {
	cfg := circuitBreakerConfig{
		failureRateThreshold:     50,
		minimumNumberOfCalls:     20,
		slidingWindowSize:        50,
		waitDurationInOpenState:  100 * time.Millisecond,
		permittedCallsInHalfOpen: 3,
	}
	cb := newCircuitBreaker(cfg)

	ctx := context.Background()
	var wg sync.WaitGroup

	// Launch many concurrent goroutines that all fail
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cb.execute(ctx, func() error {
				return errors.New("test error")
			})
		}()
	}

	wg.Wait()

	// Circuit should have opened due to high failure rate
	if cb.getState() != stateOpen {
		t.Errorf("Expected circuit to be Open after concurrent failures, got %v (failure rate: %d%%)", cb.getState(), cb.getFailureRate())
	}
}

func TestCircuitBreakerManager_Singleton(t *testing.T) {
	mgr1 := getCircuitBreakerManager()
	mgr2 := getCircuitBreakerManager()

	if mgr1 != mgr2 {
		t.Errorf("Expected singleton instance, got different instances")
	}
}

func TestCircuitBreakerManager_PerHostIsolation(t *testing.T) {
	mgr := getCircuitBreakerManager()

	host1 := "host1.example.com"
	host2 := "host2.example.com"

	cb1 := mgr.getCircuitBreaker(host1)
	cb2 := mgr.getCircuitBreaker(host2)

	if cb1 == cb2 {
		t.Errorf("Expected different circuit breakers for different hosts")
	}

	// Same host should return same circuit breaker
	cb1Again := mgr.getCircuitBreaker(host1)
	if cb1 != cb1Again {
		t.Errorf("Expected same circuit breaker for same host")
	}
}

func TestCircuitBreakerManager_ConcurrentAccess(t *testing.T) {
	mgr := getCircuitBreakerManager()
	var wg sync.WaitGroup

	// Use unique host names to avoid conflicts with other tests
	hosts := []string{"sliding-host1", "sliding-host2", "sliding-host3"}

	// Count how many breakers exist before our test
	mgr.mu.RLock()
	initialCount := len(mgr.breakers)
	mgr.mu.RUnlock()

	// Launch many concurrent goroutines accessing circuit breakers
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			host := hosts[index%len(hosts)]
			cb := mgr.getCircuitBreaker(host)
			if cb == nil {
				t.Errorf("Expected non-nil circuit breaker")
			}
		}(i)
	}

	wg.Wait()

	// Verify we added exactly 3 circuit breakers
	mgr.mu.RLock()
	finalCount := len(mgr.breakers)
	mgr.mu.RUnlock()

	if finalCount-initialCount != 3 {
		t.Errorf("Expected 3 new circuit breakers, got %d (initial: %d, final: %d)", finalCount-initialCount, initialCount, finalCount)
	}

	// Verify all our hosts have circuit breakers
	for _, host := range hosts {
		cb := mgr.getCircuitBreaker(host)
		if cb == nil {
			t.Errorf("Expected circuit breaker for host %s", host)
		}
	}
}

func TestCircuitBreaker_FailureRateCalculation(t *testing.T) {
	cfg := circuitBreakerConfig{
		failureRateThreshold:     50,
		minimumNumberOfCalls:     20,
		slidingWindowSize:        30,
		waitDurationInOpenState:  1 * time.Second,
		permittedCallsInHalfOpen: 2,
	}
	cb := newCircuitBreaker(cfg)

	ctx := context.Background()

	// Execute 30 calls: 15 failures, 15 successes (50% failure rate)
	for i := 0; i < 30; i++ {
		if i%2 == 0 {
			cb.execute(ctx, func() error {
				return errors.New("test error")
			})
		} else {
			cb.execute(ctx, func() error {
				return nil
			})
		}
	}

	// Failure rate should be 50%
	failureRate := cb.getFailureRate()
	if failureRate != 50 {
		t.Errorf("Expected 50%% failure rate, got %d%%", failureRate)
	}

	// Circuit should be open (50% >= 50% threshold)
	if cb.getState() != stateOpen {
		t.Errorf("Expected circuit to be Open at 50%% failure rate, got %v", cb.getState())
	}
}

func TestCircuitBreaker_ContextCancellation(t *testing.T) {
	cfg := defaultCircuitBreakerConfig()
	cb := newCircuitBreaker(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	// Circuit breaker should still execute (doesn't check context)
	err := cb.execute(ctx, func() error {
		return nil
	})

	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}
