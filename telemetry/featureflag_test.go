package telemetry

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestGetFeatureFlagCache_Singleton(t *testing.T) {
	// Reset singleton for testing
	flagCacheInstance = nil
	flagCacheOnce = sync.Once{}

	cache1 := getFeatureFlagCache()
	cache2 := getFeatureFlagCache()

	if cache1 != cache2 {
		t.Error("Expected singleton instances to be the same")
	}
}

func TestFeatureFlagCache_GetOrCreateContext(t *testing.T) {
	cache := &featureFlagCache{
		contexts: make(map[string]*featureFlagContext),
	}

	host := "test-host.databricks.com"

	// First call should create context and increment refCount to 1
	ctx1 := cache.getOrCreateContext(host)
	if ctx1 == nil {
		t.Fatal("Expected context to be created")
	}
	if ctx1.refCount != 1 {
		t.Errorf("Expected refCount to be 1, got %d", ctx1.refCount)
	}

	// Second call should reuse context and increment refCount to 2
	ctx2 := cache.getOrCreateContext(host)
	if ctx2 != ctx1 {
		t.Error("Expected to get the same context instance")
	}
	if ctx2.refCount != 2 {
		t.Errorf("Expected refCount to be 2, got %d", ctx2.refCount)
	}

	// Verify cache duration is set
	if ctx1.cacheDuration != 15*time.Minute {
		t.Errorf("Expected cache duration to be 15 minutes, got %v", ctx1.cacheDuration)
	}
}

func TestFeatureFlagCache_ReleaseContext(t *testing.T) {
	cache := &featureFlagCache{
		contexts: make(map[string]*featureFlagContext),
	}

	host := "test-host.databricks.com"

	// Create context with refCount = 2
	cache.getOrCreateContext(host)
	cache.getOrCreateContext(host)

	// First release should decrement to 1
	cache.releaseContext(host)
	ctx, exists := cache.contexts[host]
	if !exists {
		t.Fatal("Expected context to still exist")
	}
	if ctx.refCount != 1 {
		t.Errorf("Expected refCount to be 1, got %d", ctx.refCount)
	}

	// Second release should remove context
	cache.releaseContext(host)
	_, exists = cache.contexts[host]
	if exists {
		t.Error("Expected context to be removed when refCount reaches 0")
	}

	// Release non-existent context should not panic
	cache.releaseContext("non-existent-host")
}

func TestFeatureFlagCache_IsTelemetryEnabled_Cached(t *testing.T) {
	cache := &featureFlagCache{
		contexts: make(map[string]*featureFlagContext),
	}

	host := "test-host.databricks.com"
	ctx := cache.getOrCreateContext(host)

	// Set cached value
	ctx.flags = map[string]bool{
		flagEnableTelemetry: true,
	}
	ctx.lastFetched = time.Now()

	// Should return cached value without HTTP call
	result, err := cache.isTelemetryEnabled(context.Background(), host, nil, "1.0.0")
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if result != true {
		t.Error("Expected cached value to be returned")
	}
}

func TestFeatureFlagCache_IsTelemetryEnabled_Expired(t *testing.T) {
	// Create mock server
	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"flags": [{"name": "databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForGoDriver", "value": "true"}]}`))
	}))
	defer server.Close()

	cache := &featureFlagCache{
		contexts: make(map[string]*featureFlagContext),
	}

	host := server.URL // Use full URL for testing
	ctx := cache.getOrCreateContext(host)

	// Set expired cached value
	ctx.flags = map[string]bool{
		flagEnableTelemetry: false,
	}
	ctx.lastFetched = time.Now().Add(-20 * time.Minute) // Expired

	// Should fetch fresh value
	httpClient := &http.Client{}
	result, err := cache.isTelemetryEnabled(context.Background(), host, httpClient, "1.0.0")
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if result != true {
		t.Error("Expected fresh value to be fetched and returned")
	}
	if callCount != 1 {
		t.Errorf("Expected HTTP call to be made once, got %d calls", callCount)
	}

	// Verify cache was updated
	if ctx.flags[flagEnableTelemetry] != true {
		t.Error("Expected cache to be updated with new value")
	}
}

func TestFeatureFlagCache_IsTelemetryEnabled_NoContext(t *testing.T) {
	cache := &featureFlagCache{
		contexts: make(map[string]*featureFlagContext),
	}

	host := "non-existent-host.databricks.com"

	// Should return false for non-existent context (network error expected)
	httpClient := &http.Client{Timeout: 1 * time.Second}
	result, err := cache.isTelemetryEnabled(context.Background(), host, httpClient, "1.0.0")
	// Error expected due to network failure, but should not panic
	if result != false {
		t.Error("Expected false for non-existent context")
	}
	// err is expected to be non-nil due to DNS/network failure, but that's okay
	_ = err
}

func TestFeatureFlagCache_IsTelemetryEnabled_ErrorFallback(t *testing.T) {
	// Create mock server that returns error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	cache := &featureFlagCache{
		contexts: make(map[string]*featureFlagContext),
	}

	host := server.URL // Use full URL for testing
	ctx := cache.getOrCreateContext(host)

	// Set cached value
	ctx.flags = map[string]bool{
		flagEnableTelemetry: true,
	}
	ctx.lastFetched = time.Now().Add(-20 * time.Minute) // Expired

	// Should return cached value on error
	httpClient := &http.Client{}
	result, err := cache.isTelemetryEnabled(context.Background(), host, httpClient, "1.0.0")
	if err != nil {
		t.Errorf("Expected no error (fallback to cache), got %v", err)
	}
	if result != true {
		t.Error("Expected cached value to be returned on fetch error")
	}
}

func TestFeatureFlagCache_IsTelemetryEnabled_ErrorNoCache(t *testing.T) {
	// Create mock server that returns error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	cache := &featureFlagCache{
		contexts: make(map[string]*featureFlagContext),
	}

	host := server.URL // Use full URL for testing
	cache.getOrCreateContext(host)

	// No cached value, should return error
	httpClient := &http.Client{}
	result, err := cache.isTelemetryEnabled(context.Background(), host, httpClient, "1.0.0")
	if err == nil {
		t.Error("Expected error when no cache available and fetch fails")
	}
	if result != false {
		t.Error("Expected false when no cache available and fetch fails")
	}
}

func TestFeatureFlagCache_ConcurrentAccess(t *testing.T) {
	cache := &featureFlagCache{
		contexts: make(map[string]*featureFlagContext),
	}

	host := "test-host.databricks.com"
	numGoroutines := 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Concurrent getOrCreateContext
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			cache.getOrCreateContext(host)
		}()
	}
	wg.Wait()

	// Verify refCount
	ctx, exists := cache.contexts[host]
	if !exists {
		t.Fatal("Expected context to exist")
	}
	if ctx.refCount != numGoroutines {
		t.Errorf("Expected refCount to be %d, got %d", numGoroutines, ctx.refCount)
	}

	// Concurrent releaseContext
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			cache.releaseContext(host)
		}()
	}
	wg.Wait()

	// Verify context is removed
	_, exists = cache.contexts[host]
	if exists {
		t.Error("Expected context to be removed after all releases")
	}
}

func TestFeatureFlagContext_IsExpired(t *testing.T) {
	tests := []struct {
		name     string
		flags    map[string]bool
		fetched  time.Time
		duration time.Duration
		want     bool
	}{
		{
			name:     "no cache",
			flags:    nil,
			fetched:  time.Time{},
			duration: 15 * time.Minute,
			want:     true,
		},
		{
			name:     "fresh cache",
			flags:    map[string]bool{flagEnableTelemetry: true},
			fetched:  time.Now(),
			duration: 15 * time.Minute,
			want:     false,
		},
		{
			name:     "expired cache",
			flags:    map[string]bool{flagEnableTelemetry: true},
			fetched:  time.Now().Add(-20 * time.Minute),
			duration: 15 * time.Minute,
			want:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := &featureFlagContext{
				flags:         tt.flags,
				lastFetched:   tt.fetched,
				cacheDuration: tt.duration,
			}
			if got := ctx.isExpired(); got != tt.want {
				t.Errorf("isExpired() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFetchFeatureFlags_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify request
		if r.Method != "GET" {
			t.Errorf("Expected GET request, got %s", r.Method)
		}
		expectedPath := "/api/2.0/connector-service/feature-flags/GOLANG/1.0.0"
		if r.URL.Path != expectedPath {
			t.Errorf("Expected %s path, got %s", expectedPath, r.URL.Path)
		}

		// Return success response
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"flags": [{"name": "databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForGoDriver", "value": "true"}]}`))
	}))
	defer server.Close()

	host := server.URL // Use full URL for testing
	httpClient := &http.Client{}

	flags, err := fetchFeatureFlags(context.Background(), host, httpClient, "1.0.0")
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if !flags[flagEnableTelemetry] {
		t.Error("Expected telemetry feature flag to be enabled")
	}
}

func TestFetchFeatureFlags_Disabled(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"flags": [{"name": "databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForGoDriver", "value": "false"}]}`))
	}))
	defer server.Close()

	host := server.URL // Use full URL for testing
	httpClient := &http.Client{}

	flags, err := fetchFeatureFlags(context.Background(), host, httpClient, "1.0.0")
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if flags[flagEnableTelemetry] {
		t.Error("Expected telemetry feature flag to be disabled")
	}
}

func TestFetchFeatureFlags_FlagNotPresent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"flags": []}`))
	}))
	defer server.Close()

	host := server.URL // Use full URL for testing
	httpClient := &http.Client{}

	flags, err := fetchFeatureFlags(context.Background(), host, httpClient, "1.0.0")
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if flags[flagEnableTelemetry] {
		t.Error("Expected telemetry feature flag to be false when not present")
	}
}

func TestFetchFeatureFlags_HTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	host := server.URL // Use full URL for testing
	httpClient := &http.Client{}

	_, err := fetchFeatureFlags(context.Background(), host, httpClient, "1.0.0")
	if err == nil {
		t.Error("Expected error for HTTP 500")
	}
}

func TestFetchFeatureFlags_InvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`invalid json`))
	}))
	defer server.Close()

	host := server.URL // Use full URL for testing
	httpClient := &http.Client{}

	_, err := fetchFeatureFlags(context.Background(), host, httpClient, "1.0.0")
	if err == nil {
		t.Error("Expected error for invalid JSON")
	}
}

func TestFetchFeatureFlags_ContextCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	host := server.URL // Use full URL for testing
	httpClient := &http.Client{}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := fetchFeatureFlags(ctx, host, httpClient, "1.0.0")
	if err == nil {
		t.Error("Expected error for cancelled context")
	}
}
