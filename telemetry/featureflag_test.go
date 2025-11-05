package telemetry

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestGetFeatureFlagCache_Singleton(t *testing.T) {
	cache1 := getFeatureFlagCache()
	cache2 := getFeatureFlagCache()

	if cache1 != cache2 {
		t.Error("getFeatureFlagCache should return the same instance")
	}
}

func TestGetOrCreateContext_CreatesNew(t *testing.T) {
	cache := &featureFlagCache{
		contexts: make(map[string]*featureFlagContext),
	}

	ctx := cache.getOrCreateContext("host1.databricks.com")

	if ctx == nil {
		t.Fatal("getOrCreateContext should return a context")
	}

	if ctx.refCount != 1 {
		t.Errorf("expected refCount=1, got %d", ctx.refCount)
	}

	if ctx.cacheDuration != 15*time.Minute {
		t.Errorf("expected cacheDuration=15m, got %v", ctx.cacheDuration)
	}
}

func TestGetOrCreateContext_IncrementsRefCount(t *testing.T) {
	cache := &featureFlagCache{
		contexts: make(map[string]*featureFlagContext),
	}

	ctx1 := cache.getOrCreateContext("host1.databricks.com")
	ctx2 := cache.getOrCreateContext("host1.databricks.com")

	if ctx1 != ctx2 {
		t.Error("should return same context for same host")
	}

	if ctx1.refCount != 2 {
		t.Errorf("expected refCount=2, got %d", ctx1.refCount)
	}
}

func TestReleaseContext_DecrementsRefCount(t *testing.T) {
	cache := &featureFlagCache{
		contexts: make(map[string]*featureFlagContext),
	}

	cache.getOrCreateContext("host1.databricks.com")
	cache.getOrCreateContext("host1.databricks.com")

	cache.releaseContext("host1.databricks.com")

	ctx := cache.contexts["host1.databricks.com"]
	if ctx == nil {
		t.Fatal("context should still exist")
	}

	if ctx.refCount != 1 {
		t.Errorf("expected refCount=1, got %d", ctx.refCount)
	}
}

func TestReleaseContext_RemovesContextAtZero(t *testing.T) {
	cache := &featureFlagCache{
		contexts: make(map[string]*featureFlagContext),
	}

	cache.getOrCreateContext("host1.databricks.com")
	cache.releaseContext("host1.databricks.com")

	if _, exists := cache.contexts["host1.databricks.com"]; exists {
		t.Error("context should be removed when refCount reaches zero")
	}
}

func TestReleaseContext_NonExistentHost(t *testing.T) {
	cache := &featureFlagCache{
		contexts: make(map[string]*featureFlagContext),
	}

	// Should not panic
	cache.releaseContext("nonexistent.host")
}

func TestIsTelemetryEnabled_NoContext(t *testing.T) {
	cache := &featureFlagCache{
		contexts: make(map[string]*featureFlagContext),
	}

	enabled, err := cache.isTelemetryEnabled(context.Background(), "host1.databricks.com", http.DefaultClient)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if enabled {
		t.Error("expected false when no context exists")
	}
}

func TestIsTelemetryEnabled_CacheHit(t *testing.T) {
	cache := &featureFlagCache{
		contexts: make(map[string]*featureFlagContext),
	}

	// Create context with cached value
	enabled := true
	cache.contexts["host1.databricks.com"] = &featureFlagContext{
		enabled:       &enabled,
		lastFetched:   time.Now(),
		refCount:      1,
		cacheDuration: 15 * time.Minute,
	}

	result, err := cache.isTelemetryEnabled(context.Background(), "host1.databricks.com", http.DefaultClient)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if !result {
		t.Error("expected true from cache")
	}
}

func TestIsTelemetryEnabled_CacheExpired(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := map[string]interface{}{
			"flags": map[string]bool{
				"databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForAdbc": true,
			},
		}
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	cache := &featureFlagCache{
		contexts: make(map[string]*featureFlagContext),
	}

	// Create context with expired cache
	enabled := false
	cache.contexts[server.URL] = &featureFlagContext{
		enabled:       &enabled,
		lastFetched:   time.Now().Add(-20 * time.Minute), // Expired
		refCount:      1,
		cacheDuration: 15 * time.Minute,
	}

	result, err := cache.isTelemetryEnabled(context.Background(), server.URL, http.DefaultClient)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if !result {
		t.Error("expected true from fresh fetch")
	}

	// Verify cache was updated
	ctx := cache.contexts[server.URL]
	if ctx.enabled == nil || !*ctx.enabled {
		t.Error("cache should be updated with fresh value")
	}
}

func TestIsTelemetryEnabled_FetchError_FallbackToCache(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	cache := &featureFlagCache{
		contexts: make(map[string]*featureFlagContext),
	}

	// Create context with expired cache
	enabled := true
	cache.contexts[server.URL] = &featureFlagContext{
		enabled:       &enabled,
		lastFetched:   time.Now().Add(-20 * time.Minute), // Expired
		refCount:      1,
		cacheDuration: 15 * time.Minute,
	}

	result, err := cache.isTelemetryEnabled(context.Background(), server.URL, http.DefaultClient)

	if err != nil {
		t.Errorf("should not return error when fallback to cache: %v", err)
	}

	if !result {
		t.Error("expected true from cached fallback value")
	}
}

func TestIsTelemetryEnabled_FetchError_NoCache(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	cache := &featureFlagCache{
		contexts: make(map[string]*featureFlagContext),
	}

	// Create context without cached value
	cache.contexts[server.URL] = &featureFlagContext{
		enabled:       nil,
		refCount:      1,
		cacheDuration: 15 * time.Minute,
	}

	result, err := cache.isTelemetryEnabled(context.Background(), server.URL, http.DefaultClient)

	if err == nil {
		t.Error("expected error when fetch fails and no cache")
	}

	if result {
		t.Error("expected false when fetch fails and no cache")
	}
}

func TestIsExpired_NotFetched(t *testing.T) {
	ctx := &featureFlagContext{
		enabled:       nil,
		cacheDuration: 15 * time.Minute,
	}

	if !ctx.isExpired() {
		t.Error("should be expired when not fetched")
	}
}

func TestIsExpired_Fresh(t *testing.T) {
	enabled := true
	ctx := &featureFlagContext{
		enabled:       &enabled,
		lastFetched:   time.Now(),
		cacheDuration: 15 * time.Minute,
	}

	if ctx.isExpired() {
		t.Error("should not be expired when fresh")
	}
}

func TestIsExpired_Expired(t *testing.T) {
	enabled := true
	ctx := &featureFlagContext{
		enabled:       &enabled,
		lastFetched:   time.Now().Add(-20 * time.Minute),
		cacheDuration: 15 * time.Minute,
	}

	if !ctx.isExpired() {
		t.Error("should be expired after TTL")
	}
}

func TestFetchFeatureFlag_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify request
		if r.Method != "GET" {
			t.Errorf("expected GET, got %s", r.Method)
		}

		flags := r.URL.Query().Get("flags")
		expectedFlag := "databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForAdbc"
		if flags != expectedFlag {
			t.Errorf("expected flag=%s, got %s", expectedFlag, flags)
		}

		// Send response
		response := map[string]interface{}{
			"flags": map[string]bool{
				"databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForAdbc": true,
			},
		}
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	enabled, err := fetchFeatureFlag(context.Background(), server.URL, http.DefaultClient)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if !enabled {
		t.Error("expected true from API response")
	}
}

func TestFetchFeatureFlag_FalseValue(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := map[string]interface{}{
			"flags": map[string]bool{
				"databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForAdbc": false,
			},
		}
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	enabled, err := fetchFeatureFlag(context.Background(), server.URL, http.DefaultClient)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if enabled {
		t.Error("expected false from API response")
	}
}

func TestFetchFeatureFlag_HTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	_, err := fetchFeatureFlag(context.Background(), server.URL, http.DefaultClient)

	if err == nil {
		t.Error("expected error on HTTP failure")
	}
}

func TestFetchFeatureFlag_InvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("invalid json"))
	}))
	defer server.Close()

	_, err := fetchFeatureFlag(context.Background(), server.URL, http.DefaultClient)

	if err == nil {
		t.Error("expected error on invalid JSON")
	}
}

func TestConcurrentAccess(t *testing.T) {
	cache := &featureFlagCache{
		contexts: make(map[string]*featureFlagContext),
	}

	var wg sync.WaitGroup
	numGoroutines := 100
	host := "host1.databricks.com"

	// Concurrent getOrCreateContext
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cache.getOrCreateContext(host)
		}()
	}

	wg.Wait()

	ctx := cache.contexts[host]
	if ctx.refCount != numGoroutines {
		t.Errorf("expected refCount=%d, got %d", numGoroutines, ctx.refCount)
	}

	// Concurrent releaseContext
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cache.releaseContext(host)
		}()
	}

	wg.Wait()

	if _, exists := cache.contexts[host]; exists {
		t.Error("context should be removed after all releases")
	}
}

func TestConcurrentFetch(t *testing.T) {
	callCount := 0
	var mu sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		callCount++
		mu.Unlock()

		response := map[string]interface{}{
			"flags": map[string]bool{
				"databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForAdbc": true,
			},
		}
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	cache := &featureFlagCache{
		contexts: make(map[string]*featureFlagContext),
	}

	cache.getOrCreateContext(server.URL)

	var wg sync.WaitGroup
	numGoroutines := 50

	// Concurrent isTelemetryEnabled calls
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cache.isTelemetryEnabled(context.Background(), server.URL, http.DefaultClient)
		}()
	}

	wg.Wait()

	// Should significantly reduce HTTP calls due to caching
	// Note: Initial concurrent calls may race before cache is populated,
	// but subsequent calls should all use cache
	mu.Lock()
	defer mu.Unlock()
	if callCount >= numGoroutines {
		t.Errorf("no caching detected: %d HTTP calls for %d goroutines", callCount, numGoroutines)
	}
	// Expect significant reduction (< 50% of goroutines)
	if callCount > numGoroutines/2 {
		t.Logf("warning: more HTTP calls than expected: %d (caching could be more effective)", callCount)
	}
}

func TestMultipleHosts(t *testing.T) {
	cache := &featureFlagCache{
		contexts: make(map[string]*featureFlagContext),
	}

	hosts := []string{"host1.databricks.com", "host2.databricks.com", "host3.databricks.com"}

	// Create contexts for multiple hosts
	for _, host := range hosts {
		cache.getOrCreateContext(host)
		cache.getOrCreateContext(host) // Increment to 2
	}

	// Verify all contexts exist
	for _, host := range hosts {
		ctx, exists := cache.contexts[host]
		if !exists {
			t.Errorf("context should exist for %s", host)
		}
		if ctx.refCount != 2 {
			t.Errorf("expected refCount=2 for %s, got %d", host, ctx.refCount)
		}
	}

	// Release one host completely
	cache.releaseContext(hosts[0])
	cache.releaseContext(hosts[0])

	// Verify only first host is removed
	if _, exists := cache.contexts[hosts[0]]; exists {
		t.Errorf("context should be removed for %s", hosts[0])
	}

	for _, host := range hosts[1:] {
		if _, exists := cache.contexts[host]; !exists {
			t.Errorf("context should still exist for %s", host)
		}
	}
}
