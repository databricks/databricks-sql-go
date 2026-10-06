package featureflags

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const featureFlagName = "databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForGoDriver"

func TestGetFeatureFlagCache_Singleton(t *testing.T) {
	// Reset singleton for testing
	flagCacheInstance = nil
	flagCacheOnce = sync.Once{}

	cache1 := GetCache()
	cache2 := GetCache()

	if cache1 != cache2 {
		t.Error("Expected singleton instances to be the same")
	}
}

func TestFeatureFlagCache_GetOrCreateContext(t *testing.T) {
	cache := &Cache{
		contexts: make(map[string]*featureFlagContext),
	}

	host := "test-host.databricks.com"

	// First call should create context and increment refCount to 1
	ctx1 := cache.Acquire(host)
	if ctx1 == nil {
		t.Fatal("Expected context to be created")
	}
	if ctx1.refCount != 1 {
		t.Errorf("Expected refCount to be 1, got %d", ctx1.refCount)
	}

	// Second call should reuse context and increment refCount to 2
	ctx2 := cache.Acquire(host)
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
	cache := &Cache{
		contexts: make(map[string]*featureFlagContext),
	}

	host := "test-host.databricks.com"

	// Create context with refCount = 2
	cache.Acquire(host)
	cache.Acquire(host)

	// First release should decrement to 1
	cache.Release(host)
	ctx, exists := cache.contexts[cacheKey(host)]
	if !exists {
		t.Fatal("Expected context to still exist")
	}
	if ctx.refCount != 1 {
		t.Errorf("Expected refCount to be 1, got %d", ctx.refCount)
	}

	// Second release should remove context
	cache.Release(host)
	_, exists = cache.contexts[cacheKey(host)]
	if exists {
		t.Error("Expected context to be removed when refCount reaches 0")
	}

	// Release non-existent context should not panic
	cache.Release("non-existent-host")
}

func TestFeatureFlagCache_GetBool_Cached(t *testing.T) {
	cache := &Cache{
		contexts: make(map[string]*featureFlagContext),
	}

	host := "test-host.databricks.com"
	ctx := cache.Acquire(host)

	// Set cached value
	enabled := true
	ctx.flags = map[string]string{featureFlagName: strconv.FormatBool(enabled)}
	ctx.lastFetched = time.Now()

	// Should return cached value without HTTP call
	result, err := cache.GetBool(context.Background(), Request{Host: host, DriverVersion: "test-version", UserAgent: "test-ua", HTTPClient: nil}, featureFlagName)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if result != true {
		t.Error("Expected cached value to be returned")
	}
}

func TestFeatureFlagCache_GetBool_Expired(t *testing.T) {
	// Create mock server
	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"flags": [{"name": "databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForGoDriver", "value": "true"}], "ttl_seconds": 300}`))
	}))
	defer server.Close()

	cache := &Cache{
		contexts: make(map[string]*featureFlagContext),
	}

	host := server.URL // Use full URL for testing
	ctx := cache.Acquire(host)

	// Set expired cached value
	enabled := false
	ctx.flags = map[string]string{featureFlagName: strconv.FormatBool(enabled)}
	ctx.lastFetched = time.Now().Add(-20 * time.Minute) // Expired

	// Should fetch fresh value
	httpClient := &http.Client{}
	result, err := cache.GetBool(context.Background(), Request{Host: host, DriverVersion: "test-version", UserAgent: "test-ua", HTTPClient: httpClient}, featureFlagName)
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
	if ctx.flags[featureFlagName] != "true" {
		t.Error("Expected cache to be updated with new value")
	}
}

func TestFeatureFlagCache_GetBool_NoContext(t *testing.T) {
	cache := &Cache{
		contexts: make(map[string]*featureFlagContext),
	}

	host := "non-existent-host.databricks.com"

	// Should return false for non-existent context
	result, err := cache.GetBool(context.Background(), Request{Host: host, DriverVersion: "test-version", UserAgent: "test-ua", HTTPClient: nil}, featureFlagName)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if result != false {
		t.Error("Expected false for non-existent context")
	}
}

func TestFeatureFlagCache_GetBool_ErrorFallback(t *testing.T) {
	// Create mock server that returns error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	cache := &Cache{
		contexts: make(map[string]*featureFlagContext),
	}

	host := server.URL // Use full URL for testing
	ctx := cache.Acquire(host)

	// Set cached value
	enabled := true
	ctx.flags = map[string]string{featureFlagName: strconv.FormatBool(enabled)}
	ctx.lastFetched = time.Now().Add(-20 * time.Minute) // Expired

	// Should return cached value on error
	httpClient := &http.Client{}
	result, err := cache.GetBool(context.Background(), Request{Host: host, DriverVersion: "test-version", UserAgent: "test-ua", HTTPClient: httpClient}, featureFlagName)
	if err != nil {
		t.Errorf("Expected no error (fallback to cache), got %v", err)
	}
	if result != true {
		t.Error("Expected cached value to be returned on fetch error")
	}
}

func TestFeatureFlagCache_GetBool_ErrorNoCache(t *testing.T) {
	// Create mock server that returns error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	cache := &Cache{
		contexts: make(map[string]*featureFlagContext),
	}

	host := server.URL // Use full URL for testing
	cache.Acquire(host)

	// No cached value, should return error
	httpClient := &http.Client{}
	result, err := cache.GetBool(context.Background(), Request{Host: host, DriverVersion: "test-version", UserAgent: "test-ua", HTTPClient: httpClient}, featureFlagName)
	if err == nil {
		t.Error("Expected error when no cache available and fetch fails")
	}
	if result != false {
		t.Error("Expected false when no cache available and fetch fails")
	}
}

func TestFeatureFlagCache_ConcurrentAccess(t *testing.T) {
	cache := &Cache{
		contexts: make(map[string]*featureFlagContext),
	}

	host := "test-host.databricks.com"
	numGoroutines := 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Concurrent Acquire
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			cache.Acquire(host)
		}()
	}
	wg.Wait()

	// Verify refCount
	ctx, exists := cache.contexts[cacheKey(host)]
	if !exists {
		t.Fatal("Expected context to exist")
	}
	if ctx.refCount != numGoroutines {
		t.Errorf("Expected refCount to be %d, got %d", numGoroutines, ctx.refCount)
	}

	// Concurrent Release
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			cache.Release(host)
		}()
	}
	wg.Wait()

	// Verify context is removed
	_, exists = cache.contexts[cacheKey(host)]
	if exists {
		t.Error("Expected context to be removed after all releases")
	}
}

func TestFeatureFlagContext_IsExpired(t *testing.T) {
	tests := []struct {
		name     string
		flags    map[string]string
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
			flags:    map[string]string{featureFlagName: "true"},
			fetched:  time.Now(),
			duration: 15 * time.Minute,
			want:     false,
		},
		{
			name:     "expired cache",
			flags:    map[string]string{featureFlagName: "true"},
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

func TestFetchFeatureFlag_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify request method
		if r.Method != "GET" {
			t.Errorf("Expected GET request, got %s", r.Method)
		}

		// Return success response using new connector-service format
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"flags": [{"name": "databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForGoDriver", "value": "true"}], "ttl_seconds": 300}`))
	}))
	defer server.Close()

	host := server.URL // Use full URL for testing
	httpClient := &http.Client{}

	flags, _, err := fetchFeatureFlags(context.Background(), Request{Host: host, DriverVersion: "test-version", UserAgent: "test-ua", HTTPClient: httpClient})
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if flags[featureFlagName] != "true" {
		t.Error("Expected feature flag to be enabled")
	}
}

// TestFetchFeatureFlag_SetsUserAgent verifies the configured User-Agent is
// sent on feature-flag GETs so traffic is attributable in access logs.
func TestFetchFeatureFlag_SetsUserAgent(t *testing.T) {
	const wantUA = "godatabrickssqlconnector/9.9.9"
	gotUA := ""
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUA = r.Header.Get("User-Agent")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"flags": [], "ttl_seconds": 300}`))
	}))
	defer server.Close()

	_, _, err := fetchFeatureFlags(context.Background(), Request{Host: server.URL, DriverVersion: "9.9.9", UserAgent: wantUA, HTTPClient: &http.Client{}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotUA != wantUA {
		t.Errorf("User-Agent: got %q, want %q", gotUA, wantUA)
	}
}

func TestFetchFeatureFlag_Disabled(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"flags": [{"name": "databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForGoDriver", "value": "false"}], "ttl_seconds": 300}`))
	}))
	defer server.Close()

	host := server.URL // Use full URL for testing
	httpClient := &http.Client{}

	flags, _, err := fetchFeatureFlags(context.Background(), Request{Host: host, DriverVersion: "test-version", UserAgent: "test-ua", HTTPClient: httpClient})
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if flags[featureFlagName] == "true" {
		t.Error("Expected feature flag to be disabled")
	}
}

func TestFetchFeatureFlag_FlagNotPresent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"flags": [], "ttl_seconds": 300}`))
	}))
	defer server.Close()

	host := server.URL // Use full URL for testing
	httpClient := &http.Client{}

	flags, _, err := fetchFeatureFlags(context.Background(), Request{Host: host, DriverVersion: "test-version", UserAgent: "test-ua", HTTPClient: httpClient})
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if flags[featureFlagName] == "true" {
		t.Error("Expected feature flag to be false when not present")
	}
}

func TestFetchFeatureFlag_HTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	host := server.URL // Use full URL for testing
	httpClient := &http.Client{}

	_, _, err := fetchFeatureFlags(context.Background(), Request{Host: host, DriverVersion: "test-version", UserAgent: "test-ua", HTTPClient: httpClient})
	if err == nil {
		t.Error("Expected error for HTTP 500")
	}
}

func TestFetchFeatureFlag_InvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`invalid json`))
	}))
	defer server.Close()

	host := server.URL // Use full URL for testing
	httpClient := &http.Client{}

	_, _, err := fetchFeatureFlags(context.Background(), Request{Host: host, DriverVersion: "test-version", UserAgent: "test-ua", HTTPClient: httpClient})
	if err == nil {
		t.Error("Expected error for invalid JSON")
	}
}

func TestFetchFeatureFlag_ContextCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	host := server.URL // Use full URL for testing
	httpClient := &http.Client{}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, _, err := fetchFeatureFlags(ctx, Request{Host: host, DriverVersion: "test-version", UserAgent: "test-ua", HTTPClient: httpClient})
	if err == nil {
		t.Error("Expected error for cancelled context")
	}
}

func TestTypedFlags(t *testing.T) {
	cache := &Cache{contexts: make(map[string]*featureFlagContext)}
	entry := cache.Acquire("test-host")
	entry.flags = map[string]string{
		"bool": "true", "int32": "2147483647", "int64": "9223372036854775807",
		"double": "1.25", "string": `"hello"`, "list": `["a","b"]`,
		"overflow": "9223372036854775808", "null": "null", "badlist": `["a",null]`,
	}
	entry.lastFetched = time.Now()
	ctx, request := context.Background(), Request{Host: "test-host"}
	b, err := cache.GetBool(ctx, request, "bool")
	require.NoError(t, err)
	require.True(t, b)
	i32, err := cache.GetInt32(ctx, request, "int32")
	require.NoError(t, err)
	require.Equal(t, int32(2147483647), i32)
	i64, err := cache.GetInt64(ctx, request, "int64")
	require.NoError(t, err)
	require.Equal(t, int64(9223372036854775807), i64)
	d, err := cache.GetDouble(ctx, request, "double")
	require.NoError(t, err)
	require.Equal(t, 1.25, d)
	s, err := cache.GetString(ctx, request, "string")
	require.NoError(t, err)
	require.Equal(t, "hello", s)
	list, err := cache.GetStringList(ctx, request, "list")
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b"}, list)
	for _, name := range []string{"overflow", "double", "bool", "null", "missing"} {
		_, err := cache.GetInt64(ctx, request, name)
		require.Error(t, err, name)
	}
	_, err = cache.GetInt32(ctx, request, "int64")
	require.Error(t, err)
	_, err = cache.GetStringList(ctx, request, "badlist")
	require.Error(t, err)
	for _, name := range []string{"null", "missing"} {
		b, err := cache.GetBool(ctx, request, name)
		require.NoError(t, err)
		require.False(t, b)
	}
}

func TestWorkspaceFetchAndRefresh(t *testing.T) {
	var calls atomic.Int32
	var fail atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if fail.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		require.Equal(t, "GET", r.Method)
		require.Equal(t, featureFlagEndpointPath+"test-version", r.URL.Path)
		require.Equal(t, "test-ua", r.UserAgent())
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"flags":       []map[string]string{{"name": "flag", "value": r.Header.Get("X-Databricks-Org-Id")}},
			"ttl_seconds": 60,
		})
	}))
	defer server.Close()
	cache := &Cache{contexts: make(map[string]*featureFlagContext)}
	entry := cache.Acquire(server.URL, "1")
	require.Same(t, entry, cache.Acquire("alias-host", "1"))
	require.NotSame(t, entry, cache.Acquire(server.URL, "2"))
	require.Same(t, cache.Acquire("TEST-host"), cache.Acquire("https://test-host/"))
	request := Request{Host: server.URL, WorkspaceID: "1", DriverVersion: "test-version", UserAgent: "test-ua", HTTPClient: server.Client()}
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			value, err := cache.GetInt64(context.Background(), request, "flag")
			if err != nil || value != 1 {
				t.Errorf("cold concurrent read = %d, %v", value, err)
			}
		}()
	}
	wg.Wait()
	require.Equal(t, int32(1), calls.Load())
	require.Equal(t, time.Minute, entry.cacheDuration)
	entry.lastFetched = time.Now().Add(-61 * time.Second)
	value, err := cache.GetInt64(context.Background(), request, "flag")
	require.NoError(t, err)
	require.Equal(t, int64(1), value)
	require.Equal(t, int32(2), calls.Load())
	entry.lastFetched = time.Now().Add(-61 * time.Second)
	fail.Store(true)
	value, err = cache.GetInt64(context.Background(), request, "flag")
	require.NoError(t, err)
	require.Equal(t, int64(1), value) // Stale value survives failed refresh.
	fail.Store(false)
	request.WorkspaceID = "2"
	value, err = cache.GetInt64(context.Background(), request, "flag")
	require.NoError(t, err)
	require.Equal(t, int64(2), value)
	cache.Release(server.URL, "2")
	require.NotContains(t, cache.contexts, cacheKey(server.URL, "2"))
}
