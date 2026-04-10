package telemetry

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

const (
	// featureFlagCacheDuration is how long to cache feature flag values
	featureFlagCacheDuration = 15 * time.Minute
	// featureFlagHTTPTimeout is the default timeout for feature flag HTTP requests
	featureFlagHTTPTimeout = 10 * time.Second
	// featureFlagEndpointPath is the path for feature flag endpoint
	featureFlagEndpointPath = "/api/2.0/connector-service/feature-flags/GOLANG/"
	// featureFlagName is the name of the Go driver telemetry feature flag
	featureFlagName = "databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForGoDriver"
)

// featureFlagCache manages feature flag state per host with reference counting.
// This prevents rate limiting by caching feature flag responses.
type featureFlagCache struct {
	mu       sync.RWMutex
	contexts map[string]*featureFlagContext
}

// featureFlagContext holds feature flag state and reference count for a host.
type featureFlagContext struct {
	mu            sync.RWMutex // protects enabled, lastFetched, fetching
	enabled       *bool
	lastFetched   time.Time
	refCount      int // protected by featureFlagCache.mu
	cacheDuration time.Duration
	fetching      bool // true if a fetch is in progress
}

var (
	flagCacheOnce     sync.Once
	flagCacheInstance *featureFlagCache
)

// getFeatureFlagCache returns the singleton instance.
func getFeatureFlagCache() *featureFlagCache {
	flagCacheOnce.Do(func() {
		flagCacheInstance = &featureFlagCache{
			contexts: make(map[string]*featureFlagContext),
		}
	})
	return flagCacheInstance
}

// getOrCreateContext gets or creates a feature flag context for the host.
// Increments reference count.
func (c *featureFlagCache) getOrCreateContext(host string) *featureFlagContext {
	c.mu.Lock()
	defer c.mu.Unlock()

	ctx, exists := c.contexts[host]
	if !exists {
		ctx = &featureFlagContext{
			cacheDuration: featureFlagCacheDuration,
		}
		c.contexts[host] = ctx
	}
	ctx.refCount++
	return ctx
}

// releaseContext decrements reference count for the host.
// Removes context when ref count reaches zero.
func (c *featureFlagCache) releaseContext(host string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if ctx, exists := c.contexts[host]; exists {
		ctx.refCount--
		if ctx.refCount <= 0 {
			delete(c.contexts, host)
		}
	}
}

// isTelemetryEnabled checks if telemetry is enabled for the host.
// Uses cached value if available and not expired.
func (c *featureFlagCache) isTelemetryEnabled(ctx context.Context, host string, driverVersion string, httpClient *http.Client) (bool, error) {
	c.mu.RLock()
	flagCtx, exists := c.contexts[host]
	c.mu.RUnlock()

	if !exists {
		return false, nil
	}

	// Fast path: check cache under read lock.
	flagCtx.mu.RLock()
	if flagCtx.enabled != nil && time.Since(flagCtx.lastFetched) < flagCtx.cacheDuration {
		enabled := *flagCtx.enabled
		flagCtx.mu.RUnlock()
		return enabled, nil
	}
	if flagCtx.fetching {
		if flagCtx.enabled != nil {
			enabled := *flagCtx.enabled
			flagCtx.mu.RUnlock()
			return enabled, nil
		}
		flagCtx.mu.RUnlock()
		return false, nil
	}
	flagCtx.mu.RUnlock()

	// Slow path: need a write lock to set fetching=true.
	// Re-check all conditions under write lock (double-checked locking) to avoid
	// a data race and to prevent duplicate fetches from concurrent goroutines.
	flagCtx.mu.Lock()
	if flagCtx.enabled != nil && time.Since(flagCtx.lastFetched) < flagCtx.cacheDuration {
		enabled := *flagCtx.enabled
		flagCtx.mu.Unlock()
		return enabled, nil
	}
	if flagCtx.fetching {
		if flagCtx.enabled != nil {
			enabled := *flagCtx.enabled
			flagCtx.mu.Unlock()
			return enabled, nil
		}
		flagCtx.mu.Unlock()
		return false, nil
	}
	flagCtx.fetching = true
	flagCtx.mu.Unlock()

	// Fetch fresh value (outside lock so other readers are not blocked).
	enabled, err := fetchFeatureFlag(ctx, host, driverVersion, httpClient)

	// Update cache.
	flagCtx.mu.Lock()
	flagCtx.fetching = false
	if err == nil {
		flagCtx.enabled = &enabled
		flagCtx.lastFetched = time.Now()
	}
	result := false
	var returnErr error
	if err != nil {
		if flagCtx.enabled != nil {
			result = *flagCtx.enabled // Return stale cached value on error
		} else {
			returnErr = err
		}
	} else {
		result = enabled
	}
	flagCtx.mu.Unlock()

	return result, returnErr
}

// isExpired returns true if the cache has expired.
func (c *featureFlagContext) isExpired() bool {
	return c.enabled == nil || time.Since(c.lastFetched) > c.cacheDuration
}

// fetchFeatureFlag fetches the feature flag value from Databricks.
func fetchFeatureFlag(ctx context.Context, host string, driverVersion string, httpClient *http.Client) (bool, error) {
	// Add timeout to context if it doesn't have a deadline
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, featureFlagHTTPTimeout)
		defer cancel()
	}

	// Construct endpoint URL using connector-service endpoint like JDBC
	hostURL := ensureHTTPScheme(host)
	endpoint := fmt.Sprintf("%s%s%s", hostURL, featureFlagEndpointPath, driverVersion)

	req, err := http.NewRequestWithContext(ctx, "GET", endpoint, nil)
	if err != nil {
		return false, fmt.Errorf("failed to create feature flag request: %w", err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return false, fmt.Errorf("failed to fetch feature flag: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		// Read and discard body to allow HTTP connection reuse
		_, _ = io.Copy(io.Discard, resp.Body)
		return false, fmt.Errorf("feature flag check failed: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, fmt.Errorf("failed to read feature flag response: %w", err)
	}

	var result struct {
		Flags []struct {
			Name  string `json:"name"`
			Value string `json:"value"`
		} `json:"flags"`
		TTLSeconds int `json:"ttl_seconds"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return false, fmt.Errorf("failed to decode feature flag response: %w", err)
	}

	// Look for Go driver telemetry feature flag
	for _, flag := range result.Flags {
		if flag.Name == featureFlagName {
			enabled := flag.Value == "true"
			return enabled, nil
		}
	}

	return false, nil
}
