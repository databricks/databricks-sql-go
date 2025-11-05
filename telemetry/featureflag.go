package telemetry

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

// featureFlagCache manages feature flag state per host with reference counting.
// This prevents rate limiting by caching feature flag responses.
type featureFlagCache struct {
	mu       sync.RWMutex
	contexts map[string]*featureFlagContext
}

// featureFlagContext holds feature flag state and reference count for a host.
type featureFlagContext struct {
	enabled       *bool
	lastFetched   time.Time
	refCount      int
	cacheDuration time.Duration
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
			cacheDuration: 15 * time.Minute,
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
func (c *featureFlagCache) isTelemetryEnabled(ctx context.Context, host string, httpClient *http.Client) (bool, error) {
	c.mu.RLock()
	flagCtx, exists := c.contexts[host]
	c.mu.RUnlock()

	if !exists {
		return false, nil
	}

	// Check if cache is valid
	if flagCtx.enabled != nil && time.Since(flagCtx.lastFetched) < flagCtx.cacheDuration {
		return *flagCtx.enabled, nil
	}

	// Fetch fresh value
	enabled, err := fetchFeatureFlag(ctx, host, httpClient)
	if err != nil {
		// Return cached value on error, or false if no cache
		if flagCtx.enabled != nil {
			return *flagCtx.enabled, nil
		}
		return false, err
	}

	// Update cache
	c.mu.Lock()
	flagCtx.enabled = &enabled
	flagCtx.lastFetched = time.Now()
	c.mu.Unlock()

	return enabled, nil
}

// isExpired returns true if the cache has expired.
func (c *featureFlagContext) isExpired() bool {
	return c.enabled == nil || time.Since(c.lastFetched) > c.cacheDuration
}

// fetchFeatureFlag fetches the feature flag value from the Databricks API.
func fetchFeatureFlag(ctx context.Context, host string, httpClient *http.Client) (bool, error) {
	// Build endpoint URL, adding https:// if no scheme present
	endpoint := host
	if !strings.HasPrefix(host, "http://") && !strings.HasPrefix(host, "https://") {
		endpoint = "https://" + host
	}
	endpoint = endpoint + "/api/2.0/feature-flags"

	req, err := http.NewRequestWithContext(ctx, "GET", endpoint, nil)
	if err != nil {
		return false, fmt.Errorf("failed to create request: %w", err)
	}

	// Add query parameters
	q := req.URL.Query()
	q.Add("flags", "databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForAdbc")
	req.URL.RawQuery = q.Encode()

	resp, err := httpClient.Do(req)
	if err != nil {
		return false, fmt.Errorf("failed to fetch feature flag: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("feature flag check failed with status: %d", resp.StatusCode)
	}

	var result struct {
		Flags map[string]bool `json:"flags"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return false, fmt.Errorf("failed to decode response: %w", err)
	}

	return result.Flags["databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForAdbc"], nil
}
