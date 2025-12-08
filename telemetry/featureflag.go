package telemetry

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	// featureFlagCacheDuration is how long to cache feature flag values
	featureFlagCacheDuration = 15 * time.Minute
	// featureFlagHTTPTimeout is the default timeout for feature flag HTTP requests
	featureFlagHTTPTimeout = 10 * time.Second
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
func (c *featureFlagCache) isTelemetryEnabled(ctx context.Context, host string, httpClient *http.Client) (bool, error) {
	c.mu.RLock()
	flagCtx, exists := c.contexts[host]
	c.mu.RUnlock()

	if !exists {
		return false, nil
	}

	// Check if cache is valid (with proper locking)
	flagCtx.mu.RLock()
	if flagCtx.enabled != nil && time.Since(flagCtx.lastFetched) < flagCtx.cacheDuration {
		enabled := *flagCtx.enabled
		flagCtx.mu.RUnlock()
		return enabled, nil
	}

	// Check if another goroutine is already fetching
	if flagCtx.fetching {
		// Return cached value if available, otherwise wait
		if flagCtx.enabled != nil {
			enabled := *flagCtx.enabled
			flagCtx.mu.RUnlock()
			return enabled, nil
		}
		flagCtx.mu.RUnlock()
		// No cached value and fetch in progress, return false
		return false, nil
	}

	// Mark as fetching
	flagCtx.fetching = true
	flagCtx.mu.RUnlock()

	// Fetch fresh value
	enabled, err := fetchFeatureFlag(ctx, host, httpClient)

	// Update cache (with proper locking)
	flagCtx.mu.Lock()
	flagCtx.fetching = false
	if err == nil {
		flagCtx.enabled = &enabled
		flagCtx.lastFetched = time.Now()
	}
	// On error, keep the old cached value if it exists
	result := false
	var returnErr error
	if err != nil {
		if flagCtx.enabled != nil {
			result = *flagCtx.enabled
			returnErr = nil // Return cached value without error
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
func fetchFeatureFlag(ctx context.Context, host string, httpClient *http.Client) (bool, error) {
	// Add timeout to context if it doesn't have a deadline
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, featureFlagHTTPTimeout)
		defer cancel()
	}

	// Construct endpoint URL, adding https:// if not already present
	var endpoint string
	if strings.HasPrefix(host, "http://") || strings.HasPrefix(host, "https://") {
		endpoint = fmt.Sprintf("%s/api/2.0/feature-flags", host)
	} else {
		endpoint = fmt.Sprintf("https://%s/api/2.0/feature-flags", host)
	}

	req, err := http.NewRequestWithContext(ctx, "GET", endpoint, nil)
	if err != nil {
		return false, fmt.Errorf("failed to create feature flag request: %w", err)
	}

	// Add query parameter for the specific feature flag
	q := req.URL.Query()
	q.Add("flags", "databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForGoDriver")
	req.URL.RawQuery = q.Encode()

	resp, err := httpClient.Do(req)
	if err != nil {
		return false, fmt.Errorf("failed to fetch feature flag: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		// Read and discard body to allow HTTP connection reuse
		io.Copy(io.Discard, resp.Body)
		return false, fmt.Errorf("feature flag check failed: %d", resp.StatusCode)
	}

	var result struct {
		Flags map[string]bool `json:"flags"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return false, fmt.Errorf("failed to decode feature flag response: %w", err)
	}

	enabled, ok := result.Flags["databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForGoDriver"]
	if !ok {
		return false, nil
	}

	return enabled, nil
}
