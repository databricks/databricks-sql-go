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

	// Feature flag names
	// flagEnableTelemetry controls whether telemetry is enabled for the Go driver
	flagEnableTelemetry = "databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForGoDriver"
	// Add more feature flags here as needed:
	// flagEnableNewFeature = "databricks.partnerplatform.clientConfigsFeatureFlags.enableNewFeatureForGoDriver"
)

// featureFlagCache manages feature flag state per host with reference counting.
// This prevents rate limiting by caching feature flag responses.
type featureFlagCache struct {
	mu       sync.RWMutex
	contexts map[string]*featureFlagContext
}

// featureFlagContext holds feature flag state and reference count for a host.
type featureFlagContext struct {
	mu            sync.RWMutex    // protects flags, lastFetched, fetching
	flags         map[string]bool // cached feature flags by name
	lastFetched   time.Time       // when flags were last fetched
	refCount      int             // protected by featureFlagCache.mu
	cacheDuration time.Duration   // how long to cache flags
	fetching      bool            // true if a fetch is in progress
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

// getFeatureFlag retrieves a specific feature flag value for the host.
// This is the generic method that handles caching and fetching for any flag.
// Uses cached value if available and not expired.
func (c *featureFlagCache) getFeatureFlag(ctx context.Context, host string, httpClient *http.Client, flagName string) (bool, error) {
	c.mu.RLock()
	flagCtx, exists := c.contexts[host]
	c.mu.RUnlock()

	// If context doesn't exist, create it and make initial blocking fetch
	if !exists {
		c.mu.Lock()
		// Double-check after acquiring write lock
		flagCtx, exists = c.contexts[host]
		if !exists {
			flagCtx = &featureFlagContext{
				cacheDuration: featureFlagCacheDuration,
				fetching:      true, // Mark as fetching
			}
			c.contexts[host] = flagCtx
		}
		c.mu.Unlock()

		// If we just created the context, make the initial blocking fetch
		if !exists {
			flags, err := fetchFeatureFlags(ctx, host, httpClient)

			flagCtx.mu.Lock()
			flagCtx.fetching = false
			if err == nil {
				flagCtx.flags = flags
				flagCtx.lastFetched = time.Now()
				result := flags[flagName]
				flagCtx.mu.Unlock()
				return result, nil
			}
			// On error for first fetch, fail-safe: return false (telemetry disabled)
			flagCtx.mu.Unlock()
			return false, nil
		}
	}

	// Check if cache is valid (with proper locking)
	flagCtx.mu.RLock()
	if flagCtx.flags != nil && time.Since(flagCtx.lastFetched) < flagCtx.cacheDuration {
		// Cache is valid, return the cached flag value
		enabled := flagCtx.flags[flagName] // returns false if flag not found
		flagCtx.mu.RUnlock()
		return enabled, nil
	}

	// Check if another goroutine is already fetching
	if flagCtx.fetching {
		// Return cached value if available, otherwise wait
		if flagCtx.flags != nil {
			enabled := flagCtx.flags[flagName]
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

	// Fetch fresh values for all flags
	flags, err := fetchFeatureFlags(ctx, host, httpClient)

	// Update cache (with proper locking)
	flagCtx.mu.Lock()
	flagCtx.fetching = false
	if err == nil {
		flagCtx.flags = flags
		flagCtx.lastFetched = time.Now()
	}
	// On error, keep the old cached values if they exist
	result := false
	var returnErr error
	if err != nil {
		if flagCtx.flags != nil {
			result = flagCtx.flags[flagName]
			returnErr = nil // Return cached value without error
		} else {
			returnErr = err
		}
	} else {
		result = flags[flagName]
	}
	flagCtx.mu.Unlock()

	return result, returnErr
}

// isTelemetryEnabled checks if telemetry is enabled for the host.
// Uses cached value if available and not expired.
func (c *featureFlagCache) isTelemetryEnabled(ctx context.Context, host string, httpClient *http.Client) (bool, error) {
	return c.getFeatureFlag(ctx, host, httpClient, flagEnableTelemetry)
}

// isExpired returns true if the cache has expired.
func (c *featureFlagContext) isExpired() bool {
	return c.flags == nil || time.Since(c.lastFetched) > c.cacheDuration
}

// getAllFeatureFlags returns a list of all feature flags to fetch.
// Add new flags here when adding new features.
func getAllFeatureFlags() []string {
	return []string{
		flagEnableTelemetry,
		// Add more flags here as needed:
		// flagEnableNewFeature,
	}
}

// fetchFeatureFlags fetches multiple feature flag values from Databricks in a single request.
// Returns a map of flag names to their boolean values.
func fetchFeatureFlags(ctx context.Context, host string, httpClient *http.Client) (map[string]bool, error) {
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
		return nil, fmt.Errorf("failed to create feature flag request: %w", err)
	}

	// Add query parameter with comma-separated list of feature flags
	// This fetches all flags in a single request for efficiency
	allFlags := getAllFeatureFlags()
	q := req.URL.Query()
	q.Add("flags", strings.Join(allFlags, ","))
	req.URL.RawQuery = q.Encode()

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch feature flags: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		// Read and discard body to allow HTTP connection reuse
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil, fmt.Errorf("feature flag check failed: %d", resp.StatusCode)
	}

	var result struct {
		Flags map[string]bool `json:"flags"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode feature flag response: %w", err)
	}

	// Return the full map of flags
	// Flags not present in the response will have false value when accessed
	if result.Flags == nil {
		return make(map[string]bool), nil
	}

	return result.Flags, nil
}
