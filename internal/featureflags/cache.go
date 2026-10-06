// Package featureflags shares connector-service flags without requiring telemetry or a session.
package featureflags

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/databricks/databricks-sql-go/internal/client"
	"golang.org/x/sync/singleflight"
)

const (
	// featureFlagCacheDuration is the fallback when the server omits a valid TTL.
	featureFlagCacheDuration = 15 * time.Minute
	// featureFlagHTTPTimeout is the default timeout for feature flag HTTP requests
	featureFlagHTTPTimeout = 10 * time.Second
	// featureFlagEndpointPath is the path for feature flag endpoint
	featureFlagEndpointPath = "/api/2.0/connector-service/feature-flags/GOLANG/"
)

// Request provides caller-owned authenticated transport, usable before opening a session.
// Cache entries retain only values, never credentials or HTTP clients.
type Request struct {
	Host, WorkspaceID, DriverVersion, UserAgent string
	HTTPClient                                  *http.Client
}

// Cache shares values per workspace (normalized host when workspace ID is unavailable).
// Acquire a reference before reading, and Release it when the consumer closes.
type Cache struct {
	mu       sync.RWMutex
	contexts map[string]*featureFlagContext
}

// featureFlagContext holds feature flag state and reference count for a workspace.
type featureFlagContext struct {
	mu            sync.RWMutex // protects flags, lastFetched, cacheDuration
	flags         map[string]string
	lastFetched   time.Time
	refCount      int // protected by Cache.mu
	cacheDuration time.Duration
	fetch         singleflight.Group
	refresh       sync.Mutex // concurrent stale readers skip an in-flight refresh
}

var (
	flagCacheOnce     sync.Once
	flagCacheInstance *Cache
)

// GetCache returns the process-wide cache shared by driver consumers.
func GetCache() *Cache {
	flagCacheOnce.Do(func() {
		flagCacheInstance = &Cache{
			contexts: make(map[string]*featureFlagContext),
		}
	})
	return flagCacheInstance
}

func hostURL(host string) string {
	if !strings.Contains(host, "://") {
		host = "https://" + host
	}
	return strings.TrimRight(host, "/")
}

func cacheKey(host string, workspaceID ...string) string {
	if len(workspaceID) > 0 && workspaceID[0] != "" {
		return "workspace:" + workspaceID[0]
	}
	normalized := strings.ToLower(hostURL(host))
	if strings.HasPrefix(normalized, "https://") {
		normalized = strings.TrimSuffix(normalized, ":443")
	}
	return "host:" + normalized
}

// Acquire increments the reference count without making a request.
func (c *Cache) Acquire(host string, workspaceID ...string) *featureFlagContext {
	host = cacheKey(host, workspaceID...)
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

// Release removes the entry when its last consumer closes.
func (c *Cache) Release(host string, workspaceID ...string) {
	host = cacheKey(host, workspaceID...)
	c.mu.Lock()
	defer c.mu.Unlock()

	if ctx, exists := c.contexts[host]; exists {
		ctx.refCount--
		if ctx.refCount <= 0 {
			delete(c.contexts, host)
		}
	}
}

func (c *Cache) getValue(ctx context.Context, request Request, name string) (string, error) {
	c.mu.RLock()
	flagCtx, exists := c.contexts[cacheKey(request.Host, request.WorkspaceID)]
	c.mu.RUnlock()

	if !exists {
		return "", nil
	}

	flagCtx.mu.RLock()
	cachedFlags := flagCtx.flags
	if !flagCtx.isExpired() {
		value := cachedFlags[name]
		flagCtx.mu.RUnlock()
		return value, nil
	}
	flagCtx.mu.RUnlock()

	load := func() (map[string]string, error) {
		flagCtx.mu.RLock()
		if !flagCtx.isExpired() {
			flags := flagCtx.flags
			flagCtx.mu.RUnlock()
			return flags, nil
		}
		flagCtx.mu.RUnlock()
		flags, ttl, err := fetchFeatureFlags(ctx, request)
		flagCtx.mu.Lock()
		defer flagCtx.mu.Unlock()
		if err == nil {
			flagCtx.flags, flagCtx.cacheDuration = flags, ttl
			flagCtx.lastFetched = time.Now()
		}
		if flagCtx.flags != nil {
			return flagCtx.flags, nil // Retain stale values on a refresh failure.
		}
		return nil, err
	}
	if cachedFlags != nil {
		if !flagCtx.refresh.TryLock() {
			return cachedFlags[name], nil
		}
		defer flagCtx.refresh.Unlock()
		flags, err := load()
		return flags[name], err
	}

	result := flagCtx.fetch.DoChan("", func() (any, error) { return load() })
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case fetched := <-result:
		if fetched.Err != nil {
			return "", fetched.Err
		}
		return fetched.Val.(map[string]string)[name], nil
	}
}

// isExpired returns true if the cache has expired.
func (c *featureFlagContext) isExpired() bool {
	return c.flags == nil || time.Since(c.lastFetched) >= c.cacheDuration
}

// GetBool defaults to false for absent/null flags; malformed values return false and an error.
func (c *Cache) GetBool(ctx context.Context, request Request, name string) (bool, error) {
	return read[bool](c, ctx, request, name)
}

func (c *Cache) GetInt32(ctx context.Context, request Request, name string) (int32, error) {
	return read[int32](c, ctx, request, name)
}

func (c *Cache) GetInt64(ctx context.Context, request Request, name string) (int64, error) {
	return read[int64](c, ctx, request, name)
}

func (c *Cache) GetDouble(ctx context.Context, request Request, name string) (float64, error) {
	return read[float64](c, ctx, request, name)
}

func (c *Cache) GetString(ctx context.Context, request Request, name string) (string, error) {
	return read[string](c, ctx, request, name)
}

func (c *Cache) GetStringList(ctx context.Context, request Request, name string) ([]string, error) {
	items, err := read[[]*string](c, ctx, request, name)
	if err != nil {
		return nil, err
	}
	values := make([]string, len(items))
	for i, item := range items {
		if item == nil {
			return nil, fmt.Errorf("feature flag %q is not a string list", name)
		}
		values[i] = *item
	}
	return values, nil
}

// Consumers choose the expected SAFE type. Non-boolean getters return an error
// for missing/null/invalid values, so callers can select a suitable default.
func read[T any](c *Cache, ctx context.Context, request Request, name string) (T, error) {
	var value T
	raw, err := c.getValue(ctx, request, name)
	if err != nil {
		return value, err
	}
	if raw == "" || strings.TrimSpace(raw) == "null" {
		if _, boolean := any(value).(bool); boolean {
			return value, nil
		}
		return value, fmt.Errorf("feature flag %q is missing or null", name)
	}
	if err := json.Unmarshal([]byte(raw), &value); err != nil {
		var zero T
		return zero, err
	}
	return value, nil
}

func fetchFeatureFlags(ctx context.Context, request Request) (map[string]string, time.Duration, error) {
	if request.HTTPClient == nil {
		return nil, 0, fmt.Errorf("feature flags require an authenticated HTTP client")
	}
	// Add timeout to context if it doesn't have a deadline
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, featureFlagHTTPTimeout)
		defer cancel()
	}

	// Construct endpoint URL using connector-service endpoint like JDBC
	endpoint := fmt.Sprintf("%s%s%s", hostURL(request.Host), featureFlagEndpointPath, request.DriverVersion)

	// Feature-flag GET shares the same rate-limit group as /telemetry-ext on
	// the server side, so a 429/503 here should also fail fast rather than
	// being retried 5× by retryablehttp.
	ctx = client.WithSkipTransientRetries(ctx)

	req, err := http.NewRequestWithContext(ctx, "GET", endpoint, nil)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to create feature flag request: %w", err)
	}
	if request.UserAgent != "" {
		req.Header.Set("User-Agent", request.UserAgent)
	}
	if request.WorkspaceID != "" {
		req.Header.Set("X-Databricks-Org-Id", request.WorkspaceID)
	}

	resp, err := request.HTTPClient.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to fetch feature flag: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		// Read and discard body to allow HTTP connection reuse
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil, 0, fmt.Errorf("feature flag check failed: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to read feature flag response: %w", err)
	}

	var result struct {
		Flags []struct {
			Name  string `json:"name"`
			Value string `json:"value"`
		} `json:"flags"`
		TTLSeconds int `json:"ttl_seconds"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, 0, fmt.Errorf("failed to decode feature flag response: %w", err)
	}

	flags := make(map[string]string, len(result.Flags))
	for _, flag := range result.Flags {
		flags[flag.Name] = flag.Value
	}
	ttl := featureFlagCacheDuration
	if seconds := result.TTLSeconds; seconds > 0 && int64(seconds) <= int64((1<<63-1)/time.Second) {
		ttl = time.Duration(seconds) * time.Second
	}
	return flags, ttl, nil
}
