// Package warehouse_cache maintains a process-wide cache of warehouses known to
// reject the legacy Thrift protocol.
//
// A Reyden / Real-Time SQL warehouse rejects a Thrift OpenSession — the SQL
// Gateway proxy stamps SQLSTATE KP001 on the rejection. When the driver
// auto-recovers by re-opening on the kernel backend, it records the warehouse
// here so later connections to the same warehouse skip the doomed Thrift attempt
// and open on the kernel directly.
//
// Keyed by (host, warehouse_id) — the host is part of the key so the same
// warehouse id observed on two different workspaces never collides. Entries
// expire after TTLSeconds so a warehouse later reconfigured to accept Thrift
// is eventually retried.
package warehouse_cache

import (
	"regexp"
	"strings"
	"sync"
	"time"
)

const (
	// TTLSeconds controls how long a cached Reyden warehouse entry remains valid.
	// Matches the ADBC driver's 6-hour horizon.
	TTLSeconds = 6 * 60 * 60
)

var (
	// warehousePathRE matches warehouse and endpoint paths like
	// /sql/1.0/warehouses/<id> or .../endpoints/<id>; the id stops at the
	// next /, ?, or & (e.g. a ?o= SPOG routing param). All-purpose-compute
	// cluster paths carry no warehouse id and never match — they are never
	// Reyden warehouses.
	warehousePathRE = regexp.MustCompile(`(?:/|^)(?:warehouses|endpoints)/([^?&/]+)`)
)

// ExtractWarehouseID returns the warehouse/endpoint id embedded in httpPath, or empty string.
func ExtractWarehouseID(httpPath string) string {
	if httpPath == "" {
		return ""
	}
	match := warehousePathRE.FindStringSubmatch(httpPath)
	if match != nil && len(match) > 1 {
		return match[1]
	}
	return ""
}

// Cache is a thread-safe cache of warehouses known to reject Thrift.
type Cache struct {
	mu     sync.RWMutex
	// (host_lowercased, warehouse_id) -> expiry deadline (monotonic time)
	expiry map[[2]string]time.Time
}

// NewCache returns a new thread-safe cache.
func NewCache() *Cache {
	return &Cache{
		expiry: make(map[[2]string]time.Time),
	}
}

// makeKey returns the cache key for (host, warehouse_id), with host lowercased.
func makeKey(host, warehouseID string) [2]string {
	return [2]string{strings.ToLower(host), warehouseID}
}

// MarkReyden records that warehouseID on host rejects the Thrift protocol.
// Performs opportunistic sweep of expired entries.
func (c *Cache) MarkReyden(host, warehouseID string) {
	if warehouseID == "" {
		return
	}
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()

	// Opportunistic sweep: mark_reyden only runs on an actual Thrift
	// rejection (rare), so purging every expired entry here is near-free
	// and bounds the cache to warehouses seen within the TTL window
	// rather than every warehouse ever seen.
	for key := range c.expiry {
		if c.expiry[key].Before(now) {
			delete(c.expiry, key)
		}
	}

	key := makeKey(host, warehouseID)
	c.expiry[key] = now.Add(time.Duration(TTLSeconds) * time.Second)
}

// IsKnownReyden returns whether warehouseID on host is known (unexpired) to reject Thrift.
func (c *Cache) IsKnownReyden(host, warehouseID string) bool {
	if warehouseID == "" {
		return false
	}
	now := time.Now()
	c.mu.RLock()
	defer c.mu.RUnlock()

	key := makeKey(host, warehouseID)
	deadline, ok := c.expiry[key]
	if !ok {
		return false
	}
	if deadline.Before(now) {
		// Entry is expired but not yet lazily evicted.
		// Lazy eviction happens on the next MarkReyden call.
		return false
	}
	return true
}

// Clear resets the cache. Intended for tests.
func (c *Cache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.expiry = make(map[[2]string]time.Time)
}

// Global singleton cache, multi-tenant safe via the host component of the key.
var globalCache = NewCache()

// MarkReyden records that warehouseID on host rejects the Thrift protocol.
func MarkReyden(host, warehouseID string) {
	globalCache.MarkReyden(host, warehouseID)
}

// IsKnownReyden returns whether warehouseID on host is known to reject Thrift.
func IsKnownReyden(host, warehouseID string) bool {
	return globalCache.IsKnownReyden(host, warehouseID)
}

// ClearCache resets the global cache. Intended for tests.
func ClearCache() {
	globalCache.Clear()
}
