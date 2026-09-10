package warehouse_cache

import (
	"testing"
	"time"
)

func TestExtractWarehouseID(t *testing.T) {
	tests := []struct {
		path     string
		expected string
	}{
		{"/sql/1.0/warehouses/abc123", "abc123"},
		{"/sql/1.0/endpoints/def456", "def456"},
		{"/sql/1.0/warehouses/abc123?o=42", "abc123"},
		{"/sql/1.0/warehouses/abc123?o=42&other=val", "abc123"},
		{"sql/1.0/warehouses/wh", "wh"},
		{"/sql/protocolv1/o/1234567890/0101-cluster", ""},
		{"", ""},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			result := ExtractWarehouseID(tt.path)
			if result != tt.expected {
				t.Errorf("ExtractWarehouseID(%q) = %q, want %q", tt.path, result, tt.expected)
			}
		})
	}
}

func TestCacheMarkAndIsKnown(t *testing.T) {
	cache := NewCache()
	host := "host.example.com"
	warehouseID := "wh-123"

	// Initially not known
	if cache.IsKnownReyden(host, warehouseID) {
		t.Errorf("warehouse should not be known initially")
	}

	// After marking, should be known
	cache.MarkReyden(host, warehouseID)
	if !cache.IsKnownReyden(host, warehouseID) {
		t.Errorf("warehouse should be known after marking")
	}
}

func TestCacheHostCaseInsensitive(t *testing.T) {
	cache := NewCache()
	warehouseID := "wh-123"

	cache.MarkReyden("Host.Example.COM", warehouseID)

	// Lookup with different case should still find it
	if !cache.IsKnownReyden("host.example.com", warehouseID) {
		t.Errorf("warehouse lookup should be case-insensitive")
	}
}

func TestCacheDistinctHostsDoNotCollide(t *testing.T) {
	cache := NewCache()
	warehouseID := "wh-123"

	cache.MarkReyden("host-a", warehouseID)

	// Same warehouse id on different host should not be treated as Reyden
	if cache.IsKnownReyden("host-b", warehouseID) {
		t.Errorf("different hosts should not collide")
	}
}

func TestCacheDistinctWarehousesDoNotCollide(t *testing.T) {
	cache := NewCache()
	host := "host.example.com"

	cache.MarkReyden(host, "wh-a")

	// Different warehouse on same host should not collide
	if cache.IsKnownReyden(host, "wh-b") {
		t.Errorf("different warehouses should not collide")
	}
}

func TestCacheExpiry(t *testing.T) {
	cache := &Cache{expiry: make(map[[2]string]time.Time)}
	host := "host.example.com"
	warehouseID := "wh-123"

	// Mark with zero TTL (entry immediately expires)
	now := time.Now()
	key := makeKey(host, warehouseID)
	cache.mu.Lock()
	cache.expiry[key] = now.Add(-1 * time.Nanosecond)
	cache.mu.Unlock()

	// Should be expired
	if cache.IsKnownReyden(host, warehouseID) {
		t.Errorf("warehouse should be expired")
	}
}

func TestCacheSweepsExpiredEntries(t *testing.T) {
	cache := &Cache{expiry: make(map[[2]string]time.Time)}
	host := "host.example.com"

	// Add an entry that's already expired
	now := time.Now()
	key1 := makeKey(host, "old-wh")
	key2 := makeKey(host, "new-wh")

	cache.mu.Lock()
	cache.expiry[key1] = now.Add(-1 * time.Second)
	cache.mu.Unlock()

	if len(cache.expiry) != 1 {
		t.Errorf("cache should have 1 entry")
	}

	// Mark a new warehouse should trigger sweep
	cache.MarkReyden(host, "new-wh")

	cache.mu.RLock()
	defer cache.mu.RUnlock()

	// Old entry should be gone
	if _, ok := cache.expiry[key1]; ok {
		t.Errorf("expired entry should be swept")
	}
	// New entry should be present
	if _, ok := cache.expiry[key2]; !ok {
		t.Errorf("new entry should be present after sweep")
	}
}

func TestGlobalCache(t *testing.T) {
	ClearCache()

	host := "example.com"
	warehouseID := "wh-global"

	if IsKnownReyden(host, warehouseID) {
		t.Errorf("warehouse should not be known initially")
	}

	MarkReyden(host, warehouseID)

	if !IsKnownReyden(host, warehouseID) {
		t.Errorf("warehouse should be known after marking")
	}

	ClearCache()

	if IsKnownReyden(host, warehouseID) {
		t.Errorf("warehouse should not be known after clear")
	}
}

func TestCacheEmptyWarehouseID(t *testing.T) {
	cache := NewCache()

	// Should not crash or add empty entries
	cache.MarkReyden("host", "")

	if cache.IsKnownReyden("host", "") {
		t.Errorf("empty warehouse id should not be cached")
	}
}
