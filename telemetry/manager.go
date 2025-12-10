package telemetry

import (
	"net/http"
	"sync"

	"github.com/databricks/databricks-sql-go/logger"
)

// clientManager manages one telemetry client per host.
// Prevents rate limiting by sharing clients across connections.
type clientManager struct {
	mu      sync.RWMutex
	clients map[string]*clientHolder
}

// clientHolder holds a telemetry client and its reference count.
type clientHolder struct {
	client   *telemetryClient
	refCount int
}

var (
	managerOnce     sync.Once
	managerInstance *clientManager
)

// getClientManager returns the singleton instance.
func getClientManager() *clientManager {
	managerOnce.Do(func() {
		managerInstance = &clientManager{
			clients: make(map[string]*clientHolder),
		}
	})
	return managerInstance
}

// getOrCreateClient gets or creates a telemetry client for the host.
// Increments reference count.
func (m *clientManager) getOrCreateClient(host string, httpClient *http.Client, cfg *Config) *telemetryClient {
	m.mu.Lock()
	defer m.mu.Unlock()

	holder, exists := m.clients[host]
	if !exists {
		holder = &clientHolder{
			client: newTelemetryClient(host, httpClient, cfg),
		}
		m.clients[host] = holder
		_ = holder.client.start() // Start background flush goroutine
	}
	holder.refCount++
	return holder.client
}

// releaseClient decrements reference count for the host.
// Closes and removes client when ref count reaches zero.
func (m *clientManager) releaseClient(host string) error {
	m.mu.Lock()
	holder, exists := m.clients[host]
	if !exists {
		m.mu.Unlock()
		return nil
	}

	holder.refCount--
	if holder.refCount < 0 {
		// This should never happen - indicates a bug where releaseClient was called more than getOrCreateClient
		logger.Logger.Debug().Str("host", host).Int("refCount", holder.refCount).Msg("telemetry client refCount became negative")
	}
	if holder.refCount <= 0 {
		delete(m.clients, host)
		m.mu.Unlock()
		return holder.client.close() // Close and flush
	}

	m.mu.Unlock()
	return nil
}
