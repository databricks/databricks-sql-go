package telemetry

import (
	"net/http"
	"sync"

	"github.com/databricks/databricks-sql-go/logger"
)

// clientManager manages one telemetry client per host.
//
// Design:
// - Creates a single telemetryClient per host, shared across multiple connections
// - Prevents rate limiting by consolidating telemetry from all connections to the same host
// - Uses reference counting to track active connections and cleanup when last connection closes
// - Thread-safe using sync.RWMutex for concurrent access from multiple goroutines
//
// The manager handles synchronization for client lifecycle (create/release),
// while the telemetryClient itself must be thread-safe for concurrent data operations.
type clientManager struct {
	mu      sync.RWMutex             // Protects the clients map
	clients map[string]*clientHolder // host -> client holder mapping
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
		client := newTelemetryClient(host, httpClient, cfg)
		if err := client.start(); err != nil {
			// Failed to start client, don't add to map
			logger.Logger.Debug().Str("host", host).Err(err).Msg("failed to start telemetry client")
			return nil
		}
		holder = &clientHolder{
			client: client,
		}
		m.clients[host] = holder
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

// shutdown closes all telemetry clients and clears the manager.
// This should be called on application shutdown.
func (m *clientManager) shutdown() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var lastErr error
	for host, holder := range m.clients {
		if err := holder.client.close(); err != nil {
			logger.Logger.Debug().Str("host", host).Err(err).Msg("error closing telemetry client during shutdown")
			lastErr = err
		}
	}
	// Clear the map
	m.clients = make(map[string]*clientHolder)
	return lastErr
}
