package telemetry

import (
	"net/http"
	"sync"
	"testing"
)

func TestGetClientManager_Singleton(t *testing.T) {
	// Reset singleton for testing
	managerInstance = nil
	managerOnce = sync.Once{}

	manager1 := getClientManager()
	manager2 := getClientManager()

	if manager1 != manager2 {
		t.Error("Expected singleton instances to be the same")
	}
}

func TestClientManager_GetOrCreateClient(t *testing.T) {
	manager := &clientManager{
		clients: make(map[string]*clientHolder),
	}

	host := "test-host.databricks.com"
	httpClient := &http.Client{}
	cfg := DefaultConfig()

	// First call should create client and increment refCount to 1
	client1 := manager.getOrCreateClient(host, httpClient, cfg)
	if client1 == nil {
		t.Fatal("Expected client to be created")
	}

	holder, exists := manager.clients[host]
	if !exists {
		t.Fatal("Expected holder to exist in clients map")
	}
	if holder.refCount != 1 {
		t.Errorf("Expected refCount to be 1, got %d", holder.refCount)
	}
	if !client1.started {
		t.Error("Expected client to be started")
	}

	// Second call should reuse client and increment refCount to 2
	client2 := manager.getOrCreateClient(host, httpClient, cfg)
	if client2 != client1 {
		t.Error("Expected to get the same client instance")
	}
	if holder.refCount != 2 {
		t.Errorf("Expected refCount to be 2, got %d", holder.refCount)
	}
}

func TestClientManager_GetOrCreateClient_DifferentHosts(t *testing.T) {
	manager := &clientManager{
		clients: make(map[string]*clientHolder),
	}

	host1 := "host1.databricks.com"
	host2 := "host2.databricks.com"
	httpClient := &http.Client{}
	cfg := DefaultConfig()

	client1 := manager.getOrCreateClient(host1, httpClient, cfg)
	client2 := manager.getOrCreateClient(host2, httpClient, cfg)

	if client1 == client2 {
		t.Error("Expected different clients for different hosts")
	}

	if len(manager.clients) != 2 {
		t.Errorf("Expected 2 clients in manager, got %d", len(manager.clients))
	}
}

func TestClientManager_ReleaseClient(t *testing.T) {
	manager := &clientManager{
		clients: make(map[string]*clientHolder),
	}

	host := "test-host.databricks.com"
	httpClient := &http.Client{}
	cfg := DefaultConfig()

	// Create client with refCount = 2
	manager.getOrCreateClient(host, httpClient, cfg)
	manager.getOrCreateClient(host, httpClient, cfg)

	// First release should decrement to 1
	err := manager.releaseClient(host)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	holder, exists := manager.clients[host]
	if !exists {
		t.Fatal("Expected holder to still exist")
	}
	if holder.refCount != 1 {
		t.Errorf("Expected refCount to be 1, got %d", holder.refCount)
	}
	if holder.client.closed {
		t.Error("Expected client not to be closed yet")
	}

	// Second release should remove client
	err = manager.releaseClient(host)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	_, exists = manager.clients[host]
	if exists {
		t.Error("Expected holder to be removed when refCount reaches 0")
	}
	if !holder.client.closed {
		t.Error("Expected client to be closed when removed")
	}
}

func TestClientManager_ReleaseClient_NonExistent(t *testing.T) {
	manager := &clientManager{
		clients: make(map[string]*clientHolder),
	}

	// Release non-existent host should not panic or error
	err := manager.releaseClient("non-existent-host.databricks.com")
	if err != nil {
		t.Errorf("Expected no error for non-existent host, got %v", err)
	}
}

func TestClientManager_ConcurrentAccess(t *testing.T) {
	manager := &clientManager{
		clients: make(map[string]*clientHolder),
	}

	host := "test-host.databricks.com"
	httpClient := &http.Client{}
	cfg := DefaultConfig()
	numGoroutines := 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Concurrent getOrCreateClient
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			client := manager.getOrCreateClient(host, httpClient, cfg)
			if client == nil {
				t.Error("Expected client to be created")
			}
		}()
	}
	wg.Wait()

	// Verify refCount
	holder, exists := manager.clients[host]
	if !exists {
		t.Fatal("Expected holder to exist")
	}
	if holder.refCount != numGoroutines {
		t.Errorf("Expected refCount to be %d, got %d", numGoroutines, holder.refCount)
	}

	// Concurrent releaseClient
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			_ = manager.releaseClient(host)
		}()
	}
	wg.Wait()

	// Verify client is removed
	_, exists = manager.clients[host]
	if exists {
		t.Error("Expected holder to be removed after all releases")
	}
}

func TestClientManager_ConcurrentAccessMultipleHosts(t *testing.T) {
	manager := &clientManager{
		clients: make(map[string]*clientHolder),
	}

	hosts := []string{
		"host1.databricks.com",
		"host2.databricks.com",
		"host3.databricks.com",
	}
	httpClient := &http.Client{}
	cfg := DefaultConfig()
	numGoroutinesPerHost := 50

	var wg sync.WaitGroup

	// Concurrent access to multiple hosts
	for _, host := range hosts {
		for i := 0; i < numGoroutinesPerHost; i++ {
			wg.Add(1)
			go func(h string) {
				defer wg.Done()
				_ = manager.getOrCreateClient(h, httpClient, cfg)
			}(host)
		}
	}
	wg.Wait()

	// Verify all hosts have correct refCount
	if len(manager.clients) != len(hosts) {
		t.Errorf("Expected %d clients, got %d", len(hosts), len(manager.clients))
	}

	for _, host := range hosts {
		holder, exists := manager.clients[host]
		if !exists {
			t.Errorf("Expected holder for host %s to exist", host)
			continue
		}
		if holder.refCount != numGoroutinesPerHost {
			t.Errorf("Expected refCount for host %s to be %d, got %d", host, numGoroutinesPerHost, holder.refCount)
		}
	}
}

func TestClientManager_ReleaseClientPartial(t *testing.T) {
	manager := &clientManager{
		clients: make(map[string]*clientHolder),
	}

	host := "test-host.databricks.com"
	httpClient := &http.Client{}
	cfg := DefaultConfig()

	// Create 5 references
	for i := 0; i < 5; i++ {
		manager.getOrCreateClient(host, httpClient, cfg)
	}

	// Release 3 references
	for i := 0; i < 3; i++ {
		_ = manager.releaseClient(host)
	}

	// Should still have 2 references
	holder, exists := manager.clients[host]
	if !exists {
		t.Fatal("Expected holder to still exist")
	}
	if holder.refCount != 2 {
		t.Errorf("Expected refCount to be 2, got %d", holder.refCount)
	}
	if holder.client.closed {
		t.Error("Expected client not to be closed with remaining references")
	}
}

func TestClientManager_ClientStartCalled(t *testing.T) {
	manager := &clientManager{
		clients: make(map[string]*clientHolder),
	}

	host := "test-host.databricks.com"
	httpClient := &http.Client{}
	cfg := DefaultConfig()

	client := manager.getOrCreateClient(host, httpClient, cfg)

	if !client.started {
		t.Error("Expected start() to be called on new client")
	}
}

func TestClientManager_ClientCloseCalled(t *testing.T) {
	manager := &clientManager{
		clients: make(map[string]*clientHolder),
	}

	host := "test-host.databricks.com"
	httpClient := &http.Client{}
	cfg := DefaultConfig()

	client := manager.getOrCreateClient(host, httpClient, cfg)
	_ = manager.releaseClient(host)

	if !client.closed {
		t.Error("Expected close() to be called when client is removed")
	}
}

func TestClientManager_MultipleGetOrCreateSameClient(t *testing.T) {
	manager := &clientManager{
		clients: make(map[string]*clientHolder),
	}

	host := "test-host.databricks.com"
	httpClient := &http.Client{}
	cfg := DefaultConfig()

	// Get same client multiple times
	client1 := manager.getOrCreateClient(host, httpClient, cfg)
	client2 := manager.getOrCreateClient(host, httpClient, cfg)
	client3 := manager.getOrCreateClient(host, httpClient, cfg)

	// All should be same instance
	if client1 != client2 || client2 != client3 {
		t.Error("Expected all calls to return the same client instance")
	}

	// Verify refCount is 3
	holder := manager.clients[host]
	if holder.refCount != 3 {
		t.Errorf("Expected refCount to be 3, got %d", holder.refCount)
	}
}

func TestClientManager_Shutdown(t *testing.T) {
	manager := &clientManager{
		clients: make(map[string]*clientHolder),
	}

	hosts := []string{
		"host1.databricks.com",
		"host2.databricks.com",
		"host3.databricks.com",
	}
	httpClient := &http.Client{}
	cfg := DefaultConfig()

	// Create clients for multiple hosts
	clients := make([]*telemetryClient, 0, len(hosts))
	for _, host := range hosts {
		client := manager.getOrCreateClient(host, httpClient, cfg)
		clients = append(clients, client)
	}

	// Verify clients are created
	if len(manager.clients) != len(hosts) {
		t.Errorf("Expected %d clients, got %d", len(hosts), len(manager.clients))
	}

	// Shutdown should close all clients
	err := manager.shutdown()
	if err != nil {
		t.Errorf("Expected no error during shutdown, got %v", err)
	}

	// Verify all clients are closed
	for i, client := range clients {
		if !client.closed {
			t.Errorf("Expected client for host %s to be closed", hosts[i])
		}
	}

	// Verify clients map is empty
	if len(manager.clients) != 0 {
		t.Errorf("Expected clients map to be empty after shutdown, got %d clients", len(manager.clients))
	}
}

func TestClientManager_ShutdownWithActiveRefs(t *testing.T) {
	manager := &clientManager{
		clients: make(map[string]*clientHolder),
	}

	host := "test-host.databricks.com"
	httpClient := &http.Client{}
	cfg := DefaultConfig()

	// Create client with multiple references
	client := manager.getOrCreateClient(host, httpClient, cfg)
	manager.getOrCreateClient(host, httpClient, cfg)
	manager.getOrCreateClient(host, httpClient, cfg)

	holder := manager.clients[host]
	if holder.refCount != 3 {
		t.Errorf("Expected refCount to be 3, got %d", holder.refCount)
	}

	// Shutdown should still close client even with active references
	err := manager.shutdown()
	if err != nil {
		t.Errorf("Expected no error during shutdown, got %v", err)
	}

	// Verify client is closed
	if !client.closed {
		t.Error("Expected client to be closed after shutdown")
	}

	// Verify clients map is empty
	if len(manager.clients) != 0 {
		t.Errorf("Expected clients map to be empty after shutdown, got %d clients", len(manager.clients))
	}
}

func TestClientManager_ShutdownEmptyManager(t *testing.T) {
	manager := &clientManager{
		clients: make(map[string]*clientHolder),
	}

	// Shutdown on empty manager should not error
	err := manager.shutdown()
	if err != nil {
		t.Errorf("Expected no error shutting down empty manager, got %v", err)
	}

	// Verify map is still empty
	if len(manager.clients) != 0 {
		t.Errorf("Expected clients map to be empty, got %d clients", len(manager.clients))
	}
}
