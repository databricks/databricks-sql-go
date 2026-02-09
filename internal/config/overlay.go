package config

import (
	"context"
	"net/http"
	"strconv"
)

// ConfigValue represents a configuration value that can be set by client or resolved from server.
// This implements the config overlay pattern: client > server > default
//
// T is the type of the configuration value (bool, string, int, etc.)
//
// Example usage:
//
//	type MyConfig struct {
//	    EnableFeature ConfigValue[bool]
//	    BatchSize     ConfigValue[int]
//	}
//
//	// Client explicitly sets value (overrides server)
//	config.EnableFeature = NewConfigValue(true)
//
//	// Client doesn't set value (use server)
//	config.EnableFeature = ConfigValue[bool]{} // nil/unset
//
//	// Resolve value with overlay priority
//	enabled := config.EnableFeature.Resolve(ctx, serverResolver, defaultValue)
type ConfigValue[T any] struct {
	// value is the client-set configuration value
	// nil = not set by client (use server config)
	// non-nil = explicitly set by client (overrides server)
	value *T
}

// NewConfigValue creates a ConfigValue with a client-set value.
// The value will override any server-side configuration.
func NewConfigValue[T any](value T) ConfigValue[T] {
	return ConfigValue[T]{value: &value}
}

// IsSet returns true if the client explicitly set this configuration value.
func (cv ConfigValue[T]) IsSet() bool {
	return cv.value != nil
}

// Get returns the client-set value and whether it was set.
// If not set, returns zero value and false.
func (cv ConfigValue[T]) Get() (T, bool) {
	if cv.value != nil {
		return *cv.value, true
	}
	var zero T
	return zero, false
}

// ServerResolver defines how to fetch a configuration value from the server.
// Implementations should handle caching, retries, and error handling.
type ServerResolver[T any] interface {
	// Resolve fetches the configuration value from the server.
	// Returns the value and any error encountered.
	// On error, the config overlay will fall back to the default value.
	Resolve(ctx context.Context, host string, httpClient *http.Client) (T, error)
}

// Resolve applies config overlay priority to determine the final value:
//
//	Priority 1: Client Config - if explicitly set (overrides server)
//	Priority 2: Server Config - resolved via serverResolver (when client doesn't set)
//	Priority 3: Default Value - used when server unavailable/errors (fail-safe)
//
// Parameters:
//   - ctx: Context for server requests
//   - serverResolver: How to fetch from server (can be nil if no server config)
//   - defaultValue: Fail-safe default when client unset and server unavailable
//
// Returns: The resolved configuration value following overlay priority
func (cv ConfigValue[T]) Resolve(
	ctx context.Context,
	serverResolver ServerResolver[T],
	defaultValue T,
) T {
	// Priority 1: Client explicitly set (overrides everything)
	if cv.value != nil {
		return *cv.value
	}

	// Priority 2: Try server config (if resolver provided)
	if serverResolver != nil {
		// Note: We pass empty host/httpClient here. Actual resolver should have these injected
		// This is a simplified interface - real usage would inject dependencies
		if serverValue, err := serverResolver.Resolve(ctx, "", nil); err == nil {
			return serverValue
		}
	}

	// Priority 3: Fail-safe default
	return defaultValue
}

// ResolveWithContext is a more flexible version that takes host and httpClient.
// This is the recommended method for production use.
func (cv ConfigValue[T]) ResolveWithContext(
	ctx context.Context,
	host string,
	httpClient *http.Client,
	serverResolver ServerResolver[T],
	defaultValue T,
) T {
	// Priority 1: Client explicitly set (overrides everything)
	if cv.value != nil {
		return *cv.value
	}

	// Priority 2: Try server config (if resolver provided)
	if serverResolver != nil {
		if serverValue, err := serverResolver.Resolve(ctx, host, httpClient); err == nil {
			return serverValue
		}
	}

	// Priority 3: Fail-safe default
	return defaultValue
}

// ParseBoolConfigValue parses a string value into a ConfigValue[bool].
// Returns unset ConfigValue if the parameter is not present.
//
// Example:
//
//	params := map[string]string{"enableFeature": "true"}
//	value := ParseBoolConfigValue(params, "enableFeature")
//	// value.IsSet() == true, value.Get() == (true, true)
func ParseBoolConfigValue(params map[string]string, key string) ConfigValue[bool] {
	if v, ok := params[key]; ok {
		enabled := (v == "true" || v == "1")
		return NewConfigValue(enabled)
	}
	return ConfigValue[bool]{} // Unset
}

// ParseStringConfigValue parses a string value into a ConfigValue[string].
// Returns unset ConfigValue if the parameter is not present.
func ParseStringConfigValue(params map[string]string, key string) ConfigValue[string] {
	if v, ok := params[key]; ok {
		return NewConfigValue(v)
	}
	return ConfigValue[string]{} // Unset
}

// ParseIntConfigValue parses a string value into a ConfigValue[int].
// Returns unset ConfigValue if the parameter is not present or invalid.
func ParseIntConfigValue(params map[string]string, key string) ConfigValue[int] {
	if v, ok := params[key]; ok {
		if i, err := strconv.Atoi(v); err == nil {
			return NewConfigValue(i)
		}
	}
	return ConfigValue[int]{} // Unset
}
