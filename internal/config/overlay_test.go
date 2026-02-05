package config

import (
	"context"
	"errors"
	"net/http"
	"testing"
)

// mockServerResolver is a test helper for ServerResolver
type mockServerResolver[T any] struct {
	value T
	err   error
}

func (m *mockServerResolver[T]) Resolve(ctx context.Context, host string, httpClient *http.Client) (T, error) {
	return m.value, m.err
}

func TestConfigValue_IsSet(t *testing.T) {
	t.Run("unset value", func(t *testing.T) {
		var cv ConfigValue[bool]
		if cv.IsSet() {
			t.Error("Expected IsSet to return false for unset value")
		}
	})

	t.Run("set value", func(t *testing.T) {
		cv := NewConfigValue(true)
		if !cv.IsSet() {
			t.Error("Expected IsSet to return true for set value")
		}
	})
}

func TestConfigValue_Get(t *testing.T) {
	t.Run("unset value", func(t *testing.T) {
		var cv ConfigValue[bool]
		val, ok := cv.Get()
		if ok {
			t.Error("Expected Get to return false for unset value")
		}
		if val != false {
			t.Error("Expected Get to return zero value for unset value")
		}
	})

	t.Run("set value", func(t *testing.T) {
		cv := NewConfigValue(true)
		val, ok := cv.Get()
		if !ok {
			t.Error("Expected Get to return true for set value")
		}
		if val != true {
			t.Errorf("Expected Get to return true, got %v", val)
		}
	})
}

func TestConfigValue_ResolveWithContext_Priority1_ClientOverride(t *testing.T) {
	t.Run("client true overrides server false", func(t *testing.T) {
		cv := NewConfigValue(true)
		resolver := &mockServerResolver[bool]{value: false, err: nil}
		ctx := context.Background()

		result := cv.ResolveWithContext(ctx, "host", &http.Client{}, resolver, false)

		if result != true {
			t.Error("Expected client value (true) to override server value (false)")
		}
	})

	t.Run("client false overrides server true", func(t *testing.T) {
		cv := NewConfigValue(false)
		resolver := &mockServerResolver[bool]{value: true, err: nil}
		ctx := context.Background()

		result := cv.ResolveWithContext(ctx, "host", &http.Client{}, resolver, true)

		if result != false {
			t.Error("Expected client value (false) to override server value (true)")
		}
	})

	t.Run("client set overrides server error", func(t *testing.T) {
		cv := NewConfigValue(true)
		resolver := &mockServerResolver[bool]{value: false, err: errors.New("server error")}
		ctx := context.Background()

		result := cv.ResolveWithContext(ctx, "host", &http.Client{}, resolver, false)

		if result != true {
			t.Error("Expected client value (true) to override server error")
		}
	})
}

func TestConfigValue_ResolveWithContext_Priority2_ServerConfig(t *testing.T) {
	t.Run("use server when client unset - server true", func(t *testing.T) {
		var cv ConfigValue[bool] // Unset
		resolver := &mockServerResolver[bool]{value: true, err: nil}
		ctx := context.Background()

		result := cv.ResolveWithContext(ctx, "host", &http.Client{}, resolver, false)

		if result != true {
			t.Error("Expected server value (true) when client unset")
		}
	})

	t.Run("use server when client unset - server false", func(t *testing.T) {
		var cv ConfigValue[bool] // Unset
		resolver := &mockServerResolver[bool]{value: false, err: nil}
		ctx := context.Background()

		result := cv.ResolveWithContext(ctx, "host", &http.Client{}, resolver, true)

		if result != false {
			t.Error("Expected server value (false) when client unset")
		}
	})
}

func TestConfigValue_ResolveWithContext_Priority3_Default(t *testing.T) {
	t.Run("use default when client unset and server errors", func(t *testing.T) {
		var cv ConfigValue[bool] // Unset
		resolver := &mockServerResolver[bool]{value: false, err: errors.New("server error")}
		ctx := context.Background()

		result := cv.ResolveWithContext(ctx, "host", &http.Client{}, resolver, true)

		if result != true {
			t.Error("Expected default value (true) when client unset and server errors")
		}
	})

	t.Run("use default when client unset and no resolver", func(t *testing.T) {
		var cv ConfigValue[bool] // Unset
		ctx := context.Background()

		result := cv.ResolveWithContext(ctx, "host", &http.Client{}, nil, true)

		if result != true {
			t.Error("Expected default value (true) when client unset and no resolver")
		}
	})
}

func TestConfigValue_DifferentTypes(t *testing.T) {
	t.Run("string type", func(t *testing.T) {
		cv := NewConfigValue("client-value")
		resolver := &mockServerResolver[string]{value: "server-value", err: nil}
		ctx := context.Background()

		result := cv.ResolveWithContext(ctx, "host", &http.Client{}, resolver, "default-value")

		if result != "client-value" {
			t.Errorf("Expected 'client-value', got %s", result)
		}
	})

	t.Run("int type", func(t *testing.T) {
		cv := NewConfigValue(100)
		resolver := &mockServerResolver[int]{value: 200, err: nil}
		ctx := context.Background()

		result := cv.ResolveWithContext(ctx, "host", &http.Client{}, resolver, 300)

		if result != 100 {
			t.Errorf("Expected 100, got %d", result)
		}
	})
}

func TestParseBoolConfigValue(t *testing.T) {
	t.Run("parse true", func(t *testing.T) {
		params := map[string]string{"enableFeature": "true"}
		cv := ParseBoolConfigValue(params, "enableFeature")

		if !cv.IsSet() {
			t.Error("Expected value to be set")
		}
		val, _ := cv.Get()
		if val != true {
			t.Error("Expected value to be true")
		}
	})

	t.Run("parse 1", func(t *testing.T) {
		params := map[string]string{"enableFeature": "1"}
		cv := ParseBoolConfigValue(params, "enableFeature")

		val, _ := cv.Get()
		if val != true {
			t.Error("Expected value to be true")
		}
	})

	t.Run("parse false", func(t *testing.T) {
		params := map[string]string{"enableFeature": "false"}
		cv := ParseBoolConfigValue(params, "enableFeature")

		val, _ := cv.Get()
		if val != false {
			t.Error("Expected value to be false")
		}
	})

	t.Run("missing key", func(t *testing.T) {
		params := map[string]string{}
		cv := ParseBoolConfigValue(params, "enableFeature")

		if cv.IsSet() {
			t.Error("Expected value to be unset when key missing")
		}
	})
}

func TestParseStringConfigValue(t *testing.T) {
	t.Run("parse value", func(t *testing.T) {
		params := map[string]string{"endpoint": "https://example.com"}
		cv := ParseStringConfigValue(params, "endpoint")

		val, _ := cv.Get()
		if val != "https://example.com" {
			t.Errorf("Expected 'https://example.com', got %s", val)
		}
	})

	t.Run("missing key", func(t *testing.T) {
		params := map[string]string{}
		cv := ParseStringConfigValue(params, "endpoint")

		if cv.IsSet() {
			t.Error("Expected value to be unset when key missing")
		}
	})
}

func TestParseIntConfigValue(t *testing.T) {
	t.Run("parse valid int", func(t *testing.T) {
		params := map[string]string{"batchSize": "100"}
		cv := ParseIntConfigValue(params, "batchSize")

		val, _ := cv.Get()
		if val != 100 {
			t.Errorf("Expected 100, got %d", val)
		}
	})

	t.Run("parse invalid int", func(t *testing.T) {
		params := map[string]string{"batchSize": "invalid"}
		cv := ParseIntConfigValue(params, "batchSize")

		if cv.IsSet() {
			t.Error("Expected value to be unset when int invalid")
		}
	})

	t.Run("missing key", func(t *testing.T) {
		params := map[string]string{}
		cv := ParseIntConfigValue(params, "batchSize")

		if cv.IsSet() {
			t.Error("Expected value to be unset when key missing")
		}
	})
}
