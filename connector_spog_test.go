package dbsql

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractSpogHeaders(t *testing.T) {
	tests := []struct {
		name     string
		httpPath string
		want     map[string]string
	}{
		{
			name:     "no query string returns nil",
			httpPath: "/sql/1.0/warehouses/abc123",
			want:     nil,
		},
		{
			name:     "empty httpPath returns nil",
			httpPath: "",
			want:     nil,
		},
		{
			name:     "query string with o= extracts org id",
			httpPath: "/sql/1.0/warehouses/abc123?o=7064161269814046",
			want:     map[string]string{"x-databricks-org-id": "7064161269814046"},
		},
		{
			name:     "query string without o= returns nil",
			httpPath: "/sql/1.0/warehouses/abc123?other=val",
			want:     nil,
		},
		{
			name:     "empty o= value returns nil",
			httpPath: "/sql/1.0/warehouses/abc123?o=",
			want:     nil,
		},
		{
			name:     "o= among multiple params extracts correctly",
			httpPath: "/sql/1.0/warehouses/abc?foo=1&o=12345&bar=2",
			want:     map[string]string{"x-databricks-org-id": "12345"},
		},
		{
			name:     "first o= wins when duplicated",
			httpPath: "/sql/1.0/warehouses/abc?o=first&o=second",
			want:     map[string]string{"x-databricks-org-id": "first"},
		},
		{
			name:     "just ? with nothing after returns nil",
			httpPath: "/sql/1.0/warehouses/abc?",
			want:     nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := extractSpogHeaders(tc.httpPath)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestHeaderInjectingTransport_InjectsHeader(t *testing.T) {
	var gotHeader string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeader = r.Header.Get("x-databricks-org-id")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := withSpogHeaders(&http.Client{}, map[string]string{
		"x-databricks-org-id": "7064161269814046",
	})

	req, err := http.NewRequest("GET", srv.URL, nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	assert.Equal(t, "7064161269814046", gotHeader, "SPOG header should be injected")
}

func TestHeaderInjectingTransport_DoesNotOverrideCallerSet(t *testing.T) {
	var gotHeader string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeader = r.Header.Get("x-databricks-org-id")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := withSpogHeaders(&http.Client{}, map[string]string{
		"x-databricks-org-id": "from-wrapper",
	})

	req, err := http.NewRequest("GET", srv.URL, nil)
	require.NoError(t, err)
	req.Header.Set("x-databricks-org-id", "from-caller")
	resp, err := client.Do(req)
	require.NoError(t, err)
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	assert.Equal(t, "from-caller", gotHeader, "caller-set header must not be overridden")
}

func TestHeaderInjectingTransport_PreservesOtherHeaders(t *testing.T) {
	var gotAuth, gotSpog, gotCustom string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotSpog = r.Header.Get("x-databricks-org-id")
		gotCustom = r.Header.Get("X-Custom")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := withSpogHeaders(&http.Client{}, map[string]string{
		"x-databricks-org-id": "abc",
	})

	req, err := http.NewRequest("GET", srv.URL, nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer xxx")
	req.Header.Set("X-Custom", "hello")
	resp, err := client.Do(req)
	require.NoError(t, err)
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	assert.Equal(t, "Bearer xxx", gotAuth)
	assert.Equal(t, "hello", gotCustom)
	assert.Equal(t, "abc", gotSpog)
}

func TestWithSpogHeaders_OriginalClientUntouched(t *testing.T) {
	originalTransport := &countingTransport{}
	original := &http.Client{Transport: originalTransport}

	wrapped := withSpogHeaders(original, map[string]string{"x-databricks-org-id": "x"})

	// Original client's transport should NOT be the wrapper type.
	_, isWrapped := original.Transport.(*headerInjectingTransport)
	assert.False(t, isWrapped, "original client's transport must not be mutated")

	// Wrapped client MUST have the wrapper.
	_, isWrapped = wrapped.Transport.(*headerInjectingTransport)
	assert.True(t, isWrapped, "wrapped client must have headerInjectingTransport")
}

type countingTransport struct {
	count int
}

func (c *countingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	c.count++
	return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(""))}, nil
}
