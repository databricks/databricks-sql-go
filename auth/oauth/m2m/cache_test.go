package m2m

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHTTPClientCopiesShareCache(t *testing.T) {
	server, hits := tokenServer(t, 100)
	useTokenEndpoint(t, server.URL, time.Second)
	shared := NewAuthenticator("id", "secret", "adb-test.azuredatabricks.net").(*authClient)
	first := shared.WithHTTPClient(server.Client()).(*authClient)
	second := first.WithHTTPClient(&http.Client{}).(*authClient)
	for _, authenticator := range []*authClient{first, second, shared} {
		assert.Same(t, shared.cache, authenticator.cache)
		_, err := authenticate(t, authenticator, 5*time.Second)
		require.NoError(t, err)
		authenticator.cache.token.Expiry = time.Now().Add(time.Hour)
	}
	assert.Equal(t, int32(1), hits.Load())
	results := make(chan *authClient, 20)
	for range 20 {
		go func() { results <- shared.WithHTTPClient(server.Client()).(*authClient) }()
	}
	for range 20 {
		assert.Same(t, shared.cache, (<-results).cache)
	}
}
