package m2m

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTokenCachesPartitionByTransport(t *testing.T) {
	server, hits := tokenServer(t, 100)
	useTokenEndpoint(t, server.URL, time.Second)
	shared := NewAuthenticator("id", "secret", "adb-test.azuredatabricks.net").(*authClient)
	firstTransport := http.DefaultTransport.(*http.Transport).Clone()
	secondTransport := firstTransport.Clone()
	t.Cleanup(firstTransport.CloseIdleConnections)
	t.Cleanup(secondTransport.CloseIdleConnections)
	firstClient := &http.Client{Transport: firstTransport}
	secondClient := &http.Client{Transport: secondTransport}
	first := shared.WithHTTPClientForTransport(firstClient, firstTransport).(*authClient)
	builtIn := shared.WithHTTPClientForTransport(secondClient, nil).(*authClient)
	second := shared.WithHTTPClientForTransport(secondClient, secondTransport).(*authClient)
	for index, authenticator := range []*authClient{first, builtIn, second} {
		_, err := authenticate(t, authenticator, 5*time.Second)
		require.NoError(t, err)
		assert.Equal(t, int32(index+1), hits.Load())
		authenticator.cache.token.Expiry = time.Now().Add(time.Hour)
	}
	assert.NotSame(t, first.cache, builtIn.cache)
	assert.NotSame(t, first.cache.config, builtIn.cache.config)
	assert.NotSame(t, first.cache.token, builtIn.cache.token)
	assert.NotSame(t, second.cache, builtIn.cache)
	for _, authenticator := range []*authClient{
		second.WithHTTPClientForTransport(firstClient, firstTransport).(*authClient),
		first.WithHTTPClientForTransport(firstClient, nil).(*authClient),
		shared.WithHTTPClient(secondClient).(*authClient),
	} {
		_, err := authenticate(t, authenticator, 5*time.Second)
		require.NoError(t, err)
		assert.Equal(t, int32(3), hits.Load())
	}
	_, err := authenticate(t, shared, 5*time.Second)
	require.NoError(t, err)
	assert.Equal(t, int32(4), hits.Load())
	results := make(chan *authClient, 20)
	for range 20 {
		go func() { results <- shared.WithHTTPClientForTransport(firstClient, nil).(*authClient) }()
	}
	for range 20 {
		assert.Same(t, builtIn.cache, (<-results).cache)
	}
}

func TestNonComparableTransportsDoNotShareCaches(t *testing.T) {
	shared := NewAuthenticator("id", "secret", "adb-test.azuredatabricks.net").(*authClient)
	transport := m2mCacheRoundTripper(func(*http.Request) (*http.Response, error) { return nil, nil })
	for _, candidate := range []http.RoundTripper{transport, struct{ http.RoundTripper }{transport}} {
		first := shared.WithHTTPClientForTransport(&http.Client{Transport: candidate}, candidate).(*authClient)
		second := shared.WithHTTPClientForTransport(&http.Client{Transport: candidate}, candidate).(*authClient)
		assert.NotSame(t, first.cache, second.cache)
		assert.Same(t, first.caches, second.caches)
	}
}
