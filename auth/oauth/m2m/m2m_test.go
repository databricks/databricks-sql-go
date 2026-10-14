package m2m

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestM2MScopes(t *testing.T) {
	t.Run("default should be [all-apis]", func(t *testing.T) {
		auth := NewAuthenticator("id", "secret", "staging.cloud.company.com").(*authClient)
		assert.Equal(t, "id", auth.clientID)
		assert.Equal(t, "secret", auth.clientSecret)
		assert.Equal(t, []string{"all-apis"}, auth.scopes)

		auth = NewAuthenticatorWithScopes("id", "secret", "staging.cloud.company.com", nil).(*authClient)
		assert.Equal(t, "id", auth.clientID)
		assert.Equal(t, "secret", auth.clientSecret)
		assert.Equal(t, []string{"all-apis"}, auth.scopes)

		auth = NewAuthenticatorWithScopes("id", "secret", "staging.cloud.company.com", []string{}).(*authClient)
		assert.Equal(t, "id", auth.clientID)
		assert.Equal(t, "secret", auth.clientSecret)
		assert.Equal(t, []string{"all-apis"}, auth.scopes)
	})

	t.Run("should not add all-apis to passed scopes", func(t *testing.T) {
		auth := NewAuthenticatorWithScopes("id", "secret", "staging.cloud.company.com", []string{"sql"}).(*authClient)
		assert.Equal(t, "id", auth.clientID)
		assert.Equal(t, "secret", auth.clientSecret)
		assert.Equal(t, []string{"sql"}, auth.scopes)
	})

	t.Run("should keep all-apis if already in passed scopes", func(t *testing.T) {
		auth := NewAuthenticatorWithScopes("id", "secret", "staging.cloud.company.com", []string{"all-apis", "my-scope"}).(*authClient)
		assert.Equal(t, "id", auth.clientID)
		assert.Equal(t, "secret", auth.clientSecret)
		assert.Equal(t, []string{"all-apis", "my-scope"}, auth.scopes)
	})
}

// recordingTransport answers every request with a fixed token body and keeps
// the User-Agent it was sent with.
type recordingTransport struct {
	userAgents []string
}

func (rt *recordingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	rt.userAgents = append(rt.userAgents, req.Header.Get("User-Agent"))
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": {"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"access_token":"tok","token_type":"Bearer","expires_in":3600}`)),
		Request:    req,
	}, nil
}

func TestAuthenticateUsesInjectedHTTPClient(t *testing.T) {
	// Azure hosts resolve their endpoints offline, so no discovery request is made.
	const host = "adb-1.2.azuredatabricks.net"

	rt := &recordingTransport{}
	auth := NewAuthenticator("id", "secret", host).(*authClient)
	auth.SetHTTPClient(&http.Client{Transport: &userAgentTransport{base: rt, userAgent: "driver/1.0 (isv)"}})

	req, _ := http.NewRequest(http.MethodPost, "https://"+host+"/sql/1.0/warehouses/x", nil)
	require.NoError(t, auth.Authenticate(req))
	assert.Equal(t, "Bearer tok", req.Header.Get("Authorization"))
	assert.Equal(t, []string{"driver/1.0 (isv)"}, rt.userAgents)

	// The cached token source is reused: no second token request.
	req2, _ := http.NewRequest(http.MethodPost, "https://"+host+"/sql/1.0/warehouses/x", nil)
	require.NoError(t, auth.Authenticate(req2))
	assert.Len(t, rt.userAgents, 1)
}

type userAgentTransport struct {
	base      http.RoundTripper
	userAgent string
}

func (t *userAgentTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Set("User-Agent", t.userAgent)
	return t.base.RoundTrip(req)
}
