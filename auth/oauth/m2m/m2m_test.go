package m2m

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
