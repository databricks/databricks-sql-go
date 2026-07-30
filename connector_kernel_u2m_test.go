package dbsql

import (
	"testing"

	"github.com/databricks/databricks-sql-go/auth/oauth/m2m"
	"github.com/databricks/databricks-sql-go/auth/pat"

	"github.com/stretchr/testify/assert"
)

// u2mShaped stands in for the browser-based U2M authenticator, which exposes
// U2MClientID() (its real constructor does live OIDC discovery, unusable in a unit
// test). The kernel-U2M telemetry guard keys off exactly this method.
type u2mShaped struct{}

func (u2mShaped) U2MClientID() string { return "databricks-sql-connector" }

// TestInteractiveU2MAuthenticatorDetection pins the structural check the kernel-U2M
// telemetry guard relies on: only a U2M-shaped authenticator satisfies it, so
// PAT/M2M keep authenticated telemetry while U2M does not trigger a 2nd browser.
func TestInteractiveU2MAuthenticatorDetection(t *testing.T) {
	_, isU2M := interface{}(u2mShaped{}).(interactiveU2MAuthenticator)
	assert.True(t, isU2M, "a U2M-shaped authenticator must satisfy interactiveU2MAuthenticator")

	_, patIsU2M := interface{}(&pat.PATAuth{AccessToken: "dapi-x"}).(interactiveU2MAuthenticator)
	assert.False(t, patIsU2M, "PAT must NOT satisfy interactiveU2MAuthenticator")

	m2mAuth := m2m.NewAuthenticator("cid", "secret", "dbc-1234.cloud.databricks.com")
	_, m2mIsU2M := m2mAuth.(interactiveU2MAuthenticator)
	assert.False(t, m2mIsU2M, "M2M must NOT satisfy interactiveU2MAuthenticator")
}
