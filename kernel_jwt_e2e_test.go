//go:build cgo && databricks_kernel

package dbsql

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
)

// TestKernelE2EJWTM2MSelect1 exercises JWT private-key M2M auth end to end
// through the kernel backend: WithJWTPrivateKeyM2M(...) + WithUseKernel(true) →
// the kernel signs a client assertion with the private key, the workspace's
// OAuth IdP (e.g. Entra ID, via TokenURL) verifies it against the service
// principal's registered public key, and SELECT 1 returns a row.
//
// Gated on JWT-specific env so machines without a provisioned SP + key skip it
// (the shared kernel E2E suite only has PAT creds). Required:
//
//	DATABRICKS_HOST                  workspace hostname
//	DATABRICKS_HTTP_PATH             /sql/1.0/warehouses/<id>
//	DATABRICKS_JWT_CLIENT_ID         service principal / OAuth client id
//	DATABRICKS_JWT_KEY_FILE          path to the PEM private key
//	DATABRICKS_JWT_KID               key id (Entra: the cert x5t thumbprint)
//	DATABRICKS_JWT_PASSPHRASE        passphrase for an encrypted PKCS#8 key (optional)
//	DATABRICKS_JWT_ALGORITHM         JWT signing algorithm (optional; default RS256)
//	DATABRICKS_JWT_TOKEN_URL         IdP token endpoint (optional; OIDC discovery otherwise)
//	DATABRICKS_JWT_SCOPES            space-separated scope override (optional)
func TestKernelE2EJWTM2MSelect1(t *testing.T) {
	host := os.Getenv("DATABRICKS_HOST")
	httpPath := os.Getenv("DATABRICKS_HTTP_PATH")
	clientID := os.Getenv("DATABRICKS_JWT_CLIENT_ID")
	keyFile := os.Getenv("DATABRICKS_JWT_KEY_FILE")
	kid := os.Getenv("DATABRICKS_JWT_KID")
	if host == "" || httpPath == "" || clientID == "" || keyFile == "" || kid == "" {
		t.Skip("JWT M2M creds unset (DATABRICKS_HOST / DATABRICKS_HTTP_PATH / " +
			"DATABRICKS_JWT_CLIENT_ID / DATABRICKS_JWT_KEY_FILE / DATABRICKS_JWT_KID)")
	}

	var scopes []string
	if s := os.Getenv("DATABRICKS_JWT_SCOPES"); s != "" {
		scopes = strings.Fields(s)
	}

	connector, err := NewConnector(
		WithServerHostname(host),
		WithHTTPPath(httpPath),
		WithJWTPrivateKeyM2M(JWTPrivateKeyM2MConfig{
			ClientID:   clientID,
			KeyFile:    keyFile,
			Kid:        kid,
			Passphrase: os.Getenv("DATABRICKS_JWT_PASSPHRASE"),
			Algorithm:  os.Getenv("DATABRICKS_JWT_ALGORITHM"),
			TokenURL:   os.Getenv("DATABRICKS_JWT_TOKEN_URL"),
			Scopes:     scopes,
		}),
		WithUseKernel(true),
	)
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	var got int
	if err := db.QueryRowContext(context.Background(), "SELECT 1").Scan(&got); err != nil {
		t.Fatalf("SELECT 1 via kernel JWT M2M: %v", err)
	}
	if got != 1 {
		t.Fatalf("SELECT 1 = %d, want 1", got)
	}
}
