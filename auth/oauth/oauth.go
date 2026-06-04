package oauth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/rs/zerolog/log"
	"golang.org/x/oauth2"
)

// hostConfigTimeout bounds the /.well-known/databricks-config lookup (and the
// subsequent OIDC discovery) so they cannot stall connection setup; on any
// failure we fall back to bare-host OIDC discovery.
const hostConfigTimeout = 10 * time.Second

// accountIDPlaceholder is the token the host-metadata oidc_endpoint uses for the
// account id, e.g. "https://<host>/oidc/accounts/{account_id}".
const accountIDPlaceholder = "{account_id}"

var azureTenants = map[string]string{
	".dev.azuredatabricks.net":     "62a912ac-b58e-4c1d-89ea-b2dbfc7358fc",
	".staging.azuredatabricks.net": "4a67d088-db5c-48f1-9ff2-0aace800ae68",
	".azuredatabricks.net":         "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d",
	".databricks.azure.us":         "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d",
	".databricks.azure.cn":         "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d",
}

func GetEndpoint(ctx context.Context, hostName string) (oauth2.Endpoint, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	cloud := InferCloudFromHost(hostName)

	if cloud == Unknown {
		return oauth2.Endpoint{}, errors.New("unsupported cloud type")
	}

	if cloud == Azure {
		authURL := fmt.Sprintf("https://%s/oidc/oauth2/v2.0/authorize", hostName)
		tokenURL := fmt.Sprintf("https://%s/oidc/oauth2/v2.0/token", hostName)
		return oauth2.Endpoint{AuthURL: authURL, TokenURL: tokenURL}, nil
	}

	// AWS / GCP. Resolve the OIDC issuer via /.well-known/databricks-config so that
	// unified / SPOG hosts (one host fronting workspaces across multiple accounts)
	// use their account-rooted endpoint instead of the account-agnostic console login.
	// For normal workspace hosts this resolves to https://<host>/oidc, identical to
	// the previous behavior.
	//
	// NOTE: this client uses the default transport, matching the existing
	// oidc.NewProvider discovery below. A connector-supplied transport / TLS config
	// (WithTransport, WithSkipTLSHostVerify) is not yet threaded into the OAuth
	// endpoint-resolution path; that is a pre-existing limitation, tracked separately.
	client := &http.Client{Timeout: hostConfigTimeout}
	issuerURL := resolveOIDCIssuer(ctx, client, hostName)
	ctx = oidc.ClientContext(ctx, client)
	ctx = oidc.InsecureIssuerURLContext(ctx, issuerURL)
	provider, err := oidc.NewProvider(ctx, issuerURL)
	if err != nil {
		return oauth2.Endpoint{}, err
	}

	endpoint := provider.Endpoint()

	return endpoint, err
}

// hostMetadata is the subset of /.well-known/databricks-config we consume.
type hostMetadata struct {
	OIDCEndpoint string `json:"oidc_endpoint"`
	AccountID    string `json:"account_id"`
}

// resolveOIDCIssuer returns the OIDC issuer URL to use for AWS/GCP OAuth discovery.
//
// On a unified / SPOG host, the bare-host OIDC discovery doc points at the
// account-agnostic account-console login. That mints a token for the caller's
// default account, which the target workspace rejects ("Invalid Token") when the
// workspace belongs to a different account. Such hosts advertise the correct,
// account-rooted OIDC endpoint via /.well-known/databricks-config (with an
// {account_id} placeholder); we consult it and substitute the account id.
//
// For a normal workspace host the advertised endpoint is just https://<host>/oidc,
// so the result is identical to the historical bare-host issuer. Any failure or
// unusable value (endpoint absent, non-200, unparseable, missing/empty account_id,
// non-https, non-Databricks host, timeout) falls back to the bare-host issuer,
// preserving existing behavior.
func resolveOIDCIssuer(ctx context.Context, client *http.Client, hostName string) string {
	fallback := fmt.Sprintf("https://%s/oidc", hostName)

	cfgURL := fmt.Sprintf("https://%s/.well-known/databricks-config", hostName)
	meta, ok := fetchHostMetadata(ctx, client, cfgURL)
	if !ok || meta.OIDCEndpoint == "" {
		log.Debug().Msgf("oauth: no usable databricks-config for %q; using bare-host OIDC issuer", hostName)
		return fallback
	}

	// An account-rooted endpoint needs a non-empty account_id; otherwise the
	// placeholder would resolve to a malformed ".../accounts/" issuer. Fall back
	// rather than emit it (the function's documented contract).
	if strings.Contains(meta.OIDCEndpoint, accountIDPlaceholder) && meta.AccountID == "" {
		log.Warn().Msgf("oauth: databricks-config for %q has an %s placeholder but empty account_id; using bare-host OIDC issuer", hostName, accountIDPlaceholder)
		return fallback
	}

	issuer := substituteAccountID(meta)
	if !isValidDatabricksIssuer(issuer) {
		log.Warn().Msgf("oauth: databricks-config for %q advertised an unusable oidc_endpoint %q; using bare-host OIDC issuer", hostName, issuer)
		return fallback
	}
	return issuer
}

// substituteAccountID resolves the {account_id} placeholder in the advertised
// oidc_endpoint. Workspace hosts have no placeholder and are returned unchanged.
func substituteAccountID(meta hostMetadata) string {
	return strings.ReplaceAll(meta.OIDCEndpoint, accountIDPlaceholder, meta.AccountID)
}

// isValidDatabricksIssuer guards the metadata-supplied OIDC issuer before it is
// passed to discovery: it must be an https URL on a recognized Databricks domain
// with the {account_id} placeholder fully resolved. This bounds the trust placed
// in the host-supplied document (the issuer-match check is disabled via
// InsecureIssuerURLContext because the discovered issuer is cross-host) and avoids
// cleartext OAuth from an http:// endpoint.
func isValidDatabricksIssuer(issuer string) bool {
	if strings.Contains(issuer, accountIDPlaceholder) {
		return false
	}
	u, err := url.Parse(issuer)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" {
		return false
	}
	return InferCloudFromHost(u.Hostname()) != Unknown
}

// fetchHostMetadata GETs /.well-known/databricks-config and decodes it. The bool
// is false on any failure (request error, non-200, unparseable body) so callers
// fall back to bare-host discovery.
func fetchHostMetadata(ctx context.Context, client *http.Client, url string) (hostMetadata, bool) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return hostMetadata{}, false
	}

	resp, err := client.Do(req)
	if err != nil {
		return hostMetadata{}, false
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		return hostMetadata{}, false
	}

	var meta hostMetadata
	if err := json.NewDecoder(resp.Body).Decode(&meta); err != nil {
		return hostMetadata{}, false
	}
	return meta, true
}

func GetScopes(hostName string, scopes []string) []string {
	for _, s := range []string{oidc.ScopeOfflineAccess} {
		if !HasScope(scopes, s) {
			scopes = append(scopes, s)
		}
	}

	cloudType := InferCloudFromHost(hostName)
	if cloudType == Azure {
		userImpersonationScope := fmt.Sprintf("%s/user_impersonation", azureTenants[GetAzureDnsZone(hostName)])
		if !HasScope(scopes, userImpersonationScope) {
			scopes = append(scopes, userImpersonationScope)
		}
	} else {
		if !HasScope(scopes, "sql") {
			scopes = append(scopes, "sql")
		}
	}

	return scopes
}

func HasScope(scopes []string, scope string) bool {
	for _, s := range scopes {
		if s == scope {
			return true
		}
	}
	return false
}

var databricksAWSDomains []string = []string{
	".cloud.databricks.com",
	".cloud.databricks.us",
	".dev.databricks.com",
}

var databricksAzureDomains []string = []string{
	".azuredatabricks.net",
	".databricks.azure.cn",
	".databricks.azure.us",
}

var databricksGCPDomains []string = []string{
	".gcp.databricks.com",
}

type CloudType int

const (
	AWS = iota
	Azure
	GCP
	Unknown
)

func (cl CloudType) String() string {
	switch cl {
	case AWS:
		return "AWS"
	case Azure:
		return "Azure"
	case GCP:
		return "GCP"
	}

	return "Unknown"
}

func InferCloudFromHost(hostname string) CloudType {

	for _, d := range databricksAzureDomains {
		if strings.Contains(hostname, d) {
			return Azure
		}
	}

	for _, d := range databricksAWSDomains {
		if strings.Contains(hostname, d) {
			return AWS
		}
	}

	for _, d := range databricksGCPDomains {
		if strings.Contains(hostname, d) {
			return GCP
		}
	}

	// Unified / SPOG (Single Pane of Glass) AWS hosts use bare *.databricks.com
	// custom URLs (e.g. <name>.databricks.com, <name>.staging.databricks.com) that
	// match none of the lists above. Treat them as AWS. This is checked last so the
	// more specific Azure (.azuredatabricks.net) and GCP (.gcp.databricks.com) hosts
	// are classified first.
	if strings.Contains(hostname, "databricks.com") {
		return AWS
	}

	return Unknown
}

func GetAzureDnsZone(hostname string) string {
	for _, d := range databricksAzureDomains {
		if strings.Contains(hostname, d) {
			return d
		}
	}
	return ""
}
