package oauth

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
)

const (
	azureEnterpriseAppId = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"
)

func GetEndpoint(ctx context.Context, hostName string) (oauth2.Endpoint, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	cloud := InferCloudFromHost(hostName)
	if cloud == unknown {
		return oauth2.Endpoint{}, errors.New("unsupported cloud type")
	}

	if cloud == azure {
		authURL := fmt.Sprintf("https://%s/oidc/oauth2/v2.0/authorize", hostName)
		tokenURL := fmt.Sprintf("https://%s/oidc/oauth2/v2.0/token", hostName)
		return oauth2.Endpoint{AuthURL: authURL, TokenURL: tokenURL}, nil
	}

	issuerURL := fmt.Sprintf("https://%s/oidc", hostName)
	ctx = oidc.InsecureIssuerURLContext(ctx, issuerURL)
	provider, err := oidc.NewProvider(ctx, issuerURL)
	if err != nil {
		return oauth2.Endpoint{}, err
	}

	endpoint := provider.Endpoint()

	return endpoint, err
}

func GetScopes(hostName string, scopes []string) []string {
	for _, s := range []string{oidc.ScopeOfflineAccess, oidc.ScopeOpenID} {
		if !hasScope(scopes, s) {
			scopes = append(scopes, s)
		}
	}

	cloudType := InferCloudFromHost(hostName)
	if cloudType == azure {
		userImpersonationScope := fmt.Sprintf("%s/user_impersonation", azureEnterpriseAppId)
		if !hasScope(scopes, userImpersonationScope) {
			scopes = append(scopes, userImpersonationScope)
		}
	} else {
		if !hasScope(scopes, "sql") {
			scopes = append(scopes, "sql")
		}
	}

	return scopes
}

func hasScope(scopes []string, scope string) bool {
	for _, s := range scopes {
		if s == scope {
			return true
		}
	}
	return false
}

var databricksAWSDomains []string = []string{
	".cloud.databricks.com",
	".dev.databricks.com",
}

var databricksAzureDomains []string = []string{
	".azuredatabricks.net",
	".databricks.azure.cn",
	".databricks.azure.us",
}

var databricksGCPDomains []string = []string{".gcp.databricks.com"}

type cloudType int

const (
	aws = iota
	azure
	gcp
	unknown
)

func InferCloudFromHost(hostname string) cloudType {

	for _, d := range databricksAzureDomains {
		if strings.Contains(hostname, d) {
			return azure
		}
	}

	for _, d := range databricksAWSDomains {
		if strings.Contains(hostname, d) {
			return aws
		}
	}

	for _, d := range databricksGCPDomains {
		if strings.Contains(hostname, d) {
			return gcp
		}
	}

	return unknown
}
