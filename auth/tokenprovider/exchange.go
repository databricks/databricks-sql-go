package tokenprovider

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/rs/zerolog/log"
)

// FederationProvider wraps another token provider and automatically handles token exchange
type FederationProvider struct {
	baseProvider   TokenProvider
	databricksHost string
	clientID       string // For SP-wide federation
	httpClient     *http.Client
	// Settings for token exchange
	returnOriginalTokenIfAuthenticated bool
}

// NewFederationProvider creates a federation provider that wraps another provider
// It automatically detects when token exchange is needed and falls back gracefully
func NewFederationProvider(baseProvider TokenProvider, databricksHost string) *FederationProvider {
	return &FederationProvider{
		baseProvider:                       baseProvider,
		databricksHost:                     databricksHost,
		httpClient:                         &http.Client{Timeout: 30 * time.Second},
		returnOriginalTokenIfAuthenticated: true,
	}
}

// NewFederationProviderWithClientID creates a provider for SP-wide federation (M2M)
func NewFederationProviderWithClientID(baseProvider TokenProvider, databricksHost, clientID string) *FederationProvider {
	return &FederationProvider{
		baseProvider:                       baseProvider,
		databricksHost:                     databricksHost,
		clientID:                           clientID,
		httpClient:                         &http.Client{Timeout: 30 * time.Second},
		returnOriginalTokenIfAuthenticated: true,
	}
}

// GetToken gets token from base provider and exchanges if needed
func (p *FederationProvider) GetToken(ctx context.Context) (*Token, error) {
	// Check if context is already cancelled
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("federation provider: context cancelled: %w", err)
	}

	// Get token from base provider
	baseToken, err := p.baseProvider.GetToken(ctx)
	if err != nil {
		return nil, fmt.Errorf("federation provider: failed to get base token: %w", err)
	}

	// Check if token is a JWT and needs exchange
	if p.needsTokenExchange(baseToken.AccessToken) {
		log.Debug().Msgf("federation provider: attempting token exchange for %s", p.baseProvider.Name())

		// Try token exchange
		exchangedToken, err := p.tryTokenExchange(ctx, baseToken.AccessToken)
		if err != nil {
			log.Warn().Err(err).Msg("federation provider: token exchange failed, using original token")
			return baseToken, nil // Fall back to original token
		}

		log.Debug().Msg("federation provider: token exchange successful")
		return exchangedToken, nil
	}

	// Use original token
	return baseToken, nil
}

// needsTokenExchange determines if a token needs exchange by checking if it's from a different issuer
func (p *FederationProvider) needsTokenExchange(tokenString string) bool {
	// Try to parse as JWT without verification
	// We use ParseUnverified because:
	// 1. We only need to inspect claims (issuer), not validate the signature
	// 2. We don't have the public key for external identity providers
	// 3. Token validation will be done by Databricks during exchange
	token, _, err := new(jwt.Parser).ParseUnverified(tokenString, jwt.MapClaims{})
	if err != nil {
		log.Debug().Err(err).Msg("federation provider: not a JWT token, skipping exchange")
		return false
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return false
	}

	issuer, ok := claims["iss"].(string)
	if !ok {
		return false
	}

	// Check if issuer is different from Databricks host
	return !p.isSameHost(issuer, p.databricksHost)
}

// tryTokenExchange attempts to exchange the token with Databricks
func (p *FederationProvider) tryTokenExchange(ctx context.Context, subjectToken string) (*Token, error) {
	// Build exchange URL - add scheme if not present
	exchangeURL := p.databricksHost
	if !strings.HasPrefix(exchangeURL, "http://") && !strings.HasPrefix(exchangeURL, "https://") {
		// Default to HTTPS for security
		exchangeURL = "https://" + exchangeURL
	} else if strings.HasPrefix(exchangeURL, "http://") {
		// Warn if using insecure HTTP for token exchange
		log.Warn().Msgf("federation provider: using insecure HTTP for token exchange: %s", exchangeURL)
	}
	if !strings.HasSuffix(exchangeURL, "/") {
		exchangeURL += "/"
	}
	exchangeURL += "oidc/v1/token"

	// Prepare form data for token exchange
	data := url.Values{}
	data.Set("grant_type", "urn:ietf:params:oauth:grant-type:token-exchange")
	data.Set("scope", "sql")
	data.Set("subject_token_type", "urn:ietf:params:oauth:token-type:jwt")
	data.Set("subject_token", subjectToken)

	if p.returnOriginalTokenIfAuthenticated {
		data.Set("return_original_token_if_authenticated", "true")
	}

	// Add client_id for SP-wide federation
	if p.clientID != "" {
		data.Set("client_id", p.clientID)
	}

	// Create request
	req, err := http.NewRequestWithContext(ctx, "POST", exchangeURL, strings.NewReader(data.Encode()))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "*/*")

	// Make request
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("exchange failed with status %d: %s", resp.StatusCode, string(body))
	}

	// Parse response
	var tokenResp struct {
		AccessToken string `json:"access_token"`
		TokenType   string `json:"token_type"`
		ExpiresIn   int    `json:"expires_in"`
		Scope       string `json:"scope"`
	}

	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	// Validate token response
	if tokenResp.AccessToken == "" {
		return nil, fmt.Errorf("token exchange returned empty access token")
	}
	if tokenResp.TokenType == "" {
		log.Debug().Msg("token exchange: token_type not specified, defaulting to Bearer")
		tokenResp.TokenType = "Bearer"
	}
	if tokenResp.ExpiresIn < 0 {
		return nil, fmt.Errorf("token exchange returned invalid expires_in: %d", tokenResp.ExpiresIn)
	}

	token := &Token{
		AccessToken: tokenResp.AccessToken,
		TokenType:   tokenResp.TokenType,
		Scopes:      strings.Fields(tokenResp.Scope),
	}

	if tokenResp.ExpiresIn > 0 {
		token.ExpiresAt = time.Now().Add(time.Duration(tokenResp.ExpiresIn) * time.Second)
	}

	return token, nil
}

// isSameHost compares two URLs to see if they have the same host
func (p *FederationProvider) isSameHost(url1, url2 string) bool {
	// Add scheme to url2 if it doesn't have one (databricksHost may not have scheme)
	parsedURL2 := url2
	if !strings.HasPrefix(url2, "http://") && !strings.HasPrefix(url2, "https://") {
		parsedURL2 = "https://" + url2
	}

	u1, err1 := url.Parse(url1)
	u2, err2 := url.Parse(parsedURL2)

	if err1 != nil || err2 != nil {
		log.Debug().Msgf("federation provider: failed to parse URLs for comparison: url1=%s err1=%v, url2=%s err2=%v",
			url1, err1, parsedURL2, err2)
		return false
	}

	// Use Hostname() instead of Host to ignore port differences
	// This handles cases like "host.com:443" == "host.com" for HTTPS
	isSame := u1.Hostname() == u2.Hostname()
	log.Debug().Msgf("federation provider: host comparison: %s vs %s = %v", u1.Hostname(), u2.Hostname(), isSame)
	return isSame
}

// Name returns the provider name
func (p *FederationProvider) Name() string {
	baseName := p.baseProvider.Name()
	if p.clientID != "" {
		clientIDDisplay := p.clientID
		if len(p.clientID) > 8 {
			clientIDDisplay = p.clientID[:8]
		}
		return fmt.Sprintf("federation[%s,sp:%s]", baseName, clientIDDisplay) // Truncate client ID for readability
	}
	return fmt.Sprintf("federation[%s]", baseName)
}
