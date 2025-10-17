package tokenprovider

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
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
	// Try to parse as JWT
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
		exchangeURL = "https://" + exchangeURL
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
// This matches Python's behavior: ignores port differences (e.g., :443 vs no port for HTTPS)
func (p *FederationProvider) isSameHost(url1, url2 string) bool {
	// Add scheme to url2 if it doesn't have one (databricksHost may not have scheme)
	parsedURL2 := url2
	if !strings.HasPrefix(url2, "http://") && !strings.HasPrefix(url2, "https://") {
		parsedURL2 = "https://" + url2
	}

	u1, err1 := url.Parse(url1)
	u2, err2 := url.Parse(parsedURL2)

	if err1 != nil || err2 != nil {
		return false
	}

	// Use Hostname() instead of Host to ignore port differences
	// This handles cases like "host.com:443" == "host.com" for HTTPS
	return u1.Hostname() == u2.Hostname()
}

// Name returns the provider name
func (p *FederationProvider) Name() string {
	baseName := p.baseProvider.Name()
	if p.clientID != "" {
		return fmt.Sprintf("federation[%s,sp:%s]", baseName, p.clientID[:8]) // Truncate client ID for readability
	}
	return fmt.Sprintf("federation[%s]", baseName)
}

// JWTProvider creates tokens from JWT credentials (for M2M flows)
type JWTProvider struct {
	clientID      string
	privateKey    interface{}
	tokenURL      string
	scopes        []string
	audience      string
	keyID         string
	httpClient    *http.Client
	jwtExpiration time.Duration
}

// NewJWTProvider creates a provider that uses JWT assertion for M2M authentication
func NewJWTProvider(clientID, tokenURL string, privateKey interface{}) *JWTProvider {
	return &JWTProvider{
		clientID:      clientID,
		privateKey:    privateKey,
		tokenURL:      tokenURL,
		scopes:        []string{"sql"},
		httpClient:    &http.Client{Timeout: 30 * time.Second},
		jwtExpiration: 5 * time.Minute,
	}
}

// WithScopes sets the OAuth scopes
func (p *JWTProvider) WithScopes(scopes []string) *JWTProvider {
	p.scopes = scopes
	return p
}

// WithAudience sets the JWT audience
func (p *JWTProvider) WithAudience(audience string) *JWTProvider {
	p.audience = audience
	return p
}

// WithKeyID sets the JWT key ID
func (p *JWTProvider) WithKeyID(keyID string) *JWTProvider {
	p.keyID = keyID
	return p
}

// GetToken creates a JWT assertion and exchanges it for an access token
func (p *JWTProvider) GetToken(ctx context.Context) (*Token, error) {
	// Create JWT assertion
	jwtToken, err := p.createJWTAssertion()
	if err != nil {
		return nil, fmt.Errorf("jwt provider: failed to create JWT assertion: %w", err)
	}

	// Exchange JWT for access token
	return p.exchangeJWTForToken(ctx, jwtToken)
}

// createJWTAssertion creates a JWT assertion for client credentials flow
func (p *JWTProvider) createJWTAssertion() (string, error) {
	now := time.Now()
	claims := jwt.MapClaims{
		"iss": p.clientID,
		"sub": p.clientID,
		"aud": p.audience,
		"iat": now.Unix(),
		"exp": now.Add(p.jwtExpiration).Unix(),
	}

	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)

	if p.keyID != "" {
		token.Header["kid"] = p.keyID
	}

	return token.SignedString(p.privateKey)
}

// exchangeJWTForToken exchanges JWT assertion for access token
func (p *JWTProvider) exchangeJWTForToken(ctx context.Context, jwtAssertion string) (*Token, error) {
	data := url.Values{}
	data.Set("grant_type", "client_credentials")
	data.Set("client_assertion_type", "urn:ietf:params:oauth:client-assertion-type:jwt-bearer")
	data.Set("client_assertion", jwtAssertion)
	data.Set("scope", strings.Join(p.scopes, " "))

	req, err := http.NewRequestWithContext(ctx, "POST", p.tokenURL, strings.NewReader(data.Encode()))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

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
		return nil, fmt.Errorf("token request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var tokenResp struct {
		AccessToken string `json:"access_token"`
		TokenType   string `json:"token_type"`
		ExpiresIn   int    `json:"expires_in"`
		Scope       string `json:"scope"`
	}

	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
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

// Name returns the provider name
func (p *JWTProvider) Name() string {
	return "jwt-assertion"
}

// ParsePrivateKeyFromPEM parses a private key from PEM format
func ParsePrivateKeyFromPEM(pemData []byte, passphrase string) (interface{}, error) {
	block, _ := pem.Decode(pemData)
	if block == nil {
		return nil, fmt.Errorf("failed to decode PEM block")
	}

	var keyBytes []byte
	var err error

	if passphrase != "" && x509.IsEncryptedPEMBlock(block) {
		keyBytes, err = x509.DecryptPEMBlock(block, []byte(passphrase))
		if err != nil {
			return nil, fmt.Errorf("failed to decrypt PEM block: %w", err)
		}
	} else {
		keyBytes = block.Bytes
	}

	// Try parsing as PKCS1 RSA private key first
	if key, err := x509.ParsePKCS1PrivateKey(keyBytes); err == nil {
		return key, nil
	}

	// Try parsing as PKCS8 private key
	if key, err := x509.ParsePKCS8PrivateKey(keyBytes); err == nil {
		return key, nil
	}

	return nil, fmt.Errorf("unsupported private key format")
}