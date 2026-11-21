// Package githubauth authenticates GitHub REST calls with a GitHub App
// installation token, falling back to a static personal access token (PAT)
// when the App is not configured or a token mint fails.
//
// It depends only on the standard library: the App JWT is signed with
// crypto/rsa (RS256) and installation tokens are minted with a plain
// net/http POST. Errors and log lines never carry the JWT, the private key,
// a token value or a GitHub response body.
package githubauth

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Token sources reported by TokenSource.Token and used as the {source}
// metric label.
const (
	SourceApp = "app"
	SourcePAT = "pat"
)

// Mint results used as the {result} label of github_token_mint_total.
const (
	MintResultSuccess = "success"
	MintResultError   = "error"
)

// DefaultAPIURL is the public GitHub REST API base URL.
const DefaultAPIURL = "https://api.github.com"

const (
	jwtBackdate      = 60 * time.Second
	jwtLifetime      = 5 * time.Minute
	tokenRefreshSkew = 5 * time.Minute
	mintBackoff      = 60 * time.Second
	mintTimeout      = 10 * time.Second
	maxMintBody      = 1 << 20
)

// TokenSource yields the credential for the next GitHub REST request.
type TokenSource interface {
	// Token returns the credential value and its source (SourceApp or
	// SourcePAT). An empty value with a nil error means unauthenticated.
	Token(ctx context.Context) (value, source string, err error)
	// Invalidate drops a cached credential if it still equals value, so the
	// next Token call mints a fresh one (used after a 401).
	Invalidate(value string)
}

// Metrics receives the package's counters and gauges. Services back it with
// their Prometheus registry:
//
//	github_requests_by_source_total{source}  -> IncRequest
//	github_rate_limit_remaining{source}      -> SetRateLimitRemaining
//	github_token_mint_total{result}          -> IncTokenMint
type Metrics interface {
	IncRequest(source string)
	SetRateLimitRemaining(source string, remaining float64)
	IncTokenMint(result string)
}

type nopMetrics struct{}

func (nopMetrics) IncRequest(string)                     {}
func (nopMetrics) SetRateLimitRemaining(string, float64) {}
func (nopMetrics) IncTokenMint(string)                   {}

func metricsOrNop(m Metrics) Metrics {
	if m == nil {
		return nopMetrics{}
	}
	return m
}

func loggerOrDefault(l *slog.Logger) *slog.Logger {
	if l == nil {
		return slog.Default()
	}
	return l
}

// AppConfig identifies a GitHub App installation.
type AppConfig struct {
	AppID          int64
	InstallationID int64
	// PrivateKeyPEM is the App private key, PKCS#1 or PKCS#8 PEM.
	PrivateKeyPEM string
	// APIURL defaults to DefaultAPIURL.
	APIURL string
	// Logger defaults to slog.Default().
	Logger *slog.Logger
	// Metrics may be nil.
	Metrics Metrics
}

type staticSource struct{ pat string }

// NewStaticTokenSource returns a TokenSource that always yields pat with
// source SourcePAT.
func NewStaticTokenSource(pat string) TokenSource {
	return staticSource{pat: strings.TrimSpace(pat)}
}

func (s staticSource) Token(context.Context) (string, string, error) {
	return s.pat, SourcePAT, nil
}

func (staticSource) Invalidate(string) {}

type appSource struct {
	appID          int64
	installationID int64
	key            *rsa.PrivateKey
	mintURL        string
	client         *http.Client
	now            func() time.Time
	fallbackPAT    string
	logger         *slog.Logger
	metrics        Metrics

	mu           sync.Mutex
	token        string
	expiresAt    time.Time
	backoffUntil time.Time
}

// NewAppTokenSource returns a TokenSource that mints and caches GitHub App
// installation tokens. The private key is parsed here, so a bad key fails at
// construction. client must not route through a Transport built on this
// source (the mint call carries its own JWT); nil uses a 10s-timeout client.
// When a mint fails the source returns fallbackPAT with SourcePAT and does
// not retry the mint for 60 seconds.
func NewAppTokenSource(cfg AppConfig, client *http.Client, now func() time.Time, fallbackPAT string) (TokenSource, error) {
	if cfg.AppID <= 0 {
		return nil, errors.New("githubauth: app id must be positive")
	}
	if cfg.InstallationID <= 0 {
		return nil, errors.New("githubauth: installation id must be positive")
	}
	key, err := parsePrivateKey(cfg.PrivateKeyPEM)
	if err != nil {
		return nil, err
	}
	apiURL := strings.TrimRight(strings.TrimSpace(cfg.APIURL), "/")
	if apiURL == "" {
		apiURL = DefaultAPIURL
	}
	if client == nil {
		client = &http.Client{Timeout: mintTimeout}
	}
	if now == nil {
		now = time.Now
	}
	return &appSource{
		appID:          cfg.AppID,
		installationID: cfg.InstallationID,
		key:            key,
		mintURL:        fmt.Sprintf("%s/app/installations/%d/access_tokens", apiURL, cfg.InstallationID),
		client:         client,
		now:            now,
		fallbackPAT:    strings.TrimSpace(fallbackPAT),
		logger:         loggerOrDefault(cfg.Logger),
		metrics:        metricsOrNop(cfg.Metrics),
	}, nil
}

// Token returns the cached installation token while it has more than five
// minutes left, otherwise mints a new one. Concurrent callers share a single
// in-flight mint.
func (s *appSource) Token(ctx context.Context) (string, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	if s.token != "" && now.Before(s.expiresAt.Add(-tokenRefreshSkew)) {
		return s.token, SourceApp, nil
	}
	if now.Before(s.backoffUntil) {
		return s.fallback()
	}

	token, expiresAt, err := s.mint(ctx, now)
	if err != nil {
		s.metrics.IncTokenMint(MintResultError)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return "", "", ctxErr
		}
		s.backoffUntil = now.Add(mintBackoff)
		s.logger.Error("githubauth: installation token mint failed; using PAT fallback for 60s",
			"installation_id", s.installationID, "error", err)
		return s.fallback()
	}
	s.metrics.IncTokenMint(MintResultSuccess)
	s.token = token
	s.expiresAt = expiresAt
	return token, SourceApp, nil
}

func (s *appSource) fallback() (string, string, error) {
	if s.fallbackPAT == "" {
		return "", "", errors.New("githubauth: installation token unavailable and no fallback PAT configured")
	}
	return s.fallbackPAT, SourcePAT, nil
}

// Invalidate drops the cached installation token if it equals value.
func (s *appSource) Invalidate(value string) {
	if value == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.token == value {
		s.token = ""
		s.expiresAt = time.Time{}
	}
}

type mintResponse struct {
	Token     string    `json:"token"`
	ExpiresAt time.Time `json:"expires_at"`
}

func (s *appSource) mint(ctx context.Context, now time.Time) (string, time.Time, error) {
	jwt, err := signJWT(s.key, s.appID, now)
	if err != nil {
		return "", time.Time{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.mintURL, http.NoBody)
	if err != nil {
		return "", time.Time{}, errors.New("githubauth: build mint request")
	}
	req.Header.Set("Authorization", "Bearer "+jwt)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-Github-Api-Version", "2022-11-28")

	resp, err := s.client.Do(req)
	if err != nil {
		// *url.Error carries only method and URL; the URL holds the
		// installation id, never a credential.
		return "", time.Time{}, fmt.Errorf("githubauth: mint request: %w", err)
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxMintBody))
	if resp.StatusCode != http.StatusCreated {
		return "", time.Time{}, fmt.Errorf("githubauth: mint returned status %d", resp.StatusCode)
	}
	if readErr != nil {
		return "", time.Time{}, errors.New("githubauth: read mint response")
	}
	var out mintResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return "", time.Time{}, errors.New("githubauth: decode mint response")
	}
	if out.Token == "" || out.ExpiresAt.IsZero() {
		return "", time.Time{}, errors.New("githubauth: mint response missing token or expires_at")
	}
	return out.Token, out.ExpiresAt, nil
}

// jwtHeader is the base64url form of {"alg":"RS256","typ":"JWT"}.
const jwtHeader = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9"

type jwtClaims struct {
	IssuedAt  int64  `json:"iat"`
	ExpiresAt int64  `json:"exp"`
	Issuer    string `json:"iss"`
}

// signJWT builds the RS256 App JWT: iat backdated 60s for clock drift,
// exp 5 minutes ahead (GitHub caps it at 10), iss the App id.
func signJWT(key *rsa.PrivateKey, appID int64, now time.Time) (string, error) {
	claims, err := json.Marshal(jwtClaims{
		IssuedAt:  now.Add(-jwtBackdate).Unix(),
		ExpiresAt: now.Add(jwtLifetime).Unix(),
		Issuer:    strconv.FormatInt(appID, 10),
	})
	if err != nil {
		return "", errors.New("githubauth: encode jwt claims")
	}
	signingInput := jwtHeader + "." + base64.RawURLEncoding.EncodeToString(claims)
	digest := sha256.Sum256([]byte(signingInput))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		return "", errors.New("githubauth: sign jwt")
	}
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}

func parsePrivateKey(pemText string) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(bytes.TrimSpace([]byte(pemText)))
	if block == nil {
		return nil, errors.New("githubauth: private key is not PEM encoded")
	}
	if key, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return key, nil
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, errors.New("githubauth: private key is neither PKCS#1 nor PKCS#8")
	}
	key, ok := parsed.(*rsa.PrivateKey)
	if !ok {
		return nil, errors.New("githubauth: private key is not RSA")
	}
	return key, nil
}

// FromValuesOptions carries the optional dependencies of
// NewTokenSourceFromValues.
type FromValuesOptions struct {
	APIURL  string
	Client  *http.Client
	Now     func() time.Time
	Logger  *slog.Logger
	Metrics Metrics
}

// NewTokenSourceFromValues builds a TokenSource from raw secret values (as
// read from environment variables). With all three App values empty it
// returns a static PAT source. Partial App config, a non-numeric id or a bad
// key is logged as an error and also yields the static PAT source, so a
// misconfigured App never crashes the service.
func NewTokenSourceFromValues(appID, installationID, privateKeyPEM, pat string, opts FromValuesOptions) TokenSource {
	appID = strings.TrimSpace(appID)
	installationID = strings.TrimSpace(installationID)
	privateKeyPEM = strings.TrimSpace(privateKeyPEM)
	logger := loggerOrDefault(opts.Logger)
	static := NewStaticTokenSource(pat)

	if appID == "" && installationID == "" && privateKeyPEM == "" {
		return static
	}
	var missing []string
	if appID == "" {
		missing = append(missing, "GITHUB_APP_ID")
	}
	if installationID == "" {
		missing = append(missing, "GITHUB_APP_INSTALLATION_ID")
	}
	if privateKeyPEM == "" {
		missing = append(missing, "GITHUB_APP_PRIVATE_KEY")
	}
	if len(missing) > 0 {
		logger.Error("githubauth: partial GitHub App config; using PAT", "missing", strings.Join(missing, ","))
		return static
	}
	id, err := strconv.ParseInt(appID, 10, 64)
	if err != nil {
		logger.Error("githubauth: GITHUB_APP_ID is not an integer; using PAT")
		return static
	}
	inst, err := strconv.ParseInt(installationID, 10, 64)
	if err != nil {
		logger.Error("githubauth: GITHUB_APP_INSTALLATION_ID is not an integer; using PAT")
		return static
	}
	src, err := NewAppTokenSource(AppConfig{
		AppID:          id,
		InstallationID: inst,
		PrivateKeyPEM:  privateKeyPEM,
		APIURL:         opts.APIURL,
		Logger:         logger,
		Metrics:        opts.Metrics,
	}, opts.Client, opts.Now, pat)
	if err != nil {
		logger.Error("githubauth: invalid GitHub App config; using PAT", "error", err)
		return static
	}
	return src
}
