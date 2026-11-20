package k8sauth

import (
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// DefaultServiceAccountTokenPath is the standard in-cluster path for the projected SA token.
const DefaultServiceAccountTokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"

// TokenSource provides cached, periodically-reloaded service account tokens from disk.
type TokenSource struct {
	tokenPath string
	ttl       time.Duration
	now       func() time.Time
	mu        sync.RWMutex
	cached    string
	expiresAt time.Time
}

// TokenSourceOption configures a TokenSource.
type TokenSourceOption func(*TokenSource)

// WithTokenSourceClock configures a custom clock for testing.
func WithTokenSourceClock(now func() time.Time) TokenSourceOption {
	return func(ts *TokenSource) {
		if now != nil {
			ts.now = now
		}
	}
}

// WithTokenSourceTTL configures a custom reload TTL.
func WithTokenSourceTTL(ttl time.Duration) TokenSourceOption {
	return func(ts *TokenSource) {
		if ttl > 0 {
			ts.ttl = ttl
		}
	}
}

// NewTokenSource returns a TokenSource that reloads the projected SA token from tokenPath.
func NewTokenSource(tokenPath string, opts ...TokenSourceOption) *TokenSource {
	if tokenPath == "" {
		tokenPath = DefaultServiceAccountTokenPath
	}
	ts := &TokenSource{
		tokenPath: tokenPath,
		ttl:       60 * time.Second,
		now:       time.Now,
	}
	for _, opt := range opts {
		opt(ts)
	}
	return ts
}

// TokenPath returns the file path from which tokens are read.
func (ts *TokenSource) TokenPath() string {
	return ts.tokenPath
}

// Token returns the current cached service account token, reloading from disk if expired.
func (ts *TokenSource) Token() (string, error) {
	ts.mu.RLock()
	if ts.cached != "" && ts.now().Before(ts.expiresAt) {
		tok := ts.cached
		ts.mu.RUnlock()
		return tok, nil
	}
	ts.mu.RUnlock()

	ts.mu.Lock()
	defer ts.mu.Unlock()

	// Double-check after acquiring write lock
	if ts.cached != "" && ts.now().Before(ts.expiresAt) {
		return ts.cached, nil
	}

	data, err := os.ReadFile(ts.tokenPath)
	if err != nil {
		return "", fmt.Errorf("read serviceaccount token from %s: %w", ts.tokenPath, err)
	}

	token := strings.TrimSpace(string(data))
	if token == "" {
		return "", fmt.Errorf("serviceaccount token at %s is empty", ts.tokenPath)
	}

	ts.cached = token
	ts.expiresAt = ts.now().Add(ts.ttl)
	return token, nil
}

type serviceAccountTransport struct {
	base        http.RoundTripper
	tokenSource *TokenSource
}

func (t *serviceAccountTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	token, err := t.tokenSource.Token()
	if err != nil {
		return nil, fmt.Errorf("serviceaccount transport: %w", err)
	}

	r := req.Clone(req.Context())
	r.Header.Set("Authorization", "Bearer "+token)
	return t.base.RoundTrip(r)
}

// NewServiceAccountTransport wraps base with automatic Authorization: Bearer <token> injection.
func NewServiceAccountTransport(base http.RoundTripper, tokenPath string) http.RoundTripper {
	return NewServiceAccountTransportWithSource(base, NewTokenSource(tokenPath))
}

// NewServiceAccountTransportWithSource wraps base using an existing TokenSource.
func NewServiceAccountTransportWithSource(base http.RoundTripper, ts *TokenSource) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	if ts == nil {
		ts = NewTokenSource("")
	}
	return &serviceAccountTransport{
		base:        base,
		tokenSource: ts,
	}
}
