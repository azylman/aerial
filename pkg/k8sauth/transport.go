package k8sauth

import (
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// DefaultTokenPath is the standard mounted path for projected Kubernetes ServiceAccount tokens.
const DefaultTokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"

// TokenSource provides cached access to a Kubernetes ServiceAccount token with a 60-second reload TTL.
type TokenSource struct {
	tokenPath string
	reloadTTL time.Duration
	now       func() time.Time
	isDefault bool

	mu       sync.RWMutex
	token    string
	lastRead time.Time
}

// NewTokenSource creates a TokenSource for the token file at tokenPath with a 60s reload TTL.
func NewTokenSource(tokenPath string, now func() time.Time) *TokenSource {
	isDefault := false
	if tokenPath == "" {
		tokenPath = DefaultTokenPath
		isDefault = true
	}
	if now == nil {
		now = time.Now
	}
	return &TokenSource{
		tokenPath: tokenPath,
		reloadTTL: 60 * time.Second,
		now:       now,
		isDefault: isDefault,
	}
}

// Token returns the current token, refreshing from disk if more than reloadTTL has elapsed since the last read.
func (ts *TokenSource) Token() (string, error) {
	ts.mu.RLock()
	if ts.token != "" && ts.now().Sub(ts.lastRead) < ts.reloadTTL {
		tok := ts.token
		ts.mu.RUnlock()
		return tok, nil
	}
	ts.mu.RUnlock()

	ts.mu.Lock()
	defer ts.mu.Unlock()

	// Double-check under write lock
	if ts.token != "" && ts.now().Sub(ts.lastRead) < ts.reloadTTL {
		return ts.token, nil
	}

	data, err := os.ReadFile(ts.tokenPath)
	if err != nil {
		return "", fmt.Errorf("read serviceaccount token from %s: %w", ts.tokenPath, err)
	}

	tok := strings.TrimSpace(string(data))
	if tok == "" {
		return "", fmt.Errorf("serviceaccount token at %s is empty", ts.tokenPath)
	}

	ts.token = tok
	ts.lastRead = ts.now()
	return tok, nil
}

// ServiceAccountTransport wraps an http.RoundTripper to inject Authorization: Bearer <token> headers.
type ServiceAccountTransport struct {
	base        http.RoundTripper
	tokenSource *TokenSource
}

// NewServiceAccountTransport creates an http.RoundTripper that injects the ServiceAccount token from tokenPath.
func NewServiceAccountTransport(base http.RoundTripper, tokenPath string) http.RoundTripper {
	return NewServiceAccountTransportWithSource(base, NewTokenSource(tokenPath, time.Now))
}

// NewServiceAccountTransportWithSource creates an http.RoundTripper with an explicit TokenSource.
func NewServiceAccountTransportWithSource(base http.RoundTripper, ts *TokenSource) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return &ServiceAccountTransport{
		base:        base,
		tokenSource: ts,
	}
}

// RoundTrip clones the request and injects the Authorization: Bearer <token> header.
func (t *ServiceAccountTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	tok, err := t.tokenSource.Token()
	if err != nil {
		if t.tokenSource != nil && t.tokenSource.isDefault {
			return t.base.RoundTrip(req)
		}
		return nil, fmt.Errorf("k8sauth: obtain serviceaccount token: %w", err)
	}
	r := req.Clone(req.Context())
	r.Header.Set("Authorization", "Bearer "+tok)
	return t.base.RoundTrip(r)
}

// CloseIdleConnections forwards to the base transport if supported.
func (t *ServiceAccountTransport) CloseIdleConnections() {
	if c, ok := t.base.(interface{ CloseIdleConnections() }); ok {
		c.CloseIdleConnections()
	}
}
