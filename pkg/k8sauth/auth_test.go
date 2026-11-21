package k8sauth

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type cacheEntryPair struct {
	key string
	val string
}

func TestLRUCache_Get(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		prepopulate []cacheEntryPair
		ttl         time.Duration
		timeAdvance time.Duration
		opKey       string
		wantVal     string
		wantOk      bool
	}{
		{
			name:    "missing key returns zero and false",
			opKey:   "missing",
			wantVal: "",
			wantOk:  false,
		},
		{
			name:        "existing key within ttl returns value and true",
			prepopulate: []cacheEntryPair{{key: "k1", val: "v1"}},
			ttl:         time.Minute,
			timeAdvance: 10 * time.Second,
			opKey:       "k1",
			wantVal:     "v1",
			wantOk:      true,
		},
		{
			name:        "expired key returns zero and false and evicts",
			prepopulate: []cacheEntryPair{{key: "k1", val: "v1"}},
			ttl:         10 * time.Second,
			timeAdvance: 11 * time.Second,
			opKey:       "k1",
			wantVal:     "",
			wantOk:      false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			nowTime := time.Date(2026, 11, 20, 1, 0, 0, 0, time.UTC)
			nowFunc := func() time.Time { return nowTime }

			cache := NewLRUCache[string](10, tc.ttl, nowFunc)
			for _, p := range tc.prepopulate {
				cache.Set(p.key, p.val)
			}

			if tc.timeAdvance > 0 {
				nowTime = nowTime.Add(tc.timeAdvance)
			}

			gotVal, gotOk := cache.Get(tc.opKey)
			if gotOk != tc.wantOk || gotVal != tc.wantVal {
				t.Fatalf("Get(%q) = (%q, %v), want (%q, %v)", tc.opKey, gotVal, gotOk, tc.wantVal, tc.wantOk)
			}
		})
	}
}

func TestLRUCache_Eviction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		maxEntries   int
		prepopulate  []cacheEntryPair
		touchKey     string
		newKey       string
		newVal       string
		wantEvicted  string
		wantRetained string
	}{
		{
			name:         "evicts oldest entry on overflow",
			maxEntries:   2,
			prepopulate:  []cacheEntryPair{{key: "k1", val: "v1"}, {key: "k2", val: "v2"}},
			newKey:       "k3",
			newVal:       "v3",
			wantEvicted:  "k1",
			wantRetained: "k2",
		},
		{
			name:         "accessed key is preserved while unaccessed oldest is evicted",
			maxEntries:   2,
			prepopulate:  []cacheEntryPair{{key: "k1", val: "v1"}, {key: "k2", val: "v2"}},
			touchKey:     "k1",
			newKey:       "k3",
			newVal:       "v3",
			wantEvicted:  "k2",
			wantRetained: "k1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cache := NewLRUCache[string](tc.maxEntries, time.Minute, time.Now)
			for _, p := range tc.prepopulate {
				cache.Set(p.key, p.val)
			}

			if tc.touchKey != "" {
				_, _ = cache.Get(tc.touchKey)
			}

			cache.Set(tc.newKey, tc.newVal)

			if _, ok := cache.Get(tc.wantEvicted); ok {
				t.Fatalf("expected key %q to be evicted", tc.wantEvicted)
			}
			if val, ok := cache.Get(tc.wantRetained); !ok || val == "" {
				t.Fatalf("expected key %q to be retained, got ok=%v", tc.wantRetained, ok)
			}
			if val, ok := cache.Get(tc.newKey); !ok || val != tc.newVal {
				t.Fatalf("expected new key %q=%q, got ok=%v, val=%q", tc.newKey, tc.newVal, ok, val)
			}
		})
	}
}

func TestLRUCache_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		maxEntries  int
		ttl         time.Duration
		useNilNow   bool
		prepopulate []cacheEntryPair
		deleteKey   string
		clear       bool
		wantLen     int
	}{
		{
			name:        "delete removes entry",
			maxEntries:  10,
			ttl:         time.Minute,
			prepopulate: []cacheEntryPair{{key: "k1", val: "v1"}},
			deleteKey:   "k1",
			wantLen:     0,
		},
		{
			name:        "delete missing is no-op",
			maxEntries:  10,
			ttl:         time.Minute,
			prepopulate: []cacheEntryPair{{key: "k1", val: "v1"}},
			deleteKey:   "k2",
			wantLen:     1,
		},
		{
			name:        "clear empties cache",
			maxEntries:  10,
			ttl:         time.Minute,
			prepopulate: []cacheEntryPair{{key: "k1", val: "v1"}, {key: "k2", val: "v2"}},
			clear:       true,
			wantLen:     0,
		},
		{
			name:       "defaults with nil clock",
			useNilNow:  true,
			maxEntries: 0,
			ttl:        0,
			wantLen:    0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var nowFunc func() time.Time
			if !tc.useNilNow {
				nowFunc = time.Now
			}

			cache := NewLRUCache[string](tc.maxEntries, tc.ttl, nowFunc)
			for _, p := range tc.prepopulate {
				cache.Set(p.key, p.val)
			}

			if tc.deleteKey != "" {
				cache.Delete(tc.deleteKey)
			}
			if tc.clear {
				cache.Clear()
			}

			if gotLen := cache.Len(); gotLen != tc.wantLen {
				t.Fatalf("cache.Len() = %d, want %d", gotLen, tc.wantLen)
			}
		})
	}
}

func TestTokenSource_ReadAndCache(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		initialTok  string
		updatedTok  string
		timeAdvance time.Duration
		wantToken   string
		useNilClock bool
	}{
		{
			name:        "initial read with nil clock",
			initialTok:  "tok-1",
			wantToken:   "tok-1",
			useNilClock: true,
		},
		{
			name:        "cached within reload ttl",
			initialTok:  "tok-1",
			updatedTok:  "tok-2",
			timeAdvance: 30 * time.Second,
			wantToken:   "tok-1",
		},
		{
			name:        "reloaded after reload ttl",
			initialTok:  "tok-1",
			updatedTok:  "tok-2",
			timeAdvance: 61 * time.Second,
			wantToken:   "tok-2",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tokenFile := filepath.Join(t.TempDir(), "token")
			if err := os.WriteFile(tokenFile, []byte(tc.initialTok), 0o600); err != nil {
				t.Fatalf("write token: %v", err)
			}

			nowTime := time.Date(2026, 11, 20, 1, 0, 0, 0, time.UTC)
			nowFunc := func() time.Time { return nowTime }
			if tc.useNilClock {
				nowFunc = nil
			}

			ts := NewTokenSource(tokenFile, nowFunc)

			tok, err := ts.Token()
			if err != nil {
				t.Fatalf("initial ts.Token() error = %v", err)
			}
			if tok != tc.initialTok {
				t.Fatalf("initial ts.Token() = %q, want %q", tok, tc.initialTok)
			}

			if tc.updatedTok != "" {
				if err := os.WriteFile(tokenFile, []byte(tc.updatedTok), 0o600); err != nil {
					t.Fatalf("update token file: %v", err)
				}
				nowTime = nowTime.Add(tc.timeAdvance)
				tok2, err2 := ts.Token()
				if err2 != nil {
					t.Fatalf("second ts.Token() error = %v", err2)
				}
				if tok2 != tc.wantToken {
					t.Fatalf("second ts.Token() = %q, want %q", tok2, tc.wantToken)
				}
			}
		})
	}
}

func TestTokenSource_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		writeToken bool
		content    string
	}{
		{
			name:       "missing file on disk",
			writeToken: false,
		},
		{
			name:       "empty token file",
			writeToken: true,
			content:    "  \t\n  ",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tokenFile := filepath.Join(t.TempDir(), "token")
			if tc.writeToken {
				if err := os.WriteFile(tokenFile, []byte(tc.content), 0o600); err != nil {
					t.Fatalf("write token file: %v", err)
				}
			}

			ts := NewTokenSource(tokenFile, time.Now)
			if _, err := ts.Token(); err == nil {
				t.Fatalf("expected error from ts.Token(), got nil")
			}
		})
	}
}

type staticRoundTripper struct {
	status   int
	body     string
	err      error
	captured *http.Request
}

func (s *staticRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	s.captured = req
	if s.err != nil {
		return nil, s.err
	}
	statusCode := s.status
	if statusCode == 0 {
		statusCode = http.StatusOK
	}
	return &http.Response{
		StatusCode: statusCode,
		Body:       io.NopCloser(bytes.NewBufferString(s.body)),
		Header:     make(http.Header),
	}, nil
}

func TestAuthenticator_NilSafety(t *testing.T) {
	t.Parallel()

	var nilAuth *Authenticator
	if nilAuth.Enabled() {
		t.Errorf("nil Authenticator.Enabled() should be false")
	}
	if nilAuth.Config().Enabled {
		t.Errorf("nil Authenticator.Config().Enabled should be false")
	}

	auth := NewAuthenticator(Config{Enabled: true})
	if !auth.Enabled() {
		t.Errorf("enabled Authenticator.Enabled() should be true")
	}
	if !auth.Config().Enabled {
		t.Errorf("enabled Authenticator.Config().Enabled should be true")
	}
}

func TestExtractBearerToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		authHeader string
		wantToken  string
		wantErr    bool
	}{
		{
			name:       "valid bearer header",
			authHeader: "Bearer token-12345",
			wantToken:  "token-12345",
			wantErr:    false,
		},
		{
			name:       "lowercase bearer header",
			authHeader: "bearer token-case-insensitive",
			wantToken:  "token-case-insensitive",
			wantErr:    false,
		},
		{
			name:       "missing auth header",
			authHeader: "",
			wantErr:    true,
		},
		{
			name:       "basic auth header",
			authHeader: "Basic dXNlcjpwYXNz",
			wantErr:    true,
		},
		{
			name:       "empty token after bearer",
			authHeader: "Bearer    ",
			wantErr:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			req, _ := http.NewRequest(http.MethodGet, "http://localhost/test", http.NoBody)
			if tc.authHeader != "" {
				req.Header.Set("Authorization", tc.authHeader)
			}

			tok, err := ExtractBearerToken(req)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ExtractBearerToken() error = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr && tok != tc.wantToken {
				t.Fatalf("ExtractBearerToken() = %q, want %q", tok, tc.wantToken)
			}
		})
	}
}

func TestContextHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setCtx    bool
		identity  CallerIdentity
		wantFound bool
	}{
		{
			name:   "found in context",
			setCtx: true,
			identity: CallerIdentity{
				Username:    "system:serviceaccount:default:eri-agent-alpha",
				CallerAgent: "agent-alpha",
				IsService:   false,
			},
			wantFound: true,
		},
		{
			name:      "not found in context",
			setCtx:    false,
			wantFound: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			if tc.setCtx {
				ctx = WithCallerIdentity(ctx, tc.identity)
			}

			id, ok := CallerFromContext(ctx)
			if ok != tc.wantFound {
				t.Fatalf("CallerFromContext() ok = %v, want %v", ok, tc.wantFound)
			}
			if tc.wantFound && id.CallerAgent != tc.identity.CallerAgent {
				t.Fatalf("CallerFromContext() CallerAgent = %q, want %q", id.CallerAgent, tc.identity.CallerAgent)
			}
		})
	}
}

func TestAuthenticate_Success(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		bearerToken  string
		httpBody     string
		wantIdentity CallerIdentity
	}{
		{
			name:        "authenticated agent via alias",
			bearerToken: "alias-token",
			httpBody:    `{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":true,"user":{"username":"system:serviceaccount:default:eri","groups":["system:authenticated"]}}}`,
			wantIdentity: CallerIdentity{
				Username:    "system:serviceaccount:default:eri",
				CallerAgent: "aerial",
				Groups:      []string{"system:authenticated"},
				IsService:   false,
			},
		},
		{
			name:        "authenticated agent via prefix",
			bearerToken: "prefix-token",
			httpBody:    `{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":true,"user":{"username":"system:serviceaccount:default:eri-agent-alpha","groups":["system:authenticated"]}}}`,
			wantIdentity: CallerIdentity{
				Username:    "system:serviceaccount:default:eri-agent-alpha",
				CallerAgent: "agent-alpha",
				Groups:      []string{"system:authenticated"},
				IsService:   false,
			},
		},
		{
			name:        "authenticated control plane service",
			bearerToken: "service-token",
			httpBody:    `{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":true,"user":{"username":"system:serviceaccount:default:synapse","groups":["system:authenticated"]}}}`,
			wantIdentity: CallerIdentity{
				Username:    "system:serviceaccount:default:synapse",
				CallerAgent: "synapse",
				Groups:      []string{"system:authenticated"},
				IsService:   true,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tokenFile := filepath.Join(t.TempDir(), "token")
			if err := os.WriteFile(tokenFile, []byte("host-sa-token"), 0o600); err != nil {
				t.Fatalf("write host token: %v", err)
			}

			rt := &staticRoundTripper{status: http.StatusOK, body: tc.httpBody}
			auth := NewAuthenticator(Config{
				Enabled:      true,
				TokenPath:    tokenFile,
				APIServerURL: "https://k8s.local",
				HTTPClient:   &http.Client{Transport: rt},
			})

			id, err := auth.Authenticate(context.Background(), tc.bearerToken)
			if err != nil {
				t.Fatalf("Authenticate() unexpected error = %v", err)
			}
			if id.CallerAgent != tc.wantIdentity.CallerAgent {
				t.Fatalf("CallerAgent = %q, want %q", id.CallerAgent, tc.wantIdentity.CallerAgent)
			}
			if id.IsService != tc.wantIdentity.IsService {
				t.Fatalf("IsService = %v, want %v", id.IsService, tc.wantIdentity.IsService)
			}
		})
	}
}

func TestAuthenticate_CustomConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		bearerToken  string
		prefix       string
		aliases      map[string]string
		httpBody     string
		wantIdentity CallerIdentity
	}{
		{
			name:        "authenticated agent via saName alias",
			bearerToken: "saname-alias-token",
			aliases:     map[string]string{"eri": "aerial"},
			httpBody:    `{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":true,"user":{"username":"system:serviceaccount:custom:eri"}}}`,
			wantIdentity: CallerIdentity{
				Username:    "system:serviceaccount:custom:eri",
				CallerAgent: "aerial",
				IsService:   false,
			},
		},
		{
			name:        "authenticated agent via saName prefix",
			bearerToken: "saname-prefix-token",
			prefix:      "eri-",
			httpBody:    `{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":true,"user":{"username":"system:serviceaccount:custom:eri-agent-beta"}}}`,
			wantIdentity: CallerIdentity{
				Username:    "system:serviceaccount:custom:eri-agent-beta",
				CallerAgent: "agent-beta",
				IsService:   false,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tokenFile := filepath.Join(t.TempDir(), "token")
			if err := os.WriteFile(tokenFile, []byte("host-sa-token"), 0o600); err != nil {
				t.Fatalf("write host token: %v", err)
			}

			rt := &staticRoundTripper{status: http.StatusOK, body: tc.httpBody}
			cfg := Config{
				Enabled:        true,
				TokenPath:      tokenFile,
				AgentSAPrefix:  tc.prefix,
				SAAgentAliases: tc.aliases,
				APIServerURL:   "https://k8s.local",
				HTTPClient:     &http.Client{Transport: rt},
			}
			auth := NewAuthenticator(cfg)

			id, err := auth.Authenticate(context.Background(), tc.bearerToken)
			if err != nil {
				t.Fatalf("Authenticate() unexpected error = %v", err)
			}
			if id.CallerAgent != tc.wantIdentity.CallerAgent {
				t.Fatalf("CallerAgent = %q, want %q", id.CallerAgent, tc.wantIdentity.CallerAgent)
			}
			if id.IsService != tc.wantIdentity.IsService {
				t.Fatalf("IsService = %v, want %v", id.IsService, tc.wantIdentity.IsService)
			}
		})
	}
}

func TestAuthenticate_CacheHit(t *testing.T) {
	t.Parallel()

	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("host-sa-token"), 0o600); err != nil {
		t.Fatalf("write host token: %v", err)
	}

	rt := &staticRoundTripper{
		status: http.StatusOK,
		body:   `{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":true,"user":{"username":"system:serviceaccount:default:eri-agent-beta"}}}`,
	}
	auth := NewAuthenticator(Config{
		Enabled:      true,
		TokenPath:    tokenFile,
		APIServerURL: "https://k8s.local",
		HTTPClient:   &http.Client{Transport: rt},
	})

	id, err := auth.Authenticate(context.Background(), "cached-token")
	if err != nil {
		t.Fatalf("Authenticate() unexpected error = %v", err)
	}
	if id.CallerAgent != "agent-beta" {
		t.Fatalf("CallerAgent = %q, want %q", id.CallerAgent, "agent-beta")
	}

	rt.err = errors.New("transport should not be called on cache hit")
	cachedID, err := auth.Authenticate(context.Background(), "cached-token")
	if err != nil {
		t.Fatalf("Authenticate() repeat cache hit error = %v", err)
	}
	if cachedID.CallerAgent != "agent-beta" {
		t.Fatalf("cached CallerAgent = %q, want %q", cachedID.CallerAgent, "agent-beta")
	}
}

func TestAuthenticate_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		bearerToken  string
		hostTokenErr bool
		httpStatus   int
		httpBody     string
		transportErr bool
		wantUnauth   bool
	}{
		{
			name:        "empty token error",
			bearerToken: "",
			wantUnauth:  true,
		},
		{
			name:         "host token failure",
			bearerToken:  "test-token",
			hostTokenErr: true,
		},
		{
			name:         "network transport error",
			bearerToken:  "test-token",
			transportErr: true,
		},
		{
			name:        "api server http 500 error",
			bearerToken: "test-token",
			httpStatus:  http.StatusInternalServerError,
			httpBody:    `{"error":"internal"}`,
		},
		{
			name:        "api server malformed json",
			bearerToken: "test-token",
			httpStatus:  http.StatusOK,
			httpBody:    `not-json`,
		},
		{
			name:        "unauthenticated token",
			bearerToken: "bad-token",
			httpStatus:  http.StatusOK,
			httpBody:    `{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":false,"error":"token expired"}}`,
			wantUnauth:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tokenFile := filepath.Join(t.TempDir(), "token")
			if !tc.hostTokenErr {
				if err := os.WriteFile(tokenFile, []byte("host-sa-token"), 0o600); err != nil {
					t.Fatalf("write host token: %v", err)
				}
			}

			rt := &staticRoundTripper{status: tc.httpStatus, body: tc.httpBody}
			if tc.transportErr {
				rt.err = errors.New("simulated network failure")
			}

			cfg := Config{
				Enabled:      true,
				TokenPath:    tokenFile,
				APIServerURL: "https://k8s.local",
				HTTPClient:   &http.Client{Transport: rt},
			}
			auth := NewAuthenticator(cfg)

			_, err := auth.Authenticate(context.Background(), tc.bearerToken)
			if err == nil {
				t.Fatalf("expected error from Authenticate(), got nil")
			}

			if tc.wantUnauth {
				uErr, ok := errors.AsType[*UnauthorizedError](err)
				if !ok {
					t.Fatalf("expected UnauthorizedError, got %T: %v", err, err)
				}
				if uErr.StatusCode() != http.StatusUnauthorized {
					t.Fatalf("StatusCode() = %d, want 401", uErr.StatusCode())
				}
				if uErr.Error() == "" {
					t.Fatalf("expected non-empty Error()")
				}
			}
		})
	}
}

func TestAuthorize_Allowed(t *testing.T) {
	t.Parallel()

	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("host-sa-token"), 0o600); err != nil {
		t.Fatalf("write host token: %v", err)
	}

	rt := &staticRoundTripper{
		status: http.StatusOK,
		body:   `{"apiVersion":"authorization.k8s.io/v1","kind":"SubjectAccessReview","status":{"allowed":true}}`,
	}

	auth := NewAuthenticator(Config{
		Enabled:      true,
		TokenPath:    tokenFile,
		APIServerURL: "https://k8s.local",
		HTTPClient:   &http.Client{Transport: rt},
	})

	caller := CallerIdentity{
		Username:    "system:serviceaccount:default:eri-aerial",
		CallerAgent: "aerial",
		Groups:      []string{"group-a"},
		Extra:       map[string][]string{"role": {"admin"}},
	}
	if err := auth.Authorize(context.Background(), caller, "builds", "get", "aerial"); err != nil {
		t.Fatalf("Authorize() unexpected error = %v", err)
	}

	// Repeat call must hit cache without transport invocation
	rt.err = errors.New("transport should not be called on cache hit")
	if err := auth.Authorize(context.Background(), caller, "builds", "get", "aerial"); err != nil {
		t.Fatalf("Authorize() cache hit error = %v", err)
	}

	// Call with different groups must not hit cache
	callerDiffGroup := CallerIdentity{
		Username:    "system:serviceaccount:default:eri-aerial",
		CallerAgent: "aerial",
		Groups:      []string{"group-b"},
		Extra:       map[string][]string{"role": {"admin"}},
	}
	if err := auth.Authorize(context.Background(), callerDiffGroup, "builds", "get", "aerial"); err == nil {
		t.Fatalf("expected transport error on different groups cache miss, got nil")
	}
}

func TestAuthorize_Denied(t *testing.T) {
	t.Parallel()

	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("host-sa-token"), 0o600); err != nil {
		t.Fatalf("write host token: %v", err)
	}

	rt := &staticRoundTripper{
		status: http.StatusOK,
		body:   `{"apiVersion":"authorization.k8s.io/v1","kind":"SubjectAccessReview","status":{"allowed":false,"reason":"RBAC: access denied"}}`,
	}

	auth := NewAuthenticator(Config{
		Enabled:      true,
		TokenPath:    tokenFile,
		APIServerURL: "https://k8s.local",
		HTTPClient:   &http.Client{Transport: rt},
	})

	caller := CallerIdentity{Username: "system:serviceaccount:default:eri-agent-alpha", CallerAgent: "agent-alpha"}
	err := auth.Authorize(context.Background(), caller, "builds/all", "create", "aerial")
	if err == nil {
		t.Fatalf("expected ForbiddenError, got nil")
	}

	fErr, ok := errors.AsType[*ForbiddenError](err)
	if !ok {
		t.Fatalf("expected ForbiddenError, got %T: %v", err, err)
	}
	if fErr.StatusCode() != http.StatusForbidden {
		t.Fatalf("StatusCode() = %d, want 403", fErr.StatusCode())
	}
	if fErr.Error() == "" {
		t.Fatalf("expected non-empty Error()")
	}
}

func TestAuthorize_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		hostTokenErr bool
		httpStatus   int
		httpBody     string
		transportErr bool
	}{
		{
			name:         "host token error",
			hostTokenErr: true,
		},
		{
			name:         "transport error",
			transportErr: true,
		},
		{
			name:       "http 500 error",
			httpStatus: http.StatusInternalServerError,
			httpBody:   `{"error":"internal"}`,
		},
		{
			name:       "malformed json response",
			httpStatus: http.StatusOK,
			httpBody:   `not-valid-json`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tokenFile := filepath.Join(t.TempDir(), "token")
			if !tc.hostTokenErr {
				if err := os.WriteFile(tokenFile, []byte("host-sa-token"), 0o600); err != nil {
					t.Fatalf("write host token: %v", err)
				}
			}

			rt := &staticRoundTripper{status: tc.httpStatus, body: tc.httpBody}
			if tc.transportErr {
				rt.err = errors.New("simulated transport error")
			}

			auth := NewAuthenticator(Config{
				Enabled:      true,
				TokenPath:    tokenFile,
				APIServerURL: "https://k8s.local",
				HTTPClient:   &http.Client{Transport: rt},
			})

			caller := CallerIdentity{Username: "system:serviceaccount:default:eri"}
			if err := auth.Authorize(context.Background(), caller, "schedules", "get", ""); err == nil {
				t.Fatalf("expected error from Authorize(), got nil")
			}
		})
	}
}

func TestIsValidAgentSlug(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		slug string
		want bool
	}{
		{name: "single letter", slug: "a", want: true},
		{name: "single digit", slug: "9", want: true},
		{name: "hyphenated slug", slug: "agent-alpha-1", want: true},
		{name: "max 63 chars", slug: strings.Repeat("a", 63), want: true},
		{name: "empty slug", slug: "", want: false},
		{name: "64 chars too long", slug: strings.Repeat("a", 64), want: false},
		{name: "leading hyphen", slug: "-agent", want: false},
		{name: "trailing hyphen", slug: "agent-", want: false},
		{name: "uppercase rejected", slug: "Agent", want: false},
		{name: "underscore rejected", slug: "agent_alpha", want: false},
		{name: "dot rejected", slug: "agent.alpha", want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := IsValidAgentSlug(tc.slug); got != tc.want {
				t.Errorf("IsValidAgentSlug(%q) = %v, want %v", tc.slug, got, tc.want)
			}
		})
	}
}

func TestResolveTargetAgent_Read(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		requestedTarget string
		baseResource    string
		wantTarget      string
		wantResource    string
		wantErr         bool
	}{
		{
			name:            "valid agent slug target",
			requestedTarget: "agent-alpha",
			baseResource:    "builds",
			wantTarget:      "agent-alpha",
			wantResource:    "builds",
		},
		{
			name:            "empty target",
			requestedTarget: "",
			baseResource:    "transcripts",
			wantTarget:      "",
			wantResource:    "transcripts",
		},
		{
			name:            "wildcard target",
			requestedTarget: "*",
			baseResource:    "prs",
			wantTarget:      "*",
			wantResource:    "prs",
		},
		{
			name:            "invalid slug target fails validation",
			requestedTarget: "Invalid_Agent",
			baseResource:    "memories",
			wantErr:         true,
		},
	}

	caller := CallerIdentity{Username: "system:serviceaccount:default:eri-aerial", CallerAgent: "aerial"}
	auth := NewAuthenticator(Config{})

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			target, resource, err := auth.ResolveTargetAgent(caller, tc.requestedTarget, tc.baseResource, "get")
			if (err != nil) != tc.wantErr {
				t.Fatalf("ResolveTargetAgent() error = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantErr {
				vErr, ok := errors.AsType[*ValidationError](err)
				if !ok || vErr.StatusCode() != http.StatusBadRequest || vErr.Error() == "" {
					t.Fatalf("expected valid ValidationError, got %v", err)
				}
				return
			}
			if target != tc.wantTarget || resource != tc.wantResource {
				t.Fatalf("ResolveTargetAgent() = (%q, %q), want (%q, %q)", target, resource, tc.wantTarget, tc.wantResource)
			}
		})
	}
}

func TestResolveTargetAgent_WriteService(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		requestedTarget string
		wantTarget      string
		wantResource    string
		wantErr         bool
	}{
		{
			name:            "missing target fails validation",
			requestedTarget: "",
			wantErr:         true,
		},
		{
			name:            "invalid slug target fails validation",
			requestedTarget: "rogue_agent!",
			wantErr:         true,
		},
		{
			name:            "valid target escalates to all",
			requestedTarget: "agent-alpha",
			wantTarget:      "agent-alpha",
			wantResource:    "prs/all",
		},
	}

	serviceCaller := CallerIdentity{Username: "system:serviceaccount:default:synapse", CallerAgent: "synapse", IsService: true}
	auth := NewAuthenticator(Config{})

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			target, resource, err := auth.ResolveTargetAgent(serviceCaller, tc.requestedTarget, "prs", "create")
			if (err != nil) != tc.wantErr {
				t.Fatalf("ResolveTargetAgent() error = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantErr {
				vErr, ok := errors.AsType[*ValidationError](err)
				if !ok || vErr.StatusCode() != http.StatusBadRequest || vErr.Error() == "" {
					t.Fatalf("expected valid ValidationError, got %v", err)
				}
				return
			}
			if target != tc.wantTarget || resource != tc.wantResource {
				t.Fatalf("ResolveTargetAgent() = (%q, %q), want (%q, %q)", target, resource, tc.wantTarget, tc.wantResource)
			}
		})
	}
}

func TestResolveTargetAgent_WriteAgent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		requestedTarget string
		baseResource    string
		verb            string
		wantTarget      string
		wantResource    string
		wantErr         bool
	}{
		{
			name:            "empty target defaults to self",
			requestedTarget: "",
			baseResource:    "schedules",
			verb:            "create",
			wantTarget:      "aerial",
			wantResource:    "schedules",
		},
		{
			name:            "explicit self target stays base resource",
			requestedTarget: "aerial",
			baseResource:    "builds",
			verb:            "update",
			wantTarget:      "aerial",
			wantResource:    "builds",
		},
		{
			name:            "cross agent target escalates to all",
			requestedTarget: "agent-beta",
			baseResource:    "builds",
			verb:            "patch",
			wantTarget:      "agent-beta",
			wantResource:    "builds/all",
		},
		{
			name:            "wildcard target escalates to all",
			requestedTarget: "*",
			baseResource:    "memories",
			verb:            "delete",
			wantTarget:      "*",
			wantResource:    "memories/all",
		},
		{
			name:            "invalid slug target fails validation",
			requestedTarget: "non_existent_agent",
			baseResource:    "schedules",
			verb:            "delete",
			wantErr:         true,
		},
	}

	agentCaller := CallerIdentity{Username: "system:serviceaccount:default:eri-aerial", CallerAgent: "aerial", IsService: false}
	auth := NewAuthenticator(Config{})

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			target, resource, err := auth.ResolveTargetAgent(agentCaller, tc.requestedTarget, tc.baseResource, tc.verb)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ResolveTargetAgent() error = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantErr {
				vErr, ok := errors.AsType[*ValidationError](err)
				if !ok || vErr.StatusCode() != http.StatusBadRequest || vErr.Error() == "" {
					t.Fatalf("expected valid ValidationError, got %v", err)
				}
				return
			}
			if target != tc.wantTarget || resource != tc.wantResource {
				t.Fatalf("ResolveTargetAgent() = (%q, %q), want (%q, %q)", target, resource, tc.wantTarget, tc.wantResource)
			}
		})
	}
}

func TestAuthorizeTarget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		caller          CallerIdentity
		baseResource    string
		verb            string
		requestedTarget string
		sarAllowed      bool
		sarStatus       int
		wantTarget      string
		wantErr         bool
		wantForbidden   bool
		wantValidation  bool
	}{
		{
			name: "self write authorized",
			caller: CallerIdentity{
				Username:    "system:serviceaccount:default:eri-aerial",
				CallerAgent: "aerial",
				IsService:   false,
			},
			baseResource:    "builds",
			verb:            "create",
			requestedTarget: "aerial",
			sarAllowed:      true,
			sarStatus:       http.StatusOK,
			wantTarget:      "aerial",
		},
		{
			name: "cross agent write forbidden",
			caller: CallerIdentity{
				Username:    "system:serviceaccount:default:eri-agent-alpha",
				CallerAgent: "agent-alpha",
				IsService:   false,
			},
			baseResource:    "builds",
			verb:            "create",
			requestedTarget: "agent-beta",
			sarAllowed:      false,
			sarStatus:       http.StatusOK,
			wantErr:         true,
			wantForbidden:   true,
		},
		{
			name: "service caller missing target validation error",
			caller: CallerIdentity{
				Username:    "system:serviceaccount:default:synapse",
				CallerAgent: "synapse",
				IsService:   true,
			},
			baseResource:    "prs",
			verb:            "create",
			requestedTarget: "",
			wantErr:         true,
			wantValidation:  true,
		},
		{
			name: "apiserver error propagates",
			caller: CallerIdentity{
				Username:    "system:serviceaccount:default:eri-aerial",
				CallerAgent: "aerial",
				IsService:   false,
			},
			baseResource:    "builds",
			verb:            "get",
			requestedTarget: "aerial",
			sarStatus:       http.StatusInternalServerError,
			wantErr:         true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tokenFile := filepath.Join(t.TempDir(), "token")
			if err := os.WriteFile(tokenFile, []byte("host-sa-token"), 0o600); err != nil {
				t.Fatalf("write host token: %v", err)
			}

			sarBody := fmt.Sprintf(`{"apiVersion":"authorization.k8s.io/v1","kind":"SubjectAccessReview","status":{"allowed":%v}}`, tc.sarAllowed)
			rt := &staticRoundTripper{status: tc.sarStatus, body: sarBody}

			auth := NewAuthenticator(Config{
				Enabled:      true,
				TokenPath:    tokenFile,
				APIServerURL: "https://k8s.local",
				HTTPClient:   &http.Client{Transport: rt},
			})

			target, err := auth.AuthorizeTarget(context.Background(), tc.caller, tc.baseResource, tc.verb, tc.requestedTarget)
			if (err != nil) != tc.wantErr {
				t.Fatalf("AuthorizeTarget() error = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantValidation {
				if _, ok := errors.AsType[*ValidationError](err); !ok {
					t.Fatalf("expected ValidationError, got %T: %v", err, err)
				}
			}
			if tc.wantForbidden {
				if _, ok := errors.AsType[*ForbiddenError](err); !ok {
					t.Fatalf("expected ForbiddenError, got %T: %v", err, err)
				}
			}
			if tc.wantErr {
				return
			}
			if target != tc.wantTarget {
				t.Fatalf("targetAgent = %q, want %q", target, tc.wantTarget)
			}
		})
	}
}

func TestMiddleware(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		enabled      bool
		exemptPaths  []string
		reqPath      string
		authHeader   string
		authStatus   int
		authBody     string
		transportErr bool
		wantStatus   int
		wantAgent    string
	}{
		{
			name:       "disabled middleware passes through unauthenticated",
			enabled:    false,
			reqPath:    "/api/secure",
			wantStatus: http.StatusOK,
		},
		{
			name:        "exempt path passes through unauthenticated",
			enabled:     true,
			exemptPaths: []string{"/healthz"},
			reqPath:     "/healthz",
			wantStatus:  http.StatusOK,
		},
		{
			name:        "exempt path with trailing slash passes through",
			enabled:     true,
			exemptPaths: []string{"/healthz"},
			reqPath:     "/healthz/",
			wantStatus:  http.StatusOK,
		},
		{
			name:       "missing authorization header returns 401",
			enabled:    true,
			reqPath:    "/api/secure",
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "malformed authorization header returns 401",
			enabled:    true,
			reqPath:    "/api/secure",
			authHeader: "Basic invalid-token",
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "authenticate failure returns 401",
			enabled:    true,
			reqPath:    "/api/secure",
			authHeader: "Bearer bad-token",
			authStatus: http.StatusOK,
			authBody:   `{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":false}}`,
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:         "authenticate infrastructure failure returns 500",
			enabled:      true,
			reqPath:      "/api/secure",
			authHeader:   "Bearer valid-token",
			transportErr: true,
			wantStatus:   http.StatusInternalServerError,
		},
		{
			name:       "authenticated request attaches caller to context",
			enabled:    true,
			reqPath:    "/api/secure",
			authHeader: "Bearer valid-token",
			authStatus: http.StatusOK,
			authBody:   `{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":true,"user":{"username":"system:serviceaccount:default:eri-agent-alpha"}}}`,
			wantStatus: http.StatusOK,
			wantAgent:  "agent-alpha",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tokenFile := filepath.Join(t.TempDir(), "token")
			if err := os.WriteFile(tokenFile, []byte("host-sa-token"), 0o600); err != nil {
				t.Fatalf("write host token: %v", err)
			}

			rt := &staticRoundTripper{status: tc.authStatus, body: tc.authBody}
			if tc.transportErr {
				rt.err = errors.New("simulated network error")
			}

			auth := NewAuthenticator(Config{
				Enabled:      tc.enabled,
				TokenPath:    tokenFile,
				APIServerURL: "https://k8s.local",
				HTTPClient:   &http.Client{Transport: rt},
			})

			var capturedAgent string
			handler := auth.Middleware(tc.exemptPaths...)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if id, ok := CallerFromContext(r.Context()); ok {
					capturedAgent = id.CallerAgent
				}
				w.WriteHeader(http.StatusOK)
			}))

			req := httptest.NewRequest(http.MethodGet, tc.reqPath, http.NoBody)
			if tc.authHeader != "" {
				req.Header.Set("Authorization", tc.authHeader)
			}
			rec := httptest.NewRecorder()

			handler.ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("response status = %d, want %d (body: %s)", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if tc.wantAgent != "" && capturedAgent != tc.wantAgent {
				t.Fatalf("captured caller agent = %q, want %q", capturedAgent, tc.wantAgent)
			}
		})
	}
}

func TestNewAuthenticator_APIServerURL(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		envHost string
		envPort string
		wantURL string
	}{
		{
			name:    "default url when env unset",
			wantURL: "https://kubernetes.default.svc",
		},
		{
			name:    "custom url with trailing slash trimmed",
			url:     "https://apiserver.custom.domain/",
			wantURL: "https://apiserver.custom.domain",
		},
		{
			name:    "environment service host and port used",
			envHost: "192.0.2.1",
			envPort: "443",
			wantURL: "https://192.0.2.1:443",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.envHost != "" {
				t.Setenv("KUBERNETES_SERVICE_HOST", tc.envHost)
				t.Setenv("KUBERNETES_SERVICE_PORT", tc.envPort)
			} else {
				t.Setenv("KUBERNETES_SERVICE_HOST", "")
				t.Setenv("KUBERNETES_SERVICE_PORT", "")
			}

			auth := NewAuthenticator(Config{APIServerURL: tc.url})
			if auth.cfg.APIServerURL != tc.wantURL {
				t.Fatalf("APIServerURL = %q, want %q", auth.cfg.APIServerURL, tc.wantURL)
			}
		})
	}
}

func TestNewAuthenticator_Defaults(t *testing.T) {
	t.Parallel()

	auth := NewAuthenticator(Config{})
	if auth.cfg.CacheTTL <= 0 {
		t.Fatalf("expected positive default CacheTTL, got %v", auth.cfg.CacheTTL)
	}
	if auth.cfg.CacheMaxEntries <= 0 {
		t.Fatalf("expected positive default CacheMaxEntries, got %d", auth.cfg.CacheMaxEntries)
	}
	if auth.cfg.AgentSAPrefix == "" {
		t.Fatalf("expected non-empty default AgentSAPrefix")
	}
	if auth.cfg.TokenPath == "" {
		t.Fatalf("expected non-empty default TokenPath")
	}
}

func TestNewAuthenticator_CustomConfig(t *testing.T) {
	t.Parallel()

	cfg := Config{
		TokenPath:       "/custom/token",
		AgentSAPrefix:   "custom-prefix-",
		SAAgentAliases:  map[string]string{"custom": "aerial"},
		CacheTTL:        10 * time.Second,
		CacheMaxEntries: 500,
	}
	auth := NewAuthenticator(cfg)

	if auth.cfg.TokenPath != cfg.TokenPath {
		t.Fatalf("TokenPath = %q, want %q", auth.cfg.TokenPath, cfg.TokenPath)
	}
	if auth.cfg.AgentSAPrefix != cfg.AgentSAPrefix {
		t.Fatalf("AgentSAPrefix = %q, want %q", auth.cfg.AgentSAPrefix, cfg.AgentSAPrefix)
	}
	if auth.cfg.CacheTTL != cfg.CacheTTL {
		t.Fatalf("CacheTTL = %v, want %v", auth.cfg.CacheTTL, cfg.CacheTTL)
	}
	if auth.cfg.CacheMaxEntries != cfg.CacheMaxEntries {
		t.Fatalf("CacheMaxEntries = %d, want %d", auth.cfg.CacheMaxEntries, cfg.CacheMaxEntries)
	}
}
