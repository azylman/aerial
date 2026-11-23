package k8sauth

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
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
				if _, ok := cache.Get(tc.touchKey); !ok {
					t.Fatalf("expected touch key %q to exist", tc.touchKey)
				}
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

			req, err := http.NewRequest(http.MethodGet, "http://localhost/test", http.NoBody)
			if err != nil {
				t.Fatalf("http.NewRequest: %v", err)
			}
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

// newTestAuth returns an enabled Authenticator that posts to https://k8s.local
// through rt. The host token file exists only when hostToken is true; its
// path is auth.Config().TokenPath.
func newTestAuth(t *testing.T, rt http.RoundTripper, hostToken bool, cfg Config) *Authenticator {
	t.Helper()
	cfg.Enabled = true
	cfg.TokenPath = filepath.Join(t.TempDir(), "token")
	cfg.APIServerURL = "https://k8s.local"
	cfg.HTTPClient = &http.Client{Transport: rt}
	if hostToken {
		writeTokenFile(t, cfg.TokenPath, "host-sa-token")
	}
	return NewAuthenticator(cfg)
}

// authErrText returns err's text with the host token path (when set)
// replaced by TOKEN, or "" for a nil error.
func authErrText(auth *Authenticator, err error) string {
	if path := auth.Config().TokenPath; path != "" {
		return tokenErrText(err, path)
	}
	if err == nil {
		return ""
	}
	return err.Error()
}

// authErrStatus returns the StatusCode of a coded error, or 0.
func authErrStatus(err error) int {
	if coded, ok := errors.AsType[interface {
		error
		StatusCode() int
	}](err); ok {
		return coded.StatusCode()
	}
	return 0
}

// tokenReview returns an authenticated TokenReview response body for user.
func tokenReview(user string) string {
	return `{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":true,"user":{"username":"` +
		user + `","groups":["system:authenticated"]}}}`
}

// sar returns a SubjectAccessReview response body with the given verdict.
func sar(allowed bool) string {
	return fmt.Sprintf(`{"apiVersion":"authorization.k8s.io/v1","kind":"SubjectAccessReview","status":{"allowed":%t}}`, allowed)
}

var errAuthTransport = errors.New("simulated network failure")

func TestAuthenticate(t *testing.T) {
	t.Parallel()

	authed := []string{"system:authenticated"}
	tests := []struct {
		name        string
		cfg         Config
		token       string
		noHostToken bool
		rt          staticRoundTripper
		want        CallerIdentity
		wantErr     string
		wantStatus  int
	}{
		{
			name: "agent via username alias", token: "alias-token", rt: staticRoundTripper{body: tokenReview("system:serviceaccount:default:eri")},
			want: CallerIdentity{Username: "system:serviceaccount:default:eri", CallerAgent: "aerial", Groups: authed},
		},
		{
			name: "agent via username prefix", token: "prefix-token",
			rt:   staticRoundTripper{body: tokenReview("system:serviceaccount:default:eri-agent-alpha")},
			want: CallerIdentity{Username: "system:serviceaccount:default:eri-agent-alpha", CallerAgent: "agent-alpha", Groups: authed},
		},
		{
			name: "control plane service", token: "service-token", rt: staticRoundTripper{body: tokenReview("system:serviceaccount:default:synapse")},
			want: CallerIdentity{Username: "system:serviceaccount:default:synapse", CallerAgent: "synapse", Groups: authed, IsService: true},
		},
		{
			name: "agent via saName alias", cfg: Config{SAAgentAliases: map[string]string{"eri": "aerial"}}, token: "saname-alias-token",
			rt:   staticRoundTripper{body: tokenReview("system:serviceaccount:custom:eri")},
			want: CallerIdentity{Username: "system:serviceaccount:custom:eri", CallerAgent: "aerial", Groups: authed},
		},
		{
			name: "agent via saName prefix", cfg: Config{AgentSAPrefix: "eri-"}, token: "saname-prefix-token",
			rt:   staticRoundTripper{body: tokenReview("system:serviceaccount:custom:eri-agent-beta")},
			want: CallerIdentity{Username: "system:serviceaccount:custom:eri-agent-beta", CallerAgent: "agent-beta", Groups: authed},
		},
		{name: "empty bearer token", wantErr: "empty bearer token", wantStatus: http.StatusUnauthorized},
		{
			name: "missing host token", token: "test-token", noHostToken: true,
			wantErr: "k8sauth: tokenreview: read host token: read serviceaccount token from TOKEN: open TOKEN: no such file or directory",
		},
		{
			name: "network transport error", token: "test-token", rt: staticRoundTripper{err: errAuthTransport},
			wantErr: `k8sauth: tokenreview: execute request: Post "https://k8s.local/apis/authentication.k8s.io/v1/tokenreviews": simulated network failure`,
		},
		{
			name: "api server http 500", token: "test-token",
			rt:      staticRoundTripper{status: http.StatusInternalServerError, body: `{"error":"internal"}`},
			wantErr: "k8sauth: tokenreview: api returned status 500",
		},
		{
			name: "api server malformed json", token: "test-token", rt: staticRoundTripper{body: "not-json"},
			wantErr: "k8sauth: tokenreview: decode response: invalid character 'o' in literal null (expecting 'u')",
		},
		{
			name: "unauthenticated with reason", token: "bad-token", wantErr: "token expired", wantStatus: http.StatusUnauthorized,
			rt: staticRoundTripper{body: `{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":false,"error":"token expired"}}`},
		},
		{
			name: "unauthenticated without reason", token: "bad-token", wantErr: "token not authenticated", wantStatus: http.StatusUnauthorized,
			rt: staticRoundTripper{body: `{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":false}}`},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rt := tc.rt
			auth := newTestAuth(t, &rt, !tc.noHostToken, tc.cfg)
			got, err := auth.Authenticate(context.Background(), tc.token)
			if gotErr := authErrText(auth, err); gotErr != tc.wantErr {
				t.Fatalf("Authenticate() error = %q, want %q", gotErr, tc.wantErr)
			}
			if gotStatus := authErrStatus(err); gotStatus != tc.wantStatus {
				t.Fatalf("Authenticate() error status = %d, want %d", gotStatus, tc.wantStatus)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("Authenticate() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestAuthenticate_CacheHit(t *testing.T) {
	t.Parallel()

	rt := &staticRoundTripper{body: tokenReview("system:serviceaccount:default:eri-agent-beta")}
	auth := newTestAuth(t, rt, true, Config{})

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

func TestAuthorize(t *testing.T) {
	t.Parallel()

	caller := CallerIdentity{
		Username:    "system:serviceaccount:default:eri-agent-alpha",
		CallerAgent: "agent-alpha",
		Groups:      []string{"group-a"},
		Extra:       map[string][]string{"role": {"admin"}},
	}
	tests := []struct {
		name        string
		noHostToken bool
		rt          staticRoundTripper
		wantErr     string
		wantStatus  int
	}{
		{name: "allowed", rt: staticRoundTripper{body: sar(true)}},
		{
			name: "denied with reason", wantErr: "RBAC: access denied", wantStatus: http.StatusForbidden,
			rt: staticRoundTripper{body: `{"apiVersion":"authorization.k8s.io/v1","kind":"SubjectAccessReview","status":{"allowed":false,"reason":"RBAC: access denied"}}`},
		},
		{
			name: "denied without reason", rt: staticRoundTripper{body: sar(false)}, wantStatus: http.StatusForbidden,
			wantErr: `access denied for user "system:serviceaccount:default:eri-agent-alpha" on builds/all/create (target: "aerial")`,
		},
		{
			name: "missing host token", noHostToken: true,
			wantErr: "k8sauth: subjectaccessreview: read host token: read serviceaccount token from TOKEN: open TOKEN: no such file or directory",
		},
		{
			name: "transport error", rt: staticRoundTripper{err: errAuthTransport},
			wantErr: `k8sauth: subjectaccessreview: execute request: Post "https://k8s.local/apis/authorization.k8s.io/v1/subjectaccessreviews": simulated network failure`,
		},
		{
			name: "api server http 500", rt: staticRoundTripper{status: http.StatusInternalServerError, body: `{"error":"internal"}`},
			wantErr: "k8sauth: subjectaccessreview: api returned status 500",
		},
		{
			name: "api server malformed json", rt: staticRoundTripper{body: "not-valid-json"},
			wantErr: "k8sauth: subjectaccessreview: decode response: invalid character 'o' in literal null (expecting 'u')",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rt := tc.rt
			auth := newTestAuth(t, &rt, !tc.noHostToken, Config{})
			err := auth.Authorize(context.Background(), caller, "builds/all", "create", "aerial")
			if gotErr := authErrText(auth, err); gotErr != tc.wantErr {
				t.Fatalf("Authorize() error = %q, want %q", gotErr, tc.wantErr)
			}
			if gotStatus := authErrStatus(err); gotStatus != tc.wantStatus {
				t.Fatalf("Authorize() error status = %d, want %d", gotStatus, tc.wantStatus)
			}
		})
	}
}

func TestAuthorize_CacheHit(t *testing.T) {
	t.Parallel()

	rt := &staticRoundTripper{body: sar(true)}
	auth := newTestAuth(t, rt, true, Config{})

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
	callerDiffGroup := caller
	callerDiffGroup.Groups = []string{"group-b"}
	if err := auth.Authorize(context.Background(), callerDiffGroup, "builds", "get", "aerial"); err == nil {
		t.Fatalf("expected transport error on different groups cache miss, got nil")
	}
}

func TestResolveTargetAgent(t *testing.T) {
	t.Parallel()

	agent := CallerIdentity{Username: "system:serviceaccount:default:eri-aerial", CallerAgent: "aerial"}
	service := CallerIdentity{Username: "system:serviceaccount:default:synapse", CallerAgent: "synapse", IsService: true}
	tests := []struct {
		name         string
		caller       CallerIdentity
		target       string
		resource     string
		verb         string
		wantTarget   string
		wantResource string
		wantErr      string
		wantStatus   int
	}{
		{
			name: "read valid slug target", caller: agent, target: "agent-alpha", resource: "builds", verb: "get",
			wantTarget: "agent-alpha", wantResource: "builds",
		},
		{name: "read empty target", caller: agent, resource: "transcripts", verb: "get", wantResource: "transcripts"},
		{name: "read wildcard target", caller: agent, target: "*", resource: "prs", verb: "list", wantTarget: "*", wantResource: "prs"},
		{
			name: "read invalid slug fails validation", caller: agent, target: "Invalid_Agent", resource: "memories", verb: "get",
			wantErr: `invalid target agent "Invalid_Agent"`, wantStatus: http.StatusBadRequest,
		},
		{
			name: "service write missing target fails validation", caller: service, resource: "prs", verb: "create",
			wantErr: `missing or invalid target agent "" for service principal`, wantStatus: http.StatusBadRequest,
		},
		{
			name: "service write invalid slug fails validation", caller: service, target: "rogue_agent!", resource: "prs", verb: "create",
			wantErr: `missing or invalid target agent "rogue_agent!" for service principal`, wantStatus: http.StatusBadRequest,
		},
		{
			name: "service write valid target escalates to all", caller: service, target: "agent-alpha", resource: "prs", verb: "create",
			wantTarget: "agent-alpha", wantResource: "prs/all",
		},
		{
			name: "agent write empty target defaults to self", caller: agent, resource: "schedules", verb: "create",
			wantTarget: "aerial", wantResource: "schedules",
		},
		{
			name: "agent write explicit self stays base resource", caller: agent, target: "aerial", resource: "builds", verb: "update",
			wantTarget: "aerial", wantResource: "builds",
		},
		{
			name: "agent write cross agent escalates to all", caller: agent, target: "agent-beta", resource: "builds", verb: "patch",
			wantTarget: "agent-beta", wantResource: "builds/all",
		},
		{
			name: "agent write wildcard escalates to all", caller: agent, target: "*", resource: "memories", verb: "delete",
			wantTarget: "*", wantResource: "memories/all",
		},
		{
			name: "agent write invalid slug fails validation", caller: agent, target: "non_existent_agent", resource: "schedules", verb: "delete",
			wantErr: `invalid target agent "non_existent_agent"`, wantStatus: http.StatusBadRequest,
		},
	}

	auth := &Authenticator{}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			target, resource, err := auth.ResolveTargetAgent(tc.caller, tc.target, tc.resource, tc.verb)
			if gotErr := authErrText(auth, err); gotErr != tc.wantErr {
				t.Fatalf("ResolveTargetAgent() error = %q, want %q", gotErr, tc.wantErr)
			}
			if gotStatus := authErrStatus(err); gotStatus != tc.wantStatus {
				t.Fatalf("ResolveTargetAgent() error status = %d, want %d", gotStatus, tc.wantStatus)
			}
			if target != tc.wantTarget || resource != tc.wantResource {
				t.Fatalf("ResolveTargetAgent() = (%q, %q), want (%q, %q)", target, resource, tc.wantTarget, tc.wantResource)
			}
		})
	}
}

// authConfigDefaults returns the Config NewAuthenticator builds from an empty
// Config with client and apiServerURL.
func authConfigDefaults(client *http.Client, apiServerURL string) Config {
	return Config{
		TokenPath:       "/var/run/secrets/kubernetes.io/serviceaccount/token",
		AgentSAPrefix:   "system:serviceaccount:default:eri-",
		SAAgentAliases:  map[string]string{"system:serviceaccount:default:eri": "aerial"},
		CacheTTL:        45 * time.Second,
		CacheMaxEntries: 1024,
		APIServerURL:    apiServerURL,
		HTTPClient:      client,
	}
}

func TestNewAuthenticator(t *testing.T) {
	// Not parallel: rows set KUBERNETES_SERVICE_HOST and KUBERNETES_SERVICE_PORT.
	// Every row passes HTTPClient so no row reads the in-cluster CA bundle.
	client := &http.Client{Transport: &staticRoundTripper{}}
	custom := Config{
		Enabled:         true,
		TokenPath:       "/custom/token",
		AgentSAPrefix:   "custom-prefix-",
		SAAgentAliases:  map[string]string{"custom": "aerial"},
		CacheTTL:        10 * time.Second,
		CacheMaxEntries: 500,
		APIServerURL:    "https://apiserver.example.com",
		HTTPClient:      client,
	}
	tests := []struct {
		name    string
		cfg     Config
		envHost string
		envPort string
		want    Config
	}{
		{name: "defaults when env unset", cfg: Config{HTTPClient: client}, want: authConfigDefaults(client, "https://kubernetes.default.svc")},
		{
			name: "custom url trailing slash trimmed", cfg: Config{APIServerURL: "https://apiserver.example.com/", HTTPClient: client},
			want: authConfigDefaults(client, "https://apiserver.example.com"),
		},
		{
			name: "service host and port env used", cfg: Config{HTTPClient: client}, envHost: "192.0.2.1", envPort: "443",
			want: authConfigDefaults(client, "https://192.0.2.1:443"),
		},
		{
			name: "host env without port falls back", cfg: Config{HTTPClient: client}, envHost: "192.0.2.1",
			want: authConfigDefaults(client, "https://kubernetes.default.svc"),
		},
		{name: "custom config kept", cfg: custom, want: custom},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("KUBERNETES_SERVICE_HOST", tc.envHost)
			t.Setenv("KUBERNETES_SERVICE_PORT", tc.envPort)

			got := NewAuthenticator(tc.cfg).Config()
			if got.Now == nil {
				t.Fatalf("NewAuthenticator() Config().Now = nil, want time.Now default")
			}
			got.Now = nil
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("NewAuthenticator() Config() = %+v, want %+v", got, tc.want)
			}
		})
	}
}
