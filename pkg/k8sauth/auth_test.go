package k8sauth

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func trResp(auth bool, user string, groups []string, extra map[string][]string) tokenReviewResponse {
	return tokenReviewResponse{
		APIVersion: "authentication.k8s.io/v1", Kind: "TokenReview",
		Status: tokenReviewStatus{
			Authenticated: auth,
			User:          tokenReviewUser{Username: user, Groups: groups, Extra: extra},
		},
	}
}

func sarResp(allowed bool, reason string) sarResponse {
	return sarResponse{
		APIVersion: "authorization.k8s.io/v1", Kind: "SubjectAccessReview",
		Status: sarStatus{Allowed: allowed, Reason: reason},
	}
}

func handleTokenReview(w http.ResponseWriter, r *http.Request, counter *int32) {
	if counter != nil {
		atomic.AddInt32(counter, 1)
	}
	var trReq tokenReviewRequest
	if err := json.NewDecoder(r.Body).Decode(&trReq); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	switch trReq.Spec.Token {
	case "token-500":
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("api server internal error"))
	case "token-bad-json":
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("invalid json"))
	case "token-unauthenticated":
		_ = json.NewEncoder(w).Encode(trResp(false, "", nil, nil))
	case "token-aerial":
		_ = json.NewEncoder(w).Encode(trResp(true, "system:serviceaccount:default:eri-aerial", []string{"system:serviceaccounts"}, map[string][]string{"role": {"operator"}}))
	case "token-eri":
		_ = json.NewEncoder(w).Encode(trResp(true, "system:serviceaccount:default:eri", nil, nil))
	case "token-miorine":
		_ = json.NewEncoder(w).Encode(trResp(true, "system:serviceaccount:default:eri-miorine", nil, nil))
	case "token-suletta":
		_ = json.NewEncoder(w).Encode(trResp(true, "system:serviceaccount:default:eri-suletta", nil, nil))
	case "token-synapse":
		_ = json.NewEncoder(w).Encode(trResp(true, "system:serviceaccount:default:synapse", nil, nil))
	case "token-custom-sa":
		_ = json.NewEncoder(w).Encode(trResp(true, "custom-sa", nil, nil))
	case "token-clean-prefix":
		_ = json.NewEncoder(w).Encode(trResp(true, "system:serviceaccount:default:prefix-test", nil, nil))
	default:
		w.WriteHeader(http.StatusUnauthorized)
	}
}

func handleSAR(w http.ResponseWriter, r *http.Request, counter *int32) {
	if counter != nil {
		atomic.AddInt32(counter, 1)
	}
	var sarReq sarRequest
	if err := json.NewDecoder(r.Body).Decode(&sarReq); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	switch sarReq.Spec.User {
	case "user-500":
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("sar internal error"))
	case "user-bad-json":
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("invalid json"))
	case "user-denied":
		_ = json.NewEncoder(w).Encode(sarResp(false, "permission denied by policy"))
	case "user-denied-no-reason":
		_ = json.NewEncoder(w).Encode(sarResp(false, ""))
	default:
		if strings.HasSuffix(sarReq.Spec.ResourceAttributes.Resource, "/all") {
			isSupervisor := sarReq.Spec.User == "system:serviceaccount:default:eri-aerial" ||
				sarReq.Spec.User == "system:serviceaccount:default:synapse"
			if isSupervisor {
				_ = json.NewEncoder(w).Encode(sarResp(true, ""))
			} else {
				_ = json.NewEncoder(w).Encode(sarResp(false, "subresource /all requires supervisor role"))
			}
		} else {
			_ = json.NewEncoder(w).Encode(sarResp(true, ""))
		}
	}
}

func newMockK8sServer(t *testing.T, trCounter, sarCounter *int32) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/apis/authentication.k8s.io/v1/tokenreviews":
			handleTokenReview(w, r, trCounter)
		case "/apis/authorization.k8s.io/v1/subjectaccessreviews":
			handleSAR(w, r, sarCounter)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
}

func writeTempToken(t *testing.T, token string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "token")
	if err := os.WriteFile(p, []byte(token), 0o600); err != nil {
		t.Fatalf("failed to write temp token: %v", err)
	}
	return p
}

func TestLRUCache(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		maxEntries int
		ttl        time.Duration
		nilClock   bool
		initKeys   []string
		initVals   []string
		advanceSec int
		getKey     string
		wantVal    string
		wantFound  bool
		deleteKey  string
		purgeAfter bool
		wantLen    int
	}{
		{name: "basic hit", maxEntries: 10, ttl: 10 * time.Second, initKeys: []string{"k1", "k2"}, initVals: []string{"v1", "v2"}, getKey: "k1", wantVal: "v1", wantFound: true, wantLen: 2},
		{name: "missing key", maxEntries: 10, ttl: 10 * time.Second, initKeys: []string{"k1"}, initVals: []string{"v1"}, getKey: "k99", wantVal: "", wantFound: false, wantLen: 1},
		{name: "ttl expired evicted", maxEntries: 10, ttl: 5 * time.Second, initKeys: []string{"k1"}, initVals: []string{"v1"}, advanceSec: 10, getKey: "k1", wantVal: "", wantFound: false, wantLen: 0},
		{name: "lru capacity eviction", maxEntries: 2, ttl: 10 * time.Second, initKeys: []string{"k1", "k2", "k3"}, initVals: []string{"v1", "v2", "v3"}, getKey: "k1", wantVal: "", wantFound: false, wantLen: 2},
		{name: "update existing key", maxEntries: 5, ttl: 10 * time.Second, initKeys: []string{"k1", "k1"}, initVals: []string{"v1", "v1-updated"}, getKey: "k1", wantVal: "v1-updated", wantFound: true, wantLen: 1},
		{name: "delete key", maxEntries: 5, ttl: 10 * time.Second, initKeys: []string{"k1", "k2"}, initVals: []string{"v1", "v2"}, deleteKey: "k1", getKey: "k1", wantVal: "", wantFound: false, wantLen: 1},
		{name: "purge clears all", maxEntries: 5, ttl: 10 * time.Second, initKeys: []string{"k1", "k2"}, initVals: []string{"v1", "v2"}, purgeAfter: true, getKey: "k1", wantVal: "", wantFound: false, wantLen: 0},
		{name: "default fallbacks", maxEntries: 0, ttl: 0, initKeys: []string{"k1"}, initVals: []string{"v1"}, getKey: "k1", wantVal: "v1", wantFound: true, wantLen: 1},
		{name: "nil clock fallback", maxEntries: 10, ttl: 10 * time.Second, nilClock: true, initKeys: []string{"k1"}, initVals: []string{"v1"}, getKey: "k1", wantVal: "v1", wantFound: true, wantLen: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			nowTime := time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC)
			var nowFunc func() time.Time
			if !tc.nilClock {
				nowFunc = func() time.Time { return nowTime }
			}

			cache := NewLRUCache[string](tc.maxEntries, tc.ttl, nowFunc)
			for i := range tc.initKeys {
				cache.Set(tc.initKeys[i], tc.initVals[i])
			}

			if tc.deleteKey != "" {
				cache.Delete(tc.deleteKey)
			}
			if tc.purgeAfter {
				cache.Purge()
			}
			if tc.advanceSec > 0 {
				nowTime = nowTime.Add(time.Duration(tc.advanceSec) * time.Second)
			}

			val, ok := cache.Get(tc.getKey)
			if ok != tc.wantFound || val != tc.wantVal || cache.Len() != tc.wantLen {
				t.Fatalf("cache.Get(%q) = (%q, %v) len=%d, want (%q, %v) len=%d", tc.getKey, val, ok, cache.Len(), tc.wantVal, tc.wantFound, tc.wantLen)
			}
		})
	}
}

type tokenSourceTestCase struct {
	name           string
	fileContent    string
	customTTL      time.Duration
	advanceSec     int
	updateFile     string
	wantToken      string
	wantErr        bool
	wantPath       string
	emptyTokenPath bool
	doubleCheck    bool
}

func execTokenSourceCase(t *testing.T, tc tokenSourceTestCase) {
	t.Helper()
	nowTime := time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC)
	nowFunc := func() time.Time { return nowTime }

	var tokenPath string
	switch {
	case tc.emptyTokenPath:
		tokenPath = ""
	case tc.wantPath != "":
		tokenPath = tc.wantPath
	default:
		tokenPath = writeTempToken(t, tc.fileContent)
	}

	ts := NewTokenSource(tokenPath, WithTokenSourceClock(nowFunc), WithTokenSourceTTL(tc.customTTL))
	expectedPath := tokenPath
	if tc.emptyTokenPath {
		expectedPath = DefaultServiceAccountTokenPath
	}
	if ts.TokenPath() != expectedPath {
		t.Fatalf("ts.TokenPath() = %q, want %q", ts.TokenPath(), expectedPath)
	}

	tok, err := ts.Token()
	if tc.wantErr {
		if err == nil {
			t.Fatalf("expected error, got nil token %q", tok)
		}
		return
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if tc.doubleCheck {
		tok2, err2 := ts.Token()
		if err2 != nil || tok2 != tc.wantToken {
			t.Fatalf("second call token = (%q, %v), want (%q, nil)", tok2, err2, tc.wantToken)
		}
	}

	if tc.updateFile != "" {
		if writeErr := os.WriteFile(tokenPath, []byte(tc.updateFile), 0o600); writeErr != nil {
			t.Fatalf("failed to update token file: %v", writeErr)
		}
		nowTime = nowTime.Add(time.Duration(tc.advanceSec) * time.Second)
		tok, err = ts.Token()
		if err != nil {
			t.Fatalf("unexpected error after reload: %v", err)
		}
	}

	if tok != tc.wantToken {
		t.Fatalf("ts.Token() = %q, want %q", tok, tc.wantToken)
	}
}

func TestTokenSource(t *testing.T) {
	t.Parallel()

	tests := []tokenSourceTestCase{
		{name: "valid token read and cached", fileContent: "initial-token-123\n", wantToken: "initial-token-123"},
		{name: "cache hit before expiry", fileContent: "cached-token\n", customTTL: 30 * time.Second, advanceSec: 10, updateFile: "new-unseen\n", wantToken: "cached-token"},
		{name: "reloaded after ttl expiry", fileContent: "old-token\n", customTTL: 10 * time.Second, advanceSec: 15, updateFile: "reloaded-token\n", wantToken: "reloaded-token"},
		{name: "empty token file returns error", fileContent: "   \n", wantErr: true},
		{name: "non-existent file returns error", fileContent: "", wantErr: true, wantPath: "/non/existent/path/for/token"},
		{name: "empty path defaults to default", emptyTokenPath: true, wantErr: true, wantPath: DefaultServiceAccountTokenPath},
		{name: "double check hit returns cached", fileContent: "double-checked\n", doubleCheck: true, wantToken: "double-checked"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			execTokenSourceCase(t, tc)
		})
	}
}

type roundTripRecorder struct {
	capturedAuthHeader string
}

func (r *roundTripRecorder) RoundTrip(req *http.Request) (*http.Response, error) {
	r.capturedAuthHeader = req.Header.Get("Authorization")
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(bytes.NewReader([]byte("ok"))),
		Header:     make(http.Header),
	}, nil
}

func TestNewServiceAccountTransport(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		tokenVal    string
		tokenErr    bool
		nilBase     bool
		nilSource   bool
		wantAuthHdr string
		wantErr     bool
	}{
		{name: "successful header injection", tokenVal: "my-sa-bearer-jwt", wantAuthHdr: "Bearer my-sa-bearer-jwt"},
		{name: "token read failure returns error", tokenErr: true, wantErr: true},
		{name: "nil base and nil source fallback", nilBase: true, nilSource: true, tokenErr: true, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := &roundTripRecorder{}
			var tokenPath string
			if tc.tokenErr {
				tokenPath = "/non/existent/token"
			} else {
				tokenPath = writeTempToken(t, tc.tokenVal)
			}

			var transport http.RoundTripper
			if tc.nilBase || tc.nilSource {
				transport = NewServiceAccountTransportWithSource(nil, nil)
			} else {
				transport = NewServiceAccountTransport(rec, tokenPath)
			}
			client := &http.Client{Transport: transport}

			req, err := http.NewRequest(http.MethodGet, "http://example.com/api", http.NoBody)
			if err != nil {
				t.Fatalf("failed to create request: %v", err)
			}

			resp, err := client.Do(req)
			if tc.wantErr && err == nil {
				_ = resp.Body.Close()
				t.Fatalf("expected error from RoundTrip, got nil")
			}
			if !tc.wantErr {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				_ = resp.Body.Close()
				if rec.capturedAuthHeader != tc.wantAuthHdr {
					t.Fatalf("Authorization header = %q, want %q", rec.capturedAuthHeader, tc.wantAuthHdr)
				}
			}
		})
	}
}

func TestExtractBearerToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		authHeader string
		nilRequest bool
		wantToken  string
		wantErr    bool
	}{
		{name: "valid standard bearer token", authHeader: "Bearer eyJhbGciOi...", wantToken: "eyJhbGciOi..."},
		{name: "case-insensitive bearer prefix", authHeader: "bearer token-val", wantToken: "token-val"},
		{name: "missing authorization header", authHeader: "", wantErr: true},
		{name: "malformed scheme", authHeader: "Basic dXNlcjpwYXNz", wantErr: true},
		{name: "bearer without token string", authHeader: "Bearer ", wantErr: true},
		{name: "nil request pointer", nilRequest: true, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var req *http.Request
			if !tc.nilRequest {
				var err error
				req, err = http.NewRequest(http.MethodGet, "/test", http.NoBody)
				if err != nil {
					t.Fatalf("failed to create request: %v", err)
				}
				if tc.authHeader != "" {
					req.Header.Set("Authorization", tc.authHeader)
				}
			}

			tok, err := ExtractBearerToken(req)
			if tc.wantErr && err == nil {
				t.Fatalf("expected error, got token %q", tok)
			}
			if !tc.wantErr && (err != nil || tok != tc.wantToken) {
				t.Fatalf("token = (%q, %v), want (%q, nil)", tok, err, tc.wantToken)
			}
		})
	}
}

func TestContextHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		identity  CallerIdentity
		attachCtx bool
		nilCtx    bool
		wantFound bool
	}{
		{
			name: "identity stored and retrieved",
			identity: CallerIdentity{
				Username:    "system:serviceaccount:default:eri-aerial",
				CallerAgent: "aerial",
				Groups:      []string{"system:serviceaccounts"},
				IsService:   false,
			},
			attachCtx: true,
			wantFound: true,
		},
		{name: "identity not attached to context", attachCtx: false, wantFound: false},
		{name: "nil context passed", nilCtx: true, wantFound: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var ctx context.Context
			if !tc.nilCtx {
				ctx = context.Background()
				if tc.attachCtx {
					ctx = WithCallerIdentity(ctx, tc.identity)
				}
			}

			id, ok := CallerFromContext(ctx)
			if ok != tc.wantFound {
				t.Fatalf("CallerFromContext ok = %v, want %v", ok, tc.wantFound)
			}
			if tc.wantFound && (id.Username != tc.identity.Username || id.CallerAgent != tc.identity.CallerAgent) {
				t.Fatalf("retrieved identity = %+v, want %+v", id, tc.identity)
			}
		})
	}
}

func TestNewAuthenticatorDefaults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		inCfg            Config
		wantTokenPath    string
		wantAgentSAPref  string
		wantAliasAerial  string
		wantConfigAgents []string
		wantCacheTTL     time.Duration
		wantMaxEntries   int
	}{
		{
			name:             "empty config gets all defaults",
			inCfg:            Config{},
			wantTokenPath:    DefaultServiceAccountTokenPath,
			wantAgentSAPref:  DefaultAgentSAPrefix,
			wantAliasAerial:  "aerial",
			wantConfigAgents: []string{"aerial", "miorine", "suletta"},
			wantCacheTTL:     45 * time.Second,
			wantMaxEntries:   1024,
		},
		{
			name: "custom config preserved",
			inCfg: Config{
				TokenPath:        "/custom/token",
				AgentSAPrefix:    "custom-prefix-",
				SAAgentAliases:   map[string]string{"custom-user": "custom-agent"},
				ConfiguredAgents: []string{"agent-x", "agent-y"},
				CacheTTL:         10 * time.Second,
				CacheMaxEntries:  500,
			},
			wantTokenPath:    "/custom/token",
			wantAgentSAPref:  "custom-prefix-",
			wantAliasAerial:  "",
			wantConfigAgents: []string{"agent-x", "agent-y"},
			wantCacheTTL:     10 * time.Second,
			wantMaxEntries:   500,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			auth := NewAuthenticator(tc.inCfg)
			cfg := auth.Config()

			if cfg.TokenPath != tc.wantTokenPath || cfg.AgentSAPrefix != tc.wantAgentSAPref {
				t.Fatalf("TokenPath=%q SAPrefix=%q, want (%q, %q)", cfg.TokenPath, cfg.AgentSAPrefix, tc.wantTokenPath, tc.wantAgentSAPref)
			}
			if tc.wantAliasAerial != "" && cfg.SAAgentAliases["system:serviceaccount:default:eri"] != tc.wantAliasAerial {
				t.Fatalf("alias for eri = %q, want %q", cfg.SAAgentAliases["system:serviceaccount:default:eri"], tc.wantAliasAerial)
			}
			if len(cfg.ConfiguredAgents) != len(tc.wantConfigAgents) || cfg.CacheTTL != tc.wantCacheTTL || cfg.CacheMaxEntries != tc.wantMaxEntries {
				t.Fatalf("mismatch in agents len=%d, ttl=%v, maxEntries=%d", len(cfg.ConfiguredAgents), cfg.CacheTTL, cfg.CacheMaxEntries)
			}
		})
	}
}

func TestNewAuthenticatorEnvAPIServerURL(t *testing.T) {
	t.Setenv("KUBERNETES_SERVICE_HOST", "10.96.0.1")
	t.Setenv("KUBERNETES_SERVICE_PORT", "443")

	auth := NewAuthenticator(Config{})
	if auth.Config().APIServerURL != "https://10.96.0.1:443" {
		t.Fatalf("APIServerURL = %q, want https://10.96.0.1:443", auth.Config().APIServerURL)
	}
}

type authTestCase struct {
	name             string
	bearerToken      string
	agentSAPrefix    string
	aliases          map[string]string
	hostTokenErr     bool
	badURL           bool
	wantCallerAgent  string
	wantIsService    bool
	wantUnauthErr    bool
	wantErrSubstring string
}

func execAuthenticateCase(t *testing.T, srv *httptest.Server, trCount *int32, tc authTestCase) {
	t.Helper()
	var hostTokenPath string
	if tc.hostTokenErr {
		hostTokenPath = "/non/existent/host/token"
	} else {
		hostTokenPath = writeTempToken(t, "host-pod-token")
	}

	serverURL := srv.URL
	if tc.badURL {
		serverURL = "http://127.0.0.1:0"
	}

	prefix := DefaultAgentSAPrefix
	if tc.agentSAPrefix != "" {
		prefix = tc.agentSAPrefix
	}

	aliases := map[string]string{"system:serviceaccount:default:eri": "aerial"}
	if tc.aliases != nil {
		aliases = tc.aliases
	}

	auth := NewAuthenticator(Config{
		Enabled:        true,
		APIServerURL:   serverURL,
		TokenPath:      hostTokenPath,
		AgentSAPrefix:  prefix,
		SAAgentAliases: aliases,
		HTTPClient:     srv.Client(),
	})
	caller, err := auth.Authenticate(context.Background(), tc.bearerToken)

	if tc.wantErrSubstring != "" {
		if err == nil || !strings.Contains(err.Error(), tc.wantErrSubstring) {
			t.Fatalf("expected error containing %q, got %v", tc.wantErrSubstring, err)
		}
		if tc.wantUnauthErr {
			if _, ok := errors.AsType[*UnauthorizedError](err); !ok {
				t.Fatalf("expected UnauthorizedError, got %T: %v", err, err)
			}
		}
		return
	}

	if err != nil || caller.CallerAgent != tc.wantCallerAgent || caller.IsService != tc.wantIsService {
		t.Fatalf("Authenticate = (%+v, %v), want agent=%q service=%v", caller, err, tc.wantCallerAgent, tc.wantIsService)
	}

	caller2, err2 := auth.Authenticate(context.Background(), tc.bearerToken)
	if err2 != nil || caller2.CallerAgent != tc.wantCallerAgent || atomic.LoadInt32(trCount) != 1 {
		t.Fatalf("cached Authenticate call failed or bypassed cache: requests=%d err=%v", atomic.LoadInt32(trCount), err2)
	}
}

func TestAuthenticate(t *testing.T) {
	t.Parallel()

	tests := []authTestCase{
		{name: "aerial agent token authenticated and mapped", bearerToken: "token-aerial", wantCallerAgent: "aerial"},
		{name: "eri legacy alias token mapped to aerial", bearerToken: "token-eri", wantCallerAgent: "aerial"},
		{name: "miorine agent token mapped from eri- prefix", bearerToken: "token-miorine", wantCallerAgent: "miorine"},
		{name: "suletta agent token mapped from eri- prefix", bearerToken: "token-suletta", wantCallerAgent: "suletta"},
		{name: "synapse service principal detected as service", bearerToken: "token-synapse", wantCallerAgent: "synapse", wantIsService: true},
		{name: "custom short sa prefix mapped", bearerToken: "token-clean-prefix", agentSAPrefix: "system:serviceaccount:default:prefix-", wantCallerAgent: "test"},
		{name: "short saName alias matched", bearerToken: "token-custom-sa", aliases: map[string]string{"custom-sa": "custom-agent"}, wantCallerAgent: "custom-agent"},
		{name: "clean prefix without colon matching saName", bearerToken: "token-suletta", agentSAPrefix: "eri-", wantCallerAgent: "suletta"},
		{name: "empty bearer token rejected with 401", bearerToken: "", wantUnauthErr: true, wantErrSubstring: "empty bearer token"},
		{name: "unauthenticated token rejected with 401", bearerToken: "token-unauthenticated", wantUnauthErr: true, wantErrSubstring: "token not authenticated"},
		{name: "host token read failure returns error", bearerToken: "token-aerial", hostTokenErr: true, wantErrSubstring: "read serviceaccount token"},
		{name: "api server 500 error propagated", bearerToken: "token-500", wantErrSubstring: "tokenreview returned status 500"},
		{name: "api server invalid json response handled", bearerToken: "token-bad-json", wantErrSubstring: "decode TokenReview response"},
		{name: "client transport failure returns error", bearerToken: "token-aerial", badURL: true, wantErrSubstring: "execute TokenReview"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var trCount int32
			srv := newMockK8sServer(t, &trCount, nil)
			defer srv.Close()
			execAuthenticateCase(t, srv, &trCount, tc)
		})
	}
}

type authorizeTestCase struct {
	name             string
	caller           CallerIdentity
	resource         string
	verb             string
	targetAgent      string
	hostTokenErr     bool
	badURL           bool
	wantErrSubstring string
	wantForbidden    bool
}

func execAuthorizeCase(t *testing.T, srv *httptest.Server, sarCount *int32, tc authorizeTestCase) {
	t.Helper()
	var hostTokenPath string
	if tc.hostTokenErr {
		hostTokenPath = "/non/existent/host/token"
	} else {
		hostTokenPath = writeTempToken(t, "host-pod-token")
	}

	serverURL := srv.URL
	if tc.badURL {
		serverURL = "http://127.0.0.1:0"
	}

	auth := NewAuthenticator(Config{
		Enabled:      true,
		APIServerURL: serverURL,
		TokenPath:    hostTokenPath,
		HTTPClient:   srv.Client(),
	})

	err := auth.Authorize(context.Background(), tc.caller, tc.resource, tc.verb, tc.targetAgent)
	if tc.wantErrSubstring != "" {
		if err == nil || !strings.Contains(err.Error(), tc.wantErrSubstring) {
			t.Fatalf("expected error containing %q, got %v", tc.wantErrSubstring, err)
		}
		if tc.wantForbidden {
			if _, ok := errors.AsType[*ForbiddenError](err); !ok {
				t.Fatalf("expected ForbiddenError, got %T: %v", err, err)
			}
		}
		return
	}

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err2 := auth.Authorize(context.Background(), tc.caller, tc.resource, tc.verb, tc.targetAgent)
	if err2 != nil || atomic.LoadInt32(sarCount) != 1 {
		t.Fatalf("cached Authorize call failed or bypassed cache: requests=%d err=%v", atomic.LoadInt32(sarCount), err2)
	}
}

func TestAuthorize(t *testing.T) {
	t.Parallel()

	tests := []authorizeTestCase{
		{
			name:        "read resource allowed for standard agent",
			caller:      CallerIdentity{Username: "system:serviceaccount:default:eri-miorine", CallerAgent: "miorine"},
			resource:    "builds",
			verb:        "get",
			targetAgent: "miorine",
		},
		{
			name:        "supervisor write on /all allowed",
			caller:      CallerIdentity{Username: "system:serviceaccount:default:eri-aerial", CallerAgent: "aerial"},
			resource:    "builds/all",
			verb:        "create",
			targetAgent: "miorine",
		},
		{
			name:             "standard agent write on /all denied with 403",
			caller:           CallerIdentity{Username: "system:serviceaccount:default:eri-suletta", CallerAgent: "suletta"},
			resource:         "builds/all",
			verb:             "create",
			targetAgent:      "miorine",
			wantErrSubstring: "subresource /all requires supervisor role",
			wantForbidden:    true,
		},
		{
			name:             "denied user explicitly rejected",
			caller:           CallerIdentity{Username: "user-denied"},
			resource:         "schedules",
			verb:             "list",
			targetAgent:      "aerial",
			wantErrSubstring: "permission denied by policy",
			wantForbidden:    true,
		},
		{
			name:             "denied user with empty reason uses fallback",
			caller:           CallerIdentity{Username: "user-denied-no-reason"},
			resource:         "schedules",
			verb:             "list",
			targetAgent:      "aerial",
			wantErrSubstring: "forbidden: user \"user-denied-no-reason\" is not authorized to list schedules",
			wantForbidden:    true,
		},
		{
			name:             "host token read failure returns error",
			caller:           CallerIdentity{Username: "system:serviceaccount:default:eri-aerial"},
			resource:         "builds",
			verb:             "get",
			hostTokenErr:     true,
			wantErrSubstring: "read serviceaccount token",
		},
		{
			name:             "api server 500 error handled",
			caller:           CallerIdentity{Username: "user-500"},
			resource:         "builds",
			verb:             "get",
			wantErrSubstring: "subjectaccessreview returned status 500",
		},
		{
			name:             "api server bad json handled",
			caller:           CallerIdentity{Username: "user-bad-json"},
			resource:         "builds",
			verb:             "get",
			wantErrSubstring: "decode SubjectAccessReview response",
		},
		{
			name:             "client transport failure returns error",
			caller:           CallerIdentity{Username: "system:serviceaccount:default:eri-aerial"},
			resource:         "builds",
			verb:             "get",
			badURL:           true,
			wantErrSubstring: "execute SubjectAccessReview",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var sarCount int32
			srv := newMockK8sServer(t, nil, &sarCount)
			defer srv.Close()
			execAuthorizeCase(t, srv, &sarCount, tc)
		})
	}
}

func TestResolveTargetAgent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		caller           CallerIdentity
		requestedTarget  string
		baseResource     string
		verb             string
		wantTarget       string
		wantErrSubstring string
	}{
		{name: "read verb empty target", caller: CallerIdentity{CallerAgent: "suletta"}, requestedTarget: "", baseResource: "builds", verb: "get", wantTarget: ""},
		{name: "read verb wildcard target", caller: CallerIdentity{CallerAgent: "suletta"}, requestedTarget: "*", baseResource: "builds", verb: "list", wantTarget: "*"},
		{name: "read verb configured target", caller: CallerIdentity{CallerAgent: "suletta"}, requestedTarget: "miorine", baseResource: "builds", verb: "get", wantTarget: "miorine"},
		{name: "read verb unknown target", caller: CallerIdentity{CallerAgent: "suletta"}, requestedTarget: "bogus", baseResource: "builds", verb: "get", wantErrSubstring: "unknown target agent"},
		{name: "write service empty target", caller: CallerIdentity{CallerAgent: "synapse", IsService: true}, requestedTarget: "", baseResource: "builds", verb: "create", wantErrSubstring: "missing or unknown target agent"},
		{name: "write service wildcard", caller: CallerIdentity{CallerAgent: "synapse", IsService: true}, requestedTarget: "*", baseResource: "builds", verb: "create", wantErrSubstring: "missing or unknown target agent"},
		{name: "write service unknown target", caller: CallerIdentity{CallerAgent: "synapse", IsService: true}, requestedTarget: "unknown", baseResource: "builds", verb: "create", wantErrSubstring: "missing or unknown target agent"},
		{name: "write service valid target", caller: CallerIdentity{CallerAgent: "synapse", IsService: true}, requestedTarget: "aerial", baseResource: "builds", verb: "create", wantTarget: "aerial"},
		{name: "write agent empty defaults self", caller: CallerIdentity{CallerAgent: "miorine"}, requestedTarget: "", baseResource: "memories", verb: "create", wantTarget: "miorine"},
		{name: "write agent configured target", caller: CallerIdentity{CallerAgent: "aerial"}, requestedTarget: "suletta", baseResource: "memories", verb: "update", wantTarget: "suletta"},
		{name: "write agent wildcard target", caller: CallerIdentity{CallerAgent: "aerial"}, requestedTarget: "*", baseResource: "prs", verb: "delete", wantTarget: "*"},
		{name: "write agent unknown target", caller: CallerIdentity{CallerAgent: "aerial"}, requestedTarget: "nonexistent", baseResource: "prs", verb: "delete", wantErrSubstring: "unknown target agent"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			auth := NewAuthenticator(Config{})
			target, err := auth.ResolveTargetAgent(tc.caller, tc.requestedTarget, tc.baseResource, tc.verb)

			if tc.wantErrSubstring != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrSubstring) {
					t.Fatalf("expected error containing %q, got target=%q err=%v", tc.wantErrSubstring, target, err)
				}
				if _, ok := errors.AsType[*ValidationError](err); !ok {
					t.Fatalf("expected ValidationError, got %T: %v", err, err)
				}
				return
			}

			if err != nil || target != tc.wantTarget {
				t.Fatalf("ResolveTargetAgent = (%q, %v), want (%q, nil)", target, err, tc.wantTarget)
			}
		})
	}
}

func TestAuthorizeTarget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		caller           CallerIdentity
		baseResource     string
		verb             string
		requestedTarget  string
		wantTarget       string
		wantErrSubstring string
		wantForbidden    bool
	}{
		{
			name:            "read operation passes base resource for standard agent",
			caller:          CallerIdentity{Username: "system:serviceaccount:default:eri-suletta", CallerAgent: "suletta"},
			baseResource:    "builds",
			verb:            "get",
			requestedTarget: "aerial",
			wantTarget:      "aerial",
		},
		{
			name:            "self write operation evaluates base resource and succeeds",
			caller:          CallerIdentity{Username: "system:serviceaccount:default:eri-suletta", CallerAgent: "suletta"},
			baseResource:    "builds",
			verb:            "create",
			requestedTarget: "",
			wantTarget:      "suletta",
		},
		{
			name:             "cross-agent write from standard agent escalates to /all and is rejected",
			caller:           CallerIdentity{Username: "system:serviceaccount:default:eri-suletta", CallerAgent: "suletta"},
			baseResource:     "builds",
			verb:             "create",
			requestedTarget:  "miorine",
			wantErrSubstring: "subresource /all requires supervisor role",
			wantForbidden:    true,
		},
		{
			name:            "cross-agent write from supervisor agent escalates to /all and succeeds",
			caller:          CallerIdentity{Username: "system:serviceaccount:default:eri-aerial", CallerAgent: "aerial"},
			baseResource:    "builds",
			verb:            "create",
			requestedTarget: "miorine",
			wantTarget:      "miorine",
		},
		{
			name:            "service principal write escalates to /all and succeeds with valid target",
			caller:          CallerIdentity{Username: "system:serviceaccount:default:synapse", CallerAgent: "synapse", IsService: true},
			baseResource:    "prs",
			verb:            "update",
			requestedTarget: "aerial",
			wantTarget:      "aerial",
		},
		{
			name:             "service principal write with missing target fails with validation error",
			caller:           CallerIdentity{Username: "system:serviceaccount:default:synapse", CallerAgent: "synapse", IsService: true},
			baseResource:     "prs",
			verb:             "update",
			requestedTarget:  "",
			wantErrSubstring: "missing or unknown target agent for service principal",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := newMockK8sServer(t, nil, nil)
			defer srv.Close()

			hostTokenPath := writeTempToken(t, "host-pod-token")
			auth := NewAuthenticator(Config{
				Enabled:      true,
				APIServerURL: srv.URL,
				TokenPath:    hostTokenPath,
				HTTPClient:   srv.Client(),
			})

			target, err := auth.AuthorizeTarget(context.Background(), tc.caller, tc.baseResource, tc.verb, tc.requestedTarget)
			if tc.wantErrSubstring != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrSubstring) {
					t.Fatalf("expected error containing %q, got target=%q err=%v", tc.wantErrSubstring, target, err)
				}
				if tc.wantForbidden {
					if _, ok := errors.AsType[*ForbiddenError](err); !ok {
						t.Fatalf("expected ForbiddenError, got %T: %v", err, err)
					}
				}
				return
			}

			if err != nil || target != tc.wantTarget {
				t.Fatalf("AuthorizeTarget = (%q, %v), want (%q, nil)", target, err, tc.wantTarget)
			}
		})
	}
}

func TestMiddleware(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		enabled        bool
		exemptPaths    []string
		requestPath    string
		authHeader     string
		wantStatusCode int
		wantAgent      string
	}{
		{name: "disabled middleware passes through unauthenticated", enabled: false, requestPath: "/builds", wantStatusCode: http.StatusOK},
		{name: "exempt path /health passes through unauthenticated", enabled: true, exemptPaths: []string{"/health", "/healthz", "/metrics"}, requestPath: "/health", wantStatusCode: http.StatusOK},
		{name: "exempt path with trailing slash in config matches request without slash", enabled: true, exemptPaths: []string{"/health/"}, requestPath: "/health", wantStatusCode: http.StatusOK},
		{name: "exempt path /metrics passes through unauthenticated", enabled: true, exemptPaths: []string{"/health", "/healthz", "/metrics"}, requestPath: "/metrics", wantStatusCode: http.StatusOK},
		{name: "missing bearer token rejected with 401", enabled: true, requestPath: "/builds", authHeader: "", wantStatusCode: http.StatusUnauthorized},
		{name: "unauthenticated token rejected with 401", enabled: true, requestPath: "/builds", authHeader: "Bearer token-unauthenticated", wantStatusCode: http.StatusUnauthorized},
		{name: "valid bearer token populates caller identity in request context", enabled: true, requestPath: "/builds", authHeader: "Bearer token-aerial", wantStatusCode: http.StatusOK, wantAgent: "aerial"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := newMockK8sServer(t, nil, nil)
			defer srv.Close()

			hostTokenPath := writeTempToken(t, "host-token")
			auth := NewAuthenticator(Config{
				Enabled:      tc.enabled,
				APIServerURL: srv.URL,
				TokenPath:    hostTokenPath,
				HTTPClient:   srv.Client(),
			})

			exempt := tc.exemptPaths
			if exempt == nil {
				exempt = []string{"/health", "/healthz", "/metrics"}
			}

			var capturedAgent string
			handler := auth.Middleware(exempt...)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if id, ok := CallerFromContext(r.Context()); ok {
					capturedAgent = id.CallerAgent
				}
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("handled"))
			}))

			req := httptest.NewRequest(http.MethodGet, tc.requestPath, http.NoBody)
			if tc.authHeader != "" {
				req.Header.Set("Authorization", tc.authHeader)
			}
			w := httptest.NewRecorder()

			handler.ServeHTTP(w, req)

			if w.Code != tc.wantStatusCode {
				t.Fatalf("status code = %d, want %d (body: %s)", w.Code, tc.wantStatusCode, w.Body.String())
			}
			if tc.wantAgent != "" && capturedAgent != tc.wantAgent {
				t.Fatalf("captured agent = %q, want %q", capturedAgent, tc.wantAgent)
			}
		})
	}
}

func TestErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		err     error
		wantErr string
	}{
		{name: "validation error custom message", err: NewValidationError("bad agent parameter"), wantErr: "bad agent parameter"},
		{name: "unauthorized error custom message", err: NewUnauthorizedError("missing token"), wantErr: "missing token"},
		{name: "forbidden error custom message", err: NewForbiddenError("insufficient permissions"), wantErr: "insufficient permissions"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.err.Error() != tc.wantErr {
				t.Fatalf("err.Error() = %q, want %q", tc.err.Error(), tc.wantErr)
			}
		})
	}
}
