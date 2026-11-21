package router

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/azylman/aerial/pkg/githubauth"
	"github.com/azylman/aerial/services/synapse/internal/metrics"
)

// authRecorder serves GitHub REST JSON and records each Authorization header.
type authRecorder struct {
	mu   sync.Mutex
	auth []string
}

func (a *authRecorder) server(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		a.mu.Lock()
		a.auth = append(a.auth, r.Header.Get("Authorization"))
		a.mu.Unlock()
		w.Header().Set("X-Ratelimit-Remaining", "4321")
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func (a *authRecorder) headers() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.auth...)
}

func TestRouterGitHubTransportCredential(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		pat        string
		src        githubauth.TokenSource
		wantGitHub string
	}{
		{name: "no token source attaches config PAT", pat: "pat-secret", wantGitHub: "Bearer pat-secret"},
		{name: "no token source and no PAT is unauthenticated", wantGitHub: ""},
		{name: "injected source wins over config PAT", pat: "pat-secret", src: githubauth.NewStaticTokenSource("injected"), wantGitHub: "Bearer injected"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gh := &authRecorder{}
			ghSrv := gh.server(t)
			other := &authRecorder{}
			otherSrv := other.server(t)
			srv := NewRouterServer(fixedConfig(Config{GitHubAPIURL: ghSrv.URL, GitHubToken: tc.pat}), nil, WithGitHubTokenSource(tc.src))
			var out map[string]any
			if err := srv.getGitHubJSON(context.Background(), "/repos/o/r", 1<<20, &out); err != nil {
				t.Fatalf("getGitHubJSON: %v", err)
			}
			req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, otherSrv.URL+"/blob", http.NoBody)
			if err != nil {
				t.Fatalf("NewRequest: %v", err)
			}
			resp, err := srv.githubClient.Do(req)
			if err != nil {
				t.Fatalf("non-GitHub host request: %v", err)
			}
			_ = resp.Body.Close()
			if got := gh.headers(); len(got) != 1 || got[0] != tc.wantGitHub {
				t.Errorf("GitHub Authorization = %q, want [%q]", got, tc.wantGitHub)
			}
			if got := other.headers(); len(got) != 1 || got[0] != "" {
				t.Errorf("non-GitHub Authorization = %q, want [\"\"]", got)
			}
		})
	}
}

func testAppKeyPEM(t *testing.T) string {
	t.Helper()
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(k)}))
}

// newAppMintServer answers installation 42 token mints with app-token.
func newAppMintServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/app/installations/42/access_tokens" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"token":"app-token","expires_at":"2099-01-01T00:00:00Z"}`))
	}))
	t.Cleanup(srv.Close)
	return srv
}

// TestNewGitHubTokenSource mutates the process-wide credential gauge, so it
// runs serially.
func TestNewGitHubTokenSource(t *testing.T) {
	keyPEM := testAppKeyPEM(t)
	mint := newAppMintServer(t)
	appEnv := gitHubAppEnv{AppID: "7", InstallationID: "42", PrivateKeyPEM: keyPEM}
	tests := []struct {
		name       string
		credential string
		env        gitHubAppEnv
		wantValue  string
		wantSource string
		wantAppG   float64
	}{
		{name: "app with App env mints app token", credential: "app", env: appEnv, wantValue: "app-token", wantSource: "app", wantAppG: 1},
		{name: "pat with App env uses PAT", credential: "pat", env: appEnv, wantValue: "pat-secret", wantSource: "pat", wantAppG: 0},
		{name: "app without App env stays on PAT", credential: "app", wantValue: "pat-secret", wantSource: "pat", wantAppG: 1},
		{name: "partial App env stays on PAT", credential: "app", env: gitHubAppEnv{AppID: "7"}, wantValue: "pat-secret", wantSource: "pat", wantAppG: 1},
		{name: "app with unreachable mint falls back to PAT", credential: "app", env: gitHubAppEnv{AppID: "7", InstallationID: "99", PrivateKeyPEM: keyPEM}, wantValue: "pat-secret", wantSource: "pat", wantAppG: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfgSrc := fixedConfig(Config{GitHubAPIURL: mint.URL, GitHubToken: "pat-secret", GitHubCredential: tc.credential})
			src := newGitHubTokenSource(cfgSrc, tc.env)
			value, source, err := src.Token(context.Background())
			if err != nil {
				t.Fatalf("Token: %v", err)
			}
			if value != tc.wantValue || source != tc.wantSource {
				t.Errorf("Token = (%q, %q), want (%q, %q)", value, source, tc.wantValue, tc.wantSource)
			}
			if got := testutil.ToFloat64(metrics.GitHubCredentialConfigured.WithLabelValues("app")); got != tc.wantAppG {
				t.Errorf("github_credential_configured{credential=app} = %v, want %v", got, tc.wantAppG)
			}
		})
	}
}

func TestConfigPATSource(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		cfgSrc ConfigSource
		want   string
	}{
		{name: "nil config source", want: ""},
		{name: "nil snapshot", cfgSrc: &nilConfigSource{}, want: ""},
		{name: "trimmed PAT", cfgSrc: fixedConfig(Config{GitHubToken: " pat-x "}), want: "pat-x"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			src := configPATSource{cfgSrc: tc.cfgSrc}
			src.Invalidate("ignored")
			value, source, err := src.Token(context.Background())
			if err != nil || value != tc.want || source != githubauth.SourcePAT {
				t.Errorf("Token = (%q, %q, %v), want (%q, pat, nil)", value, source, err, tc.want)
			}
		})
	}
}

func TestGitHubAPIHost(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		cfgSrc ConfigSource
		want   string
	}{
		{name: "nil config source", want: "api.github.com"},
		{name: "nil snapshot", cfgSrc: &nilConfigSource{}, want: "api.github.com"},
		{name: "empty URL", cfgSrc: fixedConfig(Config{}), want: "api.github.com"},
		{name: "enterprise URL host", cfgSrc: fixedConfig(Config{GitHubAPIURL: "https://ghe.example.com/api/v3"}), want: "ghe.example.com"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := githubAPIHost(tc.cfgSrc); got != tc.want {
				t.Errorf("githubAPIHost = %q, want %q", got, tc.want)
			}
		})
	}
}

// nilConfigSource returns a nil snapshot.
type nilConfigSource struct{}

func (*nilConfigSource) Current() *Config { return nil }
