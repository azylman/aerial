package router

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/azylman/aerial/pkg/githubauth"
	"github.com/azylman/aerial/pkg/prwire"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/synapse/internal/config"
	"github.com/azylman/aerial/services/synapse/internal/metrics"
)

// --- 3. GitHub Webhook HTTP Handler & Event Processing ---

const testGitHubWebhookSecret = "test-secret"

func signTestGitHubWebhook(secret, payload string) string {
	mac := hmac.New(sha256.New, []byte(strings.TrimSpace(secret)))
	mac.Write([]byte(payload))
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

type ghWebhookTestCase struct {
	name, path, wantMetricSubstr string
	body                         io.Reader
	headers                      map[string]string
	bus                          *mockEventBus
	hasBus                       bool
	webhookSecret                string
	noWebhookSecret              bool
	wantStatus, wantInserted     int
}

func assertGitHubWebhookResponse(t *testing.T, tc ghWebhookTestCase, rec *httptest.ResponseRecorder, routes http.Handler) {
	t.Helper()
	if rec.Code != tc.wantStatus {
		t.Fatalf("expected status %d, got %d", tc.wantStatus, rec.Code)
	}
	if tc.wantInserted > 0 && tc.bus != nil {
		if len(tc.bus.insertedJobs) != tc.wantInserted {
			t.Fatalf("expected %d inserted jobs, got %d", tc.wantInserted, len(tc.bus.insertedJobs))
		}
		var resp map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to parse json response: %v", err)
		}
		if resp["status"] != "accepted" || resp["event"] != tc.headers["X-GitHub-Event"] || resp["delivery"] != tc.headers["X-GitHub-Delivery"] {
			t.Errorf("unexpected response body: %+v", resp)
		}
		if tc.bus.insertedJobs[0].Event != tc.headers["X-GitHub-Event"] || tc.bus.insertedJobs[0].Delivery != tc.headers["X-GitHub-Delivery"] {
			t.Errorf("unexpected inserted jobs: %+v", tc.bus.insertedJobs)
		}
	}
	if tc.wantMetricSubstr != "" {
		recM := doHTTPRequest(routes, http.MethodGet, "/metrics", nil, nil)
		if !strings.Contains(recM.Body.String(), tc.wantMetricSubstr) {
			t.Errorf("expected /metrics to include %q, got:\n%s", tc.wantMetricSubstr, recM.Body.String())
		}
	}
}

func runGitHubWebhookCase(t *testing.T, tc ghWebhookTestCase) {
	t.Helper()
	t.Parallel()
	secret := tc.webhookSecret
	if secret == "" && !tc.noWebhookSecret {
		secret = testGitHubWebhookSecret
	}
	srv := NewRouterServer(fixedConfig(Config{GitHubWebhookSecret: secret}), nil)
	if tc.bus != nil {
		srv.SetEventBus(tc.bus)
	}
	busCh := make(chan string, 1)
	if tc.hasBus {
		memBus := pubsub.NewMemoryBus()
		t.Cleanup(func() { _ = memBus.Close() })
		srv.SetEventBus(memBus)
		srv.onProcessed = func(_, delivery string) {
			select {
			case busCh <- delivery:
			default:
			}
		}
		if err := srv.StartJobConsumers(context.Background()); err != nil {
			t.Fatalf("StartJobConsumers() error: %v", err)
		}
	}
	routes := srv.Routes()
	rec := doHTTPRequest(routes, http.MethodPost, tc.path, tc.body, tc.headers)
	assertGitHubWebhookResponse(t, tc, rec, routes)
	if tc.hasBus && tc.wantStatus == http.StatusOK {
		select {
		case gotDel := <-busCh:
			if gotDel != tc.headers["X-GitHub-Delivery"] {
				t.Errorf("bus consumer delivery = %q, want %q", gotDel, tc.headers["X-GitHub-Delivery"])
			}
		case <-time.After(2 * time.Second):
			t.Errorf("timed out waiting for synapse-github bus consumer")
		}
	}
}

func TestHandleGitHubWebhook(t *testing.T) {
	t.Parallel()

	pingBody := `{"zen":"Keep it simple"}`
	validPingSig := signTestGitHubWebhook(testGitHubWebhookSecret, pingBody)

	tests := []ghWebhookTestCase{
		{name: "missing X-GitHub-Event header", path: "/api/webhooks/github", body: strings.NewReader(`{}`), wantStatus: http.StatusBadRequest},
		{name: "read error in body", path: "/api/webhooks/github", body: errReader{}, headers: map[string]string{"X-GitHub-Event": "ping"}, wantStatus: http.StatusBadRequest},
		{name: "bus nil", path: "/api/webhooks/github", body: strings.NewReader(pingBody), headers: map[string]string{"X-GitHub-Event": "ping", "X-Hub-Signature-256": validPingSig}, wantStatus: http.StatusInternalServerError},
		{name: "bus publish error", path: "/api/webhooks/github", body: strings.NewReader(pingBody), headers: map[string]string{"X-GitHub-Event": "ping", "X-Hub-Signature-256": validPingSig}, bus: &mockEventBus{err: errors.New("simulated database failure")}, wantStatus: http.StatusInternalServerError},
		{name: "api path success", path: "/api/webhooks/github", body: strings.NewReader(pingBody), headers: map[string]string{"X-GitHub-Event": "ping", "X-GitHub-Delivery": "del-1234", "X-Hub-Signature-256": validPingSig}, bus: &mockEventBus{}, wantStatus: http.StatusOK, wantInserted: 1},
		{name: "bus only success", path: "/api/webhooks/github", body: strings.NewReader(pingBody), headers: map[string]string{"X-GitHub-Event": "ping", "X-GitHub-Delivery": "del-bus-1", "X-Hub-Signature-256": validPingSig}, hasBus: true, wantStatus: http.StatusOK},
		{name: "legacy path success", path: "/webhooks/github", body: strings.NewReader(pingBody), headers: map[string]string{"X-GitHub-Event": "ping", "X-GitHub-Delivery": "del-1234", "X-Hub-Signature-256": validPingSig}, bus: &mockEventBus{}, wantStatus: http.StatusOK, wantInserted: 1},
		{name: "empty secret fails closed without signature", path: "/api/webhooks/github", body: strings.NewReader(pingBody), headers: map[string]string{"X-GitHub-Event": "ping"}, noWebhookSecret: true, wantStatus: http.StatusUnauthorized},
		{name: "empty secret fails closed even with signature header", path: "/api/webhooks/github", body: strings.NewReader(pingBody), headers: map[string]string{"X-GitHub-Event": "ping", "X-Hub-Signature-256": validPingSig}, noWebhookSecret: true, wantStatus: http.StatusUnauthorized},
		{name: "secret configured missing signature header", path: "/api/webhooks/github", body: strings.NewReader(pingBody), headers: map[string]string{"X-GitHub-Event": "ping"}, webhookSecret: testGitHubWebhookSecret, wantStatus: http.StatusUnauthorized},
		{name: "secret configured malformed signature prefix", path: "/api/webhooks/github", body: strings.NewReader(pingBody), headers: map[string]string{"X-GitHub-Event": "ping", "X-Hub-Signature-256": "sha1=1234"}, webhookSecret: testGitHubWebhookSecret, wantStatus: http.StatusUnauthorized},
		{name: "secret configured malformed signature hex", path: "/api/webhooks/github", body: strings.NewReader(pingBody), headers: map[string]string{"X-GitHub-Event": "ping", "X-Hub-Signature-256": "sha256=invalidhex!"}, webhookSecret: testGitHubWebhookSecret, wantStatus: http.StatusUnauthorized},
		{name: "secret configured mismatched hmac digest", path: "/api/webhooks/github", body: strings.NewReader(pingBody), headers: map[string]string{"X-GitHub-Event": "ping", "X-Hub-Signature-256": "sha256=0000000000000000000000000000000000000000000000000000000000000000"}, webhookSecret: testGitHubWebhookSecret, wantStatus: http.StatusUnauthorized},
		{name: "secret configured valid hmac digest", path: "/api/webhooks/github", body: strings.NewReader(pingBody), headers: map[string]string{"X-GitHub-Event": "ping", "X-GitHub-Delivery": "del-sig-1", "X-Hub-Signature-256": validPingSig}, bus: &mockEventBus{}, webhookSecret: testGitHubWebhookSecret, wantStatus: http.StatusOK, wantInserted: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runGitHubWebhookCase(t, tc)
		})
	}
}

func TestIsForeignBotEvent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		nilCfg      bool
		cfgBots     map[string]BotConfig
		cfgBotID    string
		meta        AerialPRMetadata
		wantForeign bool
	}{
		{
			name:        "nil cfgSrc returns false",
			nilCfg:      true,
			meta:        AerialPRMetadata{Agent: "aerial"},
			wantForeign: false,
		},
		{
			name:        "empty agent is not foreign",
			cfgBots:     map[string]BotConfig{"aerial": {}},
			meta:        AerialPRMetadata{},
			wantForeign: false,
		},
		{
			name:        "configured bot is not foreign",
			cfgBots:     map[string]BotConfig{"aerial": {}, "agent-alpha": {}},
			meta:        AerialPRMetadata{Agent: "agent-alpha"},
			wantForeign: false,
		},
		{
			name:        "case-insensitive configured bot is not foreign",
			cfgBots:     map[string]BotConfig{"aerial": {}, "agent-alpha": {}},
			meta:        AerialPRMetadata{Agent: "AGENT-ALPHA"},
			wantForeign: false,
		},
		{
			name:        "unconfigured bot is foreign",
			cfgBots:     map[string]BotConfig{"aerial": {}},
			meta:        AerialPRMetadata{Agent: "agent-beta"},
			wantForeign: true,
		},
		{
			name:        "legacy single bot config matching is not foreign",
			cfgBotID:    "aerial",
			meta:        AerialPRMetadata{BotID: "aerial"},
			wantForeign: false,
		},
		{
			name:        "legacy single bot config foreign",
			cfgBotID:    "aerial",
			meta:        AerialPRMetadata{BotID: "agent-beta"},
			wantForeign: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var srv *RouterServer
			if tc.nilCfg {
				srv = &RouterServer{}
			} else {
				srv = newTestRouter(Config{Bots: tc.cfgBots, BotID: tc.cfgBotID}, nil, nil)
			}
			got := srv.isForeignBotEvent(tc.meta)
			if got != tc.wantForeign {
				t.Errorf("isForeignBotEvent() = %v, want %v", got, tc.wantForeign)
			}
		})
	}
}

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
		{name: "non-integer App ID falls back to PAT", credential: "app", env: gitHubAppEnv{AppID: "not-a-number", InstallationID: "42", PrivateKeyPEM: keyPEM}, wantValue: "pat-secret", wantSource: "pat", wantAppG: 1},
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

// githubBodyServer answers every GitHub API request with code and body and
// counts the requests.
func githubBodyServer(t *testing.T, code int, body string, calls *atomic.Int64) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestResolveAutoMergeDisabledPR(t *testing.T) {
	t.Parallel()
	known := true
	tests := []struct {
		name       string
		res        ResolvedGitHubEvent
		pr         GitHubPullRequest
		ghCode     int
		ghBody     string
		wantOK     bool
		wantErr    bool
		wantState  string
		wantBranch string
		wantCalls  int64
	}{
		{name: "known mergeability passes through without a fetch", res: ResolvedGitHubEvent{Repo: "example/aerial", PRNumber: 7}, pr: GitHubPullRequest{Number: 7, Mergeable: &known, MergeableState: "clean"}, wantOK: true, wantState: "clean"},
		{name: "pr-less event passes through without a fetch", res: ResolvedGitHubEvent{Repo: "example/aerial"}, pr: GitHubPullRequest{}, wantOK: true},
		{
			name: "unknown mergeability is fetched and filled from the event", res: ResolvedGitHubEvent{Repo: "example/aerial", PRNumber: 7, Branch: "feat", HeadSHA: "h1"}, pr: GitHubPullRequest{Number: 7},
			ghCode: http.StatusOK, ghBody: `{"number":7,"state":"open","mergeable_state":"dirty","head":{"sha":"h1"}}`, wantOK: true, wantState: "dirty", wantBranch: "feat", wantCalls: 1,
		},
		{name: "merged pr is skipped", res: ResolvedGitHubEvent{Repo: "example/aerial", PRNumber: 7}, ghCode: http.StatusOK, ghBody: `{"number":7,"merged":true,"mergeable_state":"clean"}`, wantCalls: 1},
		{name: "closed pr is skipped", res: ResolvedGitHubEvent{Repo: "example/aerial", PRNumber: 7}, ghCode: http.StatusOK, ghBody: `{"number":7,"state":"closed","mergeable_state":"clean"}`, wantCalls: 1},
		{name: "moved head is skipped", res: ResolvedGitHubEvent{Repo: "example/aerial", PRNumber: 7, HeadSHA: "h1"}, ghCode: http.StatusOK, ghBody: `{"number":7,"state":"open","mergeable_state":"dirty","head":{"sha":"h2"}}`, wantCalls: 1},
		{name: "fetch failure surfaces", res: ResolvedGitHubEvent{Repo: "example/aerial", PRNumber: 7}, ghCode: http.StatusInternalServerError, ghBody: `{}`, wantErr: true, wantCalls: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int64
			gh := githubBodyServer(t, tc.ghCode, tc.ghBody, &calls)
			srv := newTestRouter(Config{GitHubAPIURL: gh.URL}, gh.Client(), nil)
			res := tc.res
			got, ok, err := srv.resolveAutoMergeDisabledPR(context.Background(), &res, tc.pr)
			if (err != nil) != tc.wantErr || ok != tc.wantOK {
				t.Fatalf("resolveAutoMergeDisabledPR() = ok %v err %v, want ok %v wantErr %v", ok, err, tc.wantOK, tc.wantErr)
			}
			if ok && (got.MergeableState != tc.wantState || got.Head.Ref != tc.wantBranch) {
				t.Errorf("pr = state %q branch %q, want %q %q", got.MergeableState, got.Head.Ref, tc.wantState, tc.wantBranch)
			}
			if got := calls.Load(); got != tc.wantCalls {
				t.Errorf("github calls = %d, want %d", got, tc.wantCalls)
			}
		})
	}
}

func TestFetchLatestRoboRevComment(t *testing.T) {
	t.Parallel()
	const head = "abcdef1234567890"
	tests := []struct {
		name     string
		prNumber int
		ghCode   int
		ghBody   string
		want     string
	}{
		{name: "latest matching roborev comment wins", prNumber: 7, ghCode: http.StatusOK, ghBody: `[{"body":"## roborev: old ` + "`abcdef1" + `"},{"body":"unrelated"},{"body":"## roborev: newest ` + "`abcdef1" + `"}]`, want: "## roborev: newest `abcdef1"},
		{name: "override comment counts", prNumber: 7, ghCode: http.StatusOK, ghBody: `[{"body":"## roborev-override for ` + head + `"}]`, want: "## roborev-override for " + head},
		{name: "comment for another head is ignored", prNumber: 7, ghCode: http.StatusOK, ghBody: `[{"body":"## roborev: ` + "`9999999" + `"}]`},
		{name: "fetch failure yields nothing", prNumber: 7, ghCode: http.StatusInternalServerError, ghBody: `{}`},
		{name: "pr-less lookup yields nothing", prNumber: 0, ghCode: http.StatusOK, ghBody: `[]`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int64
			gh := githubBodyServer(t, tc.ghCode, tc.ghBody, &calls)
			srv := newTestRouter(Config{GitHubAPIURL: gh.URL}, gh.Client(), nil)
			if got := srv.fetchLatestRoboRevComment(context.Background(), "example/aerial", tc.prNumber, head); strings.TrimSpace(got) != tc.want {
				t.Errorf("fetchLatestRoboRevComment() = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestLogBuildMetadataWarning swaps the process-wide slog default, so it
// runs before the parallel tests instead of alongside them.
func TestLogBuildMetadataWarning(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
		if a.Key == slog.TimeKey {
			return slog.Attr{}
		}
		return a
	}})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	logBuildMetadataWarning(context.Background(), "refreshing build metadata", "example/aerial", 42, errors.New("boom"))

	const want = `level=WARN msg="[synapse] [build_metadata] refreshing build metadata" repo=example/aerial pr=42 error=boom` + "\n"
	if !strings.Contains(buf.String(), want) {
		t.Errorf("log = %q, want line %q", buf.String(), want)
	}
}

func TestParseAerialPRMetadata(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, body, wantTarget, wantBotID string
		wantOK                            bool
	}{
		{name: "no trailer", body: "Standard PR body\n<!-- regular comment -->"},
		{name: "unclosed trailer", body: "Body\n<!-- aerial-pr: {\"target_id\":\"100000000000000002\"}"},
		{name: "invalid json", body: "Body\n<!-- aerial-pr: {bad-json} -->"},
		{name: "invalid target and empty", body: "Body\n<!-- aerial-pr: {\"target_id\":\"bad\"} -->"},
		{name: "valid target", body: "<!-- x -->\n<!-- aerial-pr: {\"target_id\":\"100000000000000002\"} -->", wantTarget: "100000000000000002", wantOK: true},
		{name: "discord prefixed target and pool prefix", body: "<!-- aerial-pr: {\"target_id\":\"stateful_medium:discord-100000000000000002\"} -->", wantTarget: "100000000000000002", wantOK: true},
		{name: "valid bot id only", body: "<!-- aerial-pr: {\"bot_id\":\"eri-sub\"} -->", wantBotID: "eri-sub", wantOK: true},
		{name: "valid target and bot id", body: "<!-- aerial-pr: {\"target_id\":\"100000000000000002\",\"bot_id\":\"eri-sub\"} -->", wantTarget: "100000000000000002", wantBotID: "eri-sub", wantOK: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ParseAerialPRMetadata(tc.body)
			if got.OK() != tc.wantOK || got.TargetID != tc.wantTarget || got.BotID != tc.wantBotID {
				t.Fatalf("ParseAerialPRMetadata() = %+v (ok=%v), want target=%q bot=%q ok=%v", got, got.OK(), tc.wantTarget, tc.wantBotID, tc.wantOK)
			}
		})
	}
}

func TestAutoUpdatePolicy(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		cfg  Config
		repo string
		want *prwire.AutoUpdatePolicy
	}{
		{
			name: "authors, default owner, and the repo's collaborators sorted by author",
			cfg: Config{AutoUpdatePRAuthors: []string{"bot"}, DefaultOwner: true, CollaboratorCIAlerts: []config.CollaboratorCIAlertRule{
				{Repo: "azylman/aerial", ChannelID: "111111111111111111", Authors: map[string]string{"zed": "<@2>", "amy": "<@1>", "blank": " "}},
				{Repo: "azylman/mirrormere", ChannelID: "333333333333333333", Authors: map[string]string{"other": "<@3>"}},
			}},
			repo: "azylman/aerial",
			want: &prwire.AutoUpdatePolicy{Authors: []string{"bot"}, DefaultOwner: true, Collaborators: []prwire.CollaboratorAlert{
				{Author: "amy", ChannelID: "111111111111111111", Mention: "<@1>"},
				{Author: "zed", ChannelID: "111111111111111111", Mention: "<@2>"},
			}},
		},
		{name: "empty config renders an empty policy", repo: "azylman/aerial", want: &prwire.AutoUpdatePolicy{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := NewRouterServer(fixedConfig(tc.cfg), nil)
			if got := srv.autoUpdatePolicy(tc.repo); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("autoUpdatePolicy() = %+v, want %+v", got, tc.want)
			}
		})
	}
}
