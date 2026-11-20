package router

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/azylman/aerial/services/synapse/internal/restateclient"
)

// countingResolveRegistry wraps a registry and counts ResolvePRByNumber calls
// to prove gated paths add no reads when shadow mode is off.
type countingResolveRegistry struct {
	PRRegistryUpdater

	calls atomic.Int64
}

// hydratedLen reports the number of cached hydration keys. It lives in the
// test file because production code never reads the cache size; the deadcode
// gate flags test-only helpers on production types.
func hydratedLen(r *shadowRestateReporter) int {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.hydrated)
}

func (c *countingResolveRegistry) ResolvePRByNumber(ctx context.Context, repo string, prNumber int) (string, string, string, string, error) {
	c.calls.Add(1)
	return c.PRRegistryUpdater.ResolvePRByNumber(ctx, repo, prNumber)
}

// shadowCapture is a hermetic fake Restate ingress: it records every handler
// invocation without any real Restate dependency.
type shadowCapture struct {
	mu     sync.Mutex
	paths  []string
	bodies []json.RawMessage
}

func newShadowCapture() (*shadowCapture, *httptest.Server) {
	rec := &shadowCapture{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		rec.mu.Lock()
		rec.paths = append(rec.paths, r.URL.EscapedPath())
		rec.bodies = append(rec.bodies, append(json.RawMessage(nil), raw...))
		rec.mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	}))
	return rec, srv
}

func (c *shadowCapture) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.paths)
}

func (c *shadowCapture) firstBody(t *testing.T) restateclient.PREvent {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.bodies) == 0 {
		t.Fatal("no shadow request captured")
	}
	var ev restateclient.PREvent
	if err := json.Unmarshal(c.bodies[0], &ev); err != nil {
		t.Fatalf("captured body is not a PREvent: %v", err)
	}
	return ev
}

func shadowTestHeadSHA() string { return strings.Repeat("a", 40) }

func shadowTestMergeSHA() string { return strings.Repeat("b", 40) }

func TestShadowReporterEnabled(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		restateURL string
		want       bool
	}{
		{name: "empty url disables shadow", restateURL: "", want: false},
		{name: "blank url disables shadow", restateURL: "   ", want: false},
		{name: "loopback url enables shadow", restateURL: "http://127.0.0.1:8080", want: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := newShadowRestateReporter(Config{RestateURL: tc.restateURL}, nil)
			if r.Enabled() != tc.want {
				t.Errorf("Enabled() = %v, want %v", r.Enabled(), tc.want)
			}
		})
	}
}

func TestCheckOutcomeHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		conclusion string
		action     string
		nilRes     bool
		want       string
	}{
		{name: "nil event maps to no handler", nilRes: true, want: ""},
		{name: "completed success maps to check passed", conclusion: conclusionSuccess, action: ghStatusCompleted, want: "HandleCheckPassed"},
		{name: "completed failure maps to check failed", conclusion: conclusionFailure, action: ghStatusCompleted, want: "HandleCheckFailed"},
		{name: "timed out maps to check failed", conclusion: conclusionTimedOut, action: ghStatusCompleted, want: "HandleCheckFailed"},
		{name: "in progress maps to no handler", conclusion: "", action: "in_progress", want: ""},
		{name: "cancelled maps to no handler", conclusion: "cancelled", action: ghStatusCompleted, want: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var res *ResolvedGitHubEvent
			if !tc.nilRes {
				res = &ResolvedGitHubEvent{Conclusion: tc.conclusion, Action: tc.action}
			}
			if got := checkOutcomeHandler(res); got != tc.want {
				t.Errorf("checkOutcomeHandler() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestShadowPREventSends(t *testing.T) {
	t.Parallel()

	headSHA := shadowTestHeadSHA()

	tests := []struct {
		name         string
		restateURL   string
		resolveSHA   string
		resolveMerge string
		evMerge      string
		wantCalls    int
		wantDiv      uint64
	}{
		{name: "disabled shadow sends nothing", restateURL: "", resolveSHA: headSHA, wantCalls: 0, wantDiv: 0},
		{name: "matching registry hydrates clean and sends", restateURL: "enabled", resolveSHA: headSHA, wantCalls: 1, wantDiv: 0},
		{name: "mismatched registry records divergence and still sends", restateURL: "enabled", resolveSHA: shadowTestMergeSHA(), wantCalls: 1, wantDiv: 1},
		{name: "mismatched merge records divergence and still sends", restateURL: "enabled", resolveSHA: headSHA, resolveMerge: shadowTestHeadSHA(), evMerge: shadowTestMergeSHA(), wantCalls: 1, wantDiv: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			shot, srv := newShadowCapture()
			defer srv.Close()
			url := tc.restateURL
			if url == "enabled" {
				url = srv.URL
			}
			srv2 := newTestRouter(Config{RestateURL: url}, srv.Client(), nil, nil)
			reg := &mockPRRegistry{resolveHeadSHA: tc.resolveSHA, resolveMergeSHA: tc.resolveMerge, resolveTargetID: lifecycleTargetID}
			srv2.SetRegistry(reg)
			reporter := srv2.restateShadow()
			reporter.client = restateclient.NewClient(url, "PRObject", "RepoObject", srv.Client())
			if url == "" {
				reporter.client = restateclient.NewClient("", "PRObject", "RepoObject", srv.Client())
			}

			srv2.shadowPREvent(context.Background(), "HandleCheckPassed", restateclient.PREvent{
				Repo: "aerial", PRNumber: 7, HeadSHA: headSHA, MergeSHA: tc.evMerge,
			})

			if got := shot.count(); got != tc.wantCalls {
				t.Errorf("captured shadow calls = %d, want %d", got, tc.wantCalls)
			}
			if got := reporter.Divergences(); got != tc.wantDiv {
				t.Errorf("divergences = %d, want %d", got, tc.wantDiv)
			}
			if tc.wantCalls > 0 {
				ev := shot.firstBody(t)
				if ev.PRNumber != 7 || ev.HeadSHA != headSHA || ev.Repo != "azylman/aerial" {
					t.Errorf("captured event = %+v, want repo azylman/aerial pr 7 head %s", ev, headSHA)
				}
			}
		})
	}
}

func TestClosedActionShadowMirror(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		merged      bool
		wantHandler string
	}{
		{name: "merged close mirrors HandleMerged", merged: true, wantHandler: "HandleMerged"},
		{name: "unmerged close mirrors HandleClosed", merged: false, wantHandler: "HandleClosed"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			shot, srv := newShadowCapture()
			defer srv.Close()
			bus := &mockEventBus{}
			reg := &mockPRRegistry{
				resolveTargetID: lifecycleTargetID, resolveHeadSHA: shadowTestHeadSHA(),
				mergedTargetID: lifecycleTargetID, closedTargetID: lifecycleTargetID,
			}
			r := newTestRouter(Config{RestateURL: srv.URL}, srv.Client(), reg, &mockOutboundDispatcher{}, bus)
			r.SetRestateShadow(newShadowRestateReporter(Config{RestateURL: srv.URL}, srv.Client()))
			res := &ResolvedGitHubEvent{
				Repo: "aerial", PRNumber: 7, Branch: "feat/x",
				HeadSHA: shadowTestHeadSHA(), MergeSHA: shadowTestMergeSHA(),
			}
			if err := r.handlePRClosedAction(context.Background(), res, tc.merged); err != nil {
				t.Fatalf("handlePRClosedAction() err = %v", err)
			}
			if got := shot.count(); got != 1 {
				t.Fatalf("captured shadow calls = %d, want 1", got)
			}
			shot.mu.Lock()
			path := shot.paths[0]
			shot.mu.Unlock()
			if !strings.HasSuffix(path, "/"+tc.wantHandler+"/send") {
				t.Errorf("shadow path = %q, want suffix %q", path, tc.wantHandler)
			}
		})
	}
}

func TestShadowReporterNilSafe(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		report  *shadowRestateReporter
		wantNum uint64
		wantEn  bool
	}{
		{name: "nil reporter counts zero and stays disabled", report: nil, wantNum: 0, wantEn: false},
		{name: "empty reporter counts zero and stays disabled", report: &shadowRestateReporter{}, wantNum: 0, wantEn: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.report.Sends(); got != tc.wantNum {
				t.Errorf("Sends() = %d, want %d", got, tc.wantNum)
			}
			if got := tc.report.SendErrors(); got != tc.wantNum {
				t.Errorf("SendErrors() = %d, want %d", got, tc.wantNum)
			}
			if got := tc.report.Divergences(); got != tc.wantNum {
				t.Errorf("Divergences() = %d, want %d", got, tc.wantNum)
			}
			if got := tc.report.Enabled(); got != tc.wantEn {
				t.Errorf("Enabled() = %v, want %v", got, tc.wantEn)
			}
		})
	}
}

// nilCurrentSource is a config source whose snapshot is missing, exercising
// the nil-current branch of the shadow reporter lookup.
type nilCurrentSource struct{}

func (*nilCurrentSource) Current() *Config { return nil }

func TestRestateShadowServerFallback(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		srv  *RouterServer
		want bool
	}{
		{name: "nil server reports disabled", srv: nil, want: false},
		{name: "server without config reports disabled", srv: &RouterServer{}, want: false},
		{name: "injected disabled reporter stays disabled", srv: &RouterServer{shadowRestate: newShadowRestateReporter(Config{}, nil)}, want: false},
		{name: "nil config snapshot reports disabled", srv: NewRouterServer(&nilCurrentSource{}, nil), want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.srv.restateShadow().Enabled(); got != tc.want {
				t.Errorf("restateShadow().Enabled() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestShadowPREventOutcomes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		statusCode int
		wantSends  uint64
		wantErrors uint64
	}{
		{name: "accepted ingress counts a send", statusCode: http.StatusAccepted, wantSends: 1, wantErrors: 0},
		{name: "failing ingress fails closed and counts an error", statusCode: http.StatusInternalServerError, wantSends: 0, wantErrors: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.statusCode)
			}))
			defer srv.Close()
			headSHA := shadowTestHeadSHA()
			r := newTestRouter(Config{RestateURL: srv.URL}, srv.Client(), &mockPRRegistry{resolveHeadSHA: headSHA}, nil)
			r.SetRestateShadow(newShadowRestateReporter(Config{RestateURL: srv.URL}, srv.Client()))
			r.shadowPREvent(context.Background(), "HandleCheckPassed", restateclient.PREvent{
				Repo: "aerial", PRNumber: 7, HeadSHA: headSHA,
			})
			rep := r.restateShadow()
			if got := rep.Sends(); got != tc.wantSends {
				t.Errorf("Sends() = %d, want %d", got, tc.wantSends)
			}
			if got := rep.SendErrors(); got != tc.wantErrors {
				t.Errorf("SendErrors() = %d, want %d", got, tc.wantErrors)
			}
		})
	}
}

func TestShadowHydrationCachesFirstTouch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		firstSHA  string
		secondSHA string
		wantDiv   uint64
	}{
		{name: "matching projection twice records no divergence", firstSHA: shadowTestHeadSHA(), secondSHA: shadowTestHeadSHA(), wantDiv: 0},
		{name: "first mismatch sticks even after projection catches up", firstSHA: shadowTestMergeSHA(), secondSHA: shadowTestHeadSHA(), wantDiv: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			shot, srv := newShadowCapture()
			defer srv.Close()
			headSHA := shadowTestHeadSHA()
			reg := &mockPRRegistry{resolveHeadSHA: tc.firstSHA}
			r := newTestRouter(Config{RestateURL: srv.URL}, srv.Client(), reg, nil)
			r.SetRestateShadow(newShadowRestateReporter(Config{RestateURL: srv.URL}, srv.Client()))
			ev := restateclient.PREvent{Repo: "aerial", PRNumber: 7, HeadSHA: headSHA}
			r.shadowPREvent(context.Background(), "HandleCheckPassed", ev)
			reg.resolveHeadSHA = tc.secondSHA
			r.shadowPREvent(context.Background(), "HandleCheckPassed", ev)
			if got := r.restateShadow().Divergences(); got != tc.wantDiv {
				t.Errorf("Divergences() = %d, want %d", got, tc.wantDiv)
			}
			if got := shot.count(); got != 2 {
				t.Errorf("captured shadow calls = %d, want 2", got)
			}
		})
	}
}

func TestShadowSnapshotWithoutRegistry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		resolveErr error
		wantCalls  int
	}{
		{name: "nil registry skips hydration and still sends", resolveErr: nil, wantCalls: 1},
		{name: "registry error skips hydration and still sends", resolveErr: errors.New("db down"), wantCalls: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			shot, srv := newShadowCapture()
			defer srv.Close()
			r := newTestRouter(Config{RestateURL: srv.URL}, srv.Client(), nil, nil)
			if tc.resolveErr != nil {
				r.SetRegistry(&mockPRRegistry{resolveErr: tc.resolveErr})
			}
			r.SetRestateShadow(newShadowRestateReporter(Config{RestateURL: srv.URL}, srv.Client()))
			r.shadowPREvent(context.Background(), "HandleCheckPassed", restateclient.PREvent{
				Repo: "aerial", PRNumber: 7, HeadSHA: shadowTestHeadSHA(),
			})
			if got := shot.count(); got != tc.wantCalls {
				t.Errorf("captured shadow calls = %d, want %d", got, tc.wantCalls)
			}
			if got := r.restateShadow().Divergences(); got != 0 {
				t.Errorf("Divergences() = %d, want 0", got)
			}
		})
	}
}

func TestShadowPreSyncConflict(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		enabled      bool
		status       string
		statusErr    bool
		wantConflict bool
	}{
		{name: "conflict status reports conflict", enabled: true, status: prConflict, wantConflict: true},
		{name: "open status reports no conflict", enabled: true, status: prOpen, wantConflict: false},
		{name: "registry error reports no conflict", enabled: true, status: prConflict, statusErr: true, wantConflict: false},
		{name: "disabled shadow never reports conflict", enabled: false, status: prConflict, wantConflict: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			url := ""
			if tc.enabled {
				url = "http://127.0.0.1:8080"
			}
			reg := &mockPRRegistry{buildLifecycle: PRBuildLifecycle{Status: tc.status}}
			if tc.statusErr {
				reg.buildLifecycleErr = errors.New("db down")
			}
			r := newTestRouter(Config{RestateURL: url}, nil, reg, nil)
			if got := r.shadowPreSyncConflict(context.Background(), "aerial", 7); got != tc.wantConflict {
				t.Errorf("shadowPreSyncConflict() = %v, want %v", got, tc.wantConflict)
			}
		})
	}
}

func TestShadowConflictResolved(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		nilRes    bool
		wantCalls int
	}{
		{name: "resolved conflict mirrors HandleConflictResolved", nilRes: false, wantCalls: 1},
		{name: "nil event sends nothing", nilRes: true, wantCalls: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			shot, srv := newShadowCapture()
			defer srv.Close()
			r := newTestRouter(Config{RestateURL: srv.URL}, srv.Client(), &mockPRRegistry{resolveHeadSHA: shadowTestHeadSHA()}, nil)
			r.SetRestateShadow(newShadowRestateReporter(Config{RestateURL: srv.URL}, srv.Client()))
			var res *ResolvedGitHubEvent
			if !tc.nilRes {
				res = &ResolvedGitHubEvent{Repo: "aerial", PRNumber: 7, HeadSHA: shadowTestHeadSHA(), Branch: "feat/x"}
			}
			r.shadowConflictResolved(context.Background(), res)
			if got := shot.count(); got != tc.wantCalls {
				t.Errorf("captured shadow calls = %d, want %d", got, tc.wantCalls)
			}
			if tc.wantCalls > 0 {
				shot.mu.Lock()
				path := shot.paths[0]
				shot.mu.Unlock()
				if !strings.HasSuffix(path, "/HandleConflictResolved/send") {
					t.Errorf("shadow path = %q, want HandleConflictResolved suffix", path)
				}
			}
		})
	}
}

func TestShadowDeployDispatchedGuards(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		prNumber  int
		headSHA   string
		mergeSHA  string
		wantCalls int
		wantDiv   uint64
	}{
		{name: "valid dispatch mirrors HandleDeployDispatched", prNumber: 7, headSHA: shadowTestHeadSHA(), mergeSHA: shadowTestMergeSHA(), wantCalls: 1, wantDiv: 0},
		{name: "head-only dispatch sends without divergence", prNumber: 7, headSHA: shadowTestHeadSHA(), wantCalls: 1, wantDiv: 0},
		{name: "merge-only dispatch sends without divergence", prNumber: 7, mergeSHA: shadowTestMergeSHA(), wantCalls: 1, wantDiv: 0},
		{name: "zero pr sends nothing", prNumber: 0, headSHA: shadowTestHeadSHA(), wantCalls: 0, wantDiv: 0},
		{name: "blank shas send nothing", prNumber: 7, headSHA: "  ", mergeSHA: " ", wantCalls: 0, wantDiv: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			shot, srv := newShadowCapture()
			defer srv.Close()
			r := newTestRouter(Config{RestateURL: srv.URL}, srv.Client(), &mockPRRegistry{resolveHeadSHA: shadowTestHeadSHA(), resolveMergeSHA: shadowTestMergeSHA()}, nil)
			r.SetRestateShadow(newShadowRestateReporter(Config{RestateURL: srv.URL}, srv.Client()))
			r.shadowDeployDispatched(context.Background(), "aerial", "api", tc.headSHA, tc.mergeSHA, tc.prNumber)
			if got := shot.count(); got != tc.wantCalls {
				t.Errorf("captured shadow calls = %d, want %d", got, tc.wantCalls)
			}
			if got := r.restateShadow().Divergences(); got != tc.wantDiv {
				t.Errorf("Divergences() = %d, want %d", got, tc.wantDiv)
			}
		})
	}
}

func TestShadowRolloutProgressGuards(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		prNumber  int
		wantCalls int
	}{
		{name: "valid observation mirrors HandleRolloutProgress", prNumber: 7, wantCalls: 1},
		{name: "zero pr sends nothing", prNumber: 0, wantCalls: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			shot, srv := newShadowCapture()
			defer srv.Close()
			r := newTestRouter(Config{RestateURL: srv.URL}, srv.Client(), &mockPRRegistry{resolveHeadSHA: shadowTestHeadSHA()}, nil)
			r.SetRestateShadow(newShadowRestateReporter(Config{RestateURL: srv.URL}, srv.Client()))
			r.shadowRolloutProgress(context.Background(), "aerial", tc.prNumber, shadowTestHeadSHA(), shadowTestMergeSHA(), "api")
			if got := shot.count(); got != tc.wantCalls {
				t.Errorf("captured shadow calls = %d, want %d", got, tc.wantCalls)
			}
		})
	}
}

func TestRestateShadowConcurrentInit(t *testing.T) {
	// Hammers lazy reporter initialization from concurrent handlers. Under
	// -race this fails on the unsynchronized s.shadowRestate read/write.
	r := newTestRouter(Config{RestateURL: "http://127.0.0.1:1"}, nil, &mockPRRegistry{resolveHeadSHA: shadowTestHeadSHA()}, nil)
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for range 25 {
				if n%2 == 0 {
					r.SetRestateShadow(newShadowRestateReporter(Config{}, nil))
				} else {
					_ = r.restateShadow()
				}
			}
		}(i)
	}
	wg.Wait()
}

func TestShadowHydrationBounded(t *testing.T) {
	_, srv := newShadowCapture()
	defer srv.Close()
	r := newTestRouter(Config{RestateURL: srv.URL}, srv.Client(), &mockPRRegistry{resolveHeadSHA: shadowTestHeadSHA()}, nil)
	r.SetRestateShadow(newShadowRestateReporter(Config{RestateURL: srv.URL}, srv.Client()))
	for pr := 1; pr <= maxShadowHydratedKeys+50; pr++ {
		r.shadowPREvent(context.Background(), "HandleCheckPassed", restateclient.PREvent{
			Repo: "aerial", PRNumber: pr, HeadSHA: shadowTestHeadSHA(),
		})
	}
	if got := hydratedLen(r.restateShadow()); got > maxShadowHydratedKeys {
		t.Errorf("hydrated keys = %d, want at most %d", got, maxShadowHydratedKeys)
	}
}

func TestShadowDeployStartedSHAsGated(t *testing.T) {
	t.Parallel()

	headSHA := shadowTestHeadSHA()

	tests := []struct {
		name       string
		restateURL string
		prNumber   int
		resolveErr bool
		wantHead   string
		wantCalls  int64
	}{
		{name: "disabled shadow resolves nothing and reads nothing", restateURL: "", prNumber: 7, wantHead: "", wantCalls: 0},
		{name: "enabled shadow resolves SHAs from the projection", restateURL: "http://127.0.0.1:8080", prNumber: 7, wantHead: headSHA, wantCalls: 1},
		{name: "enabled shadow with zero pr reads nothing", restateURL: "http://127.0.0.1:8080", prNumber: 0, wantHead: "", wantCalls: 0},
		{name: "enabled shadow with registry error reads once and returns empty", restateURL: "http://127.0.0.1:8080", prNumber: 7, resolveErr: true, wantHead: "", wantCalls: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			inner := &mockPRRegistry{resolveHeadSHA: headSHA}
			if tc.resolveErr {
				inner.resolveErr = errors.New("db down")
			}
			reg := &countingResolveRegistry{PRRegistryUpdater: inner}
			r := newTestRouter(Config{RestateURL: tc.restateURL}, nil, reg, nil)
			gotHead, gotMerge := r.shadowDeployStartedSHAs(context.Background(), "aerial", tc.prNumber)
			if gotHead != tc.wantHead || gotMerge != "" {
				t.Errorf("shadowDeployStartedSHAs() = (%q, %q), want (%q, %q)", gotHead, gotMerge, tc.wantHead, "")
			}
			if got := reg.calls.Load(); got != tc.wantCalls {
				t.Errorf("registry reads = %d, want %d", got, tc.wantCalls)
			}
		})
	}
}

func TestShadowRepoEventSends(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		restateURL string
		fail       bool
		wantCalls  int
		wantErrors uint64
	}{
		{name: "disabled shadow sends nothing", restateURL: "", wantCalls: 0, wantErrors: 0},
		{name: "enabled shadow sends main push", restateURL: "enabled", wantCalls: 1, wantErrors: 0},
		{name: "failing ingress fails closed and counts an error", restateURL: "enabled", fail: true, wantCalls: 0, wantErrors: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			shot, srv := newShadowCapture()
			defer srv.Close()
			client := srv.Client()
			if tc.fail {
				srv.Close()
				srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusInternalServerError)
				}))
				defer srv.Close()
				shot = &shadowCapture{}
			}
			url := tc.restateURL
			if url == "enabled" {
				url = srv.URL
			}
			srv2 := newTestRouter(Config{RestateURL: url}, client, nil, nil)
			reporter := srv2.restateShadow()
			reporter.client = restateclient.NewClient(url, "PRObject", "RepoObject", client)
			if url == "" {
				reporter.client = restateclient.NewClient("", "PRObject", "RepoObject", client)
			}

			srv2.shadowRepoEvent(context.Background(), "HandleMainPush", restateclient.RepoEvent{
				Repo: "aerial", MainSHA: shadowTestHeadSHA(),
			})

			if got := shot.count(); got != tc.wantCalls {
				t.Errorf("captured shadow calls = %d, want %d", got, tc.wantCalls)
			}
			if got := reporter.SendErrors(); got != tc.wantErrors {
				t.Errorf("SendErrors() = %d, want %d", got, tc.wantErrors)
			}
		})
	}
}
