package router

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/azylman/aerial/services/synapse/internal/metrics"
	"github.com/azylman/aerial/services/synapse/internal/restateclient"
)

// shadowRestateTimeout bounds synchronous dark sends so a wedged Restate
// ingress can never stall a webhook or watcher tick. Shadow mode is
// synchronous by design: no goroutines to leak, and the legacy path keeps
// serving even when Restate is down.
const shadowRestateTimeout = 2 * time.Second

// shadowRestateSnapshot is the read-only pr_registry projection captured on
// first touch of a pre-existing open PR. It is compared against the incoming
// event, never consulted to decide the legacy transition.
type shadowRestateSnapshot struct {
	headSHA  string
	mergeSHA string
	branch   string
	targetID string
	found    bool
}

// maxShadowHydratedKeys bounds the first-touch hydration cache so a
// long-lived synapse cannot grow it without limit. Eviction is FIFO: an
// evicted key simply re-hydrates (and may recount a lingering divergence)
// on its next touch.
const maxShadowHydratedKeys = 1024

// shadowRestateReporter mirrors github/deploy entrypoints at the Restate
// PRObject/RepoObject ingress in shadow mode. The legacy in-memory trackers
// stay authoritative; shadow results are only logged and counted. A nil or
// disabled reporter is a no-op, so every hook site stays a single call.
type shadowRestateReporter struct {
	client  *restateclient.Client
	enabled bool

	mu       sync.Mutex
	hydrated map[string]shadowRestateSnapshot
	order    []string

	sends       atomic.Uint64
	sendErrors  atomic.Uint64
	divergences atomic.Uint64
}

// newShadowRestateReporter builds a reporter from YAML config. An empty
// restate_url leaves the reporter disabled: every send is skipped without
// emitting a request. No environment variables are consulted.
func newShadowRestateReporter(cfg Config, httpClient *http.Client) *shadowRestateReporter {
	url := strings.TrimSpace(cfg.RestateURL)
	if httpClient == nil {
		httpClient = &http.Client{Timeout: shadowRestateTimeout}
	}
	return &shadowRestateReporter{
		client:   restateclient.NewClient(url, cfg.RestatePRService, cfg.RestateRepoService, httpClient),
		enabled:  url != "",
		hydrated: make(map[string]shadowRestateSnapshot),
	}
}

// Enabled reports whether shadow sends are gated on.
func (r *shadowRestateReporter) Enabled() bool {
	return r != nil && r.enabled
}

// Sends counts journaled shadow requests that returned 2xx.
func (r *shadowRestateReporter) Sends() uint64 {
	if r == nil {
		return 0
	}
	return r.sends.Load()
}

// SendErrors counts shadow requests that failed closed (logged only).
func (r *shadowRestateReporter) SendErrors() uint64 {
	if r == nil {
		return 0
	}
	return r.sendErrors.Load()
}

// Divergences counts first-touch hydration mismatches against pr_registry.
// The cutover gate requires this to stay zero.
func (r *shadowRestateReporter) Divergences() uint64 {
	if r == nil {
		return 0
	}
	return r.divergences.Load()
}

// restateShadow returns the server's reporter, lazily built from the current
// YAML snapshot so tests can inject a fake via SetRestateShadow and
// production picks up restate_url without touching RunServer wiring.
func (s *RouterServer) restateShadow() *shadowRestateReporter {
	if s == nil {
		return newShadowRestateReporter(Config{}, nil)
	}
	// A disabled lazy reporter is rebuilt on every call so a config reload
	// that sets restate_url takes effect without touching RunServer wiring.
	s.shadowMu.Lock()
	defer s.shadowMu.Unlock()
	if s.shadowRestate != nil && s.shadowRestate.Enabled() {
		return s.shadowRestate
	}
	current := newShadowRestateReporter(s.currentConfig(), nil)
	if current.Enabled() {
		s.shadowRestate = current
		return current
	}
	if s.shadowRestate != nil {
		return s.shadowRestate
	}
	return current
}

// currentConfig returns the live config snapshot, tolerating a nil server.
func (s *RouterServer) currentConfig() Config {
	if s == nil || s.cfgSrc == nil {
		return Config{}
	}
	if cfg := s.cfgSrc.Current(); cfg != nil {
		return *cfg
	}
	return Config{}
}

// shadowPREvent hydrates the PR key from the pr_registry projection on first
// touch, records a divergence when the event disagrees with the projection,
// then mirrors the event at the frozen PRObject handler. Legacy semantics are
// untouched: failures are logged and counted, never returned.
func (s *RouterServer) shadowPREvent(ctx context.Context, handler string, ev restateclient.PREvent) {
	r := s.restateShadow()
	if !r.Enabled() || ev.PRNumber <= 0 {
		return
	}
	s.hydrateShadowPR(ctx, r, ev)
	if err := r.client.SendPREvent(ctx, handler, ev); err != nil {
		r.sendErrors.Add(1)
		metrics.RecordRestateShadowSend(handler, "error")
		slog.WarnContext(ctx, "[synapse] [restate-shadow] pr send failed closed",
			"handler", handler, "repo", ev.Repo, "pr", ev.PRNumber, "error", err)
		return
	}
	r.sends.Add(1)
	metrics.RecordRestateShadowSend(handler, "sent")
}

// shadowRepoEvent mirrors repo-level events (main-push index, fan-out) at the
// frozen RepoObject handler. RepoObject holds only the 50-SHA index and
// tombstones, so there is nothing to hydrate against.
func (s *RouterServer) shadowRepoEvent(ctx context.Context, handler string, ev restateclient.RepoEvent) {
	r := s.restateShadow()
	if !r.Enabled() || strings.TrimSpace(ev.Repo) == "" {
		return
	}
	if err := r.client.SendRepoEvent(ctx, handler, ev); err != nil {
		r.sendErrors.Add(1)
		metrics.RecordRestateShadowSend(handler, "error")
		slog.WarnContext(ctx, "[synapse] [restate-shadow] repo send failed closed",
			"handler", handler, "repo", ev.Repo, "error", err)
		return
	}
	r.sends.Add(1)
	metrics.RecordRestateShadowSend(handler, "sent")
}

// hydrateShadowPR captures the pr_registry projection on first touch of a PR
// key and emits a divergence when the live event disagrees. In-flight PRs
// started under legacy code hydrate here instead of replaying history.
func (s *RouterServer) hydrateShadowPR(ctx context.Context, r *shadowRestateReporter, ev restateclient.PREvent) {
	key := restateclient.PRObjectKey(ev.Repo, ev.PRNumber)
	r.mu.Lock()
	if _, ok := r.hydrated[key]; ok {
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()
	// The projection read runs without holding r.mu so one slow registry
	// cannot stall shadow sends for every other PR.
	snap := s.loadShadowSnapshot(ctx, ev.Repo, ev.PRNumber)
	r.mu.Lock()
	_, ok := r.hydrated[key]
	if !ok {
		r.hydrated[key] = snap
		r.order = append(r.order, key)
		for len(r.order) > maxShadowHydratedKeys {
			oldest := r.order[0]
			r.order = r.order[1:]
			delete(r.hydrated, oldest)
		}
	}
	r.mu.Unlock()
	// Divergence is a first-touch property: the loser of a concurrent double
	// hydration sees the winner's entry and skips, and later sends for the
	// same key are compared by Restate itself (handlers drop stale events),
	// so a lingering legacy-side gap counts once instead of on every retry.
	if ok || !snap.found {
		return
	}
	if mismatchSHA(snap.headSHA, ev.HeadSHA) {
		r.divergences.Add(1)
		metrics.RecordRestateShadowDivergence("head_sha_mismatch")
		slog.WarnContext(ctx, "[synapse] [restate-shadow] head SHA diverged from projection",
			"repo", ev.Repo, "pr", ev.PRNumber, "event_head_sha", ev.HeadSHA)
	}
	if ev.MergeSHA != "" && mismatchSHA(snap.mergeSHA, ev.MergeSHA) {
		r.divergences.Add(1)
		metrics.RecordRestateShadowDivergence("merge_sha_mismatch")
		slog.WarnContext(ctx, "[synapse] [restate-shadow] merge SHA diverged from projection",
			"repo", ev.Repo, "pr", ev.PRNumber, "event_merge_sha", ev.MergeSHA)
	}
}

// loadShadowSnapshot reads the read-only projection for one PR key. The
// projection is never consulted to decide a transition; routing only.
func (s *RouterServer) loadShadowSnapshot(ctx context.Context, repo string, prNumber int) shadowRestateSnapshot {
	if s == nil || s.registry == nil {
		return shadowRestateSnapshot{}
	}
	targetID, branch, headSHA, mergeSHA, err := s.registry.ResolvePRByNumber(ctx, repo, prNumber)
	if err != nil {
		return shadowRestateSnapshot{}
	}
	return shadowRestateSnapshot{headSHA: headSHA, mergeSHA: mergeSHA, branch: branch, targetID: targetID, found: true}
}

func mismatchSHA(projected, live string) bool {
	projected = strings.TrimSpace(projected)
	live = strings.TrimSpace(live)
	return projected != "" && live != "" && !strings.EqualFold(projected, live)
}

// shadowDeployStartedSHAs best-effort resolves SHAs for the deferred
// deploy_started event. The read is gated on shadow mode so the disabled
// path keeps its prior zero-extra-read cost; a miss leaves SHAs empty.
func (s *RouterServer) shadowDeployStartedSHAs(ctx context.Context, repo string, prNumber int) (headSHA, mergeSHA string) {
	if r := s.restateShadow(); !r.Enabled() {
		return "", ""
	}
	if s == nil || s.registry == nil || prNumber <= 0 {
		return "", ""
	}
	_, _, headSHA, mergeSHA, err := s.registry.ResolvePRByNumber(ctx, repo, prNumber)
	if err != nil {
		return "", ""
	}
	return headSHA, mergeSHA
}

// shadowPreSyncConflict snapshots whether the projection still shows a PR in
// conflict before the legacy synchronize transition clears it. The read is
// gated on the shadow reporter: when shadow mode is off it returns false
// without touching the registry.
func (s *RouterServer) shadowPreSyncConflict(ctx context.Context, repo string, prNumber int) bool {
	if r := s.restateShadow(); !r.Enabled() || s == nil || s.registry == nil || prNumber <= 0 {
		return false
	}
	lifecycle, err := s.registry.LoadPRBuildLifecycle(ctx, repo, prNumber)
	if err != nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(lifecycle.Status), prConflict)
}

// shadowConflictResolved mirrors an observed conflict-to-sync recovery in
// shadow mode. The legacy synchronize path already cleared the conflict;
// this only reports the observation.
func (s *RouterServer) shadowConflictResolved(ctx context.Context, res *ResolvedGitHubEvent) {
	if res == nil {
		return
	}
	s.shadowPREvent(ctx, "HandleConflictResolved", restateclient.PREvent{
		Repo: res.Repo, PRNumber: res.PRNumber, HeadSHA: res.HeadSHA, Branch: res.Branch,
	})
}

// shadowDeployDispatched mirrors one deploy_success dispatch in shadow mode.
// The dual legacy dedup keys (PR-keyed and job-plus-SHA) are untouched; the
// shadow send carries the job name so the Restate effect key stays scoped.
// Head and merge SHAs ride in their own fields: the deployed commit is the
// merge SHA, and comparing it against the projected head SHA would record a
// false divergence on every normal deploy where head != merge.
func (s *RouterServer) shadowDeployDispatched(ctx context.Context, repo, jobName, headSHA, mergeSHA string, prNumber int) {
	if prNumber <= 0 || (strings.TrimSpace(headSHA) == "" && strings.TrimSpace(mergeSHA) == "") {
		return
	}
	s.shadowPREvent(ctx, "HandleDeployDispatched", restateclient.PREvent{
		Repo: repo, PRNumber: prNumber, HeadSHA: headSHA, MergeSHA: mergeSHA, JobName: jobName,
	})
}

// shadowRolloutProgress mirrors kustomization progress observations in shadow
// mode. Deploy failures have no frozen PRObject handler, so only progress
// and dispatch observations are mirrored; failures stay legacy-only.
func (s *RouterServer) shadowRolloutProgress(ctx context.Context, repo string, prNumber int, headSHA, mergeSHA, jobName string) {
	if prNumber <= 0 {
		return
	}
	s.shadowPREvent(ctx, "HandleRolloutProgress", restateclient.PREvent{
		Repo: repo, PRNumber: prNumber, HeadSHA: headSHA, MergeSHA: mergeSHA, JobName: jobName,
	})
}

// checkOutcomeHandler maps a check/workflow conclusion to its frozen PRObject
// handler. Empty means no shadow send: pending, cancelled, and neutral
// outcomes carry no PRObject transition.
func checkOutcomeHandler(res *ResolvedGitHubEvent) string {
	if res == nil {
		return ""
	}
	switch strings.ToLower(strings.TrimSpace(res.Conclusion)) {
	case conclusionSuccess:
		return "HandleCheckPassed"
	case conclusionFailure, conclusionTimedOut:
		return "HandleCheckFailed"
	default:
		return ""
	}
}

// shadowCheckOutcome mirrors one check_run or workflow_run conclusion in
// shadow mode. Unmanaged collaborator runs stay inside PRObject via the same
// path; the client never resolves a build ID and never signals BuildRouter.
func (s *RouterServer) shadowCheckOutcome(ctx context.Context, res *ResolvedGitHubEvent) {
	handler := checkOutcomeHandler(res)
	if handler == "" || res.PRNumber <= 0 || strings.TrimSpace(res.HeadSHA) == "" {
		return
	}
	s.shadowPREvent(ctx, handler, restateclient.PREvent{
		Repo:      res.Repo,
		PRNumber:  res.PRNumber,
		HeadSHA:   res.HeadSHA,
		MergeSHA:  res.MergeSHA,
		CheckName: res.CheckName,
		Branch:    res.Branch,
	})
}
