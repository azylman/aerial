package router

// Cross-module parity goldens, legacy side. Every scenario under
// testdata/prlifecycle/parity/ is one PR lifecycle: the inputs synapse
// receives (webhooks, alerts, watcher observations, register calls), the
// effect multiset the legacy pipeline performs (by kind and
// generation-free match key) and the hand-derived final pr_registry rows.
// This test drives the legacy router over those inputs with a stateful fake
// registry that applies each registry call's SQL transition in memory, a
// fake GitHub API, a fake dispatcher and a recording event bus, and asserts
// it lands on exactly the golden effects and rows. The prstate module's
// parity_test.go replays the matching Restate event stream through
// PRObject/RepoObject with effects_owner=restate over the same files, so
// both owners are proven to produce the same outcome.
//
// Effects are collected the way the shadow gates compare them: every
// published aerial.pr.> fact keyed by restateclient.FactMatchKey, and every
// other legacy effect from its [synapse-effect] line. The one exception is
// image promotion: the step drives promoteWorkflowRunImages, whose caller
// logs the line, so the promotion is read from the dispatcher instead.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/azylman/aerial/services/synapse/internal/restateclient"
)

// parityFixtureDir holds one golden lifecycle per file.
const parityFixtureDir = "../../../../testdata/prlifecycle/parity/"

// parityEffect is one effect by kind and generation-free match key.
type parityEffect struct {
	Kind string `json:"kind"`
	Key  string `json:"key"`
}

// parityRow is the pr_registry row a lifecycle ends on.
type parityRow struct {
	Repo     string         `json:"repo"`
	PRNumber int            `json:"pr_number"`
	Branch   string         `json:"branch"`
	HeadSHA  string         `json:"head_sha"`
	MergeSHA string         `json:"merge_sha"`
	TargetID string         `json:"target_id"`
	Status   string         `json:"status"`
	Title    string         `json:"title"`
	Metadata map[string]any `json:"metadata"`
}

// parityRollout records reconcile requests for an image rollout.
type parityRollout struct {
	Repo     string                          `json:"repo"`
	PRNumber int                             `json:"pr_number"`
	HeadSHA  string                          `json:"head_sha"`
	Branch   string                          `json:"branch"`
	TargetID string                          `json:"target_id"`
	Jobs     []string                        `json:"jobs"`
	Requests map[string]kustomizationRequest `json:"requests"`
}

// parityObservation is one Ready Kustomization the watcher observes.
type parityObservation struct {
	Namespace              string `json:"namespace"`
	Name                   string `json:"name"`
	RequestedAt            string `json:"requested_at"`
	LastHandledReconcileAt string `json:"last_handled_reconcile_at"`
	LastAppliedRevision    string `json:"last_applied_revision"`
}

// parityPromotion is one image promotion for a commit.
type parityPromotion struct {
	Repo      string   `json:"repo"`
	PRNumber  int      `json:"pr_number"`
	CommitSHA string   `json:"commit_sha"`
	TargetID  string   `json:"target_id"`
	Images    []string `json:"images"`
}

// parityFailure is one final failed deploy attempt.
type parityFailure struct {
	Repo     string `json:"repo"`
	PRNumber int    `json:"pr_number"`
	HeadSHA  string `json:"head_sha"`
	Branch   string `json:"branch"`
	TargetID string `json:"target_id"`
	Error    string `json:"error"`
}

// paritySynapseStep is one legacy input; Kind selects the populated field.
type paritySynapseStep struct {
	Kind        string             `json:"kind"`
	Register    json.RawMessage    `json:"register"`
	Event       string             `json:"event"`
	Payload     json.RawMessage    `json:"payload"`
	Deploy      *DeployEvent       `json:"deploy"`
	Observation *parityObservation `json:"observation"`
	Rollout     *parityRollout     `json:"rollout"`
	Promotion   *parityPromotion   `json:"promotion"`
	Failure     *parityFailure     `json:"failure"`
}

// parityScenario is one golden file; Restate-only fields are ignored here.
type parityScenario struct {
	Name         string              `json:"name"`
	GitHubRoutes map[string]string   `json:"github_routes"`
	Synapse      []paritySynapseStep `json:"synapse"`
	Effects      []parityEffect      `json:"effects"`
	Rows         []parityRow         `json:"rows"`
}

func loadParityScenarios(t *testing.T) []parityScenario {
	t.Helper()
	paths, err := filepath.Glob(parityFixtureDir + "*.json")
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 12 {
		t.Fatalf("parity goldens = %d, want 12", len(paths))
	}
	sort.Strings(paths)
	out := make([]parityScenario, 0, len(paths))
	for _, p := range paths {
		raw, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		var sc parityScenario
		if err := json.Unmarshal(raw, &sc); err != nil {
			t.Fatalf("%s: %v", p, err)
		}
		out = append(out, sc)
	}
	return out
}

// parityEffectSink collects the [synapse-effect] lines logged under one ctx.
type parityEffectSink struct {
	mu      sync.Mutex
	effects []parityEffect
}

type parityEffectSinkKey struct{}

func (s *parityEffectSink) add(e parityEffect) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.effects = append(s.effects, e)
}

func (s *parityEffectSink) snapshot() []parityEffect {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]parityEffect(nil), s.effects...)
}

// effectCaptureHandler forwards every record and, for a ctx carrying a
// parityEffectSink, also captures [synapse-effect] lines into it. TestMain
// installs it as the slog default, so parallel tests never share a sink.
type effectCaptureHandler struct {
	next slog.Handler
}

func (h effectCaptureHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return h.next.Enabled(ctx, l) || ctx.Value(parityEffectSinkKey{}) != nil
}

func (h effectCaptureHandler) Handle(ctx context.Context, r slog.Record) error {
	if sink, ok := ctx.Value(parityEffectSinkKey{}).(*parityEffectSink); ok && strings.Contains(r.Message, "legacy effect performed") {
		var e parityEffect
		r.Attrs(func(a slog.Attr) bool {
			switch a.Key {
			case "kind":
				e.Kind = a.Value.String()
			case "effect_key":
				e.Key = a.Value.String()
			}
			return true
		})
		sink.add(e)
	}
	if !h.next.Enabled(ctx, r.Level) {
		return nil
	}
	return h.next.Handle(ctx, r)
}

func (h effectCaptureHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return effectCaptureHandler{next: h.next.WithAttrs(attrs)}
}

func (h effectCaptureHandler) WithGroup(name string) slog.Handler {
	return effectCaptureHandler{next: h.next.WithGroup(name)}
}

// parityRegRow is one in-memory pr_registry row.
type parityRegRow struct {
	branch, head, merge, target, status, title string
	meta                                       map[string]any
}

// parityRegistry applies each registry call's SQL transition to in-memory
// rows. Calls no golden lifecycle reaches fall through to mockPRRegistry.
type parityRegistry struct {
	*mockPRRegistry

	mu   sync.Mutex
	rows map[string]*parityRegRow
}

func newParityRegistry() *parityRegistry {
	return &parityRegistry{mockPRRegistry: &mockPRRegistry{}, rows: map[string]*parityRegRow{}}
}

func parityRowKey(repo string, pr int) string { return restateclient.PRObjectKey(repo, pr) }

func (r *parityRegistry) row(repo string, pr int) *parityRegRow {
	return r.rows[parityRowKey(repo, pr)]
}

// metaStrings reads a string-set metadata key.
func metaStrings(meta map[string]any, key string) []string {
	switch v := meta[key].(type) {
	case []string:
		return slices.Clone(v)
	case []any:
		out := make([]string, 0, len(v))
		for _, e := range v {
			if s, ok := e.(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}

// metaUnion returns the sorted, de-duplicated union of base and add.
func metaUnion(base []string, add ...string) []string {
	out := slices.Clone(base)
	for _, v := range add {
		if v = strings.TrimSpace(v); v != "" && !slices.Contains(out, v) {
			out = append(out, v)
		}
	}
	slices.Sort(out)
	return out
}

func statusOneOf(status string, set ...string) bool { return slices.Contains(set, status) }

// clearHeadScoped mirrors the SQL that drops head-scoped metadata.
func clearHeadScoped(meta map[string]any) {
	if n, ok := meta["ci_fail_count"].(float64); ok && n > 3 {
		delete(meta, "ci_fail_count")
	}
	delete(meta, "banner")
	delete(meta, "ci_checks")
	delete(meta, "build_metadata")
}

func (r *parityRegistry) RegisterPR(_ context.Context, req PRRegisterRequest) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	add := map[string]any{}
	if strings.TrimSpace(req.Metadata) != "" {
		if err := json.Unmarshal([]byte(req.Metadata), &add); err != nil {
			return "", err
		}
	}
	key := parityRowKey(req.Repo, req.PRNumber)
	row, ok := r.rows[key]
	if !ok {
		r.rows[key] = &parityRegRow{branch: req.Branch, head: req.HeadSHA, target: req.TargetID, status: prOpen, title: req.Title, meta: add}
		return req.TargetID, nil
	}
	if !statusOneOf(row.status, prMerged, prSynced, statusDeploying, statusDeployed) {
		if row.head != req.HeadSHA {
			clearHeadScoped(row.meta)
		} else {
			delete(row.meta, "banner")
		}
		row.status = prOpen
	}
	row.branch, row.head, row.target = req.Branch, req.HeadSHA, req.TargetID
	if req.Title != "" {
		row.title = req.Title
	}
	maps.Copy(row.meta, add)
	return req.TargetID, nil
}

// applyMerged mirrors UpdatePRMerged and BackfillPushMergeSHA.
func (r *parityRegistry) applyMerged(repo string, pr int, mergeSHA string) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	row := r.row(repo, pr)
	if row == nil {
		return ""
	}
	if mergeSHA != "" {
		row.merge = mergeSHA
	}
	if row.status != statusDeployFailedRow {
		delete(row.meta, "banner")
	}
	if !statusOneOf(row.status, prSynced, statusDeploying, statusDeployed, statusDeployFailedRow) {
		row.status = prMerged
	}
	return row.target
}

func (r *parityRegistry) UpdatePRMerged(_ context.Context, repo string, pr int, mergeSHA string) (string, error) {
	return r.applyMerged(repo, pr, strings.TrimSpace(mergeSHA)), nil
}

func (r *parityRegistry) BackfillPushMergeSHA(_ context.Context, repo string, pr int, mergeSHA string) (string, error) {
	return r.applyMerged(repo, pr, strings.TrimSpace(mergeSHA)), nil
}

func (r *parityRegistry) UpdatePRSync(_ context.Context, repo string, pr int, headSHA string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	row := r.row(repo, pr)
	if row == nil || row.merge != "" || statusOneOf(row.status, prMerged, prSynced, prClosedRow, statusDeploying, statusDeployed, statusDeployFailedRow) {
		return "", nil
	}
	_, banner := row.meta["banner"]
	if row.head == headSHA && !statusOneOf(row.status, prCIFailed, prConflict) && !banner {
		return "", nil
	}
	if row.head != headSHA {
		clearHeadScoped(row.meta)
	} else {
		delete(row.meta, "banner")
	}
	row.head, row.status = headSHA, prOpen
	return row.target, nil
}

func (r *parityRegistry) UpdatePRClosedUnmerged(_ context.Context, repo string, pr int) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	row := r.row(repo, pr)
	if row == nil {
		return "", nil
	}
	row.status = prClosedRow
	delete(row.meta, "banner")
	return row.target, nil
}

// atomicTransition mirrors the ci_failed and conflict transitions.
func (r *parityRegistry) atomicTransition(repo string, pr int, to string) (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	row := r.row(repo, pr)
	if row == nil || row.merge != "" || row.status == to || statusOneOf(row.status, prMerged, prSynced, prClosedRow, statusDeploying, statusDeployed, statusDeployFailedRow) {
		return "", false
	}
	row.status = to
	return row.target, true
}

func (r *parityRegistry) AtomicTransitionCIFailed(_ context.Context, repo string, pr int) (string, bool, error) {
	target, ok := r.atomicTransition(repo, pr, prCIFailed)
	return target, ok, nil
}

func (r *parityRegistry) AtomicTransitionConflict(_ context.Context, repo string, pr int) (string, bool, error) {
	target, ok := r.atomicTransition(repo, pr, prConflict)
	return target, ok, nil
}

func (r *parityRegistry) ResolvePRBySHA(_ context.Context, repo, sha string) (int, string, string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for key, row := range r.rows {
		if row.head == sha || (row.merge != "" && row.merge == sha) {
			var number int
			if _, err := fmtSscanPR(key, &number); err == nil && strings.HasPrefix(key, normalizeRepo(repo)+"#") {
				return number, row.branch, row.target, nil
			}
		}
	}
	return 0, "", "", nil
}

// fmtSscanPR parses the PR number out of a repo#pr key.
func fmtSscanPR(key string, out *int) (int, error) {
	_, num, ok := strings.Cut(key, "#")
	if !ok {
		return 0, errors.New("no pr number")
	}
	n := 0
	for _, c := range num {
		if c < '0' || c > '9' {
			return 0, errors.New("bad pr number")
		}
		n = n*10 + int(c-'0')
	}
	*out = n
	return 1, nil
}

func (r *parityRegistry) ResolvePRByNumber(_ context.Context, repo string, pr int) (string, string, string, string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	row := r.row(repo, pr)
	if row == nil {
		return "", "", "", "", nil
	}
	return row.target, row.branch, row.head, row.merge, nil
}

func (r *parityRegistry) RecordPRCheckStatus(_ context.Context, repo string, pr int, headSHA, checkName, checkState string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	row := r.row(repo, pr)
	if row == nil {
		return false, nil
	}
	preMerge := statusOneOf(row.status, prOpen, prCIFailed, prConflict) && row.head == headSHA
	postMerge := statusOneOf(row.status, prMerged, prSynced) && row.merge == headSHA
	if !preMerge && !postMerge {
		return false, nil
	}
	checks, _ := row.meta["ci_checks"].(map[string]any)
	if checks == nil {
		checks = map[string]any{}
		row.meta["ci_checks"] = checks
	}
	if checks[checkName] == checkState {
		return false, nil
	}
	checks[checkName] = checkState
	return true, nil
}

func (r *parityRegistry) TransitionDeploying(_ context.Context, repo string, pr int, jobs []string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	row := r.row(repo, pr)
	jobs = metaUnion(nil, jobs...)
	if row == nil || len(jobs) == 0 || statusOneOf(row.status, prClosedRow, prConflict) {
		return "", nil
	}
	deployed := metaStrings(row.meta, "deployed_jobs")
	pending := slices.DeleteFunc(slices.Clone(jobs), func(j string) bool { return slices.Contains(deployed, j) })
	if len(pending) == 0 {
		return "", nil
	}
	delete(row.meta, "banner")
	delete(row.meta, "deploy_failed_published")
	row.meta["jobs"] = slices.DeleteFunc(metaUnion(metaStrings(row.meta, "jobs"), jobs...), func(j string) bool { return slices.Contains(deployed, j) })
	row.meta["started_jobs"] = metaUnion(metaStrings(row.meta, "started_jobs"), jobs...)
	row.status = statusDeploying
	return row.target, nil
}

func (r *parityRegistry) RecordKustomizationRequests(_ context.Context, repo string, pr int, requests map[string]kustomizationRequest) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	row := r.row(repo, pr)
	if row == nil {
		return nil
	}
	recorded := map[string]any{}
	for name, req := range requests {
		recorded[name] = map[string]any{"namespace": req.Namespace, "requested_at": req.RequestedAt}
	}
	delete(row.meta, deployedKustomizationsKey)
	delete(row.meta, "deployed_jobs")
	row.meta[kustomizationRequestsKey] = recorded
	return nil
}

func (r *parityRegistry) HasKustomizationRequests(_ context.Context, repo string, pr int) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	row := r.row(repo, pr)
	if row == nil {
		return false, nil
	}
	reqs, _ := row.meta[kustomizationRequestsKey].(map[string]any)
	return len(reqs) > 0, nil
}

func (r *parityRegistry) MarkJobsDeployed(_ context.Context, repo string, pr int, jobs []string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	row := r.row(repo, pr)
	if row == nil || !statusOneOf(row.status, statusDeploying, statusDeployFailedRow) {
		return false, nil
	}
	eligible := metaUnion(metaStrings(row.meta, "started_jobs"), metaStrings(row.meta, "jobs")...)
	deployed := metaStrings(row.meta, "deployed_jobs")
	var add []string
	for _, j := range metaUnion(nil, jobs...) {
		if (len(eligible) == 0 || slices.Contains(eligible, j)) && !slices.Contains(deployed, j) {
			add = append(add, j)
		}
	}
	if len(add) == 0 {
		return false, nil
	}
	row.meta["deployed_jobs"] = metaUnion(deployed, add...)
	return true, nil
}

func (r *parityRegistry) MarkKustomizationsDeployed(_ context.Context, repo string, pr int, names []string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	row := r.row(repo, pr)
	if row == nil || !statusOneOf(row.status, statusDeploying, statusDeployFailedRow) {
		return false, nil
	}
	reqs, _ := row.meta[kustomizationRequestsKey].(map[string]any)
	if len(reqs) == 0 {
		return false, nil
	}
	deployed := metaStrings(row.meta, deployedKustomizationsKey)
	for _, n := range names {
		if _, ok := reqs[n]; ok {
			deployed = metaUnion(deployed, n)
		}
	}
	row.meta[deployedKustomizationsKey] = deployed
	for name := range reqs {
		if !slices.Contains(deployed, name) {
			return false, nil
		}
	}
	delete(row.meta, "banner")
	delete(row.meta, "deploy_failed_published")
	row.meta["deploy_success_pending"] = true
	row.status = statusDeployed
	return true, nil
}

func (r *parityRegistry) ListDeployingPRs(context.Context) ([]DeployingPR, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	keys := make([]string, 0, len(r.rows))
	for k := range r.rows {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var out []DeployingPR
	for _, k := range keys {
		row := r.rows[k]
		reqs, _ := row.meta[kustomizationRequestsKey].(map[string]any)
		pending, _ := row.meta["deploy_success_pending"].(bool)
		if row.status == statusDeploying || (row.status == statusDeployFailedRow && len(reqs) > 0) || (row.status == statusDeployed && pending) ||
			(statusOneOf(row.status, prMerged, prSynced) && len(metaStrings(row.meta, "jobs")) > 0) {
			var number int
			if _, err := fmtSscanPR(k, &number); err != nil {
				return nil, err
			}
			repo, _, _ := strings.Cut(k, "#")
			out = append(out, DeployingPR{Repo: repo, PRNumber: number, Branch: row.branch, HeadSHA: row.head, MergeSHA: row.merge, TargetID: row.target, Metadata: cloneMeta(row.meta)})
		}
	}
	return out, nil
}

// cloneMeta deep-copies metadata through JSON, like a database read.
func cloneMeta(meta map[string]any) map[string]any {
	raw, _ := json.Marshal(meta)
	out := map[string]any{}
	_ = json.Unmarshal(raw, &out)
	return out
}

func (r *parityRegistry) UpdatePRStatus(_ context.Context, repo string, pr int, status string, metadata map[string]any) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	row := r.row(repo, pr)
	if row == nil || strings.TrimSpace(status) == "" {
		return nil
	}
	row.status = status
	maps.Copy(row.meta, metadata)
	return nil
}

func (r *parityRegistry) AtomicTransitionDeployFailed(_ context.Context, repo string, pr int) (string, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	row := r.row(repo, pr)
	if row == nil {
		return "", false, nil
	}
	published, _ := row.meta["deploy_failed_published"].(bool)
	switch {
	case statusOneOf(row.status, statusDeploying, prMerged, prSynced):
		row.meta["deploy_failed_published"] = false
	case row.status == statusDeployFailedRow && !published:
	default:
		return "", false, nil
	}
	row.status = statusDeployFailedRow
	return row.target, true, nil
}

func (r *parityRegistry) AcknowledgeDeployFailedPublished(_ context.Context, repo string, pr int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if row := r.row(repo, pr); row != nil && row.status == statusDeployFailedRow {
		row.meta["deploy_failed_published"] = true
	}
	return nil
}

func (r *parityRegistry) LoadRawPRMetadata(_ context.Context, repo string, pr int) (map[string]any, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	row := r.row(repo, pr)
	if row == nil {
		return map[string]any{}, false, nil
	}
	return cloneMeta(row.meta), true, nil
}

func (r *parityRegistry) LoadPRBuildLifecycle(_ context.Context, repo string, pr int) (PRBuildLifecycle, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	row := r.row(repo, pr)
	if row == nil {
		return PRBuildLifecycle{}, nil
	}
	return PRBuildLifecycle{Status: row.status, HeadSHA: row.head, MergeSHA: row.merge, Started: len(metaStrings(row.meta, "started_jobs")) > 0}, nil
}

func (*parityRegistry) LoadPRBuildMetadata(context.Context, string, int) (BuildMetadata, bool, error) {
	return BuildMetadata{}, false, nil
}

func (r *parityRegistry) IsPRDeployed(_ context.Context, repo string, pr int) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	row := r.row(repo, pr)
	return row != nil && row.status == statusDeployed, nil
}

// rowsView renders every row in the golden shape with zero-valued
// metadata pruned, since prstate projects empty values as absent keys.
func (r *parityRegistry) rowsView() map[string]parityRow {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]parityRow, len(r.rows))
	for key, row := range r.rows {
		repo, _, _ := strings.Cut(key, "#")
		var number int
		_, _ = fmtSscanPR(key, &number)
		out[key] = parityRow{Repo: repo, PRNumber: number, Branch: row.branch, HeadSHA: row.head, MergeSHA: row.merge, TargetID: row.target, Status: row.status, Title: row.title, Metadata: pruneMeta(cloneMeta(row.meta))}
	}
	return out
}

// pruneMeta drops zero values (false, 0, "", empty collections).
func pruneMeta(meta map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range meta {
		switch x := v.(type) {
		case bool:
			if !x {
				continue
			}
		case float64:
			if x == 0 {
				continue
			}
		case string:
			if x == "" {
				continue
			}
		case []any:
			if len(x) == 0 {
				continue
			}
		case map[string]any:
			if len(x) == 0 {
				continue
			}
		case nil:
			continue
		}
		out[k] = v
	}
	return out
}

// parityHarness is one scenario's legacy router plus its fakes.
type parityHarness struct {
	t    *testing.T
	srv  *RouterServer
	reg  *parityRegistry
	bus  *mockEventBus
	disp *mockOutboundDispatcher
	sink *parityEffectSink
	ctx  context.Context
}

func newParityHarness(t *testing.T, sc parityScenario) *parityHarness {
	t.Helper()
	routes := map[string]kubeRoute{}
	for k, body := range sc.GitHubRoutes {
		routes[k] = kubeRoute{body: body}
	}
	_, ts := newKubeFixture(t, routes)
	h := &parityHarness{t: t, reg: newParityRegistry(), bus: &mockEventBus{}, disp: &mockOutboundDispatcher{}, sink: &parityEffectSink{}}
	cfg := Config{GitHubAPIURL: ts.URL, GitHubToken: "token", DefaultOwner: true, AutoUpdatePRAuthors: []string{"azylman"}}
	h.srv = newTestRouter(cfg, ts.Client(), h.reg, h.disp, h.bus)
	h.ctx = context.WithValue(t.Context(), parityEffectSinkKey{}, h.sink)
	return h
}

// run drives one legacy input through the router.
func (h *parityHarness) run(step paritySynapseStep) {
	h.t.Helper()
	switch step.Kind {
	case "register":
		req := httptest.NewRequestWithContext(h.ctx, http.MethodPost, "/internal/pr/register", bytes.NewReader(step.Register))
		rec := httptest.NewRecorder()
		h.srv.Routes().ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			h.t.Fatalf("register = %d %s", rec.Code, rec.Body.String())
		}
	case "github":
		if _, err := h.srv.ProcessGitHubEvent(h.ctx, step.Event, "delivery-"+step.Event, step.Payload); err != nil {
			h.t.Fatalf("%s webhook: %v", step.Event, err)
		}
	case "deploy":
		if _, err := h.srv.ProcessDeployEvent(h.ctx, *step.Deploy); err != nil {
			h.t.Fatalf("deploy alert: %v", err)
		}
	case "kustomization":
		o := step.Observation
		obs := kustomizationObservation{
			Ref:         fluxKustomizationRef{Namespace: o.Namespace, Name: o.Name},
			RequestedAt: o.RequestedAt, LastHandledReconcileAt: o.LastHandledReconcileAt, LastAppliedRevision: o.LastAppliedRevision,
			Conditions: readyCond("True", "ReconciliationSucceeded", ""),
		}
		if err := h.srv.ApplyKustomizationStatus(h.ctx, obs); err != nil {
			h.t.Fatalf("kustomization %s: %v", o.Name, err)
		}
	case "rollout_requested":
		h.runRollout(step.Rollout)
	case "image_promotion":
		p := step.Promotion
		if err := h.srv.promoteWorkflowRunImages(h.ctx, p.Images, ImageReadyEventRequest{Repo: p.Repo, CommitSHA: p.CommitSHA, PRNumber: p.PRNumber, TargetID: p.TargetID}); err != nil {
			h.t.Fatalf("image promotion: %v", err)
		}
	case "deploy_attempt_failed":
		f := step.Failure
		h.srv.handleDeployAttemptFailure(h.ctx, f.Repo, f.PRNumber, f.Branch, f.HeadSHA, f.TargetID, true, errors.New(f.Error))
	default:
		h.t.Fatalf("unknown synapse step kind %q", step.Kind)
	}
}

// runRollout runs workflow-run phases b, d and e: a tracked PR goes
// deploying, then the requests are recorded and reconciled.
func (h *parityHarness) runRollout(r *parityRollout) {
	h.t.Helper()
	trackPR := r.PRNumber > 0
	if trackPR {
		if err := h.srv.startWorkflowRunPRDeploying(h.ctx, r.Repo, r.PRNumber, r.HeadSHA, r.TargetID, r.Jobs); err != nil {
			h.t.Fatalf("start deploying: %v", err)
		}
	}
	names := make([]string, 0, len(r.Requests))
	for name := range r.Requests {
		names = append(names, name)
	}
	sort.Strings(names)
	refs := make([]fluxKustomizationRef, 0, len(names))
	for _, name := range names {
		refs = append(refs, fluxKustomizationRef{Namespace: r.Requests[name].Namespace, Name: name})
	}
	if err := h.srv.recordAndReconcileRequests(h.ctx, r.Repo, r.PRNumber, r.Branch, r.HeadSHA, r.TargetID, r.Jobs, refs, r.Requests, trackPR, true); err != nil {
		h.t.Fatalf("record and reconcile: %v", err)
	}
}

// effects collects the legacy effect multiset.
func (h *parityHarness) effects() []parityEffect {
	var out []parityEffect
	h.bus.mu.Lock()
	for _, f := range h.bus.publishedPREvents {
		out = append(out, parityEffect{Kind: restateclient.FactEffectKind(f.Event), Key: restateclient.FactMatchKey(f)})
	}
	h.bus.mu.Unlock()
	for _, e := range h.sink.snapshot() {
		if strings.HasPrefix(e.Kind, restateclient.FactEffectPrefix) {
			continue
		}
		out = append(out, e)
	}
	seen := map[string]bool{}
	for _, p := range h.disp.PromoteCalls() {
		key := restateclient.EffectMatchKey(p.Repo, p.PRNumber, p.CommitSHA, restateclient.EffectImagePromote, "", "")
		if !seen[key] {
			seen[key] = true
			out = append(out, parityEffect{Kind: restateclient.EffectImagePromote, Key: key})
		}
	}
	for range h.disp.PromptCalls() {
		out = append(out, parityEffect{Kind: restateclient.EffectEriPrompt})
	}
	for range h.disp.DispatchCalls() {
		out = append(out, parityEffect{Kind: restateclient.EffectEriBackground})
	}
	for range h.disp.DirectMessageCalls() {
		out = append(out, parityEffect{Kind: restateclient.EffectEriDirectMessage})
	}
	return out
}

func sortParityEffects(in []parityEffect) []parityEffect {
	out := append([]parityEffect{}, in...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Kind != out[j].Kind {
			return out[i].Kind < out[j].Kind
		}
		return out[i].Key < out[j].Key
	})
	return out
}

// goldenRows normalizes the golden rows through JSON like the fake's.
func goldenRows(t *testing.T, rows []parityRow) map[string]parityRow {
	t.Helper()
	out := make(map[string]parityRow, len(rows))
	for _, r := range rows {
		r.Metadata = pruneMeta(cloneMeta(r.Metadata))
		out[parityRowKey(r.Repo, r.PRNumber)] = r
	}
	return out
}

// TestLegacyLifecycleParity drives every golden lifecycle through the
// legacy router and asserts its effects and final rows equal the golden.
func TestLegacyLifecycleParity(t *testing.T) {
	t.Parallel()
	tests := loadParityScenarios(t)
	for _, tc := range tests {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			h := newParityHarness(t, tc)
			for _, step := range tc.Synapse {
				h.run(step)
			}
			if got, want := sortParityEffects(h.effects()), sortParityEffects(tc.Effects); !slices.Equal(got, want) {
				t.Errorf("effects =\n%+v\nwant\n%+v", got, want)
			}
			gotRows, wantRows := h.reg.rowsView(), goldenRows(t, tc.Rows)
			gotJSON, _ := json.MarshalIndent(gotRows, "", " ")
			wantJSON, _ := json.MarshalIndent(wantRows, "", " ")
			if !bytes.Equal(gotJSON, wantJSON) {
				t.Errorf("rows =\n%s\nwant\n%s", gotJSON, wantJSON)
			}
		})
	}
}

// statusDeployFailedRow is the deploy_failed registry status.
const statusDeployFailedRow = eventDeployFailed

// prClosedRow is the closed registry status.
const prClosedRow = ghActionClosed
