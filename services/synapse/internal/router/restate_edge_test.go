package router

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/azylman/aerial/pkg/prwire"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/pubsubtest"
	"github.com/azylman/aerial/pkg/restateingresstest"
	"github.com/azylman/aerial/services/synapse/internal/github"
	"github.com/azylman/aerial/services/synapse/internal/metrics"
)

// parityFixtureDir holds one golden lifecycle per file.
const parityFixtureDir = "../../../../testdata/prlifecycle/parity/"

// parityObservation is one Ready Kustomization the watcher observes.
type parityObservation struct {
	Namespace              string `json:"namespace"`
	Name                   string `json:"name"`
	RequestedAt            string `json:"requested_at"`
	LastHandledReconcileAt string `json:"last_handled_reconcile_at"`
	LastAppliedRevision    string `json:"last_applied_revision"`
}

// paritySynapseStep is one input synapse receives; Kind selects the
// populated field. Rollout, promotion and attempt-failure steps are decoded
// only by the prstate parity test.
type paritySynapseStep struct {
	Kind        string             `json:"kind"`
	Register    json.RawMessage    `json:"register"`
	Event       string             `json:"event"`
	Payload     json.RawMessage    `json:"payload"`
	Deploy      *DeployEvent       `json:"deploy"`
	Observation *parityObservation `json:"observation"`
}

// parityRestateSend names one send of a golden's restate stream; the
// event payload is pinned by the prstate parity test.
type parityRestateSend struct {
	Object  string `json:"object"`
	Handler string `json:"handler"`
}

// parityScenario is one golden file; synapse reads only its inputs and the
// handler names of its restate stream (TestRestateOwnedLifecycleEdge). The
// effects, rows and fact rollout states are proven by the prstate replay.
type parityScenario struct {
	Name    string              `json:"name"`
	Restate []parityRestateSend `json:"restate"`
	Synapse []paritySynapseStep `json:"synapse"`
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

// acceptBus records every publish: queued webhook jobs and aerial.pr.>
// lifecycle facts.
type acceptBus struct {
	pubsub.Bus

	mu    sync.Mutex
	jobs  []any
	facts int
}

func (b *acceptBus) Publish(_ context.Context, subject, _ string, payload any) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch payload.(type) {
	case GitHubWebhookArgs, DeployWebhookArgs:
		b.jobs = append(b.jobs, payload)
	}
	if strings.HasPrefix(subject, pubsub.SubjectAerialPRPrefix) {
		b.facts++
	}
	return nil
}

// drain returns and clears the queued jobs.
func (b *acceptBus) drain() []any {
	b.mu.Lock()
	defer b.mu.Unlock()
	jobs := b.jobs
	b.jobs = nil
	return jobs
}

func (b *acceptBus) factCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.facts
}

// parityAcceptStep reports whether a golden synapse input is a real accept
// point (register, GitHub webhook, deploy alert, watcher observation).
// Rollout, promotion and attempt-failure steps have no synapse accept
// point; their restate-owned twins are reached through the workflow_run
// webhook and are pinned by the restate_owned_deploy tests.
func parityAcceptStep(kind string) bool {
	return slices.Contains([]string{"register", "github", "deploy", "kustomization"}, kind)
}

// acceptEdge is a server whose every boundary records: Restate ingress, the
// bus and the dispatcher.
type acceptEdge struct {
	srv     *RouterServer
	ingress *ownedIngress
	disp    *mockOutboundDispatcher
	bus     *acceptBus
}

func newAcceptEdge(t *testing.T) *acceptEdge {
	t.Helper()
	e := &acceptEdge{
		ingress: &ownedIngress{
			sendCode: http.StatusAccepted, callCode: http.StatusOK, callBody: "{}",
			handlerBodies: map[string]string{"Register": `{"target_id":"100000000000000001"}`, "ListRollouts": `{"rollouts":null}`},
		},
		disp: &mockOutboundDispatcher{}, bus: &acceptBus{},
	}
	e.srv = NewRouterServer(fixedConfig(Config{
		RestateURL: e.ingress.serve(t), DefaultOwner: true,
		GitHubAPIURL: ownedGitHub(t, "[]"), GitHubToken: "token", AutoUpdatePRAuthors: []string{"azylman"},
		GitHubWebhookSecret: testGitHubWebhookSecret,
	}), nil)
	e.srv.SetEventBus(e.bus)
	e.srv.SetDispatcher(e.disp)
	return e
}

// run drives every accept-point step, replaying each queued job through its
// consumer, and returns how many steps it drove.
func (e *acceptEdge) run(ctx context.Context, t *testing.T, steps []paritySynapseStep) int {
	t.Helper()
	driven := 0
	for _, step := range steps {
		if !parityAcceptStep(step.Kind) {
			continue
		}
		driven++
		driveAcceptStep(ctx, t, e.srv, step)
		for _, job := range e.bus.drain() {
			replayAcceptJob(ctx, t, e.srv, job)
		}
	}
	return driven
}

// synapseEffects counts every side effect synapse itself performed.
func (e *acceptEdge) synapseEffects() int {
	e.disp.mu.Lock()
	defer e.disp.mu.Unlock()
	return e.bus.factCount() + len(e.disp.promptCalls) + len(e.disp.dispatchCalls) +
		len(e.disp.directMessageCalls) + len(e.disp.reconcileCalls)
}

// ownedHandlers counts the Restate writes by handler (one-way sends plus
// the request-response Register).
func (e *acceptEdge) ownedHandlers(t *testing.T) map[string]int {
	t.Helper()
	got := map[string]int{}
	paths, _ := e.ingress.sends()
	for _, p := range paths {
		trimmed := strings.TrimSuffix(p, "/send")
		got[trimmed[strings.LastIndex(trimmed, "/")+1:]]++
	}
	for _, c := range e.ingress.calls() {
		if strings.HasSuffix(c, "/Register") {
			got["Register"]++
		}
	}
	return got
}

// goldenHandlers counts a golden's restate stream by handler.
func goldenHandlers(sends []parityRestateSend) map[string]int {
	want := map[string]int{}
	for _, r := range sends {
		want[r.Handler]++
	}
	return want
}

// TestRestateOwnedLifecycleEdge drives every golden lifecycle's accept
// points, replaying queued webhook jobs through their JetStream consumers.
// synapse must perform zero effects (no aerial.pr.> publish, no
// eri, GitHub, Flux or image call), and its restate-owned sends must equal
// the golden's restate stream handler for handler (only a subset when the
// golden also has non-accept steps). The prstate parity test replays that
// stream and proves PRObject/RepoObject perform the golden effect multiset.
func TestRestateOwnedLifecycleEdge(t *testing.T) {
	t.Parallel()
	tests := loadParityScenarios(t)
	for _, tc := range tests {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			e := newAcceptEdge(t)
			driven := e.run(t.Context(), t, tc.Synapse)
			if got := e.synapseEffects(); got != 0 {
				t.Errorf("synapse effects = %d, want 0", got)
			}
			got, want := e.ownedHandlers(t), goldenHandlers(tc.Restate)
			if driven == len(tc.Synapse) && !maps.Equal(got, want) {
				t.Errorf("restate-owned sends = %v, want %v", got, want)
			}
			for h := range got {
				if want[h] == 0 {
					t.Errorf("send %s is not in the golden restate stream %v", h, want)
				}
			}
			if driven > 0 && len(got) == 0 {
				t.Errorf("%d accept steps made no restate write", driven)
			}
		})
	}
}

// driveAcceptStep sends one golden input through its real accept point.
func driveAcceptStep(ctx context.Context, t *testing.T, srv *RouterServer, step paritySynapseStep) {
	t.Helper()
	switch step.Kind {
	case "register":
		req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/internal/pr/register", bytes.NewReader(step.Register))
		rec := httptest.NewRecorder()
		srv.Routes().ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("register = %d %s", rec.Code, rec.Body.String())
		}
	case "github":
		req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/webhooks/github", bytes.NewReader(step.Payload))
		req.Header.Set("X-Github-Event", step.Event)
		req.Header.Set("X-Github-Delivery", "delivery-"+step.Event)
		req.Header.Set("X-Hub-Signature-256", signTestGitHubWebhook(testGitHubWebhookSecret, string(step.Payload)))
		rec := httptest.NewRecorder()
		srv.Routes().ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s webhook = %d %s", step.Event, rec.Code, rec.Body.String())
		}
	case "deploy":
		req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/webhooks/flux", http.NoBody)
		rec := httptest.NewRecorder()
		srv.enqueueOrProcessDeployEvent(rec, req, *step.Deploy, "")
		if rec.Code >= http.StatusBadRequest {
			t.Fatalf("deploy alert = %d %s", rec.Code, rec.Body.String())
		}
	case "kustomization":
		o := step.Observation
		obs := kustomizationObservation{
			Ref:         fluxKustomizationRef{Namespace: o.Namespace, Name: o.Name},
			RequestedAt: o.RequestedAt, LastHandledReconcileAt: o.LastHandledReconcileAt, LastAppliedRevision: o.LastAppliedRevision,
			Conditions: readyCond("True", "ReconciliationSucceeded", ""),
		}
		if err := srv.ApplyKustomizationStatus(ctx, obs); err != nil {
			t.Fatalf("kustomization %s: %v", o.Name, err)
		}
	}
}

// replayAcceptJob runs one queued job through its JetStream consumer, the
// way the worker would.
func replayAcceptJob(ctx context.Context, t *testing.T, srv *RouterServer, job any) {
	t.Helper()
	data, err := json.Marshal(job)
	if err != nil {
		t.Fatalf("marshal job: %v", err)
	}
	switch job.(type) {
	case GitHubWebhookArgs:
		err = srv.HandleGitHubJob(ctx, &pubsubtest.Message{Payload: data, Subj: "aerial.jobs.synapse", Delivered: 1})
	case DeployWebhookArgs:
		err = srv.HandleDeployJob(ctx, &pubsubtest.Message{Payload: data, Subj: "aerial.jobs.synapse", Delivered: 1})
	}
	if err != nil {
		t.Fatalf("replay job: %v", err)
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

// TestRestateSend pins the one send core every Restate request goes through:
// a missing restate_url and a transport/5xx failure are retryable, a 4xx is
// terminal, and a 2xx counts as sent.
func TestRestateSend(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		status       int
		noURL        bool
		wantErr      bool
		wantTerminal bool
	}{
		{name: "accepted send succeeds", status: http.StatusAccepted},
		{name: "server error is retryable", status: http.StatusServiceUnavailable, wantErr: true},
		{name: "client error is terminal", status: http.StatusNotFound, wantErr: true, wantTerminal: true},
		{name: "missing restate url is retryable", noURL: true, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
			}))
			t.Cleanup(srv.Close)
			cfg := Config{RestateURL: srv.URL}
			if tc.noURL {
				cfg.RestateURL = ""
			}
			s := NewRouterServer(fixedConfig(cfg), nil)
			err := s.sendOwnedPR(context.Background(), "HandleMerged", prwire.PREvent{Repo: "azylman/aerial", PRNumber: 7, MergeSHA: "abc123"})
			if (err != nil) != tc.wantErr {
				t.Fatalf("sendOwnedPR() err = %v, wantErr %v", err, tc.wantErr)
			}
			failed, terminal := restateSendFailure(err)
			if failed != tc.wantErr || terminal != tc.wantTerminal {
				t.Errorf("restateSendFailure() = %v/%v, want %v/%v", failed, terminal, tc.wantErr, tc.wantTerminal)
			}
		})
	}
}

// TestRestateSendSkipsKeylessEvents pins that PR-less and repo-less events
// have no Restate object key and send nothing.
func TestRestateSendSkipsKeylessEvents(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		pr   prwire.PREvent
		repo prwire.RepoEvent
		sha  string
	}{
		{name: "pr-less events without sha", pr: prwire.PREvent{Repo: "azylman/aerial"}, repo: prwire.RepoEvent{}, sha: ""},
		{name: "repo-less events with zero sha", pr: prwire.PREvent{PRNumber: 7}, repo: prwire.RepoEvent{MainSHA: "abc123"}, sha: "0000000"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int64
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				w.WriteHeader(http.StatusAccepted)
			}))
			t.Cleanup(srv.Close)
			s := NewRouterServer(fixedConfig(Config{RestateURL: srv.URL}), nil)
			ctx := context.Background()
			pr := tc.pr
			pr.PRNumber = 0
			errs := errors.Join(s.sendOwnedPR(ctx, "HandleMerged", tc.pr), s.sendOwnedRepo(ctx, "HandleMainPush", tc.repo),
				s.sendOwnedPROrSHA(ctx, "HandlePushBySHA", "HandleMerged", tc.sha, pr))
			if errs != nil {
				t.Fatalf("sends = %v", errs)
			}
			if got := calls.Load(); got != 0 {
				t.Errorf("restate requests = %d, want 0", got)
			}
		})
	}
}

const (
	recoveryHead  = "c0de232700000000000000000000000000002327"
	recoveryMerge = "353b1b0500000000000000000000000000002327"
)

// recoveryRepo and recoveryPR are the decoded Restate object path prefixes
// the recovery path calls: the RepoObject SHA index and the PRObject.
const (
	recoveryRepo = "/RepoObject/azylman/aerial/"
	recoveryPR   = "/PRObject/azylman/aerial#2327/"
)

// recoverySends returns the one-way sends the ingress received, in order.
func recoverySends(in *restateingresstest.Server) []restateingresstest.Call {
	var sends []restateingresstest.Call
	for _, c := range in.Calls() {
		if strings.HasSuffix(c.Path, "/send") {
			sends = append(sends, c)
		}
	}
	return sends
}

// recoveryHandler is the Restate handler a send targeted.
func recoveryHandler(c restateingresstest.Call) string {
	path := strings.TrimSuffix(c.Path, "/send")
	return path[strings.LastIndex(path, "/")+1:]
}

// recoveryGitHub serves the commits/<sha>/pulls lookup with code and body
// and an empty open-PR list.
func recoveryGitHub(t *testing.T, code int, body string) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/commits/") {
			w.WriteHeader(code)
			_, _ = w.Write([]byte(body))
			return
		}
		_, _ = w.Write([]byte(`[]`))
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func TestShouldRecoverMerge(t *testing.T) {
	t.Parallel()
	merged := github.CommitPR{Number: 2327, HeadSHA: recoveryHead, Merged: true, MergeCommitSHA: recoveryMerge}
	tests := []struct {
		name string
		pr   github.CommitPR
		sha  string
		view prwire.DeployView
		want bool
	}{
		{name: "open tracked pr merged on the commit recovers", pr: merged, sha: recoveryMerge, view: prwire.DeployView{Found: true, Status: "open"}, want: true},
		{name: "merge sha match ignores case and space", pr: merged, sha: " " + strings.ToUpper(recoveryMerge) + " ", view: prwire.DeployView{Found: true}, want: true},
		{name: "pr merged on another commit in restate still recovers", pr: merged, sha: recoveryMerge, view: prwire.DeployView{Found: true, MergeSHA: recoveryHead}, want: true},
		{name: "already merged on the commit sends nothing", pr: merged, sha: recoveryMerge, view: prwire.DeployView{Found: true, MergeSHA: strings.ToUpper(recoveryMerge)}},
		{name: "untracked pr sends nothing", pr: merged, sha: recoveryMerge},
		{name: "unmerged pr sends nothing", pr: github.CommitPR{Number: 2327, HeadSHA: recoveryHead, MergeCommitSHA: recoveryMerge}, sha: recoveryMerge, view: prwire.DeployView{Found: true}},
		{name: "merge commit other than the sha sends nothing", pr: merged, sha: recoveryHead, view: prwire.DeployView{Found: true}},
		{name: "blank sha sends nothing", pr: merged, view: prwire.DeployView{Found: true}},
		{name: "no pr sends nothing", pr: github.CommitPR{Merged: true, MergeCommitSHA: recoveryMerge}, sha: recoveryMerge, view: prwire.DeployView{Found: true}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := shouldRecoverMerge(tc.pr, tc.sha, tc.view); got != tc.want {
				t.Errorf("shouldRecoverMerge() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestResolveOwnedSHAPRMergeRecovery drives the restate-owned resolve and
// recover path. Rows run serially because they share the global
// merge-recovered counter.
func TestResolveOwnedSHAPRMergeRecovery(t *testing.T) {
	mergedPR := `[{"number":2327,"state":"closed","head":{"ref":"feat/redaction-purge","sha":"` + recoveryHead + `"},"merged_at":"2026-11-21T08:38:21Z","merge_commit_sha":"` + recoveryMerge + `"}]`
	openView := `{"found":true,"repo":"azylman/aerial","pr_number":2327,"status":"open","head_sha":"` + recoveryHead + `"}`
	tests := []struct {
		name         string
		index        string
		ghCode       int
		ghBody       string
		view         string
		viewCode     int
		sendCode     int
		wantPR       int
		wantSends    []string
		wantRecorded float64
	}{
		{name: "send failure records no recovery", index: `{"found":false}`, ghCode: 200, ghBody: mergedPR, view: openView, sendCode: 500, wantPR: 2327, wantSends: []string{"HandleMerged"}},
		{name: "index hit sends no merge", index: `{"pr_number":2327,"branch":"feat/redaction-purge","found":true}`, ghCode: 200, ghBody: mergedPR, view: openView, wantPR: 2327},
		{name: "github fallback on a merged pr recovers the merge", index: `{"found":false}`, ghCode: 200, ghBody: mergedPR, view: openView, wantPR: 2327, wantSends: []string{"HandleMerged"}, wantRecorded: 1},
		{name: "restate already merged on the sha sends nothing", index: `{"found":false}`, ghCode: 200, ghBody: mergedPR, view: `{"found":true,"status":"deploying","merge_sha":"` + recoveryMerge + `"}`, wantPR: 2327},
		{name: "unmerged pr sends nothing", index: `{"found":false}`, ghCode: 200, ghBody: `[{"number":2327,"state":"open","head":{"ref":"feat/redaction-purge","sha":"` + recoveryHead + `"},"merge_commit_sha":"` + recoveryMerge + `"}]`, view: openView, wantPR: 2327},
		{name: "merge commit other than the sha sends nothing", index: `{"found":false}`, ghCode: 200, ghBody: `[{"number":2327,"state":"closed","head":{"ref":"feat/redaction-purge","sha":"` + recoveryHead + `"},"merged_at":"2026-11-21T08:38:21Z","merge_commit_sha":"` + recoveryHead + `"}]`, view: openView, wantPR: 2327},
		{name: "untracked pr sends nothing", index: `{"found":false}`, ghCode: 200, ghBody: mergedPR, view: `{"found":false}`, wantPR: 2327},
		{name: "deploy view failure sends nothing", index: `{"found":false}`, ghCode: 200, ghBody: mergedPR, view: "boom", viewCode: 500, wantPR: 2327},
		{name: "github error resolves nothing and sends nothing", index: `{"found":false}`, ghCode: 500, ghBody: "boom", view: openView},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ingress := restateingresstest.New(t)
			ingress.Routes = map[string]restateingresstest.Response{
				recoveryRepo + "ResolvePRBySHA":  {Body: tc.index},
				recoveryPR + "GetDeployView":     {Status: tc.viewCode, Body: tc.view},
				recoveryPR + "HandleMerged/send": {Status: tc.sendCode},
			}
			cfg := Config{RestateURL: ingress.URL(), GitHubAPIURL: recoveryGitHub(t, tc.ghCode, tc.ghBody)}
			srv := NewRouterServer(fixedConfig(cfg), nil)
			ctx := context.Background()
			counter := metrics.RestateMergeRecoveredTotal.WithLabelValues(mergeRecoveryPathOwnedDeploy)
			before := testutil.ToFloat64(counter)
			prNum, _ := srv.resolveOwnedSHAPR(ctx, "azylman/aerial", recoveryMerge)
			if prNum != tc.wantPR {
				t.Errorf("resolveOwnedSHAPR() pr = %d, want %d", prNum, tc.wantPR)
			}
			sends := recoverySends(ingress)
			handlers := make([]string, 0, len(sends))
			for _, s := range sends {
				handlers = append(handlers, recoveryHandler(s))
			}
			if strings.Join(handlers, ",") != strings.Join(tc.wantSends, ",") {
				t.Errorf("sends = %v, want %v", handlers, tc.wantSends)
			}
			if got := testutil.ToFloat64(counter) - before; got != tc.wantRecorded {
				t.Errorf("merge recovered delta = %v, want %v", got, tc.wantRecorded)
			}
		})
	}
}

// TestOwnedDeployEventRecoversMergeFirst pins the order on the restate-owned
// deploy path: the recovered HandleMerged is sent before the deploy event,
// under the deterministic merge-recovery idempotency identity.
func TestOwnedDeployEventRecoversMergeFirst(t *testing.T) {
	mergedPR := `[{"number":2327,"state":"closed","head":{"ref":"feat/redaction-purge","sha":"` + recoveryHead + `"},"merged_at":"2026-11-21T08:38:21Z","merge_commit_sha":"` + recoveryMerge + `"}]`
	tests := []struct {
		name        string
		evt         DeployEvent
		wantFirst   string
		wantSecond  string
		wantKeyPart string
	}{
		{
			name:        "deploy failure on the merge commit",
			evt:         DeployEvent{Event: eventDeployFailed, Repo: "azylman/aerial", CommitSHA: recoveryMerge, JobName: "aerial-core", Status: "failed"},
			wantFirst:   "HandleMerged",
			wantSecond:  "HandleDeployFailed",
			wantKeyPart: "merge-recovery:azylman/aerial#2327@" + recoveryMerge,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ingress := restateingresstest.New(t)
			ingress.Routes = map[string]restateingresstest.Response{
				recoveryRepo + "ResolvePRBySHA": {Body: `{"found":false}`},
				recoveryPR + "GetDeployView":    {Body: `{"found":true,"repo":"azylman/aerial","pr_number":2327,"status":"open","target_id":"100000000000000003","head_sha":"` + recoveryHead + `"}`},
			}
			cfg := Config{RestateURL: ingress.URL(), GitHubAPIURL: recoveryGitHub(t, 200, mergedPR)}
			srv := newTestRouter(cfg, nil, &mockOutboundDispatcher{}, &mockEventBus{})
			ctx := context.Background()
			if _, err := srv.processOwnedDeployEvent(ctx, tc.evt); err != nil {
				t.Fatalf("processOwnedDeployEvent() err = %v", err)
			}
			sends := recoverySends(ingress)
			if len(sends) != 2 || recoveryHandler(sends[1]) != tc.wantSecond {
				t.Fatalf("sends = %+v, want the recovered merge then the deploy event", sends)
			}
			if recoveryHandler(sends[0]) != tc.wantFirst || !strings.Contains(sends[0].IdempotencyKey, tc.wantKeyPart) {
				t.Errorf("first send = %s key %q, want %s keyed %q", recoveryHandler(sends[0]), sends[0].IdempotencyKey, tc.wantFirst, tc.wantKeyPart)
			}
			var ev prwire.PREvent
			if err := json.Unmarshal(sends[0].Body, &ev); err != nil {
				t.Fatal(err)
			}
			if ev.HeadSHA != recoveryHead || ev.MergeSHA != recoveryMerge {
				t.Errorf("recovered merge = %+v, want head %s merge %s", ev, recoveryHead, recoveryMerge)
			}
		})
	}
}
