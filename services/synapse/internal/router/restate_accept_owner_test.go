package router

import (
	"bytes"
	"context"
	"encoding/json"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/synapse/internal/restateclient"
)

// acceptJobMsg is one queued JetStream job replayed into its consumer.
type acceptJobMsg struct{ data []byte }

func (m *acceptJobMsg) Data() []byte                   { return m.data }
func (*acceptJobMsg) Subject() string                  { return "aerial.jobs.synapse" }
func (*acceptJobMsg) Header() nats.Header              { return nil }
func (*acceptJobMsg) NumDelivered() uint64             { return 1 }
func (*acceptJobMsg) Ack() error                       { return nil }
func (*acceptJobMsg) NakWithDelay(time.Duration) error { return nil }
func (*acceptJobMsg) Term() error                      { return nil }
func (*acceptJobMsg) Snooze(time.Duration) error       { return nil }
func (*acceptJobMsg) InProgress() error                { return nil }

// acceptBus records every publish: queued webhook jobs (with the
// effects_owner they were stamped with) and aerial.pr.> lifecycle facts.
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

// jobOwner reads the effects_owner a queued job was stamped with.
func jobOwner(job any) string {
	switch j := job.(type) {
	case GitHubWebhookArgs:
		return j.EffectsOwner
	case DeployWebhookArgs:
		return j.EffectsOwner
	}
	return ""
}

func TestNewEventEffectsOwner(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		authoritative bool
		want          string
	}{
		{name: "authoritative snapshot is restate-owned", authoritative: true, want: "restate"},
		{name: "non-authoritative snapshot is legacy", authoritative: false, want: "legacy"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := newEventEffectsOwner(tc.authoritative); got != tc.want {
				t.Errorf("newEventEffectsOwner(%v) = %q, want %q", tc.authoritative, got, tc.want)
			}
		})
	}
}

// TestWebhookAcceptStampsOwner proves both webhook accept points stamp the
// queued job from the config snapshot at accept time, and that a
// restate-owned flux alert is queued without touching pr_registry.
func TestWebhookAcceptStampsOwner(t *testing.T) {
	t.Parallel()
	const pingBody = `{"zen":"Keep it simple"}`
	const fluxBody = `{"involvedObject":{"kind":"Kustomization","name":"aerial-core","namespace":"flux-system"},"severity":"info","reason":"ReconciliationSucceeded","message":"applied","metadata":{"revision":"main@sha1:abcdef1234"}}`
	tests := []struct {
		name          string
		path          string
		event         string
		body          string
		authoritative bool
		wantOwner     string
		wantDBCalls   bool
	}{
		{name: "github webhook authoritative stamps restate", path: "/webhooks/github", event: "ping", body: pingBody, authoritative: true, wantOwner: "restate"},
		{name: "github webhook non-authoritative stamps legacy", path: "/webhooks/github", event: "ping", body: pingBody, wantOwner: "legacy"},
		{name: "flux webhook authoritative stamps restate without registry", path: "/webhooks/flux", body: fluxBody, authoritative: true, wantOwner: "restate"},
		{name: "flux webhook non-authoritative stamps legacy after registry enrichment", path: "/webhooks/flux", body: fluxBody, wantOwner: "legacy", wantDBCalls: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := &atomic.Int64{}
			srv := NewRouterServer(fixedConfig(Config{GitHubWebhookSecret: testGitHubWebhookSecret, RestateAuthoritative: tc.authoritative}), nil)
			srv.SetRegistry(NewPostgresPRRegistry(countingPool(db)))
			bus := &acceptBus{}
			srv.SetEventBus(bus)
			req := httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			if tc.event != "" {
				req.Header.Set("X-Github-Event", tc.event)
				req.Header.Set("X-Github-Delivery", "d-accept")
				req.Header.Set("X-Hub-Signature-256", signTestGitHubWebhook(testGitHubWebhookSecret, tc.body))
			}
			rec := httptest.NewRecorder()
			srv.Routes().ServeHTTP(rec, req)
			jobs := bus.drain()
			if len(jobs) != 1 {
				t.Fatalf("queued jobs = %d (status %d %s), want 1", len(jobs), rec.Code, rec.Body.String())
			}
			if got := jobOwner(jobs[0]); got != tc.wantOwner {
				t.Errorf("job effects_owner = %q, want %q", got, tc.wantOwner)
			}
			if got := db.Load() > 0; got != tc.wantDBCalls {
				t.Errorf("pr_registry touched = %v (calls %d), want %v", got, db.Load(), tc.wantDBCalls)
			}
		})
	}
}

// TestFluxWatcherStampsOwner proves each watcher sink call snapshots the
// owner from config: authoritative ticks list rollouts from the Restate
// index, others from pr_registry.
func TestFluxWatcherStampsOwner(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		authoritative bool
		wantIngress   bool
		wantDBCalls   bool
	}{
		{name: "authoritative tick reads the restate rollout index", authoritative: true, wantIngress: true},
		{name: "non-authoritative tick reads pr_registry", wantDBCalls: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ingress := &ownedIngress{sendCode: http.StatusAccepted, callCode: http.StatusOK, callBody: `{"rollouts":null}`}
			db := &atomic.Int64{}
			srv := NewRouterServer(fixedConfig(Config{RestateURL: ingress.serve(t), RestateAuthoritative: tc.authoritative}), nil)
			srv.SetRegistry(NewPostgresPRRegistry(countingPool(db)))
			if _, err := srv.HasPendingRollouts(context.Background()); err != nil {
				t.Fatalf("HasPendingRollouts() err = %v", err)
			}
			if got := len(ingress.calls()) > 0; got != tc.wantIngress {
				t.Errorf("restate calls = %v, want %v", ingress.calls(), tc.wantIngress)
			}
			if got := db.Load() > 0; got != tc.wantDBCalls {
				t.Errorf("pr_registry touched = %v (calls %d), want %v", got, db.Load(), tc.wantDBCalls)
			}
		})
	}
}

// parityAcceptStep reports whether a golden synapse input is a real accept
// point (register, GitHub webhook, deploy alert, watcher observation).
// Rollout, promotion and attempt-failure steps call legacy internals
// directly; their restate-owned twins are reached through the workflow_run
// webhook and are pinned by the restate_owned_deploy tests.
func parityAcceptStep(kind string) bool {
	return slices.Contains([]string{"register", "github", "deploy", "kustomization"}, kind)
}

// acceptEdge is an authoritative server whose every boundary records:
// Restate ingress, pr_registry queries, the bus and the dispatcher.
type acceptEdge struct {
	srv     *RouterServer
	ingress *ownedIngress
	db      *atomic.Int64
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
		db: &atomic.Int64{}, disp: &mockOutboundDispatcher{}, bus: &acceptBus{},
	}
	e.srv = NewRouterServer(fixedConfig(Config{
		RestateURL: e.ingress.serve(t), RestateAuthoritative: true, DefaultOwner: true,
		GitHubAPIURL: ownedGitHub(t, "[]"), GitHubToken: "token", AutoUpdatePRAuthors: []string{"azylman"},
		GitHubWebhookSecret: testGitHubWebhookSecret,
	}), nil)
	e.srv.SetRegistry(NewPostgresPRRegistry(countingPool(e.db)))
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
			if got := jobOwner(job); got != restateclient.EffectsOwnerRestate {
				t.Fatalf("queued job effects_owner = %q, want restate", got)
			}
			replayAcceptJob(ctx, t, e.srv, job)
		}
	}
	return driven
}

// synapseEffects counts every side effect synapse itself performed.
func (e *acceptEdge) synapseEffects() int {
	e.disp.mu.Lock()
	defer e.disp.mu.Unlock()
	return e.bus.factCount() + len(e.disp.gitPushCalls) + len(e.disp.promptCalls) + len(e.disp.dispatchCalls) +
		len(e.disp.directMessageCalls) + len(e.disp.promoteCalls) + len(e.disp.reconcileCalls)
}

// ownedHandlers counts the Restate writes by handler (one-way sends plus
// the request-response Register) and reports any send not stamped
// restate-owned.
func (e *acceptEdge) ownedHandlers(t *testing.T) map[string]int {
	t.Helper()
	got := map[string]int{}
	paths, bodies := e.ingress.sends()
	for i, p := range paths {
		trimmed := strings.TrimSuffix(p, "/send")
		got[trimmed[strings.LastIndex(trimmed, "/")+1:]]++
		if !strings.Contains(bodies[i], `"effects_owner":"restate"`) {
			t.Errorf("send %s body %s is not restate-owned", p, bodies[i])
		}
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
// points with restate_authoritative on and no pre-stamped context: each
// event is stamped restate at accept time and queued webhook jobs are
// replayed through their JetStream consumers. synapse must make zero
// pr_registry calls and perform zero effects (no aerial.pr.> publish, no
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
			if got := e.db.Load(); got != 0 {
				t.Errorf("pr_registry calls = %d, want 0", got)
			}
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
// way the worker would, so the publish-time stamp is the one in effect.
func replayAcceptJob(ctx context.Context, t *testing.T, srv *RouterServer, job any) {
	t.Helper()
	data, err := json.Marshal(job)
	if err != nil {
		t.Fatalf("marshal job: %v", err)
	}
	switch job.(type) {
	case GitHubWebhookArgs:
		err = srv.HandleGitHubJob(ctx, &acceptJobMsg{data: data})
	case DeployWebhookArgs:
		err = srv.HandleDeployJob(ctx, &acceptJobMsg{data: data})
	}
	if err != nil {
		t.Fatalf("replay job: %v", err)
	}
}
