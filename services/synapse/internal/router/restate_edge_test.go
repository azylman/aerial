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
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/azylman/aerial/pkg/pubsub"
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
// Rollout, promotion and attempt-failure steps call legacy internals
// directly; their restate-owned twins are reached through the workflow_run
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
		err = srv.HandleGitHubJob(ctx, &acceptJobMsg{data: data})
	case DeployWebhookArgs:
		err = srv.HandleDeployJob(ctx, &acceptJobMsg{data: data})
	}
	if err != nil {
		t.Fatalf("replay job: %v", err)
	}
}
