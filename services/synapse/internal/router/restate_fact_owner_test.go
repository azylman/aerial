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
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/synapse/internal/restateclient"
)

// ingressCall is one recorded Restate ingress request: the PRObject handler
// and the event names of the facts its payload carried.
type ingressCall struct {
	handler string
	facts   string
}

// ingressRecorder records every Restate ingress request and answers every
// verdict with a publish decision.
type ingressRecorder struct {
	mu    sync.Mutex
	calls []ingressCall
}

func newIngressRecorder(t *testing.T, failing bool) (*httptest.Server, *ingressRecorder) {
	t.Helper()
	rec := &ingressRecorder{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(strings.Trim(r.URL.EscapedPath(), "/"), "/")
		handler := ""
		if len(parts) >= 3 {
			handler = parts[2]
		}
		body, _ := io.ReadAll(r.Body)
		var ev restateclient.PREvent
		_ = json.Unmarshal(body, &ev)
		names := make([]string, 0, len(ev.Facts))
		for _, f := range ev.Facts {
			names = append(names, f.Event)
		}
		rec.mu.Lock()
		rec.calls = append(rec.calls, ingressCall{handler: handler, facts: strings.Join(names, ",")})
		rec.mu.Unlock()
		if failing {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"disposition":"apply","should_publish":true}`))
	}))
	t.Cleanup(srv.Close)
	return srv, rec
}

func (r *ingressRecorder) snapshot() []ingressCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]ingressCall(nil), r.calls...)
}

// ownerServer builds a lifecycle router against the recorder.
func ownerServer(t *testing.T, authoritative, failing bool, reg *mockPRRegistry) (*RouterServer, *mockEventBus, *ingressRecorder) {
	t.Helper()
	ingress, rec := newIngressRecorder(t, failing)
	cfg := lifecycleCfg()
	cfg.RestateAuthoritative = authoritative
	cfg.RestateURL = ingress.URL
	bus := &mockEventBus{}
	return newLifecycleServer(cfg, bus, reg, &mockOutboundDispatcher{}), bus, rec
}

func ownerCtx(owner string) context.Context {
	return withEffectsOwner(context.Background(), owner)
}

func assertOwnerRun(t *testing.T, rec *ingressRecorder, bus *mockEventBus, wantCalls []ingressCall, wantPublishes int) {
	t.Helper()
	got := rec.snapshot()
	if len(got) != len(wantCalls) {
		t.Fatalf("ingress calls = %+v, want %+v", got, wantCalls)
	}
	for i := range got {
		if got[i] != wantCalls[i] {
			t.Fatalf("ingress call %d = %+v, want %+v", i, got[i], wantCalls[i])
		}
	}
	bus.mu.Lock()
	n := len(bus.publishedPREvents)
	bus.mu.Unlock()
	if n != wantPublishes {
		t.Fatalf("published lifecycle events = %d, want %d", n, wantPublishes)
	}
}

const (
	ownerLegacy  = restateclient.EffectsOwnerLegacy
	ownerRestate = restateclient.EffectsOwnerRestate
)

// TestTransitionRegisteredConflictEffectsOwner pins conflict ownership:
// legacy events journal the merged HandleConflict verdict with the fact and
// publish it themselves (or, authoritative, resolve and ack the legacy
// verdict pair), while restate-owned events route the fact and never
// publish.
func TestTransitionRegisteredConflictEffectsOwner(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		authoritative bool
		failing       bool
		wantErr       bool
		owner         string
		updated       bool
		wantCalls     []ingressCall
		wantPublishes int
	}{
		{name: "legacy transition journals then publishes", owner: ownerLegacy, updated: true, wantCalls: []ingressCall{{handler: "HandleConflict", facts: "conflict"}}, wantPublishes: 1},
		{name: "legacy fallback journals then publishes", owner: ownerLegacy, wantCalls: []ingressCall{{handler: "HandleConflict", facts: "conflict"}}, wantPublishes: 1},
		{name: "restate transition routes the fact without publishing", owner: ownerRestate, updated: true, wantCalls: []ingressCall{{handler: "HandleConflict", facts: "conflict"}}},
		{name: "restate fallback routes the fact without publishing", owner: ownerRestate, wantCalls: []ingressCall{{handler: "HandleConflict", facts: "conflict"}}},
		{name: "authoritative restate routes the fact without publishing", authoritative: true, owner: ownerRestate, updated: true, wantCalls: []ingressCall{{handler: "HandleConflict", facts: "conflict"}}},
		{name: "authoritative restate route failure surfaces", authoritative: true, failing: true, wantErr: true, owner: ownerRestate, updated: true, wantCalls: []ingressCall{{handler: "HandleConflict", facts: "conflict"}}},
		{name: "authoritative restate fallback route failure surfaces", authoritative: true, failing: true, wantErr: true, owner: ownerRestate, wantCalls: []ingressCall{{handler: "HandleConflict", facts: "conflict"}}},
		{name: "authoritative legacy verdict failure surfaces", authoritative: true, failing: true, wantErr: true, owner: ownerLegacy, updated: true, wantCalls: []ingressCall{{handler: "HandleConflictDetected"}}},
		{
			name: "authoritative legacy resolves publishes and acks", authoritative: true, owner: ownerLegacy, updated: true,
			wantCalls: []ingressCall{{handler: "HandleConflictDetected"}, {handler: "HandleConflictPublished"}}, wantPublishes: 1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			reg := &mockPRRegistry{conflictTargetID: lifecycleTargetID, conflictUpdated: tc.updated, resolveTargetID: lifecycleTargetID}
			srv, bus, rec := ownerServer(t, tc.authoritative, tc.failing, reg)
			handled, err := srv.transitionRegisteredConflict(ownerCtx(tc.owner), "azylman/aerial", 42, "feat/branch", "sha-conflict")
			if (err != nil) != tc.wantErr || handled == tc.wantErr {
				t.Fatalf("transitionRegisteredConflict() handled = %v, err = %v, wantErr %v", handled, err, tc.wantErr)
			}
			assertOwnerRun(t, rec, bus, tc.wantCalls, tc.wantPublishes)
		})
	}
}

// TestPublishUnownedDeployEventEffectsOwner pins unowned deploy_success
// ownership across authority modes.
func TestPublishUnownedDeployEventEffectsOwner(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		authoritative bool
		failing       bool
		wantErr       bool
		owner         string
		wantCalls     []ingressCall
		wantPublishes int
	}{
		{name: "legacy journals the merged verdict then publishes", owner: ownerLegacy, wantCalls: []ingressCall{{handler: "HandleDeploySuccess", facts: "deploy_success"}}, wantPublishes: 1},
		{name: "restate routes the fact without publishing", owner: ownerRestate, wantCalls: []ingressCall{{handler: "HandleDeploySuccess", facts: "deploy_success"}}},
		{name: "authoritative restate routes the fact without publishing", authoritative: true, owner: ownerRestate, wantCalls: []ingressCall{{handler: "HandleDeploySuccess", facts: "deploy_success"}}},
		{name: "authoritative restate route failure surfaces", authoritative: true, failing: true, wantErr: true, owner: ownerRestate, wantCalls: []ingressCall{{handler: "HandleDeploySuccess", facts: "deploy_success"}}},
		{
			name: "authoritative legacy resolves publishes and acks", authoritative: true, owner: ownerLegacy,
			wantCalls: []ingressCall{{handler: "HandleDeployDispatched"}, {handler: "HandleDeploySuccessPublished"}}, wantPublishes: 1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv, bus, rec := ownerServer(t, tc.authoritative, tc.failing, &mockPRRegistry{resolveTargetID: lifecycleTargetID})
			evt := DeployEvent{Event: eventDeploySuccess, Repo: "azylman/aerial", PRNumber: 42, JobName: "brain", CommitSHA: "sha123", TargetID: lifecycleTargetID, Timestamp: time.Now().UTC()}
			_, err := srv.publishUnownedDeployEvent(ownerCtx(tc.owner), evt, false)
			if (err != nil) != tc.wantErr {
				t.Fatalf("publishUnownedDeployEvent() err = %v, wantErr %v", err, tc.wantErr)
			}
			assertOwnerRun(t, rec, bus, tc.wantCalls, tc.wantPublishes)
		})
	}
}

// TestHandleDeploySyncSuccessEffectsOwner pins sync-success deploy_success
// ownership.
func TestHandleDeploySyncSuccessEffectsOwner(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		authoritative bool
		failing       bool
		wantErr       bool
		owner         string
		wantCalls     []ingressCall
		wantPublishes int
	}{
		{name: "legacy journals then publishes", owner: ownerLegacy, wantCalls: []ingressCall{{handler: "HandleDeploySuccess", facts: "deploy_success"}}, wantPublishes: 1},
		{name: "restate routes the fact without publishing", owner: ownerRestate, wantCalls: []ingressCall{{handler: "HandleDeploySuccess", facts: "deploy_success"}}},
		{name: "authoritative restate route failure surfaces", authoritative: true, failing: true, wantErr: true, owner: ownerRestate, wantCalls: []ingressCall{{handler: "HandleDeploySuccess", facts: "deploy_success"}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv, bus, rec := ownerServer(t, tc.authoritative, tc.failing, &mockPRRegistry{})
			evt := DeployEvent{Event: eventSyncSuccess, Repo: "azylman/aerial", PRNumber: 42, CommitSHA: "sha123", TargetID: lifecycleTargetID}
			err := srv.handleDeploySyncSuccess(ownerCtx(tc.owner), evt, false)
			if (err != nil) != tc.wantErr {
				t.Fatalf("handleDeploySyncSuccess() err = %v, wantErr %v", err, tc.wantErr)
			}
			assertOwnerRun(t, rec, bus, tc.wantCalls, tc.wantPublishes)
		})
	}
}

// TestHandleDeployFailedOrRollbackEffectsOwner pins Flux deploy_failed
// ownership: legacy journals the failure, publishes, and attaches the fact
// to the ack; restate-owned routes the failure with the fact.
func TestHandleDeployFailedOrRollbackEffectsOwner(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		authoritative bool
		failing       bool
		wantErr       bool
		owner         string
		wantCalls     []ingressCall
		wantPublishes int
	}{
		{
			name: "legacy journals publishes and acks with the fact", owner: ownerLegacy,
			wantCalls: []ingressCall{{handler: "HandleDeployFailed"}, {handler: "HandleDeployFailed", facts: "deploy_failed"}}, wantPublishes: 1,
		},
		{name: "restate routes the failure with the fact", owner: ownerRestate, wantCalls: []ingressCall{{handler: "HandleDeployFailed", facts: "deploy_failed"}}},
		{name: "authoritative restate route failure surfaces", authoritative: true, failing: true, wantErr: true, owner: ownerRestate, wantCalls: []ingressCall{{handler: "HandleDeployFailed", facts: "deploy_failed"}}},
		{name: "authoritative legacy journal failure surfaces", authoritative: true, failing: true, wantErr: true, owner: ownerLegacy, wantCalls: []ingressCall{{handler: "HandleDeployFailed"}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv, bus, rec := ownerServer(t, tc.authoritative, tc.failing, &mockPRRegistry{deployFailedUpdated: true, deployFailedTargetID: lifecycleTargetID})
			evt := DeployEvent{Event: eventDeployFailed, Repo: "azylman/aerial", PRNumber: 42, CommitSHA: "sha123", TargetID: lifecycleTargetID}
			err := srv.handleDeployFailedOrRollback(ownerCtx(tc.owner), evt, "prefix")
			if (err != nil) != tc.wantErr {
				t.Fatalf("handleDeployFailedOrRollback() err = %v, wantErr %v", err, tc.wantErr)
			}
			assertOwnerRun(t, rec, bus, tc.wantCalls, tc.wantPublishes)
		})
	}
}

// TestHandleDeployAttemptFailureEffectsOwner pins the final-attempt split:
// a restate-owned attempt arms PRObject's deadline on every failure, a
// legacy final attempt runs synapse's deploy_failed path, and a legacy
// non-final attempt does nothing.
func TestHandleDeployAttemptFailureEffectsOwner(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		authoritative bool
		failing       bool
		wantErr       bool
		owner         string
		final         bool
		wantCalls     []ingressCall
		wantPublishes int
	}{
		{name: "restate attempt arms the deadline", owner: ownerRestate, wantCalls: []ingressCall{{handler: "HandleDeployAttemptFailed", facts: "deploy_failed"}}},
		{
			name: "legacy final attempt fails the deploy", owner: ownerLegacy, final: true,
			wantCalls: []ingressCall{{handler: "HandleDeployFailed"}, {handler: "HandleDeployFailed", facts: "deploy_failed"}}, wantPublishes: 1,
		},
		{name: "legacy non-final attempt does nothing", owner: ownerLegacy},
		{name: "authoritative restate report failure is logged", authoritative: true, failing: true, owner: ownerRestate, wantCalls: []ingressCall{{handler: "HandleDeployAttemptFailed", facts: "deploy_failed"}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv, bus, rec := ownerServer(t, tc.authoritative, tc.failing, &mockPRRegistry{deployFailedUpdated: true, resolveTargetID: lifecycleTargetID, resolveMergeSHA: "m1"})
			srv.handleDeployAttemptFailure(ownerCtx(tc.owner), "azylman/aerial", 42, "main", "sha123", lifecycleTargetID, tc.final, errors.New("promote failed"))
			assertOwnerRun(t, rec, bus, tc.wantCalls, tc.wantPublishes)
		})
	}
}

// TestCompleteKustomizationRequestAuthoritativeEffectsOwner pins watcher
// deploy_success ownership on the authoritative path.
func TestCompleteKustomizationRequestAuthoritativeEffectsOwner(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		authoritative bool
		failing       bool
		wantErr       bool
		owner         string
		wantCalls     []ingressCall
		wantPublishes int
	}{
		{
			name: "legacy resolves publishes and acks", owner: ownerLegacy,
			wantCalls: []ingressCall{{handler: "HandleDeployDispatched"}, {handler: "HandleRolloutProgress"}, {handler: "HandleDeploySuccessPublished"}}, wantPublishes: 1,
		},
		{
			name: "restate routes progress and the fact without publishing", owner: ownerRestate,
			wantCalls: []ingressCall{{handler: "HandleRolloutProgress"}, {handler: "HandleDeploySuccess", facts: "deploy_success"}},
		},
		{name: "restate progress failure surfaces", failing: true, wantErr: true, owner: ownerRestate, wantCalls: []ingressCall{{handler: "HandleRolloutProgress"}}},
		{name: "legacy verdict failure surfaces", failing: true, wantErr: true, owner: ownerLegacy, wantCalls: []ingressCall{{handler: "HandleDeployDispatched"}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv, bus, rec := ownerServer(t, true, tc.failing, &mockPRRegistry{})
			pr := DeployingPR{Repo: "azylman/aerial", PRNumber: 42, HeadSHA: "h1", MergeSHA: "m1", TargetID: lifecycleTargetID}
			err := srv.completeKustomizationRequestAuthoritative(ownerCtx(tc.owner), pr, "eri")
			if (err != nil) != tc.wantErr {
				t.Fatalf("completeKustomizationRequestAuthoritative() err = %v, wantErr %v", err, tc.wantErr)
			}
			assertOwnerRun(t, rec, bus, tc.wantCalls, tc.wantPublishes)
		})
	}
}

// TestHandleImagePromotionDeployFailedEffectsOwner pins image-promotion
// deploy_failed ownership.
func TestHandleImagePromotionDeployFailedEffectsOwner(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		authoritative bool
		failing       bool
		wantErr       bool
		owner         string
		wantCalls     []ingressCall
		wantPublishes int
	}{
		{
			name: "legacy journals publishes and acks with the fact", owner: ownerLegacy,
			wantCalls: []ingressCall{{handler: "HandleDeployFailed"}, {handler: "HandleDeployFailed", facts: "deploy_failed"}}, wantPublishes: 1,
		},
		{name: "restate routes the failure with the fact", owner: ownerRestate, wantCalls: []ingressCall{{handler: "HandleDeployFailed", facts: "deploy_failed"}}},
		{name: "authoritative restate route failure surfaces", authoritative: true, failing: true, wantErr: true, owner: ownerRestate, wantCalls: []ingressCall{{handler: "HandleDeployFailed", facts: "deploy_failed"}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv, bus, rec := ownerServer(t, tc.authoritative, tc.failing, &mockPRRegistry{deployFailedUpdated: true, resolveMergeSHA: "m1"})
			err := srv.handleImagePromotionDeployFailed(ownerCtx(tc.owner), "azylman/aerial", 42, "main", "sha123", "", errors.New("boom"))
			if (err != nil) != tc.wantErr {
				t.Fatalf("handleImagePromotionDeployFailed() err = %v, wantErr %v", err, tc.wantErr)
			}
			assertOwnerRun(t, rec, bus, tc.wantCalls, tc.wantPublishes)
		})
	}
}

// TestPublishLegacyFact pins that publishLegacyFact publishes the
// fact and skips the [synapse-effect] record without a bus.
func TestPublishLegacyFact(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		withBus       bool
		wantPublishes int
	}{
		{name: "bus publishes the fact", withBus: true, wantPublishes: 1},
		{name: "missing bus publishes nothing"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bus := &mockEventBus{}
			srv := NewRouterServer(fixedConfig(lifecycleCfg()), nil)
			if tc.withBus {
				srv.bus = bus
			}
			fact := pubsub.PRLifecycleEvent{Event: prConflict, Repo: "aerial", PRNumber: 42, HeadSHA: "h1"}
			if err := srv.publishLegacyFact(context.Background(), fact); err != nil {
				t.Fatalf("publishLegacyFact() err = %v", err)
			}
			bus.mu.Lock()
			defer bus.mu.Unlock()
			if len(bus.publishedPREvents) != tc.wantPublishes {
				t.Fatalf("published = %d, want %d", len(bus.publishedPREvents), tc.wantPublishes)
			}
		})
	}
}

// TestRecoverCheckRunEffectsOwner pins CI-recovery ownership: both owners
// journal the merged HandleCIRecovered verdict with the synchronize fact,
// only legacy publishes it, and sync or journal failures surface.
func TestRecoverCheckRunEffectsOwner(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		authoritative bool
		failing       bool
		syncErr       error
		wantErr       bool
		owner         string
		wantCalls     []ingressCall
		wantPublishes int
	}{
		{name: "legacy journals then publishes", owner: ownerLegacy, wantCalls: []ingressCall{{handler: "HandleCIRecovered", facts: "synchronize"}}, wantPublishes: 1},
		{name: "restate routes the fact without publishing", owner: ownerRestate, wantCalls: []ingressCall{{handler: "HandleCIRecovered", facts: "synchronize"}}},
		{name: "authoritative journal failure surfaces", authoritative: true, failing: true, wantErr: true, owner: ownerRestate, wantCalls: []ingressCall{{handler: "HandleCIRecovered", facts: "synchronize"}}},
		{name: "registry sync failure surfaces", syncErr: errors.New("db down"), wantErr: true, owner: ownerLegacy},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv, bus, rec := ownerServer(t, tc.authoritative, tc.failing, &mockPRRegistry{err: tc.syncErr, syncTargetID: lifecycleTargetID})
			res := &ResolvedGitHubEvent{Repo: "azylman/aerial", PRNumber: 42, HeadSHA: "h1", Branch: "feat/x", CheckName: "lint"}
			err := srv.recoverCheckRun(ownerCtx(tc.owner), res)
			if (err != nil) != tc.wantErr {
				t.Fatalf("recoverCheckRun() err = %v, wantErr %v", err, tc.wantErr)
			}
			assertOwnerRun(t, rec, bus, tc.wantCalls, tc.wantPublishes)
		})
	}
}
