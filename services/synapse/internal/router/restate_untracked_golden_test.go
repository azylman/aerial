package router

// Untracked-upstream golden, synapse side: testdata/prlifecycle/shadow/
// untracked-upstream-kustomization.json pins that an upstream Flux source
// commit resolved to a PR nobody registered (kubernetes-sigs/gateway-api#5433)
// never starts a rollout. Legacy patches the Kustomization once per
// delivery and keeps no row; the restate-owned path patches the same way
// when PRObject's deploy view does not find the PR, and sends PRObject
// nothing. The prstate module's TestShadowUntrackedUpstreamGolden replays
// the PRObject side of the same file.

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"reflect"
	"testing"
)

// untrackedOwnedEvent is one restate-owned deploy alert for the upstream PR.
type untrackedOwnedEvent struct {
	Name       string `json:"name"`
	Event      string `json:"event"`
	SourceKind string `json:"source_kind"`
	JobName    string `json:"job_name"`
}

// untrackedSynapseGolden is the synapse view of the untracked golden;
// prstate-only cases are ignored.
type untrackedSynapseGolden struct {
	PR struct {
		Repo     string `json:"repo"`
		PRNumber int    `json:"pr_number"`
	} `json:"pr"`
	Kustomizations []fluxKustomizationRef `json:"kustomizations"`
	LegacyEffects  []parityEffect         `json:"legacy_effects"`
	OwnedEvents    []untrackedOwnedEvent  `json:"owned_events"`
}

func loadUntrackedGolden(t *testing.T) untrackedSynapseGolden {
	t.Helper()
	raw, err := os.ReadFile(shadowGoldenDir + "untracked-upstream-kustomization.json")
	if err != nil {
		t.Fatal(err)
	}
	var g untrackedSynapseGolden
	if err := json.Unmarshal(raw, &g); err != nil {
		t.Fatal(err)
	}
	return g
}

// TestShadowUntrackedUpstreamLegacyDeferGolden pins the legacy side: each
// hourly delivery for the upstream PR patches its Kustomization once, logs
// one [synapse-effect] flux_reconcile under the golden match key, publishes
// no deploy_started (no registry row), and journals the request to PRObject
// without a head SHA.
func TestShadowUntrackedUpstreamLegacyDeferGolden(t *testing.T) {
	t.Parallel()
	g := loadUntrackedGolden(t)
	tests := []struct {
		name       string
		deliveries int
	}{
		{name: "one delivery patches once", deliveries: 1},
		{name: "hourly redelivery patches again and still tracks nothing", deliveries: 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			shot, srv := newShadowCapture()
			defer srv.Close()
			cfg := Config{RestateURL: srv.URL}
			reg := &mockPRRegistry{skipDeployingTransition: true}
			disp := &mockOutboundDispatcher{repoKustomizations: map[string][]fluxKustomizationRef{g.PR.Repo: g.Kustomizations}}
			bus := &mockEventBus{}
			r := newTestRouter(cfg, srv.Client(), reg, disp, bus)
			r.SetRestateShadow(newShadowRestateReporter(cfg, srv.Client()))
			sink := &parityEffectSink{}
			ctx := context.WithValue(context.Background(), parityEffectSinkKey{}, sink)
			var wantEffects []parityEffect
			var wantPatches []fluxKustomizationRef
			for range tc.deliveries {
				deferred, err := r.deferPRToWatcher(ctx, g.PR.Repo, g.PR.PRNumber)
				if err != nil || !deferred {
					t.Fatalf("deferPRToWatcher() = (%v, %v), want (true, nil)", deferred, err)
				}
				wantEffects = append(wantEffects, g.LegacyEffects...)
				wantPatches = append(wantPatches, g.Kustomizations...)
			}
			if got := sink.snapshot(); !reflect.DeepEqual(got, wantEffects) {
				t.Errorf("[synapse-effect] lines = %+v, want %+v", got, wantEffects)
			}
			if got := disp.ReconcileCalls(); !reflect.DeepEqual(got, wantPatches) {
				t.Errorf("reconciles = %+v, want %+v", got, wantPatches)
			}
			if len(bus.publishedPREvents) != 0 {
				t.Errorf("published lifecycle facts = %+v, want none", bus.publishedPREvents)
			}
			sends := capturedShadowSends(t, shot)
			if len(sends) != tc.deliveries {
				t.Fatalf("shadow sends = %d, want %d", len(sends), tc.deliveries)
			}
			for _, s := range sends {
				wire, _ := normalizeShadowWire(s.Event).(map[string]any)
				if _, hasHead := wire["head_sha"]; s.Handler != "HandleKustomizationRequested" || hasHead {
					t.Errorf("shadow send = %s %+v, want HandleKustomizationRequested without head_sha", s.Handler, s.Event)
				}
			}
		})
	}
}

// TestShadowUntrackedUpstreamOwnedGolden pins the restate-owned side: when
// PRObject's deploy view does not find the upstream PR, a deploy alert
// patches its Kustomizations once and sends PRObject nothing, so no rollout
// is tracked, and the watcher reports no pending rollout for it.
func TestShadowUntrackedUpstreamOwnedGolden(t *testing.T) {
	t.Parallel()
	g := loadUntrackedGolden(t)
	if len(g.OwnedEvents) == 0 {
		t.Fatal("no owned events in the untracked golden")
	}
	tests := g.OwnedEvents
	for _, tc := range tests {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			h := newOwnedHarness(t, Config{SystemChannelID: ownedSystem}, &ownedIngress{sendCode: http.StatusAccepted, callCode: http.StatusOK, callBody: "{}", handlerBodies: map[string]string{
				"GetDeployView": `{"found":false}`,
				"ListRollouts":  `{"rollouts":[{"repo":"kubernetes-sigs/gateway-api","pr_number":5433}]}`,
			}}, "{}")
			h.disp.repoKustomizations = map[string][]fluxKustomizationRef{g.PR.Repo: g.Kustomizations}
			evt := DeployEvent{Event: tc.Event, SourceKind: tc.SourceKind, JobName: tc.JobName, Repo: g.PR.Repo, PRNumber: g.PR.PRNumber, CommitSHA: "c0ffee5433"}
			if _, err := h.srv.ProcessDeployEvent(ownedCtx(), evt); err != nil {
				t.Fatalf("ProcessDeployEvent() err = %v", err)
			}
			if got := h.disp.ReconcileCalls(); !reflect.DeepEqual(got, g.Kustomizations) {
				t.Errorf("reconciles = %+v, want %+v", got, g.Kustomizations)
			}
			assertOwnedDeploy(t, h, nil, nil, len(g.Kustomizations))
			pending, err := h.srv.HasPendingRollouts(ownedCtx())
			if err != nil || pending {
				t.Errorf("HasPendingRollouts() = (%v, %v), want (false, nil)", pending, err)
			}
		})
	}
}
