package router

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"

	"github.com/azylman/aerial/services/synapse/internal/restateclient"
)

// effectCaptureServer is a declarative Restate ingress fake that records
// every decoded PREvent body and acknowledges with 202.
type effectCaptureServer struct {
	mu     sync.Mutex
	events []restateclient.PREvent
}

func (c *effectCaptureServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var ev restateclient.PREvent
	if err := json.NewDecoder(r.Body).Decode(&ev); err == nil {
		c.mu.Lock()
		c.events = append(c.events, ev)
		c.mu.Unlock()
	}
	w.WriteHeader(http.StatusAccepted)
}

func (c *effectCaptureServer) Events() []restateclient.PREvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]restateclient.PREvent(nil), c.events...)
}

// TestCollaboratorConflictCarriesRenderedDM pins the eri effect ownership
// split at the collaborator conflict call site: the routed
// HandleCollaboratorAlert event always carries the rendered DM, a legacy
// event still sends it from synapse, and a restate-owned event leaves the
// send to PRObject.
func TestCollaboratorConflictCarriesRenderedDM(t *testing.T) {
	t.Parallel()
	pr := GitHubPullRequest{
		Number: 42,
		Title:  "Conflict PR",
		State:  "open",
		User: struct {
			Login string `json:"login"`
		}{Login: "collab-user"},
		Head: GitHubPRRef{Ref: "feat/conflict", SHA: "sha-conflict-9"},
	}
	wantMsg := formatCollaboratorConflictMessage("<@123456>", "azylman/aerial", 42, "Conflict PR", "feat/conflict", "sha-conflict-9")
	wantEffects := []restateclient.EriEffect{{Kind: "eri_direct_message", ChannelID: "111111111111111111", Content: wantMsg}}

	tests := []struct {
		name      string
		owner     string
		wantOwner string
		wantDMs   int
	}{
		{name: "legacy event sends the DM and carries it", owner: "legacy", wantOwner: "legacy", wantDMs: 1},
		{name: "restate event carries the DM and skips the send", owner: "restate", wantOwner: "restate", wantDMs: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			capture := &effectCaptureServer{}
			ts := httptest.NewServer(capture)
			t.Cleanup(ts.Close)
			cfg := lifecycleCfg()
			cfg.RestateAuthoritative = true
			cfg.RestateURL = ts.URL
			cfg.CollaboratorCIAlerts = []CollaboratorCIAlertRule{{
				Repo:      "azylman/aerial",
				ChannelID: "111111111111111111",
				Authors:   map[string]string{"collab-user": "<@123456>"},
			}}
			disp := &mockOutboundDispatcher{}
			srv := newLifecycleServer(cfg, &mockEventBus{}, nil, disp)
			ctx := withEffectsOwner(context.Background(), tc.owner)
			if err := srv.notifyCollaboratorConflict(ctx, "azylman/aerial", pr); err != nil {
				t.Fatalf("notifyCollaboratorConflict() err = %v", err)
			}
			events := capture.Events()
			if len(events) != 1 {
				t.Fatalf("routed events = %d, want 1", len(events))
			}
			if events[0].EffectsOwner != tc.wantOwner {
				t.Errorf("effects_owner = %q, want %q", events[0].EffectsOwner, tc.wantOwner)
			}
			if !reflect.DeepEqual(events[0].EriEffects, wantEffects) {
				t.Errorf("eri_effects = %+v, want %+v", events[0].EriEffects, wantEffects)
			}
			if got := len(disp.DirectMessageCalls()); got != tc.wantDMs {
				t.Errorf("DirectMessageCalls() = %d, want %d", got, tc.wantDMs)
			}
		})
	}
}
