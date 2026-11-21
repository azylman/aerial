package router

// Sibling-bot golden, synapse side: testdata/prlifecycle/shadow/
// sibling-bot-synchronize.json pins that one synapse serving every
// configured bot processes a sibling-owned PR's synchronize on the legacy
// path (row head written, synchronize fact, one legacy-owned
// HandleSynchronized carrying the stored owner) and still drops a bot
// outside cfg.Bots. The prstate module's TestShadowSiblingBotSynchronizeGolden
// replays the PRObject side of the same file.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"testing"
)

// siblingGoldenPR is the webhook view of one golden PR.
type siblingGoldenPR struct {
	Repo      string `json:"repo"`
	PRNumber  int    `json:"pr_number"`
	Branch    string `json:"branch"`
	HeadSHA   string `json:"head_sha"`
	BeforeSHA string `json:"before_sha"`
	TargetID  string `json:"target_id"`
	Title     string `json:"title"`
}

// siblingGoldenCase is the synapse view of one case; prstate-only fields are ignored.
type siblingGoldenCase struct {
	Name    string          `json:"name"`
	BodyBot string          `json:"body_bot"`
	PR      siblingGoldenPR `json:"pr"`
	Legacy  struct {
		SyncHeads []string `json:"sync_heads"`
		Facts     []string `json:"facts"`
	} `json:"legacy"`
	Shadow []shadowGoldenSend `json:"shadow"`
}

// siblingGoldenPayload renders the pull_request synchronize webhook body.
func siblingGoldenPayload(t *testing.T, tc siblingGoldenCase) []byte {
	t.Helper()
	body := fmt.Sprintf("Sibling PR\n\n<!-- aerial-pr:{\"target_id\":%q,\"bot_id\":%q} -->", tc.PR.TargetID, tc.BodyBot)
	raw, err := json.Marshal(map[string]any{
		"action": "synchronize",
		"number": tc.PR.PRNumber,
		"before": tc.PR.BeforeSHA,
		"pull_request": map[string]any{
			"number": tc.PR.PRNumber,
			"title":  tc.PR.Title,
			"body":   body,
			"head":   map[string]any{"ref": tc.PR.Branch, "sha": tc.PR.HeadSHA},
		},
		"repository": map[string]any{"full_name": tc.PR.Repo},
	})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// TestShadowSiblingBotSynchronizeGolden pins the legacy side of a
// synchronize for a configured sibling bot and for a bot outside cfg.Bots.
func TestShadowSiblingBotSynchronizeGolden(t *testing.T) {
	t.Parallel()
	raw, err := os.ReadFile(shadowGoldenDir + "sibling-bot-synchronize.json")
	if err != nil {
		t.Fatal(err)
	}
	var g struct {
		Bots  []string            `json:"bots"`
		Cases []siblingGoldenCase `json:"cases"`
	}
	if err := json.Unmarshal(raw, &g); err != nil {
		t.Fatal(err)
	}
	if len(g.Cases) == 0 {
		t.Fatal("no sibling golden cases")
	}
	tests := g.Cases
	for _, tc := range tests {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			shot, srv := newShadowCapture()
			defer srv.Close()
			cfg := Config{RestateURL: srv.URL, BotID: "aerial", Bots: map[string]BotConfig{}}
			for _, b := range g.Bots {
				cfg.Bots[b] = BotConfig{}
			}
			reg := &mockPRRegistry{
				resolveTargetID: tc.PR.TargetID,
				rawMetadata:     map[string]any{"agent": tc.BodyBot, "bot_id": tc.BodyBot},
			}
			bus := &mockEventBus{}
			r := newTestRouter(cfg, srv.Client(), reg, &mockOutboundDispatcher{}, bus)
			r.SetRestateShadow(newShadowRestateReporter(cfg, srv.Client()))
			if _, err := r.processGitHubPullRequestEvent(context.Background(), "del-"+tc.Name, siblingGoldenPayload(t, tc), &ResolvedGitHubEvent{}); err != nil {
				t.Fatalf("processGitHubPullRequestEvent err = %v", err)
			}
			if got := prCallHeads(reg.syncCalls); !reflect.DeepEqual(got, tc.Legacy.SyncHeads) {
				t.Errorf("UpdatePRSync heads = %v, want %v", got, tc.Legacy.SyncHeads)
			}
			if got := publishedFactNames(bus); !reflect.DeepEqual(got, tc.Legacy.Facts) {
				t.Errorf("published facts = %v, want %v", got, tc.Legacy.Facts)
			}
			assertShadowWire(t, journaledShadowSends(t, shot), tc.Shadow)
		})
	}
}

// publishedFactNames lists the aerial.pr.> events published, in order.
func publishedFactNames(bus *mockEventBus) []string {
	bus.mu.Lock()
	defer bus.mu.Unlock()
	names := make([]string, 0, len(bus.publishedPREvents))
	for _, ev := range bus.publishedPREvents {
		names = append(names, ev.Event)
	}
	return names
}

// journaledShadowSends drops the owner-lookup GetBuildLifecycle reads; only
// journaled sends are part of the shadow wire.
func journaledShadowSends(t *testing.T, shot *shadowCapture) []shadowGoldenSend {
	t.Helper()
	all := capturedShadowSends(t, shot)
	out := make([]shadowGoldenSend, 0, len(all))
	for _, s := range all {
		if s.Handler != "GetBuildLifecycle" {
			out = append(out, s)
		}
	}
	return out
}

// assertShadowWire compares captured sends to golden sends, zero values ignored.
func assertShadowWire(t *testing.T, got, want []shadowGoldenSend) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("shadow sends = %d, want %d: %+v", len(got), len(want), got)
	}
	for i := range got {
		gotJSON, _ := json.Marshal([]any{got[i].Object, got[i].Handler, normalizeShadowWire(got[i].Event)})
		wantJSON, _ := json.Marshal([]any{want[i].Object, want[i].Handler, normalizeShadowWire(want[i].Event)})
		if !bytes.Equal(gotJSON, wantJSON) {
			t.Errorf("shadow send %d:\n got  %s\n want %s", i, gotJSON, wantJSON)
		}
	}
}
