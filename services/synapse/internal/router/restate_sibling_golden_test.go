package router

// Synchronize goldens, synapse side: testdata/prlifecycle/shadow/
// sibling-bot-synchronize.json pins that one synapse serving every
// configured bot processes a sibling-owned PR's synchronize on the legacy
// path (row head written, synchronize fact, one legacy-owned
// HandleSynchronized carrying the stored owner) and still drops a bot
// outside cfg.Bots. stale-head-synchronize.json pins the same legacy wire
// for a synchronize whose before SHA PRObject never saw, a late redelivery
// and a normal advance: legacy stays last-write-wins with one fact per
// webhook. The prstate module's TestShadowSiblingBotSynchronizeGolden and
// TestShadowStaleHeadSynchronizeGolden replay the PRObject side.

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

// syncGoldenFile is one synchronize golden file, synapse view.
type syncGoldenFile struct {
	Bots  []string            `json:"bots"`
	Cases []siblingGoldenCase `json:"cases"`
}

// syncGoldenRow is one synchronize golden case with its file's bots.
type syncGoldenRow struct {
	golden siblingGoldenCase
	bots   []string
}

// loadSyncGoldens flattens every case of the named synchronize goldens
// into one table, names prefixed by file.
func loadSyncGoldens(t *testing.T, files ...string) []syncGoldenRow {
	t.Helper()
	rows := make([]syncGoldenRow, 0, len(files))
	for _, f := range files {
		raw, err := os.ReadFile(shadowGoldenDir + f)
		if err != nil {
			t.Fatal(err)
		}
		var g syncGoldenFile
		if err := json.Unmarshal(raw, &g); err != nil {
			t.Fatal(err)
		}
		if len(g.Cases) == 0 {
			t.Fatalf("no cases in %s", f)
		}
		for _, c := range g.Cases {
			c.Name = f + "/" + c.Name
			rows = append(rows, syncGoldenRow{golden: c, bots: g.Bots})
		}
	}
	return rows
}

// TestShadowSiblingBotSynchronizeGolden pins the legacy side of a
// synchronize for a configured sibling bot, a bot outside cfg.Bots, and a
// stale, late or normal before SHA.
func TestShadowSiblingBotSynchronizeGolden(t *testing.T) {
	t.Parallel()
	tests := loadSyncGoldens(t, "sibling-bot-synchronize.json", "stale-head-synchronize.json")
	for _, tc := range tests {
		t.Run(tc.golden.Name, func(t *testing.T) {
			t.Parallel()
			shot, srv := newShadowCapture()
			defer srv.Close()
			cfg := Config{RestateURL: srv.URL, BotID: "aerial", Bots: map[string]BotConfig{}}
			for _, b := range tc.bots {
				cfg.Bots[b] = BotConfig{}
			}
			reg := &mockPRRegistry{
				resolveTargetID: tc.golden.PR.TargetID,
				rawMetadata:     map[string]any{"agent": tc.golden.BodyBot, "bot_id": tc.golden.BodyBot},
			}
			bus := &mockEventBus{}
			r := newTestRouter(cfg, srv.Client(), reg, &mockOutboundDispatcher{}, bus)
			r.SetRestateShadow(newShadowRestateReporter(cfg, srv.Client()))
			if _, err := r.processGitHubPullRequestEvent(context.Background(), "del-"+tc.golden.Name, siblingGoldenPayload(t, tc.golden), &ResolvedGitHubEvent{}); err != nil {
				t.Fatalf("processGitHubPullRequestEvent err = %v", err)
			}
			if got := prCallHeads(reg.syncCalls); !reflect.DeepEqual(got, tc.golden.Legacy.SyncHeads) {
				t.Errorf("UpdatePRSync heads = %v, want %v", got, tc.golden.Legacy.SyncHeads)
			}
			if got := publishedFactNames(bus); !reflect.DeepEqual(got, tc.golden.Legacy.Facts) {
				t.Errorf("published facts = %v, want %v", got, tc.golden.Legacy.Facts)
			}
			assertShadowWire(t, journaledShadowSends(t, shot), tc.golden.Shadow)
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
