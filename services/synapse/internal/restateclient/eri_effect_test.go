package restateclient

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"

	"github.com/azylman/aerial/pkg/prwire"
)

// fixtureDir is the shared cross-module golden fixture directory at the
// aerial repo root; prstate reads the same files.
const fixtureDir = "../../../../testdata/prlifecycle/"

func readFixture(t *testing.T, name string, out any) {
	t.Helper()
	raw, err := os.ReadFile(fixtureDir + name)
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	if err := json.Unmarshal(raw, out); err != nil {
		t.Fatalf("decode fixture %s: %v", name, err)
	}
}

type wireFixture struct {
	Fields struct {
		Repo      string `json:"repo"`
		PRNumber  int    `json:"pr_number"`
		HeadSHA   string `json:"head_sha"`
		CheckName string `json:"check_name"`
		Branch    string `json:"branch"`
		Title     string `json:"title"`
		Unmanaged bool   `json:"unmanaged"`
		Agent     string `json:"agent"`
		ChannelID string `json:"channel_id"`
		Content   string `json:"content"`
	} `json:"fields"`
	Event map[string]any `json:"event"`
}

// TestPREventEriEffectsWire pins the HandleCollaboratorAlert payload that
// carries a rendered DM: synapse marshals it key for key to the shared
// golden event prstate decodes, and an event with no effects omits the key.
func TestPREventEriEffectsWire(t *testing.T) {
	t.Parallel()
	var fx wireFixture
	readFixture(t, "eri_effect_wire.json", &fx)
	f := fx.Fields
	withEffect := prwire.PREvent{
		Repo: f.Repo, PRNumber: f.PRNumber, HeadSHA: f.HeadSHA, CheckName: f.CheckName,
		Branch: f.Branch, Title: f.Title, Unmanaged: f.Unmanaged, Agent: f.Agent,
		EriEffects: []prwire.EriEffect{{Kind: prwire.EffectEriDirectMessage, ChannelID: f.ChannelID, Content: f.Content}},
	}
	withoutEffect := withEffect
	withoutEffect.EriEffects = nil
	wantWithout := make(map[string]any, len(fx.Event))
	for k, v := range fx.Event {
		if k != "eri_effects" {
			wantWithout[k] = v
		}
	}
	tests := []struct {
		name string
		ev   prwire.PREvent
		want map[string]any
	}{
		{name: "rendered DM matches the golden event", ev: withEffect, want: fx.Event},
		{name: "no effects omits eri_effects", ev: withoutEffect, want: wantWithout},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			raw, err := json.Marshal(tc.ev)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			var got map[string]any
			if err := json.Unmarshal(raw, &got); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("wire = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestRepoEventAutoUpdateWire pins the HandleAutoUpdate payload carrying the
// rendered auto-update policy: synapse marshals it key for key to the shared
// golden event prstate decodes.
func TestRepoEventAutoUpdateWire(t *testing.T) {
	t.Parallel()
	var fx struct {
		Event map[string]any `json:"event"`
	}
	readFixture(t, "github_effect_wire.json", &fx)
	tests := []struct {
		name string
		ev   prwire.RepoEvent
		want map[string]any
	}{
		{
			name: "gate-success trigger matches the golden event",
			ev: prwire.RepoEvent{
				Repo: "azylman/mirrormere", PRNumber: 1010, HeadSHA: "def5678", CheckName: "CI Gate",
				AutoUpdate: &prwire.AutoUpdatePolicy{
					Authors: []string{"aerial-bot", "dependabot[bot]"}, DefaultOwner: true,
					Collaborators: []prwire.CollaboratorAlert{{Author: "octo", ChannelID: "111111111111111111", Mention: "<@42>"}},
				},
			},
			want: fx.Event,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			raw, err := json.Marshal(tc.ev)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			var got map[string]any
			if err := json.Unmarshal(raw, &got); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("wire = %v, want %v", got, tc.want)
			}
		})
	}
}
