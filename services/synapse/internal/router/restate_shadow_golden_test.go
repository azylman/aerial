package router

// Shadow goldens, legacy side: testdata/prlifecycle/shadow/ pins the wire
// synapse sends Restate with effects_owner=legacy while it still writes
// pr_registry itself. This test drives the legacy watcher completion over
// each case's steps against a capturing fake Restate ingress and asserts it
// sends exactly the golden shadow events. The prstate module's
// shadow_golden_test.go replays the same events through PRObject and
// asserts it lands on the row the legacy registry writes.

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// shadowGoldenDir holds the shared legacy-owned wire goldens.
const shadowGoldenDir = "../../../../testdata/prlifecycle/shadow/"

// shadowGoldenStep is one watcher completion of one Kustomization.
type shadowGoldenStep struct {
	Kustomization      string   `json:"kustomization"`
	ReadyJobs          []string `json:"ready_jobs"`
	LegacyTransitioned bool     `json:"legacy_transitioned"`
}

// shadowGoldenSend is one Restate invocation synapse sends.
type shadowGoldenSend struct {
	Object  string         `json:"object"`
	Handler string         `json:"handler"`
	Event   map[string]any `json:"event"`
}

// shadowGoldenCase is one legacy-owned sequence; prstate-only fields are
// ignored. PR, when set, overrides the file-level deploying PR.
type shadowGoldenCase struct {
	Name   string             `json:"name"`
	PR     *DeployingPR       `json:"pr"`
	Steps  []shadowGoldenStep `json:"steps"`
	Shadow []shadowGoldenSend `json:"shadow"`
}

// rolloutGoldenRow is one watcher-completion golden case with its
// deploying PR resolved.
type rolloutGoldenRow struct {
	golden shadowGoldenCase
	pr     DeployingPR
}

// loadRolloutGoldens flattens every case of the named watcher-completion
// goldens into one table, names prefixed by file.
func loadRolloutGoldens(t *testing.T, files ...string) []rolloutGoldenRow {
	t.Helper()
	rows := make([]rolloutGoldenRow, 0, len(files))
	for _, f := range files {
		raw, err := os.ReadFile(shadowGoldenDir + f)
		if err != nil {
			t.Fatal(err)
		}
		var g shadowGolden
		if err := json.Unmarshal(raw, &g); err != nil {
			t.Fatal(err)
		}
		if len(g.Cases) == 0 {
			t.Fatalf("no cases in %s", f)
		}
		for _, c := range g.Cases {
			pr := g.PR
			if c.PR != nil {
				pr = *c.PR
			}
			c.Name = f + "/" + c.Name
			rows = append(rows, rolloutGoldenRow{golden: c, pr: pr})
		}
	}
	return rows
}

// shadowGolden is one shadow golden file.
type shadowGolden struct {
	PR    DeployingPR        `json:"pr"`
	Cases []shadowGoldenCase `json:"cases"`
}

// normalizeShadowWire drops zero values and fact timestamps so a captured
// body compares against a golden event that lists only meaningful fields.
func normalizeShadowWire(v any) any {
	switch x := v.(type) {
	case map[string]any:
		out := map[string]any{}
		for k, val := range x {
			if k == "occurred_at" {
				continue
			}
			n := normalizeShadowWire(val)
			switch nv := n.(type) {
			case nil:
				continue
			case string:
				if nv == "" {
					continue
				}
			case float64:
				if nv == 0 {
					continue
				}
			case bool:
				if !nv {
					continue
				}
			case []any:
				if len(nv) == 0 {
					continue
				}
			}
			out[k] = n
		}
		return out
	case []any:
		out := make([]any, 0, len(x))
		for _, e := range x {
			out = append(out, normalizeShadowWire(e))
		}
		return out
	default:
		return v
	}
}

// capturedShadowSends decodes every captured invocation into golden form.
func capturedShadowSends(t *testing.T, shot *shadowCapture) []shadowGoldenSend {
	t.Helper()
	shot.mu.Lock()
	defer shot.mu.Unlock()
	out := make([]shadowGoldenSend, 0, len(shot.paths))
	for i, p := range shot.paths {
		parts := strings.Split(strings.Trim(p, "/"), "/")
		if len(parts) < 3 {
			t.Fatalf("unexpected shadow path %q", p)
		}
		var ev map[string]any
		if err := json.Unmarshal(shot.bodies[i], &ev); err != nil {
			t.Fatalf("decode shadow body: %v", err)
		}
		out = append(out, shadowGoldenSend{Object: parts[0], Handler: parts[2], Event: ev})
	}
	return out
}

// TestShadowRolloutReadyJobsGolden pins that every legacy-owned rollout
// completion carries the ready jobs the legacy registry recorded, and that
// the completing tick journals rollout progress before deploy_success.
// merged-stale-head-deploy.json pins that those sends carry the row head
// and merge SHA even when PRObject's head is stale (prstate's
// TestShadowMergedStaleHeadDeployGolden replays them).
func TestShadowRolloutReadyJobsGolden(t *testing.T) {
	t.Parallel()
	tests := loadRolloutGoldens(t, "rollout-ready-jobs.json", "merged-stale-head-deploy.json")
	for _, tc := range tests {
		t.Run(tc.golden.Name, func(t *testing.T) {
			t.Parallel()
			shot, srv := newShadowCapture()
			defer srv.Close()
			cfg := Config{RestateURL: srv.URL}
			reg := &mockPRRegistry{resolveTargetID: tc.pr.TargetID, resolveHeadSHA: tc.pr.HeadSHA, resolveMergeSHA: tc.pr.MergeSHA}
			r := newTestRouter(cfg, srv.Client(), reg, &mockOutboundDispatcher{}, &mockEventBus{})
			r.SetRestateShadow(newShadowRestateReporter(cfg, srv.Client()))
			for _, step := range tc.golden.Steps {
				reg.markKustomizationsRes = step.LegacyTransitioned
				if err := r.completeKustomizationRequest(context.Background(), tc.pr, step.Kustomization, step.ReadyJobs); err != nil {
					t.Fatalf("completeKustomizationRequest(%s) err = %v", step.Kustomization, err)
				}
			}
			got := capturedShadowSends(t, shot)
			if len(got) != len(tc.golden.Shadow) {
				t.Fatalf("shadow sends = %d, want %d: %+v", len(got), len(tc.golden.Shadow), got)
			}
			for i := range got {
				gotJSON, _ := json.Marshal([]any{got[i].Object, got[i].Handler, normalizeShadowWire(got[i].Event)})
				wantJSON, _ := json.Marshal([]any{tc.golden.Shadow[i].Object, tc.golden.Shadow[i].Handler, normalizeShadowWire(tc.golden.Shadow[i].Event)})
				if !bytes.Equal(gotJSON, wantJSON) {
					t.Errorf("shadow send %d:\n got  %s\n want %s", i, gotJSON, wantJSON)
				}
			}
		})
	}
}
