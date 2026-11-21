package prstate

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
)

func wireKeys(t *testing.T, v any) string {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("json.Marshal() = %v, want nil", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("json.Unmarshal() = %v, want nil", err)
	}
	keys := make([]string, 0, len(decoded))
	for k := range decoded {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return strings.Join(keys, ",")
}

// TestContractWireFormat pins the snake_case wire format the synapse
// ingress invoker emits. Both sides assert the identical key set, so drift
// on either side fails loudly before consumer cutover.
func TestContractWireFormat(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		got  string
		want string
	}{
		{name: "pr event wire keys", got: wireKeys(t, PREvent{}), want: `branch,build_id,check_name,event,gate_generation,head_sha,job_name,merge_sha,pr_number,reopen_epoch,repo,title,unmanaged`},
		{name: "repo event wire keys", got: wireKeys(t, RepoEvent{}), want: `generation,head_sha,main_sha,pr_number,repo`},
		{name: "stamped pr event wire keys", got: wireKeys(t, PREvent{EffectsOwner: EffectsOwnerLegacy}), want: `branch,build_id,check_name,effects_owner,event,gate_generation,head_sha,job_name,merge_sha,pr_number,reopen_epoch,repo,title,unmanaged`},
		{name: "registry payload wire keys", got: wireKeys(t, PREvent{TargetID: "t", CheckState: "passed", Jobs: []string{"a"}, DeployedJobs: []string{"a"}, JobState: "running", KustomizationRequests: map[string]KustomizationRequest{"k": {}}, BeforeSHA: "b", DeployedKustomizations: []string{"k"}, Banner: "b", Acknowledged: true, BuildMetadata: &BuildMetadata{}}), want: `acknowledged,banner,before_sha,branch,build_id,build_metadata,check_name,check_state,deployed_jobs,deployed_kustomizations,event,gate_generation,head_sha,job_name,job_state,jobs,kustomization_requests,merge_sha,pr_number,reopen_epoch,repo,target_id,title,unmanaged`},
		{name: "stamped repo event wire keys", got: wireKeys(t, RepoEvent{EffectsOwner: EffectsOwnerRestate}), want: `effects_owner,generation,head_sha,main_sha,pr_number,repo`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.got != tc.want {
				t.Errorf("wire keys = %q, want %q", tc.got, tc.want)
			}
		})
	}
}

func TestParseEffectsOwner(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		raw  string
		want EffectsOwner
	}{
		{name: "missing stamp is restate", raw: "", want: EffectsOwnerRestate},
		{name: "legacy stays legacy", raw: "legacy", want: EffectsOwnerLegacy},
		{name: "restate with padding and case", raw: " RESTATE ", want: EffectsOwnerRestate},
		{name: "unknown stamp is restate", raw: "both", want: EffectsOwnerRestate},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := ParseEffectsOwner(tc.raw); got != tc.want {
				t.Errorf("ParseEffectsOwner(%q) = %q, want %q", tc.raw, got, tc.want)
			}
		})
	}
}

func TestPREventUnmarshalEffectsOwner(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		payload   string
		wantErr   bool
		wantOwner EffectsOwner
		wantPR    int
		wantAgent string
	}{
		{name: "missing stamp decodes restate", payload: `{"repo":"azylman/aerial","pr_number":7,"agent":"aerial"}`, wantOwner: EffectsOwnerRestate, wantPR: 7, wantAgent: "aerial"},
		{name: "restate stamp decodes restate", payload: `{"repo":"azylman/aerial","pr_number":8,"effects_owner":"restate"}`, wantOwner: EffectsOwnerRestate, wantPR: 8},
		{name: "unknown stamp decodes restate", payload: `{"repo":"azylman/aerial","pr_number":9,"effects_owner":"synapse"}`, wantOwner: EffectsOwnerRestate, wantPR: 9},
		{name: "legacy stamp decodes legacy", payload: `{"repo":"azylman/aerial","pr_number":10,"effects_owner":" Legacy "}`, wantOwner: EffectsOwnerLegacy, wantPR: 10},
		{name: "malformed payload errors", payload: `{"pr_number":"seven"}`, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var ev PREvent
			err := json.Unmarshal([]byte(tc.payload), &ev)
			if (err != nil) != tc.wantErr {
				t.Fatalf("json.Unmarshal() err = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if ev.EffectsOwner != tc.wantOwner || ev.PRNumber != tc.wantPR || ev.Agent != tc.wantAgent {
				t.Errorf("decoded = owner %q pr %d agent %q, want owner %q pr %d agent %q", ev.EffectsOwner, ev.PRNumber, ev.Agent, tc.wantOwner, tc.wantPR, tc.wantAgent)
			}
		})
	}
}

func TestRepoEventUnmarshalEffectsOwner(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		payload   string
		wantErr   bool
		wantOwner EffectsOwner
		wantSHA   string
	}{
		{name: "missing stamp decodes restate", payload: `{"repo":"azylman/aerial","main_sha":"abc","agent":"aerial"}`, wantOwner: EffectsOwnerRestate, wantSHA: "abc"},
		{name: "restate stamp decodes restate", payload: `{"repo":"azylman/aerial","main_sha":"def","effects_owner":"Restate"}`, wantOwner: EffectsOwnerRestate, wantSHA: "def"},
		{name: "legacy stamp decodes legacy", payload: `{"repo":"azylman/aerial","main_sha":"0a1","effects_owner":"legacy"}`, wantOwner: EffectsOwnerLegacy, wantSHA: "0a1"},
		{name: "malformed payload errors", payload: `{"generation":"one"}`, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var ev RepoEvent
			err := json.Unmarshal([]byte(tc.payload), &ev)
			if (err != nil) != tc.wantErr {
				t.Fatalf("json.Unmarshal() err = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if ev.EffectsOwner != tc.wantOwner || ev.MainSHA != tc.wantSHA {
				t.Errorf("decoded = owner %q sha %q, want owner %q sha %q", ev.EffectsOwner, ev.MainSHA, tc.wantOwner, tc.wantSHA)
			}
		})
	}
}
