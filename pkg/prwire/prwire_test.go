package prwire

import (
	"bytes"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
)

// orderedKeys returns the top-level JSON object keys of v in marshaled
// order, comma-joined. Field order is load-bearing: prstate journals these
// bytes and synapse digests them into send Idempotency-Keys.
func orderedKeys(t *testing.T, v any) string {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("json.Marshal() = %v, want nil", err)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	if _, err := dec.Token(); err != nil {
		t.Fatalf("open object: %v", err)
	}
	var keys []string
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			t.Fatalf("read key: %v", err)
		}
		key, ok := tok.(string)
		if !ok {
			t.Fatalf("key token = %v, want string", tok)
		}
		keys = append(keys, key)
		var skip json.RawMessage
		if err := dec.Decode(&skip); err != nil {
			t.Fatalf("read value: %v", err)
		}
	}
	return strings.Join(keys, ",")
}

func TestWireKeyOrder(t *testing.T) {
	t.Parallel()
	pr := 7
	kreq := map[string]KustomizationRequest{"k": {Namespace: "flux-system", RequestedAt: "t"}}
	fact := []pubsub.PRLifecycleEvent{{Event: "opened", OccurredAt: time.Unix(0, 0).UTC()}}
	policy := &AutoUpdatePolicy{Authors: []string{"a"}, DefaultOwner: true, Collaborators: []CollaboratorAlert{{Author: "a"}}}
	meta := &BuildMetadata{Kind: BuildMetadataFinal}
	tests := []struct {
		name string
		v    any
		want string
	}{
		{name: "zero pr event keeps the frozen key set with agent omitted", v: PREvent{}, want: "event,repo,pr_number,head_sha,merge_sha,check_name,job_name,branch,title,gate_generation,reopen_epoch,unmanaged,build_id"},
		{name: "full pr event puts agent fourth and kustomization and auto_update after build_id", v: PREvent{Agent: "agent-alpha", Kustomization: "k", AutoUpdate: policy, BeforeSHA: "b", TargetID: "t", CheckState: "passed", Jobs: []string{"j"}, DeployedJobs: []string{"j"}, JobState: "running", KustomizationRequests: kreq, DeployedKustomizations: []string{"k"}, Banner: "b", BuildMetadata: meta, EriEffects: []EriEffect{{Kind: EffectEriDirectMessage}}, Facts: fact, ExpectedJobs: []string{"j"}, ObservedReconcileAt: "t"}, want: "event,repo,pr_number,agent,head_sha,merge_sha,check_name,job_name,branch,title,gate_generation,reopen_epoch,unmanaged,build_id,kustomization,auto_update,before_sha,target_id,check_state,jobs,deployed_jobs,job_state,kustomization_requests,deployed_kustomizations,banner,build_metadata,eri_effects,facts,expected_jobs,observed_reconcile_at"},
		{name: "zero repo event has no agent key", v: RepoEvent{}, want: "repo,main_sha,generation,pr_number,head_sha"},
		{name: "full repo event", v: RepoEvent{Kustomization: "k", CheckName: "c", RunID: 1, Attempt: 1, AutoUpdate: policy, Ref: "r", TargetID: "t", Branch: "b", Images: []string{"i"}, Jobs: []string{"j"}, KustomizationRequests: kreq, Facts: fact, DeployFailedFacts: fact}, want: "repo,main_sha,generation,pr_number,head_sha,kustomization,check_name,run_id,attempt,auto_update,ref,target_id,branch,images,jobs,kustomization_requests,facts,deploy_failed_facts"},
		{name: "sha event", v: SHAEvent{}, want: "repo,sha,handler,event"},
		{name: "full eri effect", v: EriEffect{Kind: EffectEriPrompt, ChannelID: "c", Content: "x", Effort: "low", SkipIfActive: true}, want: "kind,channel_id,content,effort,skip_if_active"},
		{name: "zero eri effect", v: EriEffect{}, want: "kind,content"},
		{name: "full auto update policy", v: *policy, want: "authors,default_owner,collaborators"},
		{name: "collaborator alert", v: CollaboratorAlert{}, want: "author,channel_id,mention"},
		{name: "kustomization request", v: KustomizationRequest{}, want: "namespace,requested_at"},
		{name: "build selection", v: BuildSelection{}, want: "service,image_name"},
		{name: "zero build result", v: BuildResult{}, want: "service,image_name,state,pushed,source_attempt"},
		{name: "full build result", v: BuildResult{ImageRefs: []string{"r"}, Digest: "d"}, want: "service,image_name,state,pushed,source_attempt,image_refs,digest"},
		{name: "zero build metadata", v: BuildMetadata{}, want: "version,kind,repository,workflow_path,run_id,run_number,run_attempt,event,source_sha,execution_sha,pr_number,selected"},
		{name: "full build metadata", v: BuildMetadata{PRNumber: &pr, Results: []BuildResult{{}}, Service: "s", ImageName: "i", State: "x", Pushed: true, SourceAttempt: 1, ImageRefs: []string{"r"}, Digest: "d"}, want: "version,kind,repository,workflow_path,run_id,run_number,run_attempt,event,source_sha,execution_sha,pr_number,selected,results,service,image_name,state,pushed,source_attempt,image_refs,digest"},
		{name: "full register request", v: RegisterRequest{Title: "t", Agent: "agent-alpha", BotID: "b", Metadata: map[string]string{"k": "v"}, FallbackTargetID: "f", EnforceOwner: true, ExpectedOwner: "agent-alpha"}, want: "repo,pr_number,branch,head_sha,target_id,title,agent,bot_id,metadata,fallback_target_id,enforce_owner,expected_owner"},
		{name: "register response", v: RegisterResponse{}, want: "target_id,agent,status"},
		{name: "full build lifecycle", v: BuildLifecycle{BuildMetadata: meta, Agent: "agent-alpha"}, want: "found,status,head_sha,merge_sha,started,has_build_metadata,build_metadata,agent"},
		{name: "full deploy view", v: DeployView{Agent: "agent-alpha", Jobs: []string{"j"}, DeployedJobs: []string{"j"}, KustomizationRequests: kreq, DeployedKustomizations: []string{"k"}, DeploySuccessPending: true, HasExpectedDeployJobs: true}, want: "found,repo,pr_number,status,branch,head_sha,merge_sha,target_id,agent,jobs,deployed_jobs,kustomization_requests,deployed_kustomizations,deploy_success_pending,has_expected_deploy_jobs"},
		{name: "rollout ref", v: RolloutRef{}, want: "repo,pr_number"},
		{name: "full retry readiness request", v: RetryReadinessRequest{Agent: "agent-alpha", EnforceOwner: true, ExpectedOwner: "agent-alpha"}, want: "merge_sha,agent,enforce_owner,expected_owner"},
		{name: "full retry readiness response", v: RetryReadinessResponse{Requests: kreq, EffectKey: "e"}, want: "status,kustomization_requests,effect_key"},
		{name: "resolve sha response", v: ResolveSHAResponse{}, want: "pr_number,branch,found"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := orderedKeys(t, tc.v); got != tc.want {
				t.Errorf("json keys = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestIsZeroSHA(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		sha  string
		want bool
	}{
		{name: "empty", sha: "", want: true},
		{name: "single zero", sha: "0", want: true},
		{name: "forty zeros", sha: "0000000000000000000000000000000000000000", want: true},
		{name: "trailing one", sha: "0000000000000000000000000000000000000001", want: false},
		{name: "real short sha", sha: "abc123", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := IsZeroSHA(tc.sha); got != tc.want {
				t.Errorf("IsZeroSHA(%q) = %v, want %v", tc.sha, got, tc.want)
			}
		})
	}
}

func TestWireGoldenRoundTrip(t *testing.T) {
	t.Parallel()
	dm := PREvent{
		Repo: "azylman/mirrormere", PRNumber: 1010, HeadSHA: "def5678", CheckName: "lint",
		Branch: "feature/x", Title: "Fix lint", Unmanaged: true, Agent: "aerial",
		EriEffects: []EriEffect{{Kind: EffectEriDirectMessage, ChannelID: "111111111111111111", Content: "<@42> CI check lint failed on mirrormere#1010"}},
	}
	dmBare := dm
	dmBare.EriEffects = nil
	au := RepoEvent{
		Repo: "azylman/mirrormere", PRNumber: 1010, HeadSHA: "def5678", CheckName: "CI Gate",
		AutoUpdate: &AutoUpdatePolicy{
			Authors: []string{"aerial-bot", "dependabot[bot]"}, DefaultOwner: true,
			Collaborators: []CollaboratorAlert{{Author: "octo", ChannelID: "111111111111111111", Mention: "<@42>"}},
		},
	}
	tests := []struct {
		name    string
		fixture string
		want    any
		bare    any
	}{
		{name: "collaborator DM pr event", fixture: "eri_effect_wire.json", want: dm, bare: dmBare},
		{name: "auto-update repo event", fixture: "github_effect_wire.json", want: au, bare: au},
	}
	toMap := func(t *testing.T, raw []byte) map[string]any {
		t.Helper()
		var m map[string]any
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatalf("decode: %v", err)
		}
		return m
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			raw, err := os.ReadFile("../../testdata/prlifecycle/" + tc.fixture)
			if err != nil {
				t.Fatalf("read fixture: %v", err)
			}
			var fx struct {
				Event json.RawMessage `json:"event"`
			}
			if err = json.Unmarshal(raw, &fx); err != nil {
				t.Fatalf("decode fixture: %v", err)
			}
			wantMap := toMap(t, fx.Event)

			got, err := json.Marshal(tc.want)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			if gm := toMap(t, got); !reflect.DeepEqual(gm, wantMap) {
				t.Errorf("marshal = %v, want %v", gm, wantMap)
			}

			decoded := reflect.New(reflect.TypeOf(tc.want))
			if err = json.Unmarshal(fx.Event, decoded.Interface()); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if !reflect.DeepEqual(decoded.Elem().Interface(), tc.want) {
				t.Errorf("unmarshal = %+v, want %+v", decoded.Elem().Interface(), tc.want)
			}

			delete(wantMap, "eri_effects")
			bare, err := json.Marshal(tc.bare)
			if err != nil {
				t.Fatalf("marshal bare: %v", err)
			}
			if bm := toMap(t, bare); !reflect.DeepEqual(bm, wantMap) {
				t.Errorf("effect-free marshal = %v, want %v", bm, wantMap)
			}
		})
	}
}
