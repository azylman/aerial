package prstate

// Shadow goldens, prstate side: testdata/prlifecycle/shadow/ pins the
// legacy-owned wire synapse sent while it still wrote pr_registry. prstate
// now owns every effect (the retired ownership stamp is stripped from the
// captures), so each captured event stream replays under
// restate ownership and must still land on the hand-derived state and
// effects.
// The projection-compare fields the goldens carry (divergent, superseded,
// intended_effects) belonged to the retired shadow compare and are ignored.

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// shadowWantState is the hand-derived lifecycle state a case ends on.
type shadowWantState struct {
	Status                 string   `json:"status"`
	Jobs                   []string `json:"jobs"`
	DeployedJobs           []string `json:"deployed_jobs"`
	DeployedKustomizations []string `json:"deployed_kustomizations"`
}

// shadowCase is one legacy-owned sequence; synapse-only fields are ignored.
type shadowCase struct {
	Name      string             `json:"name"`
	Shadow    []parityInvocation `json:"shadow"`
	WantState shadowWantState    `json:"want_state"`
}

// shadowGolden is one shadow golden file.
type shadowGolden struct {
	Setup []parityInvocation `json:"setup"`
	Cases []shadowCase       `json:"cases"`
}

func loadShadowGolden(t *testing.T, name string) shadowGolden {
	t.Helper()
	raw, err := os.ReadFile(prlifecycleFixtureDir + "shadow/" + name)
	require.NoError(t, err)
	var g shadowGolden
	require.NoError(t, json.Unmarshal(raw, &g))
	require.NotEmpty(t, g.Cases)
	return g
}

// TestShadowRolloutReadyJobsGolden pins that the ready jobs synapse carries
// on rollout completions land in PRObject's deployed_jobs exactly as the
// legacy registry recorded them.
func TestShadowRolloutReadyJobsGolden(t *testing.T) {
	t.Parallel()
	g := loadShadowGolden(t, "rollout-ready-jobs.json")
	tests := g.Cases
	for _, tc := range tests {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			sim := newParitySim(t)
			for _, inv := range append(append([]parityInvocation(nil), g.Setup...), tc.Shadow...) {
				sim.send(inv)
			}
			key := tc.Shadow[0]
			raw, ok := sim.kv[PRObjectServiceName+"/"+objectKey(t, key)+"/"+prObjectStateKey]
			require.True(t, ok, "PRObject state committed")
			st, err := decodeInput[PRState](raw)
			require.NoError(t, err)
			got := shadowWantState{Status: st.RegistryStatus(), Jobs: st.Jobs, DeployedJobs: st.DeployedJobs, DeployedKustomizations: st.DeployedKustomizations}
			require.Equal(t, tc.WantState, got)
		})
	}
}

// untrackedWant is the hand-derived outcome of one untracked-upstream case.
// IntendedEffects is set only on cases that pin legacy intent recording;
// those no longer exist in prstate and are left out (restateOwnedCases).
type untrackedWant struct {
	Persisted       bool           `json:"persisted"`
	Status          string         `json:"status"`
	IntendedEffects []string       `json:"intended_effects"`
	Effects         []parityEffect `json:"effects"`
	Rollouts        []RolloutRef   `json:"rollouts"`
	RepoIndexed     bool           `json:"repo_indexed"`
}

// untrackedCase is one untracked-upstream sequence: optional setup, the
// event stream synapse sends, and the outcome.
type untrackedCase struct {
	Name   string             `json:"name"`
	Setup  []parityInvocation `json:"setup"`
	Stream []parityInvocation `json:"stream"`
	Want   untrackedWant      `json:"want"`
}

// restateOwnedCases drops the cases that pin legacy intended-effect
// recording; their restate-owned twins stay in the golden.
func restateOwnedCases(cases []untrackedCase) []untrackedCase {
	var out []untrackedCase
	for _, c := range cases {
		if len(c.Want.IntendedEffects) == 0 {
			out = append(out, c)
		}
	}
	return out
}

// untrackedGolden is untracked-upstream-kustomization.json; synapse-only
// fields are ignored here.
type untrackedGolden struct {
	Cases []untrackedCase `json:"cases"`
}

// TestShadowUntrackedUpstreamGolden pins that rollout events for a PR
// PRObject never saw register, opened or merge persist nothing,
// index nothing and run no fact or reconcile, while a tracked merged PR
// still records, reconciles once per generation and is tracked as a
// rollout. Captured legacy streams replay as captured.
func TestShadowUntrackedUpstreamGolden(t *testing.T) {
	t.Parallel()
	raw, err := os.ReadFile(prlifecycleFixtureDir + "shadow/untracked-upstream-kustomization.json")
	require.NoError(t, err)
	var g untrackedGolden
	require.NoError(t, json.Unmarshal(raw, &g))
	require.NotEmpty(t, g.Cases)
	tests := restateOwnedCases(g.Cases)
	require.Len(t, tests, 4)
	for _, tc := range tests {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			sim := newParitySim(t)
			for _, inv := range tc.Setup {
				sim.send(inv)
			}
			sim.effects = nil
			for _, inv := range tc.Stream {
				sim.send(inv)
			}
			first := tc.Stream[0]
			got := untrackedWant{Effects: sim.effects}
			if raw, ok := sim.kv[PRObjectServiceName+"/"+objectKey(t, first)+"/"+prObjectStateKey]; ok {
				st, err := decodeInput[PRState](raw)
				require.NoError(t, err)
				got.Persisted, got.Status = true, st.Status
			}
			if raw, ok := sim.kv[RepoObjectServiceName+"/"+RolloutIndexKey+"/"+rolloutIndexStateKey]; ok {
				idx, err := decodeInput[RolloutIndexState](raw)
				require.NoError(t, err)
				got.Rollouts = idx.Active()
			}
			addr := parityInvocation{Object: RepoObjectServiceName, Event: first.Event}
			_, got.RepoIndexed = sim.kv[RepoObjectServiceName+"/"+objectKey(t, addr)+"/"+repoObjectStateKey]
			require.Equal(t, tc.Want, got)
		})
	}
}

// siblingWant is the hand-derived PRObject outcome of one sibling-bot case.
type siblingWant struct {
	Owner   string `json:"owner"`
	HeadSHA string `json:"head_sha"`
	Status  string `json:"status"`
}

// siblingCase registers the PR, then replays the sends synapse emits for
// its synchronize; synapse-only fields are ignored.
type siblingCase struct {
	Name   string             `json:"name"`
	Setup  []parityInvocation `json:"setup"`
	Shadow []parityInvocation `json:"shadow"`
	Want   siblingWant        `json:"want"`
}

// TestShadowSiblingBotSynchronizeGolden pins that a configured sibling
// bot's synchronize moves PRObject to the head the legacy row records with
// the registered owner, while a bot outside cfg.Bots (no send) keeps the
// registered head.
func TestShadowSiblingBotSynchronizeGolden(t *testing.T) {
	t.Parallel()
	raw, err := os.ReadFile(prlifecycleFixtureDir + "shadow/sibling-bot-synchronize.json")
	require.NoError(t, err)
	var g struct {
		Cases []siblingCase `json:"cases"`
	}
	require.NoError(t, json.Unmarshal(raw, &g))
	require.NotEmpty(t, g.Cases)
	tests := g.Cases
	for _, tc := range tests {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			sim := newParitySim(t)
			for _, inv := range append(append([]parityInvocation(nil), tc.Setup...), tc.Shadow...) {
				sim.send(inv)
			}
			st := simPRState(t, sim, tc.Setup[0])
			require.Equal(t, tc.Want, siblingWant{Owner: st.OwnerAgent(), HeadSHA: st.HeadSHA, Status: st.RegistryStatus()})
		})
	}
}

// staleHeadWant is the hand-derived outcome of one stale-head case: the
// PRObject state after the captured shadow sends, the RepoObject lookup
// of that head, the PR status once the check_run on it routes, and the
// effects the restate-owned replay performs.
type staleHeadWant struct {
	Owner            string         `json:"owner"`
	HeadSHA          string         `json:"head_sha"`
	PriorHeads       []string       `json:"prior_heads"`
	Status           string         `json:"status"`
	ResolvedPR       int            `json:"resolved_pr"`
	StatusAfterCheck string         `json:"status_after_check"`
	RestateEffects   []parityEffect `json:"restate_effects"`
}

// staleHeadCase replays setup plus the captured shadow sends (then an
// optional SHA-only check_run), and separately setup plus the
// restate-owned stream; synapse-only fields are ignored.
type staleHeadCase struct {
	Name     string             `json:"name"`
	Setup    []parityInvocation `json:"setup"`
	Shadow   []parityInvocation `json:"shadow"`
	Restate  []parityInvocation `json:"restate"`
	CheckRun *parityInvocation  `json:"check_run"`
	Want     staleHeadWant      `json:"want"`
}

// newStaleHeadSim is the parity sim plus the RepoObject SHA routing
// handlers, so an unindexed check_run is held and expires instead of
// routing.
func newStaleHeadSim(t *testing.T) *paritySim {
	t.Helper()
	sim := newParitySim(t)
	repo := NewRepoObjectService()
	sim.handlers["RepoObject/HandleCheckRunBySHA"] = bindErr(repo.HandleCheckRunBySHA)
	sim.handlers["RepoObject/"+RepoHandlerHeldReplay] = bindErr(repo.HandleHeldReplay)
	sim.handlers["RepoObject/"+RepoHandlerHeldExpiry] = bindErr(repo.HandleHeldExpiry)
	return sim
}

// simPRState decodes the committed PRObject state for inv's key.
func simPRState(t *testing.T, sim *paritySim, inv parityInvocation) PRState {
	t.Helper()
	raw, ok := sim.kv[PRObjectServiceName+"/"+objectKey(t, inv)+"/"+prObjectStateKey]
	require.True(t, ok, "PRObject state committed")
	st, err := decodeInput[PRState](raw)
	require.NoError(t, err)
	return st
}

// TestShadowStaleHeadSynchronizeGolden pins that a synchronize whose before
// SHA PRObject never saw recovers the stale head (state, SHA index, check
// routing and the restate-owned synchronize fact), while a
// late redelivery of a prior head still drops stale and the normal
// before==stored synchronize is unchanged.
func TestShadowStaleHeadSynchronizeGolden(t *testing.T) {
	t.Parallel()
	raw, err := os.ReadFile(prlifecycleFixtureDir + "shadow/stale-head-synchronize.json")
	require.NoError(t, err)
	var g struct {
		Cases []staleHeadCase `json:"cases"`
	}
	require.NoError(t, json.Unmarshal(raw, &g))
	require.NotEmpty(t, g.Cases)
	tests := g.Cases
	for _, tc := range tests {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			sim := newStaleHeadSim(t)
			for _, inv := range append(append([]parityInvocation(nil), tc.Setup...), tc.Shadow...) {
				sim.send(inv)
			}
			st := simPRState(t, sim, tc.Setup[0])
			got := staleHeadWant{Owner: st.OwnerAgent(), HeadSHA: st.HeadSHA, PriorHeads: st.PriorHeads, Status: st.RegistryStatus()}
			repoRaw, ok := sim.kv[RepoObjectServiceName+"/"+RepoObjectKey(st.Repo)+"/"+repoObjectStateKey]
			require.True(t, ok, "RepoObject index committed")
			repoSt, err := decodeInput[RepoState](repoRaw)
			require.NoError(t, err)
			if entry, found := repoSt.ResolveSHA(st.HeadSHA); found {
				got.ResolvedPR = entry.PRNumber
			}
			if tc.CheckRun != nil {
				sim.send(*tc.CheckRun)
				after := simPRState(t, sim, tc.Setup[0])
				got.StatusAfterCheck = after.RegistryStatus()
			}
			owned := newStaleHeadSim(t)
			for _, inv := range tc.Setup {
				owned.send(inv)
			}
			owned.effects = nil
			for _, inv := range tc.Restate {
				owned.send(inv)
			}
			got.RestateEffects = owned.effects
			require.Equal(t, tc.Want, got)
		})
	}
}

// mergedStaleHeadWant is the hand-derived outcome of one merged-stale-head
// case: PRObject after the captured shadow sends, the
// RepoObject lookup of its head, the active rollouts, and the
// deploy_success effect, identical for the capture and the
// restate-owned stream.
type mergedStaleHeadWant struct {
	Status                 string         `json:"status"`
	HeadSHA                string         `json:"head_sha"`
	PriorHeads             []string       `json:"prior_heads"`
	DeployedJobs           []string       `json:"deployed_jobs"`
	DeployedKustomizations []string       `json:"deployed_kustomizations"`
	ResolvedPR             int            `json:"resolved_pr"`
	Rollouts               []RolloutRef   `json:"rollouts"`
	Effects                []parityEffect `json:"effects"`
}

// mergedStaleHeadCase replays setup plus the captured shadow sends, and
// separately setup plus the restate-owned stream;
// synapse-only fields are ignored.
type mergedStaleHeadCase struct {
	Name    string              `json:"name"`
	Setup   []parityInvocation  `json:"setup"`
	Shadow  []parityInvocation  `json:"shadow"`
	Restate []parityInvocation  `json:"restate"`
	Want    mergedStaleHeadWant `json:"want"`
}

// simActiveRollouts reads the active rollout index, nil when never written.
func simActiveRollouts(t *testing.T, sim *paritySim) []RolloutRef {
	t.Helper()
	raw, ok := sim.kv[RepoObjectServiceName+"/"+RolloutIndexKey+"/"+rolloutIndexStateKey]
	if !ok {
		return nil
	}
	idx, err := decodeInput[RolloutIndexState](raw)
	require.NoError(t, err)
	return idx.Active()
}

// TestShadowMergedStaleHeadDeployGolden pins that a PR merged while
// PRObject's head was stale still completes its rollout: the same-merge
// closed event or the first applied deploy event repairs the head, every
// rollout progress and deploy_success applies on the merge SHA, the PR
// leaves ListRollouts, and the deploy_success effect matches for the
// captured and the restate-owned stream; deploy events naming another merge commit or a prior
// head with no merge SHA still drop stale.
func TestShadowMergedStaleHeadDeployGolden(t *testing.T) {
	t.Parallel()
	raw, err := os.ReadFile(prlifecycleFixtureDir + "shadow/merged-stale-head-deploy.json")
	require.NoError(t, err)
	var g struct {
		Cases []mergedStaleHeadCase `json:"cases"`
	}
	require.NoError(t, json.Unmarshal(raw, &g))
	require.NotEmpty(t, g.Cases)
	tests := g.Cases
	for _, tc := range tests {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			sim := newParitySim(t)
			for _, inv := range tc.Setup {
				sim.send(inv)
			}
			sim.effects = nil
			for _, inv := range tc.Shadow {
				sim.send(inv)
			}
			st := simPRState(t, sim, tc.Setup[0])
			got := mergedStaleHeadWant{
				Status: st.RegistryStatus(), HeadSHA: st.HeadSHA, PriorHeads: st.PriorHeads,
				DeployedJobs: st.DeployedJobs, DeployedKustomizations: st.DeployedKustomizations,
				Rollouts: simActiveRollouts(t, sim), Effects: sim.effects,
			}
			repoRaw, ok := sim.kv[RepoObjectServiceName+"/"+RepoObjectKey(st.Repo)+"/"+repoObjectStateKey]
			require.True(t, ok, "RepoObject index committed")
			repoSt, err := decodeInput[RepoState](repoRaw)
			require.NoError(t, err)
			if entry, found := repoSt.ResolveSHA(st.HeadSHA); found {
				got.ResolvedPR = entry.PRNumber
			}
			owned := newParitySim(t)
			for _, inv := range tc.Setup {
				owned.send(inv)
			}
			owned.effects = nil
			for _, inv := range tc.Restate {
				owned.send(inv)
			}
			require.Equal(t, got.Effects, owned.effects, "restate-owned stream publishes exactly the captured stream's effects")
			require.Equal(t, tc.Want, got)
		})
	}
}

// lostMergeWant is the PRObject end state after a merge recovered from a
// deploy commit lookup, plus the status after each recovery/deploy step.
type lostMergeWant struct {
	Stages                 []string       `json:"stages"`
	Status                 string         `json:"status"`
	HeadSHA                string         `json:"head_sha"`
	MergeSHA               string         `json:"merge_sha"`
	PriorHeads             []string       `json:"prior_heads"`
	DeployedJobs           []string       `json:"deployed_jobs"`
	DeployedKustomizations []string       `json:"deployed_kustomizations"`
	ResolvedMergePR        int            `json:"resolved_merge_pr"`
	Rollouts               []RolloutRef   `json:"rollouts"`
	Effects                []parityEffect `json:"effects"`
}

// lostMergeCase replays setup, the recovered merge and the deploy stream
// once from the captured shadow sends and once from the
// restate-owned stream; synapse-only fields (lookup) are ignored.
type lostMergeCase struct {
	Name            string             `json:"name"`
	Setup           []parityInvocation `json:"setup"`
	RecoveryShadow  []parityInvocation `json:"recovery_shadow"`
	RecoveryRestate []parityInvocation `json:"recovery_restate"`
	DeployShadow    []parityInvocation `json:"deploy_shadow"`
	DeployRestate   []parityInvocation `json:"deploy_restate"`
	Late            []parityInvocation `json:"late"`
	Want            lostMergeWant      `json:"want"`
}

// TestShadowLostMergeWebhooksGolden pins lost-merge-webhooks.json: a merge
// synapse recovers from a deploy commit lookup lands PRObject merged ->
// deploying -> deployed for both the captured and the restate-owned stream
// with the same deploy_success, and the late real closed/push webhooks for
// that merge change nothing and run no effects.
func TestShadowLostMergeWebhooksGolden(t *testing.T) {
	t.Parallel()
	raw, err := os.ReadFile(prlifecycleFixtureDir + "shadow/lost-merge-webhooks.json")
	require.NoError(t, err)
	var g struct {
		Cases []lostMergeCase `json:"cases"`
	}
	require.NoError(t, json.Unmarshal(raw, &g))
	require.NotEmpty(t, g.Cases)
	tests := g.Cases
	for _, tc := range tests {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			sim := newParitySim(t)
			for _, inv := range tc.Setup {
				sim.send(inv)
			}
			sim.effects = nil
			var stages []string
			for _, inv := range append(append([]parityInvocation(nil), tc.RecoveryShadow...), tc.DeployShadow...) {
				sim.send(inv)
				st := simPRState(t, sim, tc.Setup[0])
				stages = append(stages, st.RegistryStatus())
			}
			st := simPRState(t, sim, tc.Setup[0])
			got := lostMergeWant{
				Stages: stages, Status: st.RegistryStatus(), HeadSHA: st.HeadSHA, MergeSHA: st.MergeSHA,
				PriorHeads: st.PriorHeads, DeployedJobs: st.DeployedJobs, DeployedKustomizations: st.DeployedKustomizations,
				Rollouts: simActiveRollouts(t, sim), Effects: sim.effects,
			}
			repoRaw, ok := sim.kv[RepoObjectServiceName+"/"+RepoObjectKey(st.Repo)+"/"+repoObjectStateKey]
			require.True(t, ok, "RepoObject index committed")
			repoSt, err := decodeInput[RepoState](repoRaw)
			require.NoError(t, err)
			if entry, found := repoSt.ResolveSHA(st.MergeSHA); found {
				got.ResolvedMergePR = entry.PRNumber
			}
			sim.effects = nil
			for _, inv := range tc.Late {
				sim.send(inv)
			}
			late := simPRState(t, sim, tc.Setup[0])
			require.Equal(t, st, late, "late closed/push for the recovered merge is a no-op")
			require.Empty(t, sim.effects, "late closed/push runs no effect")

			owned := newParitySim(t)
			for _, inv := range tc.Setup {
				owned.send(inv)
			}
			owned.effects = nil
			for _, inv := range append(append([]parityInvocation(nil), tc.RecoveryRestate...), tc.DeployRestate...) {
				owned.send(inv)
			}
			ownedSt := simPRState(t, owned, tc.Setup[0])
			require.Equal(t, got.Effects, owned.effects, "restate-owned stream publishes exactly the captured stream's effects")
			require.Equal(t, got.Status, ownedSt.RegistryStatus(), "both streams land on the same status")
			require.Equal(t, got.DeployedJobs, ownedSt.DeployedJobs, "both streams deploy the same jobs")
			owned.effects = nil
			for _, inv := range tc.Late {
				owned.send(inv)
			}
			require.Empty(t, owned.effects, "late closed/push runs no restate-owned effect")
			require.Equal(t, tc.Want, got)
		})
	}
}

// selfRegisteredWant is the PRObject end state of one dependabot-opened
// case, identical for the captured and the restate-owned stream.
type selfRegisteredWant struct {
	Status       string         `json:"status"`
	TargetID     string         `json:"target_id"`
	Title        string         `json:"title"`
	Tracked      bool           `json:"tracked"`
	DeployedJobs []string       `json:"deployed_jobs"`
	Rollouts     []RolloutRef   `json:"rollouts"`
	Effects      []parityEffect `json:"effects"`
}

// selfRegisteredCase replays the opened sends and the lifecycle once from
// the captured shadow sends and once from the restate-owned
// stream; synapse-only fields (pull_request, legacy, row) are ignored.
type selfRegisteredCase struct {
	Name             string             `json:"name"`
	PullRequest      PREvent            `json:"pull_request"`
	OpenedShadow     []parityInvocation `json:"opened_shadow"`
	OpenedRestate    []parityInvocation `json:"opened_restate"`
	LifecycleShadow  []parityInvocation `json:"lifecycle_shadow"`
	LifecycleRestate []parityInvocation `json:"lifecycle_restate"`
	Want             selfRegisteredWant `json:"want"`
}

// simSelfRegisteredState reads the PR's committed state, or a fresh state
// when no send ever committed one (a PR synapse journaled nothing for).
func simSelfRegisteredState(t *testing.T, sim *paritySim, pr PREvent) PRState {
	t.Helper()
	raw, ok := sim.kv[PRObjectServiceName+"/"+PRObjectKey(pr.Repo, pr.PRNumber)+"/"+prObjectStateKey]
	if !ok {
		return *NewPRState(pr.Repo, pr.PRNumber)
	}
	st, err := decodeInput[PRState](raw)
	require.NoError(t, err)
	return st
}

// deployEffects drops the opened fact: the captured shadow stream has no
// opened send (legacy published it itself), so only the restate-owned
// stream runs it; every lifecycle effect stays.
func deployEffects(effects []parityEffect) []parityEffect {
	var out []parityEffect
	for _, e := range effects {
		if !strings.HasSuffix(e.Kind, "_opened") {
			out = append(out, e)
		}
	}
	return out
}

// TestShadowDependabotOpenedGolden pins dependabot-opened.json: a
// self-registered dependabot PR lands in PRObject with the target and title
// legacy wrote, is tracked and deploys with the same effects for the
// captured and the restate-owned stream, while a PR nobody registers stays
// untracked.
func TestShadowDependabotOpenedGolden(t *testing.T) {
	t.Parallel()
	raw, err := os.ReadFile(prlifecycleFixtureDir + "shadow/dependabot-opened.json")
	require.NoError(t, err)
	var g struct {
		Cases []selfRegisteredCase `json:"cases"`
	}
	require.NoError(t, json.Unmarshal(raw, &g))
	require.NotEmpty(t, g.Cases)
	tests := g.Cases
	for _, tc := range tests {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			sim := newParitySim(t)
			for _, inv := range append(append([]parityInvocation(nil), tc.OpenedShadow...), tc.LifecycleShadow...) {
				sim.send(inv)
			}
			st := simSelfRegisteredState(t, sim, tc.PullRequest)
			snap := BuildProjection(&st)
			got := selfRegisteredWant{
				Status: st.RegistryStatus(), TargetID: st.TargetID, Title: st.Title, Tracked: snap.Tracked(),
				DeployedJobs: st.DeployedJobs, Rollouts: simActiveRollouts(t, sim), Effects: sim.effects,
			}

			owned := newParitySim(t)
			for _, inv := range append(append([]parityInvocation(nil), tc.OpenedRestate...), tc.LifecycleRestate...) {
				owned.send(inv)
			}
			ownedSt := simSelfRegisteredState(t, owned, tc.PullRequest)
			ownedSnap := BuildProjection(&ownedSt)
			require.Equal(t, got.Effects, deployEffects(owned.effects), "restate-owned stream publishes exactly the captured stream's effects")
			require.Equal(t, got.Status, ownedSt.RegistryStatus(), "both streams land on the same status")
			require.Equal(t, got.TargetID, ownedSt.TargetID, "both streams store the legacy target")
			require.Equal(t, got.Title, ownedSt.Title, "both streams store the legacy title")
			require.Equal(t, got.Tracked, ownedSnap.Tracked(), "both streams agree on tracking")
			require.Equal(t, got.DeployedJobs, ownedSt.DeployedJobs, "both streams deploy the same jobs")
			require.Empty(t, simActiveRollouts(t, owned), "restate-owned ListRollouts is empty")
			require.Equal(t, tc.Want, got)
		})
	}
}
