package prstate

// Shadow goldens, prstate side: testdata/prlifecycle/shadow/ pins the
// legacy-owned wire synapse sends while it still writes pr_registry. This
// test replays each case's restate-owned setup plus the captured shadow
// sends through the real PRObject handlers and asserts the committed state
// matches the hand-derived row the legacy registry writes. synapse's
// restate_shadow_golden_test.go asserts it sends exactly those shadow events.

import (
	"encoding/json"
	"os"
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
// on legacy-owned rollout completions land in PRObject's deployed_jobs
// exactly as the legacy registry records them.
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

// untrackedGolden is untracked-upstream-kustomization.json; synapse-only
// fields are ignored here.
type untrackedGolden struct {
	Cases []untrackedCase `json:"cases"`
}

// TestShadowUntrackedUpstreamGolden pins that rollout events for a PR
// PRObject never saw register, Hydrate, opened or merge persist nothing,
// index nothing and record no fact, reconcile or intended effect, while a
// tracked merged PR still records, reconciles once per generation and is
// tracked as a rollout.
func TestShadowUntrackedUpstreamGolden(t *testing.T) {
	t.Parallel()
	raw, err := os.ReadFile(prlifecycleFixtureDir + "shadow/untracked-upstream-kustomization.json")
	require.NoError(t, err)
	var g untrackedGolden
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
			for _, inv := range tc.Stream {
				sim.send(inv)
			}
			first := tc.Stream[0]
			got := untrackedWant{Effects: sim.effects}
			if raw, ok := sim.kv[PRObjectServiceName+"/"+objectKey(t, first)+"/"+prObjectStateKey]; ok {
				st, err := decodeInput[PRState](raw)
				require.NoError(t, err)
				got.Persisted, got.Status = true, st.Status
				for _, e := range st.IntendedEffects {
					got.IntendedEffects = append(got.IntendedEffects, e.EffectKey)
				}
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

// legacyOwnerWant is the hand-derived outcome of one legacy-owner case.
type legacyOwnerWant struct {
	Owner           string   `json:"owner"`
	Status          string   `json:"status"`
	Divergent       []string `json:"divergent"`
	IntendedEffects []string `json:"intended_effects"`
}

// legacyOwnerCase hydrates PRObject from row, replays stream, then compares
// the projection against compare_row (row when absent).
type legacyOwnerCase struct {
	Name       string             `json:"name"`
	Row        RegistryRow        `json:"row"`
	CompareRow *RegistryRow       `json:"compare_row"`
	Stream     []parityInvocation `json:"stream"`
	Want       legacyOwnerWant    `json:"want"`
}

// TestShadowLegacyAgentColumnOwnerGolden pins that a legacy-era row whose
// owner lives only in the agent column agrees on ownership with the
// PRObject Hydrate built from it after a main push, with no intended
// effect, while an explicit metadata.agent naming another owner diverges.
func TestShadowLegacyAgentColumnOwnerGolden(t *testing.T) {
	t.Parallel()
	raw, err := os.ReadFile(prlifecycleFixtureDir + "shadow/legacy-agent-column-owner.json")
	require.NoError(t, err)
	var g struct {
		Cases []legacyOwnerCase `json:"cases"`
	}
	require.NoError(t, json.Unmarshal(raw, &g))
	require.NotEmpty(t, g.Cases)
	tests := g.Cases
	for _, tc := range tests {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			sim := newParitySim(t)
			hydrate, err := json.Marshal(struct {
				Repo     string      `json:"repo"`
				PRNumber int         `json:"pr_number"`
				Row      RegistryRow `json:"row"`
			}{tc.Row.Repo, tc.Row.PRNumber, tc.Row})
			require.NoError(t, err)
			sim.send(parityInvocation{Object: PRObjectServiceName, Handler: "Hydrate", Event: hydrate})
			for _, inv := range tc.Stream {
				sim.send(inv)
			}
			stRaw, ok := sim.kv[PRObjectServiceName+"/"+objectKey(t, tc.Stream[0])+"/"+prObjectStateKey]
			require.True(t, ok, "PRObject state committed")
			st, err := decodeInput[PRState](stRaw)
			require.NoError(t, err)
			row := tc.Row
			if tc.CompareRow != nil {
				row = *tc.CompareRow
			}
			got := legacyOwnerWant{Owner: st.OwnerAgent(), Status: st.RegistryStatus(), Divergent: CompareProjection(BuildProjection(&st), row, true).Divergent}
			for _, e := range st.IntendedEffects {
				got.IntendedEffects = append(got.IntendedEffects, e.EffectKey)
			}
			require.Equal(t, tc.Want, got)
		})
	}
}

// siblingWant is the hand-derived PRObject outcome of one sibling-bot case.
type siblingWant struct {
	Owner     string   `json:"owner"`
	HeadSHA   string   `json:"head_sha"`
	Status    string   `json:"status"`
	Divergent []string `json:"divergent"`
}

// siblingCase registers the PR, replays the legacy-owned shadow sends
// synapse emits for its synchronize, then compares the projection against
// the row legacy wrote; synapse-only fields are ignored.
type siblingCase struct {
	Name   string             `json:"name"`
	Setup  []parityInvocation `json:"setup"`
	Shadow []parityInvocation `json:"shadow"`
	Want   siblingWant        `json:"want"`
	Row    RegistryRow        `json:"row"`
}

// TestShadowSiblingBotSynchronizeGolden pins that a configured sibling
// bot's synchronize moves PRObject to the head the legacy row records with
// the registered owner and no projection divergence, while a bot outside
// cfg.Bots (no send) keeps the registered head the untouched row holds.
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
			stRaw, ok := sim.kv[PRObjectServiceName+"/"+objectKey(t, tc.Setup[0])+"/"+prObjectStateKey]
			require.True(t, ok, "PRObject state committed")
			st, err := decodeInput[PRState](stRaw)
			require.NoError(t, err)
			got := siblingWant{Owner: st.OwnerAgent(), HeadSHA: st.HeadSHA, Status: st.RegistryStatus(), Divergent: CompareProjection(BuildProjection(&st), tc.Row, true).Divergent}
			require.Equal(t, tc.Want, got)
		})
	}
}

// staleHeadWant is the hand-derived outcome of one stale-head case: the
// PRObject state after the legacy-owned shadow sends, the RepoObject lookup
// of that head, the PR status once the check_run on it routes, and the
// effects the restate-owned replay performs.
type staleHeadWant struct {
	Owner            string         `json:"owner"`
	HeadSHA          string         `json:"head_sha"`
	PriorHeads       []string       `json:"prior_heads"`
	Status           string         `json:"status"`
	Superseded       bool           `json:"superseded"`
	Divergent        []string       `json:"divergent"`
	ResolvedPR       int            `json:"resolved_pr"`
	StatusAfterCheck string         `json:"status_after_check"`
	RestateEffects   []parityEffect `json:"restate_effects"`
}

// staleHeadCase replays setup plus the legacy-owned shadow sends (then an
// optional SHA-only check_run), and separately setup plus the restate-owned
// stream; synapse-only fields are ignored.
type staleHeadCase struct {
	Name     string             `json:"name"`
	Setup    []parityInvocation `json:"setup"`
	Shadow   []parityInvocation `json:"shadow"`
	Restate  []parityInvocation `json:"restate"`
	CheckRun *parityInvocation  `json:"check_run"`
	Want     staleHeadWant      `json:"want"`
	Row      RegistryRow        `json:"row"`
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
// SHA PRObject never saw recovers the stale head (state, projection, SHA
// index, check routing and the restate-owned synchronize fact), while a
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
			cmp := CompareProjection(BuildProjection(&st), tc.Row, true)
			got := staleHeadWant{Owner: st.OwnerAgent(), HeadSHA: st.HeadSHA, PriorHeads: st.PriorHeads, Status: st.RegistryStatus(), Superseded: cmp.Superseded, Divergent: cmp.Divergent}
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
// case: PRObject after the legacy-owned shadow sends, the RepoObject lookup
// of its head, the active rollouts, and the deploy_success effect, both as
// recorded intended (legacy) and as published by the restate-owned replay.
type mergedStaleHeadWant struct {
	Status                 string         `json:"status"`
	HeadSHA                string         `json:"head_sha"`
	PriorHeads             []string       `json:"prior_heads"`
	DeployedJobs           []string       `json:"deployed_jobs"`
	DeployedKustomizations []string       `json:"deployed_kustomizations"`
	ResolvedPR             int            `json:"resolved_pr"`
	Rollouts               []RolloutRef   `json:"rollouts"`
	Divergent              []string       `json:"divergent"`
	Effects                []parityEffect `json:"effects"`
}

// mergedStaleHeadCase replays setup plus the legacy-owned shadow sends, and
// separately setup plus the restate-owned stream; synapse-only fields are
// ignored. A case without a row skips the projection compare.
type mergedStaleHeadCase struct {
	Name    string              `json:"name"`
	Setup   []parityInvocation  `json:"setup"`
	Shadow  []parityInvocation  `json:"shadow"`
	Restate []parityInvocation  `json:"restate"`
	Want    mergedStaleHeadWant `json:"want"`
	Row     *RegistryRow        `json:"row"`
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
// leaves ListRollouts, and the deploy_success effect matches in both
// ownership modes; deploy events naming another merge commit or a prior
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
			for _, inv := range append(append([]parityInvocation(nil), tc.Setup...), tc.Shadow...) {
				sim.send(inv)
			}
			st := simPRState(t, sim, tc.Setup[0])
			got := mergedStaleHeadWant{
				Status: st.RegistryStatus(), HeadSHA: st.HeadSHA, PriorHeads: st.PriorHeads,
				DeployedJobs: st.DeployedJobs, DeployedKustomizations: st.DeployedKustomizations,
				Rollouts: simActiveRollouts(t, sim),
			}
			if tc.Row != nil {
				got.Divergent = CompareProjection(BuildProjection(&st), *tc.Row, true).Divergent
			}
			repoRaw, ok := sim.kv[RepoObjectServiceName+"/"+RepoObjectKey(st.Repo)+"/"+repoObjectStateKey]
			require.True(t, ok, "RepoObject index committed")
			repoSt, err := decodeInput[RepoState](repoRaw)
			require.NoError(t, err)
			if entry, found := repoSt.ResolveSHA(st.HeadSHA); found {
				got.ResolvedPR = entry.PRNumber
			}
			for _, e := range st.IntendedEffects {
				if e.Kind == "pr_fact_deploy_success" {
					got.Effects = append(got.Effects, parityEffect{Kind: e.Kind, Key: e.MatchKey})
				}
			}
			owned := newParitySim(t)
			for _, inv := range tc.Setup {
				owned.send(inv)
			}
			owned.effects = nil
			for _, inv := range tc.Restate {
				owned.send(inv)
			}
			require.Equal(t, got.Effects, owned.effects, "restate-owned replay publishes exactly the intended effects")
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
	Divergent              []string       `json:"divergent"`
	Effects                []parityEffect `json:"effects"`
}

// lostMergeCase replays setup, the recovered merge and the deploy stream
// once legacy-owned and once restate-owned; synapse-only fields (lookup)
// are ignored.
type lostMergeCase struct {
	Name            string             `json:"name"`
	Setup           []parityInvocation `json:"setup"`
	RecoveryShadow  []parityInvocation `json:"recovery_shadow"`
	RecoveryRestate []parityInvocation `json:"recovery_restate"`
	DeployShadow    []parityInvocation `json:"deploy_shadow"`
	DeployRestate   []parityInvocation `json:"deploy_restate"`
	Late            []parityInvocation `json:"late"`
	Want            lostMergeWant      `json:"want"`
	Row             RegistryRow        `json:"row"`
}

// TestShadowLostMergeWebhooksGolden pins lost-merge-webhooks.json: a merge
// synapse recovers from a deploy commit lookup lands PRObject merged ->
// deploying -> deployed in both effect modes, on the legacy row, with the
// same deploy_success, and the late real closed/push webhooks for that
// merge change nothing and run no effects.
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
				Rollouts:  simActiveRollouts(t, sim),
				Divergent: CompareProjection(BuildProjection(&st), tc.Row, true).Divergent,
			}
			repoRaw, ok := sim.kv[RepoObjectServiceName+"/"+RepoObjectKey(st.Repo)+"/"+repoObjectStateKey]
			require.True(t, ok, "RepoObject index committed")
			repoSt, err := decodeInput[RepoState](repoRaw)
			require.NoError(t, err)
			if entry, found := repoSt.ResolveSHA(st.MergeSHA); found {
				got.ResolvedMergePR = entry.PRNumber
			}
			for _, e := range st.IntendedEffects {
				got.Effects = append(got.Effects, parityEffect{Kind: e.Kind, Key: e.MatchKey})
			}
			intended := len(st.IntendedEffects)
			for _, inv := range tc.Late {
				sim.send(inv)
			}
			late := simPRState(t, sim, tc.Setup[0])
			require.Equal(t, st, late, "late closed/push for the recovered merge is a no-op")
			require.Len(t, late.IntendedEffects, intended, "late closed/push records no effect")

			owned := newParitySim(t)
			for _, inv := range tc.Setup {
				owned.send(inv)
			}
			owned.effects = nil
			for _, inv := range append(append([]parityInvocation(nil), tc.RecoveryRestate...), tc.DeployRestate...) {
				owned.send(inv)
			}
			ownedSt := simPRState(t, owned, tc.Setup[0])
			require.Equal(t, got.Effects, owned.effects, "restate-owned replay publishes exactly the intended effects")
			require.Equal(t, got.Status, ownedSt.RegistryStatus(), "both modes land on the same status")
			require.Equal(t, got.DeployedJobs, ownedSt.DeployedJobs, "both modes deploy the same jobs")
			owned.effects = nil
			for _, inv := range tc.Late {
				owned.send(inv)
			}
			require.Empty(t, owned.effects, "late closed/push runs no restate-owned effect")
			require.Equal(t, tc.Want, got)
		})
	}
}
