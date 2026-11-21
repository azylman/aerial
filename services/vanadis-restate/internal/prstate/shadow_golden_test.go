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
