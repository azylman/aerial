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
