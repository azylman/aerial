package prstate

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/pubsub"
)

// rsBuild is a fenced-identity build snapshot for azylman/aerial#7.
func rsBuild(source string, pr int, selected []string, results ...BuildResult) *BuildMetadata {
	sel := make([]BuildSelection, 0, len(selected))
	for _, s := range selected {
		sel = append(sel, BuildSelection{Service: s, ImageName: s})
	}
	return &BuildMetadata{Version: 1, Kind: "progress", Repository: "azylman/aerial", SourceSHA: source, PRNumber: &pr, Selected: sel, Results: results}
}

func rsResult(svc, state string, pushed bool) BuildResult {
	return BuildResult{Service: svc, ImageName: svc, State: state, Pushed: pushed, SourceAttempt: 1}
}

// rsState is azylman/aerial#7 at head h7 / merge m7 with the given row status.
func rsState(status string, jobs, started, deployed []string, jobStates map[string]string, m *BuildMetadata) *PRState {
	st := NewPRState("azylman/aerial", 7)
	st.HeadSHA, st.MergeSHA, st.Status = "h7", "m7", status
	st.Jobs, st.StartedJobs, st.DeployedJobs, st.JobStates, st.BuildMetadata = jobs, started, deployed, jobStates, m
	return st
}

func TestBuildPRRolloutState(t *testing.T) {
	t.Parallel()
	five := []string{"eri", "haro", "coven", "vision", "senses"}
	fiveResults := []BuildResult{rsResult("eri", "running", false), rsResult("haro", "success", true), rsResult("coven", "success", false), rsResult("vision", "cancelled", false)}
	tests := []struct {
		name string
		st   *PRState
		want *pubsub.PRRolloutState
	}{
		{"nil state", nil, nil},
		{"no jobs", rsState("open", nil, nil, nil, nil, nil), nil},
		{"empty job state map", rsState("open", nil, nil, nil, map[string]string{}, nil), nil},
		{"partially started", rsState("deploying", []string{"apps", "core"}, []string{"apps"}, nil, nil, nil), &pubsub.PRRolloutState{ExpectedJobs: []string{"apps", "core"}, StartedJobs: []string{"apps"}}},
		{"partially deployed", rsState("deploying", []string{"core"}, []string{"apps", "core"}, []string{"apps"}, nil, nil), &pubsub.PRRolloutState{ExpectedJobs: []string{"core"}, StartedJobs: []string{"apps", "core"}, DeployedJobs: []string{"apps"}}},
		{"fully deployed", rsState("deployed", nil, []string{"apps", "core"}, []string{"apps", "core"}, nil, nil), &pubsub.PRRolloutState{StartedJobs: []string{"apps", "core"}, DeployedJobs: []string{"apps", "core"}}},
		{"lists trimmed and deduplicated", rsState("deploying", []string{" core", "core ", ""}, []string{"apps", " apps"}, nil, nil, nil), &pubsub.PRRolloutState{ExpectedJobs: []string{"core"}, StartedJobs: []string{"apps"}}},
		{"job state keys normalized", rsState("deploying", []string{"eri"}, []string{"eri"}, nil, map[string]string{" Eri ": " rolling ", " ": "x"}, nil), &pubsub.PRRolloutState{ExpectedJobs: []string{"eri"}, StartedJobs: []string{"eri"}, JobStates: map[string]string{"eri": "rolling"}}},
		{"build metadata job states", rsState("open", nil, nil, nil, map[string]string{"haro": "deployed"}, rsBuild("h7", 7, five, fiveResults...)), &pubsub.PRRolloutState{ExpectedJobs: five, JobStates: map[string]string{"eri": "image_building", "haro": "deployed", "coven": "image_unpublished", "vision": "image_failed", "senses": "image_pending"}}},
		{"build metadata extends row jobs on merged", rsState("merged", []string{"apps", "eri"}, nil, nil, nil, rsBuild("m7", 7, []string{"eri"}, rsResult("eri", "success", true))), &pubsub.PRRolloutState{ExpectedJobs: []string{"apps", "eri"}, JobStates: map[string]string{"eri": "image_built"}}},
		{"build metadata empty selection", rsState("open", []string{"apps"}, nil, nil, nil, rsBuild("h7", 7, nil)), &pubsub.PRRolloutState{ExpectedJobs: []string{"apps"}}},
		{"build metadata stale head ignored", rsState("ci_failed", []string{"apps"}, nil, nil, nil, rsBuild("h6", 7, []string{"eri"})), &pubsub.PRRolloutState{ExpectedJobs: []string{"apps"}}},
		{"build metadata while deploying ignored", rsState("deploying", []string{"apps"}, nil, nil, nil, rsBuild("m7", 7, []string{"eri"})), &pubsub.PRRolloutState{ExpectedJobs: []string{"apps"}}},
		{"build metadata for another pr ignored", rsState("open", nil, nil, nil, nil, rsBuild("h7", 8, []string{"eri"})), nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, BuildPRRolloutState(tc.st))
		})
	}
}

func TestWithRolloutState(t *testing.T) {
	t.Parallel()
	carried := &pubsub.PRRolloutState{DeployedJobs: []string{"carried"}}
	st := rsState("deployed", nil, []string{"apps"}, []string{"apps"}, nil, nil)
	tests := []struct {
		name string
		st   *PRState
		in   *pubsub.PRRolloutState
		want *pubsub.PRRolloutState
	}{
		{"missing state renders from committed state", st, nil, &pubsub.PRRolloutState{StartedJobs: []string{"apps"}, DeployedJobs: []string{"apps"}}},
		{"carried state is kept", st, carried, carried},
		{"nothing to render stays nil", rsState("open", nil, nil, nil, nil, nil), nil, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := withRolloutState(tc.st, pubsub.PRLifecycleEvent{Event: "deploy_success", RolloutState: tc.in})
			require.Equal(t, tc.want, got.RolloutState)
		})
	}
}

// rolloutRenderCase is one testdata/prlifecycle/rollout_state/render.json
// case, shared with synapse's legacy buildPRRolloutState parity test.
type rolloutRenderCase struct {
	Name         string                 `json:"name"`
	Fenced       bool                   `json:"build_metadata_fenced"`
	Row          rolloutRow             `json:"row"`
	RolloutState *pubsub.PRRolloutState `json:"rollout_state"`
}

// rolloutRow is the part of a golden pr_registry row the rollout render
// reads; the fixture's other row keys are ignored.
type rolloutRow struct {
	Repo     string             `json:"repo"`
	PRNumber int                `json:"pr_number"`
	HeadSHA  string             `json:"head_sha"`
	MergeSHA string             `json:"merge_sha"`
	Status   string             `json:"status"`
	Metadata ProjectionMetadata `json:"metadata"`
}

// state seeds a fresh PRState with the row's rollout inputs.
func (r rolloutRow) state() *PRState {
	st := NewPRState(r.Repo, r.PRNumber)
	st.HeadSHA, st.MergeSHA, st.Status = r.HeadSHA, r.MergeSHA, r.Status
	st.Jobs, st.StartedJobs, st.DeployedJobs = r.Metadata.Jobs, r.Metadata.StartedJobs, r.Metadata.DeployedJobs
	st.JobStates, st.BuildMetadata = r.Metadata.JobStates, r.Metadata.BuildMetadata
	return st
}

// TestBuildPRRolloutStateLegacyParity seeds PRState from each golden row
// and asserts the render equals the hand-derived legacy rollout state.
func TestBuildPRRolloutStateLegacyParity(t *testing.T) {
	t.Parallel()
	raw, err := os.ReadFile(prlifecycleFixtureDir + "rollout_state/render.json")
	require.NoError(t, err)
	var doc struct {
		Cases []rolloutRenderCase `json:"cases"`
	}
	require.NoError(t, json.Unmarshal(raw, &doc))
	require.Len(t, doc.Cases, 10)
	for _, tc := range doc.Cases {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			st := tc.Row.state()
			require.Equal(t, tc.Fenced, st.renderableBuildMetadata() != nil, "fence")
			require.Equal(t, tc.RolloutState, BuildPRRolloutState(st))
		})
	}
}
