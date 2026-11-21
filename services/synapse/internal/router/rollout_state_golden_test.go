package router

// Rollout-state goldens, synapse side: testdata/prlifecycle/rollout_state/
// pins the rollout state legacy buildPRRolloutState renders for one
// pr_registry row. PRObject renders restate-owned facts from the hydrated
// row and must match the same file.

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"

	"github.com/azylman/aerial/pkg/pubsub"
)

const rolloutStateFixture = "../../../../testdata/prlifecycle/rollout_state/render.json"

type rolloutRenderRow struct {
	Metadata map[string]any `json:"metadata"`
}

type rolloutRenderCase struct {
	Name         string                 `json:"name"`
	Fenced       bool                   `json:"build_metadata_fenced"`
	Row          rolloutRenderRow       `json:"row"`
	RolloutState *pubsub.PRRolloutState `json:"rollout_state"`
}

// TestLegacyRolloutStateGolden renders each golden row the way
// loadPRRolloutState does: the raw metadata, with build_metadata replaced
// by the validated snapshot when LoadPRBuildMetadata returns it (fenced)
// and dropped otherwise.
func TestLegacyRolloutStateGolden(t *testing.T) {
	t.Parallel()
	raw, err := os.ReadFile(rolloutStateFixture)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Cases []rolloutRenderCase `json:"cases"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	if len(doc.Cases) != 10 {
		t.Fatalf("cases = %d, want 10", len(doc.Cases))
	}
	for _, tc := range doc.Cases {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			meta := tc.Row.Metadata
			snapshot, hasSnapshot := meta["build_metadata"]
			delete(meta, "build_metadata")
			if tc.Fenced {
				if !hasSnapshot {
					t.Fatal("fenced case has no build_metadata")
				}
				rawSnap, err := json.Marshal(snapshot)
				if err != nil {
					t.Fatal(err)
				}
				m, err := parseStoredBuildMetadata(rawSnap)
				if err != nil {
					t.Fatalf("fenced build_metadata invalid: %v", err)
				}
				meta["build_metadata"] = m
			}
			if got := buildPRRolloutState(meta); !reflect.DeepEqual(got, tc.RolloutState) {
				t.Errorf("rollout state = %+v, want %+v", got, tc.RolloutState)
			}
		})
	}
}
