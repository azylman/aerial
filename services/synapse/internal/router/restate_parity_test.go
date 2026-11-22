package router

// Cross-module parity goldens, synapse side. Every scenario under
// testdata/prlifecycle/parity/ is one PR lifecycle: the inputs synapse
// receives (webhooks, alerts, watcher observations, register calls) and the
// Restate event stream they must produce. TestRestateOwnedLifecycleEdge
// drives synapse over those inputs and checks its sends against the stream;
// the prstate module's parity_test.go replays the same stream through
// PRObject/RepoObject and proves the golden effects and rows.

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// parityFixtureDir holds one golden lifecycle per file.
const parityFixtureDir = "../../../../testdata/prlifecycle/parity/"

// parityObservation is one Ready Kustomization the watcher observes.
type parityObservation struct {
	Namespace              string `json:"namespace"`
	Name                   string `json:"name"`
	RequestedAt            string `json:"requested_at"`
	LastHandledReconcileAt string `json:"last_handled_reconcile_at"`
	LastAppliedRevision    string `json:"last_applied_revision"`
}

// paritySynapseStep is one input synapse receives; Kind selects the
// populated field. Rollout, promotion and attempt-failure steps are decoded
// only by the prstate parity test.
type paritySynapseStep struct {
	Kind        string             `json:"kind"`
	Register    json.RawMessage    `json:"register"`
	Event       string             `json:"event"`
	Payload     json.RawMessage    `json:"payload"`
	Deploy      *DeployEvent       `json:"deploy"`
	Observation *parityObservation `json:"observation"`
}

// parityRestateSend names one send of a golden's restate stream; the
// event payload is pinned by the prstate parity test.
type parityRestateSend struct {
	Object  string `json:"object"`
	Handler string `json:"handler"`
}

// parityScenario is one golden file; synapse reads only its inputs and the
// handler names of its restate stream (TestRestateOwnedLifecycleEdge). The
// effects, rows and fact rollout states are proven by the prstate replay.
type parityScenario struct {
	Name    string              `json:"name"`
	Restate []parityRestateSend `json:"restate"`
	Synapse []paritySynapseStep `json:"synapse"`
}

func loadParityScenarios(t *testing.T) []parityScenario {
	t.Helper()
	paths, err := filepath.Glob(parityFixtureDir + "*.json")
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 12 {
		t.Fatalf("parity goldens = %d, want 12", len(paths))
	}
	sort.Strings(paths)
	out := make([]parityScenario, 0, len(paths))
	for _, p := range paths {
		raw, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		var sc parityScenario
		if err := json.Unmarshal(raw, &sc); err != nil {
			t.Fatalf("%s: %v", p, err)
		}
		out = append(out, sc)
	}
	return out
}
