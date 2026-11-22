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

	"github.com/azylman/aerial/pkg/pubsub"
)

// parityFixtureDir holds one golden lifecycle per file.
const parityFixtureDir = "../../../../testdata/prlifecycle/parity/"

// parityEffect is one effect by kind and generation-free match key.
type parityEffect struct {
	Kind string `json:"kind"`
	Key  string `json:"key"`
}

// parityRow is the registry row a lifecycle ends on.
type parityRow struct {
	Repo     string         `json:"repo"`
	PRNumber int            `json:"pr_number"`
	Branch   string         `json:"branch"`
	HeadSHA  string         `json:"head_sha"`
	MergeSHA string         `json:"merge_sha"`
	TargetID string         `json:"target_id"`
	Status   string         `json:"status"`
	Title    string         `json:"title"`
	Metadata map[string]any `json:"metadata"`
}

// parityRollout records reconcile requests for an image rollout.
type parityRollout struct {
	Repo     string                          `json:"repo"`
	PRNumber int                             `json:"pr_number"`
	HeadSHA  string                          `json:"head_sha"`
	Branch   string                          `json:"branch"`
	TargetID string                          `json:"target_id"`
	Jobs     []string                        `json:"jobs"`
	Requests map[string]kustomizationRequest `json:"requests"`
}

// parityObservation is one Ready Kustomization the watcher observes.
type parityObservation struct {
	Namespace              string `json:"namespace"`
	Name                   string `json:"name"`
	RequestedAt            string `json:"requested_at"`
	LastHandledReconcileAt string `json:"last_handled_reconcile_at"`
	LastAppliedRevision    string `json:"last_applied_revision"`
}

// parityPromotion is one image promotion for a commit.
type parityPromotion struct {
	Repo      string   `json:"repo"`
	PRNumber  int      `json:"pr_number"`
	CommitSHA string   `json:"commit_sha"`
	TargetID  string   `json:"target_id"`
	Images    []string `json:"images"`
}

// parityFailure is one final failed deploy attempt.
type parityFailure struct {
	Repo     string `json:"repo"`
	PRNumber int    `json:"pr_number"`
	HeadSHA  string `json:"head_sha"`
	Branch   string `json:"branch"`
	TargetID string `json:"target_id"`
	Error    string `json:"error"`
}

// paritySynapseStep is one legacy input; Kind selects the populated field.
type paritySynapseStep struct {
	Kind        string             `json:"kind"`
	Register    json.RawMessage    `json:"register"`
	Event       string             `json:"event"`
	Payload     json.RawMessage    `json:"payload"`
	Deploy      *DeployEvent       `json:"deploy"`
	Observation *parityObservation `json:"observation"`
	Rollout     *parityRollout     `json:"rollout"`
	Promotion   *parityPromotion   `json:"promotion"`
	Failure     *parityFailure     `json:"failure"`
}

// parityRestateSend names one send of a golden's restate stream; the
// event payload is pinned by the prstate parity test.
type parityRestateSend struct {
	Object  string `json:"object"`
	Handler string `json:"handler"`
}

// parityScenario is one golden file; of the restate stream only the
// handler names are read here (TestRestateOwnedLifecycleEdge).
type parityScenario struct {
	Name    string              `json:"name"`
	Restate []parityRestateSend `json:"restate"`
	// OwnedExtraSends counts, by handler, the state-only sends the
	// restate-owned edge makes beyond the golden restate stream (they carry
	// no effect, so the prstate replay omits them).
	OwnedExtraSends map[string]int      `json:"owned_extra_sends"`
	GitHubRoutes    map[string]string   `json:"github_routes"`
	Synapse         []paritySynapseStep `json:"synapse"`
	Effects         []parityEffect      `json:"effects"`
	// FactRolloutStates is the rollout state each published fact carries;
	// prstate's restate-owned publishes must match it.
	FactRolloutStates []parityFactState `json:"fact_rollout_states"`
	Rows              []parityRow       `json:"rows"`
}

// parityFactState is the rollout state one published fact carried, keyed
// by the fact's generation-free match key.
type parityFactState struct {
	Key          string                 `json:"key"`
	RolloutState *pubsub.PRRolloutState `json:"rollout_state"`
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
