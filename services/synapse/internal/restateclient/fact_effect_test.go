package restateclient

import (
	"testing"

	"github.com/azylman/aerial/pkg/pubsub"
)

// TestFactMatchKey pins the generation-free fact key against the shared
// golden fixture prstate's FactEffectKey test reads, plus synapse-only
// SHA fallbacks.
func TestFactMatchKey(t *testing.T) {
	t.Parallel()
	var fx struct {
		Cases []struct {
			Name string                  `json:"name"`
			Fact pubsub.PRLifecycleEvent `json:"fact"`
			Want string                  `json:"want"`
		} `json:"cases"`
	}
	readFixture(t, "fact_match_keys.json", &fx)
	if len(fx.Cases) == 0 {
		t.Fatal("fixture has no cases")
	}
	type tcase struct {
		name string
		fact pubsub.PRLifecycleEvent
		want string
	}
	tests := make([]tcase, 0, 2+len(fx.Cases))
	tests = append(tests,
		tcase{name: "zero head falls back to the merge sha", fact: pubsub.PRLifecycleEvent{Event: "deploy_failed", Repo: "aerial", PRNumber: 3, HeadSHA: "0000000", MergeSHA: "m3"}, want: "azylman/aerial#3@m3:pr_fact_deploy_failed"},
		tcase{name: "sha-less fact keys an empty sha", fact: pubsub.PRLifecycleEvent{Event: "pr_updated", Repo: "aerial", PRNumber: 3, MergeSHA: "000"}, want: "azylman/aerial#3@:pr_fact_pr_updated"},
	)
	for _, c := range fx.Cases {
		tests = append(tests, tcase{name: "golden " + c.Name, fact: c.Fact, want: c.Want})
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := FactMatchKey(tc.fact); got != tc.want {
				t.Fatalf("FactMatchKey() = %q, want %q", got, tc.want)
			}
		})
	}
}
