package prstate

// Readiness-gate hermetic replay probe (chunk 06, build-1795109791-2ea91b63).
//
// Replays one CAPTURED real webhook payload identity
// (services/synapse/testdata/readiness/aerial-pr-44221326112.run.json:
// repo azylman/aerial, PR 1962, head SHA
// 5a6b51bdfb931bbb0e2d321ff53886f0ada64fea) through the journaled
// HandleCheckEvent effect path with two live invocations per row. Exactly one
// Discord alert must fire per repo+PR+headSHA+gateGeneration+effectKind key.
// The duplicate row proves exactly-once dedup; the new-check-name and
// new-generation rows are the sensitivity controls that each refire exactly
// once, proving the probe counts live effects instead of hardcoding one.
//
// A simulated-restart row is deliberately absent: ObjectContext carries the
// unexported exclusiveObject method and the SDK mocks package ships no
// ObjectContext fake, so driving the real Restate handler surface requires a
// live Restate server binary, which hermetic unit tests must never launch.
// Test-side state copies cannot prove server-side journal durability, so no
// such row is kept. True restart durability rides on Restate's embedded
// RocksDB state/journal store and is covered by the post-cutover soak.

import (
	"context"
	"testing"
)

const (
	gateCapturedHeadSHA = "5a6b51bdfb931bbb0e2d321ff53886f0ada64fea"
)

type gateReplayRow struct {
	name          string
	secondCheck   string
	newGeneration bool
	wantAlerts    int
}

func TestReadinessGateHermeticReplay(t *testing.T) {
	t.Parallel()
	rows := []gateReplayRow{
		{
			name:       "duplicate replay fires once",
			wantAlerts: 1,
		},
		{
			name:        "new check name refires",
			secondCheck: "lint",
			wantAlerts:  2,
		},
		{
			name:          "new generation refires",
			newGeneration: true,
			wantAlerts:    2,
		},
	}
	for _, tc := range rows {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			st := NewPRState("azylman/aerial", 1962)
			alerter := &FakeDiscordAlerter{}
			router := &FakeBuildRouterInvoker{}
			apply := func(check string) {
				t.Helper()
				ev := PREvent{
					Event:     "check_failed",
					Repo:      "azylman/aerial",
					PRNumber:  1962,
					HeadSHA:   gateCapturedHeadSHA,
					CheckName: check,
					Branch:    "feature",
					BuildID:   "build-1795109791-2ea91b63",
				}
				if _, err := HandleCheckEvent(ctx, st, ev, router, alerter); err != nil {
					t.Fatalf("HandleCheckEvent(%q) error = %v", check, err)
				}
			}
			apply("build-metadata")
			if tc.newGeneration {
				st.RecordKustomizationRequests()
			}
			second := "build-metadata"
			if tc.secondCheck != "" {
				second = tc.secondCheck
			}
			apply(second)
			if got := len(fakeAlerterKeys(alerter)); got != tc.wantAlerts {
				t.Errorf("alerts fired = %d, want %d (keys %v)", got, tc.wantAlerts, fakeAlerterKeys(alerter))
			}
		})
	}
}
