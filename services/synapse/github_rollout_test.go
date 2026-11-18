package main

import (
	"context"
	"errors"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func TestCommitSHAPRCacheHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		nilServer  bool
		repo       string
		sha        string
		prNumber   int
		branch     string
		corruptKey string
		corruptVal any
		wantLookup bool
		wantPR     int
		wantBranch string
	}{
		{
			name:       "nil receiver returns false on lookup and no-ops on record",
			nilServer:  true,
			repo:       "azylman/aerial",
			sha:        "abcdef123456",
			prNumber:   42,
			branch:     "feat/test",
			wantLookup: false,
		},
		{
			name:       "empty repo is no-op",
			repo:       "",
			sha:        "abcdef123456",
			prNumber:   42,
			branch:     "feat/test",
			wantLookup: false,
		},
		{
			name:       "empty sha is no-op",
			repo:       "azylman/aerial",
			sha:        "",
			prNumber:   42,
			branch:     "feat/test",
			wantLookup: false,
		},
		{
			name:       "zero sha is no-op",
			repo:       "azylman/aerial",
			sha:        "0000000000000000000000000000000000000000",
			prNumber:   42,
			branch:     "feat/test",
			wantLookup: false,
		},
		{
			name:       "invalid prNumber <= 0 is no-op",
			repo:       "azylman/aerial",
			sha:        "abcdef123456",
			prNumber:   0,
			branch:     "feat/test",
			wantLookup: false,
		},
		{
			name:       "valid record and lookup with trimming and case normalization",
			repo:       " Azylman/Aerial ",
			sha:        " ABCDEF123456 ",
			prNumber:   100,
			branch:     " feat/normalized ",
			wantLookup: true,
			wantPR:     100,
			wantBranch: "feat/normalized",
		},
		{
			name:       "corrupt cache entry type returns false",
			repo:       "azylman/aerial",
			sha:        "corrupttype",
			corruptKey: "azylman/aerial@corrupttype",
			corruptVal: "not-a-GitHubCheckRunPR",
			wantLookup: false,
		},
		{
			name:       "corrupt cache entry zero PR number returns false",
			repo:       "azylman/aerial",
			sha:        "corruptpr",
			corruptKey: "azylman/aerial@corruptpr",
			corruptVal: GitHubCheckRunPR{Number: 0},
			wantLookup: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var srv *RouterServer
			if !tc.nilServer {
				srv = &RouterServer{}
			}
			if tc.corruptKey != "" && srv != nil {
				srv.commitSHAPRs.Store(tc.corruptKey, tc.corruptVal)
			} else {
				srv.recordCommitSHAPR(tc.repo, tc.sha, tc.prNumber, tc.branch)
			}
			prNum, branch, ok := srv.lookupCommitSHAPR(tc.repo, tc.sha)
			if ok != tc.wantLookup {
				t.Fatalf("lookupCommitSHAPR() ok = %v, want %v", ok, tc.wantLookup)
			}
			if prNum != tc.wantPR {
				t.Errorf("lookupCommitSHAPR() prNum = %d, want %d", prNum, tc.wantPR)
			}
			if branch != tc.wantBranch {
				t.Errorf("lookupCommitSHAPR() branch = %q, want %q", branch, tc.wantBranch)
			}
		})
	}
}

func TestUpdatedForCurrentMainHelpers(t *testing.T) {
	t.Parallel()

	var nilSrv *RouterServer
	nilSrv.markUpdatedForCurrentMain("azylman/aerial", 123)
	if nilSrv.wasUpdatedForCurrentMain("azylman/aerial", 123) {
		t.Errorf("expected false for nil server")
	}
	nilSrv.clearUpdatedPRsForRepo("azylman/aerial")

	srv := &RouterServer{}
	srv.markUpdatedForCurrentMain("azylman/aerial", 0)
	if srv.wasUpdatedForCurrentMain("azylman/aerial", 0) {
		t.Errorf("expected false for pr <= 0")
	}

	srv.markUpdatedForCurrentMain("azylman/aerial", 42)
	srv.markUpdatedForCurrentMain("azylman/aerial", 43)
	srv.markUpdatedForCurrentMain("azylman/other", 99)

	if !srv.wasUpdatedForCurrentMain("azylman/aerial", 42) {
		t.Errorf("expected true for PR 42")
	}
	if !srv.wasUpdatedForCurrentMain("azylman/aerial", 43) {
		t.Errorf("expected true for PR 43")
	}
	if !srv.wasUpdatedForCurrentMain("azylman/other", 99) {
		t.Errorf("expected true for PR 99")
	}

	srv.clearUpdatedPRsForRepo("azylman/aerial")

	if srv.wasUpdatedForCurrentMain("azylman/aerial", 42) {
		t.Errorf("expected false for cleared PR 42")
	}
	if srv.wasUpdatedForCurrentMain("azylman/aerial", 43) {
		t.Errorf("expected false for cleared PR 43")
	}
	if !srv.wasUpdatedForCurrentMain("azylman/other", 99) {
		t.Errorf("expected true for remaining PR 99")
	}

	srv.markUpdatedForCurrentMain("azylman/aerial", 42)
	srv.clearUpdatedPRsForMainCommit("azylman/aerial", "mainsha1")
	if srv.wasUpdatedForCurrentMain("azylman/aerial", 42) {
		t.Errorf("expected false after new main commit mainsha1")
	}

	srv.markUpdatedForCurrentMain("azylman/aerial", 42)
	srv.clearUpdatedPRsForMainCommit("azylman/aerial", "mainsha1")
	if !srv.wasUpdatedForCurrentMain("azylman/aerial", 42) {
		t.Errorf("expected true on duplicate redelivery of same main commit mainsha1")
	}

	srv.clearUpdatedPRsForMainCommit("azylman/aerial", "mainsha2")
	if srv.wasUpdatedForCurrentMain("azylman/aerial", 42) {
		t.Errorf("expected false after advancing to mainsha2")
	}

	srv.markUpdatedForCurrentMain("azylman/aerial", 42)
	srv.clearUpdatedPRsForMainCommit("azylman/aerial", "mainsha1")
	if !srv.wasUpdatedForCurrentMain("azylman/aerial", 42) {
		t.Errorf("expected true on stale redelivery of earlier main commit mainsha1 after mainsha2")
	}
}

type failedCheckTrackerTestCase struct {
	name                    string
	nilServer               bool
	repo                    string
	prNumber                int
	headSHA                 string
	presetHeadSHA           string
	presetFailedChecks      []string
	presetClearChecks       []string
	wantEmitAdditional      []bool
	presetNonRoboRevBlocked bool
	evalCheck               string
	wantRecover             bool
}

func runFailedCheckTrackerCase(t *testing.T, tc failedCheckTrackerTestCase) {
	t.Helper()
	t.Parallel()
	var srv *RouterServer
	if !tc.nilServer {
		srv = &RouterServer{}
	}
	if srv != nil && tc.presetNonRoboRevBlocked {
		srv.nonRoboRevFailedSHAs.Store(prCheckKey(tc.repo, tc.prNumber), tc.presetHeadSHA)
	}
	if srv != nil && tc.presetHeadSHA != "" {
		for i, c := range tc.presetFailedChecks {
			emit := srv.recordFailedCheckRun(tc.repo, tc.prNumber, tc.presetHeadSHA, c)
			if i < len(tc.wantEmitAdditional) && emit != tc.wantEmitAdditional[i] {
				t.Errorf("recordFailedCheckRun(%q) emitAdditional = %v, want %v", c, emit, tc.wantEmitAdditional[i])
			}
		}
		for _, c := range tc.presetClearChecks {
			srv.clearActiveFailedCheckRun(tc.repo, tc.prNumber, tc.presetHeadSHA, c)
		}
	}
	var got bool
	switch {
	case isGateAggregatorCheck(tc.evalCheck):
		got = srv.canRecoverCheckSuccess(context.Background(), tc.repo, tc.prNumber, tc.evalCheck, tc.headSHA)
	case isRoboRevCheck(tc.evalCheck):
		got = srv.canRecoverRoboRevSuccess(context.Background(), tc.repo, tc.prNumber, tc.headSHA)
	default:
		got = srv.canRecoverNonGateCheckSuccess(tc.repo, tc.prNumber, tc.evalCheck, tc.headSHA)
	}
	if got != tc.wantRecover {
		t.Fatalf("canRecover(%q) = %v, want %v", tc.evalCheck, got, tc.wantRecover)
	}
}

func failedCheckTrackerCasesPart1() []failedCheckTrackerTestCase {
	return []failedCheckTrackerTestCase{
		{
			name:        "nil server returns false",
			nilServer:   true,
			repo:        "azylman/aerial",
			prNumber:    10,
			headSHA:     "sha10",
			evalCheck:   "Unit Tests",
			wantRecover: false,
		},
		{
			name:        "invalid PR number returns false",
			repo:        "azylman/aerial",
			prNumber:    0,
			headSHA:     "sha10",
			evalCheck:   "Unit Tests",
			wantRecover: false,
		},
		{
			name:        "empty head sha returns false",
			repo:        "azylman/aerial",
			prNumber:    10,
			headSHA:     "",
			evalCheck:   "Unit Tests",
			wantRecover: false,
		},
		{
			name:        "zero head sha returns false",
			repo:        "azylman/aerial",
			prNumber:    10,
			headSHA:     "0000000000000000000000000000000000000000",
			evalCheck:   "Unit Tests",
			wantRecover: false,
		},
		{
			name:        "empty check name returns false",
			repo:        "azylman/aerial",
			prNumber:    10,
			headSHA:     "sha10",
			evalCheck:   "",
			wantRecover: false,
		},
		{
			name:               "single check failure and rerun success recovers",
			repo:               "azylman/aerial",
			prNumber:           11,
			headSHA:            "sha11",
			presetHeadSHA:      "sha11",
			presetFailedChecks: []string{"Unit Tests"},
			evalCheck:          "Unit Tests",
			wantRecover:        true,
		},
		{
			name:               "unrelated check that never failed returns false",
			repo:               "azylman/aerial",
			prNumber:           12,
			headSHA:            "sha12",
			presetHeadSHA:      "sha12",
			presetFailedChecks: []string{"Unit Tests"},
			evalCheck:          "Lint",
			wantRecover:        false,
		},
		{
			name:               "mismatched sha returns false",
			repo:               "azylman/aerial",
			prNumber:           13,
			headSHA:            "sha13-new",
			presetHeadSHA:      "sha13-old",
			presetFailedChecks: []string{"Unit Tests"},
			evalCheck:          "Unit Tests",
			wantRecover:        false,
		},
	}
}

func failedCheckTrackerCasesPart2() []failedCheckTrackerTestCase {
	return []failedCheckTrackerTestCase{
		{
			name:               "two checks failed: one cleared active and one succeeds recovers",
			repo:               "azylman/aerial",
			prNumber:           14,
			headSHA:            "sha14",
			presetHeadSHA:      "sha14",
			presetFailedChecks: []string{"Lint", "Unit Tests"},
			presetClearChecks:  []string{"Lint"},
			evalCheck:          "Unit Tests",
			wantRecover:        true,
		},
		{
			name:               "two checks failed: one still active prevents recovery",
			repo:               "azylman/aerial",
			prNumber:           15,
			headSHA:            "sha15",
			presetHeadSHA:      "sha15",
			presetFailedChecks: []string{"Lint", "Unit Tests"},
			evalCheck:          "Unit Tests",
			wantRecover:        false,
		},
		{
			name:               "roborev failing first returns false, then unit tests returns emitAdditional true",
			repo:               "azylman/aerial",
			prNumber:           16,
			headSHA:            "sha16",
			presetHeadSHA:      "sha16",
			presetFailedChecks: []string{"roborev", "Unit Tests"},
			wantEmitAdditional: []bool{false, true},
			evalCheck:          "Unit Tests",
			wantRecover:        false,
		},
		{
			name:               "duplicate check failure for unit tests returns emitAdditional false",
			repo:               "azylman/aerial",
			prNumber:           17,
			headSHA:            "sha17",
			presetHeadSHA:      "sha17",
			presetFailedChecks: []string{"roborev", "Unit Tests", "Unit Tests"},
			wantEmitAdditional: []bool{false, true, false},
			evalCheck:          "Unit Tests",
			wantRecover:        false,
		},
		{
			name:                    "multi-check recovery: clearing roborev first leaves non-roborev active preventing roborev recovery",
			repo:                    "azylman/aerial",
			prNumber:                18,
			headSHA:                 "sha18",
			presetHeadSHA:           "sha18",
			presetFailedChecks:      []string{"roborev", "Unit Tests"},
			presetNonRoboRevBlocked: true,
			evalCheck:               "roborev",
			wantRecover:             false,
		},
		{
			name:               "multi-check recovery: clearing non-roborev clears blocker and allows roborev recovery",
			repo:               "azylman/aerial",
			prNumber:           19,
			headSHA:            "sha19",
			presetHeadSHA:      "sha19",
			presetFailedChecks: []string{"roborev", "Unit Tests"},
			presetClearChecks:  []string{"Unit Tests"},
			evalCheck:          "roborev",
			wantRecover:        true,
		},
		{
			name:                    "multi-check recovery: non-roborev succeeds after roborev cleared recovers",
			repo:                    "azylman/aerial",
			prNumber:                20,
			headSHA:                 "sha20",
			presetHeadSHA:           "sha20",
			presetFailedChecks:      []string{"roborev", "Unit Tests"},
			presetClearChecks:       []string{"roborev"},
			presetNonRoboRevBlocked: true,
			evalCheck:               "Unit Tests",
			wantRecover:             true,
		},
		{
			name:               "gate check success blocked when roborev has active failure",
			repo:               "azylman/aerial",
			prNumber:           21,
			headSHA:            "sha21",
			presetHeadSHA:      "sha21",
			presetFailedChecks: []string{"roborev"},
			evalCheck:          "CI Gate",
			wantRecover:        false,
		},
	}
}

func TestFailedCheckTrackerHelpers(t *testing.T) {
	t.Parallel()

	tests := append(failedCheckTrackerCasesPart1(), failedCheckTrackerCasesPart2()...)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runFailedCheckTrackerCase(t, tc)
		})
	}
}

func TestIsCachedFailedCISHA(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		nilServer  bool
		repo       string
		prNumber   int
		headSHA    string
		presetMap  string
		presetSHA  string
		wantCached bool
	}{
		{
			name:       "nil server returns false",
			nilServer:  true,
			repo:       "azylman/aerial",
			prNumber:   10,
			headSHA:    "sha10",
			wantCached: false,
		},
		{
			name:       "empty repo returns false",
			repo:       "",
			prNumber:   10,
			headSHA:    "sha10",
			wantCached: false,
		},
		{
			name:       "invalid prNumber <= 0 returns false",
			repo:       "azylman/aerial",
			prNumber:   0,
			headSHA:    "sha10",
			wantCached: false,
		},
		{
			name:       "empty head sha returns false",
			repo:       "azylman/aerial",
			prNumber:   10,
			headSHA:    "",
			wantCached: false,
		},
		{
			name:       "zero head sha returns false",
			repo:       "azylman/aerial",
			prNumber:   10,
			headSHA:    "0000000000000000000000000000000000000000",
			wantCached: false,
		},
		{
			name:       "uncached pr returns false",
			repo:       "azylman/aerial",
			prNumber:   10,
			headSHA:    "sha10",
			wantCached: false,
		},
		{
			name:       "cached in failedCheckSHAs returns true",
			repo:       "azylman/aerial",
			prNumber:   10,
			headSHA:    "sha10",
			presetMap:  "failedCheckSHAs",
			presetSHA:  "sha10",
			wantCached: true,
		},
		{
			name:       "cached in nonRoboRevFailedSHAs returns true",
			repo:       "azylman/aerial",
			prNumber:   10,
			headSHA:    "sha10",
			presetMap:  "nonRoboRevFailedSHAs",
			presetSHA:  "sha10",
			wantCached: true,
		},
		{
			name:       "cached in cancelledCheckSHAs returns true",
			repo:       "azylman/aerial",
			prNumber:   10,
			headSHA:    "sha10",
			presetMap:  "cancelledCheckSHAs",
			presetSHA:  "sha10",
			wantCached: true,
		},
		{
			name:       "cached sha mismatch returns false",
			repo:       "azylman/aerial",
			prNumber:   10,
			headSHA:    "sha10-new",
			presetMap:  "failedCheckSHAs",
			presetSHA:  "sha10-old",
			wantCached: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var srv *RouterServer
			if !tc.nilServer {
				srv = &RouterServer{}
				key := prCheckKey(tc.repo, tc.prNumber)
				switch tc.presetMap {
				case "failedCheckSHAs":
					srv.failedCheckSHAs.Store(key, tc.presetSHA)
				case "nonRoboRevFailedSHAs":
					srv.nonRoboRevFailedSHAs.Store(key, tc.presetSHA)
				case "cancelledCheckSHAs":
					srv.cancelledCheckSHAs.Store(key, tc.presetSHA)
				}
			}
			got := srv.isCachedFailedCISHA(tc.repo, tc.prNumber, tc.headSHA)
			if got != tc.wantCached {
				t.Fatalf("isCachedFailedCISHA() = %v, want %v", got, tc.wantCached)
			}
		})
	}
}

func TestParseAerialPRMetadata(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, body, wantTarget, wantBotID string
		wantOK                            bool
	}{
		{name: "no trailer", body: "Standard PR body\n<!-- regular comment -->"},
		{name: "unclosed trailer", body: "Body\n<!-- aerial-pr: {\"target_id\":\"100000000000000002\"}"},
		{name: "invalid json", body: "Body\n<!-- aerial-pr: {bad-json} -->"},
		{name: "invalid target and empty", body: "Body\n<!-- aerial-pr: {\"target_id\":\"bad\"} -->"},
		{name: "valid target", body: "<!-- x -->\n<!-- aerial-pr: {\"target_id\":\"100000000000000002\"} -->", wantTarget: "100000000000000002", wantOK: true},
		{name: "discord prefixed target and pool prefix", body: "<!-- aerial-pr: {\"target_id\":\"stateful_medium:discord-100000000000000002\"} -->", wantTarget: "100000000000000002", wantOK: true},
		{name: "valid bot id only", body: "<!-- aerial-pr: {\"bot_id\":\"eri-sub\"} -->", wantBotID: "eri-sub", wantOK: true},
		{name: "valid target and bot id", body: "<!-- aerial-pr: {\"target_id\":\"100000000000000002\",\"bot_id\":\"eri-sub\"} -->", wantTarget: "100000000000000002", wantBotID: "eri-sub", wantOK: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ParseAerialPRMetadata(tc.body)
			if got.OK() != tc.wantOK || got.TargetID != tc.wantTarget || got.BotID != tc.wantBotID {
				t.Fatalf("ParseAerialPRMetadata() = %+v (ok=%v), want target=%q bot=%q ok=%v", got, got.OK(), tc.wantTarget, tc.wantBotID, tc.wantOK)
			}
		})
	}
}

func TestClassifyCheckState_Unit(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		status     string
		conclusion string
		want       string
	}{
		{name: "completed success", status: "completed", conclusion: "success", want: "passed"},
		{name: "completed neutral", status: "completed", conclusion: "neutral", want: "passed"},
		{name: "completed skipped", status: "completed", conclusion: "skipped", want: "passed"},
		{name: "completed failure", status: "completed", conclusion: "failure", want: "failed"},
		{name: "completed timed_out", status: "completed", conclusion: "timed_out", want: "failed"},
		{name: "completed cancelled", status: "completed", conclusion: "cancelled", want: "failed"},
		{name: "completed action_required", status: "completed", conclusion: "action_required", want: "failed"},
		{name: "completed unknown conclusion", status: "completed", conclusion: "stale", want: ""},
		{name: "queued", status: "queued", want: "pending"},
		{name: "in_progress", status: "in_progress", want: "pending"},
		{name: "waiting", status: "waiting", want: "pending"},
		{name: "requested", status: "requested", want: "pending"},
		{name: "pending", status: "pending", want: "pending"},
		{name: "other non-completed", status: "custom_status", want: "pending"},
		{name: "empty status", status: "", want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := classifyCheckState(tc.status, tc.conclusion)
			if got != tc.want {
				t.Fatalf("classifyCheckState(%q, %q) = %q, want %q", tc.status, tc.conclusion, got, tc.want)
			}
		})
	}
}

func TestExtractStringMapFromMetadata_Unit(t *testing.T) {
	t.Parallel()

	if res := extractStringMapFromMetadata(nil, "key"); res != nil {
		t.Errorf("expected nil for nil meta, got %v", res)
	}
	if res := extractStringMapFromMetadata(map[string]any{}, "key"); res != nil {
		t.Errorf("expected nil for missing key, got %v", res)
	}

	paddedKey := " ERI "
	metaObj := map[string]any{
		"job_states": map[string]any{
			paddedKey: "verifying_health",
			"":        "invalid",
			"HARO":    "starting",
		},
	}
	gotObj := extractStringMapFromMetadata(metaObj, "job_states")
	if gotObj["eri"] != "verifying_health" || gotObj["haro"] != "starting" {
		t.Errorf("unexpected map from interface: %+v", gotObj)
	}

	metaStr := map[string]any{
		"job_states": map[string]string{
			" CADVISOR ": " healthy ",
		},
	}
	gotStr := extractStringMapFromMetadata(metaStr, "job_states")
	if gotStr["cadvisor"] != "healthy" {
		t.Errorf("unexpected map from string map: %+v", gotStr)
	}
}

func TestRecordFailedCISHA_Unit(t *testing.T) {
	t.Parallel()
	var nilSrv *RouterServer
	if nilSrv.recordFailedCISHA("azylman/aerial", 42, "sha1") {
		t.Errorf("expected false for nil server")
	}
	nilSrv.clearFailedCISHA("azylman/aerial", 42)

	srv := &RouterServer{}
	if srv.recordFailedCISHA("", 42, "sha1") {
		t.Errorf("expected false for empty repo")
	}
	if srv.recordFailedCISHA("azylman/aerial", 0, "sha1") {
		t.Errorf("expected false for pr <= 0")
	}
	if srv.recordFailedCISHA("azylman/aerial", 42, "") {
		t.Errorf("expected false for empty sha")
	}
	if srv.recordFailedCISHA("azylman/aerial", 42, "0000000000000000000000000000000000000000") {
		t.Errorf("expected false for zero sha")
	}

	if !srv.recordFailedCISHA("azylman/aerial", 42, "sha1") {
		t.Errorf("expected true on initial record")
	}
	if srv.recordFailedCISHA("azylman/aerial", 42, "sha1") {
		t.Errorf("expected false on duplicate record")
	}

	srv.clearFailedCISHA("azylman/aerial", 0)
	srv.clearFailedCISHA("azylman/aerial", 42)
	if srv.isCachedFailedCISHA("azylman/aerial", 42, "sha1") {
		t.Errorf("expected sha1 to be cleared")
	}
}

func TestPublishDeployLifecycleEvent_Unit(t *testing.T) {
	t.Parallel()
	bus := &mockEventBus{}
	srv := &RouterServer{bus: bus}
	evt := DeployEvent{
		Repo:     "azylman/aerial",
		PRNumber: 42,
	}
	if err := srv.publishDeployLifecycleEvent(context.Background(), evt, "deploy_success", "deployed"); err != nil {
		t.Fatalf("publishDeployLifecycleEvent() err = %v", err)
	}
	if len(bus.publishedPREvents) != 1 || bus.publishedPREvents[0].JobName != "git-sync" {
		t.Errorf("expected jobName git-sync, got %v", bus.publishedPREvents)
	}
}

func TestFailedCheckRunTracking_Validation(t *testing.T) {
	t.Parallel()
	srv := &RouterServer{}
	repo := "azylman/aerial"
	pr := 42
	sha1 := "abc1234"

	var nilSrv *RouterServer
	nilSrv.recordFailedCheckRun(repo, pr, sha1, "lint")
	if nilSrv.loadMatchingFailedCheckState(repo, pr, sha1) != nil {
		t.Errorf("expected nil for nil server")
	}
	if nilSrv.hasActiveRoboRevFailure(repo, pr, sha1) {
		t.Errorf("expected false for nil server hasActiveRoboRevFailure")
	}
	if srv.hasActiveRoboRevFailure("", pr, sha1) {
		t.Errorf("expected false for empty repo")
	}
	if srv.hasActiveRoboRevFailure(repo, 0, sha1) {
		t.Errorf("expected false for pr <= 0")
	}
	if srv.hasActiveRoboRevFailure(repo, pr, "") {
		t.Errorf("expected false for empty sha")
	}
	if srv.hasActiveRoboRevFailure(repo, pr, sha1) {
		t.Errorf("expected false before any failures recorded")
	}

	srv.recordFailedCheckRun("", pr, sha1, "lint")
	srv.recordFailedCheckRun(repo, 0, sha1, "lint")
	srv.recordFailedCheckRun(repo, pr, "", "lint")
	srv.recordFailedCheckRun(repo, pr, "0000000000000000000000000000000000000000", "lint")
	srv.recordFailedCheckRun(repo, pr, sha1, "")
}

func TestFailedCheckRunTracking_Flow(t *testing.T) {
	t.Parallel()
	srv := &RouterServer{}
	repo := "azylman/aerial"
	pr := 42
	sha1 := "abc1234"
	sha2 := "def5678"

	if srv.recordFailedCheckRun(repo, pr, sha1, "roborev") {
		t.Errorf("expected false for first failure on sha1")
	}
	if !srv.hasActiveRoboRevFailure(repo, pr, sha1) {
		t.Errorf("expected true after recording roborev failure")
	}
	if !srv.recordFailedCheckRun(repo, pr, sha1, "lint") {
		t.Errorf("expected true for second distinct failure on sha1")
	}
	if srv.recordFailedCheckRun(repo, pr, sha1, "lint") {
		t.Errorf("expected false for duplicate failure")
	}
	if !srv.recordFailedCheckRun(repo, pr, sha1, "unit") {
		t.Errorf("expected true for third distinct failure on sha1")
	}

	state := srv.loadMatchingFailedCheckState(repo, pr, sha1)
	if state == nil || len(state.activeFailed) != 2 || !state.roboRevActiveFailed {
		t.Fatalf("expected state with 2 active non-roborev and roborev active failed, got %v", state)
	}

	srv.clearActiveFailedCheckRun(repo, pr, sha1, "roborev")
	if srv.hasActiveRoboRevFailure(repo, pr, sha1) {
		t.Errorf("expected false after clearing roborev active failure")
	}
	srv.clearActiveFailedCheckRun(repo, pr, sha1, "lint")
	srv.clearActiveFailedCheckRun(repo, pr, sha1, "")
	srv.clearActiveFailedCheckRun(repo, pr, "other-sha", "unit")

	if srv.canRecoverNonGateCheckSuccess(repo, pr, "", sha1) {
		t.Errorf("expected false for empty check")
	}
	if srv.canRecoverNonGateCheckSuccess(repo, pr, "non-existent", sha1) {
		t.Errorf("expected false for never-failed check")
	}
	if !srv.canRecoverNonGateCheckSuccess(repo, pr, "unit", sha1) {
		t.Errorf("expected true recovering last active failed check")
	}

	srv.recordFailedCheckRun(repo, pr, sha2, "lint")
	state2 := srv.loadMatchingFailedCheckState(repo, pr, sha2)
	if state2 == nil || state2.headSHA != sha2 || len(state2.activeFailed) != 1 {
		t.Fatalf("expected reset state for sha2, got %v", state2)
	}

	if srv.hasCancelledCheckForSHA(repo, pr, sha1) {
		t.Errorf("expected false when no cancelled check")
	}
	srv.cancelledCheckSHAs.Store(prCheckKey(repo, pr), sha1)
	if !srv.hasCancelledCheckForSHA(repo, pr, sha1) {
		t.Errorf("expected true after storing cancelled check")
	}
}

type ownedMapRegistry struct {
	mockPRRegistry

	ownedPRs  map[int]bool
	ownedSHAs map[string]int
}

func (r *ownedMapRegistry) ResolvePRByNumber(ctx context.Context, repo string, prNumber int) (string, string, string, string, error) {
	if err := r.checkErr(r.resolveErr); err != nil {
		return "", "", "", "", err
	}
	if r.ownedPRs != nil && r.ownedPRs[prNumber] {
		return "100000000000000001", "feat/test", "sha123", "", nil
	}
	return "", "", "", "", nil
}

func (r *ownedMapRegistry) ResolvePRBySHA(ctx context.Context, repo string, sha string) (int, string, string, error) {
	if err := r.checkErr(r.resolveErr); err != nil {
		return 0, "", "", err
	}
	if r.ownedSHAs != nil {
		if prNum, ok := r.ownedSHAs[sha]; ok {
			return prNum, "feat/test", "100000000000000001", nil
		}
	}
	return 0, "", "", nil
}

func TestHandlePullRequestRegistryAction_TrailerScoping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		botID          string
		defaultOwner   bool
		prBody         string
		action         string
		merged         bool
		wantRegistered bool
		wantClosed     bool
		wantEvents     []wantPREvent
	}{
		{
			name:           "matching bot_id registered",
			botID:          "aerial",
			defaultOwner:   false,
			prBody:         "Some PR\n\n<!-- aerial-pr:{\"target_id\":\"123456789012345678\",\"bot_id\":\"aerial\"} -->",
			wantRegistered: true,
		},
		{
			name:           "case-insensitive matching bot_id registered",
			botID:          "aerial",
			defaultOwner:   false,
			prBody:         "Some PR\n\n<!-- aerial-pr:{\"target_id\":\"123456789012345678\",\"bot_id\":\"AERIAL\"} -->",
			wantRegistered: true,
		},
		{
			name:           "peer bot_id skipped",
			botID:          "aerial",
			defaultOwner:   true,
			prBody:         "Some PR\n\n<!-- aerial-pr:{\"target_id\":\"123456789012345678\",\"bot_id\":\"coven\"} -->",
			wantRegistered: false,
		},
		{
			name:           "untagged registered when default owner",
			botID:          "aerial",
			defaultOwner:   true,
			prBody:         "Some PR\n\n<!-- aerial-pr:{\"target_id\":\"123456789012345678\"} -->",
			wantRegistered: true,
		},
		{
			name:           "untagged skipped when not default owner",
			botID:          "aerial",
			defaultOwner:   false,
			prBody:         "Some PR\n\n<!-- aerial-pr:{\"target_id\":\"123456789012345678\"} -->",
			wantRegistered: false,
		},
		{
			name:           "closed unmerged pr skips registration and marks pr closed",
			botID:          "aerial",
			defaultOwner:   true,
			prBody:         "Some PR\n\n<!-- aerial-pr:{\"target_id\":\"123456789012345678\",\"bot_id\":\"aerial\"} -->",
			action:         "closed",
			merged:         false,
			wantRegistered: false,
			wantClosed:     true,
			wantEvents:     []wantPREvent{{Event: "closed_unmerged", PRNumber: 101, HeadSHA: "sha101", Status: "closed_unmerged"}},
		},
		{
			name:           "untagged synchronize skips registration and publishes synchronize",
			botID:          "aerial",
			defaultOwner:   false,
			action:         "synchronize",
			wantRegistered: false,
			wantEvents:     []wantPREvent{{Event: "synchronize", PRNumber: 101, HeadSHA: "sha101", Status: "open", Title: "Test PR"}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			reg := &mockPRRegistry{}
			cfg := Config{
				BotID:        tc.botID,
				DefaultOwner: tc.defaultOwner,
			}
			bus := &mockEventBus{}
			srv := newTestRouter(cfg, nil, reg, nil, bus)
			action := tc.action
			if action == "" {
				action = "opened"
			}
			res := &ResolvedGitHubEvent{
				Repo:     "azylman/aerial",
				PRNumber: 101,
				Branch:   "feat/something",
				HeadSHA:  "sha101",
				Action:   action,
			}
			pr := GitHubPullRequest{
				Number: 101,
				Title:  "Test PR",
				Body:   tc.prBody,
				Merged: tc.merged,
			}

			err := srv.handlePullRequestRegistryAction(context.Background(), res, pr)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantRegistered && len(reg.ensureCalls) == 0 {
				t.Fatalf("expected PR to be registered, but EnsurePRRegistered was not called")
			}
			if !tc.wantRegistered && len(reg.ensureCalls) > 0 {
				t.Fatalf("expected PR NOT to be registered, but EnsurePRRegistered was called %d times", len(reg.ensureCalls))
			}
			if tc.wantClosed && len(reg.closedUnmergedCalls) == 0 {
				t.Fatalf("expected PR to be marked closed unmerged, but UpdatePRClosedUnmerged was not called")
			}
			if !tc.wantClosed && len(reg.closedUnmergedCalls) > 0 {
				t.Fatalf("expected PR NOT to be marked closed unmerged, but UpdatePRClosedUnmerged was called %d times", len(reg.closedUnmergedCalls))
			}
			assertPREvents(t, bus, tc.wantEvents)
		})
	}
}

func TestHandleGitHubWebhook_TeeAndForwarding(t *testing.T) {
	t.Parallel()

	tests := teeAndForwardingTestCases()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var botACalls, botBCalls atomic.Int32
			var receivedEventA, receivedDeliveryA string
			var mu sync.Mutex

			serverA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				botACalls.Add(1)
				mu.Lock()
				receivedEventA = r.Header.Get("X-Github-Event")
				receivedDeliveryA = r.Header.Get("X-Github-Delivery")
				mu.Unlock()
				w.WriteHeader(http.StatusOK)
			}))
			defer serverA.Close()

			serverB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				botBCalls.Add(1)
				w.WriteHeader(http.StatusOK)
			}))
			defer serverB.Close()

			reg := &ownedMapRegistry{
				ownedPRs:  tc.ownedPRs,
				ownedSHAs: tc.ownedSHAs,
			}
			cfg := Config{
				BotID:               "aerial",
				GitHubWebhookSecret: testGitHubWebhookSecret,
				WebhookForwardURLs: map[string]string{
					"bot_a":  serverA.URL,
					"bot_b":  serverB.URL,
					"aerial": "http://127.0.0.1:9999", // self, must never be called
				},
			}
			bus := &mockEventBus{}
			srv := newTestRouter(cfg, serverA.Client(), reg, nil, bus)

			headers := map[string]string{
				"X-Github-Event":      tc.event,
				"X-Github-Delivery":   "del-test-1234",
				"X-Hub-Signature-256": signTestGitHubWebhook(testGitHubWebhookSecret, tc.payload),
				"Content-Type":        "application/json",
			}
			rec := doHTTPRequest(srv.Routes(), http.MethodPost, "/api/webhooks/github", strings.NewReader(tc.payload), headers)
			if rec.Code != http.StatusOK {
				t.Fatalf("expected status 200, got %d: %s", rec.Code, rec.Body.String())
			}

			if int(botACalls.Load()) != tc.wantBotACalls {
				t.Errorf("bot_a calls = %d, want %d", botACalls.Load(), tc.wantBotACalls)
			}
			if int(botBCalls.Load()) != tc.wantBotBCalls {
				t.Errorf("bot_b calls = %d, want %d", botBCalls.Load(), tc.wantBotBCalls)
			}

			if tc.wantBotACalls > 0 {
				mu.Lock()
				defer mu.Unlock()
				if receivedEventA != tc.event {
					t.Errorf("received X-Github-Event = %q, want %q", receivedEventA, tc.event)
				}
				if receivedDeliveryA != "del-test-1234" {
					t.Errorf("received X-Github-Delivery = %q, want del-test-1234", receivedDeliveryA)
				}
			}
		})
	}
}

type teeAndForwardingTestCase struct {
	name          string
	event         string
	payload       string
	ownedPRs      map[int]bool
	ownedSHAs     map[string]int
	wantBotACalls int
	wantBotBCalls int
}

func teeAndForwardingTestCases() []teeAndForwardingTestCase {
	return []teeAndForwardingTestCase{
		{
			name:          "pull_request tagged with bot_a forwards strictly to bot_a",
			event:         "pull_request",
			payload:       `{"number": 101, "pull_request": {"body": "PR<!-- aerial-pr:{\"bot_id\":\"bot_a\"} -->"}, "repository": {"full_name": "azylman/aerial"}}`,
			wantBotACalls: 1,
			wantBotBCalls: 0,
		},
		{
			name:          "pull_request tagged with self does not forward",
			event:         "pull_request",
			payload:       `{"number": 102, "pull_request": {"body": "PR<!-- aerial-pr:{\"bot_id\":\"aerial\"} -->"}, "repository": {"full_name": "azylman/aerial"}}`,
			wantBotACalls: 0,
			wantBotBCalls: 0,
		},
		{
			name:          "pull_request untagged and locally owned does not forward",
			event:         "pull_request",
			payload:       `{"number": 103, "pull_request": {"body": "Plain PR"}, "repository": {"full_name": "azylman/aerial"}}`,
			ownedPRs:      map[int]bool{103: true},
			wantBotACalls: 0,
			wantBotBCalls: 0,
		},
		{
			name:          "pull_request untagged and not locally owned forwards to all peers",
			event:         "pull_request",
			payload:       `{"number": 104, "pull_request": {"body": "Plain PR"}, "repository": {"full_name": "azylman/aerial"}}`,
			ownedPRs:      map[int]bool{},
			wantBotACalls: 1,
			wantBotBCalls: 1,
		},
		{
			name:          "push to main forwards to all peers",
			event:         "push",
			payload:       `{"ref": "refs/heads/main", "after": "sha_main", "repository": {"full_name": "azylman/aerial"}}`,
			wantBotACalls: 1,
			wantBotBCalls: 1,
		},
		{
			name:          "push to feature branch locally owned does not forward",
			event:         "push",
			payload:       `{"ref": "refs/heads/feat/mine", "after": "sha_owned", "repository": {"full_name": "azylman/aerial"}}`,
			ownedSHAs:     map[string]int{"sha_owned": 105},
			ownedPRs:      map[int]bool{105: true},
			wantBotACalls: 0,
			wantBotBCalls: 0,
		},
		{
			name:          "push to feature branch unowned forwards to all peers",
			event:         "push",
			payload:       `{"ref": "refs/heads/feat/other", "after": "sha_unowned", "repository": {"full_name": "azylman/aerial"}}`,
			wantBotACalls: 1,
			wantBotBCalls: 1,
		},
		{
			name:          "check_run locally owned does not forward",
			event:         "check_run",
			payload:       `{"check_run": {"head_sha": "sha_cr_owned", "pull_requests": [{"number": 106}]}, "repository": {"full_name": "azylman/aerial"}}`,
			ownedPRs:      map[int]bool{106: true},
			wantBotACalls: 0,
			wantBotBCalls: 0,
		},
		{
			name:          "check_run unowned forwards to all peers",
			event:         "check_run",
			payload:       `{"check_run": {"head_sha": "sha_cr_unowned", "pull_requests": [{"number": 107}]}, "repository": {"full_name": "azylman/aerial"}}`,
			wantBotACalls: 1,
			wantBotBCalls: 1,
		},
		{
			name:          "workflow_run locally owned does not forward",
			event:         "workflow_run",
			payload:       `{"workflow_run": {"head_sha": "sha_wf_owned", "pull_requests": [{"number": 108}]}, "repository": {"full_name": "azylman/aerial"}}`,
			ownedPRs:      map[int]bool{108: true},
			wantBotACalls: 0,
			wantBotBCalls: 0,
		},
		{
			name:          "workflow_run unowned forwards to all peers",
			event:         "workflow_run",
			payload:       `{"workflow_run": {"head_sha": "sha_wf_unowned", "pull_requests": [{"number": 109}]}, "repository": {"full_name": "azylman/aerial"}}`,
			wantBotACalls: 1,
			wantBotBCalls: 1,
		},
	}
}

type kubeRolloutTestCase struct {
	name           string
	routes         map[string]kubeRoute
	wantReconciled []string
	wantRecorded   []string
	wantErr        bool
}

func assertKubeRolloutRequests(t *testing.T, fixture *kubeFixture, tc kubeRolloutTestCase, reg *mockPRRegistry, configMapPath, kustPrefix string) {
	t.Helper()
	lastTagWrite, firstReconcile := -1, -1
	var reconciled []string
	for i, req := range fixture.Requests() {
		switch {
		case req == configMapPath:
			lastTagWrite = i
		case strings.HasPrefix(req, kustPrefix):
			if firstReconcile < 0 {
				firstReconcile = i
			}
			reconciled = append(reconciled, strings.TrimPrefix(req, kustPrefix))
		}
	}
	if lastTagWrite < 0 || firstReconcile <= lastTagWrite {
		t.Errorf("requests %v: want every configmap write before the first reconcile", fixture.Requests())
	}
	slices.Sort(reconciled)
	if !slices.Equal(reconciled, tc.wantReconciled) {
		t.Errorf("reconciled = %v, want %v", reconciled, tc.wantReconciled)
	}
	if len(reg.deployingCalls) != 1 || !slices.Equal(reg.deployingCalls[0].jobs, []string{"synapse", "eri"}) {
		t.Errorf("deploying calls = %+v, want one call with jobs [synapse eri]", reg.deployingCalls)
	}
	assertKubeRolloutRecorded(t, fixture, tc, reg, kustPrefix)
}

func assertKubeRolloutRecorded(t *testing.T, fixture *kubeFixture, tc kubeRolloutTestCase, reg *mockPRRegistry, kustPrefix string) {
	t.Helper()
	if len(reg.kustomizationRequestCalls) != 1 {
		t.Fatalf("recorded request calls = %d, want 1", len(reg.kustomizationRequestCalls))
	}
	recorded := reg.kustomizationRequestCalls[0]
	if got := slices.Sorted(maps.Keys(recorded)); !slices.Equal(got, tc.wantRecorded) {
		t.Errorf("recorded kustomizations = %v, want %v", got, tc.wantRecorded)
	}
	for name, req := range recorded {
		if req.Namespace != "flux-system" || !strings.Contains(fixture.Body(kustPrefix+name), `"`+req.RequestedAt+`"`) {
			t.Errorf("recorded %s = %+v, want namespace flux-system and the patched requestedAt (body %s)", name, req, fixture.Body(kustPrefix+name))
		}
	}
}

func runKubeRolloutCase(t *testing.T, tc kubeRolloutTestCase, configMapPath, kustPrefix string) {
	t.Helper()
	t.Parallel()
	fixture, ts := newKubeFixture(t, tc.routes)
	disp := NewDefaultOutboundDispatcher("", ts.Client())
	disp.SetKubeConfig(ts.URL, "test-token")
	reg := &mockPRRegistry{resolveTargetID: "100000000000000001"}
	srv := newTestRouter(legacyFixtureConfig(Config{GitHubAPIURL: ts.URL, DefaultOwner: true}), legacyFixtureClient(ts.Client()), reg, disp)

	err := srv.dispatchWorkflowRunImages(context.Background(), "azylman/aerial", workflowRunEvent{ID: 7, Attempt: 1, HeadSHA: legacyFixtureHead}, "main", "success", "100000000000000001", 42)
	if (err != nil) != tc.wantErr {
		t.Fatalf("err = %v, wantErr = %v", err, tc.wantErr)
	}
	assertKubeRolloutRequests(t, fixture, tc, reg, configMapPath, kustPrefix)
}

func TestDispatchWorkflowRunImages_KubeRollout(t *testing.T) {
	t.Parallel()
	const (
		jobsPath      = "GET /repos/azylman/aerial/actions/runs/7/jobs"
		configMapPath = "PATCH /api/v1/namespaces/flux-system/configmaps/aerial-image-tags"
		kustPrefix    = "PATCH /apis/kustomize.toolkit.fluxcd.io/v1/namespaces/flux-system/kustomizations/"
	)
	twoImages := `{"jobs":[{"name":"Build & Push Images to GHCR (synapse)","status":"completed","conclusion":"success"},{"name":"Build & Push Images to GHCR (eri)","status":"completed","conclusion":"success"}]}`
	ownedDeployments := `{"items":[
		{"metadata":{"labels":{"kustomize.toolkit.fluxcd.io/name":"aerial-core","kustomize.toolkit.fluxcd.io/namespace":"flux-system"}},"spec":{"template":{"spec":{"containers":[{"image":"ghcr.io/azylman/aerial-synapse:old"},{"image":"ghcr.io/azylman/aerial-eri:old"}]}}}},
		{"metadata":{"labels":{"kustomize.toolkit.fluxcd.io/name":"marida-core","kustomize.toolkit.fluxcd.io/namespace":"flux-system"}},"spec":{"template":{"spec":{"containers":[{"image":"ghcr.io/azylman/aerial-synapse:old"}]}}}}
	]}`
	fallbackKustomizations := `{"items":[{"metadata":{"name":"aerial-config","namespace":"flux-system"},"spec":{"postBuild":{"substituteFrom":[{"kind":"ConfigMap","name":"aerial-image-tags"}]}}}]}`

	tests := []kubeRolloutTestCase{
		{
			name: "every owning kustomization reconciles once after all tags are written",
			routes: kubeWorkloadRoutes(map[string]kubeRoute{
				jobsPath:                   {body: twoImages},
				kubeDeploymentsPath:        {body: ownedDeployments},
				configMapPath:              {body: `{}`},
				kustPrefix + "aerial-core": {body: `{}`},
				kustPrefix + "marida-core": {body: `{}`},
			}),
			wantReconciled: []string{"aerial-core", "marida-core"},
			wantRecorded:   []string{"aerial-core", "marida-core"},
		},
		{
			name: "no labeled owner falls back to image tag substituting kustomizations",
			routes: kubeWorkloadRoutes(map[string]kubeRoute{
				jobsPath:                     {body: twoImages},
				kubeKustomizations:           {body: fallbackKustomizations},
				configMapPath:                {body: `{}`},
				kustPrefix + "aerial-config": {body: `{}`},
			}),
			wantReconciled: []string{"aerial-config"},
			wantRecorded:   []string{"aerial-config"},
		},
		{
			name: "partial reconcile failure still records every request before patching",
			routes: kubeWorkloadRoutes(map[string]kubeRoute{
				jobsPath:                   {body: twoImages},
				kubeDeploymentsPath:        {body: ownedDeployments},
				configMapPath:              {body: `{}`},
				kustPrefix + "aerial-core": {status: http.StatusForbidden, body: `forbidden`},
				kustPrefix + "marida-core": {body: `{}`},
			}),
			wantReconciled: []string{"aerial-core", "marida-core"},
			wantRecorded:   []string{"aerial-core", "marida-core"},
			wantErr:        true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runKubeRolloutCase(t, tc, configMapPath, kustPrefix)
		})
	}
}

func TestHasFailedCIForSHA_ResponseHandling(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		status     int
		body       string
		wantResult bool
	}{
		{
			name:       "404 not found treats as not failed",
			status:     http.StatusNotFound,
			body:       `{"message":"Not Found"}`,
			wantResult: false,
		},
		{
			name:       "500 internal server error fails closed",
			status:     http.StatusInternalServerError,
			body:       `{"message":"Internal Server Error"}`,
			wantResult: true,
		},
		{
			name:       "403 rate limited fails closed",
			status:     http.StatusForbidden,
			body:       `{"message":"API rate limit exceeded"}`,
			wantResult: true,
		},
		{
			name:       "200 success with failed check returns true",
			status:     http.StatusOK,
			body:       `{"check_runs":[{"name":"Unit Tests","status":"completed","conclusion":"failure"}]}`,
			wantResult: true,
		},
		{
			name:       "200 success with pending check returns true",
			status:     http.StatusOK,
			body:       `{"check_runs":[{"name":"Unit Tests","status":"in_progress"}]}`,
			wantResult: true,
		},
		{
			name:       "200 success with all passed returns false",
			status:     http.StatusOK,
			body:       `{"check_runs":[{"name":"Unit Tests","status":"completed","conclusion":"success"}]}`,
			wantResult: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer ts.Close()

			srv := newTestRouter(Config{GitHubAPIURL: ts.URL}, ts.Client(), nil, nil)
			got := srv.hasFailedCIForSHA(context.Background(), "azylman/aerial", 42, "head123")
			if got != tc.wantResult {
				t.Errorf("hasFailedCIForSHA() = %v, want %v", got, tc.wantResult)
			}
		})
	}
}

func TestTransitionRegisteredConflict_DedupAndRetry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                string
		initialConflictInDB bool
		firstCallUpdated    bool
		firstSHA            string
		secondSHA           string
		callClearFailedCI   bool
		wantFirstHandled    bool
		wantSecondHandled   bool
		wantPublishedCount  int
	}{
		{
			name:                "first transition publishes event and second call with same sha dedupes",
			initialConflictInDB: false,
			firstCallUpdated:    true,
			firstSHA:            "sha-conflict-1",
			secondSHA:           "sha-conflict-1",
			wantFirstHandled:    true,
			wantSecondHandled:   true,
			wantPublishedCount:  1,
		},
		{
			name:                "already in conflict status publishes event once and dedupes on retry",
			initialConflictInDB: true,
			firstCallUpdated:    false,
			firstSHA:            "sha-conflict-1",
			secondSHA:           "sha-conflict-1",
			wantFirstHandled:    true,
			wantSecondHandled:   true,
			wantPublishedCount:  1,
		},
		{
			name:                "different head sha re-publishes conflict event",
			initialConflictInDB: true,
			firstCallUpdated:    false,
			firstSHA:            "sha-conflict-1",
			secondSHA:           "sha-conflict-2",
			wantFirstHandled:    true,
			wantSecondHandled:   true,
			wantPublishedCount:  2,
		},
		{
			name:                "clearing failed CI SHA resets dedup map allowing re-publish",
			initialConflictInDB: true,
			firstCallUpdated:    false,
			firstSHA:            "sha-conflict-1",
			secondSHA:           "sha-conflict-1",
			callClearFailedCI:   true,
			wantFirstHandled:    true,
			wantSecondHandled:   true,
			wantPublishedCount:  2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bus := &mockEventBus{}
			reg := &mockPRRegistry{
				conflictTargetID: "100000000000000001",
				conflictUpdated:  tc.firstCallUpdated,
			}
			if tc.initialConflictInDB {
				reg.resolveTargetID = "100000000000000001"
			}
			srv := newTestRouter(Config{}, nil, reg, nil, bus)

			handled1, err1 := srv.transitionRegisteredConflict(context.Background(), "azylman/aerial", 42, "feat/branch", tc.firstSHA)
			if err1 != nil {
				t.Fatalf("first transitionRegisteredConflict error = %v", err1)
			}
			if handled1 != tc.wantFirstHandled {
				t.Errorf("first handled = %v, want %v", handled1, tc.wantFirstHandled)
			}

			if tc.callClearFailedCI {
				srv.clearFailedCISHA("azylman/aerial", 42)
			}

			reg.conflictUpdated = false
			reg.resolveTargetID = "100000000000000001"

			handled2, err2 := srv.transitionRegisteredConflict(context.Background(), "azylman/aerial", 42, "feat/branch", tc.secondSHA)
			if err2 != nil {
				t.Fatalf("second transitionRegisteredConflict error = %v", err2)
			}
			if handled2 != tc.wantSecondHandled {
				t.Errorf("second handled = %v, want %v", handled2, tc.wantSecondHandled)
			}

			if len(bus.publishedPREvents) != tc.wantPublishedCount {
				t.Errorf("published events count = %d, want %d", len(bus.publishedPREvents), tc.wantPublishedCount)
			}
		})
	}
}

func TestCIFailureTransition_RollbackAndRetry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{
			name: "publish error rolls back check state and retry succeeds when already ci_failed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bus := &mockEventBus{err: errors.New("nats publish failed")}
			reg := &mockPRRegistry{
				ciFailedTargetID: "100000000000000001",
				ciFailedUpdated:  false,
				resolveHeadSHA:   "sha-ci-fail",
				buildLifecycle:   PRBuildLifecycle{Status: prCIFailed},
			}
			srv := newTestRouter(Config{}, nil, reg, nil, bus)

			res := &ResolvedGitHubEvent{
				Event:      ghEventCheckRun,
				Repo:       "azylman/aerial",
				PRNumber:   42,
				HeadSHA:    "sha-ci-fail",
				CheckName:  "unit-tests",
				Conclusion: conclusionFailure,
			}

			err1 := srv.handleCIFailureTransition(context.Background(), res, "test failure", "https://ci/1")
			if err1 == nil {
				t.Fatalf("first handleCIFailureTransition err = nil, want error")
			}

			entry := srv.loadMatchingFailedCheckState(res.Repo, res.PRNumber, res.HeadSHA)
			if entry == nil {
				t.Fatalf("expected non-nil failedCheckState entry")
			}
			if entry.activeFailed["unit-tests"] {
				t.Fatalf("expected active failure for unit-tests to be rolled back, got %v", entry.activeFailed)
			}

			bus.err = nil
			reg.ciFailedUpdated = false

			resRetry := &ResolvedGitHubEvent{
				Event:      ghEventCheckRun,
				Repo:       "azylman/aerial",
				PRNumber:   42,
				HeadSHA:    "sha-ci-fail",
				CheckName:  "unit-tests",
				Conclusion: conclusionFailure,
			}
			err2 := srv.handleCIFailureTransition(context.Background(), resRetry, "test failure", "https://ci/1")
			if err2 != nil {
				t.Fatalf("retry handleCIFailureTransition err = %v, want nil", err2)
			}
			if len(bus.publishedPREvents) != 1 {
				t.Errorf("published events = %d, want 1", len(bus.publishedPREvents))
			}
		})
	}
}
