package main

import (
	"context"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
)

type failedCheckTrackerTestCase struct {
	name                    string
	nilServer               bool
	repo                    string
	prNumber                int
	headSHA                 string
	presetHeadSHA           string
	presetCancelledSHA      string
	presetFailedChecks      []string
	presetClearChecks       []string
	wantEmitAdditional      []bool
	presetNonRoboRevBlocked bool
	presetRegistryCIFailed  bool
	presetRegistryHeadSHA   string
	commentsBody            string
	evalCheck               string
	wantRecover             bool
}

func runFailedCheckTrackerCase(t *testing.T, tc failedCheckTrackerTestCase) {
	t.Helper()
	t.Parallel()
	var srv *RouterServer
	if !tc.nilServer {
		var client *http.Client
		var cfg Config
		if tc.commentsBody != "" {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(tc.commentsBody))
			}))
			defer ts.Close()
			cfg.GitHubAPIURL = ts.URL
			client = ts.Client()
		}
		var reg PRRegistryUpdater
		if tc.presetRegistryCIFailed || tc.presetRegistryHeadSHA != "" {
			status := prOpen
			if tc.presetRegistryCIFailed {
				status = prCIFailed
			}
			reg = &mockPRRegistry{
				buildLifecycle: PRBuildLifecycle{
					Status:  status,
					HeadSHA: tc.presetRegistryHeadSHA,
				},
			}
		}
		srv = newTestRouter(cfg, client, reg, nil)
	}
	if srv != nil && tc.presetCancelledSHA != "" {
		srv.cancelledCheckSHAs.Store(prCheckKey(tc.repo, tc.prNumber), tc.presetCancelledSHA)
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
			name:        "nil server returns false for roborev",
			nilServer:   true,
			repo:        "azylman/aerial",
			prNumber:    10,
			headSHA:     "sha10",
			evalCheck:   "roborev",
			wantRecover: false,
		},
		{
			name:        "invalid PR number returns false for roborev",
			repo:        "azylman/aerial",
			prNumber:    0,
			headSHA:     "sha10",
			evalCheck:   "roborev",
			wantRecover: false,
		},
		{
			name:        "empty head sha returns false for roborev",
			repo:        "azylman/aerial",
			prNumber:    10,
			headSHA:     "",
			evalCheck:   "roborev",
			wantRecover: false,
		},
		{
			name:        "nil server returns false for CI Gate",
			nilServer:   true,
			repo:        "azylman/aerial",
			prNumber:    10,
			headSHA:     "sha10",
			evalCheck:   "CI Gate",
			wantRecover: false,
		},
		{
			name:        "empty head sha returns false for CI Gate",
			repo:        "azylman/aerial",
			prNumber:    10,
			headSHA:     "",
			evalCheck:   "CI Gate",
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
		{
			name:                   "ci gate recovery via preset registry ci failed",
			repo:                   "azylman/aerial",
			prNumber:               22,
			headSHA:                "sha22",
			presetRegistryCIFailed: true,
			presetRegistryHeadSHA:  "sha22",
			evalCheck:              "CI Gate",
			wantRecover:            true,
		},
		{
			name:                   "roborev recovery via preset registry ci failed",
			repo:                   "azylman/aerial",
			prNumber:               23,
			headSHA:                "sha23",
			presetRegistryCIFailed: true,
			presetRegistryHeadSHA:  "sha23",
			evalCheck:              "roborev",
			wantRecover:            true,
		},
		{
			name:                   "ci gate recovery rejected when preset registry head sha differs",
			repo:                   "azylman/aerial",
			prNumber:               24,
			headSHA:                "sha24-new",
			presetRegistryCIFailed: true,
			presetRegistryHeadSHA:  "sha24-stale",
			evalCheck:              "CI Gate",
			wantRecover:            false,
		},
		{
			name:                   "roborev recovery rejected when preset registry head sha differs",
			repo:                   "azylman/aerial",
			prNumber:               25,
			headSHA:                "sha25-new",
			presetRegistryCIFailed: true,
			presetRegistryHeadSHA:  "sha25-stale",
			evalCheck:              "roborev",
			wantRecover:            false,
		},
		{
			name:         "roborev recovery via roborev-override comment matching",
			repo:         "azylman/aerial",
			prNumber:     26,
			headSHA:      "sha26",
			commentsBody: `[{"body":"## roborev-override: manual override approved for commit ` + "`sha26`" + `"}]`,
			evalCheck:    "roborev",
			wantRecover:  true,
		},
		{
			name:               "ci gate recovery via cancelled check on same sha",
			repo:               "azylman/aerial",
			prNumber:           27,
			headSHA:            "sha27",
			presetCancelledSHA: "sha27",
			evalCheck:          "CI Gate",
			wantRecover:        true,
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
		name               string
		status             int
		body               string
		repo               string
		prNumber           int
		headSHA            string
		presetCachedFailed bool
		wantResult         bool
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
		{
			name:       "multiple check runs ascending: older failed, newer succeeded returns false",
			status:     http.StatusOK,
			body:       `{"check_runs":[{"id":100,"name":"Unit Tests","status":"completed","conclusion":"failure"},{"id":200,"name":"Unit Tests","status":"completed","conclusion":"success"}]}`,
			wantResult: false,
		},
		{
			name:       "multiple check runs descending: newer succeeded first, older failed second returns false",
			status:     http.StatusOK,
			body:       `{"check_runs":[{"id":200,"name":"Unit Tests","status":"completed","conclusion":"success"},{"id":100,"name":"Unit Tests","status":"completed","conclusion":"failure"}]}`,
			wantResult: false,
		},
		{
			name:       "multiple check runs: older succeeded, newer failed returns true",
			status:     http.StatusOK,
			body:       `{"check_runs":[{"id":100,"name":"Unit Tests","status":"completed","conclusion":"success"},{"id":200,"name":"Unit Tests","status":"completed","conclusion":"failure"}]}`,
			wantResult: true,
		},
		{
			name:       "empty repo returns false",
			repo:       "none",
			wantResult: false,
		},
		{
			name:       "invalid pr number <= 0 returns false",
			prNumber:   -1,
			wantResult: false,
		},
		{
			name:       "empty head sha returns false",
			headSHA:    "none",
			wantResult: false,
		},
		{
			name:       "zero head sha returns false",
			headSHA:    "0000000000000000000000000000000000000000",
			wantResult: false,
		},
		{
			name:               "cached failed sha returns true",
			presetCachedFailed: true,
			wantResult:         true,
		},
		{
			name:       "dedupe filters out empty check name and metadata checks",
			status:     http.StatusOK,
			body:       `{"check_runs":[{"name":"","status":"completed","conclusion":"failure"},{"name":"build-metadata","status":"completed","conclusion":"failure"},{"app":{"slug":"dependabot"},"name":"Dependabot Check","status":"completed","conclusion":"failure"},{"id":1,"name":"Unit Tests","status":"completed","conclusion":"success"}]}`,
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

			repo := "azylman/aerial"
			if tc.repo == "none" {
				repo = ""
			}
			prNumber := 42
			if tc.prNumber != 0 {
				prNumber = tc.prNumber
			}
			headSHA := "head123"
			if tc.headSHA == "none" {
				headSHA = ""
			} else if tc.headSHA != "" {
				headSHA = tc.headSHA
			}

			srv := newTestRouter(Config{GitHubAPIURL: ts.URL}, ts.Client(), nil, nil)
			if tc.presetCachedFailed {
				srv.failedCheckSHAs.Store(prCheckKey(repo, prNumber), headSHA)
			}
			got := srv.hasFailedCIForSHA(context.Background(), repo, prNumber, headSHA)
			if got != tc.wantResult {
				t.Errorf("hasFailedCIForSHA() = %v, want %v", got, tc.wantResult)
			}
		})
	}
}
