package main

import (
	"context"
	"encoding/json"
	"fmt"
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
}

func TestFailedCheckTrackerHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		nilServer          bool
		repo               string
		prNumber           int
		headSHA            string
		presetHeadSHA      string
		presetFailedChecks []string
		presetClearChecks  []string
		evalCheck          string
		wantRecover        bool
	}{
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
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var srv *RouterServer
			if !tc.nilServer {
				srv = &RouterServer{}
			}
			if srv != nil && tc.presetHeadSHA != "" {
				for _, c := range tc.presetFailedChecks {
					srv.recordFailedCheckRun(tc.repo, tc.prNumber, tc.presetHeadSHA, c)
				}
				for _, c := range tc.presetClearChecks {
					srv.clearActiveFailedCheckRun(tc.repo, tc.prNumber, tc.presetHeadSHA, c)
				}
			}
			got := srv.canRecoverNonGateCheckSuccess(tc.repo, tc.prNumber, tc.evalCheck, tc.headSHA)
			if got != tc.wantRecover {
				t.Fatalf("canRecoverNonGateCheckSuccess() = %v, want %v", got, tc.wantRecover)
			}
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
		name, body, wantTarget, wantChunk, wantBotID string
		wantOK                                       bool
	}{
		{name: "no trailer", body: "Standard PR body\n<!-- regular comment -->"},
		{name: "unclosed trailer", body: "Body\n<!-- aerial-pr: {\"target_id\":\"1562162630997581946\"}"},
		{name: "invalid json", body: "Body\n<!-- aerial-pr: {bad-json} -->"},
		{name: "invalid target and empty chunk", body: "Body\n<!-- aerial-pr: {\"target_id\":\"bad\",\"vanadis_chunk_id\":\"\"} -->"},
		{name: "valid target and chunk", body: "<!-- x -->\n<!-- aerial-pr: {\"target_id\":\"1562162630997581946\",\"vanadis_chunk_id\":\"chunk-2\"} -->", wantTarget: "1562162630997581946", wantChunk: "chunk-2", wantOK: true},
		{name: "discord prefixed target and pool prefix", body: "<!-- aerial-pr: {\"target_id\":\"stateful_medium:discord-1562162630997581946\",\"vanadis_chunk_id\":\"\"} -->", wantTarget: "1562162630997581946", wantChunk: "", wantOK: true},
		{name: "valid chunk only", body: "<!-- aerial-pr: {\"target_id\":\"\",\"vanadis_chunk_id\":\"chunk-only\"} -->", wantChunk: "chunk-only", wantOK: true},
		{name: "valid bot id only", body: "<!-- aerial-pr: {\"bot_id\":\"eri-sub\"} -->", wantBotID: "eri-sub", wantOK: true},
		{name: "valid target chunk and bot id", body: "<!-- aerial-pr: {\"target_id\":\"1562162630997581946\",\"vanadis_chunk_id\":\"chunk-3\",\"bot_id\":\"eri-sub\"} -->", wantTarget: "1562162630997581946", wantChunk: "chunk-3", wantBotID: "eri-sub", wantOK: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ParseAerialPRMetadata(tc.body)
			if got.OK() != tc.wantOK || got.TargetID != tc.wantTarget || got.VanadisChunkID != tc.wantChunk || got.BotID != tc.wantBotID {
				t.Fatalf("ParseAerialPRMetadata() = %+v (ok=%v), want target=%q chunk=%q bot=%q ok=%v", got, got.OK(), tc.wantTarget, tc.wantChunk, tc.wantBotID, tc.wantOK)
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

func TestFormatCIChecksActiveStep_Unit(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		prStatus     string
		ciChecksJSON string
		metadataJSON string
		wantLegacy   string
		want         string
	}{
		{name: "non-open status merged legacy empty", prStatus: "merged", ciChecksJSON: `{"lint":"passed"}`, wantLegacy: "", want: "Building container images (1/1 completed)"},
		{name: "non-open status closed", prStatus: "closed", ciChecksJSON: `{"lint":"passed"}`, wantLegacy: "", want: ""},
		{name: "non-open status ci_failed", prStatus: "ci_failed", ciChecksJSON: `{"lint":"passed"}`, wantLegacy: "", want: ""},
		{name: "empty ciChecksJSON", prStatus: "open", ciChecksJSON: "", wantLegacy: "", want: ""},
		{name: "empty JSON object", prStatus: "open", ciChecksJSON: "{}", wantLegacy: "", want: ""},
		{name: "invalid JSON", prStatus: "open", ciChecksJSON: "not-json", wantLegacy: "", want: ""},
		{name: "single check passed", prStatus: "open", ciChecksJSON: `{"test":"passed"}`, wantLegacy: "Waiting on CI (1/1 passed)", want: "Waiting on CI (1/1 passed)"},
		{name: "multiple checks all passed", prStatus: "open", ciChecksJSON: `{"lint":"passed","unit":"passed","integration":"passed"}`, wantLegacy: "Waiting on CI (3/3 passed)", want: "Waiting on CI (3/3 passed)"},
		{name: "mixed passed and pending", prStatus: "open", ciChecksJSON: `{"lint":"passed","build":"pending","unit":"passed"}`, wantLegacy: "Waiting on CI (2/3 passed)", want: "Waiting on CI (2/3 passed)"},
		{name: "all pending", prStatus: "open", ciChecksJSON: `{"lint":"pending","unit":"pending"}`, wantLegacy: "Waiting on CI (0/2 passed)", want: "Waiting on CI (0/2 passed)"},
		{name: "any check failed returns empty", prStatus: "open", ciChecksJSON: `{"lint":"passed","unit":"failed"}`, wantLegacy: "", want: ""},

		// Post-merge tests for formatPRWidgetActiveStep
		{
			name:         "merged with candidate jobs and started verifying_health",
			prStatus:     "merged",
			metadataJSON: `{"jobs":["eri","haro"],"started_jobs":["eri"],"deployed_jobs":[],"job_states":{"eri":"verifying_health"}}`,
			want:         "`eri`: 🔄 verifying health\n`haro`: ⏳ building image",
		},
		{
			name:         "synced with deployed job and starting job",
			prStatus:     "synced",
			metadataJSON: `{"jobs":["coven","eri"],"started_jobs":["eri"],"deployed_jobs":["coven"]}`,
			want:         "`coven`: ✅ healthy\n`eri`: 🔄 starting",
		},
		{
			name:         "deploying status with empty started_jobs defaults candidate jobs to starting",
			prStatus:     "deploying",
			metadataJSON: `{"jobs":["eri","haro"]}`,
			want:         "`eri`: 🔄 starting\n`haro`: 🔄 starting",
		},
		{
			name:         "merged with buildJobRegex service matches in ci_checks",
			prStatus:     "merged",
			ciChecksJSON: `{"Build & Push Images (mesh-proxy)":"passed","Build & Push Images (eri)":"in_progress"}`,
			want:         "`eri`: ⏳ building image\n`mesh-proxy`: 🔄 starting",
		},
		{
			name:         "merged with only unexpanded matrix build check falls back to container images count",
			prStatus:     "merged",
			ciChecksJSON: `{"Build & Push Images to GHCR (${{ matrix.service }})":"passed","CI Gate":"passed"}`,
			want:         "Building container images (2/2 completed)",
		},
		{
			name:         "merged with unexpanded matrix build check alongside real service renders only real service",
			prStatus:     "merged",
			ciChecksJSON: `{"Build & Push Images to GHCR (${{ matrix.service }})":"passed","Build & Push Images to GHCR (eri)":"in_progress"}`,
			want:         "`eri`: ⏳ building image",
		},
		{
			name:         "merged with non-matching ci_checks falls back to container images count",
			prStatus:     "merged",
			ciChecksJSON: `{"Docker Build":"passed","Test Gate":"passed"}`,
			want:         "Building container images (2/2 completed)",
		},
		{
			name:         "merged with failed check returns empty",
			prStatus:     "merged",
			ciChecksJSON: `{"Docker Build":"failed","Test Gate":"passed"}`,
			metadataJSON: `{"jobs":["eri"]}`,
			want:         "",
		},
		{
			name:         "merged fallback to meta ci_checks when ciChecksJSON is empty",
			prStatus:     "merged",
			metadataJSON: `{"ci_checks":{"Docker Build":"passed"}}`,
			want:         "Building container images (1/1 completed)",
		},
		{
			name:         "deploying kustomizations none rolled out",
			prStatus:     "deploying",
			metadataJSON: `{"jobs":["eri"],"kustomization_requests":{"marida-core":{"namespace":"flux-system","requested_at":"t1"},"aerial-core":{"namespace":"flux-system","requested_at":"t1"}}}`,
			want:         "`aerial-core`: 🔄 rolling out\n`marida-core`: 🔄 rolling out",
		},
		{
			name:         "deploying kustomizations partially rolled out",
			prStatus:     "deploying",
			metadataJSON: `{"kustomization_requests":{"marida-core":{"namespace":"flux-system","requested_at":"t1"},"aerial-core":{"namespace":"flux-system","requested_at":"t1"}},"deployed_kustomizations":["aerial-core"]}`,
			want:         "`aerial-core`: ✅ rolled out\n`marida-core`: 🔄 rolling out",
		},
		{
			name:         "deploying kustomizations all rolled out",
			prStatus:     "deploying",
			metadataJSON: `{"kustomization_requests":{"marida-core":{"namespace":"apps","requested_at":"t1"},"aerial-core":{"namespace":"flux-system","requested_at":"t1"}},"deployed_kustomizations":["aerial-core","marida-core"]}`,
			want:         "`aerial-core`: ✅ rolled out\n`marida-core`: ✅ rolled out",
		},
		{
			name:         "deploying legacy empty kustomization requests falls back to jobs",
			prStatus:     "deploying",
			metadataJSON: `{"jobs":["eri"],"kustomization_requests":{},"deployed_jobs":["eri"]}`,
			want:         "`eri`: ✅ healthy",
		},
		{
			name:     "unsupported status returns empty",
			prStatus: "deployed",
			want:     "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gotLegacy := formatCIChecksActiveStep(tc.prStatus, tc.ciChecksJSON)
			if tc.metadataJSON == "" && tc.prStatus == "open" {
				if gotLegacy != tc.wantLegacy {
					t.Fatalf("formatCIChecksActiveStep(%q, %q) = %q, want %q", tc.prStatus, tc.ciChecksJSON, gotLegacy, tc.wantLegacy)
				}
			}
			got := formatPRWidgetActiveStep(tc.prStatus, tc.ciChecksJSON, tc.metadataJSON)
			if got != tc.want {
				t.Fatalf("formatPRWidgetActiveStep(%q, %q, %q) = %q, want %q", tc.prStatus, tc.ciChecksJSON, tc.metadataJSON, got, tc.want)
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

type prAutoMergeDisabledTestCase struct {
	name                  string
	resRepo               string
	resPRNum              int
	resBranch             string
	resHeadSHA            string
	payloadMergeableState string
	payloadMergeable      *bool
	ghAPIStatus           int
	apiMergeableState     string
	apiHeadSHA            string
	apiState              string
	apiMerged             bool
	registryUpdated       bool
	registryTargetID      string
	wantConflictEnqueued  int
	wantErr               bool
	wantFailedNonRoboRev  string
}

func newAutoMergeDisabledMockServer(tc prAutoMergeDisabledTestCase) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if tc.ghAPIStatus != 0 {
			w.WriteHeader(tc.ghAPIStatus)
			return
		}
		if strings.Contains(r.URL.Path, fmt.Sprintf("/pulls/%d", tc.resPRNum)) {
			apiHeadSHA := tc.apiHeadSHA
			if apiHeadSHA == "" {
				apiHeadSHA = tc.resHeadSHA
			}
			pr := GitHubPullRequest{
				Number:         tc.resPRNum,
				State:          tc.apiState,
				Merged:         tc.apiMerged,
				MergeableState: tc.apiMergeableState,
				Head: GitHubPRRef{
					Ref: tc.resBranch,
					SHA: apiHeadSHA,
				},
			}
			_ = json.NewEncoder(w).Encode(pr)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
}

func runPRAutoMergeDisabledCase(ctx context.Context, t *testing.T, tc prAutoMergeDisabledTestCase) {
	t.Helper()
	t.Parallel()
	mockServer := newAutoMergeDisabledMockServer(tc)
	defer mockServer.Close()

	reg := &mockPRRegistry{
		conflictTargetID: tc.registryTargetID,
		conflictUpdated:  tc.registryUpdated,
	}
	river := &mockRiverInserter{}
	cfg := Config{
		GitHubAPIURL: mockServer.URL,
	}
	srv := newTestRouter(cfg, mockServer.Client(), reg, nil, river)

	res := &ResolvedGitHubEvent{
		Event:    "pull_request",
		Action:   "auto_merge_disabled",
		Repo:     tc.resRepo,
		PRNumber: tc.resPRNum,
		HeadSHA:  tc.resHeadSHA,
		Branch:   tc.resBranch,
	}
	pr := GitHubPullRequest{
		Number:         tc.resPRNum,
		MergeableState: tc.payloadMergeableState,
		Mergeable:      tc.payloadMergeable,
	}

	err := srv.handlePRAutoMergeDisabledAction(ctx, res, pr)
	if (err != nil) != tc.wantErr {
		t.Fatalf("handlePRAutoMergeDisabledAction err = %v, wantErr = %v", err, tc.wantErr)
	}
	if len(river.insertedConflictJobs) != tc.wantConflictEnqueued {
		t.Fatalf("insertedConflictJobs len = %d, want %d", len(river.insertedConflictJobs), tc.wantConflictEnqueued)
	}
	if tc.wantFailedNonRoboRev != "" {
		val, ok := srv.nonRoboRevFailedSHAs.Load(prCheckKey(tc.resRepo, tc.resPRNum))
		if !ok || fmt.Sprint(val) != tc.wantFailedNonRoboRev {
			t.Errorf("nonRoboRevFailedSHAs[%s#%d] = %v (ok=%v), want %q", tc.resRepo, tc.resPRNum, val, ok, tc.wantFailedNonRoboRev)
		}
	} else if val, ok := srv.nonRoboRevFailedSHAs.Load(prCheckKey(tc.resRepo, tc.resPRNum)); ok {
		t.Errorf("unexpected nonRoboRevFailedSHAs entry: %v", val)
	}
}

func TestHandlePRAutoMergeDisabledAction(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	trueVal := true
	falseVal := false

	tests := []prAutoMergeDisabledTestCase{
		{
			name:                  "unknown state where API returns dirty open PR transitions conflict and enqueues job",
			resRepo:               "azylman/aerial",
			resPRNum:              102,
			resBranch:             "feat/fallback",
			resHeadSHA:            "sha102",
			payloadMergeableState: "unknown",
			apiMergeableState:     "dirty",
			apiHeadSHA:            "sha102",
			apiState:              "open",
			registryUpdated:       true,
			registryTargetID:      "1555405874565091380",
			wantConflictEnqueued:  1,
			wantFailedNonRoboRev:  "sha102",
		},
		{
			name:                  "payload mergeable false with empty state is merge conflict without API lookup",
			resRepo:               "azylman/aerial",
			resPRNum:              103,
			resBranch:             "feat/unmergeable",
			resHeadSHA:            "sha103",
			payloadMergeable:      &falseVal,
			payloadMergeableState: "",
			registryUpdated:       true,
			registryTargetID:      "1555405874565091380",
			wantConflictEnqueued:  1,
			wantFailedNonRoboRev:  "sha103",
		},
		{
			name:                  "payload mergeable true with empty state is clean and ignored",
			resRepo:               "azylman/aerial",
			resPRNum:              104,
			resBranch:             "feat/clean",
			resHeadSHA:            "sha104",
			payloadMergeable:      &trueVal,
			payloadMergeableState: "",
			wantConflictEnqueued:  0,
		},
		{
			name:                  "unknown state with empty repo normalization skips API lookup and ignores non-conflict",
			resRepo:               "",
			resPRNum:              107,
			resBranch:             "feat/unknown",
			resHeadSHA:            "sha107",
			payloadMergeableState: "unknown",
			ghAPIStatus:           http.StatusInternalServerError,
			wantConflictEnqueued:  0,
		},
		{
			name:                  "unknown state where API returns merged PR is ignored cleanly",
			resRepo:               "azylman/aerial",
			resPRNum:              109,
			resBranch:             "feat/api-merged",
			resHeadSHA:            "sha109",
			payloadMergeableState: "unknown",
			apiMergeableState:     "dirty",
			apiHeadSHA:            "sha109",
			apiMerged:             true,
			wantConflictEnqueued:  0,
		},
		{
			name:                  "unknown state where API returns closed PR is ignored cleanly",
			resRepo:               "azylman/aerial",
			resPRNum:              110,
			resBranch:             "feat/api-closed",
			resHeadSHA:            "sha110",
			payloadMergeableState: "unknown",
			apiMergeableState:     "dirty",
			apiHeadSHA:            "sha110",
			apiState:              "closed",
			wantConflictEnqueued:  0,
		},
		{
			name:                  "unknown state where API returns superseded head SHA is ignored cleanly",
			resRepo:               "azylman/aerial",
			resPRNum:              111,
			resBranch:             "feat/api-superseded",
			resHeadSHA:            "sha111",
			payloadMergeableState: "unknown",
			apiMergeableState:     "dirty",
			apiHeadSHA:            "sha111-newer",
			apiState:              "open",
			wantConflictEnqueued:  0,
		},
		{
			name:                  "unknown state where API returns HTTP 500 propagates error",
			resRepo:               "azylman/aerial",
			resPRNum:              112,
			resBranch:             "feat/api-err",
			resHeadSHA:            "sha112",
			payloadMergeableState: "unknown",
			ghAPIStatus:           http.StatusInternalServerError,
			wantErr:               true,
			wantConflictEnqueued:  0,
		},
		{
			name:                  "registry returns updated false skips enqueuing conflict job",
			resRepo:               "azylman/aerial",
			resPRNum:              115,
			resBranch:             "feat/not-updated",
			resHeadSHA:            "sha115",
			payloadMergeableState: "dirty",
			registryUpdated:       false,
			wantConflictEnqueued:  0,
			wantFailedNonRoboRev:  "sha115",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runPRAutoMergeDisabledCase(ctx, t, tc)
		})
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
		return "1555405874565091380", "feat/test", "sha123", "", nil
	}
	return "", "", "", "", nil
}

func (r *ownedMapRegistry) ResolvePRBySHA(ctx context.Context, repo string, sha string) (int, string, string, error) {
	if err := r.checkErr(r.resolveErr); err != nil {
		return 0, "", "", err
	}
	if r.ownedSHAs != nil {
		if prNum, ok := r.ownedSHAs[sha]; ok {
			return prNum, "feat/test", "1555405874565091380", nil
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
			srv := newTestRouter(cfg, nil, reg, nil)
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
				BotID: "aerial",
				WebhookForwardURLs: map[string]string{
					"bot_a":  serverA.URL,
					"bot_b":  serverB.URL,
					"aerial": "http://127.0.0.1:9999", // self, must never be called
				},
			}
			river := &mockRiverInserter{}
			srv := newTestRouter(cfg, serverA.Client(), reg, nil, river)

			headers := map[string]string{
				"X-Github-Event":    tc.event,
				"X-Github-Delivery": "del-test-1234",
				"Content-Type":      "application/json",
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
	reg := &mockPRRegistry{resolveTargetID: "1555405874565091380"}
	srv := newTestRouter(Config{GitHubAPIURL: ts.URL, DefaultOwner: true}, ts.Client(), reg, disp)

	err := srv.dispatchWorkflowRunImages(context.Background(), "azylman/aerial", 7, "main", "success", "abcdef1234567890abcdef1234567890abcdef12", "1555405874565091380", 42)
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
