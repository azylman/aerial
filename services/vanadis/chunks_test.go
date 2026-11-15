package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
)

func TestNormalizeWidgetStatus(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"ci_failed", "ci_failed", "failed"},
		{"deploy_failed", "deploy_failed", "failed"},
		{"RUNNING", "RUNNING", "running"},
		{"open", "open", "open"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := normalizeWidgetStatus(tc.in); got != tc.want {
				t.Errorf("normalizeWidgetStatus(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestResolveVanadisChunkWidgetStatus(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		chunkStatus string
		prStatus    string
		want        string
	}{
		{"failed_open", "failed", "open", "open"},
		{"merged_open", "merged", "open", "merged"},
		{"failed_deployed", "failed", "deployed", "deployed"},
		{"pending_deployed", "pending", "deployed", "deployed"},
		{"pending_deploying", "pending", "deploying", "deploying"},
		{"pending_synced", "pending", "synced", "synced"},
		{"pending_failed", "pending", "failed", "failed"},
		{"pending_conflict", "pending", "conflict", "conflict"},
		{"pending_merged", "pending", "merged", "merged"},
		{"merged_closed", "merged", "closed", "merged"},
		{"pending_closed", "pending", "closed", "failed"},
		{"coding_unknown", "coding", "unknown", "coding"},
		{"empty", "", "", "open"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := resolveVanadisChunkWidgetStatus(tc.chunkStatus, tc.prStatus); got != tc.want {
				t.Errorf("resolveVanadisChunkWidgetStatus(%q, %q) = %q, want %q", tc.chunkStatus, tc.prStatus, got, tc.want)
			}
		})
	}
}

func TestIsVanadisChunkStreamingPhase(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   string
		want bool
	}{
		{"open", "open", true},
		{"pr_opened", "pr_opened", true},
		{"merged", "merged", true},
		{"synced", "synced", true},
		{"deploying", "deploying", true},
		{"deployed", "deployed", false},
		{"failed", "failed", false},
		{"pending", "pending", false},
		{"running", "running", false},
		{"empty", "", false},
		{"padded_uppercase_open", "  OPEN  ", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := isVanadisChunkStreamingPhase(tc.in); got != tc.want {
				t.Errorf("isVanadisChunkStreamingPhase(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

// widgetBuild is a one-line VanadisBuild fixture for widget sync tests.
func widgetBuild(id, status, statusMessageID, title string, createdAt time.Time) VanadisBuild {
	return VanadisBuild{ID: id, Status: status, StatusMessageID: statusMessageID, Title: title, CreatedAt: createdAt}
}

// widgetPRReq is a one-line widget sync request for a found aerial PR.
func widgetPRReq(buildID string, prNumber int, prStatus, activeStep string) SyncChunkPRWidgetRequest {
	return SyncChunkPRWidgetRequest{BuildID: buildID, Repo: "aerial", PRNumber: prNumber, PRStatus: prStatus, ActiveStep: activeStep, PRFound: true}
}

func TestBuildSyncWidgetRequest(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                string
		build               VanadisBuild
		chunks              []VanadisChunk
		req                 SyncChunkPRWidgetRequest
		wantStatus          string
		wantSkipSink        bool
		wantActiveStep      string
		wantChunkRepo       string
		wantChunkStatus     string
		wantZeroCompletedAt bool
	}{
		{name: "preserves active step and allows sink when chunk is open and build running with message", build: widgetBuild("build-1", "running", "msg-123", "Test Build", time.Unix(1760000000, 0)), chunks: []VanadisChunk{{ID: "c1", ChunkIndex: 0, PRNumber: 101, Status: "open"}}, req: widgetPRReq("build-1", 101, "open", "Waiting on CI (2/5 passed)"), wantStatus: "running", wantActiveStep: "Waiting on CI (2/5 passed)"},
		{name: "sets active step on pr_opened chunk", build: widgetBuild("build-2", "running", "msg-456", "Test Build 2", time.Time{}), chunks: []VanadisChunk{{ID: "c2", ChunkIndex: 0, PRNumber: 102, Status: "pr_opened"}}, req: widgetPRReq("build-2", 102, "open", "Waiting on CI (3/3 passed)"), wantStatus: "running", wantActiveStep: "Waiting on CI (3/3 passed)"},
		{name: "omits skipsink when build status message ID is empty", build: widgetBuild("build-3", "running", "", "Test Build 3", time.Time{}), chunks: []VanadisChunk{{ID: "c3", ChunkIndex: 0, PRNumber: 103, Status: "open"}}, req: widgetPRReq("build-3", 103, "open", "Waiting on CI (1/2 passed)"), wantStatus: "running", wantActiveStep: "Waiting on CI (1/2 passed)"},
		{name: "omits skipsink and active step when pr status is failed", build: widgetBuild("build-4", "running", "msg-789", "Test Build 4", time.Time{}), chunks: []VanadisChunk{{ID: "c4", ChunkIndex: 0, PRNumber: 104, Status: "pr_opened"}}, req: widgetPRReq("build-4", 104, "failed", "Waiting on CI (0/2 passed)"), wantStatus: "failed", wantActiveStep: ""},
		{name: "omits skipsink when active step is whitespace", build: widgetBuild("build-5", "running", "msg-999", "Test Build 5", time.Time{}), chunks: []VanadisChunk{{ID: "c5", ChunkIndex: 0, PRNumber: 105, Status: "open"}}, req: widgetPRReq("build-5", 105, "open", "   "), wantStatus: "running", wantActiveStep: ""},
		{name: "preserves active step and allows sink when chunk is merged", build: widgetBuild("build-merged", "running", "msg-merged", "Test Build Merged", time.Time{}), chunks: []VanadisChunk{{ID: "c-merged", ChunkIndex: 0, PRNumber: 106, Status: "open"}}, req: widgetPRReq("build-merged", 106, "merged", "Building container images (1/2 completed)"), wantStatus: "running", wantActiveStep: "Building container images (1/2 completed)"},
		{name: "preserves active step and allows sink when chunk is synced", build: widgetBuild("build-synced", "running", "msg-synced", "Test Build Synced", time.Time{}), chunks: []VanadisChunk{{ID: "c-synced", ChunkIndex: 0, PRNumber: 107, Status: "merged"}}, req: widgetPRReq("build-synced", 107, "synced", "Building container images (2/2 completed)"), wantStatus: "running", wantActiveStep: "Building container images (2/2 completed)"},
		{name: "preserves active step and allows sink when chunk is deploying", build: widgetBuild("build-deploying", "running", "msg-deploying", "Test Build Deploying", time.Time{}), chunks: []VanadisChunk{{ID: "c-deploying", ChunkIndex: 0, PRNumber: 108, Status: "synced"}}, req: widgetPRReq("build-deploying", 108, "deploying", "`eri`: 🔄 starting"), wantStatus: "running", wantActiveStep: "`eri`: 🔄 starting"},
		{name: "clears active step on deployed chunk", build: widgetBuild("build-deployed", "running", "msg-deployed", "Test Build Deployed", time.Time{}), chunks: []VanadisChunk{{ID: "c-deployed", ChunkIndex: 0, PRNumber: 109, Status: "deploying"}}, req: widgetPRReq("build-deployed", 109, "deployed", "All jobs healthy"), wantStatus: "running", wantActiveStep: ""},
		{name: "recovers failed build and failed chunk on open pr", build: widgetBuild("build-recover", "failed", "msg-recover", "Test Build Recover", time.Unix(1760000000, 0)), chunks: []VanadisChunk{{ID: "c-recover", ChunkIndex: 0, PRNumber: 110, Status: "failed", CompletedAt: time.Unix(1760001000, 0)}}, req: widgetPRReq("build-recover", 110, "open", "Waiting on CI (1/4 passed)"), wantStatus: "running", wantActiveStep: "Waiting on CI (1/4 passed)", wantZeroCompletedAt: true},
		{name: "resolves repo to action for action chunk", build: widgetBuild("build-action-widget", "running", "msg-action", "Test Action Widget", time.Time{}), chunks: []VanadisChunk{{ID: "c-action", ChunkIndex: 0, Mode: "action", Repo: "", Status: "running"}}, req: SyncChunkPRWidgetRequest{BuildID: "build-action-widget"}, wantStatus: "running", wantChunkRepo: "action"},
		{name: "maps completed action chunk to deployed status", build: widgetBuild("build-action-completed", "completed", "msg-action-completed", "Test Action Completed Widget", time.Time{}), chunks: []VanadisChunk{{ID: "c-action-done", ChunkIndex: 0, Mode: "action", Repo: "", Status: "completed"}}, req: SyncChunkPRWidgetRequest{BuildID: "build-action-completed"}, wantStatus: "completed", wantChunkRepo: "action", wantChunkStatus: "deployed"},
		{name: "maps merged chunk without active step to deployed status", build: widgetBuild("build-merged-idle", "running", "msg-merged-idle", "Test Merged Idle Widget", time.Time{}), chunks: []VanadisChunk{{ID: "c-merged-idle", ChunkIndex: 0, Mode: "pr", Repo: "aerial", Status: "merged", PRNumber: 120}}, req: SyncChunkPRWidgetRequest{BuildID: "build-merged-idle"}, wantStatus: "running", wantChunkStatus: "deployed"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := buildSyncWidgetRequest(tc.build, tc.chunks, tc.req)
			if got.Status != tc.wantStatus || got.SkipSink != tc.wantSkipSink {
				t.Errorf("buildSyncWidgetRequest Status = %q SkipSink = %v, want %q %v", got.Status, got.SkipSink, tc.wantStatus, tc.wantSkipSink)
			}
			if len(got.Chunks) != 1 {
				t.Fatalf("buildSyncWidgetRequest returned %d chunks, want 1", len(got.Chunks))
			}
			c := got.Chunks[0]
			if c.ActiveStep != tc.wantActiveStep {
				t.Errorf("buildSyncWidgetRequest Chunk[0].ActiveStep = %q, want %q", c.ActiveStep, tc.wantActiveStep)
			}
			if tc.wantChunkRepo != "" && c.Repo != tc.wantChunkRepo {
				t.Errorf("buildSyncWidgetRequest Chunk[0].Repo = %q, want %q", c.Repo, tc.wantChunkRepo)
			}
			if tc.wantChunkStatus != "" && c.Status != tc.wantChunkStatus {
				t.Errorf("buildSyncWidgetRequest Chunk[0].Status = %q, want %q", c.Status, tc.wantChunkStatus)
			}
			if tc.wantZeroCompletedAt && !c.CompletedAt.IsZero() {
				t.Errorf("buildSyncWidgetRequest Chunk[0].CompletedAt = %v, want zero", c.CompletedAt)
			}
			if !tc.build.CreatedAt.IsZero() && !got.StartedAt.Equal(tc.build.CreatedAt) {
				t.Errorf("buildSyncWidgetRequest StartedAt = %v, want %v", got.StartedAt, tc.build.CreatedAt)
			}
		})
	}
}

func setTestChunkStatus(s *memoryVanadisStore, bID, chunkID, status string) {
	list := s.chunks[bID]
	for i := range list {
		if list[i].ID == chunkID {
			list[i].Status = status
		}
	}
	s.chunks[bID] = list
}

func setupTestStoreWithBuild() (*memoryVanadisStore, string, []string) {
	s := newMemoryVanadisStore()
	buildID := "build-test-1"
	b := mkBuild(buildID, "123456789012345678", "Test Build Title", "Test Objective", "running")
	c1 := VanadisChunk{
		ID:         "chunk-1",
		BuildID:    buildID,
		ChunkIndex: 0,
		Mode:       "pr",
		Repo:       "aerial",
		Title:      "Chunk 1",
		Status:     "pending",
		Effort:     "low",
		CreatedAt:  time.Now().UTC(),
		UpdatedAt:  time.Now().UTC(),
	}
	c2 := VanadisChunk{
		ID:         "chunk-2",
		BuildID:    buildID,
		ChunkIndex: 1,
		Mode:       "action",
		Repo:       "aerial",
		Title:      "Chunk 2",
		Status:     "pending",
		Effort:     "medium",
		CreatedAt:  time.Now().UTC(),
		UpdatedAt:  time.Now().UTC(),
	}
	_ = s.CreateBuild(context.Background(), b, []VanadisChunk{c1, c2})
	return s, buildID, []string{"chunk-1", "chunk-2"}
}

func setTestBuildStatus(s *memoryVanadisStore, bID, status string) {
	b := s.builds[bID]
	b.Status = status
	s.builds[bID] = b
}

// attachPRs links prs[i] to chunk-(i+1) of the setupTestStoreWithBuild fixture.
func attachPRs(t *testing.T, s *memoryVanadisStore, prs ...int) {
	t.Helper()
	for i, pr := range prs {
		if _, _, _, err := s.AttachChunkPR(context.Background(), AttachChunkPRRequest{ChunkID: fmt.Sprintf("chunk-%d", i+1), PRNumber: pr, Repo: "aerial"}); err != nil {
			t.Fatalf("attach pr %d: %v", pr, err)
		}
	}
}

// replayPREvents applies earlier CI-failure and merge webhooks for aerial PRs, in that order.
func replayPREvents(t *testing.T, s *memoryVanadisStore, failedPRs, mergedPRs []int) {
	t.Helper()
	for _, pr := range failedPRs {
		if _, _, _, err := s.FailChunkByPR(context.Background(), "aerial", pr); err != nil {
			t.Fatalf("fail pr %d: %v", pr, err)
		}
	}
	for _, pr := range mergedPRs {
		if _, err := s.AdvanceChunkByPR(context.Background(), "aerial", pr); err != nil {
			t.Fatalf("advance pr %d: %v", pr, err)
		}
	}
}

func TestVanadisStore_GetChunk(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		chunkID      string
		missingBuild bool
		wantErrIs    error
		wantChunkID  string
		wantBuildID  string
	}{
		{name: "found", chunkID: "chunk-1", wantChunkID: "chunk-1", wantBuildID: "build-test-1"},
		{name: "chunk not found", chunkID: "nonexistent", wantErrIs: ErrChunkNotFound},
		{name: "build not found", chunkID: "chunk-1", missingBuild: true, wantErrIs: ErrBuildNotFound},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s, bID, _ := setupTestStoreWithBuild()
			if tc.missingBuild {
				delete(s.builds, bID)
			}
			c, b, err := s.GetChunk(context.Background(), tc.chunkID)
			if assertStoreErr(t, err, tc.wantErrIs, false) {
				if c != nil || b != nil {
					t.Errorf("GetChunk returned chunk %+v build %+v alongside error", c, b)
				}
				return
			}
			if c.ID != tc.wantChunkID || b.ID != tc.wantBuildID {
				t.Errorf("got c.ID=%q, b.ID=%q, want %q, %q", c.ID, b.ID, tc.wantChunkID, tc.wantBuildID)
			}
		})
	}
}

func TestVanadisStore_AttachChunkPR(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		priorPR      int
		buildStatus  string
		chunkStatus  string
		chunkID      string
		repo         string
		prNumber     int
		wantErrIs    error
		wantSubstr   string
		wantThreadID string
		wantPR       int
		wantStatus   string
	}{
		{name: "attaches pr to pending chunk", chunkID: "chunk-1", repo: "aerial", prNumber: 101, wantThreadID: "123456789012345678", wantPR: 101, wantStatus: "pr_opened"},
		{name: "overwrites previously attached pr", priorPR: 101, chunkID: "chunk-1", repo: "aerial", prNumber: 102, wantThreadID: "123456789012345678", wantPR: 102, wantStatus: "pr_opened"},
		{name: "mismatched repo", priorPR: 101, chunkID: "chunk-1", repo: "other-repo", prNumber: 101, wantSubstr: "does not match"},
		{name: "chunk not found", priorPR: 101, chunkID: "missing", repo: "aerial", prNumber: 101, wantErrIs: ErrChunkNotFound},
		{name: "empty chunk ID and empty repo", priorPR: 101, chunkID: "", repo: "", prNumber: 101, wantSubstr: "repo is required for standalone attach"},
		{name: "parent build completed returns ErrBuildConflict", priorPR: 101, buildStatus: "completed", chunkID: "chunk-1", repo: "aerial", prNumber: 101, wantErrIs: ErrBuildConflict},
		{name: "parent build cancelled returns ErrBuildConflict", priorPR: 101, buildStatus: "cancelled", chunkID: "chunk-1", repo: "aerial", prNumber: 101, wantErrIs: ErrBuildConflict},
		{name: "chunk cancelled returns ErrBuildConflict", priorPR: 101, chunkStatus: "cancelled", chunkID: "chunk-1", repo: "aerial", prNumber: 101, wantErrIs: ErrBuildConflict},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s, bID, _ := setupTestStoreWithBuild()
			if tc.priorPR > 0 {
				attachPRs(t, s, tc.priorPR)
			}
			if tc.buildStatus != "" {
				setTestBuildStatus(s, bID, tc.buildStatus)
			}
			if tc.chunkStatus != "" {
				setTestChunkStatus(s, bID, tc.chunkID, tc.chunkStatus)
			}
			_, _, threadID, err := s.AttachChunkPR(context.Background(), AttachChunkPRRequest{ChunkID: tc.chunkID, PRNumber: tc.prNumber, Repo: tc.repo})
			if tc.wantSubstr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantSubstr)) {
				t.Fatalf("expected error containing %q, got %v", tc.wantSubstr, err)
			}
			if assertStoreErr(t, err, tc.wantErrIs, tc.wantSubstr != "") {
				return
			}
			c, _, _ := s.GetChunk(context.Background(), tc.chunkID)
			if threadID != tc.wantThreadID || c.PRNumber != tc.wantPR || c.Status != tc.wantStatus {
				t.Errorf("AttachChunkPR thread=%q chunk pr=%d status=%q, want %q %d %q", threadID, c.PRNumber, c.Status, tc.wantThreadID, tc.wantPR, tc.wantStatus)
			}
		})
	}
}

func TestVanadisStore_GetChunkByPR(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		buildStatus string
		repo        string
		prNumber    int
		want        VanadisChunkPRLookup
	}{
		{name: "matches attached pr with next action chunk", buildStatus: "running", repo: "aerial", prNumber: 101, want: VanadisChunkPRLookup{Matched: true, ChunkID: "chunk-1", BuildID: "build-test-1", Effort: "low", HasRemainingChunks: true, NextChunkMode: "action"}},
		{name: "unknown pr is unmatched", buildStatus: "running", repo: "aerial", prNumber: 999},
		{name: "cancelled build is unmatchable", buildStatus: "cancelled", repo: "aerial", prNumber: 101},
		{name: "invalid params are unmatched", buildStatus: "running", repo: "", prNumber: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s, bID, _ := setupTestStoreWithBuild()
			attachPRs(t, s, 101)
			setTestBuildStatus(s, bID, tc.buildStatus)
			got, err := s.GetChunkByPR(context.Background(), tc.repo, tc.prNumber)
			if err != nil {
				t.Fatalf("GetChunkByPR: %v", err)
			}
			if *got != tc.want {
				t.Errorf("GetChunkByPR = %+v, want %+v", *got, tc.want)
			}
		})
	}
}

func TestVanadisStore_AdvanceChunkByPR(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name            string
		prs             []int
		failedPRs       []int
		mergedPRs       []int
		repo            string
		prNumber        int
		want            advanceOutcome
		wantBuildStatus string
	}{
		{name: "first merge advances to next chunk", prs: []int{101, 102}, repo: "aerial", prNumber: 101, want: advanceOutcome{Matched: true, Advanced: true, NextChunkIndex: 1, BuildStatus: "running", Chunks: 2}, wantBuildStatus: "running"},
		{name: "merge recovers failed build and chunk", prs: []int{111}, failedPRs: []int{111}, repo: "aerial", prNumber: 111, want: advanceOutcome{Matched: true, Advanced: true, NextChunkIndex: 1, BuildStatus: "running", Chunks: 2}, wantBuildStatus: "running"},
		{name: "repeat merge does not advance", prs: []int{101, 102}, mergedPRs: []int{101}, repo: "aerial", prNumber: 101, want: advanceOutcome{Matched: true, NextChunkIndex: 1, BuildStatus: "running", Chunks: 2}, wantBuildStatus: "running"},
		{name: "last merge completes build", prs: []int{101, 102}, mergedPRs: []int{101}, repo: "aerial", prNumber: 102, want: advanceOutcome{Matched: true, Advanced: true, BuildCompleted: true, NextChunkIndex: -1, BuildStatus: "completed", Chunks: 2}, wantBuildStatus: "completed"},
		{name: "repeat merge on completed build returns chunks without advancing", prs: []int{101, 102}, mergedPRs: []int{101, 102}, repo: "aerial", prNumber: 102, want: advanceOutcome{Matched: true, BuildCompleted: true, NextChunkIndex: -1, BuildStatus: "completed", Chunks: 2}, wantBuildStatus: "completed"},
		{name: "unknown pr is unmatched", prs: []int{101, 102}, repo: "aerial", prNumber: 999, wantBuildStatus: "running"},
		{name: "invalid params are unmatched", prs: []int{101, 102}, repo: "", prNumber: 0, wantBuildStatus: "running"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s, bID, _ := setupTestStoreWithBuild()
			attachPRs(t, s, tc.prs...)
			replayPREvents(t, s, tc.failedPRs, tc.mergedPRs)
			res, err := s.AdvanceChunkByPR(context.Background(), tc.repo, tc.prNumber)
			if err != nil {
				t.Fatalf("AdvanceChunkByPR: %v", err)
			}
			if got := outcomeOf(res); got != tc.want {
				t.Errorf("AdvanceChunkByPR = %+v, want %+v", got, tc.want)
			}
			if b, _, _ := s.GetBuild(context.Background(), bID); b.Status != tc.wantBuildStatus {
				t.Errorf("stored build status = %q, want %q", b.Status, tc.wantBuildStatus)
			}
		})
	}
}

func TestVanadisStore_FailChunkByPR(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name            string
		failedPRs       []int
		repo            string
		prNumber        int
		wantFailed      bool
		wantBuildID     string
		wantChunkID     string
		wantChunkStatus string
		wantBuildStatus string
	}{
		{name: "fails attached chunk and running build", repo: "aerial", prNumber: 101, wantFailed: true, wantBuildID: "build-test-1", wantChunkID: "chunk-1", wantChunkStatus: "failed", wantBuildStatus: "failed"},
		{name: "already failed chunk is not failed again", failedPRs: []int{101}, repo: "aerial", prNumber: 101, wantChunkStatus: "failed", wantBuildStatus: "failed"},
		{name: "unknown pr does not fail", repo: "aerial", prNumber: 999, wantChunkStatus: "pr_opened", wantBuildStatus: "running"},
		{name: "invalid args do not fail", repo: "", prNumber: 0, wantChunkStatus: "pr_opened", wantBuildStatus: "running"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s, _, _ := setupTestStoreWithBuild()
			attachPRs(t, s, 101)
			replayPREvents(t, s, tc.failedPRs, nil)
			bID, cID, failed, err := s.FailChunkByPR(context.Background(), tc.repo, tc.prNumber)
			if err != nil || failed != tc.wantFailed || bID != tc.wantBuildID || cID != tc.wantChunkID {
				t.Fatalf("FailChunkByPR = (%q, %q, %v, %v), want (%q, %q, %v, nil)", bID, cID, failed, err, tc.wantBuildID, tc.wantChunkID, tc.wantFailed)
			}
			c, b, _ := s.GetChunk(context.Background(), "chunk-1")
			if c.Status != tc.wantChunkStatus || b.Status != tc.wantBuildStatus {
				t.Errorf("stored chunk=%q build=%q, want chunk=%q build=%q", c.Status, b.Status, tc.wantChunkStatus, tc.wantBuildStatus)
			}
		})
	}
}

func TestRepoMatching_TableDriven(t *testing.T) {
	candidateTests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "empty repo returns nil",
			input:    "",
			expected: nil,
		},
		{
			name:     "bare repo adds default owner prefix",
			input:    "aerial",
			expected: []string{"aerial", "azylman/aerial"},
		},
		{
			name:     "bare repo with whitespace trims and adds default owner prefix",
			input:    "  aerial  ",
			expected: []string{"aerial", "azylman/aerial"},
		},
		{
			name:     "prefixed repo with default owner adds bare candidate",
			input:    "azylman/aerial",
			expected: []string{"azylman/aerial", "aerial"},
		},
		{
			name:     "other org repo returns single candidate",
			input:    "other-org/aerial",
			expected: []string{"other-org/aerial"},
		},
		{
			name:     "nested slash under default owner returns single candidate",
			input:    "azylman/sub/repo",
			expected: []string{"azylman/sub/repo"},
		},
	}

	for _, tc := range candidateTests {
		t.Run("candidates_"+tc.name, func(t *testing.T) {
			got := repoMatchCandidates(tc.input)
			if !slices.Equal(got, tc.expected) {
				t.Errorf("repoMatchCandidates(%q) = %v, want %v", tc.input, got, tc.expected)
			}
		})
	}
}

func TestResolveChunkRef(t *testing.T) {
	chunks := []VanadisChunk{
		{ID: "chunk-a", ChunkIndex: 0},
		{ID: "chunk-b", ChunkIndex: 1},
		{ID: "10", ChunkIndex: 2},
	}
	tests := []struct {
		name     string
		chunks   []VanadisChunk
		chunkRef string
		wantIdx  int
	}{
		{"empty_ref", chunks, "", -1},
		{"whitespace_ref", chunks, "   ", -1},
		{"match_id_exact", chunks, "chunk-a", 0},
		{"match_id_with_whitespace", chunks, "  chunk-b  ", 1},
		{"match_index_string", chunks, "0", 0},
		{"match_index_second", chunks, "1", 1},
		{"match_id_takes_precedence_over_index", chunks, "10", 2},
		{"no_match_index_out_of_bounds", chunks, "99", -1},
		{"no_match_unknown_id", chunks, "unknown", -1},
		{"empty_slice", nil, "chunk-a", -1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := resolveChunkRef(tc.chunks, tc.chunkRef)
			if got != tc.wantIdx {
				t.Errorf("resolveChunkRef() = %d, want %d", got, tc.wantIdx)
			}
		})
	}
}

func TestVanadisStore_UpdateBuild(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name        string
		storeErr    error
		seedBuild   *VanadisBuild
		buildID     string
		req         UpdateBuildRequest
		wantErrIs   error
		wantErr     bool
		wantTitle   string
		wantObj     string
		wantContext string
	}{
		{
			name:      "store_error",
			storeErr:  errors.New("boom"),
			buildID:   "b1",
			req:       UpdateBuildRequest{Title: "New"},
			wantErr:   true,
			wantErrIs: nil,
		},
		{
			name:      "empty_build_id",
			buildID:   "",
			req:       UpdateBuildRequest{Title: "New"},
			wantErr:   true,
			wantErrIs: ErrInvalidBuildInput,
		},
		{
			name:      "all_empty_fields",
			buildID:   "b1",
			req:       UpdateBuildRequest{Title: "   ", Objective: "  ", SharedContext: ""},
			wantErr:   true,
			wantErrIs: ErrInvalidBuildInput,
		},
		{
			name:      "build_not_found",
			buildID:   "missing-build",
			req:       UpdateBuildRequest{Title: "New Title"},
			wantErr:   true,
			wantErrIs: ErrBuildNotFound,
		},
		{
			name:      "build_not_running_conflict",
			seedBuild: &VanadisBuild{ID: "b-conflict", Status: "completed"},
			buildID:   "b-conflict",
			req:       UpdateBuildRequest{Title: "New Title"},
			wantErr:   true,
			wantErrIs: ErrBuildConflict,
		},
		{
			name:        "success_update_title_only",
			seedBuild:   &VanadisBuild{ID: "b-ok1", Title: "Old Title", Objective: "Original Obj", SharedContext: "Original Ctx", Status: "running"},
			buildID:     "b-ok1",
			req:         UpdateBuildRequest{Title: "Updated Title"},
			wantTitle:   "Updated Title",
			wantObj:     "Original Obj",
			wantContext: "Original Ctx",
		},
		{
			name:        "success_update_all_fields",
			seedBuild:   &VanadisBuild{ID: "b-ok2", Title: "Old Title", Objective: "Old Obj", Status: "running"},
			buildID:     "b-ok2",
			req:         UpdateBuildRequest{Title: "New Title", Objective: "New Obj", SharedContext: "New Ctx"},
			wantTitle:   "New Title",
			wantObj:     "New Obj",
			wantContext: "New Ctx",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newMemoryVanadisStore()
			if tc.storeErr != nil {
				s.updateBuildErr = tc.storeErr
			}
			if tc.seedBuild != nil {
				s.builds[tc.seedBuild.ID] = *tc.seedBuild
			}
			got, err := s.UpdateBuild(ctx, tc.buildID, tc.req)
			if tc.wantErrIs != nil {
				if !errors.Is(err, tc.wantErrIs) {
					t.Fatalf("expected error %v, got %v", tc.wantErrIs, err)
				}
				return
			}
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.Title != tc.wantTitle || got.Objective != tc.wantObj || got.SharedContext != tc.wantContext {
				t.Errorf("got title=%q obj=%q ctx=%q, want title=%q obj=%q ctx=%q", got.Title, got.Objective, got.SharedContext, tc.wantTitle, tc.wantObj, tc.wantContext)
			}
		})
	}
}

// seededStore returns a memory store holding build (when non-nil) and its chunks.
func seededStore(build *VanadisBuild, chunks []VanadisChunk) *memoryVanadisStore {
	s := newMemoryVanadisStore()
	if build != nil {
		s.builds[build.ID] = *build
	}
	if build != nil && len(chunks) > 0 {
		s.chunks[build.ID] = chunks
	}
	return s
}

func TestVanadisStore_UpdateChunk(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name         string
		storeErr     error
		seedBuild    *VanadisBuild
		seedChunks   []VanadisChunk
		buildID      string
		chunkRef     string
		req          UpdateChunkRequest
		wantErrIs    error
		wantErr      bool
		wantRepo     string
		wantTitle    string
		wantInstr    string
		wantEffort   string
		wantFilesLen int
		wantSymsLen  int
	}{
		{
			name:      "store_error",
			storeErr:  errors.New("boom"),
			buildID:   "b1",
			chunkRef:  "c1",
			req:       UpdateChunkRequest{Title: "T"},
			wantErr:   true,
			wantErrIs: nil,
		},
		{
			name:      "empty_build_id",
			buildID:   "",
			chunkRef:  "c1",
			req:       UpdateChunkRequest{Title: "T"},
			wantErr:   true,
			wantErrIs: ErrInvalidBuildInput,
		},
		{
			name:      "empty_chunk_ref",
			buildID:   "b1",
			chunkRef:  "",
			req:       UpdateChunkRequest{Title: "T"},
			wantErr:   true,
			wantErrIs: ErrInvalidBuildInput,
		},
		{
			name:      "no_updated_fields",
			buildID:   "b1",
			chunkRef:  "c1",
			req:       UpdateChunkRequest{Title: "  ", Repo: "  ", Effort: ""},
			wantErr:   true,
			wantErrIs: ErrInvalidBuildInput,
		},
		{
			name:      "invalid_effort",
			buildID:   "b1",
			chunkRef:  "c1",
			req:       UpdateChunkRequest{Effort: "ultra-high"},
			wantErr:   true,
			wantErrIs: ErrInvalidBuildInput,
		},
		{
			name:      "build_not_found",
			buildID:   "missing-build",
			chunkRef:  "c1",
			req:       UpdateChunkRequest{Title: "New Title"},
			wantErr:   true,
			wantErrIs: ErrBuildNotFound,
		},
		{
			name:       "build_not_running_conflict",
			seedBuild:  &VanadisBuild{ID: "b-stopped", Status: "completed"},
			seedChunks: []VanadisChunk{{ID: "c1", BuildID: "b-stopped", Status: "pending"}},
			buildID:    "b-stopped",
			chunkRef:   "c1",
			req:        UpdateChunkRequest{Title: "New Title"},
			wantErr:    true,
			wantErrIs:  ErrBuildConflict,
		},
		{
			name:       "chunk_not_found",
			seedBuild:  &VanadisBuild{ID: "b-running", Status: "running"},
			seedChunks: []VanadisChunk{{ID: "c1", BuildID: "b-running", Status: "pending"}},
			buildID:    "b-running",
			chunkRef:   "c999",
			req:        UpdateChunkRequest{Title: "New Title"},
			wantErr:    true,
			wantErrIs:  ErrChunkNotFound,
		},
		{
			name:       "chunk_not_pending_conflict",
			seedBuild:  &VanadisBuild{ID: "b-running", Status: "running"},
			seedChunks: []VanadisChunk{{ID: "c1", BuildID: "b-running", Status: "running"}},
			buildID:    "b-running",
			chunkRef:   "c1",
			req:        UpdateChunkRequest{Title: "New Title"},
			wantErr:    true,
			wantErrIs:  ErrBuildConflict,
		},
		{
			name:       "success_partial_update",
			seedBuild:  &VanadisBuild{ID: "b-running", Status: "running"},
			seedChunks: []VanadisChunk{{ID: "c1", BuildID: "b-running", ChunkIndex: 0, Repo: "aerial", Title: "Old Title", Instructions: "Old Instr", Status: "pending", Effort: "medium"}},
			buildID:    "b-running",
			chunkRef:   "0",
			req: UpdateChunkRequest{
				Repo:          "mirrormere",
				Title:         "Updated Title",
				Instructions:  "Updated Instr",
				Effort:        "low",
				TargetFiles:   []string{"foo.go"},
				TargetSymbols: []string{"Bar"},
			},
			wantRepo:     "mirrormere",
			wantTitle:    "Updated Title",
			wantInstr:    "Updated Instr",
			wantEffort:   "low",
			wantFilesLen: 1,
			wantSymsLen:  1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := seededStore(tc.seedBuild, tc.seedChunks)
			s.updateChunkErr = tc.storeErr
			got, err := s.UpdateChunk(ctx, tc.buildID, tc.chunkRef, tc.req)
			if assertStoreErr(t, err, tc.wantErrIs, tc.wantErr) {
				return
			}
			if got.Repo != tc.wantRepo || got.Title != tc.wantTitle || got.Instructions != tc.wantInstr || got.Effort != tc.wantEffort || len(got.TargetFiles) != tc.wantFilesLen || len(got.TargetSymbols) != tc.wantSymsLen {
				t.Errorf("unexpected updated chunk: %+v", got)
			}
		})
	}
}

func TestVanadisStore_AppendChunks(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name          string
		storeErr      error
		seedBuild     *VanadisBuild
		seedChunks    []VanadisChunk
		buildID       string
		inputs        []CreateChunkInput
		wantErrIs     error
		wantErr       bool
		wantCount     int
		wantFirstID   string
		wantFirstIdx  int
		wantFirstRepo string
	}{
		{
			name:      "store_error",
			storeErr:  errors.New("boom"),
			buildID:   "b1",
			inputs:    []CreateChunkInput{{Title: "T", Instructions: "I"}},
			wantErr:   true,
			wantErrIs: nil,
		},
		{
			name:      "empty_build_id",
			buildID:   "",
			inputs:    []CreateChunkInput{{Title: "T", Instructions: "I"}},
			wantErr:   true,
			wantErrIs: ErrInvalidBuildInput,
		},
		{
			name:      "empty_inputs",
			buildID:   "b1",
			inputs:    nil,
			wantErr:   true,
			wantErrIs: ErrInvalidBuildInput,
		},
		{
			name:      "empty_title",
			buildID:   "b1",
			inputs:    []CreateChunkInput{{Title: "", Instructions: "I"}},
			wantErr:   true,
			wantErrIs: ErrInvalidBuildInput,
		},
		{
			name:      "empty_instructions",
			buildID:   "b1",
			inputs:    []CreateChunkInput{{Title: "T", Instructions: "  "}},
			wantErr:   true,
			wantErrIs: ErrInvalidBuildInput,
		},
		{
			name:      "invalid_chunk_id",
			buildID:   "b1",
			inputs:    []CreateChunkInput{{ID: "bad:id", Title: "T", Instructions: "I"}},
			wantErr:   true,
			wantErrIs: ErrInvalidBuildInput,
		},
		{
			name:      "invalid_effort",
			buildID:   "b1",
			inputs:    []CreateChunkInput{{Title: "T", Instructions: "I", Effort: "gigantic"}},
			wantErr:   true,
			wantErrIs: ErrInvalidBuildInput,
		},

		{
			name:      "build_not_found",
			buildID:   "missing-b",
			inputs:    []CreateChunkInput{{Repo: "aerial", Title: "T", Instructions: "I"}},
			wantErr:   true,
			wantErrIs: ErrBuildNotFound,
		},
		{
			name:       "build_not_running_conflict",
			seedBuild:  &VanadisBuild{ID: "b-done", Status: "completed"},
			seedChunks: nil,
			buildID:    "b-done",
			inputs:     []CreateChunkInput{{Repo: "aerial", Title: "T", Instructions: "I"}},
			wantErr:    true,
			wantErrIs:  ErrBuildConflict,
		},
		{
			name:       "duplicate_explicit_id_conflict",
			seedBuild:  &VanadisBuild{ID: "b-run", Status: "running"},
			seedChunks: []VanadisChunk{{ID: "existing-c1", BuildID: "b-run", ChunkIndex: 0, Status: "pending"}},
			buildID:    "b-run",
			inputs:     []CreateChunkInput{{ID: "existing-c1", Repo: "aerial", Title: "T", Instructions: "I"}},
			wantErr:    true,
			wantErrIs:  ErrBuildConflict,
		},
		{
			name:      "success_append_to_empty_build",
			seedBuild: &VanadisBuild{ID: "b-empty", Status: "running"},
			buildID:   "b-empty",
			inputs: []CreateChunkInput{
				{Title: "C1", Instructions: "Do C1", Repo: "aerial"},
				{Title: "C2", Instructions: "Do C2", Repo: "aerial"},
			},
			wantCount:     2,
			wantFirstID:   "b-empty-chunk-1",
			wantFirstIdx:  0,
			wantFirstRepo: "aerial",
		},
		{
			name:      "success_append_inheriting_previous_repo_and_index",
			seedBuild: &VanadisBuild{ID: "b-exist", Status: "running"},
			seedChunks: []VanadisChunk{
				{ID: "b-exist-chunk-1", BuildID: "b-exist", ChunkIndex: 0, Repo: "mirrormere", Status: "running"},
			},
			buildID: "b-exist",
			inputs: []CreateChunkInput{
				{Title: "C2", Instructions: "Do C2", Repo: "mirrormere"},
			},
			wantCount:     1,
			wantFirstID:   "b-exist-chunk-2",
			wantFirstIdx:  1,
			wantFirstRepo: "mirrormere",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := seededStore(tc.seedBuild, tc.seedChunks)
			s.appendChunksErr = tc.storeErr
			got, err := s.AppendChunks(ctx, tc.buildID, tc.inputs)
			if assertStoreErr(t, err, tc.wantErrIs, tc.wantErr) {
				return
			}
			if len(got) != tc.wantCount {
				t.Fatalf("got %d chunks, want %d", len(got), tc.wantCount)
			}
			if got[0].ID != tc.wantFirstID || got[0].ChunkIndex != tc.wantFirstIdx || got[0].Repo != tc.wantFirstRepo {
				t.Errorf("chunk[0] mismatch: got %+v, want ID=%q Idx=%d Repo=%q", got[0], tc.wantFirstID, tc.wantFirstIdx, tc.wantFirstRepo)
			}
		})
	}
}

func TestVanadisStore_RemoveChunk(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name          string
		storeErr      error
		seedBuild     *VanadisBuild
		seedChunks    []VanadisChunk
		buildID       string
		chunkRef      string
		wantErrIs     error
		wantErr       bool
		wantRemovedID string
		wantRemainLen int
	}{
		{
			name:      "store_error",
			storeErr:  errors.New("boom"),
			buildID:   "b1",
			chunkRef:  "c1",
			wantErr:   true,
			wantErrIs: nil,
		},
		{
			name:      "empty_build_id",
			buildID:   "",
			chunkRef:  "c1",
			wantErr:   true,
			wantErrIs: ErrInvalidBuildInput,
		},
		{
			name:      "empty_chunk_ref",
			buildID:   "b1",
			chunkRef:  "",
			wantErr:   true,
			wantErrIs: ErrInvalidBuildInput,
		},
		{
			name:      "build_not_found",
			buildID:   "missing-b",
			chunkRef:  "c1",
			wantErr:   true,
			wantErrIs: ErrBuildNotFound,
		},
		{
			name:       "build_not_running_conflict",
			seedBuild:  &VanadisBuild{ID: "b-done", Status: "completed"},
			seedChunks: []VanadisChunk{{ID: "c1", BuildID: "b-done", ChunkIndex: 0, Status: "pending"}},
			buildID:    "b-done",
			chunkRef:   "c1",
			wantErr:    true,
			wantErrIs:  ErrBuildConflict,
		},
		{
			name:       "chunk_not_found",
			seedBuild:  &VanadisBuild{ID: "b-run", Status: "running"},
			seedChunks: []VanadisChunk{{ID: "c1", BuildID: "b-run", ChunkIndex: 0, Status: "pending"}},
			buildID:    "b-run",
			chunkRef:   "missing-c",
			wantErr:    true,
			wantErrIs:  ErrChunkNotFound,
		},
		{
			name:      "chunk_not_pending_conflict",
			seedBuild: &VanadisBuild{ID: "b-run", Status: "running"},
			seedChunks: []VanadisChunk{
				{ID: "c1", BuildID: "b-run", ChunkIndex: 0, Status: "running"},
				{ID: "c2", BuildID: "b-run", ChunkIndex: 1, Status: "pending"},
			},
			buildID:   "b-run",
			chunkRef:  "c1",
			wantErr:   true,
			wantErrIs: ErrBuildConflict,
		},
		{
			name:       "cannot_remove_only_chunk_conflict",
			seedBuild:  &VanadisBuild{ID: "b-run", Status: "running"},
			seedChunks: []VanadisChunk{{ID: "c1", BuildID: "b-run", ChunkIndex: 0, Status: "pending"}},
			buildID:    "b-run",
			chunkRef:   "c1",
			wantErr:    true,
			wantErrIs:  ErrBuildConflict,
		},
		{
			name:      "success_remove_middle_chunk_and_compact",
			seedBuild: &VanadisBuild{ID: "b-run", Status: "running"},
			seedChunks: []VanadisChunk{
				{ID: "c1", BuildID: "b-run", ChunkIndex: 0, Status: "running"},
				{ID: "c2", BuildID: "b-run", ChunkIndex: 1, Status: "pending"},
				{ID: "c3", BuildID: "b-run", ChunkIndex: 2, Status: "pending"},
			},
			buildID:       "b-run",
			chunkRef:      "1", // resolves to c2 by index
			wantRemovedID: "c2",
			wantRemainLen: 2,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := seededStore(tc.seedBuild, tc.seedChunks)
			s.removeChunkErr = tc.storeErr
			removed, remaining, err := s.RemoveChunk(ctx, tc.buildID, tc.chunkRef)
			if assertStoreErr(t, err, tc.wantErrIs, tc.wantErr) {
				return
			}
			if removed.ID != tc.wantRemovedID {
				t.Errorf("removed.ID = %q, want %q", removed.ID, tc.wantRemovedID)
			}
			if len(remaining) != tc.wantRemainLen {
				t.Errorf("len(remaining) = %d, want %d", len(remaining), tc.wantRemainLen)
			}
		})
	}
}

const wantStallNotice = "[VANADIS CHUNK STALLED]\nYour previous turn for chunk c-stall ended without a successful `aerial-pr.sh submit` and without calling the vanadis MCP tool fail_chunk, so no PR was linked. This is the single stall retry: reuse the existing scratch workspace from your prior attempt if it still exists instead of re-initializing, finish the remaining implementation and verification in the foreground, then submit with `--vanadis-chunk c-stall` or call fail_chunk with the blocker. If this turn also ends without either, the chunk and build will be failed.\n\n"

// stallDispatch is the observable shape of one /dispatch request made by a stall re-dispatch.
type stallDispatch struct {
	ID, SessionID, TargetID, ChunkID, Notice string
	SkipIfActive                             bool
}

// stallBrain records /dispatch and /prompt traffic from ProcessStalledChunkCheck.
type stallBrain struct {
	mu           sync.Mutex
	dispatchCode int
	dispatches   []stallDispatch
	wakeTails    []string
}

func (b *stallBrain) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch r.URL.Path {
	case "/dispatch":
		var req BrainDispatchRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		notice, _, _ := strings.Cut(req.Prompt, "[VANADIS BUILD CHUNK EXECUTION]")
		b.dispatches = append(b.dispatches, stallDispatch{ID: req.ID, SessionID: req.SessionID, TargetID: req.TargetID, ChunkID: req.VanadisChunkID, Notice: notice, SkipIfActive: req.SkipIfActive})
		w.WriteHeader(b.dispatchCode)
	case "/prompt":
		var p map[string]string
		_ = json.NewDecoder(r.Body).Decode(&p)
		b.wakeTails = append(b.wakeTails, p["prompt"][strings.LastIndex(p["prompt"], "\n")+1:])
		w.WriteHeader(http.StatusAccepted)
	default:
		w.WriteHeader(http.StatusOK)
	}
}

func TestProcessStalledChunkCheck(t *testing.T) {
	t.Parallel()
	if NewVanadisServer(Config{}, nil, nil).ProcessStalledChunkCheck(context.Background(), "c-stall") == nil {
		t.Fatal("expected error when store is nil")
	}
	redispatch := []stallDispatch{{ID: "vanadis-c-stall-turn-1", SessionID: "vanadis-c-stall", TargetID: "123456789012345678", ChunkID: "c-stall", Notice: wantStallNotice, SkipIfActive: true}}
	tests := []struct {
		name                         string
		claimID, status, buildStatus string
		prNumber, priorNudges        int
		dispatchID                   string
		dispatchCode                 int
		wantDispatches               []stallDispatch
		wantChunkStatus              string
		wantBuildStatus              string
		wantWakeTails                []string
	}{
		{name: "first stall re-dispatches with notice", claimID: "c-stall", status: "running", buildStatus: "running", dispatchCode: http.StatusAccepted, wantDispatches: redispatch, wantChunkStatus: "running", wantBuildStatus: "running"},
		{name: "second stall fails chunk and wakes thread", claimID: "c-stall", status: "running", buildStatus: "running", priorNudges: 1, dispatchCode: http.StatusAccepted, wantChunkStatus: "failed", wantBuildStatus: "failed", wantWakeTails: []string{"Chunk 1/1 (Stalled Chunk) failed: chunk stalled twice: its turn ended without submitting a PR or calling fail_chunk, both on the original dispatch and on the stall re-dispatch"}},
		{name: "re-dispatch failure fails chunk", claimID: "c-stall", status: "running", buildStatus: "running", dispatchCode: http.StatusInternalServerError, wantDispatches: redispatch, wantChunkStatus: "failed", wantBuildStatus: "failed", wantWakeTails: []string{"Chunk 1/1 (Stalled Chunk) failed: chunk stalled (turn ended without submitting a PR or calling fail_chunk) and the stall re-dispatch failed: /dispatch returned status 500:"}},
		{name: "linked pr is left alone", claimID: "c-stall", status: "pr_opened", buildStatus: "running", prNumber: 9, dispatchCode: http.StatusAccepted, wantChunkStatus: "pr_opened", wantBuildStatus: "running"},
		{name: "missing chunk is skipped", claimID: "c-gone", status: "running", buildStatus: "running", dispatchCode: http.StatusAccepted, wantChunkStatus: "running", wantBuildStatus: "running"},
		{name: "duplicate delivery with mismatched dispatch id is noop", claimID: "c-stall", status: "running", buildStatus: "running", priorNudges: 1, dispatchID: "vanadis-c-stall-turn-0", dispatchCode: http.StatusAccepted, wantChunkStatus: "running", wantBuildStatus: "running"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			recorder := &stallBrain{dispatchCode: tc.dispatchCode}
			brain := httptest.NewServer(recorder)
			defer brain.Close()
			store := newMemoryVanadisStore()
			c := mkChunk("c-stall", "b-stall", 0, "Stalled Chunk", "do it", "low", tc.status)
			c.PRNumber = tc.prNumber
			_ = store.CreateBuild(context.Background(), mkBuild("b-stall", "123456789012345678", "T", "O", tc.buildStatus), []VanadisChunk{c})
			store.stallNudges = map[string]int{"c-stall": tc.priorNudges}
			srv := NewVanadisServer(Config{BrainURL: brain.URL}, store, brain.Client())
			if err := srv.ProcessStalledChunkCheck(context.Background(), tc.claimID, tc.dispatchID); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			b, chunks, _ := store.GetBuild(context.Background(), "b-stall")
			recorder.mu.Lock()
			defer recorder.mu.Unlock()
			if !reflect.DeepEqual(recorder.dispatches, tc.wantDispatches) {
				t.Fatalf("dispatches = %+v, want %+v", recorder.dispatches, tc.wantDispatches)
			}
			if !reflect.DeepEqual(recorder.wakeTails, tc.wantWakeTails) {
				t.Fatalf("wake tails = %q, want %q", recorder.wakeTails, tc.wantWakeTails)
			}
			if chunks[0].Status != tc.wantChunkStatus || b.Status != tc.wantBuildStatus {
				t.Fatalf("chunk=%q build=%q, want chunk=%q build=%q", chunks[0].Status, b.Status, tc.wantChunkStatus, tc.wantBuildStatus)
			}
		})
	}
}

func TestServer_CompleteChunkRunsStallCheck(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var dispatches, targetIDs, chunkIDs []string
	brain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/dispatch" {
			var req BrainDispatchRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			mu.Lock()
			dispatches = append(dispatches, req.SessionID)
			targetIDs = append(targetIDs, req.TargetID)
			chunkIDs = append(chunkIDs, req.VanadisChunkID)
			mu.Unlock()
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer brain.Close()
	store := newMemoryVanadisStore()
	_ = store.CreateBuild(context.Background(), mkBuild("b-done", "123456789012345678", "T", "O", "running"), []VanadisChunk{mkChunk("c-done", "b-done", 0, "Done", "do it", "", "running")})
	server := NewVanadisServer(Config{BrainURL: brain.URL}, store, brain.Client())
	ev := pubsub.DispatchOutcomeEvent{
		DispatchID:     "vanadis-c-done-turn-0",
		VanadisChunkID: "c-done",
		Status:         "completed",
	}
	evData, _ := json.Marshal(ev)
	if err := server.HandleDispatchOutcomeEvent(context.Background(), &memoryMsg{data: evData, subject: "aerial.dispatch.completed"}); err != nil {
		t.Fatalf("HandleDispatchOutcomeEvent failed: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if !reflect.DeepEqual(dispatches, []string{"vanadis-c-done"}) {
		t.Fatalf("dispatches = %v, want [vanadis-c-done]", dispatches)
	}
	if !reflect.DeepEqual(targetIDs, []string{"123456789012345678"}) || !reflect.DeepEqual(chunkIDs, []string{"c-done"}) {
		t.Fatalf("targetIDs = %v chunkIDs = %v, want [123456789012345678], [c-done]", targetIDs, chunkIDs)
	}
}
