package main

import (
	"context"
	"encoding/json"
	"errors"
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

func TestHelpers_WidgetStatus(t *testing.T) {
	normTests := []struct {
		in   string
		want string
	}{
		{"ci_failed", "failed"},
		{"deploy_failed", "failed"},
		{"RUNNING", "running"},
		{"open", "open"},
	}
	for _, tc := range normTests {
		t.Run("normalize_"+tc.in, func(t *testing.T) {
			if got := normalizeWidgetStatus(tc.in); got != tc.want {
				t.Errorf("normalizeWidgetStatus(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}

	resolveTests := []struct {
		chunkStatus string
		prStatus    string
		want        string
	}{
		{"failed", "open", "open"},
		{"merged", "open", "merged"},
		{"failed", "deployed", "deployed"},
		{"pending", "deployed", "deployed"},
		{"pending", "deploying", "deploying"},
		{"pending", "synced", "synced"},
		{"pending", "failed", "failed"},
		{"pending", "conflict", "conflict"},
		{"pending", "merged", "merged"},
		{"merged", "closed", "merged"},
		{"pending", "closed", "failed"},
		{"coding", "unknown", "coding"},
		{"", "", "open"},
	}
	for _, tc := range resolveTests {
		t.Run("resolve_"+tc.chunkStatus+"_"+tc.prStatus, func(t *testing.T) {
			if got := resolveVanadisChunkWidgetStatus(tc.chunkStatus, tc.prStatus); got != tc.want {
				t.Errorf("resolveVanadisChunkWidgetStatus(%q, %q) = %q, want %q", tc.chunkStatus, tc.prStatus, got, tc.want)
			}
		})
	}

	streamingTests := []struct {
		in   string
		want bool
	}{
		{"open", true},
		{"pr_opened", true},
		{"merged", true},
		{"synced", true},
		{"deploying", true},
		{"deployed", false},
		{"failed", false},
		{"pending", false},
		{"running", false},
		{"", false},
		{"  OPEN  ", true},
	}
	for _, tc := range streamingTests {
		t.Run("streaming_"+tc.in, func(t *testing.T) {
			if got := isVanadisChunkStreamingPhase(tc.in); got != tc.want {
				t.Errorf("isVanadisChunkStreamingPhase(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}

	syncWidgetTests := []struct {
		name                string
		build               VanadisBuild
		chunks              []VanadisChunk
		req                 SyncChunkPRWidgetRequest
		wantStatus          string
		wantSkipSink        bool
		wantActiveStep      string
		wantZeroCompletedAt bool
	}{
		{
			name: "preserves active step and allows sink when chunk is open and build running with message",
			build: VanadisBuild{
				ID:              "build-1",
				Status:          "running",
				StatusMessageID: "msg-123",
				Title:           "Test Build",
				CreatedAt:       time.Unix(1760000000, 0),
			},
			chunks: []VanadisChunk{
				{ID: "c1", ChunkIndex: 0, PRNumber: 101, Status: "open"},
			},
			req: SyncChunkPRWidgetRequest{
				BuildID:    "build-1",
				Repo:       "aerial",
				PRNumber:   101,
				PRStatus:   "open",
				ActiveStep: "Waiting on CI (2/5 passed)",
				PRFound:    true,
			},
			wantStatus:     "running",
			wantSkipSink:   false,
			wantActiveStep: "Waiting on CI (2/5 passed)",
		},
		{
			name: "sets active step on pr_opened chunk",
			build: VanadisBuild{
				ID:              "build-2",
				Status:          "running",
				StatusMessageID: "msg-456",
				Title:           "Test Build 2",
			},
			chunks: []VanadisChunk{
				{ID: "c2", ChunkIndex: 0, PRNumber: 102, Status: "pr_opened"},
			},
			req: SyncChunkPRWidgetRequest{
				BuildID:    "build-2",
				Repo:       "aerial",
				PRNumber:   102,
				PRStatus:   "open",
				ActiveStep: "Waiting on CI (3/3 passed)",
				PRFound:    true,
			},
			wantStatus:     "running",
			wantSkipSink:   false,
			wantActiveStep: "Waiting on CI (3/3 passed)",
		},
		{
			name: "omits skipsink when build status message ID is empty",
			build: VanadisBuild{
				ID:              "build-3",
				Status:          "running",
				StatusMessageID: "",
				Title:           "Test Build 3",
			},
			chunks: []VanadisChunk{
				{ID: "c3", ChunkIndex: 0, PRNumber: 103, Status: "open"},
			},
			req: SyncChunkPRWidgetRequest{
				BuildID:    "build-3",
				Repo:       "aerial",
				PRNumber:   103,
				PRStatus:   "open",
				ActiveStep: "Waiting on CI (1/2 passed)",
				PRFound:    true,
			},
			wantStatus:     "running",
			wantSkipSink:   false,
			wantActiveStep: "Waiting on CI (1/2 passed)",
		},
		{
			name: "omits skipsink and active step when pr status is failed",
			build: VanadisBuild{
				ID:              "build-4",
				Status:          "running",
				StatusMessageID: "msg-789",
				Title:           "Test Build 4",
			},
			chunks: []VanadisChunk{
				{ID: "c4", ChunkIndex: 0, PRNumber: 104, Status: "pr_opened"},
			},
			req: SyncChunkPRWidgetRequest{
				BuildID:    "build-4",
				Repo:       "aerial",
				PRNumber:   104,
				PRStatus:   "failed",
				ActiveStep: "Waiting on CI (0/2 passed)",
				PRFound:    true,
			},
			wantStatus:     "failed",
			wantSkipSink:   false,
			wantActiveStep: "",
		},
		{
			name: "omits skipsink when active step is whitespace",
			build: VanadisBuild{
				ID:              "build-5",
				Status:          "running",
				StatusMessageID: "msg-999",
				Title:           "Test Build 5",
			},
			chunks: []VanadisChunk{
				{ID: "c5", ChunkIndex: 0, PRNumber: 105, Status: "open"},
			},
			req: SyncChunkPRWidgetRequest{
				BuildID:    "build-5",
				Repo:       "aerial",
				PRNumber:   105,
				PRStatus:   "open",
				ActiveStep: "   ",
				PRFound:    true,
			},
			wantStatus:     "running",
			wantSkipSink:   false,
			wantActiveStep: "",
		},
		{
			name: "preserves active step and allows sink when chunk is merged",
			build: VanadisBuild{
				ID:              "build-merged",
				Status:          "running",
				StatusMessageID: "msg-merged",
				Title:           "Test Build Merged",
			},
			chunks: []VanadisChunk{
				{ID: "c-merged", ChunkIndex: 0, PRNumber: 106, Status: "open"},
			},
			req: SyncChunkPRWidgetRequest{
				BuildID:    "build-merged",
				Repo:       "aerial",
				PRNumber:   106,
				PRStatus:   "merged",
				ActiveStep: "Building container images (1/2 completed)",
				PRFound:    true,
			},
			wantStatus:     "running",
			wantSkipSink:   false,
			wantActiveStep: "Building container images (1/2 completed)",
		},
		{
			name: "preserves active step and allows sink when chunk is synced",
			build: VanadisBuild{
				ID:              "build-synced",
				Status:          "running",
				StatusMessageID: "msg-synced",
				Title:           "Test Build Synced",
			},
			chunks: []VanadisChunk{
				{ID: "c-synced", ChunkIndex: 0, PRNumber: 107, Status: "merged"},
			},
			req: SyncChunkPRWidgetRequest{
				BuildID:    "build-synced",
				Repo:       "aerial",
				PRNumber:   107,
				PRStatus:   "synced",
				ActiveStep: "Building container images (2/2 completed)",
				PRFound:    true,
			},
			wantStatus:     "running",
			wantSkipSink:   false,
			wantActiveStep: "Building container images (2/2 completed)",
		},
		{
			name: "preserves active step and allows sink when chunk is deploying",
			build: VanadisBuild{
				ID:              "build-deploying",
				Status:          "running",
				StatusMessageID: "msg-deploying",
				Title:           "Test Build Deploying",
			},
			chunks: []VanadisChunk{
				{ID: "c-deploying", ChunkIndex: 0, PRNumber: 108, Status: "synced"},
			},
			req: SyncChunkPRWidgetRequest{
				BuildID:    "build-deploying",
				Repo:       "aerial",
				PRNumber:   108,
				PRStatus:   "deploying",
				ActiveStep: "`eri`: 🔄 starting",
				PRFound:    true,
			},
			wantStatus:     "running",
			wantSkipSink:   false,
			wantActiveStep: "`eri`: 🔄 starting",
		},
		{
			name: "clears active step on deployed chunk",
			build: VanadisBuild{
				ID:              "build-deployed",
				Status:          "running",
				StatusMessageID: "msg-deployed",
				Title:           "Test Build Deployed",
			},
			chunks: []VanadisChunk{
				{ID: "c-deployed", ChunkIndex: 0, PRNumber: 109, Status: "deploying"},
			},
			req: SyncChunkPRWidgetRequest{
				BuildID:    "build-deployed",
				Repo:       "aerial",
				PRNumber:   109,
				PRStatus:   "deployed",
				ActiveStep: "All jobs healthy",
				PRFound:    true,
			},
			wantStatus:     "running",
			wantSkipSink:   false,
			wantActiveStep: "",
		},
		{
			name: "recovers failed build and failed chunk on open pr",
			build: VanadisBuild{
				ID:              "build-recover",
				Status:          "failed",
				StatusMessageID: "msg-recover",
				Title:           "Test Build Recover",
				CreatedAt:       time.Unix(1760000000, 0),
			},
			chunks: []VanadisChunk{
				{ID: "c-recover", ChunkIndex: 0, PRNumber: 110, Status: "failed", CompletedAt: time.Unix(1760001000, 0)},
			},
			req: SyncChunkPRWidgetRequest{
				BuildID:    "build-recover",
				Repo:       "aerial",
				PRNumber:   110,
				PRStatus:   "open",
				ActiveStep: "Waiting on CI (1/4 passed)",
				PRFound:    true,
			},
			wantStatus:          "running",
			wantSkipSink:        false,
			wantActiveStep:      "Waiting on CI (1/4 passed)",
			wantZeroCompletedAt: true,
		},
		{
			name: "resolves repo to action for action chunk",
			build: VanadisBuild{
				ID:              "build-action-widget",
				Status:          "running",
				StatusMessageID: "msg-action",
				Title:           "Test Action Widget",
			},
			chunks: []VanadisChunk{
				{ID: "c-action", ChunkIndex: 0, Mode: "action", Repo: "", Status: "running"},
			},
			req: SyncChunkPRWidgetRequest{
				BuildID: "build-action-widget",
			},
			wantStatus:   "running",
			wantSkipSink: false,
		},
		{
			name: "maps completed action chunk to deployed status",
			build: VanadisBuild{
				ID:              "build-action-completed",
				Status:          "completed",
				StatusMessageID: "msg-action-completed",
				Title:           "Test Action Completed Widget",
			},
			chunks: []VanadisChunk{
				{ID: "c-action-done", ChunkIndex: 0, Mode: "action", Repo: "", Status: "completed"},
			},
			req: SyncChunkPRWidgetRequest{
				BuildID: "build-action-completed",
			},
			wantStatus:   "completed",
			wantSkipSink: false,
		},
	}
	for _, tc := range syncWidgetTests {
		t.Run("sync_widget_"+tc.name, func(t *testing.T) {
			got := buildSyncWidgetRequest(tc.build, tc.chunks, tc.req)
			if got.Status != tc.wantStatus {
				t.Errorf("buildSyncWidgetRequest Status = %q, want %q", got.Status, tc.wantStatus)
			}
			if got.SkipSink != tc.wantSkipSink {
				t.Errorf("buildSyncWidgetRequest SkipSink = %v, want %v", got.SkipSink, tc.wantSkipSink)
			}
			if len(got.Chunks) > 0 && got.Chunks[0].ActiveStep != tc.wantActiveStep {
				t.Errorf("buildSyncWidgetRequest Chunk[0].ActiveStep = %q, want %q", got.Chunks[0].ActiveStep, tc.wantActiveStep)
			}
			if len(got.Chunks) > 0 && len(tc.chunks) > 0 && tc.chunks[0].Mode == "action" && got.Chunks[0].Repo != "action" {
				t.Errorf("buildSyncWidgetRequest Chunk[0].Repo = %q, want action", got.Chunks[0].Repo)
			}
			if len(got.Chunks) > 0 && len(tc.chunks) > 0 && tc.chunks[0].Mode == "action" && tc.chunks[0].Status == "completed" && got.Chunks[0].Status != "deployed" {
				t.Errorf("buildSyncWidgetRequest Chunk[0].Status = %q, want deployed", got.Chunks[0].Status)
			}
			if tc.wantZeroCompletedAt && len(got.Chunks) > 0 && !got.Chunks[0].CompletedAt.IsZero() {
				t.Errorf("buildSyncWidgetRequest Chunk[0].CompletedAt = %v, want zero", got.Chunks[0].CompletedAt)
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

func TestVanadisStore_ChunkLifecycle(t *testing.T) {
	ctx := context.Background()

	t.Run("GetChunk_Success", func(t *testing.T) {
		s, bID, cIDs := setupTestStoreWithBuild()
		c, b, err := s.GetChunk(ctx, cIDs[0])
		if err != nil || c == nil || b == nil {
			t.Fatalf("GetChunk error: %v, c=%v, b=%v", err, c, b)
		}
		if c.ID != cIDs[0] || b.ID != bID {
			t.Errorf("got c.ID=%q, b.ID=%q, want %q, %q", c.ID, b.ID, cIDs[0], bID)
		}
	})

	t.Run("GetChunk_NotFound", func(t *testing.T) {
		s, _, _ := setupTestStoreWithBuild()
		c, b, err := s.GetChunk(ctx, "nonexistent")
		if !errors.Is(err, ErrChunkNotFound) || c != nil || b != nil {
			t.Errorf("expected ErrChunkNotFound, got err=%v", err)
		}
	})

	t.Run("GetChunk_BuildNotFound", func(t *testing.T) {
		s, _, cIDs := setupTestStoreWithBuild()
		delete(s.builds, "build-test-1")
		c, b, err := s.GetChunk(ctx, cIDs[0])
		if !errors.Is(err, ErrBuildNotFound) || c != nil || b != nil {
			t.Errorf("expected ErrBuildNotFound, got err=%v", err)
		}
	})

	t.Run("AttachChunkPR_Success", func(t *testing.T) {
		s, _, cIDs := setupTestStoreWithBuild()
		threadID, err := s.AttachChunkPR(ctx, cIDs[0], 101, "aerial")
		if err != nil || threadID != "123456789012345678" {
			t.Fatalf("AttachChunkPR err=%v, threadID=%q", err, threadID)
		}
		c, _, _ := s.GetChunk(ctx, cIDs[0])
		if c.PRNumber != 101 || c.Status != "pr_opened" {
			t.Errorf("expected pr_number=101, status=pr_opened, got %+v", c)
		}

		threadID, err = s.AttachChunkPR(ctx, cIDs[0], 102, "aerial")
		if err != nil || threadID != "123456789012345678" {
			t.Fatalf("AttachChunkPR overwrite err=%v, threadID=%q", err, threadID)
		}
		c, _, _ = s.GetChunk(ctx, cIDs[0])
		if c.PRNumber != 102 {
			t.Errorf("expected overwritten pr_number=102, got %d", c.PRNumber)
		}
	})

	t.Run("AttachChunkPR_Errors", func(t *testing.T) {
		tests := []struct {
			name        string
			chunkID     string
			repo        string
			buildStatus string
			chunkStatus string
			wantErr     error
			wantSubstr  string
		}{
			{name: "mismatched repo", chunkID: "chunk-1", repo: "other-repo", wantSubstr: "does not match"},
			{name: "chunk not found", chunkID: "missing", repo: "aerial", wantErr: ErrChunkNotFound},
			{name: "empty chunk ID", chunkID: "", repo: "aerial", wantSubstr: "chunk id and valid pr number are required"},
			{name: "parent build completed returns ErrBuildConflict", chunkID: "chunk-1", repo: "aerial", buildStatus: "completed", wantErr: ErrBuildConflict},
			{name: "parent build cancelled returns ErrBuildConflict", chunkID: "chunk-1", repo: "aerial", buildStatus: "cancelled", wantErr: ErrBuildConflict},
			{name: "chunk cancelled returns ErrBuildConflict", chunkID: "chunk-1", repo: "aerial", chunkStatus: "cancelled", wantErr: ErrBuildConflict},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				s, bID, cIDs := setupTestStoreWithBuild()
				_, _ = s.AttachChunkPR(ctx, cIDs[0], 101, "aerial")
				if tc.buildStatus != "" {
					b := s.builds[bID]
					b.Status = tc.buildStatus
					s.builds[bID] = b
				}
				if tc.chunkStatus != "" {
					setTestChunkStatus(s, bID, tc.chunkID, tc.chunkStatus)
				}
				_, err := s.AttachChunkPR(ctx, tc.chunkID, 101, tc.repo)
				if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
					t.Errorf("expected error %v, got %v", tc.wantErr, err)
				}
				if tc.wantSubstr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantSubstr)) {
					t.Errorf("expected error containing %q, got %v", tc.wantSubstr, err)
				}
			})
		}
	})

	t.Run("GetChunkByPR_Lifecycle", func(t *testing.T) {
		s, bID, cIDs := setupTestStoreWithBuild()
		_, _ = s.AttachChunkPR(ctx, cIDs[0], 101, "aerial")

		res, err := s.GetChunkByPR(ctx, "aerial", 101)
		if err != nil || res == nil || !res.Matched || !res.HasRemainingChunks || res.BuildID != bID || res.ChunkID != cIDs[0] || res.NextChunkMode != "action" {
			t.Fatalf("unexpected GetChunkByPR result: %+v, err: %v", res, err)
		}

		emptyRes, err := s.GetChunkByPR(ctx, "aerial", 999)
		if err != nil || emptyRes.Matched {
			t.Fatalf("expected unmatched, got %+v", emptyRes)
		}

		b := s.builds[bID]
		b.Status = "cancelled"
		s.builds[bID] = b
		cancRes, err := s.GetChunkByPR(ctx, "aerial", 101)
		if err != nil || cancRes.Matched {
			t.Errorf("expected cancelled build to be unmatchable, got %+v", cancRes)
		}

		invRes, err := s.GetChunkByPR(ctx, "", 0)
		if err != nil || invRes.Matched {
			t.Errorf("expected empty lookup for invalid params, got %+v", invRes)
		}
	})

	t.Run("AdvanceChunkByPR_Lifecycle", func(t *testing.T) {
		s, bID, cIDs := setupTestStoreWithBuild()
		_, _ = s.AttachChunkPR(ctx, cIDs[0], 101, "aerial")
		_, _ = s.AttachChunkPR(ctx, cIDs[1], 102, "aerial")

		adv1, err := s.AdvanceChunkByPR(ctx, "aerial", 101)
		if err != nil || adv1 == nil || !adv1.Matched || !adv1.Advanced || adv1.BuildCompleted || adv1.NextChunkIndex != 1 {
			t.Fatalf("unexpected adv1: %+v, err: %v", adv1, err)
		}

		sRec, bRecID, cRecIDs := setupTestStoreWithBuild()
		_, _ = sRec.AttachChunkPR(ctx, cRecIDs[0], 111, "aerial")
		_, _, _, _ = sRec.FailChunkByPR(ctx, "aerial", 111)
		advRecov, err := sRec.AdvanceChunkByPR(ctx, "aerial", 111)
		if err != nil || advRecov == nil || !advRecov.Matched || !advRecov.Advanced || advRecov.BuildStatus != "running" || advRecov.NextChunkIndex != 1 {
			t.Fatalf("expected failed build/chunk to recover on PR merge, got %+v, err: %v", advRecov, err)
		}
		if bRec, _, _ := sRec.GetBuild(ctx, bRecID); bRec.Status != "running" {
			t.Fatalf("expected recovered build status running, got %q", bRec.Status)
		}

		adv1Repeat, err := s.AdvanceChunkByPR(ctx, "aerial", 101)
		if err != nil || adv1Repeat == nil || !adv1Repeat.Matched || adv1Repeat.Advanced {
			t.Fatalf("expected adv1Repeat to not advance, got %+v", adv1Repeat)
		}

		adv2, err := s.AdvanceChunkByPR(ctx, "aerial", 102)
		if err != nil || adv2 == nil || !adv2.Matched || !adv2.Advanced || !adv2.BuildCompleted || adv2.NextChunkIndex != -1 || adv2.BuildStatus != "completed" {
			t.Fatalf("unexpected adv2: %+v, err: %v", adv2, err)
		}

		adv2Repeat, err := s.AdvanceChunkByPR(ctx, "aerial", 102)
		if err != nil || adv2Repeat == nil || !adv2Repeat.Matched || adv2Repeat.Advanced || !adv2Repeat.BuildCompleted || len(adv2Repeat.Chunks) != 2 {
			t.Fatalf("expected adv2Repeat to return completed with chunks populated and Advanced=false, got %+v, err: %v", adv2Repeat, err)
		}

		b, _, _ := s.GetBuild(ctx, bID)
		if b.Status != "completed" {
			t.Errorf("expected build status completed, got %q", b.Status)
		}

		advUnmatched, err := s.AdvanceChunkByPR(ctx, "aerial", 999)
		if err != nil || advUnmatched.Matched {
			t.Errorf("expected unmatched advance, got %+v", advUnmatched)
		}

		advInvalid, err := s.AdvanceChunkByPR(ctx, "", 0)
		if err != nil || advInvalid.Matched {
			t.Errorf("expected unmatched advance for invalid params, got %+v", advInvalid)
		}
	})

	t.Run("FailChunkByPR_Lifecycle", func(t *testing.T) {
		s, bID, cIDs := setupTestStoreWithBuild()
		_, _ = s.AttachChunkPR(ctx, cIDs[0], 101, "aerial")

		bIDGot, cIDGot, failed, err := s.FailChunkByPR(ctx, "aerial", 101)
		if err != nil || !failed || bIDGot != bID || cIDGot != cIDs[0] {
			t.Fatalf("FailChunkByPR got (%s, %s, %v, %v)", bIDGot, cIDGot, failed, err)
		}
		c, b, _ := s.GetChunk(ctx, cIDs[0])
		if c.Status != "failed" || b.Status != "failed" {
			t.Errorf("expected failed status, got c=%s, b=%s", c.Status, b.Status)
		}

		_, _, repeatFailed, _ := s.FailChunkByPR(ctx, "aerial", 101)
		if repeatFailed {
			t.Error("expected second fail call to not fail already failed chunk")
		}

		_, _, unkFailed, _ := s.FailChunkByPR(ctx, "aerial", 999)
		if unkFailed {
			t.Error("expected fail on unknown PR to return false")
		}

		_, _, invFailed, _ := s.FailChunkByPR(ctx, "", 0)
		if invFailed {
			t.Error("expected fail on invalid args to return false")
		}
	})
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
			s := newMemoryVanadisStore()
			if tc.storeErr != nil {
				s.updateChunkErr = tc.storeErr
			}
			if tc.seedBuild != nil {
				s.builds[tc.seedBuild.ID] = *tc.seedBuild
			}
			if len(tc.seedChunks) > 0 {
				s.chunks[tc.seedBuild.ID] = tc.seedChunks
			}
			got, err := s.UpdateChunk(ctx, tc.buildID, tc.chunkRef, tc.req)
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
			s := newMemoryVanadisStore()
			if tc.storeErr != nil {
				s.appendChunksErr = tc.storeErr
			}
			if tc.seedBuild != nil {
				s.builds[tc.seedBuild.ID] = *tc.seedBuild
			}
			if len(tc.seedChunks) > 0 {
				s.chunks[tc.seedBuild.ID] = tc.seedChunks
			}
			got, err := s.AppendChunks(ctx, tc.buildID, tc.inputs)
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
			s := newMemoryVanadisStore()
			if tc.storeErr != nil {
				s.removeChunkErr = tc.storeErr
			}
			if tc.seedBuild != nil {
				s.builds[tc.seedBuild.ID] = *tc.seedBuild
			}
			if len(tc.seedChunks) > 0 {
				s.chunks[tc.seedBuild.ID] = tc.seedChunks
			}
			removed, remaining, err := s.RemoveChunk(ctx, tc.buildID, tc.chunkRef)
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

func TestProcessStalledChunkCheck(t *testing.T) {
	t.Parallel()
	if NewVanadisServer(Config{}, nil, nil, nil).ProcessStalledChunkCheck(context.Background(), "c-stall") == nil {
		t.Fatal("expected error when store is nil")
	}
	tests := []struct {
		name                         string
		claimID, status, buildStatus string
		prNumber, priorNudges        int
		dispatchID                   string
		dispatchCode                 int
		wantNotices                  []string
		wantSessions                 []string
		wantChunkStatus              string
		wantBuildStatus              string
		wantWakeTails                []string
	}{
		{name: "first stall re-dispatches with notice", claimID: "c-stall", status: "running", buildStatus: "running", dispatchCode: http.StatusAccepted, wantNotices: []string{wantStallNotice}, wantSessions: []string{"vanadis-c-stall"}, wantChunkStatus: "running", wantBuildStatus: "running"},
		{name: "second stall fails chunk and wakes thread", claimID: "c-stall", status: "running", buildStatus: "running", priorNudges: 1, dispatchCode: http.StatusAccepted, wantChunkStatus: "failed", wantBuildStatus: "failed", wantWakeTails: []string{"Chunk 1/1 (Stalled Chunk) failed: chunk stalled twice: its turn ended without submitting a PR or calling fail_chunk, both on the original dispatch and on the stall re-dispatch"}},
		{name: "re-dispatch failure fails chunk", claimID: "c-stall", status: "running", buildStatus: "running", dispatchCode: http.StatusInternalServerError, wantNotices: []string{wantStallNotice}, wantSessions: []string{"vanadis-c-stall"}, wantChunkStatus: "failed", wantBuildStatus: "failed", wantWakeTails: []string{"Chunk 1/1 (Stalled Chunk) failed: chunk stalled (turn ended without submitting a PR or calling fail_chunk) and the stall re-dispatch failed: /dispatch returned status 500:"}},
		{name: "linked pr is left alone", claimID: "c-stall", status: "pr_opened", buildStatus: "running", prNumber: 9, dispatchCode: http.StatusAccepted, wantChunkStatus: "pr_opened", wantBuildStatus: "running"},
		{name: "missing chunk is skipped", claimID: "c-gone", status: "running", buildStatus: "running", dispatchCode: http.StatusAccepted, wantChunkStatus: "running", wantBuildStatus: "running"},
		{name: "duplicate delivery with mismatched dispatch id is noop", claimID: "c-stall", status: "running", buildStatus: "running", priorNudges: 1, dispatchID: "vanadis-c-stall-turn-0", dispatchCode: http.StatusAccepted, wantChunkStatus: "running", wantBuildStatus: "running"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var mu sync.Mutex
			var notices, sessions, wakeTails, targetIDs, chunkIDs []string
			var dispatchReqs []BrainDispatchRequest
			brain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				switch r.URL.Path {
				case "/dispatch":
					var req BrainDispatchRequest
					_ = json.NewDecoder(r.Body).Decode(&req)
					dispatchReqs = append(dispatchReqs, req)
					notice, _, _ := strings.Cut(req.Prompt, "[VANADIS BUILD CHUNK EXECUTION]")
					notices, sessions = append(notices, notice), append(sessions, req.SessionID)
					targetIDs, chunkIDs = append(targetIDs, req.TargetID), append(chunkIDs, req.VanadisChunkID)
					w.WriteHeader(tc.dispatchCode)
				case "/prompt":
					var p map[string]string
					_ = json.NewDecoder(r.Body).Decode(&p)
					wakeTails = append(wakeTails, p["prompt"][strings.LastIndex(p["prompt"], "\n")+1:])
					w.WriteHeader(http.StatusAccepted)
				default:
					w.WriteHeader(http.StatusOK)
				}
			}))
			defer brain.Close()
			store := newMemoryVanadisStore()
			c := mkChunk("c-stall", "b-stall", 0, "Stalled Chunk", "do it", "low", tc.status)
			c.PRNumber = tc.prNumber
			_ = store.CreateBuild(context.Background(), mkBuild("b-stall", "123456789012345678", "T", "O", tc.buildStatus), []VanadisChunk{c})
			store.stallNudges = map[string]int{"c-stall": tc.priorNudges}
			srv := NewVanadisServer(Config{BrainURL: brain.URL, SynapseURL: brain.URL}, store, &mockRiverInserter{}, brain.Client())
			if err := srv.ProcessStalledChunkCheck(context.Background(), tc.claimID, tc.dispatchID); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			b, chunks, _ := store.GetBuild(context.Background(), "b-stall")
			mu.Lock()
			defer mu.Unlock()
			if !reflect.DeepEqual(notices, tc.wantNotices) || !reflect.DeepEqual(sessions, tc.wantSessions) {
				t.Fatalf("dispatches notices=%q sessions=%v, want notices=%q sessions=%v", notices, sessions, tc.wantNotices, tc.wantSessions)
			}
			if tc.name == "first stall re-dispatches with notice" {
				if !reflect.DeepEqual(targetIDs, []string{"123456789012345678"}) || !reflect.DeepEqual(chunkIDs, []string{"c-stall"}) {
					t.Fatalf("dispatches targetIDs=%v chunkIDs=%v, want [123456789012345678], [c-stall]", targetIDs, chunkIDs)
				}
				if len(dispatchReqs) != 1 || dispatchReqs[0].ID != "vanadis-c-stall-turn-1" || !dispatchReqs[0].SkipIfActive {
					t.Fatalf("dispatchReqs = %+v, want ID 'vanadis-c-stall-turn-1' and SkipIfActive true", dispatchReqs)
				}
			}
			if !reflect.DeepEqual(wakeTails, tc.wantWakeTails) {
				t.Fatalf("wake tails = %q, want %q", wakeTails, tc.wantWakeTails)
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
	server := NewVanadisServer(Config{BrainURL: brain.URL, SynapseURL: brain.URL}, store, &mockRiverInserter{}, brain.Client())
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
