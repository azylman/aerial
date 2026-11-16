package main

import (
	"context"
	"slices"
	"testing"
	"time"
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
		{name: "preserves repo for action chunk with attached pr", build: widgetBuild("build-action-pr", "running", "msg-action-pr", "Test Action PR Widget", time.Time{}), chunks: []VanadisChunk{{ID: "c-action-pr", ChunkIndex: 0, Mode: "action", Repo: "azylman/aerial", PRNumber: 905, Status: "open"}}, req: SyncChunkPRWidgetRequest{BuildID: "build-action-pr"}, wantStatus: "running", wantChunkRepo: "azylman/aerial"},
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
		DependsOn:  []string{},
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
		DependsOn:  []string{"chunk-1"},
		CreatedAt:  time.Now().UTC(),
		UpdatedAt:  time.Now().UTC(),
	}
	_ = s.CreateBuild(context.Background(), b, []VanadisChunk{c1, c2})
	return s, buildID, []string{"chunk-1", "chunk-2"}
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
