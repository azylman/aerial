package model

import (
	"slices"
	"strings"
	"testing"
	"time"
)

func mkBuild(id, threadID, title, objective, status string) VanadisBuild {
	return VanadisBuild{ID: id, ThreadID: threadID, Tier: 2, Title: title, Objective: objective, Status: status}
}

func mkChunk(id, buildID string, idx int, title, instr, effort, status string) VanadisChunk {
	return VanadisChunk{ID: id, BuildID: buildID, ChunkIndex: idx, Mode: "pr", Repo: "aerial", Title: title, Instructions: instr, Effort: effort, Status: status, TargetFiles: []string{}, TargetSymbols: []string{}, AcceptanceCriteria: []string{}, DependsOn: []string{}}
}

func TestCapChunkEffort(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		effort string
		want   string
	}{
		{name: "empty defaults medium", effort: "", want: "medium"},
		{name: "low stays low", effort: "low", want: "low"},
		{name: "mixed case low", effort: "LOW", want: "low"},
		{name: "padded low", effort: "  low  ", want: "low"},
		{name: "medium stays medium", effort: "medium", want: "medium"},
		{name: "high maps medium", effort: "high", want: "medium"},
		{name: "unknown maps medium", effort: "huge", want: "medium"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := CapChunkEffort(tc.effort); got != tc.want {
				t.Errorf("CapChunkEffort(%q) = %q, want %q", tc.effort, got, tc.want)
			}
		})
	}
}

func TestClassifyAndSummarizeBuildEntry(t *testing.T) {
	t.Parallel()
	classifyTests := []struct {
		name        string
		buildStatus string
		chunks      []VanadisChunk
		expected    string
	}{
		{"completed status", "completed", nil, "completed"},
		{"merged status", "merged", nil, "completed"},
		{"failed status", "failed", nil, "completed"},
		{"cancelled status", "cancelled", nil, "completed"},
		{"canceled status uppercase", "CANCELED", nil, "completed"},
		{"queued status", "queued", nil, "queued"},
		{"pending status", "pending", nil, "queued"},
		{"running with empty chunks", "running", []VanadisChunk{}, "running"},
		{"running with all pending chunks", "running", []VanadisChunk{mkChunk("c1", "", 0, "", "", "", "pending"), mkChunk("c2", "", 1, "", "", "", "pending")}, "queued"},
		{"running with mixed chunks", "running", []VanadisChunk{mkChunk("c1", "", 0, "", "", "", "pending"), mkChunk("c2", "", 1, "", "", "", "running")}, "running"},
		{"running with completed chunk", "running", []VanadisChunk{mkChunk("c1", "", 0, "", "", "", "completed"), mkChunk("c2", "", 1, "", "", "", "pending")}, "running"},
	}
	for _, tc := range classifyTests {
		t.Run("classify_"+tc.name, func(t *testing.T) {
			t.Parallel()
			if got := ClassifyBuildQueueState(tc.buildStatus, tc.chunks); got != tc.expected {
				t.Errorf("ClassifyBuildQueueState(%q, chunks) = %q, want %q", tc.buildStatus, got, tc.expected)
			}
		})
	}
	b := mkBuild("b-sum", "100", "Title", "Obj", "running")
	b.CreatedAt = time.Unix(1760000000, 0)
	c1 := mkChunk("c-sum-1", "b-sum", 0, "Chunk 1", "inst", "low", "running")
	c1.StartedAt = time.Unix(1760000100, 0)
	c2 := mkChunk("c-sum-2", "b-sum", 1, "Chunk 2", "inst", "high", "pending")
	c2.StartedAt = time.Unix(1760000200, 0)
	c2.CompletedAt = time.Unix(1760000300, 0)
	entry := SummarizeBuildEntry(b, []VanadisChunk{c1, c2})
	if entry.ID != "b-sum" || entry.TotalChunks != 2 || entry.QueueState != "running" || len(entry.Chunks) != 2 || !entry.Chunks[0].StartedAt.Equal(c1.StartedAt) || !entry.Chunks[1].CompletedAt.Equal(c2.CompletedAt) || entry.Chunks[0].Effort != "low" || entry.Chunks[1].Effort != "high" {
		t.Fatalf("unexpected SummarizeBuildEntry result: %+v", entry)
	}
}

func TestSummarizeBuildEntryCounts(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		status        string
		chunkStatuses []string
		wantQueue     string
		wantCompleted int
		wantRunning   int
		wantPending   int
		wantFailed    int
		wantTotal     int
	}{
		{name: "nil chunks", status: "completed", chunkStatuses: nil, wantQueue: "completed", wantTotal: 0},
		{name: "all terminal", status: "completed", chunkStatuses: []string{"completed", "merged", "failed"}, wantQueue: "completed", wantCompleted: 2, wantFailed: 1, wantTotal: 3},
		{name: "running mix", status: "running", chunkStatuses: []string{"coding", "pr_opened", "deploying", "pending", "unknown"}, wantQueue: "running", wantRunning: 3, wantPending: 1, wantTotal: 5},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := mkBuild("b-1", "100", "T", "O", tc.status)
			var chunks []VanadisChunk
			for i, st := range tc.chunkStatuses {
				c := mkChunk("c-1", "b-1", i, "T", "I", "medium", st)
				chunks = append(chunks, c)
			}
			entry := SummarizeBuildEntry(b, chunks)
			if entry.QueueState != tc.wantQueue || entry.CompletedChunks != tc.wantCompleted || entry.RunningChunks != tc.wantRunning || entry.PendingChunks != tc.wantPending || entry.FailedChunks != tc.wantFailed || entry.TotalChunks != tc.wantTotal {
				t.Errorf("SummarizeBuildEntry() = %+v, want queue=%q completed=%d running=%d pending=%d failed=%d total=%d",
					entry, tc.wantQueue, tc.wantCompleted, tc.wantRunning, tc.wantPending, tc.wantFailed, tc.wantTotal)
			}
			if entry.Chunks == nil {
				t.Errorf("SummarizeBuildEntry().Chunks is nil, want non-nil")
			}
		})
	}
}

type parseChunkCase struct {
	name        string
	buildID     string
	raw         []CreateChunkInput
	wantErr     string
	wantCount   int
	wantFirstID string
	wantRepo    string
	wantEffort  string
	wantDepends []string
	wantMode    string
	wantLastDep []string
}

func checkParsedChunks(t *testing.T, got []VanadisChunk, tc parseChunkCase, now time.Time) {
	t.Helper()
	if len(got) != tc.wantCount {
		t.Fatalf("ParseCreateBuildChunks() count = %d, want %d", len(got), tc.wantCount)
	}
	first := got[0]
	if first.ID != tc.wantFirstID || first.Repo != tc.wantRepo || first.Effort != tc.wantEffort || first.Mode != tc.wantMode {
		t.Errorf("first chunk = %+v, want id=%q repo=%q effort=%q mode=%q", first, tc.wantFirstID, tc.wantRepo, tc.wantEffort, tc.wantMode)
	}
	if len(first.DependsOn) != len(tc.wantDepends) || (len(tc.wantDepends) > 0 && !slices.Equal(first.DependsOn, tc.wantDepends)) {
		t.Errorf("first chunk DependsOn = %v, want %v", first.DependsOn, tc.wantDepends)
	}
	if first.Title != strings.TrimSpace(tc.raw[0].Title) || first.Instructions != strings.TrimSpace(tc.raw[0].Instructions) {
		t.Errorf("first chunk title/instructions not trimmed: %+v", first)
	}
	if !first.CreatedAt.Equal(now) || !first.UpdatedAt.Equal(now) || !first.StartedAt.IsZero() {
		t.Errorf("first chunk timestamps = %+v, want created/updated %v zero started", first, now)
	}
	if first.BuildID != tc.buildID || first.ChunkIndex != 0 {
		t.Errorf("first chunk identity = %+v, want build %q index 0", first, tc.buildID)
	}
	if last := got[len(got)-1]; tc.wantLastDep != nil && !slices.Equal(last.DependsOn, tc.wantLastDep) {
		t.Errorf("last chunk DependsOn = %v, want %v", last.DependsOn, tc.wantLastDep)
	}
}

func TestParseCreateBuildChunks(t *testing.T) {
	t.Parallel()
	now := time.Unix(1760000000, 0).UTC()
	tests := []parseChunkCase{
		{
			name:    "missing title rejected",
			buildID: "b1",
			raw:     []CreateChunkInput{{ID: "c1", Instructions: "i", DependsOn: []string{}}},
			wantErr: "title and instructions are required",
		},
		{
			name:    "missing instructions rejected",
			buildID: "b1",
			raw:     []CreateChunkInput{{ID: "c1", Title: "t", DependsOn: []string{}}},
			wantErr: "title and instructions are required",
		},
		{
			name:    "invalid id rejected",
			buildID: "b1",
			raw:     []CreateChunkInput{{ID: "bad id!", Title: "t", Instructions: "i", DependsOn: []string{}}},
			wantErr: "must be 1..120 characters",
		},
		{
			name:    "invalid mode rejected",
			buildID: "b1",
			raw:     []CreateChunkInput{{ID: "c1", Mode: "bogus", Title: "t", Instructions: "i", DependsOn: []string{}}},
			wantErr: "invalid chunk mode",
		},
		{
			name:    "pr missing repo rejected",
			buildID: "b1",
			raw:     []CreateChunkInput{{ID: "c1", Title: "t", Instructions: "i", DependsOn: []string{}}},
			wantErr: "repo is required for pr mode",
		},
		{
			name:    "action with repo rejected",
			buildID: "b1",
			raw:     []CreateChunkInput{{ID: "c1", Mode: "action", Repo: "aerial", Title: "t", Instructions: "i", DependsOn: []string{}}},
			wantErr: "must have empty repo",
		},
		{
			name:    "invalid effort rejected",
			buildID: "b1",
			raw:     []CreateChunkInput{{ID: "c1", Repo: "aerial", Title: "t", Instructions: "i", Effort: "huge", DependsOn: []string{}}},
			wantErr: "invalid effort",
		},
		{
			name:    "missing depends_on rejected",
			buildID: "b1",
			raw:     []CreateChunkInput{{ID: "c1", Repo: "aerial", Title: "t", Instructions: "i"}},
			wantErr: "must explicitly specify depends_on",
		},
		{
			name:    "duplicate ids rejected",
			buildID: "b1",
			raw: []CreateChunkInput{
				{ID: "c1", Repo: "aerial", Title: "t", Instructions: "i", DependsOn: []string{}},
				{ID: "c1", Repo: "aerial", Title: "t", Instructions: "i", DependsOn: []string{}},
			},
			wantErr: "duplicate chunk id",
		},
		{
			name:    "cycle rejected",
			buildID: "b1",
			raw: []CreateChunkInput{
				{ID: "c1", Repo: "aerial", Title: "t", Instructions: "i", TargetFiles: []string{"a.go"}, DependsOn: []string{"c2"}},
				{ID: "c2", Repo: "aerial", Title: "t", Instructions: "i", TargetFiles: []string{"b.go"}, DependsOn: []string{"c1"}},
			},
			wantErr: "cycle detected",
		},
		{
			name:    "overlapping files rejected",
			buildID: "b1",
			raw: []CreateChunkInput{
				{ID: "c1", Repo: "aerial", Title: "t", Instructions: "i", TargetFiles: []string{"a.go"}, DependsOn: []string{}},
				{ID: "c2", Repo: "aerial", Title: "t", Instructions: "i", TargetFiles: []string{"a.go"}, DependsOn: []string{}},
			},
			wantErr: "overlapping target_files",
		},
		{
			name:        "minimal pr defaults",
			buildID:     "b1",
			raw:         []CreateChunkInput{{Title: "  t  ", Instructions: "  i  ", Repo: "  aerial  ", DependsOn: []string{}}},
			wantCount:   1,
			wantFirstID: "b1-chunk-1",
			wantRepo:    "aerial",
			wantEffort:  "medium",
			wantDepends: []string{},
			wantMode:    "pr",
		},
		{
			name:        "custom id high effort index dep",
			buildID:     "b1",
			raw:         []CreateChunkInput{{ID: "c1", Repo: "aerial", Title: "t", Instructions: "i", Effort: "high", DependsOn: []string{}}},
			wantCount:   1,
			wantFirstID: "c1",
			wantRepo:    "aerial",
			wantEffort:  "medium",
			wantDepends: []string{},
			wantMode:    "pr",
		},
		{
			name:    "low effort chained chunks",
			buildID: "b1",
			raw: []CreateChunkInput{
				{ID: "c1", Repo: "aerial", Title: "t", Instructions: "i", Effort: "low", TargetFiles: []string{"a.go"}, DependsOn: []string{}},
				{ID: "c2", Repo: "aerial", Title: "t", Instructions: "i", TargetFiles: []string{"a.go"}, DependsOn: []string{"0"}},
			},
			wantCount:   2,
			wantFirstID: "c1",
			wantRepo:    "aerial",
			wantEffort:  "low",
			wantDepends: []string{},
			wantMode:    "pr",
			wantLastDep: []string{"c1"},
		},
		{
			name:        "action chunk repo forced empty",
			buildID:     "b1",
			raw:         []CreateChunkInput{{ID: "c1", Mode: "action", Repo: "   ", Title: "t", Instructions: "i", DependsOn: []string{}}},
			wantCount:   1,
			wantFirstID: "c1",
			wantRepo:    "",
			wantEffort:  "medium",
			wantDepends: []string{},
			wantMode:    "action",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := ParseCreateBuildChunks(tc.buildID, now, tc.raw)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("ParseCreateBuildChunks() err = %v, want containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseCreateBuildChunks() unexpected err: %v", err)
			}
			checkParsedChunks(t, got, tc, now)
		})
	}
}
