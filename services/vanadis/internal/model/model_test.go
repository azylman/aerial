package model

import (
	"strings"
	"testing"
	"time"
)

func mkBuild(id, threadID, title, objective, status string) VanadisBuild {
	return VanadisBuild{ID: id, ThreadID: threadID, Tier: 2, Title: title, Objective: objective, Status: status}
}

func mkChunk(id, buildID string, idx int, title, instr, effort, status string) VanadisChunk {
	return VanadisChunk{ID: id, BuildID: buildID, ChunkIndex: idx, Mode: "pr", Repo: NormalizeRepo("aerial"), Title: title, Instructions: instr, Effort: effort, Status: status, TargetFiles: []string{}, TargetSymbols: []string{}, AcceptanceCriteria: []string{}, DependsOn: []string{}}
}

func TestFirstNonEmpty(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		vals []string
		want string
	}{
		{name: "empty", vals: nil, want: ""},
		{name: "all blank", vals: []string{"", "   "}, want: ""},
		{name: "first wins", vals: []string{"a", "b"}, want: "a"},
		{name: "skips blanks", vals: []string{"", "   ", "a", "b"}, want: "a"},
		{name: "trims whitespace", vals: []string{"  a  "}, want: "a"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := FirstNonEmpty(tc.vals...); got != tc.want {
				t.Errorf("FirstNonEmpty(%q) = %q, want %q", tc.vals, got, tc.want)
			}
		})
	}
}

func TestNonNilSlice(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		input    []string
		wantLen  int
		wantVals []string
	}{
		{name: "nil becomes empty", input: nil, wantLen: 0, wantVals: []string{}},
		{name: "empty stays empty", input: []string{}, wantLen: 0, wantVals: []string{}},
		{name: "values preserved", input: []string{"a", "b"}, wantLen: 2, wantVals: []string{"a", "b"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := NonNilSlice(tc.input)
			if got == nil || len(got) != tc.wantLen {
				t.Fatalf("NonNilSlice(%v) = %v, want len %d non-nil", tc.input, got, tc.wantLen)
			}
			for i, want := range tc.wantVals {
				if got[i] != want {
					t.Errorf("NonNilSlice(%v)[%d] = %q, want %q", tc.input, i, got[i], want)
				}
			}
		})
	}
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

func checkStringResult(t *testing.T, label, input, got, want, wantErr string, err error) {
	t.Helper()
	if wantErr != "" {
		if err == nil || !strings.Contains(err.Error(), wantErr) {
			t.Fatalf("%s(%q) err = %v, want containing %q", label, input, err, wantErr)
		}
		return
	}
	if err != nil || got != want {
		t.Errorf("%s(%q) = %q, %v; want %q", label, input, got, err, want)
	}
}

func TestNormalizeChunkEffort(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		effort  string
		want    string
		wantErr string
	}{
		{name: "empty defaults medium", effort: "", want: "medium"},
		{name: "medium", effort: "medium", want: "medium"},
		{name: "high maps medium", effort: "high", want: "medium"},
		{name: "upper high maps medium", effort: "HIGH", want: "medium"},
		{name: "low", effort: "low", want: "low"},
		{name: "upper low", effort: "LOW", want: "low"},
		{name: "padded low", effort: "  low  ", want: "low"},
		{name: "invalid", effort: "huge", wantErr: "invalid chunk effort"},
		{name: "blank invalid", effort: "   huge   ", wantErr: "invalid chunk effort"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := NormalizeChunkEffort(tc.effort)
			checkStringResult(t, "NormalizeChunkEffort", tc.effort, got, tc.want, tc.wantErr, err)
		})
	}
}

func TestNormalizeChunkMode(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		mode    string
		want    string
		wantErr string
	}{
		{name: "empty defaults pr", mode: "", want: "pr"},
		{name: "pr", mode: "pr", want: "pr"},
		{name: "upper pr", mode: "PR", want: "pr"},
		{name: "padded pr", mode: "  pr  ", want: "pr"},
		{name: "action", mode: "action", want: "action"},
		{name: "upper action", mode: "ACTION", want: "action"},
		{name: "invalid", mode: "bogus", wantErr: "invalid chunk mode"},
		{name: "blank invalid", mode: "  bogus  ", wantErr: "invalid chunk mode"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := NormalizeChunkMode(tc.mode)
			checkStringResult(t, "NormalizeChunkMode", tc.mode, got, tc.want, tc.wantErr, err)
		})
	}
}

func TestValidateChunkModeAndRepo(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		mode    string
		repo    string
		files   []string
		symbols []string
		wantErr string
	}{
		{name: "pr ok", mode: "pr", repo: "aerial"},
		{name: "pr default mode ok", mode: "", repo: "aerial"},
		{name: "pr trims repo", mode: "pr", repo: "  aerial  "},
		{name: "pr missing repo", mode: "pr", repo: "", wantErr: "repo is required for pr mode"},
		{name: "pr blank repo", mode: "pr", repo: "   ", wantErr: "repo is required for pr mode"},
		{name: "invalid mode", mode: "bogus", repo: "aerial", wantErr: "invalid chunk mode"},
		{name: "action ok", mode: "action"},
		{name: "action blank repo ok", mode: "action", repo: "   "},
		{name: "action with repo", mode: "action", repo: "aerial", wantErr: "must have empty repo"},
		{name: "action with files", mode: "action", files: []string{"a.go"}, wantErr: "must not specify target_files"},
		{name: "action with symbols", mode: "action", symbols: []string{"Foo"}, wantErr: "must not specify target_symbols"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateChunkModeAndRepo(0, tc.mode, tc.repo, tc.files, tc.symbols)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("ValidateChunkModeAndRepo(%q, %q) err = %v, want containing %q", tc.mode, tc.repo, err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Errorf("ValidateChunkModeAndRepo(%q, %q) unexpected err: %v", tc.mode, tc.repo, err)
			}
		})
	}
}

func TestIsValidVanadisChunkID(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		id    string
		valid bool
	}{
		{name: "single char", id: "a", valid: true},
		{name: "max length", id: strings.Repeat("a", 120), valid: true},
		{name: "blank", id: "   ", valid: false},
		{name: "space invalid", id: "chunk 1", valid: false},
		{name: "dot invalid", id: "chunk.1", valid: false},
		{name: "slash invalid", id: "chunk/1", valid: false},
		{name: "padded valid", id: "  chunk-1  ", valid: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := IsValidVanadisChunkID(tc.id); got != tc.valid {
				t.Errorf("IsValidVanadisChunkID(%q) = %v, want %v", tc.id, got, tc.valid)
			}
		})
	}
}

func TestGenerateBuildID(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
	}{
		{name: "prefixed unique ids"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			a, b := GenerateBuildID(), GenerateBuildID()
			for _, id := range []string{a, b} {
				if !strings.HasPrefix(id, "build-") || strings.Count(id, "-") != 2 {
					t.Errorf("GenerateBuildID() = %q, want build-<unix>-<hex>", id)
				}
			}
			if a == b {
				t.Errorf("GenerateBuildID() not unique: %q", a)
			}
		})
	}
}

func TestResolveChunkRef(t *testing.T) {
	t.Parallel()
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
			t.Parallel()
			if got := ResolveChunkRef(tc.chunks, tc.chunkRef); got != tc.wantIdx {
				t.Errorf("ResolveChunkRef() = %d, want %d", got, tc.wantIdx)
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
