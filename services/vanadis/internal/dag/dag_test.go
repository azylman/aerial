package dag

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/services/vanadis/internal/model"
)

type dagTestCase struct {
	name       string
	chunks     []model.VanadisChunk
	wantErr    bool
	wantSubstr string
	wantDeps   [][]string
}

func runDAGTestCases(t *testing.T, tests []dagTestCase) {
	t.Helper()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := NormalizeAndValidateBuildDAG(tc.chunks)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tc.wantSubstr)
				}
				if tc.wantSubstr != "" && !strings.Contains(err.Error(), tc.wantSubstr) {
					t.Fatalf("expected error containing %q, got %v", tc.wantSubstr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			for i, want := range tc.wantDeps {
				if len(want) == 0 && len(got[i].DependsOn) == 0 {
					continue
				}
				if !slices.Equal(got[i].DependsOn, want) {
					t.Errorf("chunk[%d].DependsOn = %v, want %v", i, got[i].DependsOn, want)
				}
			}
		})
	}
}

func TestNormalizeAndValidateBuildDAG_Dependencies(t *testing.T) {
	t.Parallel()
	tests := []dagTestCase{
		{
			name: "forward_id",
			chunks: []model.VanadisChunk{
				{ID: "pending", Mode: "action", Status: "pending", DependsOn: []string{"repair"}},
				{ID: "repair", Mode: "action", DependsOn: []string{}},
			},
			wantDeps: [][]string{{"repair"}, {}},
		},
		{
			name: "forward_index",
			chunks: []model.VanadisChunk{
				{ID: "pending", Mode: "action", Status: "pending", DependsOn: []string{"1"}},
				{ID: "repair", Mode: "action", DependsOn: []string{}},
			},
			wantDeps: [][]string{{"repair"}, {}},
		},
		{
			name:   "empty_chunks",
			chunks: nil,
		},
		{
			name: "single_chunk_missing_depends_on_rejected",
			chunks: []model.VanadisChunk{
				{ID: "c1", Mode: "pr", Repo: "aerial"},
			},
			wantErr:    true,
			wantSubstr: "must explicitly specify depends_on",
		},
		{
			name: "empty_chunk_id_rejected",
			chunks: []model.VanadisChunk{
				{ID: "", Mode: "pr", Repo: "aerial"},
			},
			wantErr:    true,
			wantSubstr: "id cannot be empty",
		},
		{
			name: "duplicate_chunk_id_rejected",
			chunks: []model.VanadisChunk{
				{ID: "c1", Mode: "pr", Repo: "aerial"},
				{ID: "c1", Mode: "pr", Repo: "aerial"},
			},
			wantErr:    true,
			wantSubstr: "duplicate chunk id",
		},
		{
			name: "empty_dependency_reference_rejected",
			chunks: []model.VanadisChunk{
				{ID: "c1", Mode: "pr", Repo: "aerial", DependsOn: []string{"   "}},
			},
			wantErr:    true,
			wantSubstr: "empty dependency reference",
		},
		{
			name: "duplicate_dependency_deduplicated",
			chunks: []model.VanadisChunk{
				{ID: "c1", Mode: "pr", Repo: "aerial", DependsOn: []string{}},
				{ID: "c2", Mode: "pr", Repo: "aerial", DependsOn: []string{"c1", "c1"}},
			},
			wantDeps: [][]string{nil, {"c1"}},
		},
		{
			name: "multi_chunk_missing_depends_on_rejected",
			chunks: []model.VanadisChunk{
				{ID: "c1", Mode: "pr", Repo: "aerial"},
				{ID: "c2", Mode: "pr", Repo: "aerial"},
			},
			wantErr:    true,
			wantSubstr: "must explicitly specify depends_on",
		},
		{
			name: "index_string_normalization",
			chunks: []model.VanadisChunk{
				{ID: "c1", Mode: "pr", Repo: "aerial", DependsOn: []string{}},
				{ID: "c2", Mode: "pr", Repo: "aerial", DependsOn: []string{"0"}},
			},
			wantDeps: [][]string{nil, {"c1"}},
		},
		{
			name: "self_dependency_rejected",
			chunks: []model.VanadisChunk{
				{ID: "c1", Mode: "pr", Repo: "aerial", DependsOn: []string{"c1"}},
			},
			wantErr:    true,
			wantSubstr: "cannot depend on itself",
		},
		{
			name: "self_dependency_by_index_rejected",
			chunks: []model.VanadisChunk{
				{ID: "c1", Mode: "pr", Repo: "aerial", DependsOn: []string{"0"}},
			},
			wantErr:    true,
			wantSubstr: "cannot depend on itself",
		},
		{
			name: "unknown_dependency_rejected",
			chunks: []model.VanadisChunk{
				{ID: "c1", Mode: "pr", Repo: "aerial", DependsOn: []string{"missing"}},
			},
			wantErr:    true,
			wantSubstr: "references unknown dependency",
		},
		{
			name: "cycle_detected",
			chunks: []model.VanadisChunk{
				{ID: "c1", Mode: "pr", Repo: "aerial", DependsOn: []string{"c2"}},
				{ID: "c2", Mode: "pr", Repo: "aerial", DependsOn: []string{"c1"}},
			},
			wantErr:    true,
			wantSubstr: "cycle detected",
		},
	}
	runDAGTestCases(t, tests)
}

func TestNormalizeAndValidateBuildDAG_DisjointFiles(t *testing.T) {
	t.Parallel()
	tests := []dagTestCase{
		{
			name: "forward_dependency_allows_overlap",
			chunks: []model.VanadisChunk{
				{ID: "pending", Mode: "pr", Repo: "aerial", DependsOn: []string{"repair"}, TargetFiles: []string{"a.go"}},
				{ID: "repair", Mode: "pr", Repo: "aerial", DependsOn: []string{}, TargetFiles: []string{"a.go"}},
			},
			wantDeps: [][]string{{"repair"}, {}},
		},
		{
			name: "mixed_order_transitive_overlap",
			chunks: []model.VanadisChunk{
				{ID: "end", Mode: "pr", Repo: "aerial", DependsOn: []string{"middle"}, TargetFiles: []string{"a.go"}},
				{ID: "start", Mode: "pr", Repo: "aerial", DependsOn: []string{}, TargetFiles: []string{"a.go"}},
				{ID: "middle", Mode: "action", DependsOn: []string{"start"}},
			},
			wantDeps: [][]string{{"middle"}, {}, {"start"}},
		},
		{
			name: "concurrent_same_repo_pr_empty_target_files_rejected",
			chunks: []model.VanadisChunk{
				{ID: "c1", Mode: "pr", Repo: "aerial", DependsOn: []string{}},
				{ID: "c2", Mode: "pr", Repo: "aerial", DependsOn: []string{}},
			},
			wantErr:    true,
			wantSubstr: "must specify target_files",
		},
		{
			name: "concurrent_same_repo_pr_overlapping_files_rejected",
			chunks: []model.VanadisChunk{
				{ID: "c1", Mode: "pr", Repo: "aerial", DependsOn: []string{}, TargetFiles: []string{"pkg/a.go", "pkg/b.go"}},
				{ID: "c2", Mode: "pr", Repo: "aerial", DependsOn: []string{}, TargetFiles: []string{"pkg/b.go", "pkg/c.go"}},
			},
			wantErr:    true,
			wantSubstr: "overlapping target_files",
		},
		{
			name: "concurrent_same_repo_pr_disjoint_files_ok",
			chunks: []model.VanadisChunk{
				{ID: "c1", Mode: "pr", Repo: "aerial", DependsOn: []string{}, TargetFiles: []string{"pkg/a.go"}},
				{ID: "c2", Mode: "pr", Repo: "aerial", DependsOn: []string{}, TargetFiles: []string{"pkg/b.go"}},
			},
			wantDeps: [][]string{nil, nil},
		},
		{
			name: "dependent_same_repo_pr_can_overlap_files",
			chunks: []model.VanadisChunk{
				{ID: "c1", Mode: "pr", Repo: "aerial", DependsOn: []string{}, TargetFiles: []string{"pkg/a.go"}},
				{ID: "c2", Mode: "pr", Repo: "aerial", DependsOn: []string{"c1"}, TargetFiles: []string{"pkg/a.go"}},
			},
			wantDeps: [][]string{nil, {"c1"}},
		},
		{
			name: "concurrent_different_repos_can_overlap_files",
			chunks: []model.VanadisChunk{
				{ID: "c1", Mode: "pr", Repo: "aerial", DependsOn: []string{}, TargetFiles: []string{"config.yaml"}},
				{ID: "c2", Mode: "pr", Repo: "aerial-config", DependsOn: []string{}, TargetFiles: []string{"config.yaml"}},
			},
			wantDeps: [][]string{nil, nil},
		},
		{
			name: "terminal_chunks_skipped_in_disjoint_check",
			chunks: []model.VanadisChunk{
				{ID: "c1", Mode: "pr", Repo: "aerial", Status: "merged", DependsOn: []string{}, TargetFiles: []string{"config.yaml"}},
				{ID: "c2", Mode: "pr", Repo: "aerial", Status: "pending", DependsOn: []string{}, TargetFiles: []string{"config.yaml"}},
				{ID: "c3", Mode: "pr", Repo: "aerial", Status: "merged", DependsOn: []string{}, TargetFiles: []string{"config.yaml"}},
			},
			wantDeps: [][]string{nil, nil, nil},
		},
	}
	runDAGTestCases(t, tests)
}

func TestCheckDisjointFiles_ChunkBEmptyTargetFiles(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		a       model.VanadisChunk
		b       model.VanadisChunk
		wantErr string
	}{
		{
			name:    "chunk b empty target files",
			a:       model.VanadisChunk{ID: "c1", TargetFiles: []string{"a.go"}},
			b:       model.VanadisChunk{ID: "c2", TargetFiles: nil},
			wantErr: `concurrent PR chunk "c2" on repo "repo" must specify target_files`,
		},
		{
			name:    "overlapping files rejected",
			a:       model.VanadisChunk{ID: "c1", TargetFiles: []string{"a.go"}},
			b:       model.VanadisChunk{ID: "c2", TargetFiles: []string{"./a.go"}},
			wantErr: "overlapping target_files",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := checkDisjointFiles(tc.a, tc.b, "repo")
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("checkDisjointFiles() err = %v, want containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("checkDisjointFiles() unexpected err: %v", err)
			}
		})
	}
}

type parseChunkCase struct {
	name        string
	buildID     string
	raw         []model.CreateChunkInput
	wantErr     string
	wantCount   int
	wantFirstID string
	wantRepo    string
	wantEffort  string
	wantDepends []string
	wantMode    string
}

func checkParsedChunks(t *testing.T, got []model.VanadisChunk, tc parseChunkCase, now time.Time) {
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
	if tc.name == "low effort chained chunks" && (len(got) != 2 || !slices.Equal(got[1].DependsOn, []string{"c1"})) {
		t.Errorf("second chunk DependsOn = %v, want [c1]", got[1].DependsOn)
	}
}

func TestParseCreateBuildChunks(t *testing.T) {
	t.Parallel()
	now := time.Unix(1760000000, 0).UTC()
	tests := []parseChunkCase{
		{
			name:    "missing title rejected",
			buildID: "b1",
			raw:     []model.CreateChunkInput{{ID: "c1", Instructions: "i", DependsOn: []string{}}},
			wantErr: "title and instructions are required",
		},
		{
			name:    "missing instructions rejected",
			buildID: "b1",
			raw:     []model.CreateChunkInput{{ID: "c1", Title: "t", DependsOn: []string{}}},
			wantErr: "title and instructions are required",
		},
		{
			name:    "invalid id rejected",
			buildID: "b1",
			raw:     []model.CreateChunkInput{{ID: "bad id!", Title: "t", Instructions: "i", DependsOn: []string{}}},
			wantErr: "must be 1..120 characters",
		},
		{
			name:    "invalid mode rejected",
			buildID: "b1",
			raw:     []model.CreateChunkInput{{ID: "c1", Mode: "bogus", Title: "t", Instructions: "i", DependsOn: []string{}}},
			wantErr: "invalid chunk mode",
		},
		{
			name:    "pr missing repo rejected",
			buildID: "b1",
			raw:     []model.CreateChunkInput{{ID: "c1", Title: "t", Instructions: "i", DependsOn: []string{}}},
			wantErr: "repo is required for pr mode",
		},
		{
			name:    "action with repo rejected",
			buildID: "b1",
			raw:     []model.CreateChunkInput{{ID: "c1", Mode: "action", Repo: "aerial", Title: "t", Instructions: "i", DependsOn: []string{}}},
			wantErr: "must have empty repo",
		},
		{
			name:    "invalid effort rejected",
			buildID: "b1",
			raw:     []model.CreateChunkInput{{ID: "c1", Repo: "aerial", Title: "t", Instructions: "i", Effort: "huge", DependsOn: []string{}}},
			wantErr: "invalid effort",
		},
		{
			name:    "missing depends_on rejected",
			buildID: "b1",
			raw:     []model.CreateChunkInput{{ID: "c1", Repo: "aerial", Title: "t", Instructions: "i"}},
			wantErr: "must explicitly specify depends_on",
		},
		{
			name:    "duplicate ids rejected",
			buildID: "b1",
			raw: []model.CreateChunkInput{
				{ID: "c1", Repo: "aerial", Title: "t", Instructions: "i", DependsOn: []string{}},
				{ID: "c1", Repo: "aerial", Title: "t", Instructions: "i", DependsOn: []string{}},
			},
			wantErr: "duplicate chunk id",
		},
		{
			name:    "cycle rejected",
			buildID: "b1",
			raw: []model.CreateChunkInput{
				{ID: "c1", Repo: "aerial", Title: "t", Instructions: "i", TargetFiles: []string{"a.go"}, DependsOn: []string{"c2"}},
				{ID: "c2", Repo: "aerial", Title: "t", Instructions: "i", TargetFiles: []string{"b.go"}, DependsOn: []string{"c1"}},
			},
			wantErr: "cycle detected",
		},
		{
			name:    "overlapping files rejected",
			buildID: "b1",
			raw: []model.CreateChunkInput{
				{ID: "c1", Repo: "aerial", Title: "t", Instructions: "i", TargetFiles: []string{"a.go"}, DependsOn: []string{}},
				{ID: "c2", Repo: "aerial", Title: "t", Instructions: "i", TargetFiles: []string{"a.go"}, DependsOn: []string{}},
			},
			wantErr: "overlapping target_files",
		},
		{
			name:        "minimal pr defaults",
			buildID:     "b1",
			raw:         []model.CreateChunkInput{{Title: "  t  ", Instructions: "  i  ", Repo: "  aerial  ", DependsOn: []string{}}},
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
			raw:         []model.CreateChunkInput{{ID: "c1", Repo: "aerial", Title: "t", Instructions: "i", Effort: "high", DependsOn: []string{}}},
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
			raw: []model.CreateChunkInput{
				{ID: "c1", Repo: "aerial", Title: "t", Instructions: "i", Effort: "low", TargetFiles: []string{"a.go"}, DependsOn: []string{}},
				{ID: "c2", Repo: "aerial", Title: "t", Instructions: "i", TargetFiles: []string{"a.go"}, DependsOn: []string{"0"}},
			},
			wantCount:   2,
			wantFirstID: "c1",
			wantRepo:    "aerial",
			wantEffort:  "low",
			wantDepends: []string{},
			wantMode:    "pr",
		},
		{
			name:        "action chunk repo forced empty",
			buildID:     "b1",
			raw:         []model.CreateChunkInput{{ID: "c1", Mode: "action", Repo: "   ", Title: "t", Instructions: "i", DependsOn: []string{}}},
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
