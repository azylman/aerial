package buildplan

import (
	"errors"
	"slices"
	"strings"
	"testing"
)

type dagTestCase struct {
	name       string
	chunks     []VanadisChunk
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
			name:   "empty_chunks",
			chunks: nil,
		},
		{
			name: "single_chunk_missing_depends_on_rejected",
			chunks: []VanadisChunk{
				{ID: "c1", Mode: "pr", Repo: "aerial"},
			},
			wantErr:    true,
			wantSubstr: "must explicitly specify depends_on",
		},
		{
			name: "empty_chunk_id_rejected",
			chunks: []VanadisChunk{
				{ID: "", Mode: "pr", Repo: "aerial"},
			},
			wantErr:    true,
			wantSubstr: "id cannot be empty",
		},
		{
			name: "duplicate_chunk_id_rejected",
			chunks: []VanadisChunk{
				{ID: "c1", Mode: "pr", Repo: "aerial"},
				{ID: "c1", Mode: "pr", Repo: "aerial"},
			},
			wantErr:    true,
			wantSubstr: "duplicate chunk id",
		},
		{
			name: "empty_dependency_reference_rejected",
			chunks: []VanadisChunk{
				{ID: "c1", Mode: "pr", Repo: "aerial", DependsOn: []string{"   "}},
			},
			wantErr:    true,
			wantSubstr: "empty dependency reference",
		},
		{
			name: "duplicate_dependency_deduplicated",
			chunks: []VanadisChunk{
				{ID: "c1", Mode: "pr", Repo: "aerial", DependsOn: []string{}},
				{ID: "c2", Mode: "pr", Repo: "aerial", DependsOn: []string{"c1", "c1"}},
			},
			wantDeps: [][]string{nil, {"c1"}},
		},
		{
			name: "multi_chunk_missing_depends_on_rejected",
			chunks: []VanadisChunk{
				{ID: "c1", Mode: "pr", Repo: "aerial", DependsOn: []string{}},
				{ID: "c2", Mode: "pr", Repo: "aerial"},
			},
			wantErr:    true,
			wantSubstr: "must explicitly specify depends_on",
		},
		{
			name: "index_string_normalization",
			chunks: []VanadisChunk{
				{ID: "c1", Mode: "pr", Repo: "aerial", DependsOn: []string{}},
				{ID: "c2", Mode: "pr", Repo: "aerial", DependsOn: []string{"0"}},
			},
			wantDeps: [][]string{nil, {"c1"}},
		},
		{
			name: "self_dependency_rejected",
			chunks: []VanadisChunk{
				{ID: "c1", Mode: "pr", Repo: "aerial", DependsOn: []string{"c1"}},
			},
			wantErr:    true,
			wantSubstr: "cannot depend on itself",
		},
		{
			name: "self_dependency_by_index_rejected",
			chunks: []VanadisChunk{
				{ID: "c1", Mode: "pr", Repo: "aerial", DependsOn: []string{"0"}},
			},
			wantErr:    true,
			wantSubstr: "cannot depend on itself",
		},
		{
			name: "unknown_dependency_rejected",
			chunks: []VanadisChunk{
				{ID: "c1", Mode: "pr", Repo: "aerial", DependsOn: []string{"missing"}},
			},
			wantErr:    true,
			wantSubstr: "references unknown dependency",
		},
		{
			name: "forward_edge",
			chunks: []VanadisChunk{
				{ID: "c1", Mode: "pr", Repo: "aerial", DependsOn: []string{"1"}},
				{ID: "c2", Mode: "pr", Repo: "aerial", DependsOn: []string{}},
			},
			wantDeps: [][]string{{"c2"}, {}},
		},
		{
			name: "forward_transitive_chain",
			chunks: []VanadisChunk{
				{ID: "c1", Mode: "pr", Repo: "aerial", DependsOn: []string{"c3"}},
				{ID: "c2", Mode: "pr", Repo: "aerial", DependsOn: []string{}},
				{ID: "c3", Mode: "pr", Repo: "aerial", DependsOn: []string{"c2"}},
			},
			wantDeps: [][]string{{"c3"}, {}, {"c2"}},
		},
		{
			name: "disconnected_nodes_with_forward_edge",
			chunks: []VanadisChunk{
				{ID: "c1", Mode: "pr", Repo: "aerial", DependsOn: []string{"c3"}, TargetFiles: []string{"a.go"}},
				{ID: "c2", Mode: "pr", Repo: "aerial", DependsOn: []string{}, TargetFiles: []string{"b.go"}},
				{ID: "c3", Mode: "pr", Repo: "aerial", DependsOn: []string{}, TargetFiles: []string{"a.go"}},
			},
			wantDeps: [][]string{{"c3"}, {}, {}},
		},
		{
			name: "cycle_detected",
			chunks: []VanadisChunk{
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
			name: "concurrent_same_repo_pr_empty_target_files_rejected",
			chunks: []VanadisChunk{
				{ID: "c1", Mode: "pr", Repo: "aerial", DependsOn: []string{}},
				{ID: "c2", Mode: "pr", Repo: "aerial", DependsOn: []string{}},
			},
			wantErr:    true,
			wantSubstr: "must specify target_files",
		},
		{
			name: "concurrent_same_repo_pr_overlapping_files_rejected",
			chunks: []VanadisChunk{
				{ID: "c1", Mode: "pr", Repo: "aerial", DependsOn: []string{}, TargetFiles: []string{"pkg/a.go", "pkg/b.go"}},
				{ID: "c2", Mode: "pr", Repo: "aerial", DependsOn: []string{}, TargetFiles: []string{"pkg/b.go", "pkg/c.go"}},
			},
			wantErr:    true,
			wantSubstr: "overlapping target_files",
		},
		{
			name: "concurrent_same_repo_pr_disjoint_files_ok",
			chunks: []VanadisChunk{
				{ID: "c1", Mode: "pr", Repo: "aerial", DependsOn: []string{}, TargetFiles: []string{"pkg/a.go"}},
				{ID: "c2", Mode: "pr", Repo: "aerial", DependsOn: []string{}, TargetFiles: []string{"pkg/b.go"}},
			},
			wantDeps: [][]string{nil, nil},
		},
		{
			name: "dependent_same_repo_pr_can_overlap_files",
			chunks: []VanadisChunk{
				{ID: "c1", Mode: "pr", Repo: "aerial", DependsOn: []string{}, TargetFiles: []string{"pkg/a.go"}},
				{ID: "c2", Mode: "pr", Repo: "aerial", DependsOn: []string{"c1"}, TargetFiles: []string{"pkg/a.go"}},
			},
			wantDeps: [][]string{nil, {"c1"}},
		},
		{
			name: "concurrent_different_repos_can_overlap_files",
			chunks: []VanadisChunk{
				{ID: "c1", Mode: "pr", Repo: "aerial", DependsOn: []string{}, TargetFiles: []string{"config.yaml"}},
				{ID: "c2", Mode: "pr", Repo: "aerial-config", DependsOn: []string{}, TargetFiles: []string{"config.yaml"}},
			},
			wantDeps: [][]string{nil, nil},
		},
		{
			name: "concurrent_mixed_modes_skip_disjoint_check",
			chunks: []VanadisChunk{
				{ID: "c1", Mode: "pr", Repo: "aerial", DependsOn: []string{}, TargetFiles: []string{"pkg/a.go"}},
				{ID: "c2", Mode: "action", Repo: "aerial", DependsOn: []string{}, TargetFiles: []string{"pkg/a.go"}},
			},
			wantDeps: [][]string{nil, nil},
		},
		{
			name: "terminal_chunks_skipped_in_disjoint_check",
			chunks: []VanadisChunk{
				{ID: "c1", Mode: "pr", Repo: "aerial", Status: "merged", DependsOn: []string{}, TargetFiles: []string{"config.yaml"}},
				{ID: "c2", Mode: "pr", Repo: "aerial", Status: "pending", DependsOn: []string{}, TargetFiles: []string{"config.yaml"}},
				{ID: "c3", Mode: "pr", Repo: "aerial", Status: "merged", DependsOn: []string{}, TargetFiles: []string{"config.yaml"}},
			},
			wantDeps: [][]string{nil, nil, nil},
		},
		{
			name: "invalid_first_mode_skips_disjoint_check",
			chunks: []VanadisChunk{
				{ID: "c1", Mode: "bogus", Repo: "aerial", DependsOn: []string{}, TargetFiles: []string{"a.go"}},
				{ID: "c2", Mode: "pr", Repo: "aerial", DependsOn: []string{}, TargetFiles: []string{"a.go"}},
			},
			wantDeps: [][]string{nil, nil},
		},
		{
			name: "invalid_second_mode_skips_disjoint_check",
			chunks: []VanadisChunk{
				{ID: "c1", Mode: "pr", Repo: "aerial", DependsOn: []string{}, TargetFiles: []string{"a.go"}},
				{ID: "c2", Mode: "bogus", Repo: "aerial", DependsOn: []string{}, TargetFiles: []string{"a.go"}},
			},
			wantDeps: [][]string{nil, nil},
		},
		{
			name: "earlier_chunk_depending_on_later_can_overlap_files",
			chunks: []VanadisChunk{
				{ID: "c1", Mode: "pr", Repo: "aerial", DependsOn: []string{"c2"}, TargetFiles: []string{"a.go"}},
				{ID: "c2", Mode: "pr", Repo: "aerial", DependsOn: []string{}, TargetFiles: []string{"a.go"}},
			},
			wantDeps: [][]string{{"c2"}, nil},
		},
		{
			name: "concurrent_second_chunk_missing_target_files_rejected",
			chunks: []VanadisChunk{
				{ID: "c1", Mode: "pr", Repo: "aerial", DependsOn: []string{}, TargetFiles: []string{"a.go"}},
				{ID: "c2", Mode: "pr", Repo: "aerial", DependsOn: []string{}},
			},
			wantErr:    true,
			wantSubstr: `concurrent PR chunk "c2" on repo "aerial" must specify target_files`,
		},
	}
	runDAGTestCases(t, tests)
}

func TestIsPRDependencyCodeReady(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		chunk VanadisChunk
		want  bool
	}{
		{name: "completed status", chunk: VanadisChunk{Status: "completed"}, want: true},
		{name: "pr mode merged", chunk: VanadisChunk{Mode: "pr", Status: "merged"}, want: true},
		{name: "pr mode deploying", chunk: VanadisChunk{Mode: "pr", Status: "deploying"}, want: true},
		{name: "pr mode running", chunk: VanadisChunk{Mode: "pr", Status: "running"}, want: false},
		{name: "action mode running", chunk: VanadisChunk{Mode: "action", Status: "running"}, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := IsPRDependencyCodeReady(tc.chunk)
			if got != tc.want {
				t.Errorf("IsPRDependencyCodeReady(%+v) = %v, want %v", tc.chunk, got, tc.want)
			}
		})
	}
}

func TestCheckDisjointFiles_ChunkBEmptyTargetFiles(t *testing.T) {
	t.Parallel()
	a := VanadisChunk{ID: "c1", TargetFiles: []string{"a.go"}}
	b := VanadisChunk{ID: "c2", TargetFiles: nil}
	err := CheckDisjointFiles(a, b, "repo")
	if err == nil || !strings.Contains(err.Error(), "concurrent PR chunk \"c2\" on repo \"repo\" must specify target_files") {
		t.Fatalf("expected error on chunk b empty target files, got %v", err)
	}
}

func TestValidateCreateBuildInputs(t *testing.T) {
	t.Parallel()
	validChunk := VanadisChunk{
		ID:           "c1",
		BuildID:      "b1",
		Title:        "Chunk 1",
		Instructions: "Do 1",
		Mode:         "pr",
		Repo:         "aerial",
		Effort:       "medium",
		DependsOn:    []string{},
	}

	tests := []struct {
		name       string
		buildID    string
		threadID   string
		objective  string
		chunks     []VanadisChunk
		wantErr    bool
		wantSubstr string
	}{
		{
			name:       "empty build id",
			buildID:    "",
			threadID:   "th1",
			objective:  "obj",
			chunks:     []VanadisChunk{validChunk},
			wantErr:    true,
			wantSubstr: "build id, thread_id, and objective cannot be empty",
		},
		{
			name:       "empty chunk fields",
			buildID:    "b1",
			threadID:   "th1",
			objective:  "obj",
			chunks:     []VanadisChunk{{ID: "", Title: "", Instructions: ""}},
			wantErr:    true,
			wantSubstr: "id, title, and instructions cannot be empty",
		},
		{
			name:       "invalid chunk id characters",
			buildID:    "b1",
			threadID:   "th1",
			objective:  "obj",
			chunks:     []VanadisChunk{{ID: "invalid/id!", Title: "Title", Instructions: "Inst"}},
			wantErr:    true,
			wantSubstr: "must be 1..120 characters",
		},
		{
			name:       "mismatched build id",
			buildID:    "b1",
			threadID:   "th1",
			objective:  "obj",
			chunks:     []VanadisChunk{{ID: "c1", BuildID: "b2", Title: "Title", Instructions: "Inst"}},
			wantErr:    true,
			wantSubstr: "does not match build id",
		},
		{
			name:       "invalid effort",
			buildID:    "b1",
			threadID:   "th1",
			objective:  "obj",
			chunks:     []VanadisChunk{{ID: "c1", Title: "Title", Instructions: "Inst", Mode: "pr", Repo: "aerial", Effort: "gigantic"}},
			wantErr:    true,
			wantSubstr: "invalid chunk effort",
		},
		{
			name:       "action mode with repo rejected",
			buildID:    "b1",
			threadID:   "th1",
			objective:  "obj",
			chunks:     []VanadisChunk{{ID: "c1", Title: "t", Instructions: "i", Mode: "action", Repo: "non-empty-repo"}},
			wantErr:    true,
			wantSubstr: "action chunk must have empty repo",
		},
		{
			name:       "invalid mode rejected",
			buildID:    "b1",
			threadID:   "th1",
			objective:  "obj",
			chunks:     []VanadisChunk{{ID: "c1", Title: "t", Instructions: "i", Mode: "bogus", DependsOn: []string{}}},
			wantErr:    true,
			wantSubstr: "bogus",
		},
		{
			name:       "invalid dag rejected",
			buildID:    "b1",
			threadID:   "th1",
			objective:  "obj",
			chunks:     []VanadisChunk{{ID: "c1", Title: "t", Instructions: "i", Mode: "action", DependsOn: []string{"c2"}}, {ID: "c2", Title: "t", Instructions: "i", Mode: "action", DependsOn: []string{"c1"}}},
			wantErr:    true,
			wantSubstr: "invalid vanadis build input: cycle detected in chunk dependencies",
		},
		{
			name:      "valid build inputs",
			buildID:   "b1",
			threadID:  "th1",
			objective: "obj",
			chunks:    []VanadisChunk{validChunk},
			wantErr:   false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateCreateBuildInputs(tc.buildID, tc.threadID, tc.objective, tc.chunks)
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
		})
	}
}

func TestCheckChunkDependenciesReady_ModeAndStatus(t *testing.T) {
	t.Parallel()
	chunks := []VanadisChunk{
		{ID: "c1", Mode: "pr", Status: "deploying"},
		{ID: "c2", Mode: "action", Status: "completed"},
		{ID: "c3", Mode: "pr", Status: "running"},
	}
	tests := []struct {
		name    string
		chunk   VanadisChunk
		wantErr bool
	}{
		{name: "pr depending on deploying pr", chunk: VanadisChunk{ID: "pr1", Mode: "pr", DependsOn: []string{"c1"}}, wantErr: false},
		{name: "pr depending on running pr", chunk: VanadisChunk{ID: "pr2", Mode: "pr", DependsOn: []string{"c3"}}, wantErr: true},
		{name: "action depending on completed action", chunk: VanadisChunk{ID: "act1", Mode: "action", DependsOn: []string{"c2"}}, wantErr: false},
		{name: "action depending on non-completed chunk", chunk: VanadisChunk{ID: "act2", Mode: "action", DependsOn: []string{"c1"}}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := CheckChunkDependenciesReady(tc.chunk, chunks)
			if (err != nil) != tc.wantErr {
				t.Errorf("CheckChunkDependenciesReady(%s) err = %v, wantErr = %v", tc.name, err, tc.wantErr)
			}
		})
	}
}

func TestNormalizeAndValidateBuildDAG_TerminalSkip(t *testing.T) {
	t.Parallel()

	// Concurrent chunks on same repo where one is terminal (merged/completed) -> disjoint check skipped
	chunks := []VanadisChunk{
		{ID: "c1", Mode: "pr", Repo: "aerial", Status: "completed", DependsOn: []string{}, TargetFiles: []string{"a.go"}},
		{ID: "c2", Mode: "pr", Repo: "aerial", Status: "pending", DependsOn: []string{}, TargetFiles: []string{"a.go"}},
	}
	_, err := NormalizeAndValidateBuildDAG(chunks)
	if err != nil {
		t.Errorf("expected valid DAG when concurrent chunk is terminal, got %v", err)
	}
}

func TestIsTerminalChunkStatus(t *testing.T) {
	t.Parallel()
	tests := []struct {
		status string
		want   bool
	}{
		{status: "pending", want: false},
		{status: "running", want: false},
		{status: "deploying", want: false},
		{status: "", want: false},
		{status: "other", want: false},
		{status: "merged", want: true},
		{status: "completed", want: true},
		{status: "failed", want: true},
		{status: "cancelled", want: true},
		{status: "  FAILED  ", want: true},
	}
	for _, tc := range tests {
		t.Run(tc.status, func(t *testing.T) {
			t.Parallel()
			if got := IsTerminalChunkStatus(tc.status); got != tc.want {
				t.Errorf("IsTerminalChunkStatus(%q) = %v, want %v", tc.status, got, tc.want)
			}
		})
	}
}

func TestBuildTransitiveClosure(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		chunks    []VanadisChunk
		wantReach [][]bool
	}{
		{name: "empty", chunks: nil, wantReach: [][]bool{}},
		{
			name: "chain plus isolated node",
			chunks: []VanadisChunk{
				{ID: "a", DependsOn: []string{"b"}},
				{ID: "b", DependsOn: []string{"c"}},
				{ID: "c", DependsOn: []string{}},
				{ID: "d", DependsOn: []string{}},
			},
			wantReach: [][]bool{
				{false, false, false, false},
				{true, false, false, false},
				{true, true, false, false},
				{false, false, false, false},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			idMap := make(map[string]int, len(tc.chunks))
			for i, c := range tc.chunks {
				idMap[c.ID] = i
			}
			got := BuildTransitiveClosure(tc.chunks, idMap)
			if len(got) != len(tc.wantReach) {
				t.Fatalf("len(reach) = %d, want %d", len(got), len(tc.wantReach))
			}
			for i := range got {
				if !slices.Equal(got[i], tc.wantReach[i]) {
					t.Errorf("reach[%d] = %v, want %v", i, got[i], tc.wantReach[i])
				}
			}
		})
	}
}

func TestValidateUpdateChunkDAG(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		existing   []VanadisChunk
		updatedIdx int
		updated    VanadisChunk
		wantSubstr string
		wantDeps   []string
	}{
		{
			name: "cycle rejected",
			existing: []VanadisChunk{
				{ID: "c1", Mode: "action", DependsOn: []string{}},
				{ID: "c2", Mode: "action", DependsOn: []string{"c1"}},
			},
			updatedIdx: 0,
			updated:    VanadisChunk{ID: "c1", Mode: "action", DependsOn: []string{"c2"}},
			wantSubstr: "invalid vanadis build input: invalid vanadis build input: cycle detected",
		},
		{
			name: "overlapping files rejected",
			existing: []VanadisChunk{
				{ID: "c1", Mode: "pr", Repo: "aerial", DependsOn: []string{}, TargetFiles: []string{"a.go"}},
				{ID: "c2", Mode: "pr", Repo: "aerial", DependsOn: []string{}, TargetFiles: []string{"b.go"}},
			},
			updatedIdx: 1,
			updated:    VanadisChunk{ID: "c2", Mode: "pr", Repo: "aerial", DependsOn: []string{}, TargetFiles: []string{"a.go"}},
			wantSubstr: "overlapping target_files",
		},
		{
			name: "index dependency normalized",
			existing: []VanadisChunk{
				{ID: "c1", Mode: "action", DependsOn: []string{}},
				{ID: "c3", Mode: "action", DependsOn: []string{"c1"}},
			},
			updatedIdx: 1,
			updated:    VanadisChunk{ID: "c3", Title: "Updated C3", Mode: "action", DependsOn: []string{"0", "c1"}},
			wantDeps:   []string{"c1"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := ValidateUpdateChunkDAG(tc.existing, tc.updatedIdx, tc.updated)
			if tc.wantSubstr != "" {
				if !errors.Is(err, ErrInvalidBuildInput) || !strings.Contains(err.Error(), tc.wantSubstr) {
					t.Fatalf("err = %v, want ErrInvalidBuildInput containing %q", err, tc.wantSubstr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !slices.Equal(got, tc.wantDeps) {
				t.Errorf("deps = %v, want %v", got, tc.wantDeps)
			}
		})
	}
}

func TestValidateAppendChunksDAG(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		existing   []VanadisChunk
		appended   []VanadisChunk
		wantSubstr string
		wantDeps   [][]string
	}{
		{
			name:     "cycle rejected",
			existing: []VanadisChunk{{ID: "c1", Mode: "action", DependsOn: []string{}}},
			appended: []VanadisChunk{
				{ID: "c2", Mode: "action", DependsOn: []string{"c3"}},
				{ID: "c3", Mode: "action", DependsOn: []string{"c2"}},
			},
			wantSubstr: "cycle detected",
		},
		{
			name:     "sequential chain normalized",
			existing: []VanadisChunk{{ID: "c1", Mode: "action", DependsOn: []string{}}},
			appended: []VanadisChunk{
				{ID: "c2", Mode: "action", DependsOn: []string{"0"}},
				{ID: "c3", Mode: "action", DependsOn: []string{"c2", "c2"}},
			},
			wantDeps: [][]string{{"c1"}, {"c2"}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := ValidateAppendChunksDAG(tc.existing, tc.appended)
			if tc.wantSubstr != "" {
				if !errors.Is(err, ErrInvalidBuildInput) || !strings.Contains(err.Error(), tc.wantSubstr) {
					t.Fatalf("err = %v, want ErrInvalidBuildInput containing %q", err, tc.wantSubstr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(got) != len(tc.wantDeps) {
				t.Fatalf("len = %d, want %d", len(got), len(tc.wantDeps))
			}
			for i := range got {
				if got[i].ID != tc.appended[i].ID || !slices.Equal(got[i].DependsOn, tc.wantDeps[i]) {
					t.Errorf("appended[%d] = %s %v, want %s %v", i, got[i].ID, got[i].DependsOn, tc.appended[i].ID, tc.wantDeps[i])
				}
			}
		})
	}
}

func TestRelinkDependenciesAfterRemoval(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		remaining   []VanadisChunk
		removedID   string
		removedDeps []string
		wantDeps    [][]string
	}{
		{
			name: "dependent inherits removed deps",
			remaining: []VanadisChunk{
				{ID: "c1", DependsOn: []string{}},
				{ID: "c3", DependsOn: []string{"c2"}},
			},
			removedID:   "c2",
			removedDeps: []string{"c1"},
			wantDeps:    [][]string{{}, {"c1"}},
		},
		{
			name:        "self dependency dropped",
			remaining:   []VanadisChunk{{ID: "c2", DependsOn: []string{"c1"}}},
			removedID:   "c1",
			removedDeps: []string{"c2"},
			wantDeps:    [][]string{{}},
		},
		{
			name:        "duplicates collapsed in order",
			remaining:   []VanadisChunk{{ID: "c3", DependsOn: []string{"c2", "c1", "c1"}}},
			removedID:   "c2",
			removedDeps: []string{"c0"},
			wantDeps:    [][]string{{"c0", "c1"}},
		},
		{
			name:        "unrelated chunk untouched",
			remaining:   []VanadisChunk{{ID: "c3", DependsOn: []string{"c1", "c1"}}},
			removedID:   "c2",
			removedDeps: []string{"c0"},
			wantDeps:    [][]string{{"c1", "c1"}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			RelinkDependenciesAfterRemoval(tc.remaining, tc.removedID, tc.removedDeps)
			for i, want := range tc.wantDeps {
				if !slices.Equal(tc.remaining[i].DependsOn, want) {
					t.Errorf("chunk[%d].DependsOn = %v, want %v", i, tc.remaining[i].DependsOn, want)
				}
			}
		})
	}
}
