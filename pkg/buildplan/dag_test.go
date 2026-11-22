package buildplan

import (
	"errors"
	"slices"
	"strings"
	"testing"
)

// dagChunk builds a chunk for DAG tests; a nil deps leaves DependsOn unset.
func dagChunk(id, mode, repo string, deps []string, files ...string) VanadisChunk {
	return VanadisChunk{ID: id, Mode: mode, Repo: repo, DependsOn: deps, TargetFiles: files}
}

// pr builds a PR chunk; deps nil means depends_on was omitted.
func pr(id, repo string, deps []string, files ...string) VanadisChunk {
	return dagChunk(id, "pr", repo, deps, files...)
}

// action builds a repo-less action chunk with an explicit (possibly empty) depends_on.
func action(id string, deps ...string) VanadisChunk {
	return dagChunk(id, "action", "", dagDeps(deps...))
}

// dagDeps returns an explicit, non-nil depends_on list.
func dagDeps(ids ...string) []string {
	if ids == nil {
		return []string{}
	}
	return ids
}

// dagChunks lists the chunks of one DAG row.
func dagChunks(cs ...VanadisChunk) []VanadisChunk { return cs }

func withStatus(c VanadisChunk, status string) VanadisChunk {
	c.Status = status
	return c
}

type dagTestCase struct {
	name       string
	wantSubstr string
	wantDeps   [][]string
	chunks     []VanadisChunk
}

func runDAGTestCases(t *testing.T, tests []dagTestCase) {
	t.Helper()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := NormalizeAndValidateBuildDAG(tc.chunks)
			if tc.wantSubstr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantSubstr) {
					t.Fatalf("err = %v, want error containing %q", err, tc.wantSubstr)
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
	none := dagDeps
	tests := []dagTestCase{
		{name: "empty_chunks"},
		{"single_chunk_missing_depends_on_rejected", "must explicitly specify depends_on", nil, dagChunks(pr("c1", "aerial", nil))},
		{"empty_chunk_id_rejected", "id cannot be empty", nil, dagChunks(pr("", "aerial", nil))},
		{"duplicate_chunk_id_rejected", "duplicate chunk id", nil, dagChunks(pr("c1", "aerial", nil), pr("c1", "aerial", nil))},
		{"empty_dependency_reference_rejected", "empty dependency reference", nil, dagChunks(pr("c1", "aerial", none("   ")))},
		{
			"duplicate_dependency_deduplicated", "",
			[][]string{nil, {"c1"}},
			dagChunks(pr("c1", "aerial", none()), pr("c2", "aerial", none("c1", "c1"))),
		},
		{
			"multi_chunk_missing_depends_on_rejected", "must explicitly specify depends_on", nil,
			dagChunks(pr("c1", "aerial", none()), pr("c2", "aerial", nil)),
		},
		{"index_string_normalization", "", [][]string{nil, {"c1"}}, dagChunks(pr("c1", "aerial", none()), pr("c2", "aerial", none("0")))},
		{"self_dependency_rejected", "cannot depend on itself", nil, dagChunks(pr("c1", "aerial", none("c1")))},
		{"self_dependency_by_index_rejected", "cannot depend on itself", nil, dagChunks(pr("c1", "aerial", none("0")))},
		{"unknown_dependency_rejected", "references unknown dependency", nil, dagChunks(pr("c1", "aerial", none("missing")))},
		{"forward_edge", "", [][]string{{"c2"}, {}}, dagChunks(pr("c1", "aerial", none("1")), pr("c2", "aerial", none()))},
		{
			"forward_transitive_chain", "",
			[][]string{{"c3"}, {}, {"c2"}},
			dagChunks(pr("c1", "aerial", none("c3")), pr("c2", "aerial", none()), pr("c3", "aerial", none("c2"))),
		},
		{
			"disconnected_nodes_with_forward_edge", "",
			[][]string{{"c3"}, {}, {}},
			dagChunks(pr("c1", "aerial", none("c3"), "a.go"), pr("c2", "aerial", none(), "b.go"), pr("c3", "aerial", none(), "a.go")),
		},
		{"cycle_detected", "cycle detected", nil, dagChunks(pr("c1", "aerial", none("c2")), pr("c2", "aerial", none("c1")))},
	}
	runDAGTestCases(t, tests)
}

func TestNormalizeAndValidateBuildDAG_DisjointFiles(t *testing.T) {
	t.Parallel()
	none := dagDeps
	tests := []dagTestCase{
		{
			"concurrent_same_repo_pr_empty_target_files_rejected", "must specify target_files", nil,
			dagChunks(pr("c1", "aerial", none()), pr("c2", "aerial", none())),
		},
		{
			"concurrent_same_repo_pr_overlapping_files_rejected", "overlapping target_files", nil,
			dagChunks(pr("c1", "aerial", none(), "pkg/a.go", "pkg/b.go"), pr("c2", "aerial", none(), "pkg/b.go", "pkg/c.go")),
		},
		{
			"concurrent_same_repo_pr_disjoint_files_ok", "",
			[][]string{nil, nil},
			dagChunks(pr("c1", "aerial", none(), "pkg/a.go"), pr("c2", "aerial", none(), "pkg/b.go")),
		},
		{
			"dependent_same_repo_pr_can_overlap_files", "",
			[][]string{nil, {"c1"}},
			dagChunks(pr("c1", "aerial", none(), "pkg/a.go"), pr("c2", "aerial", none("c1"), "pkg/a.go")),
		},
		{
			"concurrent_different_repos_can_overlap_files", "",
			[][]string{nil, nil},
			dagChunks(pr("c1", "aerial", none(), "config.yaml"), pr("c2", "aerial-config", none(), "config.yaml")),
		},
		{
			"concurrent_mixed_modes_skip_disjoint_check", "",
			[][]string{nil, nil},
			dagChunks(pr("c1", "aerial", none(), "pkg/a.go"), dagChunk("c2", "action", "aerial", none(), "pkg/a.go")),
		},
		{
			"terminal_chunks_skipped_in_disjoint_check", "",
			[][]string{nil, nil, nil},
			dagChunks(withStatus(pr("c1", "aerial", none(), "config.yaml"), "merged"),
				withStatus(pr("c2", "aerial", none(), "config.yaml"), "pending"), withStatus(pr("c3", "aerial", none(), "config.yaml"), "merged")),
		},
		{
			"invalid_first_mode_skips_disjoint_check", "",
			[][]string{nil, nil},
			dagChunks(dagChunk("c1", "bogus", "aerial", none(), "a.go"), pr("c2", "aerial", none(), "a.go")),
		},
		{
			"invalid_second_mode_skips_disjoint_check", "",
			[][]string{nil, nil},
			dagChunks(pr("c1", "aerial", none(), "a.go"), dagChunk("c2", "bogus", "aerial", none(), "a.go")),
		},
		{
			"earlier_chunk_depending_on_later_can_overlap_files", "",
			[][]string{{"c2"}, nil},
			dagChunks(pr("c1", "aerial", none("c2"), "a.go"), pr("c2", "aerial", none(), "a.go")),
		},
		{
			"concurrent_second_chunk_missing_target_files_rejected", `concurrent PR chunk "c2" on repo "aerial" must specify target_files`, nil,
			dagChunks(pr("c1", "aerial", none(), "a.go"), pr("c2", "aerial", none())),
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

	titled := func(c VanadisChunk) VanadisChunk { c.Title, c.Instructions = "t", "i"; return c }
	tests := []struct {
		name       string
		buildID    string
		chunks     []VanadisChunk
		wantSubstr string
	}{
		{"empty build id", "", dagChunks(validChunk), "build id, thread_id, and objective cannot be empty"},
		{"empty chunk fields", "b1", dagChunks(VanadisChunk{}), "id, title, and instructions cannot be empty"},
		{"invalid chunk id characters", "b1", dagChunks(titled(VanadisChunk{ID: "invalid/id!"})), "must be 1..120 characters"},
		{"mismatched build id", "b1", dagChunks(titled(VanadisChunk{ID: "c1", BuildID: "b2"})), "does not match build id"},
		{"invalid effort", "b1", dagChunks(titled(VanadisChunk{ID: "c1", Mode: "pr", Repo: "aerial", Effort: "gigantic"})), "invalid chunk effort"},
		{
			"action mode with repo rejected", "b1", dagChunks(titled(dagChunk("c1", "action", "non-empty-repo", nil))),
			"action chunk must have empty repo",
		},
		{"invalid mode rejected", "b1", dagChunks(titled(dagChunk("c1", "bogus", "", dagDeps()))), "bogus"},
		{
			"invalid dag rejected", "b1", dagChunks(titled(action("c1", "c2")), titled(action("c2", "c1"))),
			"invalid vanadis build input: cycle detected in chunk dependencies",
		},
		{"valid build inputs", "b1", dagChunks(validChunk), ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateCreateBuildInputs(tc.buildID, "th1", "obj", tc.chunks)
			if tc.wantSubstr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantSubstr) {
					t.Fatalf("err = %v, want error containing %q", err, tc.wantSubstr)
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

func TestIsTerminalBuildStatus(t *testing.T) {
	t.Parallel()
	tests := []struct {
		status string
		want   bool
	}{
		{status: "pending", want: false},
		{status: "running", want: false},
		{status: "queued", want: false},
		{status: "", want: false},
		{status: "other", want: false},
		{status: "completed", want: true},
		{status: "failed", want: true},
		{status: "cancelled", want: true},
		{status: "  COMPLETED  ", want: true},
	}
	for _, tc := range tests {
		t.Run(tc.status, func(t *testing.T) {
			t.Parallel()
			if got := IsTerminalBuildStatus(tc.status); got != tc.want {
				t.Errorf("IsTerminalBuildStatus(%q) = %v, want %v", tc.status, got, tc.want)
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
			name:   "chain plus isolated node",
			chunks: dagChunks(action("a", "b"), action("b", "c"), action("c"), action("d")),
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
			name: "cycle rejected", existing: dagChunks(action("c1"), action("c2", "c1")), updatedIdx: 0, updated: action("c1", "c2"),
			wantSubstr: "invalid vanadis build input: invalid vanadis build input: cycle detected",
		},
		{
			name: "overlapping files rejected", existing: dagChunks(pr("c1", "aerial", dagDeps(), "a.go"), pr("c2", "aerial", dagDeps(), "b.go")),
			updatedIdx: 1, updated: pr("c2", "aerial", dagDeps(), "a.go"), wantSubstr: "overlapping target_files",
		},
		{
			name: "index dependency normalized", existing: dagChunks(action("c1"), action("c3", "c1")), updatedIdx: 1, updated: action("c3", "0", "c1"),
			wantDeps: []string{"c1"},
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
			name: "cycle rejected", existing: dagChunks(action("c1")), appended: dagChunks(action("c2", "c3"), action("c3", "c2")),
			wantSubstr: "cycle detected",
		},
		{
			name: "sequential chain normalized", existing: dagChunks(action("c1")), appended: dagChunks(action("c2", "0"), action("c3", "c2", "c2")),
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
			name: "dependent inherits removed deps", remaining: dagChunks(action("c1"), action("c3", "c2")), removedID: "c2",
			removedDeps: []string{"c1"}, wantDeps: [][]string{{}, {"c1"}},
		},
		{
			name: "self dependency dropped", remaining: dagChunks(action("c2", "c1")), removedID: "c1", removedDeps: []string{"c2"},
			wantDeps: [][]string{{}},
		},
		{
			name: "duplicates collapsed in order", remaining: dagChunks(action("c3", "c2", "c1", "c1")), removedID: "c2", removedDeps: []string{"c0"},
			wantDeps: [][]string{{"c0", "c1"}},
		},
		{
			name: "unrelated chunk untouched", remaining: dagChunks(action("c3", "c1", "c1")), removedID: "c2", removedDeps: []string{"c0"},
			wantDeps: [][]string{{"c1", "c1"}},
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
