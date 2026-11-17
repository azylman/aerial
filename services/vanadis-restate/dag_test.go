package main

import (
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
			got, err := normalizeAndValidateBuildDAG(tc.chunks)
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
			name: "terminal_chunks_skipped_in_disjoint_check",
			chunks: []VanadisChunk{
				{ID: "c1", Mode: "pr", Repo: "aerial", Status: "merged", DependsOn: []string{}, TargetFiles: []string{"config.yaml"}},
				{ID: "c2", Mode: "pr", Repo: "aerial", Status: "pending", DependsOn: []string{}, TargetFiles: []string{"config.yaml"}},
				{ID: "c3", Mode: "pr", Repo: "aerial", Status: "merged", DependsOn: []string{}, TargetFiles: []string{"config.yaml"}},
			},
			wantDeps: [][]string{nil, nil, nil},
		},
	}
	runDAGTestCases(t, tests)
}

func TestValidateUpdateAndAppendChunkDAG(t *testing.T) {
	t.Parallel()

	t.Run("update_chunk_cycle_rejected", func(t *testing.T) {
		t.Parallel()
		existing := []VanadisChunk{
			{ID: "c1", Mode: "action", DependsOn: []string{}},
			{ID: "c2", Mode: "action", DependsOn: []string{"c1"}},
		}
		updated := existing[0]
		updated.DependsOn = []string{"c2"}
		_, err := validateUpdateChunkDAG(existing, 0, updated)
		if err == nil || !strings.Contains(err.Error(), "cycle detected") {
			t.Fatalf("expected cycle error, got %v", err)
		}
	})

	t.Run("update_chunk_overlapping_files_rejected", func(t *testing.T) {
		t.Parallel()
		existing := []VanadisChunk{
			{ID: "c1", Mode: "pr", Repo: "aerial", DependsOn: []string{}, TargetFiles: []string{"a.go"}},
			{ID: "c2", Mode: "pr", Repo: "aerial", DependsOn: []string{}, TargetFiles: []string{"b.go"}},
		}
		updated := existing[1]
		updated.TargetFiles = []string{"a.go"}
		_, err := validateUpdateChunkDAG(existing, 1, updated)
		if err == nil || !strings.Contains(err.Error(), "overlapping target_files") {
			t.Fatalf("expected overlapping target_files error, got %v", err)
		}
	})

	t.Run("append_chunks_cycle_rejected", func(t *testing.T) {
		t.Parallel()
		existing := []VanadisChunk{
			{ID: "c1", Mode: "action", DependsOn: []string{}},
		}
		appended := []VanadisChunk{
			{ID: "c2", Mode: "action", DependsOn: []string{"c3"}},
			{ID: "c3", Mode: "action", DependsOn: []string{"c2"}},
		}
		_, err := validateAppendChunksDAG(existing, appended)
		if err == nil || !strings.Contains(err.Error(), "cycle detected") {
			t.Fatalf("expected cycle error, got %v", err)
		}
	})

	t.Run("append_chunks_valid_sequential_default", func(t *testing.T) {
		t.Parallel()
		existing := []VanadisChunk{
			{ID: "c1", Mode: "action", DependsOn: []string{}},
		}
		appended := []VanadisChunk{
			{ID: "c2", Mode: "action", DependsOn: []string{"c1"}},
			{ID: "c3", Mode: "action", DependsOn: []string{"c2"}},
		}
		norm, err := validateAppendChunksDAG(existing, appended)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(norm) != 2 || len(norm[0].DependsOn) != 1 || norm[0].DependsOn[0] != "c1" {
			t.Fatalf("unexpected normalized appended chunks: %+v", norm)
		}
	})

	t.Run("relink_dependencies_after_removal_and_update", func(t *testing.T) {
		t.Parallel()
		chunks := []VanadisChunk{
			{ID: "c1", Mode: "action", DependsOn: []string{}},
			{ID: "c2", Mode: "action", DependsOn: []string{"c1"}},
			{ID: "c3", Mode: "action", DependsOn: []string{"c2"}},
		}
		// Remove c2
		remaining := []VanadisChunk{chunks[0], chunks[2]}
		relinkDependenciesAfterRemoval(remaining, chunks[1].ID, chunks[1].DependsOn)
		if len(remaining[1].DependsOn) != 1 || remaining[1].DependsOn[0] != "c1" {
			t.Fatalf("expected c3 to depend on c1 after c2 removal, got %+v", remaining[1].DependsOn)
		}
		// Now update c3, DAG validation should succeed
		updatedC3 := remaining[1]
		updatedC3.Title = "Updated C3"
		normDeps, err := validateUpdateChunkDAG(remaining, 1, updatedC3)
		if err != nil {
			t.Fatalf("unexpected DAG validation error after removal: %v", err)
		}
		if len(normDeps) != 1 || normDeps[0] != "c1" {
			t.Fatalf("unexpected normalized deps: %+v", normDeps)
		}
	})
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
			got := isPRDependencyCodeReady(tc.chunk)
			if got != tc.want {
				t.Errorf("isPRDependencyCodeReady(%+v) = %v, want %v", tc.chunk, got, tc.want)
			}
		})
	}
}

func TestMarshalJSONBOrEmpty(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		val  any
		want string
	}{
		{name: "nil_value", val: nil, want: "{}"},
		{name: "unmarshalable_channel", val: make(chan int), want: "{}"},
		{name: "empty_map", val: map[string]string{}, want: "{}"},
		{name: "populated_map", val: map[string]string{"foo": "bar"}, want: `{"foo":"bar"}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := string(marshalJSONBOrEmpty(tc.val))
			if got != tc.want {
				t.Errorf("marshalJSONBOrEmpty() = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestRelinkDependenciesAfterRemoval_SelfDep(t *testing.T) {
	t.Parallel()

	// c1 removed, which depended on c2. c2 remaining shouldn't add c2 to its own DependsOn.
	remaining := []VanadisChunk{
		{ID: "c2", DependsOn: []string{"c1"}},
	}
	relinkDependenciesAfterRemoval(remaining, "c1", []string{"c2"})
	if len(remaining[0].DependsOn) != 0 {
		t.Fatalf("expected empty depends_on preventing self dependency, got %v", remaining[0].DependsOn)
	}
}

func TestValidateUpdateChunkInput_Validation(t *testing.T) {
	t.Parallel()

	strPtr := func(s string) *string { return &s }

	// validateUpdateChunkInput tests
	_, err := validateUpdateChunkInput("", "c1", UpdateChunkRequest{})
	if err == nil || !strings.Contains(err.Error(), "build id and chunk ref are required") {
		t.Fatalf("expected error on empty buildID, got %v", err)
	}
	_, err = validateUpdateChunkInput("b1", "", UpdateChunkRequest{})
	if err == nil || !strings.Contains(err.Error(), "build id and chunk ref are required") {
		t.Fatalf("expected error on empty chunkRef, got %v", err)
	}
	_, err = validateUpdateChunkInput("b1", "c1", UpdateChunkRequest{})
	if err == nil || !strings.Contains(err.Error(), "at least one updated field are required") {
		t.Fatalf("expected error on empty fields, got %v", err)
	}
	_, err = validateUpdateChunkInput("b1", "c1", UpdateChunkRequest{Mode: strPtr("  ")})
	if err == nil || !strings.Contains(err.Error(), "mode cannot be empty string") {
		t.Fatalf("expected error on empty mode string, got %v", err)
	}
	_, err = validateUpdateChunkInput("b1", "c1", UpdateChunkRequest{Mode: strPtr("invalid-mode")})
	if err == nil {
		t.Fatal("expected error on invalid mode")
	}
	_, err = validateUpdateChunkInput("b1", "c1", UpdateChunkRequest{Effort: "huge"})
	if err == nil {
		t.Fatal("expected error on invalid effort")
	}
	effort, err := validateUpdateChunkInput("b1", "c1", UpdateChunkRequest{Effort: "high"})
	if err != nil || effort != "medium" {
		t.Fatalf("expected medium effort, got %q (err: %v)", effort, err)
	}
	effort, err = validateUpdateChunkInput("b1", "c1", UpdateChunkRequest{Title: "New Title"})
	if err != nil || effort != "" {
		t.Fatalf("expected empty effort, got %q (err: %v)", effort, err)
	}
}

func TestApplyChunkUpdates_Validation(t *testing.T) {
	t.Parallel()

	strPtr := func(s string) *string { return &s }

	// applyChunkUpdates tests
	c := VanadisChunk{
		ID:            "c1",
		Mode:          "pr",
		Repo:          "azylman/aerial",
		Title:         "Old Title",
		Instructions:  "Old Instr",
		TargetFiles:   []string{"main.go"},
		TargetSymbols: []string{"OldSym"},
	}

	// PR mode with empty repo error
	cEmptyRepo := c
	cEmptyRepo.Repo = ""
	err := applyChunkUpdates(&cEmptyRepo, UpdateChunkRequest{}, "")
	if err == nil || !strings.Contains(err.Error(), "repo is required for pr mode") {
		t.Fatalf("expected error on pr mode with empty repo, got %v", err)
	}

	// Action mode with repo error
	cActionWithRepo := c
	err = applyChunkUpdates(&cActionWithRepo, UpdateChunkRequest{Mode: strPtr("action"), Repo: "azylman/aerial"}, "")
	if err == nil || !strings.Contains(err.Error(), "action chunk must have empty repo") {
		t.Fatalf("expected error on action chunk with repo, got %v", err)
	}

	// Action mode with target_files error
	cActionWithFiles := c
	err = applyChunkUpdates(&cActionWithFiles, UpdateChunkRequest{Mode: strPtr("action"), TargetFiles: []string{"a.go"}}, "")
	if err == nil || !strings.Contains(err.Error(), "action chunk must not specify target_files") {
		t.Fatalf("expected error on action chunk with target_files, got %v", err)
	}

	// Action mode with target_symbols error
	cActionWithSymbols := c
	err = applyChunkUpdates(&cActionWithSymbols, UpdateChunkRequest{Mode: strPtr("action"), TargetSymbols: []string{"Sym"}}, "")
	if err == nil || !strings.Contains(err.Error(), "action chunk must not specify target_symbols") {
		t.Fatalf("expected error on action chunk with target_symbols, got %v", err)
	}

	// Valid action mode switch
	cValidAction := c
	err = applyChunkUpdates(&cValidAction, UpdateChunkRequest{
		Mode:         strPtr("action"),
		Title:        "New Action Title",
		Instructions: "New Action Instr",
	}, "low")
	if err != nil {
		t.Fatalf("unexpected error on valid action update: %v", err)
	}
	if cValidAction.Mode != "action" || cValidAction.Repo != "" || len(cValidAction.TargetFiles) != 0 || len(cValidAction.TargetSymbols) != 0 || cValidAction.Effort != "low" {
		t.Fatalf("unexpected updated action chunk: %+v", cValidAction)
	}
}

func TestCheckDisjointFiles_ChunkBEmptyTargetFiles(t *testing.T) {
	t.Parallel()
	a := VanadisChunk{ID: "c1", TargetFiles: []string{"a.go"}}
	b := VanadisChunk{ID: "c2", TargetFiles: nil}
	err := checkDisjointFiles(a, b, "repo")
	if err == nil || !strings.Contains(err.Error(), "concurrent PR chunk \"c2\" on repo \"repo\" must specify target_files") {
		t.Fatalf("expected error on chunk b empty target files, got %v", err)
	}
}

func TestRelinkDependenciesAfterRemoval_DuplicateDep(t *testing.T) {
	t.Parallel()
	remaining := []VanadisChunk{
		{ID: "c3", DependsOn: []string{"c2", "c1", "c1"}},
	}
	relinkDependenciesAfterRemoval(remaining, "c2", []string{"c0"})
	if !slices.Equal(remaining[0].DependsOn, []string{"c0", "c1"}) {
		t.Fatalf("unexpected DependsOn after relink: %v", remaining[0].DependsOn)
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
			err := validateCreateBuildInputs(tc.buildID, tc.threadID, tc.objective, tc.chunks)
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
			err := checkChunkDependenciesReady(tc.chunk, chunks)
			if (err != nil) != tc.wantErr {
				t.Errorf("checkChunkDependenciesReady(%s) err = %v, wantErr = %v", tc.name, err, tc.wantErr)
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
	_, err := normalizeAndValidateBuildDAG(chunks)
	if err != nil {
		t.Errorf("expected valid DAG when concurrent chunk is terminal, got %v", err)
	}
}
