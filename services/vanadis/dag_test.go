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
			name: "forward_id",
			chunks: []VanadisChunk{
				{ID: "pending", Mode: "action", Status: "pending", DependsOn: []string{"repair"}},
				{ID: "repair", Mode: "action", DependsOn: []string{}},
			},
			wantDeps: [][]string{{"repair"}, {}},
		},
		{
			name: "forward_index",
			chunks: []VanadisChunk{
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
				{ID: "c1", Mode: "pr", Repo: "aerial"},
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
			name: "forward_dependency_allows_overlap",
			chunks: []VanadisChunk{
				{ID: "pending", Mode: "pr", Repo: "aerial", DependsOn: []string{"repair"}, TargetFiles: []string{"a.go"}},
				{ID: "repair", Mode: "pr", Repo: "aerial", DependsOn: []string{}, TargetFiles: []string{"a.go"}},
			},
			wantDeps: [][]string{{"repair"}, {}},
		},
		{
			name: "mixed_order_transitive_overlap",
			chunks: []VanadisChunk{
				{ID: "end", Mode: "pr", Repo: "aerial", DependsOn: []string{"middle"}, TargetFiles: []string{"a.go"}},
				{ID: "start", Mode: "pr", Repo: "aerial", DependsOn: []string{}, TargetFiles: []string{"a.go"}},
				{ID: "middle", Mode: "action", DependsOn: []string{"start"}},
			},
			wantDeps: [][]string{{"middle"}, {}, {"start"}},
		},
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

func TestCheckDisjointFiles_ChunkBEmptyTargetFiles(t *testing.T) {
	t.Parallel()
	a := VanadisChunk{ID: "c1", TargetFiles: []string{"a.go"}}
	b := VanadisChunk{ID: "c2", TargetFiles: nil}
	err := checkDisjointFiles(a, b, "repo")
	if err == nil || !strings.Contains(err.Error(), "concurrent PR chunk \"c2\" on repo \"repo\" must specify target_files") {
		t.Fatalf("expected error on chunk b empty target files, got %v", err)
	}
}
