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
			name: "single_chunk_default",
			chunks: []VanadisChunk{
				{ID: "c1", Mode: "pr", Repo: "aerial"},
			},
			wantDeps: [][]string{nil},
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
				{ID: "c1", Mode: "pr", Repo: "aerial"},
				{ID: "c2", Mode: "pr", Repo: "aerial", DependsOn: []string{"c1", "c1"}},
			},
			wantDeps: [][]string{nil, {"c1"}},
		},
		{
			name: "sequential_default_applied",
			chunks: []VanadisChunk{
				{ID: "c1", Mode: "pr", Repo: "aerial"},
				{ID: "c2", Mode: "pr", Repo: "aerial"},
			},
			wantDeps: [][]string{nil, {"c1"}},
		},
		{
			name: "index_string_normalization",
			chunks: []VanadisChunk{
				{ID: "c1", Mode: "pr", Repo: "aerial"},
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
				{ID: "c1", Mode: "pr", Repo: "aerial", TargetFiles: []string{"pkg/a.go"}},
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
