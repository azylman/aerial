package main

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"
)

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
