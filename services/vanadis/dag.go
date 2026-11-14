package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

// normalizeAndValidateBuildDAG validates the build DAG: sequential default, Kahn's cycle detection, and concurrent same-repo disjoint target_files.
func normalizeAndValidateBuildDAG(chunks []VanadisChunk) ([]VanadisChunk, error) {
	if len(chunks) == 0 {
		return chunks, nil
	}
	out := make([]VanadisChunk, len(chunks))
	copy(out, chunks)

	idMap := make(map[string]int, len(out))
	for i, c := range out {
		cid := strings.TrimSpace(c.ID)
		if cid == "" {
			return nil, fmt.Errorf("%w: chunk[%d] id cannot be empty", ErrInvalidBuildInput, i)
		}
		if _, exists := idMap[cid]; exists {
			return nil, fmt.Errorf("%w: duplicate chunk id %q", ErrInvalidBuildInput, cid)
		}
		idMap[cid] = i
	}

	applySequentialDefaults(out)

	if err := resolveChunkDependencies(out, idMap); err != nil {
		return nil, err
	}

	if err := checkKahnDAGCycles(out, idMap); err != nil {
		return nil, err
	}

	reach := buildTransitiveClosure(out, idMap)
	if err := validateConcurrentDisjointPRChunks(out, reach); err != nil {
		return nil, err
	}

	return out, nil
}

func applySequentialDefaults(out []VanadisChunk) {
	for _, c := range out {
		if c.DependsOn != nil {
			return
		}
	}
	for i := range out {
		if i > 0 {
			out[i].DependsOn = []string{out[i-1].ID}
		} else {
			out[i].DependsOn = []string{}
		}
	}
}

func resolveChunkDependencies(out []VanadisChunk, idMap map[string]int) error {
	for i := range out {
		normalizedDeps := make([]string, 0, len(out[i].DependsOn))
		seenDeps := make(map[string]bool, len(out[i].DependsOn))
		for _, rawDep := range out[i].DependsOn {
			dep := strings.TrimSpace(rawDep)
			if dep == "" {
				return fmt.Errorf("%w: chunk %q has empty dependency reference", ErrInvalidBuildInput, out[i].ID)
			}
			var targetID string
			if idx, ok := idMap[dep]; ok {
				targetID = out[idx].ID
			} else if idx, err := strconv.Atoi(dep); err == nil && idx >= 0 && idx < len(out) {
				targetID = out[idx].ID
			} else {
				return fmt.Errorf("%w: chunk %q references unknown dependency %q", ErrInvalidBuildInput, out[i].ID, dep)
			}
			if targetID == out[i].ID {
				return fmt.Errorf("%w: chunk %q cannot depend on itself", ErrInvalidBuildInput, out[i].ID)
			}
			if !seenDeps[targetID] {
				seenDeps[targetID] = true
				normalizedDeps = append(normalizedDeps, targetID)
			}
		}
		out[i].DependsOn = normalizedDeps
	}
	return nil
}

func checkKahnDAGCycles(out []VanadisChunk, idMap map[string]int) error {
	N := len(out)
	inDegree := make([]int, N)
	adj := make([][]int, N)
	for v := range N {
		inDegree[v] = len(out[v].DependsOn)
		for _, depID := range out[v].DependsOn {
			u := idMap[depID]
			adj[u] = append(adj[u], v)
		}
	}

	queue := make([]int, 0, N)
	for i := range N {
		if inDegree[i] == 0 {
			queue = append(queue, i)
		}
	}

	visitedCount := 0
	for len(queue) > 0 {
		u := queue[0]
		queue = queue[1:]
		visitedCount++
		for _, v := range adj[u] {
			inDegree[v]--
			if inDegree[v] == 0 {
				queue = append(queue, v)
			}
		}
	}
	if visitedCount != N {
		return fmt.Errorf("%w: cycle detected in chunk dependencies", ErrInvalidBuildInput)
	}
	return nil
}

func buildTransitiveClosure(out []VanadisChunk, idMap map[string]int) [][]bool {
	N := len(out)
	reach := make([][]bool, N)
	for i := range N {
		reach[i] = make([]bool, N)
		for _, depID := range out[i].DependsOn {
			u := idMap[depID]
			reach[u][i] = true
		}
	}
	for k := range N {
		for i := range N {
			if reach[i][k] {
				for j := range N {
					if reach[k][j] {
						reach[i][j] = true
					}
				}
			}
		}
	}
	return reach
}

func validateConcurrentDisjointPRChunks(out []VanadisChunk, reach [][]bool) error {
	N := len(out)
	for i := range N {
		modeI, err := normalizeChunkMode(out[i].Mode)
		if err != nil || modeI != "pr" {
			continue
		}
		repoI := normalizeRepo(out[i].Repo)
		for j := i + 1; j < N; j++ {
			modeJ, err := normalizeChunkMode(out[j].Mode)
			if err != nil || modeJ != "pr" {
				continue
			}
			repoJ := normalizeRepo(out[j].Repo)
			if !strings.EqualFold(repoI, repoJ) {
				continue
			}
			if reach[i][j] || reach[j][i] {
				continue
			}
			if err := checkDisjointFiles(out[i], out[j], repoI); err != nil {
				return err
			}
		}
	}
	return nil
}

func checkDisjointFiles(a, b VanadisChunk, repo string) error {
	if len(a.TargetFiles) == 0 {
		return fmt.Errorf("%w: concurrent PR chunk %q on repo %q must specify target_files", ErrInvalidBuildInput, a.ID, repo)
	}
	if len(b.TargetFiles) == 0 {
		return fmt.Errorf("%w: concurrent PR chunk %q on repo %q must specify target_files", ErrInvalidBuildInput, b.ID, repo)
	}
	filesA := make(map[string]bool, len(a.TargetFiles))
	for _, f := range a.TargetFiles {
		normF := filepath.Clean(strings.TrimSpace(f))
		if normF != "" && normF != "." {
			filesA[normF] = true
		}
	}
	for _, f := range b.TargetFiles {
		normF := filepath.Clean(strings.TrimSpace(f))
		if normF != "" && normF != "." && filesA[normF] {
			return fmt.Errorf("%w: concurrent PR chunks %q and %q on repo %q have overlapping target_files: %s", ErrInvalidBuildInput, a.ID, b.ID, repo, normF)
		}
	}
	return nil
}

func validateCreateBuildInputs(buildID, threadID, objective string, chunks []VanadisChunk) error {
	if buildID == "" || threadID == "" || objective == "" {
		return errors.New("build id, thread_id, and objective cannot be empty")
	}
	for i, c := range chunks {
		if strings.TrimSpace(c.ID) == "" || strings.TrimSpace(c.Title) == "" || strings.TrimSpace(c.Instructions) == "" {
			return fmt.Errorf("chunk[%d] id, title, and instructions cannot be empty", i)
		}
		if !isValidVanadisChunkID(strings.TrimSpace(c.ID)) {
			return fmt.Errorf("chunks[%d].id must be 1..120 characters of [a-zA-Z0-9_-]", i)
		}
		if cb := strings.TrimSpace(c.BuildID); cb != "" && cb != buildID {
			return fmt.Errorf("chunk[%d] build_id %q does not match build id %q", i, cb, buildID)
		}
		if err := validateChunkModeAndRepo(i, c.Mode, c.Repo, c.TargetFiles, c.TargetSymbols); err != nil {
			return err
		}
		if _, err := normalizeChunkEffort(c.Effort); err != nil {
			return fmt.Errorf("chunk[%d] %w", i, err)
		}
	}
	if _, err := normalizeAndValidateBuildDAG(chunks); err != nil {
		return err
	}
	return nil
}

func isTerminalChunkStatus(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "merged", "completed", "failed", "cancelled":
		return true
	default:
		return false
	}
}

func marshalJSONBOrEmpty(v any) []byte {
	if v == nil {
		return []byte("{}")
	}
	b, err := json.Marshal(v)
	if err != nil || len(b) == 0 || string(b) == "null" {
		return []byte("{}")
	}
	return b
}

// relinkDependenciesAfterRemoval rewrites dependencies in remaining after removedID is deleted,
// removing removedID and replacing it with removedDeps.
func relinkDependenciesAfterRemoval(remaining []VanadisChunk, removedID string, removedDeps []string) {
	for i := range remaining {
		if !slices.Contains(remaining[i].DependsOn, removedID) {
			continue
		}
		newDeps := make([]string, 0, len(remaining[i].DependsOn)+len(removedDeps))
		seen := make(map[string]bool)
		for _, dep := range remaining[i].DependsOn {
			if dep == removedID {
				for _, rd := range removedDeps {
					if rd != remaining[i].ID && !seen[rd] {
						seen[rd] = true
						newDeps = append(newDeps, rd)
					}
				}
			} else if !seen[dep] {
				seen[dep] = true
				newDeps = append(newDeps, dep)
			}
		}
		remaining[i].DependsOn = newDeps
	}
}

// validateUpdateChunkDAG substitutes updated into existing, validates the entire DAG, and returns the normalized depends_on.
func validateUpdateChunkDAG(existing []VanadisChunk, updatedIdx int, updated VanadisChunk) ([]string, error) {
	candidateChunks := make([]VanadisChunk, len(existing))
	copy(candidateChunks, existing)
	candidateChunks[updatedIdx] = updated

	validated, err := normalizeAndValidateBuildDAG(candidateChunks)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidBuildInput, err)
	}
	return validated[updatedIdx].DependsOn, nil
}

// validateAppendChunksDAG combines existing chunks with appended chunks, validates the DAG, and returns the appended chunks with normalized depends_on.
func validateAppendChunksDAG(existing []VanadisChunk, appended []VanadisChunk) ([]VanadisChunk, error) {
	allChunks := make([]VanadisChunk, 0, len(existing)+len(appended))
	allChunks = append(allChunks, existing...)
	allChunks = append(allChunks, appended...)

	validatedAll, err := normalizeAndValidateBuildDAG(allChunks)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidBuildInput, err)
	}
	out := make([]VanadisChunk, len(appended))
	copy(out, appended)
	for i := range out {
		out[i].DependsOn = validatedAll[len(existing)+i].DependsOn
	}
	return out, nil
}
