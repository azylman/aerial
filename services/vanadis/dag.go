package main

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
)

// normalizeAndValidateBuildDAG validates the build DAG: explicit depends_on, Kahn's cycle detection, and concurrent same-repo disjoint target_files.
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

func resolveChunkDependencies(out []VanadisChunk, idMap map[string]int) error {
	for i := range out {
		if out[i].DependsOn == nil {
			return fmt.Errorf("%w: chunk %q must explicitly specify depends_on", ErrInvalidBuildInput, out[i].ID)
		}
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
	}
	for i := range N {
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
		if isTerminalChunkStatus(out[i].Status) {
			continue
		}
		modeI, err := normalizeChunkMode(out[i].Mode)
		if err != nil || modeI != "pr" {
			continue
		}
		repoI := normalizeRepo(out[i].Repo)
		for j := i + 1; j < N; j++ {
			if isTerminalChunkStatus(out[j].Status) {
				continue
			}
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

func isTerminalChunkStatus(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case statusMerged, statusCompleted, statusFailed, statusCancelled:
		return true
	default:
		return false
	}
}
