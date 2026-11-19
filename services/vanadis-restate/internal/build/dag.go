package build

import (
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

// normalizeAndValidateBuildDAG validates the build DAG: explicit depends_on arrays, Kahn's cycle detection, and concurrent same-repo disjoint target_files.
func normalizeAndValidateBuildDAG(chunks []model.VanadisChunk) ([]model.VanadisChunk, error) {
	if len(chunks) == 0 {
		return chunks, nil
	}
	out := make([]model.VanadisChunk, len(chunks))
	copy(out, chunks)

	idMap := make(map[string]int, len(out))
	for i, c := range out {
		cid := strings.TrimSpace(c.ID)
		if cid == "" {
			return nil, fmt.Errorf("%w: chunk[%d] id cannot be empty", store.ErrInvalidBuildInput, i)
		}
		if _, exists := idMap[cid]; exists {
			return nil, fmt.Errorf("%w: duplicate chunk id %q", store.ErrInvalidBuildInput, cid)
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

func resolveChunkDependencies(out []model.VanadisChunk, idMap map[string]int) error {
	for i := range out {
		if out[i].DependsOn == nil {
			return fmt.Errorf("%w: chunk %q must explicitly specify depends_on", store.ErrInvalidBuildInput, out[i].ID)
		}
		normalizedDeps := make([]string, 0, len(out[i].DependsOn))
		seenDeps := make(map[string]bool, len(out[i].DependsOn))
		for _, rawDep := range out[i].DependsOn {
			dep := strings.TrimSpace(rawDep)
			if dep == "" {
				return fmt.Errorf("%w: chunk %q has empty dependency reference", store.ErrInvalidBuildInput, out[i].ID)
			}
			var targetID string
			if idx, ok := idMap[dep]; ok {
				targetID = out[idx].ID
			} else if idx, err := strconv.Atoi(dep); err == nil && idx >= 0 && idx < len(out) {
				targetID = out[idx].ID
			} else {
				return fmt.Errorf("%w: chunk %q references unknown dependency %q", store.ErrInvalidBuildInput, out[i].ID, dep)
			}
			if targetID == out[i].ID {
				return fmt.Errorf("%w: chunk %q cannot depend on itself", store.ErrInvalidBuildInput, out[i].ID)
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

func checkKahnDAGCycles(out []model.VanadisChunk, idMap map[string]int) error {
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
		return fmt.Errorf("%w: cycle detected in chunk dependencies", store.ErrInvalidBuildInput)
	}
	return nil
}

func buildTransitiveClosure(out []model.VanadisChunk, idMap map[string]int) [][]bool {
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

func validateConcurrentDisjointPRChunks(out []model.VanadisChunk, reach [][]bool) error {
	for i, chunkI := range out {
		if isTerminalChunkStatus(chunkI.Status) {
			continue
		}
		modeI, err := store.NormalizeChunkMode(chunkI.Mode)
		if err != nil || modeI != "pr" {
			continue
		}
		repoI := store.NormalizeRepo(chunkI.Repo)
		for j := i + 1; j < len(out); j++ {
			chunkJ := out[j]
			if isTerminalChunkStatus(chunkJ.Status) {
				continue
			}
			modeJ, err := store.NormalizeChunkMode(chunkJ.Mode)
			if err != nil || modeJ != "pr" {
				continue
			}
			repoJ := store.NormalizeRepo(chunkJ.Repo)
			if !strings.EqualFold(repoI, repoJ) {
				continue
			}
			if (i < len(reach) && j < len(reach[i]) && reach[i][j]) || (j < len(reach) && i < len(reach[j]) && reach[j][i]) {
				continue
			}
			if err := checkDisjointFiles(chunkI, chunkJ, repoI); err != nil {
				return err
			}
		}
	}
	return nil
}

func checkDisjointFiles(a, b model.VanadisChunk, repo string) error {
	if len(a.TargetFiles) == 0 {
		return fmt.Errorf("%w: concurrent PR chunk %q on repo %q must specify target_files", store.ErrInvalidBuildInput, a.ID, repo)
	}
	if len(b.TargetFiles) == 0 {
		return fmt.Errorf("%w: concurrent PR chunk %q on repo %q must specify target_files", store.ErrInvalidBuildInput, b.ID, repo)
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
			return fmt.Errorf("%w: concurrent PR chunks %q and %q on repo %q have overlapping target_files: %s", store.ErrInvalidBuildInput, a.ID, b.ID, repo, normF)
		}
	}
	return nil
}

func validateCreateBuildInputs(buildID, threadID, objective string, chunks []model.VanadisChunk) error {
	if buildID == "" || threadID == "" || objective == "" {
		return errors.New("build id, thread_id, and objective cannot be empty")
	}
	for i, c := range chunks {
		if strings.TrimSpace(c.ID) == "" || strings.TrimSpace(c.Title) == "" || strings.TrimSpace(c.Instructions) == "" {
			return fmt.Errorf("chunk[%d] id, title, and instructions cannot be empty", i)
		}
		if !store.IsValidVanadisChunkID(strings.TrimSpace(c.ID)) {
			return fmt.Errorf("chunks[%d].id must be 1..120 characters of [a-zA-Z0-9_-]", i)
		}
		if cb := strings.TrimSpace(c.BuildID); cb != "" && cb != buildID {
			return fmt.Errorf("chunk[%d] build_id %q does not match build id %q", i, cb, buildID)
		}
		if err := store.ValidateChunkModeAndRepo(i, c.Mode, c.Repo, c.TargetFiles, c.TargetSymbols); err != nil {
			return err
		}
		if _, err := store.NormalizeChunkEffort(c.Effort); err != nil {
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
	case statusMerged, statusCompleted, statusFailed, statusCancelled:
		return true
	default:
		return false
	}
}

func isPRDependencyCodeReady(other model.VanadisChunk) bool {
	status := strings.ToLower(strings.TrimSpace(other.Status))
	if status == statusCompleted {
		return true
	}
	if strings.ToLower(strings.TrimSpace(other.Mode)) == "pr" {
		switch status {
		case statusMerged, statusDeploying:
			return true
		}
	}
	return false
}

func checkChunkDependenciesReady(chunk model.VanadisChunk, chunks []model.VanadisChunk) error {
	for _, depID := range chunk.DependsOn {
		depReady := false
		for _, other := range chunks {
			if other.ID == depID {
				if strings.ToLower(strings.TrimSpace(chunk.Mode)) == "pr" {
					depReady = isPRDependencyCodeReady(other)
				} else {
					depReady = strings.EqualFold(strings.TrimSpace(other.Status), "completed")
				}
				break
			}
		}
		if !depReady {
			return fmt.Errorf("chunk %s dependency %s is not yet completed", chunk.ID, depID)
		}
	}
	return nil
}
