// Package dag holds the pure build-DAG math for Vanadis multi-chunk builds:
// explicit dependency normalization, Kahn cycle detection, transitive
// closure, and concurrent same-repo disjoint target-file validation.
// It has zero Restate dependency so the graph rules stay independently
// testable from the reconciliation engine. This is the single copy of the
// build-DAG rules: internal/store and internal/build both call it.
package dag

import (
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
)

// Status literals mirrored from the build package. They stay package-private
// so the wire values live in exactly one place per package; behavior must
// match internal/build/requests.go and internal/build/build.go.
const (
	dagStatusCompleted = "completed"
	dagStatusFailed    = "failed"
	dagStatusCancelled = "cancelled"
	dagStatusMerged    = "merged"
	dagStatusDeploying = "deploying"
)

// NormalizeAndValidateBuildDAG validates the build DAG: explicit depends_on arrays, Kahn's cycle detection, and concurrent same-repo disjoint target_files.
func NormalizeAndValidateBuildDAG(chunks []model.VanadisChunk) ([]model.VanadisChunk, error) {
	if len(chunks) == 0 {
		return chunks, nil
	}
	out := make([]model.VanadisChunk, len(chunks))
	copy(out, chunks)

	idMap := make(map[string]int, len(out))
	for i, c := range out {
		cid := strings.TrimSpace(c.ID)
		if cid == "" {
			return nil, fmt.Errorf("%w: chunk[%d] id cannot be empty", model.ErrInvalidBuildInput, i)
		}
		if _, exists := idMap[cid]; exists {
			return nil, fmt.Errorf("%w: duplicate chunk id %q", model.ErrInvalidBuildInput, cid)
		}
		idMap[cid] = i
	}

	if err := ResolveChunkDependencies(out, idMap); err != nil {
		return nil, err
	}

	if err := CheckKahnDAGCycles(out, idMap); err != nil {
		return nil, err
	}

	reach := BuildTransitiveClosure(out, idMap)
	if err := ValidateConcurrentDisjointPRChunks(out, reach); err != nil {
		return nil, err
	}

	return out, nil
}

// ResolveChunkDependencies normalizes each chunk's DependsOn entries to
// canonical chunk IDs (accepting index strings) and rejects empty,
// unknown, self, or implicit dependencies.
func ResolveChunkDependencies(out []model.VanadisChunk, idMap map[string]int) error {
	for i := range out {
		if out[i].DependsOn == nil {
			return fmt.Errorf("%w: chunk %q must explicitly specify depends_on", model.ErrInvalidBuildInput, out[i].ID)
		}
		normalizedDeps := make([]string, 0, len(out[i].DependsOn))
		seenDeps := make(map[string]bool, len(out[i].DependsOn))
		for _, rawDep := range out[i].DependsOn {
			dep := strings.TrimSpace(rawDep)
			if dep == "" {
				return fmt.Errorf("%w: chunk %q has empty dependency reference", model.ErrInvalidBuildInput, out[i].ID)
			}
			var targetID string
			if idx, ok := idMap[dep]; ok {
				targetID = out[idx].ID
			} else if idx, err := strconv.Atoi(dep); err == nil && idx >= 0 && idx < len(out) {
				targetID = out[idx].ID
			} else {
				return fmt.Errorf("%w: chunk %q references unknown dependency %q", model.ErrInvalidBuildInput, out[i].ID, dep)
			}
			if targetID == out[i].ID {
				return fmt.Errorf("%w: chunk %q cannot depend on itself", model.ErrInvalidBuildInput, out[i].ID)
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

// CheckKahnDAGCycles rejects dependency graphs that contain a cycle.
func CheckKahnDAGCycles(out []model.VanadisChunk, idMap map[string]int) error {
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
		return fmt.Errorf("%w: cycle detected in chunk dependencies", model.ErrInvalidBuildInput)
	}
	return nil
}

// BuildTransitiveClosure computes reachability over the dependency DAG.
func BuildTransitiveClosure(out []model.VanadisChunk, idMap map[string]int) [][]bool {
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

// ValidateConcurrentDisjointPRChunks requires concurrent same-repo PR chunks
// to declare disjoint target_files.
func ValidateConcurrentDisjointPRChunks(out []model.VanadisChunk, reach [][]bool) error {
	for i, chunkI := range out {
		if IsTerminalChunkStatus(chunkI.Status) {
			continue
		}
		modeI, err := model.NormalizeChunkMode(chunkI.Mode)
		if err != nil || modeI != "pr" {
			continue
		}
		repoI := model.NormalizeRepo(chunkI.Repo)
		for j := i + 1; j < len(out); j++ {
			chunkJ := out[j]
			if IsTerminalChunkStatus(chunkJ.Status) {
				continue
			}
			modeJ, err := model.NormalizeChunkMode(chunkJ.Mode)
			if err != nil || modeJ != "pr" {
				continue
			}
			repoJ := model.NormalizeRepo(chunkJ.Repo)
			if !strings.EqualFold(repoI, repoJ) {
				continue
			}
			if (i < len(reach) && j < len(reach[i]) && reach[i][j]) || (j < len(reach) && i < len(reach[j]) && reach[j][i]) {
				continue
			}
			if err := CheckDisjointFiles(chunkI, chunkJ, repoI); err != nil {
				return err
			}
		}
	}
	return nil
}

// CheckDisjointFiles rejects two chunks that claim the same target file.
func CheckDisjointFiles(a, b model.VanadisChunk, repo string) error {
	if len(a.TargetFiles) == 0 {
		return fmt.Errorf("%w: concurrent PR chunk %q on repo %q must specify target_files", model.ErrInvalidBuildInput, a.ID, repo)
	}
	if len(b.TargetFiles) == 0 {
		return fmt.Errorf("%w: concurrent PR chunk %q on repo %q must specify target_files", model.ErrInvalidBuildInput, b.ID, repo)
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
			return fmt.Errorf("%w: concurrent PR chunks %q and %q on repo %q have overlapping target_files: %s", model.ErrInvalidBuildInput, a.ID, b.ID, repo, normF)
		}
	}
	return nil
}

// ValidateCreateBuildInputs rejects empty build fields, malformed chunks,
// and invalid DAGs before a build is persisted.
func ValidateCreateBuildInputs(buildID, threadID, objective string, chunks []model.VanadisChunk) error {
	if buildID == "" || threadID == "" || objective == "" {
		return errors.New("build id, thread_id, and objective cannot be empty")
	}
	for i, c := range chunks {
		if strings.TrimSpace(c.ID) == "" || strings.TrimSpace(c.Title) == "" || strings.TrimSpace(c.Instructions) == "" {
			return fmt.Errorf("chunk[%d] id, title, and instructions cannot be empty", i)
		}
		if !model.IsValidVanadisChunkID(strings.TrimSpace(c.ID)) {
			return fmt.Errorf("chunks[%d].id must be 1..120 characters of [a-zA-Z0-9_-]", i)
		}
		if cb := strings.TrimSpace(c.BuildID); cb != "" && cb != buildID {
			return fmt.Errorf("chunk[%d] build_id %q does not match build id %q", i, cb, buildID)
		}
		if err := model.ValidateChunkModeAndRepo(i, c.Mode, c.Repo, c.TargetFiles, c.TargetSymbols); err != nil {
			return err
		}
		if _, err := model.NormalizeChunkEffort(c.Effort); err != nil {
			return fmt.Errorf("chunk[%d] %w", i, err)
		}
	}
	if _, err := NormalizeAndValidateBuildDAG(chunks); err != nil {
		return err
	}
	return nil
}

// IsTerminalChunkStatus reports whether a chunk status ends dependency and
// disjoint-file obligations.
func IsTerminalChunkStatus(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case dagStatusMerged, dagStatusCompleted, dagStatusFailed, dagStatusCancelled:
		return true
	default:
		return false
	}
}

// IsPRDependencyCodeReady reports whether a chunk's code is available to
// dependent PR chunks (completed code, or merged/deploying PRs).
func IsPRDependencyCodeReady(other model.VanadisChunk) bool {
	status := strings.ToLower(strings.TrimSpace(other.Status))
	if status == dagStatusCompleted {
		return true
	}
	if strings.ToLower(strings.TrimSpace(other.Mode)) == "pr" {
		switch status {
		case dagStatusMerged, dagStatusDeploying:
			return true
		}
	}
	return false
}

// CheckChunkDependenciesReady verifies every declared dependency of chunk
// has reached a launchable state for the chunk's mode.
func CheckChunkDependenciesReady(chunk model.VanadisChunk, chunks []model.VanadisChunk) error {
	for _, depID := range chunk.DependsOn {
		depReady := false
		for _, other := range chunks {
			if other.ID == depID {
				if strings.ToLower(strings.TrimSpace(chunk.Mode)) == "pr" {
					depReady = IsPRDependencyCodeReady(other)
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

// ValidateUpdateChunkDAG substitutes updated into existing, validates the entire DAG, and returns the normalized depends_on.
func ValidateUpdateChunkDAG(existing []model.VanadisChunk, updatedIdx int, updated model.VanadisChunk) ([]string, error) {
	candidateChunks := make([]model.VanadisChunk, len(existing))
	copy(candidateChunks, existing)
	candidateChunks[updatedIdx] = updated

	validated, err := NormalizeAndValidateBuildDAG(candidateChunks)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", model.ErrInvalidBuildInput, err)
	}
	return validated[updatedIdx].DependsOn, nil
}

// ValidateAppendChunksDAG combines existing chunks with appended chunks, validates the DAG, and returns the appended chunks with normalized depends_on.
func ValidateAppendChunksDAG(existing []model.VanadisChunk, appended []model.VanadisChunk) ([]model.VanadisChunk, error) {
	allChunks := make([]model.VanadisChunk, 0, len(existing)+len(appended))
	allChunks = append(allChunks, existing...)
	allChunks = append(allChunks, appended...)

	validatedAll, err := NormalizeAndValidateBuildDAG(allChunks)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", model.ErrInvalidBuildInput, err)
	}
	out := make([]model.VanadisChunk, len(appended))
	copy(out, appended)
	for i := range out {
		out[i].DependsOn = validatedAll[len(existing)+i].DependsOn
	}
	return out, nil
}

// RelinkDependenciesAfterRemoval rewrites dependencies in remaining after removedID is deleted,
// removing removedID and replacing it with removedDeps.
func RelinkDependenciesAfterRemoval(remaining []model.VanadisChunk, removedID string, removedDeps []string) {
	for i := range remaining {
		c := &remaining[i]
		if !slices.Contains(c.DependsOn, removedID) {
			continue
		}
		newDeps := make([]string, 0, len(c.DependsOn)+len(removedDeps))
		seen := make(map[string]bool)
		for _, dep := range c.DependsOn {
			if dep == removedID {
				for _, rd := range removedDeps {
					if rd != c.ID && !seen[rd] {
						seen[rd] = true
						newDeps = append(newDeps, rd)
					}
				}
			} else if !seen[dep] {
				seen[dep] = true
				newDeps = append(newDeps, dep)
			}
		}
		c.DependsOn = newDeps
	}
}
