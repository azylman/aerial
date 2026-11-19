// Package dag holds Vanadis build DAG validation and chunk parsing.
package dag

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/azylman/aerial/services/vanadis/internal/model"
)

// NormalizeAndValidateBuildDAG validates the build DAG: explicit depends_on, Kahn's cycle detection, and concurrent same-repo disjoint target_files.
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

	reach := buildTransitiveClosure(out, idMap)
	if err := ValidateConcurrentDisjointPRChunks(out, reach); err != nil {
		return nil, err
	}

	return out, nil
}

// ResolveChunkDependencies normalizes explicit depends_on references to chunk IDs.
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

// CheckKahnDAGCycles detects cycles in chunk dependencies via Kahn's algorithm.
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

// ValidateConcurrentDisjointPRChunks rejects concurrent same-repo PR chunks with overlapping target_files.
func ValidateConcurrentDisjointPRChunks(out []model.VanadisChunk, reach [][]bool) error {
	N := len(out)
	for i := range N {
		if isTerminalChunkStatus(out[i].Status) {
			continue
		}
		modeI, err := model.NormalizeChunkMode(out[i].Mode)
		if err != nil || modeI != "pr" {
			continue
		}
		repoI := model.NormalizeRepo(out[i].Repo)
		for j := i + 1; j < N; j++ {
			if isTerminalChunkStatus(out[j].Status) {
				continue
			}
			modeJ, err := model.NormalizeChunkMode(out[j].Mode)
			if err != nil || modeJ != "pr" {
				continue
			}
			repoJ := model.NormalizeRepo(out[j].Repo)
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

func checkDisjointFiles(a, b model.VanadisChunk, repo string) error {
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

func isTerminalChunkStatus(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "merged", "completed", "failed", "cancelled":
		return true
	default:
		return false
	}
}

// ParseCreateBuildChunks validates raw chunk inputs and normalizes the build DAG.
func ParseCreateBuildChunks(buildID string, now time.Time, rawChunks []model.CreateChunkInput) ([]model.VanadisChunk, error) {
	chunks := make([]model.VanadisChunk, len(rawChunks))
	for i, c := range rawChunks {
		title, instructions := strings.TrimSpace(c.Title), strings.TrimSpace(c.Instructions)
		if title == "" || instructions == "" {
			return nil, fmt.Errorf("chunk[%d] title and instructions are required", i)
		}
		chunkID := model.FirstNonEmpty(strings.TrimSpace(c.ID), fmt.Sprintf("%s-chunk-%d", buildID, i+1))
		if !model.IsValidVanadisChunkID(chunkID) {
			return nil, fmt.Errorf("chunks[%d].id must be 1..120 characters of [a-zA-Z0-9_-]", i)
		}
		mode, err := model.NormalizeChunkMode(c.Mode)
		if err != nil {
			return nil, fmt.Errorf("chunk[%d] %w", i, err)
		}
		if valErr := model.ValidateChunkModeAndRepo(i, c.Mode, c.Repo, c.TargetFiles, c.TargetSymbols); valErr != nil {
			return nil, valErr
		}
		repo := ""
		if mode == "pr" {
			repo = model.NormalizeRepo(c.Repo)
		}
		effort, err := model.NormalizeChunkEffort(c.Effort)
		if err != nil {
			return nil, fmt.Errorf("chunk[%d] invalid effort: %w", i, err)
		}
		status := "pending"
		var startedAt time.Time
		chunks[i] = model.VanadisChunk{
			ID: chunkID, BuildID: buildID, ChunkIndex: i, Mode: mode,
			Repo: repo, Title: title, Instructions: instructions,
			TargetFiles: model.NonNilSlice(c.TargetFiles), TargetSymbols: model.NonNilSlice(c.TargetSymbols), Effort: effort,
			AcceptanceCriteria: model.NonNilSlice(c.AcceptanceCriteria), Status: status, DependsOn: c.DependsOn, StartedAt: startedAt, CreatedAt: now, UpdatedAt: now,
		}
	}
	return NormalizeAndValidateBuildDAG(chunks)
}
