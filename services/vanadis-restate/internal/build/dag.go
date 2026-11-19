package build

import (
	"github.com/azylman/aerial/services/vanadis-restate/internal/dag"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
)

// Thin package-private wrappers over internal/dag. They exist only so
// untouched internal/build test files keep compiling against the historic
// lowercase names; production code must call dag.* directly.
func normalizeAndValidateBuildDAG(chunks []model.VanadisChunk) ([]model.VanadisChunk, error) {
	return dag.NormalizeAndValidateBuildDAG(chunks)
}

func validateCreateBuildInputs(buildID, threadID, objective string, chunks []model.VanadisChunk) error {
	return dag.ValidateCreateBuildInputs(buildID, threadID, objective, chunks)
}

func buildTransitiveClosure(out []model.VanadisChunk, idMap map[string]int) [][]bool {
	return dag.BuildTransitiveClosure(out, idMap)
}

func validateConcurrentDisjointPRChunks(out []model.VanadisChunk, reach [][]bool) error {
	return dag.ValidateConcurrentDisjointPRChunks(out, reach)
}
