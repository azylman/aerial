package main

import (
	"github.com/azylman/aerial/services/vanadis/internal/dag"
)

var (
	// NormalizeAndValidateBuildDAG validates the build DAG: explicit depends_on, Kahn's cycle detection, and concurrent same-repo disjoint target_files.
	NormalizeAndValidateBuildDAG = dag.NormalizeAndValidateBuildDAG
	normalizeAndValidateBuildDAG = dag.NormalizeAndValidateBuildDAG

	// ResolveChunkDependencies normalizes explicit depends_on references to chunk IDs.
	ResolveChunkDependencies = dag.ResolveChunkDependencies

	// CheckKahnDAGCycles detects cycles in chunk dependencies via Kahn's algorithm.
	CheckKahnDAGCycles = dag.CheckKahnDAGCycles

	// ValidateConcurrentDisjointPRChunks rejects concurrent same-repo PR chunks with overlapping target_files.
	ValidateConcurrentDisjointPRChunks = dag.ValidateConcurrentDisjointPRChunks

	// ParseCreateBuildChunks validates raw chunk inputs and normalizes the build DAG.
	ParseCreateBuildChunks = dag.ParseCreateBuildChunks
	parseCreateBuildChunks = dag.ParseCreateBuildChunks
)
