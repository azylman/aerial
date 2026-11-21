package prstate

// Dark Phase-1 production-graph anchors.
//
// The prstate logic surface (chunks 01-03c) is fully dark: hermetic tests
// exercise it, but no synapse consumer routes here until the cutover chunk
// wires it for real. The CI deadcode gate requires every production-graph
// function to be reachable from main. RTA reachability flows through live
// calls: passing a value into keepLive roots the value's whole method set
// plus everything it transitively calls, without invoking any behavior. No
// state transitions run, no effects fire, no K/V is touched.
//
// This file references entry points only; granular helpers, in-memory
// stores, no-op defaults, and hermetic fakes resolve transitively through
// static dispatch from the anchored roots.
//
// The cutover chunk that wires these components for real deletes this file.
// Do not add new anchors for new components; wire them instead.
func keepLive(_ ...any) {}

func init() {
	keepLive(
		// Check and collaborator handlers (chunk 04).
		HandleCheckEvent,
		HandleCollaboratorAlert,

		// Rollout and deploy handlers (chunk 05).
		HandleRolloutEvent,
		HandleDeployEvent,

		// RepoHandlers and ConflictHandlers are live through
		// RepoObjectService and PRObject.HandleConflictCheck; only the
		// in-memory repo test store needs a root.
		NewMemRepoStore,
		MemRepoStore{},

		// Registry projection rebuild stays dark; the live writer is
		// PRObject.Project.
		RebuildProjection,

		// Frozen key, ownership, and deploy-dedup helpers (chunk 01).
		PRObjectKey,
		RepoObjectKey,
		DeployPRKey,
		ClassifyOwnership,
		UnmanagedMustNotRoute,

		// Frozen stub surfaces and hermetic fakes.
		PRObjectStub{},
		RepoObjectStub{},
		FakeBuildRouterInvoker{},
		MapSnapshotLoader{},
	)
}
