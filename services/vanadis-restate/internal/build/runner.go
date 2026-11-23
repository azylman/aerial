package build

import (
	"context"
	"strings"

	"github.com/azylman/aerial/pkg/buildplan"
	"github.com/azylman/aerial/services/vanadis-restate/internal/ericlient"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

// ReconcileReasonWidgetSync asks a Build object to reconcile and re-sync its
// status widget, including the failed-build widget that normal
// reconciliation skips.
const ReconcileReasonWidgetSync = "widget_sync"

// withBuildOwner scopes ctx to the build owner's agent so eri calls for the
// build reach the owner's sessions rather than this runner's AGENT_NAME.
func withBuildOwner(ctx context.Context, bld *model.VanadisBuild) context.Context {
	if bld == nil || strings.TrimSpace(bld.Agent) == "" {
		return ctx
	}
	return ericlient.WithAgent(ctx, bld.Agent)
}

// crossBuildReconcileKey carries the collector for builds other than the
// current object that a journaled step changed and that must be reconciled
// by their own runner after the step commits.
type crossBuildReconcileKey struct{}

type crossBuildReconciles struct {
	targets []store.SweptChunk
}

func withCrossBuildReconciles(ctx context.Context) (context.Context, *crossBuildReconciles) {
	c := &crossBuildReconciles{}
	return context.WithValue(ctx, crossBuildReconcileKey{}, c), c
}

// recordCrossBuildReconcile queues a Mutate(chunk_deploy_success) for another
// build on the collector in ctx, deduplicating by BuildID + "/" + ChunkID
// (ignoring empty BuildID or ChunkID).
func recordCrossBuildReconcile(ctx context.Context, target store.SweptChunk) {
	c, ok := ctx.Value(crossBuildReconcileKey{}).(*crossBuildReconciles)
	if !ok || c == nil {
		return
	}
	buildID := strings.TrimSpace(target.BuildID)
	chunkID := strings.TrimSpace(target.ChunkID)
	if buildID == "" || chunkID == "" {
		return
	}
	key := buildID + "/" + chunkID
	for _, existing := range c.targets {
		if existing.BuildID+"/"+existing.ChunkID == key {
			return
		}
	}
	c.targets = append(c.targets, store.SweptChunk{ChunkID: chunkID, BuildID: buildID})
}

func (c *crossBuildReconciles) list() []store.SweptChunk {
	if c == nil || len(c.targets) == 0 {
		return nil
	}
	return append([]store.SweptChunk(nil), c.targets...)
}

// sendCrossBuildReconciles delivers journaled cross-build deploy success mutations
// one-way to each build's own runner.
func sendCrossBuildReconciles(rt objectRuntime, targets []store.SweptChunk) {
	for _, target := range targets {
		rt.SendCrossBuildDeploySuccess(buildplan.BuildServiceName(""), target.BuildID, target.ChunkID)
	}
}
