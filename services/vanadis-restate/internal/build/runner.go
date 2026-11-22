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
	routes []store.BuildRoute
}

func withCrossBuildReconciles(ctx context.Context) (context.Context, *crossBuildReconciles) {
	c := &crossBuildReconciles{}
	return context.WithValue(ctx, crossBuildReconcileKey{}, c), c
}

// recordCrossBuildReconcile queues a Reconcile for another build on the
// collector in ctx, deduplicating by build ID.
func recordCrossBuildReconcile(ctx context.Context, route store.BuildRoute) {
	c, ok := ctx.Value(crossBuildReconcileKey{}).(*crossBuildReconciles)
	if !ok || c == nil || strings.TrimSpace(route.BuildID) == "" {
		return
	}
	for _, existing := range c.routes {
		if existing.BuildID == route.BuildID {
			return
		}
	}
	c.routes = append(c.routes, route)
}

func (c *crossBuildReconciles) list() []store.BuildRoute {
	if c == nil || len(c.routes) == 0 {
		return nil
	}
	return append([]store.BuildRoute(nil), c.routes...)
}

// sendCrossBuildReconciles delivers journaled cross-build reconciles one-way
// to each build's own runner, which evaluates it under its own lock and token.
func sendCrossBuildReconciles(rt objectRuntime, routes []store.BuildRoute) {
	for _, route := range routes {
		rt.SendReconcile(buildplan.BuildServiceName(""), route.BuildID, ReconcileReasonWidgetSync)
	}
}
