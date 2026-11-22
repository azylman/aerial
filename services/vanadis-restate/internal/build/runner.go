package build

import (
	"context"
	"strings"

	restate "github.com/restatedev/sdk-go"

	"github.com/azylman/aerial/pkg/buildplan"
	"github.com/azylman/aerial/services/vanadis-restate/internal/ericlient"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

// ReconcileReasonWidgetSync asks a Build object to reconcile and re-sync its
// status widget, including the failed-build widget that normal
// reconciliation skips.
const ReconcileReasonWidgetSync = "widget_sync"

const labelRunnerGuard = "runner-guard"

// runnerAgent is the normalized agent whose builds this object executes.
func (b *BuildObject) runnerAgent() string {
	if b == nil {
		return store.NormalizeRunnerAgent("")
	}
	return store.NormalizeRunnerAgent(b.agentName)
}

// runnerGuardResult is the journaled runner lookup for the keyed build.
type runnerGuardResult struct {
	Found       bool   `json:"found"`
	RunnerAgent string `json:"runner_agent,omitempty"`
}

// guardRunner keeps calling rt.RunRunnerGuard (restate.WithName("runner-guard"))
// so the journal entry index does not shift mid-rollout, but returns
// runnerGuardResult{Found: true, RunnerAgent: "aerial"}, nil without querying Postgres.
func (b *BuildObject) guardRunner(ctx restate.ObjectSharedContext, handlerLabel string) error {
	if b == nil {
		return nil
	}
	if b.runtimeFactory != nil {
		oc, ok := ctx.(restate.ObjectContext)
		if !ok {
			oc = nil
		}
		rt := b.runtimeFactory(oc)
		buildID := strings.TrimSpace(rt.Key())
		if buildID == "" {
			return nil
		}
		_, err := rt.RunRunnerGuard(func(rc context.Context) (runnerGuardResult, error) {
			return runnerGuardResult{Found: true, RunnerAgent: "aerial"}, nil
		})
		return err
	}

	buildID := strings.TrimSpace(restate.Key(ctx))
	if buildID == "" {
		return nil
	}
	_, err := RunJournaled(ctx, handlerLabel, labelRunnerGuard, func(rc restate.RunContext) (runnerGuardResult, error) {
		return runnerGuardResult{Found: true, RunnerAgent: "aerial"}, nil
	})
	return err
}

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
	route.RunnerAgent = store.NormalizeRunnerAgent(route.RunnerAgent)
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
		rt.SendReconcile(buildplan.BuildServiceName(route.RunnerAgent), route.BuildID, ReconcileReasonWidgetSync)
	}
}
