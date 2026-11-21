package build

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	restate "github.com/restatedev/sdk-go"

	"github.com/azylman/aerial/services/vanadis-restate/internal/ericlient"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

// ReconcileReasonWidgetSync asks a Build object to reconcile and re-sync its
// status widget, including the failed-build widget that normal
// reconciliation skips.
const ReconcileReasonWidgetSync = "widget_sync"

const labelRunnerGuard = "runner-guard"

// ErrForeignRunner means a Build object was asked to act on a build that a
// different agent's vanadis-restate runner executes.
var ErrForeignRunner = fmt.Errorf("%w: build is executed by another runner", store.ErrBuildConflict)

// runnerAgent is the normalized agent whose builds this object executes.
func (b *BuildObject) runnerAgent() string {
	if b == nil {
		return store.NormalizeRunnerAgent("")
	}
	return store.NormalizeRunnerAgent(b.agentName)
}

// RunsBuild reports whether a Build object of ownAgent executes builds
// recorded with runnerAgent. Empty and the default agent compare equal.
func RunsBuild(ownAgent, runnerAgent string) bool {
	return model.BuildServiceName(store.NormalizeRunnerAgent(ownAgent)) == model.BuildServiceName(store.NormalizeRunnerAgent(runnerAgent))
}

// runnerGuardResult is the journaled runner lookup for the keyed build.
type runnerGuardResult struct {
	Found       bool   `json:"found"`
	RunnerAgent string `json:"runner_agent,omitempty"`
}

// guardRunner refuses, with a terminal 409 and no side effects, to act on a
// build whose runner_agent belongs to a different Build service. Unknown
// builds pass through so handlers keep their existing not-found behavior.
func (b *BuildObject) guardRunner(ctx restate.ObjectContext, handlerLabel string) error {
	if b == nil || b.store == nil {
		return nil
	}
	rt := b.getRuntime(ctx)
	buildID := strings.TrimSpace(rt.Key())
	if buildID == "" {
		return nil
	}
	res, err := rt.RunRunnerGuard(func(rc context.Context) (runnerGuardResult, error) {
		res, err := b.lookupRunner(rc, buildID)
		if err != nil {
			RecordReconcileError(handlerLabel, err)
		}
		return res, ToTerminalErr(err)
	})
	if err != nil {
		return err
	}
	return b.checkRunner(handlerLabel, buildID, res)
}

func (b *BuildObject) lookupRunner(ctx context.Context, buildID string) (runnerGuardResult, error) {
	bld, _, err := b.store.GetBuild(ctx, buildID)
	if errors.Is(err, store.ErrBuildNotFound) {
		return runnerGuardResult{}, nil
	}
	if err != nil {
		return runnerGuardResult{}, err
	}
	if bld == nil {
		return runnerGuardResult{}, nil
	}
	return runnerGuardResult{Found: true, RunnerAgent: store.NormalizeRunnerAgent(bld.RunnerAgent)}, nil
}

func (b *BuildObject) checkRunner(handlerLabel, buildID string, res runnerGuardResult) error {
	if !res.Found || RunsBuild(b.runnerAgent(), res.RunnerAgent) {
		return nil
	}
	err := fmt.Errorf("%w: build %s runs on %s, not %s", ErrForeignRunner, buildID, model.BuildServiceName(res.RunnerAgent), b.ServiceName())
	RecordReconcileError(handlerLabel, err)
	return restate.ToTerminalError(err, restate.WithErrorCode(http.StatusConflict))
}

// checkCreateOwner refuses, with a terminal 400, to create a build whose
// owner agent belongs to a different Build service. An empty owner counts as
// the default agent, so a sibling runner never accepts an unowned build.
func (b *BuildObject) checkCreateOwner(agent string) error {
	if RunsBuild(b.runnerAgent(), agent) {
		return nil
	}
	err := fmt.Errorf("%w: build owned by %q belongs on %s, not %s", store.ErrInvalidBuildInput, strings.TrimSpace(agent), model.BuildServiceName(store.NormalizeRunnerAgent(agent)), b.ServiceName())
	RecordReconcileError(labelCreate, err)
	return restate.ToTerminalError(err, restate.WithErrorCode(http.StatusBadRequest))
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
		rt.SendReconcile(model.BuildServiceName(route.RunnerAgent), route.BuildID, ReconcileReasonWidgetSync)
	}
}
