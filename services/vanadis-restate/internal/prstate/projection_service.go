package prstate

// PRObject registry writer.
//
// After every committed PRObject transition, afterCommit sends a one-way
// self-send to PRObject.Project, which re-derives the snapshot from
// committed state and upserts the full row in a journaled restate.Run.
// PRObject is the single pr_registry writer. A service built without a
// projection store (hermetic tests) sends nothing.

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	restate "github.com/restatedev/sdk-go"

	"github.com/azylman/aerial/pkg/prwire"
)

// PRObject handler names for projection self-sends.
const (
	PRHandlerProject = "Project"
)

// Projection timing defaults: the retry delay paces re-projection after a
// write exhausted its in-invocation retries.
const (
	DefaultProjectionRetry = time.Minute
	projectionRunAttempts  = 5
)

var projectionWritesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "prstate_projection_writes_total",
	Help: "pr_registry projection writes by PRObject, by result (ok, error, unchanged).",
}, []string{metricLabelResult})

func init() {
	registerCollector(projectionWritesTotal)
}

// registerCollector registers c on the default registry, tolerating a
// duplicate registration and panicking on any other conflict.
func registerCollector(c prometheus.Collector) {
	if err := prometheus.Register(c); err != nil {
		if _, ok := errors.AsType[prometheus.AlreadyRegisteredError](err); !ok {
			panic(fmt.Sprintf("prstate: register metric: %v", err))
		}
	}
}

// WithProjection injects the pr_registry writer and returns s. A nil store
// disables projection.
func (s *PRObjectService) WithProjection(store ProjectionStore) *PRObjectService {
	s.projection = store
	return s
}

// afterCommit runs after a PRObject handler persisted st. It never fails:
// it syncs the rollout index and sends a one-way self-send to Project, which
// writes the pr_registry row from committed state.
func (s *PRObjectService) afterCommit(ctx restate.ObjectContext, st *PRState) {
	if st == nil || st.Repo == "" || st.PRNumber <= 0 {
		return
	}
	s.syncRolloutIndex(ctx, st)
	if s.projection != nil {
		restate.ObjectSend(ctx, prwire.PRObjectServiceName, prwire.PRObjectKey(st.Repo, st.PRNumber), PRHandlerProject).Send(ProjectRequest{})
	}
}

// ProjectRequest is the empty Project input; the object key names the PR
// and the snapshot is always re-derived from committed state.
type ProjectRequest struct{}

// Project upserts the full pr_registry row from committed state. An
// identical snapshot already projected is skipped, and so is an untracked
// PR: synapse only keeps rows for registered PRs, which always carry a
// target thread, so a PR no Register or target-carrying event
// reached gets no row until one does. A write that exhausts its
// journaled retries schedules a delayed re-projection instead of failing, so
// a database outage never gates later transitions for the PR.
func (s *PRObjectService) Project(ctx restate.ObjectContext, _ ProjectRequest) error {
	st, err := restate.Get[*PRState](ctx, s.stateKey)
	if err != nil {
		return err
	}
	if st == nil || s.projection == nil {
		return nil
	}
	snap := BuildProjection(st)
	key := ProjectionJournalKey(snap)
	if !snap.Tracked() || st.ProjectedKey == key {
		return nil
	}
	_, runErr := restate.Run(ctx, func(rc restate.RunContext) (restate.Void, error) {
		if err := ProjectRegistry(rc, s.projection, snap); err != nil {
			projectionWritesTotal.WithLabelValues("error").Inc()
			return restate.Void{}, err
		}
		projectionWritesTotal.WithLabelValues("ok").Inc()
		return restate.Void{}, nil
	}, restate.WithName("project-registry"), restate.WithMaxRetryAttempts(projectionRunAttempts))
	if runErr != nil {
		slog.ErrorContext(ctx, "[prstate] PRObject registry projection failed; retrying later", "pr", restate.Key(ctx), "error", runErr)
		restate.ObjectSend(ctx, prwire.PRObjectServiceName, restate.Key(ctx), PRHandlerProject).Send(ProjectRequest{}, restate.WithDelay(s.retryDelay))
		return nil
	}
	st.ProjectedKey = key
	restate.Set(ctx, s.stateKey, st)
	return nil
}
