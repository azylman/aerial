package prstate

// PRObject registry writer, shadow compare, and hydrator.
//
// After every committed PRObject transition, afterCommit routes the row by
// the event's effects owner:
//   - restate: a one-way self-send to PRObject.Project, which re-derives the
//     snapshot from committed state and upserts the full row in a journaled
//     restate.Run. PRObject is then the single pr_registry writer.
//   - legacy: synapse still writes the row, so prstate writes nothing; a
//     delayed one-way send to the shared CompareProjection handler reads the
//     row synapse wrote and compares it with the snapshot taken at commit.
//     The shared handler never takes the object lock, so the delay is never
//     held across an exclusive invocation.
//
// Both sends are gated on injected dependencies: a service built without a
// projection store (hermetic tests, the dark default) sends nothing.

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	restate "github.com/restatedev/sdk-go"
)

// PRObject handler names for projection self-sends.
const (
	PRHandlerProject           = "Project"
	PRHandlerCompareProjection = "CompareProjection"
)

// Projection timing defaults. The shadow delay gives synapse time to write
// its row before prstate reads it; the retry delay paces re-projection after
// a write exhausted its in-invocation retries.
const (
	DefaultShadowCompareDelay = 30 * time.Second
	DefaultProjectionRetry    = time.Minute
	projectionRunAttempts     = 5
)

var (
	projectionWritesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "prstate_projection_writes_total",
		Help: "pr_registry projection writes by PRObject, by result (ok, error, unchanged).",
	}, []string{metricLabelResult})
	projectionComparedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prstate_projection_compared_total",
		Help: "Legacy pr_registry rows compared field by field against the PRObject snapshot.",
	})
	projectionSupersededTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prstate_projection_superseded_total",
		Help: "Legacy pr_registry rows skipped by the shadow compare because head/merge SHA or status moved past the snapshot.",
	})
	projectionDivergenceTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "prstate_projection_divergence_total",
		Help: "Fields where the legacy pr_registry row diverged from the PRObject snapshot (field=row when the row is missing).",
	}, []string{"field"})
)

func init() {
	for _, c := range []prometheus.Collector{projectionWritesTotal, projectionComparedTotal, projectionSupersededTotal, projectionDivergenceTotal} {
		registerCollector(c)
	}
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

// WithProjection injects the pr_registry writer and reader and returns s.
// A nil store disables restate-owned projection; a nil reader disables the
// legacy shadow compare.
func (s *PRObjectService) WithProjection(store ProjectionStore, reader ProjectionReader) *PRObjectService {
	s.projection = store
	s.reader = reader
	return s
}

// afterCommit runs after a PRObject handler persisted st. It never fails:
// both branches are journaled one-way sends.
func (s *PRObjectService) afterCommit(ctx restate.ObjectContext, st *PRState, owner EffectsOwner) {
	if st == nil || NormalizeRepo(st.Repo) == "" || st.PRNumber <= 0 {
		return
	}
	key := PRObjectKey(st.Repo, st.PRNumber)
	if ParseEffectsOwner(string(owner)) == EffectsOwnerRestate {
		if s.projection != nil {
			restate.ObjectSend(ctx, PRObjectServiceName, key, PRHandlerProject).Send(ProjectRequest{})
		}
		return
	}
	if s.reader != nil {
		restate.ObjectSend(ctx, PRObjectServiceName, key, PRHandlerCompareProjection).Send(BuildProjection(st), restate.WithDelay(s.shadowDelay))
	}
}

// ProjectRequest is the empty Project input; the object key names the PR
// and the snapshot is always re-derived from committed state.
type ProjectRequest struct{}

// Project upserts the full pr_registry row from committed state. An
// identical snapshot already projected is skipped. A write that exhausts its
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
	if st.ProjectedKey == key {
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
		restate.ObjectSend(ctx, PRObjectServiceName, restate.Key(ctx), PRHandlerProject).Send(ProjectRequest{}, restate.WithDelay(s.retryDelay))
		return nil
	}
	st.ProjectedKey = key
	restate.Set(ctx, s.stateKey, st)
	return nil
}

// CompareProjection is the shared, read-only shadow compare for legacy
// events: it reads the row synapse wrote and records the verdict in metrics
// and a [prstate-shadow] log line. Read failures are logged and dropped;
// the compare is evidence, never authority.
func (s *PRObjectService) CompareProjection(ctx restate.ObjectSharedContext, snap ProjectionSnapshot) error {
	if s.reader == nil || NormalizeRepo(snap.Repo) == "" || snap.PRNumber <= 0 {
		return nil
	}
	_, err := restate.Run(ctx, func(rc restate.RunContext) (ProjectionComparison, error) {
		row, found, err := s.reader.LoadRegistryRow(rc, snap.Repo, snap.PRNumber)
		if err != nil {
			return ProjectionComparison{}, err
		}
		cmp := CompareProjection(snap, row, found)
		recordComparison(rc, snap, cmp)
		return cmp, nil
	}, restate.WithName("compare-projection"), restate.WithMaxRetryAttempts(projectionRunAttempts))
	if err != nil {
		slog.WarnContext(ctx, "[prstate-shadow] projection compare could not read the registry row", "pr", PRObjectKey(snap.Repo, snap.PRNumber), "error", err)
	}
	return nil
}

// recordComparison emits the metrics and log line for one verdict.
func recordComparison(ctx restate.RunContext, snap ProjectionSnapshot, cmp ProjectionComparison) {
	pr := PRObjectKey(snap.Repo, snap.PRNumber)
	switch {
	case cmp.Missing:
		projectionDivergenceTotal.WithLabelValues("row").Inc()
		slog.WarnContext(ctx, "[prstate-shadow] projection divergence: legacy registry row missing", "pr", pr, "fields", "row")
	case cmp.Superseded:
		projectionSupersededTotal.Inc()
	case len(cmp.Divergent) > 0:
		projectionComparedTotal.Inc()
		for _, field := range cmp.Divergent {
			projectionDivergenceTotal.WithLabelValues(field).Inc()
		}
		slog.WarnContext(ctx, "[prstate-shadow] projection divergence", "pr", pr, "fields", strings.Join(cmp.Divergent, ","), "status", snap.Status, "head_sha", snap.HeadSHA)
	default:
		projectionComparedTotal.Inc()
	}
}

// Hydrate seeds state from one pr_registry row (exclusive, effect-free,
// idempotent): it fills only empty fields, claims the row owner, and sends
// the head/merge SHAs to the RepoObject index when they changed. A row
// for another repo#pr fails terminally with 400.
func (s *PRObjectService) Hydrate(ctx restate.ObjectContext, req HydrateRequest) (HydrateResponse, error) {
	repo := NormalizeRepo(req.Row.Repo)
	if repo == "" || req.Row.PRNumber <= 0 || restate.Key(ctx) != PRObjectKey(repo, req.Row.PRNumber) {
		return HydrateResponse{}, restate.ToTerminalError(errHydrateKeyMismatch, restate.WithErrorCode(400))
	}
	st, err := restate.Get[*PRState](ctx, s.stateKey)
	if err != nil {
		return HydrateResponse{}, err
	}
	if st == nil {
		st = NewPRState(repo, req.Row.PRNumber)
	}
	before := snapshotIndexView(st)
	prevMerge := st.MergeSHA
	if !st.ApplyHydrate(req.Row) {
		return HydrateResponse{Agent: st.OwnerAgent(), Status: st.RegistryStatus()}, nil
	}
	restate.Set(ctx, s.stateKey, st)
	if snapshotIndexView(st) != before || st.MergeSHA != prevMerge {
		ev := PREvent{Repo: repo, PRNumber: st.PRNumber, Branch: st.Branch}
		sendPRIndex(ctx, st, ev, hydrateIndexLifecycle(st))
		if st.hydrateDeploying() {
			sendPRIndex(ctx, st, ev, IndexDeploying)
		}
	}
	return HydrateResponse{Changed: true, Agent: st.OwnerAgent(), Status: st.RegistryStatus()}, nil
}

var errHydrateKeyMismatch = errors.New("prstate: hydrate row must name this object's repo and pr_number")

// hydrateIndexLifecycle picks the index op for a hydrated PR from its
// terminal state.
func hydrateIndexLifecycle(st *PRState) string {
	switch st.Terminal {
	case TerminalMerged:
		return IndexMerged
	case TerminalClosed:
		return IndexClosed
	default:
		return IndexActive
	}
}
