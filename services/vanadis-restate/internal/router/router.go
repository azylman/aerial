package router

import (
	"context"
	"errors"
	"fmt"
	"strings"

	restate "github.com/restatedev/sdk-go"

	"github.com/azylman/aerial/pkg/buildplan"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/strutil"
	"github.com/azylman/aerial/services/vanadis-restate/internal/build"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

// BuildObjectInvoker delivers routed requests to one build's Build object.
type BuildObjectInvoker interface {
	RoutePREvent(ctx context.Context, buildID string, ev pubsub.PRLifecycleEvent) error
	RouteDispatchOutcome(ctx context.Context, buildID string, ev pubsub.DispatchOutcomeEvent) error
	RouteAttachPR(ctx context.Context, buildID string, req store.AttachChunkPRRequest) (model.AttachChunkPRResponse, error)
	RouteFailChunk(ctx context.Context, buildID string, req model.FailChunkRequest) (model.FailChunkResponse, error)
	RouteReconcile(ctx context.Context, buildID string, req build.ReconcileBuildRequest) error
}

type restateBuildObjectInvoker struct {
	ctx restate.Context
}

func (r restateBuildObjectInvoker) RoutePREvent(ctx context.Context, buildID string, ev pubsub.PRLifecycleEvent) error {
	_ = ctx
	_ = restate.ObjectSend(r.ctx, buildplan.BuildServiceName(""), buildID, "OnPREvent").Send(ev)
	return nil
}

func (r restateBuildObjectInvoker) RouteDispatchOutcome(ctx context.Context, buildID string, ev pubsub.DispatchOutcomeEvent) error {
	_ = ctx
	_ = restate.ObjectSend(r.ctx, buildplan.BuildServiceName(""), buildID, "OnDispatchOutcome").Send(ev)
	return nil
}

func (r restateBuildObjectInvoker) RouteAttachPR(ctx context.Context, buildID string, req store.AttachChunkPRRequest) (model.AttachChunkPRResponse, error) {
	_ = ctx
	return restate.Object[model.AttachChunkPRResponse](r.ctx, buildplan.BuildServiceName(""), buildID, "AttachPR").Request(req)
}

func (r restateBuildObjectInvoker) RouteFailChunk(ctx context.Context, buildID string, req model.FailChunkRequest) (model.FailChunkResponse, error) {
	_ = ctx
	return restate.Object[model.FailChunkResponse](r.ctx, buildplan.BuildServiceName(""), buildID, "FailChunk").Request(req)
}

func (r restateBuildObjectInvoker) RouteReconcile(ctx context.Context, buildID string, req build.ReconcileBuildRequest) error {
	_ = ctx
	_ = restate.ObjectSend(r.ctx, buildplan.BuildServiceName(""), buildID, "Reconcile").Send(req)
	return nil
}

// BuildRouterService resolves non-build-keyed requests and delegates them to the authoritative Build object.
type BuildRouterService struct {
	store          store.VanadisStore
	invokerFactory func(restate.Context) BuildObjectInvoker
	agentName      string
}

// NewBuildRouterService constructs a BuildRouterService.
func NewBuildRouterService(st store.VanadisStore, invokerFactory ...func(restate.Context) BuildObjectInvoker) *BuildRouterService {
	if pg, ok := st.(*store.Store); ok && pg == nil {
		st = nil
	}
	var ifn func(restate.Context) BuildObjectInvoker
	if len(invokerFactory) > 0 {
		ifn = invokerFactory[0]
	}
	return &BuildRouterService{
		store:          st,
		invokerFactory: ifn,
	}
}

// NewBuildRouterServiceWithAgent constructs a BuildRouterService with an agent-scoped Restate service name.
func NewBuildRouterServiceWithAgent(st store.VanadisStore, agentName string, invokerFactory ...func(restate.Context) BuildObjectInvoker) *BuildRouterService {
	s := NewBuildRouterService(st, invokerFactory...)
	s.agentName = strings.TrimSpace(agentName)
	return s
}

// ServiceName returns the Restate service name.
func (s *BuildRouterService) ServiceName() string {
	if s == nil {
		return buildplan.BuildRouterServiceName("")
	}
	return buildplan.BuildRouterServiceName(s.agentName)
}

func (s *BuildRouterService) getInvoker(ctx restate.Context) BuildObjectInvoker {
	if s != nil && s.invokerFactory != nil {
		return s.invokerFactory(ctx)
	}
	return restateBuildObjectInvoker{ctx: ctx}
}

func isLookupNotFound(err error) bool {
	return errors.Is(err, build.ErrChunkNotFound) || errors.Is(err, store.ErrBuildNotFound)
}

func (s *BuildRouterService) resolvePRBuildRoutes(rc restate.RunContext, repo string, ev pubsub.PRLifecycleEvent) ([]store.BuildRoute, error) {
	if s.store == nil {
		return nil, nil
	}
	routes, err := s.store.LookupPRBuildRoutes(rc, repo, ev.PRNumber)
	if err == nil && len(routes) > 0 {
		return routes, nil
	}
	if err != nil && !isLookupNotFound(err) {
		return nil, err
	}

	if chunkID := build.ExtractChunkIDFromBranch(repo, ev.Branch); chunkID != "" {
		route, err := s.store.LookupChunkBuildID(rc, chunkID)
		if err == nil && route.BuildID != "" {
			return []store.BuildRoute{route}, nil
		}
		if err != nil && !isLookupNotFound(err) {
			return nil, err
		}
		return nil, nil
	}

	return nil, nil
}

// RoutePREvent routes inbound PR lifecycle events to the owning Build Virtual Object.
func (s *BuildRouterService) RoutePREvent(ctx restate.Context, ev pubsub.PRLifecycleEvent) error {
	repo := strings.TrimSpace(ev.Repo)
	if repo == "" || ev.PRNumber <= 0 {
		err := fmt.Errorf("%w: repo and pr_number required", store.ErrInvalidBuildInput)
		build.RecordReconcileError(build.LabelRoutePREvent, err)
		return restate.ToTerminalError(err, restate.WithErrorCode(400))
	}
	ev.TargetID = strutil.FirstNonEmpty(strings.TrimSpace(ev.TargetID), model.ExtractTargetIDFromBranch(repo, ev.Branch))

	routes, err := build.RunJournaled(ctx, build.LabelRoutePREvent, "lookup-pr-chunk", func(rc restate.RunContext) ([]store.BuildRoute, error) {
		return s.resolvePRBuildRoutes(rc, repo, ev)
	})
	if err != nil {
		return err
	}

	if s.store != nil && len(routes) == 0 && strings.EqualFold(strings.TrimSpace(ev.Event), "opened") && build.CanFallbackThreadRepoLookup(ev) && model.ExtractChunkIDFromBranch(repo, ev.Branch) == "" {
		attached, aErr := s.RouteAttachPR(ctx, store.AttachChunkPRRequest{
			PRNumber: ev.PRNumber,
			Repo:     repo,
			Branch:   ev.Branch,
			Title:    ev.Title,
			TargetID: ev.TargetID,
			Quiet:    ev.Quiet,
		})
		if aErr != nil {
			return aErr
		}
		if attached.BuildID != "" {
			routes = []store.BuildRoute{{BuildID: attached.BuildID}}
		}
	}

	if len(routes) == 0 {
		return nil
	}

	inv := s.getInvoker(ctx)
	for _, route := range routes {
		if route.BuildID == "" {
			continue
		}
		if err := inv.RoutePREvent(ctx, route.BuildID, ev); err != nil {
			return err
		}
	}
	return nil
}

// RouteDispatchOutcome routes dispatch outcomes to the target Build Virtual Object.
func (s *BuildRouterService) RouteDispatchOutcome(ctx restate.Context, ev pubsub.DispatchOutcomeEvent) error {
	chunkID := strings.TrimSpace(ev.VanadisChunkID)
	if chunkID == "" {
		return nil
	}

	route, err := build.RunJournaled(ctx, build.LabelRouteDispatchOutcome, "lookup-dispatch-chunk", func(rc restate.RunContext) (store.BuildRoute, error) {
		if s.store == nil {
			return store.BuildRoute{}, nil
		}
		route, err := s.store.LookupChunkBuildID(rc, chunkID)
		if err != nil {
			if isLookupNotFound(err) {
				return store.BuildRoute{}, nil
			}
			return store.BuildRoute{}, err
		}
		return route, nil
	})
	if err != nil {
		return err
	}

	if route.BuildID == "" {
		return nil
	}

	inv := s.getInvoker(ctx)
	return inv.RouteDispatchOutcome(ctx, route.BuildID, ev)
}

// RouteAttachPR resolves the build and delegates PR attachment to the target Build.
func (s *BuildRouterService) RouteAttachPR(ctx restate.Context, req store.AttachChunkPRRequest) (model.AttachChunkPRResponse, error) {
	req.ChunkID = strings.TrimSpace(req.ChunkID)
	req.Repo = strings.TrimSpace(req.Repo)
	if req.PRNumber <= 0 || (req.ChunkID == "" && req.Repo == "") {
		err := fmt.Errorf("%w: chunk_id or repo, and pr_number required", store.ErrInvalidBuildInput)
		build.RecordReconcileError(build.LabelRouteAttachPR, err)
		return model.AttachChunkPRResponse{}, restate.ToTerminalError(err, restate.WithErrorCode(400))
	}

	// A new standalone build runs on the canonical default Build runner
	// while retaining req.Agent as the build owner.
	newBuild := func() store.BuildRoute {
		return store.BuildRoute{BuildID: buildplan.GenerateBuildID()}
	}
	route, err := build.RunJournaled(ctx, build.LabelRouteAttachPR, "resolve-attach-build-id", func(rc restate.RunContext) (store.BuildRoute, error) {
		if s.store == nil {
			return newBuild(), nil
		}
		if req.ChunkID != "" {
			route, err := s.store.LookupChunkBuildID(rc, req.ChunkID)
			if err == nil && route.BuildID != "" {
				return route, nil
			}
			if err != nil && !isLookupNotFound(err) {
				return store.BuildRoute{}, err
			}
		}
		if req.Repo != "" {
			route, _, err := s.store.LookupPRChunk(rc, req.Repo, req.PRNumber)
			if err == nil && route.BuildID != "" {
				return route, nil
			}
			if err != nil && !isLookupNotFound(err) {
				return store.BuildRoute{}, err
			}
		}
		return newBuild(), nil
	})
	if err != nil {
		return model.AttachChunkPRResponse{}, err
	}

	inv := s.getInvoker(ctx)
	return inv.RouteAttachPR(ctx, route.BuildID, req)
}

// RouteFailChunk resolves the build for a chunk and delegates failure to the Build object.
func (s *BuildRouterService) RouteFailChunk(ctx restate.Context, req model.FailChunkRequest) (model.FailChunkResponse, error) {
	chunkID := strings.TrimSpace(req.ChunkID)
	reason := strings.TrimSpace(req.Reason)
	if chunkID == "" || reason == "" {
		err := fmt.Errorf("%w: chunk_id and reason required", store.ErrInvalidBuildInput)
		build.RecordReconcileError(build.LabelRouteFailChunk, err)
		return model.FailChunkResponse{}, restate.ToTerminalError(err, restate.WithErrorCode(400))
	}

	route, err := build.RunJournaled(ctx, build.LabelRouteFailChunk, "lookup-fail-chunk-build", func(rc restate.RunContext) (store.BuildRoute, error) {
		if s.store == nil {
			return store.BuildRoute{}, build.ErrChunkNotFound
		}
		return s.store.LookupChunkBuildID(rc, chunkID)
	})
	if err != nil {
		if errors.Is(err, build.ErrChunkNotFound) {
			return model.FailChunkResponse{}, restate.ToTerminalError(build.ErrChunkNotFound, restate.WithErrorCode(404))
		}
		return model.FailChunkResponse{}, err
	}

	if req.BuildID != "" && req.BuildID != route.BuildID {
		build.RecordReconcileError(build.LabelRouteFailChunk, build.ErrChunkNotFound)
		return model.FailChunkResponse{}, build.ToTerminalErr(build.ErrChunkNotFound)
	}
	inv := s.getInvoker(ctx)
	return inv.RouteFailChunk(ctx, route.BuildID, req)
}

// RouteBuildReconcile resolves a build and sends it a one-way
// widget_sync Reconcile. Unknown builds are dropped.
func (s *BuildRouterService) RouteBuildReconcile(ctx restate.Context, buildID string) error {
	buildID = strings.TrimSpace(buildID)
	if buildID == "" {
		err := fmt.Errorf("%w: build_id required", store.ErrInvalidBuildInput)
		build.RecordReconcileError(build.LabelRouteBuildReconcile, err)
		return restate.ToTerminalError(err, restate.WithErrorCode(400))
	}
	route, err := build.RunJournaled(ctx, build.LabelRouteBuildReconcile, "lookup-reconcile-build", func(rc restate.RunContext) (store.BuildRoute, error) {
		if s.store == nil {
			return store.BuildRoute{}, nil
		}
		bld, _, err := s.store.GetBuild(rc, buildID)
		if err != nil {
			if isLookupNotFound(err) {
				return store.BuildRoute{}, nil
			}
			return store.BuildRoute{}, err
		}
		if bld == nil {
			return store.BuildRoute{}, nil
		}
		return store.BuildRoute{BuildID: bld.ID}, nil
	})
	if err != nil {
		return err
	}
	if route.BuildID == "" {
		return nil
	}
	inv := s.getInvoker(ctx)
	return inv.RouteReconcile(ctx, route.BuildID, build.ReconcileBuildRequest{Reason: build.ReconcileReasonWidgetSync})
}
