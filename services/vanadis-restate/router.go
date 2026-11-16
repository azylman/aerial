package main

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	restate "github.com/restatedev/sdk-go"

	"github.com/azylman/aerial/pkg/pubsub"
)

// FailChunkRequest carries the chunk and reason for failing a chunk.
type FailChunkRequest struct {
	DispatchID string `json:"dispatch_id,omitempty"`
	BuildID    string `json:"build_id,omitempty"`
	ChunkID    string `json:"chunk_id"`
	Reason     string `json:"reason"`
}

// FailChunkResponse represents the result of failing a chunk.
type FailChunkResponse struct {
	Failed      bool   `json:"failed"`
	BuildID     string `json:"build_id"`
	ChunkID     string `json:"chunk_id"`
	BuildStatus string `json:"build_status"`
	ChunkStatus string `json:"chunk_status"`
}

// AttachChunkPRResponse represents the result of attaching a PR to a chunk or standalone build.
type AttachChunkPRResponse struct {
	Status   string `json:"status"`
	BuildID  string `json:"build_id"`
	ChunkID  string `json:"chunk_id"`
	ThreadID string `json:"thread_id"`
}

type buildObjectInvoker interface {
	RoutePREvent(ctx context.Context, buildID string, ev pubsub.PRLifecycleEvent) error
	RouteDispatchOutcome(ctx context.Context, buildID string, ev pubsub.DispatchOutcomeEvent) error
	RouteAttachPR(ctx context.Context, buildID string, req AttachChunkPRRequest) (AttachChunkPRResponse, error)
	RouteFailChunk(ctx context.Context, buildID string, req FailChunkRequest) (FailChunkResponse, error)
}

type restateBuildObjectInvoker struct {
	ctx restate.Context
}

func (r restateBuildObjectInvoker) RoutePREvent(ctx context.Context, buildID string, ev pubsub.PRLifecycleEvent) error {
	_ = ctx
	_ = restate.ObjectSend(r.ctx, "Build", buildID, "OnPREvent").Send(ev)
	return nil
}

func (r restateBuildObjectInvoker) RouteDispatchOutcome(ctx context.Context, buildID string, ev pubsub.DispatchOutcomeEvent) error {
	_ = ctx
	_ = restate.ObjectSend(r.ctx, "Build", buildID, "OnDispatchOutcome").Send(ev)
	return nil
}

func (r restateBuildObjectInvoker) RouteAttachPR(ctx context.Context, buildID string, req AttachChunkPRRequest) (AttachChunkPRResponse, error) {
	_ = ctx
	return restate.Object[AttachChunkPRResponse](r.ctx, "Build", buildID, "AttachPR").Request(req)
}

func (r restateBuildObjectInvoker) RouteFailChunk(ctx context.Context, buildID string, req FailChunkRequest) (FailChunkResponse, error) {
	_ = ctx
	return restate.Object[FailChunkResponse](r.ctx, "Build", buildID, "FailChunk").Request(req)
}

// BuildRouterService resolves non-build-keyed requests and delegates them to the authoritative Build object.
type BuildRouterService struct {
	store          VanadisStore
	invokerFactory func(restate.Context) buildObjectInvoker
}

// NewBuildRouterService constructs a BuildRouterService.
func NewBuildRouterService(store VanadisStore, invokerFactory ...func(restate.Context) buildObjectInvoker) *BuildRouterService {
	if pg, ok := store.(*PostgresVanadisStore); ok && pg == nil {
		store = nil
	}
	var ifn func(restate.Context) buildObjectInvoker
	if len(invokerFactory) > 0 {
		ifn = invokerFactory[0]
	}
	return &BuildRouterService{
		store:          store,
		invokerFactory: ifn,
	}
}

// ServiceName returns the Restate service name.
func (*BuildRouterService) ServiceName() string {
	return "BuildRouter"
}

func (s *BuildRouterService) getInvoker(ctx restate.Context) buildObjectInvoker {
	if s != nil && s.invokerFactory != nil {
		return s.invokerFactory(ctx)
	}
	return restateBuildObjectInvoker{ctx: ctx}
}

// RoutePREvent routes inbound PR lifecycle events to the owning Build Virtual Object.
func (s *BuildRouterService) RoutePREvent(ctx restate.Context, ev pubsub.PRLifecycleEvent) error {
	repo := strings.TrimSpace(ev.Repo)
	if repo == "" || ev.PRNumber <= 0 {
		return restate.ToTerminalError(fmt.Errorf("%w: repo and pr_number required", ErrInvalidBuildInput), restate.WithErrorCode(400))
	}

	buildID, err := restate.Run(ctx, func(rc restate.RunContext) (string, error) {
		if s.store == nil {
			return "", nil
		}
		bID, _, err := s.store.LookupPRChunk(rc, repo, ev.PRNumber)
		if err != nil {
			if errors.Is(err, ErrChunkNotFound) || errors.Is(err, ErrBuildNotFound) {
				return "", nil
			}
			return "", err
		}
		return bID, nil
	}, restate.WithName("lookup-pr-chunk"))
	if err != nil {
		return err
	}

	if buildID == "" {
		event := strings.ToLower(strings.TrimSpace(ev.Event))
		if event == "opened" {
			genID, err := restate.Run(ctx, func(rc restate.RunContext) (string, error) {
				return generateBuildID(), nil
			}, restate.WithName("generate-standalone-build-id"))
			if err != nil {
				return err
			}
			buildID = genID
		}
	}

	if buildID == "" {
		return nil
	}

	inv := s.getInvoker(ctx)
	return inv.RoutePREvent(ctx, buildID, ev)
}

// RouteDispatchOutcome routes dispatch outcomes to the target Build Virtual Object.
func (s *BuildRouterService) RouteDispatchOutcome(ctx restate.Context, ev pubsub.DispatchOutcomeEvent) error {
	chunkID := strings.TrimSpace(ev.VanadisChunkID)
	if chunkID == "" {
		return nil
	}

	buildID, err := restate.Run(ctx, func(rc restate.RunContext) (string, error) {
		if s.store == nil {
			return "", nil
		}
		bID, err := s.store.LookupChunkBuildID(rc, chunkID)
		if err != nil {
			if errors.Is(err, ErrChunkNotFound) || errors.Is(err, ErrBuildNotFound) {
				return "", nil
			}
			return "", err
		}
		return bID, nil
	}, restate.WithName("lookup-dispatch-chunk"))
	if err != nil {
		return err
	}

	if buildID == "" {
		return nil
	}

	inv := s.getInvoker(ctx)
	return inv.RouteDispatchOutcome(ctx, buildID, ev)
}

// RouteAttachPR resolves the build and delegates PR attachment to the target Build.
func (s *BuildRouterService) RouteAttachPR(ctx restate.Context, req AttachChunkPRRequest) (AttachChunkPRResponse, error) {
	req.ChunkID = strings.TrimSpace(req.ChunkID)
	req.Repo = strings.TrimSpace(req.Repo)
	if req.PRNumber <= 0 || (req.ChunkID == "" && req.Repo == "") {
		return AttachChunkPRResponse{}, restate.ToTerminalError(fmt.Errorf("%w: chunk_id or repo, and pr_number required", ErrInvalidBuildInput), restate.WithErrorCode(400))
	}

	buildID, err := restate.Run(ctx, func(rc restate.RunContext) (string, error) {
		if s.store == nil {
			return generateBuildID(), nil
		}
		if req.ChunkID != "" {
			bID, err := s.store.LookupChunkBuildID(rc, req.ChunkID)
			if err == nil && bID != "" {
				return bID, nil
			}
		}
		if req.Repo != "" {
			bID, _, err := s.store.LookupPRChunk(rc, req.Repo, req.PRNumber)
			if err == nil && bID != "" {
				return bID, nil
			}
		}
		return generateBuildID(), nil
	}, restate.WithName("resolve-attach-build-id"))
	if err != nil {
		return AttachChunkPRResponse{}, err
	}

	inv := s.getInvoker(ctx)
	return inv.RouteAttachPR(ctx, buildID, req)
}

// RouteFailChunk resolves the build for a chunk and delegates failure to the Build object.
func (s *BuildRouterService) RouteFailChunk(ctx restate.Context, req FailChunkRequest) (FailChunkResponse, error) {
	chunkID := strings.TrimSpace(req.ChunkID)
	reason := strings.TrimSpace(req.Reason)
	if chunkID == "" || reason == "" {
		return FailChunkResponse{}, restate.ToTerminalError(fmt.Errorf("%w: chunk_id and reason required", ErrInvalidBuildInput), restate.WithErrorCode(400))
	}

	buildID, err := restate.Run(ctx, func(rc restate.RunContext) (string, error) {
		if s.store == nil {
			return "", ErrChunkNotFound
		}
		return s.store.LookupChunkBuildID(rc, chunkID)
	}, restate.WithName("lookup-fail-chunk-build"))
	if err != nil {
		if errors.Is(err, ErrChunkNotFound) {
			return FailChunkResponse{}, restate.ToTerminalError(ErrChunkNotFound, restate.WithErrorCode(404))
		}
		return FailChunkResponse{}, err
	}

	if req.BuildID != "" && req.BuildID != buildID {
		return FailChunkResponse{}, toTerminalErr(ErrChunkNotFound)
	}
	inv := s.getInvoker(ctx)
	return inv.RouteFailChunk(ctx, buildID, req)
}

func lockFailureParent(ctx context.Context, tx pgx.Tx, chunkID string) (string, error) {
	var parentID string
	err := tx.QueryRow(ctx, `SELECT id FROM vanadis_builds WHERE id = (SELECT build_id FROM vanadis_chunks WHERE id = $1) FOR UPDATE;`, chunkID).Scan(&parentID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrChunkNotFound
	}
	if err != nil {
		return "", fmt.Errorf("lock failure build: %w", err)
	}
	return parentID, nil
}
