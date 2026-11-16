package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
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
func (s *BuildRouterService) RoutePREvent(ctx restate.Context, ev pubsub.PRLifecycleEvent) (outErr error) {
	defer func() {
		if outErr != nil {
			recordReconcileError("RoutePREvent", outErr)
		}
	}()

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
func (s *BuildRouterService) RouteDispatchOutcome(ctx restate.Context, ev pubsub.DispatchOutcomeEvent) (outErr error) {
	defer func() {
		if outErr != nil {
			recordReconcileError("RouteDispatchOutcome", outErr)
		}
	}()

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
func (s *BuildRouterService) RouteAttachPR(ctx restate.Context, req AttachChunkPRRequest) (resp AttachChunkPRResponse, outErr error) {
	defer func() {
		if outErr != nil {
			recordReconcileError("RouteAttachPR", outErr)
		}
	}()

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
func (s *BuildRouterService) RouteFailChunk(ctx restate.Context, req FailChunkRequest) (resp FailChunkResponse, outErr error) {
	defer func() {
		if outErr != nil {
			recordReconcileError("RouteFailChunk", outErr)
		}
	}()

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

var (
	newPgxPoolFn  = pgxpool.NewWithConfig
	pingPgxPoolFn = func(ctx context.Context, pool *pgxpool.Pool) error {
		return pool.Ping(ctx)
	}
	closePgxPoolFn = func(pool *pgxpool.Pool) {
		if pool != nil {
			pool.Close()
		}
	}
)

var initPostgres = func(ctx context.Context, pgURL string) (*PostgresVanadisStore, func(context.Context), error) {
	poolConfig, err := pgxpool.ParseConfig(pgURL)
	if err != nil {
		return nil, nil, fmt.Errorf("parse postgres url: %w", err)
	}
	poolConfig.MaxConns, poolConfig.MinConns = 10, 2
	pool, err := newPgxPoolFn(ctx, poolConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("create pgxpool: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pingPgxPoolFn(pingCtx, pool); err != nil {
		closePgxPoolFn(pool)
		return nil, nil, fmt.Errorf("ping postgres: %w", err)
	}
	store := NewPostgresVanadisStore(pool)
	return store, func(context.Context) {
		closePgxPoolFn(pool)
	}, nil
}

func normalizeDiscordThreadID(s string) string {
	clean := strings.TrimSpace(s)
	if idx := strings.LastIndex(clean, ":"); idx >= 0 {
		clean = strings.TrimSpace(clean[idx+1:])
	}
	return strings.TrimSpace(strings.TrimPrefix(clean, "discord-"))
}

func generateBuildID() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return fmt.Sprintf("build-%d-%s", time.Now().Unix(), hex.EncodeToString(b))
}

func cleanRepoName(repo string) string {
	repo = strings.TrimSpace(repo)
	if idx := strings.LastIndex(repo, "/"); idx >= 0 {
		return repo[idx+1:]
	}
	return repo
}

func actionDispatchID(chunk VanadisChunk) string {
	return fmt.Sprintf("vanadis-%s-turn-%d", chunk.ID, chunk.StallNudges)
}

func actionCompletionAccepted(chunk VanadisChunk, buildStatus, dispatchID string) bool {
	if chunk.Status == statusCompleted && (buildStatus == statusRunning || buildStatus == statusCompleted) {
		chunk.Status = statusRunning
		buildStatus = statusRunning
	}
	return actionCompletionAllowed(chunk, buildStatus, dispatchID)
}

// actionLaunchKey preserves legacy acknowledgement keys outside action retries.
func actionLaunchKey(chunk VanadisChunk) string {
	if chunk.Mode == chunkModeAction && chunk.StallNudges > 0 {
		// A colon is forbidden in chunk IDs, keeping generation keys disjoint.
		return "dispatch:" + actionDispatchID(chunk)
	}
	return chunk.ID
}

// errPRTransitionSkipped means the persisted row rejected a stale lifecycle transition.
var errPRTransitionSkipped = errors.New("pr lifecycle transition not applied")

func (s *PostgresVanadisStore) CompletePRDeployment(ctx context.Context, chunkID string, rolloutState *pubsub.PRRolloutState) error {
	if s == nil || s.db == nil {
		return errors.New("database connection is nil")
	}
	chunkID = strings.TrimSpace(chunkID)
	if chunkID == "" {
		return errors.New("chunk_id cannot be empty")
	}
	var evidence any
	if rolloutState != nil {
		data, err := json.Marshal(rolloutState)
		if err != nil {
			return fmt.Errorf("marshal rollout state: %w", err)
		}
		evidence = data
	}
	tag, err := s.db.Exec(ctx, `UPDATE vanadis_chunks AS c SET status = 'completed', banner = '', rollout_state = COALESCE($1::jsonb, c.rollout_state), completed_at = COALESCE(c.completed_at, CURRENT_TIMESTAMP), updated_at = CURRENT_TIMESTAMP FROM vanadis_builds AS b WHERE c.id = $2 AND b.id = c.build_id AND (c.mode = 'pr' OR (c.mode = 'action' AND COALESCE(c.pr_number, 0) > 0)) AND c.status NOT IN ('failed', 'cancelled') AND b.status != 'cancelled' AND (b.status != 'failed' OR c.status = 'completed');`, evidence, chunkID)
	if err != nil {
		return fmt.Errorf("complete pr deployment: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return errPRTransitionSkipped
	}
	return nil
}

// UpdatePRLifecycleStatus prevents delayed pre-deployment events from reopening
// a terminal chunk. Notifications follow only an applied write.
func (s *PostgresVanadisStore) UpdatePRLifecycleStatus(ctx context.Context, chunkID, status string) error {
	if s == nil || s.db == nil {
		return errors.New("database connection is nil")
	}
	if strings.TrimSpace(chunkID) == "" {
		return errors.New("chunk_id cannot be empty")
	}
	tag, err := s.db.Exec(ctx, `UPDATE vanadis_chunks SET status = $1, completed_at = CASE WHEN $1 = 'merged' THEN COALESCE(completed_at, CURRENT_TIMESTAMP) ELSE NULL END, updated_at = CURRENT_TIMESTAMP WHERE id = $2 AND status NOT IN ('completed', 'failed', 'cancelled');`, status, strings.TrimSpace(chunkID))
	if err != nil {
		return fmt.Errorf("update pr lifecycle status: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return errPRTransitionSkipped
	}
	return nil
}

func dispatchCompletionMutation(ev pubsub.DispatchOutcomeEvent) *BuildMutationRequest {
	switch strings.ToLower(strings.TrimSpace(ev.Status)) {
	case "completed", dispatchStatusSuccess:
		return &BuildMutationRequest{Action: mutationActionCompleted, ChunkID: strings.TrimSpace(ev.VanadisChunkID), DispatchID: ev.DispatchID}
	default:
		return nil
	}
}

func isActionCompletionMutation(mutation *BuildMutationRequest, chunks []VanadisChunk) bool {
	if mutation == nil {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(mutation.Action)) {
	case mutationActionCompleted, mutationChunkDeploySuccess:
		for _, chunk := range chunks {
			if chunk.ID == mutation.ChunkID && chunk.Mode == "action" && chunk.PRNumber == 0 {
				return true
			}
		}
	}
	return false
}

func terminalNotificationRecorded(state *BuildObjectState) bool {
	return state != nil && state.TerminalNotified
}

func normalizeCompletionMutation(mutation *BuildMutationRequest) *BuildMutationRequest {
	if mutation == nil {
		return nil
	}
	switch strings.ToLower(strings.TrimSpace(mutation.Action)) {
	case mutationActionCompleted, mutationChunkDeploySuccess:
		normalized := *mutation
		normalized.ChunkID = strings.TrimSpace(normalized.ChunkID)
		return &normalized
	default:
		return mutation
	}
}

func failureSkipped(c VanadisChunk, buildStatus, dispatchID string) bool {
	return (c.Mode == chunkModeAction && !actionFailureAllowed(c, buildStatus, dispatchID)) || c.Status == statusFailed || c.Status == statusCompleted || c.Status == statusCancelled
}

func actionFailureAllowed(c VanadisChunk, buildStatus, dispatchID string) bool {
	if c.PRNumber == 0 {
		return actionCompletionAllowed(c, buildStatus, dispatchID)
	}
	// PR attachment retains action mode. PR closure and remediation retain their
	// identities, while old action-turn identities must still match the generation.
	prIdentity := dispatchID == "" || strings.HasPrefix(dispatchID, "vanadis-"+c.ID+"-cifail-") ||
		dispatchID == "vanadis-conflict-"+c.ID || strings.HasPrefix(dispatchID, "vanadis-conflict-"+c.ID+"-")
	return buildStatus == statusRunning && c.Status != statusMerged && c.Status != statusDeploying &&
		(prIdentity || dispatchID == actionDispatchID(c))
}

const dispatchStatusSuccess = "success"

const mutationActionCompleted = "action_completed"

const mutationChunkDeploySuccess = "chunk_deploy_success"

var errActionCompletionSkipped = errors.New("action completion not applied")

func (b *BuildObject) completePRDeploymentMutation(ctx context.Context, buildID string, mutation *BuildMutationRequest) error {
	build, chunks, err := b.store.GetBuild(ctx, buildID)
	if err != nil {
		return err
	}
	alreadyCompleted := false
	found := false
	for _, chunk := range chunks {
		if chunk.ID != mutation.ChunkID {
			continue
		}
		found = true
		alreadyCompleted = chunk.Status == statusCompleted
		if chunk.Mode == "action" && chunk.PRNumber == 0 {
			return b.completeActionDeploymentMutation(ctx, mutation)
		}
		if chunk.Status == statusFailed || chunk.Status == statusCancelled || build.Status == statusCancelled || (build.Status == statusFailed && chunk.Status != statusCompleted) {
			return nil
		}
	}
	if !found {
		return ErrChunkNotFound
	}
	if build.Status == statusCancelled {
		return nil
	}
	if err := b.store.CompletePRDeployment(ctx, mutation.ChunkID, mutation.RolloutState); err != nil {
		if errors.Is(err, errPRTransitionSkipped) {
			return nil
		}
		return err
	}
	if b.executor != nil && !alreadyCompleted {
		if err := b.executor.PublishExternalEvent(ctx, "vanadis_chunk", "completed", []string{mutation.ChunkID}, ""); err != nil {
			log.Printf("[vanadis-restate] publish event warning: %v", err)
		}
	}

	return nil
}

func (b *BuildObject) validateActionBuild(ctx context.Context, buildID, chunkID string) error {
	_, chunks, err := b.store.GetBuild(ctx, buildID)
	if err != nil {
		return err
	}
	for _, c := range chunks {
		if c.ID == chunkID && c.BuildID == buildID {
			return nil
		}
	}
	return ErrChunkNotFound
}

func (b *BuildObject) completeActionDeploymentMutation(ctx context.Context, mutation *BuildMutationRequest) error {
	res, err := b.store.CompleteActionChunk(ctx, mutation.ChunkID, mutation.DispatchID)
	if err != nil {
		return err
	}
	if res == nil || !res.CompletionAccepted {
		return errActionCompletionSkipped
	}
	if b.executor != nil && res.Advanced {
		if err := b.executor.PublishExternalEvent(ctx, "vanadis_chunk", "completed", []string{mutation.ChunkID}, ""); err != nil {
			log.Printf("[vanadis-restate] publish event warning: %v", err)
		}
	}
	return nil
}

func (b *BuildObject) failMutation(ctx context.Context, buildID string, mutation *BuildMutationRequest) error {
	chunkID := strings.TrimSpace(mutation.ChunkID)
	if err := b.validateActionBuild(ctx, buildID, chunkID); err != nil {
		return err
	}
	_, _, failed, err := b.store.FailChunk(ctx, chunkID, mutation.Reason, mutation.DispatchID)
	if err != nil {
		return err
	}
	if !failed {
		return errActionCompletionSkipped
	}
	return nil
}
