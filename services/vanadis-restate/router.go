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

const (
	prEventOpened = "opened"
)

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

func isLookupNotFound(err error) bool {
	return errors.Is(err, ErrChunkNotFound) || errors.Is(err, ErrBuildNotFound)
}

func canFallbackThreadRepoLookup(ev pubsub.PRLifecycleEvent) bool {
	if strings.TrimSpace(ev.TargetID) == "" || ev.Quiet {
		return false
	}
	branch := strings.TrimPrefix(strings.TrimSpace(ev.Branch), "refs/heads/")
	return !strings.HasPrefix(branch, "dependabot/")
}

func (s *BuildRouterService) resolvePRBuildID(rc restate.RunContext, repo string, ev pubsub.PRLifecycleEvent) (string, error) {
	if s.store == nil {
		return "", nil
	}
	bID, _, err := s.store.LookupPRChunk(rc, repo, ev.PRNumber)
	if err == nil && bID != "" {
		return bID, nil
	}
	if err != nil && !isLookupNotFound(err) {
		return "", err
	}

	if chunkID := extractChunkIDFromBranch(repo, ev.Branch); chunkID != "" {
		bID, err := s.store.LookupChunkBuildID(rc, chunkID)
		if err == nil && bID != "" {
			return bID, nil
		}
		if err != nil && !isLookupNotFound(err) {
			return "", err
		}
		return "", nil
	}

	if canFallbackThreadRepoLookup(ev) {
		bID, _, err := s.store.LookupUnattachedChunkByThreadAndRepo(rc, ev.TargetID, repo)
		if err == nil && bID != "" {
			return bID, nil
		}
		if err != nil && !isLookupNotFound(err) {
			return "", err
		}
	}

	return "", nil
}

// RoutePREvent routes inbound PR lifecycle events to the owning Build Virtual Object.
func (s *BuildRouterService) RoutePREvent(ctx restate.Context, ev pubsub.PRLifecycleEvent) error {
	repo := strings.TrimSpace(ev.Repo)
	if repo == "" || ev.PRNumber <= 0 {
		err := fmt.Errorf("%w: repo and pr_number required", ErrInvalidBuildInput)
		recordReconcileError(labelRoutePREvent, err)
		return restate.ToTerminalError(err, restate.WithErrorCode(400))
	}

	buildID, err := runJournaled(ctx, labelRoutePREvent, "lookup-pr-chunk", func(rc restate.RunContext) (string, error) {
		return s.resolvePRBuildID(rc, repo, ev)
	})
	if err != nil {
		return err
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

	buildID, err := runJournaled(ctx, labelRouteDispatchOutcome, "lookup-dispatch-chunk", func(rc restate.RunContext) (string, error) {
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
	})
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
		err := fmt.Errorf("%w: chunk_id or repo, and pr_number required", ErrInvalidBuildInput)
		recordReconcileError(labelRouteAttachPR, err)
		return AttachChunkPRResponse{}, restate.ToTerminalError(err, restate.WithErrorCode(400))
	}

	buildID, err := runJournaled(ctx, labelRouteAttachPR, "resolve-attach-build-id", func(rc restate.RunContext) (string, error) {
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
	})
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
		err := fmt.Errorf("%w: chunk_id and reason required", ErrInvalidBuildInput)
		recordReconcileError(labelRouteFailChunk, err)
		return FailChunkResponse{}, restate.ToTerminalError(err, restate.WithErrorCode(400))
	}

	buildID, err := runJournaled(ctx, labelRouteFailChunk, "lookup-fail-chunk-build", func(rc restate.RunContext) (string, error) {
		if s.store == nil {
			return "", ErrChunkNotFound
		}
		return s.store.LookupChunkBuildID(rc, chunkID)
	})
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

func isHexDigit(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

func extractChunkIDFromBranch(repo, branch string) string {
	repoName := strings.TrimSuffix(cleanRepoName(repo), ".git")
	if repoName == "" {
		return ""
	}
	branch = strings.TrimSpace(branch)
	branch = strings.TrimPrefix(branch, "refs/heads/")
	after, ok := strings.CutPrefix(branch, "feat/"+repoName+"-pr-vanadis-")
	if !ok {
		return ""
	}
	chunkID, suffix, hasSuffix := strings.CutLast(after, "-")
	if !hasSuffix || len(suffix) != 6 {
		return ""
	}
	for i := range suffix {
		if !isHexDigit(suffix[i]) {
			return ""
		}
	}
	if !isValidVanadisChunkID(chunkID) {
		return ""
	}
	return chunkID
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
	return buildStatus == statusRunning && c.Status != statusMerged && c.Status != statusDeploying && c.Status != statusDeployFailed &&
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
		if cErr := b.executor.CancelChunkDispatch(ctx, mutation.ChunkID); cErr != nil {
			log.Printf("[vanadis-restate] cancel chunk dispatch %s: %v", mutation.ChunkID, cErr)
		}
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

type prHeadVerifiedContextKey struct{}

func prHeadVerifiedFromContext(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	verified, ok := ctx.Value(prHeadVerifiedContextKey{}).(bool)
	return ok && verified
}

func (b *BuildObject) recordPRHeadRevision(ctx context.Context, chunkID string, ev pubsub.PRLifecycleEvent) error {
	headSHA := strings.TrimSpace(ev.HeadSHA)
	if headSHA != "" && b.executor != nil {
		if err := b.executor.validatePRHead(ctx, ev.Repo, ev.PRNumber, headSHA); err != nil {
			return err
		}
		ctx = context.WithValue(ctx, prHeadVerifiedContextKey{}, true)
	}
	if b.store == nil {
		return nil
	}
	return b.store.ResetPRCheckState(ctx, chunkID, headSHA, "")
}

// recordPRMergeRevision follows an accepted merged lifecycle transition.
func (b *BuildObject) recordPRMergeRevision(ctx context.Context, chunkID, rawHeadSHA, rawMergeSHA string) error {
	mergeSHA := strings.TrimSpace(rawMergeSHA)
	if mergeSHA == "" {
		return nil
	}
	if b.store == nil {
		return nil
	}
	return b.store.ResetPRCheckState(ctx, chunkID, strings.TrimSpace(rawHeadSHA), mergeSHA)
}

func (b *BuildObject) handlePRCheckEvent(ctx context.Context, chunkID, event string, ev pubsub.PRLifecycleEvent) error {
	checkName := strings.TrimSpace(ev.CheckName)
	if checkName == "" {
		return nil
	}
	checkState := strings.TrimSpace(ev.CheckState)
	if event == "check_failed" || event == "ci_failed" {
		checkState = "failed"
	}
	if checkState == "" {
		return nil
	}
	if b.store == nil {
		return nil
	}
	return b.store.RecordPRCheckStatus(ctx, chunkID, strings.TrimSpace(ev.HeadSHA), checkName, checkState)
}

func (b *BuildObject) lookupBuildChunk(rc restate.RunContext, buildID, chunkID string) (*VanadisBuild, VanadisChunk, error) {
	bld, chunks, err := b.store.GetBuild(rc, buildID)
	if err != nil {
		return nil, VanadisChunk{}, err
	}
	chunk := VanadisChunk{ID: chunkID, BuildID: buildID, Effort: effortMedium}
	for _, c := range chunks {
		if c.ID == chunkID {
			chunk = c
			chunk.Effort = capChunkEffort(c.Effort)
			break
		}
	}
	return bld, chunk, nil
}

func (b *BuildObject) dispatchChunkRemediationForChunk(rc restate.RunContext, bld *VanadisBuild, chunk VanadisChunk, dispatchID, prompt string) error {
	return b.executor.DispatchChunkRemediation(rc, BrainDispatchRequest{
		ID:             dispatchID,
		Source:         "vanadis-restate",
		SessionID:      "vanadis-" + strings.TrimSpace(chunk.ID),
		TargetID:       strings.TrimSpace(bld.ThreadID),
		VanadisChunkID: strings.TrimSpace(chunk.ID),
		Prompt:         prompt,
		Effort:         capChunkEffort(chunk.Effort),
		SkipIfActive:   true,
	})
}

func (b *BuildObject) handleCIRemediationCapReached(rc restate.RunContext, chunkID string, failCount int, ev pubsub.PRLifecycleEvent) error {
	bld, chk, failed, err := b.store.FailChunk(rc, chunkID, "ci_remediation_cap_reached", "")
	if err != nil {
		return err
	}
	if !failed || bld == nil || b.executor == nil {
		return nil
	}
	if cErr := b.executor.CancelChunkDispatch(rc, chunkID); cErr != nil {
		log.Printf("[vanadis-restate] cancel chunk dispatch %s: %v", chunkID, cErr)
	}
	if !bld.Quiet {
		targetID := strings.TrimSpace(bld.ThreadID)
		content := fmt.Sprintf("🛑 [%s#%d] CI check %q failed %d times (remediation cap %d/%d reached). Automatic fix loop paused for human review.", ev.Repo, ev.PRNumber, ev.CheckName, failCount, maxCIRemediationAttempts, maxCIRemediationAttempts)
		if mErr := b.executor.DispatchDirectMessage(rc, targetID, content); mErr != nil {
			log.Printf("[vanadis-restate] direct alert message warning: %v", mErr)
		}
	}
	capBanner := fmt.Sprintf("CI remediation cap (%d/%d) reached — awaiting human review", maxCIRemediationAttempts, maxCIRemediationAttempts)
	chunks := []VanadisChunk{}
	if chk != nil {
		chunks = append(chunks, *chk)
	}
	if _, sErr := b.executor.SyncBuildWidget(rc, b.store, *bld, chunks, capBanner); sErr != nil {
		log.Printf("[vanadis-restate] sync build widget warning: %v", sErr)
	}
	if pErr := b.executor.PublishExternalEvent(rc, "vanadis_chunk", "failed", []string{chunkID}, "ci_remediation_cap_reached"); pErr != nil {
		log.Printf("[vanadis-restate] publish event warning: %v", pErr)
	}
	if pErr := b.executor.PublishExternalEvent(rc, "vanadis_build", "failed", []string{bld.ID}, "ci_remediation_cap_reached"); pErr != nil {
		log.Printf("[vanadis-restate] publish event warning: %v", pErr)
	}
	return nil
}

func (b *BuildObject) handleConflictEvent(rc restate.RunContext, buildID, chunkID string, ev pubsub.PRLifecycleEvent) error {
	if b.store == nil {
		return nil
	}
	bld, chunk, err := b.lookupBuildChunk(rc, buildID, chunkID)
	if err != nil {
		return err
	}
	if head := strings.TrimSpace(ev.HeadSHA); head != "" && chunk.HeadSHA != "" && !strings.EqualFold(chunk.HeadSHA, head) {
		return errPRTransitionSkipped
	}
	if err = b.store.UpdateChunkBannerAndStatus(rc, chunkID, statusConflict, "Merge conflict detected"); err != nil {
		return err
	}
	if b.executor == nil {
		return nil
	}
	dispatchID := conflictRemediationDispatchID(chunkID, ev.HeadSHA)
	err = b.dispatchChunkRemediationForChunk(rc, bld, chunk, dispatchID, FormatConflictPrompt(ev))
	if errors.Is(err, errDispatchActive) {
		return nil
	}
	return err
}

func (b *BuildObject) handleCIFailureEvent(rc restate.RunContext, buildID, chunkID, event string, ev pubsub.PRLifecycleEvent) error {
	if isGateAggregatorCheck(ev.CheckName) {
		return nil
	}
	if err := b.handlePRCheckEvent(rc, chunkID, event, ev); err != nil {
		return err
	}
	banner := fmt.Sprintf("CI check %q failed", ev.CheckName)
	if b.store != nil {
		if err := b.store.UpdateChunkBannerAndStatus(rc, chunkID, statusCIFailed, banner); err != nil {
			return err
		}
	}
	if b.executor == nil {
		return nil
	}
	bld, chunk, err := b.lookupBuildChunk(rc, buildID, chunkID)
	if err != nil {
		return err
	}
	details := b.executor.ResolveCIFailureDetails(rc, ev.Repo, ev.PRNumber, ev.CheckName, ev.HeadSHA, ev.CheckURL, ev.Details)
	dispatchID := ciRemediationDispatchID(chunkID, ev.HeadSHA, chunk.CIFailCount+1)
	if err = b.dispatchChunkRemediationForChunk(rc, bld, chunk, dispatchID, FormatCIFailurePrompt(ev, details)); err != nil {
		if errors.Is(err, errDispatchActive) {
			return nil
		}
		return err
	}
	if chunk.CIFailCount >= maxCIRemediationAttempts {
		var failCount int
		if b.store != nil {
			failCount, err = b.store.IncrementChunkCIFailCount(rc, chunkID, banner)
			if err != nil {
				return err
			}
		}
		return b.handleCIRemediationCapReached(rc, chunkID, failCount, ev)
	}
	if b.store != nil {
		if _, err := b.store.IncrementChunkCIFailCount(rc, chunkID, banner); err != nil {
			return err
		}
	}
	return nil
}

func (b *BuildObject) handleClosedPREvent(rc restate.RunContext, chunkID string, ev pubsub.PRLifecycleEvent) error {
	if ev.Status == "merged" || b.store == nil {
		return nil
	}
	bld, _, failed, fErr := b.store.FailChunk(rc, chunkID, "pr_closed_unmerged", "")
	if fErr != nil {
		return fErr
	}
	if failed && bld != nil && bld.Status == statusFailed && b.executor != nil {
		b.notifyChunkFailure(rc, *bld, chunkID, "pr_closed_unmerged")
	}
	return nil
}

func (b *BuildObject) handleDeployFailureEvent(rc restate.RunContext, buildID, chunkID string, ev pubsub.PRLifecycleEvent) error {
	banner := FormatDeployFailureBanner(ev)
	if b.store != nil {
		if err := b.store.UpdateChunkBannerAndStatus(rc, chunkID, "deploy_failed", banner); err != nil {
			return err
		}
	}
	if b.executor != nil {
		b.dispatchThreadWakePrompt(rc, buildID, FormatDeployFailurePrompt(ev))
	}
	return nil
}

func (b *BuildObject) handlePREventType(rc restate.RunContext, buildID, chunkID, event string, ev pubsub.PRLifecycleEvent) error {
	switch event {
	case prEventOpened, "synchronize":
		return b.recordPRHeadRevision(rc, chunkID, ev)
	case "check_updated":
		return b.handlePRCheckEvent(rc, chunkID, event, ev)
	case "check_failed", "ci_failed":
		return b.handleCIFailureEvent(rc, buildID, chunkID, event, ev)
	case "conflict":
		return b.handleConflictEvent(rc, buildID, chunkID, ev)
	case "merged":
		if b.store != nil {
			if err := b.store.UpdatePRLifecycleStatus(rc, chunkID, "merged"); err != nil {
				return err
			}
		}
		if err := b.recordPRMergeRevision(rc, chunkID, ev.HeadSHA, ev.MergeSHA); err != nil {
			return err
		}
		if b.executor != nil {
			if cErr := b.executor.CancelChunkDispatch(rc, chunkID); cErr != nil {
				log.Printf("[vanadis-restate] cancel chunk dispatch %s: %v", chunkID, cErr)
			}
			if pErr := b.executor.PublishExternalEvent(rc, "vanadis_chunk", "merged", []string{chunkID}, ""); pErr != nil {
				log.Printf("[vanadis-restate] publish event warning: %v", pErr)
			}
		}
	case "closed", "closed_unmerged":
		return b.handleClosedPREvent(rc, chunkID, ev)
	case "deploy_failed", "sync_failed":
		return b.handleDeployFailureEvent(rc, buildID, chunkID, ev)
	}
	return nil
}

func (b *BuildObject) locateOrAttachPRChunk(rc restate.RunContext, buildID string, ev pubsub.PRLifecycleEvent) (string, error) {
	lookup, err := b.store.GetChunkByPR(rc, ev.Repo, ev.PRNumber)
	if err != nil {
		return "", err
	}
	if lookup != nil && lookup.Matched {
		return lookup.ChunkID, nil
	}
	return b.resolveOrAttachPRChunk(rc, buildID, ev)
}

func (b *BuildObject) dispatchPREvent(rc restate.RunContext, buildID, chunkID, event string, ev pubsub.PRLifecycleEvent) error {
	if event == "deploy_success" {
		return b.applyMutation(rc, buildID, &BuildMutationRequest{Action: mutationChunkDeploySuccess, ChunkID: chunkID, RolloutState: ev.RolloutState})
	}
	return b.handlePREventType(rc, buildID, chunkID, event, ev)
}

// OnPREvent receives PR lifecycle events for this build.
func (b *BuildObject) OnPREvent(ctx restate.ObjectContext, ev pubsub.PRLifecycleEvent) error {
	if b.store == nil {
		return nil
	}
	buildID := restate.Key(ctx)
	_, err := runJournaled(ctx, labelOnPREvent, "handle-pr-event-postgres", func(rc restate.RunContext) (bool, error) {
		chunkID, cErr := b.locateOrAttachPRChunk(rc, buildID, ev)
		if cErr != nil || chunkID == "" {
			return false, cErr
		}
		event := strings.ToLower(strings.TrimSpace(ev.Event))
		eventErr := b.dispatchPREvent(rc, buildID, chunkID, event, ev)
		if eventErr != nil {
			if errors.Is(eventErr, errPRTransitionSkipped) {
				return false, nil
			}
			return false, eventErr
		}
		b.refreshFailedPRWidget(rc, buildID, event)
		if event != "deploy_success" && ev.RolloutState != nil {
			rolloutStatus := ""
			if event == "deploy_started" {
				rolloutStatus = "deploying"
			}
			if rErr := b.store.RecordPRRolloutState(rc, chunkID, rolloutStatus, ev.RolloutState); rErr != nil {
				log.Printf("[vanadis-restate] record rollout state warning: %v", rErr)
			}
		}
		return true, nil
	})
	if err != nil {
		return err
	}
	rt := b.getRuntime(ctx)
	if _, tErr := b.executeObjectTransition(rt, labelReconcileAfterPREvent, nil); tErr != nil {
		if strings.EqualFold(strings.TrimSpace(ev.Event), "deploy_success") {
			return tErr
		}
		log.Printf("[vanadis-restate] pr event transition warning: %v", tErr)
	}
	return nil
}

func isEligibleUnattachedChunk(build *VanadisBuild, c *VanadisChunk, ev pubsub.PRLifecycleEvent) bool {
	if build == nil || c == nil {
		return false
	}
	if (c.Mode != "" && c.Mode != "pr") || c.PRNumber != 0 || c.StartedAt.IsZero() {
		return false
	}
	if c.Status != statusRunning && c.Status != statusCoding {
		return false
	}
	if buildThread := normalizeDiscordThreadID(build.ThreadID); buildThread != "" && strings.TrimSpace(ev.TargetID) != "" && normalizeDiscordThreadID(ev.TargetID) != buildThread {
		return false
	}
	return attachmentMatches(*c, *build, AttachChunkPRRequest{Repo: strings.TrimSpace(ev.Repo), PRNumber: ev.PRNumber})
}

func (b *BuildObject) syncAttachedPRWidget(rc restate.RunContext, buildID string) {
	if b.executor == nil {
		return
	}
	refreshedBuild, refreshedChunks, rErr := b.store.GetBuild(rc, buildID)
	if rErr == nil && refreshedBuild != nil {
		if _, sErr := b.executor.SyncBuildWidget(rc, b.store, *refreshedBuild, refreshedChunks, ""); sErr != nil {
			log.Printf("[vanadis-restate] sync build widget warning: %v", sErr)
		}
	}
}

func selectUnattachedTargetChunk(build *VanadisBuild, chunks []VanadisChunk, ev pubsub.PRLifecycleEvent) *VanadisChunk {
	if build == nil {
		return nil
	}
	if chunkID := extractChunkIDFromBranch(ev.Repo, ev.Branch); chunkID != "" {
		for i := range chunks {
			if chunks[i].ID == chunkID && chunks[i].BuildID == build.ID {
				if isEligibleUnattachedChunk(build, &chunks[i], ev) {
					return &chunks[i]
				}
				return nil
			}
		}
		return nil
	}
	evThread := normalizeDiscordThreadID(ev.TargetID)
	buildThread := normalizeDiscordThreadID(build.ThreadID)
	if !canFallbackThreadRepoLookup(ev) || evThread == "" || buildThread == "" || evThread != buildThread {
		return nil
	}
	var matched *VanadisChunk
	for i := range chunks {
		if chunks[i].BuildID == build.ID && isEligibleUnattachedChunk(build, &chunks[i], ev) {
			if matched != nil {
				return nil
			}
			matched = &chunks[i]
		}
	}
	return matched
}

func (b *BuildObject) resolveOrAttachPRChunk(rc restate.RunContext, buildID string, ev pubsub.PRLifecycleEvent) (string, error) {
	if b.executor == nil || (extractChunkIDFromBranch(ev.Repo, ev.Branch) == "" && !canFallbackThreadRepoLookup(ev)) {
		return "", nil
	}

	build, chunks, gErr := b.store.GetBuild(rc, buildID)
	if gErr != nil {
		if errors.Is(gErr, ErrBuildNotFound) {
			return "", nil
		}
		return "", gErr
	}

	targetChunk := selectUnattachedTargetChunk(build, chunks, ev)
	if targetChunk == nil {
		return "", nil
	}
	if vErr := b.executor.validateChunkPRAttachment(rc, *build, *targetChunk, ev); vErr != nil {
		if errors.Is(vErr, errPRTransitionSkipped) {
			return "", nil
		}
		return "", vErr
	}

	_, attachedChunkID, _, aErr := b.store.AttachChunkPR(rc, AttachChunkPRRequest{
		ChunkID:  targetChunk.ID,
		PRNumber: ev.PRNumber,
		Repo:     strings.TrimSpace(ev.Repo),
		Branch:   ev.Branch,
		Title:    ev.Title,
		TargetID: ev.TargetID,
		Quiet:    ev.Quiet,
	})
	if aErr != nil {
		if errors.Is(aErr, ErrBuildConflict) || errors.Is(aErr, ErrChunkNotFound) {
			return "", nil
		}
		return "", aErr
	}

	b.syncAttachedPRWidget(rc, buildID)
	return attachedChunkID, nil
}

// OnDispatchOutcome receives task execution outcomes.
func (b *BuildObject) OnDispatchOutcome(ctx restate.ObjectContext, ev pubsub.DispatchOutcomeEvent) error {
	if b.store == nil {
		return nil
	}
	applied, err := runJournaled(ctx, labelOnDispatchOutcome, "handle-dispatch-outcome-postgres", func(rc restate.RunContext) (bool, error) {
		chunkID := strings.TrimSpace(ev.VanadisChunkID)
		if chunkID == "" {
			return false, nil
		}
		status := strings.ToLower(strings.TrimSpace(ev.Status))
		switch status {
		case "completed", dispatchStatusSuccess:
			if err := b.validateActionBuild(rc, restate.Key(ctx), chunkID); err != nil {
				return false, err
			}
			res, err := b.store.CompleteActionChunk(rc, chunkID, ev.DispatchID)
			if err != nil {
				return false, err
			}
			return res != nil && res.CompletionAccepted, nil
		case "failed", "timeout", "crashed":
			reason := firstNonEmpty(ev.Error, "dispatch outcome failed")
			if err := b.validateActionBuild(rc, restate.Key(ctx), chunkID); err != nil {
				return false, err
			}
			_, _, failed, err := b.store.FailChunk(rc, chunkID, reason, ev.DispatchID)
			return failed, err
		}
		return true, nil
	})
	if err != nil {
		return err
	}
	if !applied {
		return nil
	}
	rt := b.getRuntime(ctx)
	mutation := dispatchCompletionMutation(ev)
	if _, tErr := b.executeObjectTransition(rt, labelReconcileAfterDispatch, mutation); tErr != nil {
		if mutation != nil {
			return tErr
		}
		log.Printf("[vanadis-restate] dispatch outcome transition warning: %v", tErr)
	}
	return nil
}

func (s *PostgresVanadisStore) RecordPRCheckStatus(ctx context.Context, chunkID, headSHA, checkName, checkState string) error {
	if s == nil || s.db == nil {
		return errors.New("database connection is nil")
	}
	id := strings.TrimSpace(chunkID)
	name := strings.TrimSpace(checkName)
	sha := strings.TrimSpace(headSHA)
	if id == "" || name == "" {
		return errors.New("chunk_id and check_name cannot be empty")
	}
	const q = `UPDATE vanadis_chunks
SET ci_checks = jsonb_set(
        CASE
            WHEN (status IN ('merged', 'synced', 'deploy_failed') OR COALESCE(ci_checks->>'_merge_sha', '') != '') AND $4 != '' AND COALESCE(ci_checks->>'_merge_sha', '') = ''
                THEN jsonb_set(COALESCE(ci_checks, '{}'::jsonb) - '_head_sha', '{_merge_sha}', to_jsonb($4::text), true)
            WHEN status NOT IN ('merged', 'synced', 'deploy_failed') AND COALESCE(ci_checks->>'_merge_sha', '') = '' AND $4 != '' AND COALESCE(ci_checks->>'_head_sha', '') = ''
                THEN jsonb_set(COALESCE(ci_checks, '{}'::jsonb), '{_head_sha}', to_jsonb($4::text), true)
            ELSE COALESCE(ci_checks, '{}'::jsonb)
        END,
        ARRAY[$1],
        to_jsonb($2::text),
        true
    ),
    status = CASE
        WHEN status = 'ci_failed' AND $2 = 'passed' AND NOT EXISTS (
            SELECT 1 FROM jsonb_each_text(jsonb_set(COALESCE(ci_checks, '{}'::jsonb), ARRAY[$1], to_jsonb($2::text), true))
            WHERE key NOT LIKE '\_%' AND value = 'failed'
        ) THEN 'pr_opened'
        ELSE status
    END,
    banner = CASE
        WHEN status = 'ci_failed' AND $2 = 'passed' AND NOT EXISTS (
            SELECT 1 FROM jsonb_each_text(jsonb_set(COALESCE(ci_checks, '{}'::jsonb), ARRAY[$1], to_jsonb($2::text), true))
            WHERE key NOT LIKE '\_%' AND value = 'failed'
        ) THEN ''
        ELSE banner
    END,
    updated_at = CURRENT_TIMESTAMP
WHERE id = $3
  AND status NOT IN ('deploying', 'deployed', 'completed', 'failed', 'cancelled')
  AND (
      $4 = ''
      OR ((status IN ('merged', 'synced', 'deploy_failed') OR COALESCE(ci_checks->>'_merge_sha', '') != '') AND (
          COALESCE(ci_checks->>'_merge_sha', '') = $4
          OR (COALESCE(ci_checks->>'_merge_sha', '') = '' AND COALESCE(ci_checks->>'_head_sha', '') IS DISTINCT FROM $4)
      ))
      OR (status NOT IN ('merged', 'synced', 'deploy_failed') AND COALESCE(ci_checks->>'_merge_sha', '') = '' AND (
          COALESCE(ci_checks->>'_head_sha', '') = ''
          OR ci_checks->>'_head_sha' = $4
      ))
  );`
	tag, err := s.db.Exec(ctx, q, name, strings.TrimSpace(checkState), id, sha)
	switch {
	case err != nil:
		return fmt.Errorf("record pr check status: %w", err)
	case tag.RowsAffected() == 0:
		return errPRTransitionSkipped
	default:
		return nil
	}
}

func (s *PostgresVanadisStore) ResetPRCheckState(ctx context.Context, chunkID, headSHA, mergeSHA string) error {
	if s == nil || s.db == nil {
		return errors.New("database connection is nil")
	}
	id := strings.TrimSpace(chunkID)
	if id == "" {
		return errors.New("chunk_id cannot be empty")
	}
	cleanHead := strings.TrimSpace(headSHA)
	cleanMerge := strings.TrimSpace(mergeSHA)
	if cleanMerge != "" {
		const mergeQ = `UPDATE vanadis_chunks
SET ci_checks = CASE
        WHEN COALESCE(ci_checks->>'_merge_sha', '') = $3
            THEN jsonb_strip_nulls(COALESCE(ci_checks, '{}'::jsonb) || jsonb_build_object('_head_sha', COALESCE(NULLIF($2::text, ''), NULLIF(ci_checks->>'_head_sha', '')), '_merge_sha', NULLIF($3::text, '')))
        ELSE jsonb_strip_nulls(jsonb_build_object('_head_sha', COALESCE(NULLIF($2::text, ''), NULLIF(ci_checks->>'_head_sha', '')), '_merge_sha', NULLIF($3::text, '')))
    END,
    banner = '',
    updated_at = CURRENT_TIMESTAMP
WHERE id = $1 AND status NOT IN ('completed', 'failed', 'cancelled');`
		tag, err := s.db.Exec(ctx, mergeQ, id, cleanHead, cleanMerge)
		if err != nil {
			return fmt.Errorf("reset merge check state: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return errPRTransitionSkipped
		}
		return nil
	}
	const syncQ = `UPDATE vanadis_chunks AS c
SET ci_checks = CASE
        WHEN $2 != '' AND LOWER(COALESCE(c.ci_checks->>'_head_sha', '')) = LOWER($2) THEN c.ci_checks - '_merge_sha'
        WHEN $2 != '' THEN jsonb_build_object('_head_sha', $2::text)
        ELSE '{}'::jsonb
    END,
    banner = '',
    status = CASE WHEN c.status IN ('ci_failed', 'conflict') THEN 'pr_opened' ELSE c.status END,
    updated_at = CURRENT_TIMESTAMP
FROM vanadis_builds AS b
WHERE c.id = $1
  AND b.id = c.build_id
  AND c.status NOT IN ('merged', 'synced', 'deploying', 'deployed', 'deploy_failed', 'completed', 'failed', 'cancelled')
  AND COALESCE(c.ci_checks->>'_merge_sha', '') = ''
  AND b.status NOT IN ('completed', 'failed', 'cancelled')
  AND ($3::boolean OR $2 = '' OR COALESCE(c.ci_checks->>'_head_sha', '') = '' OR LOWER(c.ci_checks->>'_head_sha') = LOWER($2));`
	tag, err := s.db.Exec(ctx, syncQ, id, cleanHead, prHeadVerifiedFromContext(ctx))
	if err != nil {
		return fmt.Errorf("reset pr check state: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return s.recoverFailedPR(ctx, id, cleanHead)
	}
	return nil
}

// Failed parents return from reconciliation before its normal widget sync.
func (b *BuildObject) refreshFailedPRWidget(ctx context.Context, buildID, event string) {
	if b.executor == nil || (event != prEventOpened && event != "synchronize") {
		return
	}
	build, chunks, err := b.store.GetBuild(ctx, buildID)
	if err != nil {
		log.Printf("[vanadis-restate] recovery widget read warning: %v", err)
		return
	}
	if build.Status != statusFailed {
		return
	}
	if _, err = b.executor.SyncBuildWidget(ctx, b.store, *build, chunks, "Build failed"); err != nil {
		log.Printf("[vanadis-restate] recovery widget sync warning: %v", err)
	}
}
