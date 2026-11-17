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

const prEventOpened = "opened"

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
	}

	return "", nil
}

// RoutePREvent routes inbound PR lifecycle events to the owning Build Virtual Object.
func (s *BuildRouterService) RoutePREvent(ctx restate.Context, ev pubsub.PRLifecycleEvent) (outErr error) {
	var stepRecorded bool
	defer func() {
		if outErr != nil && !stepRecorded {
			recordReconcileError(labelRoutePREvent, outErr)
		}
	}()

	repo := strings.TrimSpace(ev.Repo)
	if repo == "" || ev.PRNumber <= 0 {
		return restate.ToTerminalError(fmt.Errorf("%w: repo and pr_number required", ErrInvalidBuildInput), restate.WithErrorCode(400))
	}

	buildID, err := runJournaled(ctx, labelRoutePREvent, "lookup-pr-chunk", &stepRecorded, func(rc restate.RunContext) (string, error) {
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
func (s *BuildRouterService) RouteDispatchOutcome(ctx restate.Context, ev pubsub.DispatchOutcomeEvent) (outErr error) {
	var stepRecorded bool
	defer func() {
		if outErr != nil && !stepRecorded {
			recordReconcileError(labelRouteDispatchOutcome, outErr)
		}
	}()

	chunkID := strings.TrimSpace(ev.VanadisChunkID)
	if chunkID == "" {
		return nil
	}

	buildID, err := runJournaled(ctx, labelRouteDispatchOutcome, "lookup-dispatch-chunk", &stepRecorded, func(rc restate.RunContext) (string, error) {
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
func (s *BuildRouterService) RouteAttachPR(ctx restate.Context, req AttachChunkPRRequest) (resp AttachChunkPRResponse, outErr error) {
	var stepRecorded bool
	defer func() {
		if outErr != nil && !stepRecorded {
			recordReconcileError(labelRouteAttachPR, outErr)
		}
	}()

	req.ChunkID = strings.TrimSpace(req.ChunkID)
	req.Repo = strings.TrimSpace(req.Repo)
	if req.PRNumber <= 0 || (req.ChunkID == "" && req.Repo == "") {
		return AttachChunkPRResponse{}, restate.ToTerminalError(fmt.Errorf("%w: chunk_id or repo, and pr_number required", ErrInvalidBuildInput), restate.WithErrorCode(400))
	}

	buildID, err := runJournaled(ctx, labelRouteAttachPR, "resolve-attach-build-id", &stepRecorded, func(rc restate.RunContext) (string, error) {
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
func (s *BuildRouterService) RouteFailChunk(ctx restate.Context, req FailChunkRequest) (resp FailChunkResponse, outErr error) {
	var stepRecorded bool
	defer func() {
		if outErr != nil && !stepRecorded {
			recordReconcileError(labelRouteFailChunk, outErr)
		}
	}()

	chunkID := strings.TrimSpace(req.ChunkID)
	reason := strings.TrimSpace(req.Reason)
	if chunkID == "" || reason == "" {
		return FailChunkResponse{}, restate.ToTerminalError(fmt.Errorf("%w: chunk_id and reason required", ErrInvalidBuildInput), restate.WithErrorCode(400))
	}

	buildID, err := runJournaled(ctx, labelRouteFailChunk, "lookup-fail-chunk-build", &stepRecorded, func(rc restate.RunContext) (string, error) {
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

func recordSeenHead(rev *prRevisionState, sha string) {
	clean := strings.ToLower(strings.TrimSpace(sha))
	if clean == "" {
		return
	}
	if rev.seenHeads == nil {
		rev.seenHeads = make(map[string]bool)
	}
	rev.seenHeads[clean] = true
}

func (b *BuildObject) getChunkRevision(ctx context.Context, buildID, chunkID string) prRevisionState {
	cleanID := strings.TrimSpace(chunkID)
	b.revMu.Lock()
	if b.prRevisions != nil {
		if rev, ok := b.prRevisions[cleanID]; ok {
			b.revMu.Unlock()
			return rev
		}
	}
	b.revMu.Unlock()

	var seeded prRevisionState
	if b.store != nil && strings.TrimSpace(buildID) != "" {
		if _, chunks, err := b.store.GetBuild(ctx, buildID); err == nil {
			for _, c := range chunks {
				if c.ID == cleanID {
					seeded = prRevisionState{
						headSHA:  strings.TrimSpace(c.HeadSHA),
						mergeSHA: strings.TrimSpace(c.MergeSHA),
						merged:   c.Status == statusMerged || c.Status == statusSynced || c.Status == statusDeployFailed || strings.TrimSpace(c.MergeSHA) != "",
						headAt:   c.HeadAt,
					}
					recordSeenHead(&seeded, seeded.headSHA)
					for _, sh := range c.SeenHeads {
						recordSeenHead(&seeded, sh)
					}
					break
				}
			}
		}
	}
	if seeded.headSHA != "" || seeded.mergeSHA != "" || seeded.merged {
		b.setChunkRevision(cleanID, seeded)
	}
	return seeded
}

func (b *BuildObject) setChunkRevision(chunkID string, rev prRevisionState) {
	b.revMu.Lock()
	if b.prRevisions == nil {
		b.prRevisions = make(map[string]prRevisionState)
	}
	b.prRevisions[strings.TrimSpace(chunkID)] = rev
	b.revMu.Unlock()
}

type prHeadAtContextKey struct{}

type prHeadVerifiedContextKey struct{}

func prHeadVerifiedFromContext(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	verified, ok := ctx.Value(prHeadVerifiedContextKey{}).(bool)
	return ok && verified
}

func withPRHeadAt(ctx context.Context, at time.Time) context.Context {
	if at.IsZero() {
		return ctx
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, prHeadAtContextKey{}, at.UTC())
}

func prHeadAtFromContext(ctx context.Context) time.Time {
	if ctx == nil {
		return time.Time{}
	}
	if at, ok := ctx.Value(prHeadAtContextKey{}).(time.Time); ok {
		return at
	}
	return time.Time{}
}

func (b *BuildObject) recordPRHeadRevision(ctx context.Context, buildID, chunkID string, ev pubsub.PRLifecycleEvent) error {
	headSHA := strings.TrimSpace(ev.HeadSHA)
	rev := b.getChunkRevision(ctx, buildID, chunkID)
	if rev.merged {
		return errPRTransitionSkipped
	}
	// Webhook OccurredAt is processing time, not revision order. A delayed
	// delivery must not revive work on a superseded head, including seen SHAs.
	if headSHA != "" && !strings.EqualFold(headSHA, rev.headSHA) {
		if err := b.executor.validatePRHead(ctx, ev.Repo, ev.PRNumber, headSHA); err != nil {
			return err
		}
		ctx = context.WithValue(ctx, prHeadVerifiedContextKey{}, true)
	}
	nextHead := firstNonEmpty(headSHA, rev.headSHA)
	if err := b.store.ResetPRCheckState(withPRHeadAt(ctx, ev.OccurredAt), chunkID, nextHead, ""); err != nil {
		return err
	}
	recordSeenHead(&rev, rev.headSHA)
	recordSeenHead(&rev, nextHead)
	if !ev.OccurredAt.IsZero() {
		rev.headAt = ev.OccurredAt.UTC()
	} else if !strings.EqualFold(nextHead, rev.headSHA) {
		rev.headAt = time.Time{}
	}
	rev.headSHA = nextHead
	rev.mergeSHA = ""
	rev.merged = false
	b.setChunkRevision(chunkID, rev)
	return nil
}

// recordPRMergeRevision follows an accepted merged lifecycle transition.
func (b *BuildObject) recordPRMergeRevision(ctx context.Context, buildID, chunkID, rawHeadSHA, rawMergeSHA string) error {
	headSHA := strings.TrimSpace(rawHeadSHA)
	mergeSHA := strings.TrimSpace(rawMergeSHA)
	rev := b.getChunkRevision(ctx, buildID, chunkID)
	nextHead := firstNonEmpty(headSHA, rev.headSHA)
	nextMerge := firstNonEmpty(mergeSHA, rev.mergeSHA)
	// An absent merge SHA must not enter the pre-merge head-reset path.
	if nextMerge != "" {
		if err := b.store.ResetPRCheckState(ctx, chunkID, nextHead, nextMerge); err != nil {
			// Reload the accepted lifecycle from storage on the next event;
			// the cached pre-merge head is no longer authoritative.
			b.revMu.Lock()
			delete(b.prRevisions, strings.TrimSpace(chunkID))
			b.revMu.Unlock()
			return err
		}
	}
	recordSeenHead(&rev, nextHead)
	rev.headSHA = nextHead
	rev.mergeSHA = nextMerge
	rev.merged = true
	b.setChunkRevision(chunkID, rev)
	return nil
}

func (b *BuildObject) acceptPRCheckRevision(ctx context.Context, buildID, chunkID, rawSHA string) bool {
	sha := strings.TrimSpace(rawSHA)
	rev := b.getChunkRevision(ctx, buildID, chunkID)
	tracked := rev.headSHA
	if rev.merged {
		if rev.mergeSHA == "" && rev.headSHA != "" {
			if sha == "" || strings.EqualFold(sha, rev.headSHA) {
				return false
			}
			return true
		}
		tracked = firstNonEmpty(rev.mergeSHA, rev.headSHA)
	}
	return tracked == "" || (sha != "" && strings.EqualFold(sha, tracked))
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
	if !b.acceptPRCheckRevision(ctx, "", chunkID, ev.HeadSHA) {
		return errPRTransitionSkipped
	}
	if err := b.store.RecordPRCheckStatus(ctx, chunkID, strings.TrimSpace(ev.HeadSHA), checkName, checkState); err != nil {
		return err
	}
	// Only accepted database writes establish an otherwise unknown revision.
	rev := b.getChunkRevision(ctx, "", chunkID)
	sha := strings.TrimSpace(ev.HeadSHA)
	if sha != "" && ((rev.merged && rev.mergeSHA == "") || (!rev.merged && rev.headSHA == "")) {
		if rev.merged {
			rev.mergeSHA = sha
		} else {
			rev.headSHA = sha
			recordSeenHead(&rev, sha)
		}
		b.setChunkRevision(chunkID, rev)
	}
	return nil
}

func (b *BuildObject) handleCIFailureEvent(rc restate.RunContext, buildID, chunkID, event string, ev pubsub.PRLifecycleEvent) error {
	if !b.acceptPRCheckRevision(rc, buildID, chunkID, ev.HeadSHA) {
		return errPRTransitionSkipped
	}
	if err := b.handlePRCheckEvent(rc, chunkID, event, ev); err != nil {
		return err
	}
	if err := b.store.UpdatePRLifecycleStatus(rc, chunkID, "ci_failed"); err != nil {
		return err
	}
	if b.executor != nil {
		details := b.executor.ResolveCIFailureDetails(rc, ev.Repo, ev.PRNumber, ev.CheckName, ev.HeadSHA, ev.CheckURL, ev.Details)
		b.dispatchThreadWakePrompt(rc, buildID, FormatCIFailurePrompt(ev, details))
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
	rev := b.getChunkRevision(rc, buildID, chunkID)
	rev.headSHA = firstNonEmpty(strings.TrimSpace(ev.HeadSHA), rev.headSHA)
	rev.mergeSHA = firstNonEmpty(strings.TrimSpace(ev.MergeSHA), rev.mergeSHA)
	rev.merged = true
	b.setChunkRevision(chunkID, rev)
	if b.executor != nil {
		b.dispatchThreadWakePrompt(rc, buildID, FormatDeployFailurePrompt(ev))
	}
	return nil
}

func (b *BuildObject) handlePREventType(rc restate.RunContext, buildID, chunkID, event string, ev pubsub.PRLifecycleEvent) error {
	_ = b.getChunkRevision(rc, buildID, chunkID)
	switch event {
	case prEventOpened, "synchronize":
		return b.recordPRHeadRevision(rc, buildID, chunkID, ev)
	case "check_updated":
		return b.handlePRCheckEvent(rc, chunkID, event, ev)
	case "check_failed", "ci_failed":
		return b.handleCIFailureEvent(rc, buildID, chunkID, event, ev)
	case "conflict":
		if err := b.store.UpdatePRLifecycleStatus(rc, chunkID, "conflict"); err != nil {
			return err
		}
		if b.executor != nil {
			b.dispatchThreadWakePrompt(rc, buildID, FormatConflictPrompt(ev))
		}
	case "merged":
		if err := b.store.UpdatePRLifecycleStatus(rc, chunkID, "merged"); err != nil {
			return err
		}
		if err := b.recordPRMergeRevision(rc, buildID, chunkID, ev.HeadSHA, ev.MergeSHA); err != nil {
			return err
		}
		if b.executor != nil {
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
func (b *BuildObject) OnPREvent(ctx restate.ObjectContext, ev pubsub.PRLifecycleEvent) (outErr error) {
	var stepRecorded bool
	defer func() {
		if outErr != nil && !stepRecorded {
			recordReconcileError(labelOnPREvent, outErr)
		}
	}()

	if b.store == nil {
		return nil
	}
	buildID := restate.Key(ctx)
	_, err := runJournaled(ctx, labelOnPREvent, "handle-pr-event-postgres", &stepRecorded, func(rc restate.RunContext) (bool, error) {
		chunkID, cErr := b.locateOrAttachPRChunk(rc, buildID, ev)
		if cErr != nil || chunkID == "" {
			return false, cErr
		}
		event := strings.ToLower(strings.TrimSpace(ev.Event))
		if eventErr := b.dispatchPREvent(rc, buildID, chunkID, event, ev); eventErr != nil {
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
			stepRecorded = true
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

func (b *BuildObject) resolveOrAttachPRChunk(rc restate.RunContext, buildID string, ev pubsub.PRLifecycleEvent) (string, error) {
	chunkID := extractChunkIDFromBranch(ev.Repo, ev.Branch)
	if chunkID == "" || b.executor == nil {
		return "", nil
	}

	build, chunks, gErr := b.store.GetBuild(rc, buildID)
	if gErr != nil {
		if errors.Is(gErr, ErrBuildNotFound) {
			return "", nil
		}
		return "", gErr
	}

	var targetChunk *VanadisChunk
	for i := range chunks {
		if chunks[i].ID == chunkID && chunks[i].BuildID == buildID {
			targetChunk = &chunks[i]
			break
		}
	}
	if !isEligibleUnattachedChunk(build, targetChunk, ev) {
		return "", nil
	}
	if vErr := b.executor.validateChunkPRAttachment(rc, *build, *targetChunk, ev); vErr != nil {
		if errors.Is(vErr, errPRTransitionSkipped) {
			return "", nil
		}
		return "", vErr
	}

	_, attachedChunkID, _, aErr := b.store.AttachChunkPR(rc, AttachChunkPRRequest{
		ChunkID:  chunkID,
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
func (b *BuildObject) OnDispatchOutcome(ctx restate.ObjectContext, ev pubsub.DispatchOutcomeEvent) (outErr error) {
	var stepRecorded bool
	defer func() {
		if outErr != nil && !stepRecorded {
			recordReconcileError(labelOnDispatchOutcome, outErr)
		}
	}()
	if b.store == nil {
		return nil
	}
	applied, err := runJournaled(ctx, labelOnDispatchOutcome, "handle-dispatch-outcome-postgres", &stepRecorded, func(rc restate.RunContext) (bool, error) {
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
			stepRecorded = true
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
            THEN jsonb_strip_nulls(COALESCE(ci_checks, '{}'::jsonb) || jsonb_build_object('_head_sha', NULLIF($2::text, ''), '_merge_sha', NULLIF($3::text, '')))
        ELSE jsonb_strip_nulls(jsonb_build_object('_head_sha', NULLIF($2::text, ''), '_merge_sha', NULLIF($3::text, '')))
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
	var headAtNano int64
	if headAt := prHeadAtFromContext(ctx); !headAt.IsZero() {
		headAtNano = headAt.UTC().UnixNano()
	}
	const syncQ = `UPDATE vanadis_chunks AS c
SET ci_fail_count = CASE WHEN LOWER(COALESCE(c.ci_checks->>'_head_sha', '')) IS DISTINCT FROM LOWER($2) THEN 0 ELSE c.ci_fail_count END,
    ci_checks = CASE
        WHEN $2 != '' AND LOWER(COALESCE(c.ci_checks->>'_head_sha', '')) = LOWER($2) THEN
            CASE
                WHEN $3::bigint > 0 THEN jsonb_set(c.ci_checks - '_merge_sha', '{_head_at}', to_jsonb($3::text), true)
                ELSE c.ci_checks - '_merge_sha'
            END
        WHEN $2 != '' THEN jsonb_strip_nulls(jsonb_build_object(
            '_head_sha', $2::text,
            '_head_at', CASE WHEN $3::bigint > 0 THEN $3::text ELSE NULL END,
            '_seen_heads', NULLIF(CONCAT_WS(',', NULLIF(c.ci_checks->>'_seen_heads', ''), NULLIF(c.ci_checks->>'_head_sha', ''), $2::text), '')
        ))
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
  AND ($4::boolean OR (NOT (
      $3::bigint > 0
      AND COALESCE(c.ci_checks->>'_head_at', '') ~ '^[0-9]+$'
      AND $3::bigint < (c.ci_checks->>'_head_at')::bigint
  )
  AND (
      $2 = ''
      OR COALESCE(c.ci_checks->>'_head_sha', '') = ''
      OR LOWER(c.ci_checks->>'_head_sha') = LOWER($2)
      OR (
          $3::bigint > 0
          AND COALESCE(c.ci_checks->>'_head_at', '') ~ '^[0-9]+$'
          AND $3::bigint > (c.ci_checks->>'_head_at')::bigint
      )
      OR NOT (LOWER($2) = ANY(string_to_array(LOWER(COALESCE(c.ci_checks->>'_seen_heads', '')), ',')))
  )));`
	tag, err := s.db.Exec(ctx, syncQ, id, cleanHead, headAtNano, prHeadVerifiedFromContext(ctx))
	if err != nil {
		return fmt.Errorf("reset pr check state: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return s.recoverFailedPR(ctx, id, cleanHead, headAtNano)
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
