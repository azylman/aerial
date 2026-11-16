package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/server"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/pubsub"
)

var reconcileErrorsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "vanadis_restate_reconcile_errors_total",
		Help: "Total number of Vanadis Restate handler and reconcile transition errors.",
	},
	[]string{"handler"},
)

func init() {
	if err := prometheus.Register(reconcileErrorsTotal); err != nil {
		if _, ok := errors.AsType[prometheus.AlreadyRegisteredError](err); !ok {
			panic(fmt.Sprintf("failed to register vanadis_restate_reconcile_errors_total: %v", err))
		}
	}
	for _, h := range []string{"reconcile-build", "mutate-build", "reconcile-initial", "RoutePREvent", "RouteDispatchOutcome"} {
		reconcileErrorsTotal.WithLabelValues(h).Add(0)
	}
}

func recordReconcileError(handler string, err error) {
	if err != nil {
		reconcileErrorsTotal.WithLabelValues(handler).Inc()
	}
}

var newBusFn = func(ctx context.Context, url string) (pubsub.Bus, error) {
	return pubsub.NewJetStreamBus(ctx, url, nats.RetryOnFailedConnect(false), nats.Timeout(5*time.Second))
}

const (
	statusCompleted = "completed"
	statusFailed    = "failed"
	statusCancelled = "cancelled"
	statusNotFound  = "not_found"
)

// Config holds runtime configuration for the vanadis-restate worker service.
type Config struct {
	RestatePort string
	HealthPort  string
	VanadisURL  string
	PostgresURL string
	EriURL      string
	NATSURL     string
	GitHubToken string
	ShareRoot   string
}

// LoadConfigFromEnv loads configuration from environment variables with defaults.
func LoadConfigFromEnv() Config {
	restatePort := strings.TrimSpace(os.Getenv("RESTATE_PORT"))
	if restatePort == "" {
		restatePort = strings.TrimSpace(os.Getenv("PORT"))
	}
	if restatePort == "" {
		restatePort = "9080"
	}

	healthPort := strings.TrimSpace(os.Getenv("HEALTH_PORT"))
	if healthPort == "" {
		healthPort = "4026"
	}

	vanadisURL := strings.TrimRight(strings.TrimSpace(os.Getenv("VANADIS_URL")), "/")
	if vanadisURL == "" {
		vanadisURL = "http://vanadis.aerial:4025"
	}

	pgURL := strings.TrimSpace(firstNonEmpty(os.Getenv("DATABASE_URL"), os.Getenv("POSTGRES_URL"), httpkit.BuildPostgresDSN(os.Getenv, "aerial")))
	eriURL := strings.TrimRight(strings.TrimSpace(firstNonEmpty(os.Getenv("ERI_URL"), "http://eri.aerial")), "/")
	natsURL := strings.TrimSpace(firstNonEmpty(os.Getenv("NATS_URL"), "nats://nats.aerial:4222"))
	ghToken := strings.TrimSpace(firstNonEmpty(os.Getenv("GITHUB_TOKEN"), os.Getenv("GITHUB_PAT")))
	shareRoot := strings.TrimSpace(firstNonEmpty(os.Getenv("SHARE_ROOT"), defaultShareRoot))

	return Config{
		RestatePort: restatePort,
		HealthPort:  healthPort,
		VanadisURL:  vanadisURL,
		PostgresURL: pgURL,
		EriURL:      eriURL,
		NATSURL:     natsURL,
		GitHubToken: ghToken,
		ShareRoot:   shareRoot,
	}
}

// ReconcileBuildRequest is the input payload for the Restate Build Virtual Object.
type ReconcileBuildRequest struct {
	Reason string `json:"reason,omitempty"`
}

// ReconcileBuildResult is the output of the Restate Build Virtual Object.
type ReconcileBuildResult struct {
	BuildID        string   `json:"build_id"`
	Status         string   `json:"status"`
	LaunchedChunks []string `json:"launched_chunks,omitempty"`
	Error          string   `json:"error,omitempty"`
}

// CancelBuildRequest represents a cancel request payload.
type CancelBuildRequest struct {
	Reason string `json:"reason,omitempty"`
}

// RemoveChunkRequest represents a request to remove a chunk from a build.
type RemoveChunkRequest struct {
	ChunkRef string `json:"chunk_ref"`
}

// VanadisBuild represents a multi-chunk engineering build run.
type VanadisBuild struct {
	ID              string    `json:"id"`
	ThreadID        string    `json:"thread_id"`
	Tier            int       `json:"tier"`
	Title           string    `json:"title"`
	Objective       string    `json:"objective"`
	SharedContext   string    `json:"shared_context,omitempty"`
	Status          string    `json:"status"`
	StatusMessageID string    `json:"status_message_id,omitempty"`
	Quiet           bool      `json:"quiet"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

// VanadisChunk represents an individual ordered task chunk within a VanadisBuild.
type VanadisChunk struct {
	ID                 string                 `json:"id"`
	BuildID            string                 `json:"build_id"`
	ChunkIndex         int                    `json:"chunk_index"`
	Mode               string                 `json:"mode"`
	Repo               string                 `json:"repo,omitempty"`
	Title              string                 `json:"title"`
	Instructions       string                 `json:"instructions"`
	TargetFiles        []string               `json:"target_files"`
	TargetSymbols      []string               `json:"target_symbols"`
	Effort             string                 `json:"effort"`
	AcceptanceCriteria []string               `json:"acceptance_criteria"`
	Status             string                 `json:"status"`
	PRNumber           int                    `json:"pr_number,omitempty"`
	StallNudges        int                    `json:"stall_nudges,omitempty"`
	DependsOn          []string               `json:"depends_on"`
	CIFailCount        int                    `json:"ci_fail_count"`
	CIChecks           map[string]string      `json:"ci_checks,omitempty"`
	RolloutState       *pubsub.PRRolloutState `json:"rollout_state,omitempty"`
	Banner             string                 `json:"banner,omitempty"`
	MergeAwakeableID   string                 `json:"merge_awakeable_id,omitempty"`
	DeployAwakeableID  string                 `json:"deploy_awakeable_id,omitempty"`
	StartedAt          time.Time              `json:"started_at,omitempty"`
	CompletedAt        time.Time              `json:"completed_at,omitempty"`
	CreatedAt          time.Time              `json:"created_at"`
	UpdatedAt          time.Time              `json:"updated_at"`
}

// BuildObjectState represents the authoritative K/V state for a Vanadis build managed by BuildObject.
type BuildObjectState struct {
	TerminalNotified bool            `json:"terminal_notified,omitempty"`
	Build            VanadisBuild    `json:"build"`
	Chunks           []VanadisChunk  `json:"chunks"`
	Launched         map[string]bool `json:"launched,omitempty"`
}

// BuildMutationRequest represents a typed mutation payload targeting BuildObject.
type BuildMutationRequest struct {
	DispatchID   string                 `json:"dispatch_id,omitempty"`
	RolloutState *pubsub.PRRolloutState `json:"rollout_state,omitempty"`
	Action       string                 `json:"action"`
	ChunkID      string                 `json:"chunk_id,omitempty"`
	Reason       string                 `json:"reason,omitempty"`
}

// RestateTransitionRequest represents the payload sent to the internal Vanadis transition endpoint.
type RestateTransitionRequest struct {
	Mutation      *BuildMutationRequest `json:"mutation,omitempty"`
	PriorLaunched map[string]bool       `json:"prior_launched,omitempty"`
}

type reconcileStepOutput struct {
	BuildStatus    string            `json:"build_status"`
	LaunchedChunks []string          `json:"launched_chunks,omitempty"`
	LaunchedMap    map[string]bool   `json:"launched_map,omitempty"`
	Error          string            `json:"error,omitempty"`
	State          *BuildObjectState `json:"state,omitempty"`
}

type objectRuntime interface {
	Key() string
	GetLaunched() (map[string]bool, error)
	SetLaunched(launched map[string]bool)
	GetState() (*BuildObjectState, error)
	SetState(state BuildObjectState)
	ClearState()
	ScheduleReconcile()
	RunReconcileStep(name string, fn func(ctx context.Context) (reconcileStepOutput, error)) (reconcileStepOutput, error)
}

type restateObjectRuntime struct {
	ctx restate.ObjectContext
}

func (r restateObjectRuntime) Key() string {
	return restate.Key(r.ctx)
}

func (r restateObjectRuntime) GetLaunched() (map[string]bool, error) {
	return restate.Get[map[string]bool](r.ctx, "launched")
}

func (r restateObjectRuntime) SetLaunched(launched map[string]bool) {
	if launched == nil {
		launched = make(map[string]bool)
	}
	restate.Set(r.ctx, "launched", launched)
}

func (r restateObjectRuntime) GetState() (*BuildObjectState, error) {
	return restate.Get[*BuildObjectState](r.ctx, "state")
}

func (r restateObjectRuntime) SetState(state BuildObjectState) {
	restate.Set(r.ctx, "state", &state)
}

func (r restateObjectRuntime) ClearState() {
	restate.ClearAll(r.ctx)
}

func (r restateObjectRuntime) RunReconcileStep(name string, fn func(ctx context.Context) (reconcileStepOutput, error)) (reconcileStepOutput, error) {
	return restate.Run(r.ctx, func(rc restate.RunContext) (reconcileStepOutput, error) {
		return fn(rc)
	}, restate.WithName(name))
}

// A delayed self-send releases the exclusive invocation between active-dispatch
// probes, allowing completion and cancellation handlers to acquire the object.
func (r restateObjectRuntime) ScheduleReconcile() {
	_ = restate.ObjectSend(r.ctx, "Build", restate.Key(r.ctx), "Reconcile").Send(ReconcileBuildRequest{Reason: "active dispatch recovery"}, restate.WithDelay(time.Second))
}

// BuildObject implements the Restate-authoritative Virtual Object for multi-chunk builds.
type BuildObject struct {
	store          VanadisStore
	executor       *Executor
	vanadisURL     string
	httpClient     *http.Client
	runtimeFactory func(restate.ObjectContext) objectRuntime
}

// NewBuildObject creates a new BuildObject bound to vanadisURL with an optional HTTP client.
func NewBuildObject(vanadisURL string, httpClient *http.Client, runtimeFactory ...func(restate.ObjectContext) objectRuntime) *BuildObject {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	var rf func(restate.ObjectContext) objectRuntime
	if len(runtimeFactory) > 0 {
		rf = runtimeFactory[0]
	}
	return &BuildObject{
		vanadisURL:     strings.TrimRight(strings.TrimSpace(vanadisURL), "/"),
		httpClient:     httpClient,
		runtimeFactory: rf,
	}
}

// WithAuthority wires the PostgreSQL store and side-effect executor into BuildObject.
func (b *BuildObject) WithAuthority(store VanadisStore, exec *Executor) *BuildObject {
	if pg, ok := store.(*PostgresVanadisStore); ok && pg == nil {
		store = nil
	}
	b.store = store
	b.executor = exec
	return b
}

// ServiceName returns the Restate service name for this Virtual Object.
func (*BuildObject) ServiceName() string {
	return "Build"
}

func (b *BuildObject) notifyTransitionError(ctx context.Context, buildID, stepName string, priorState *BuildObjectState, transErr error) {
	if b == nil || b.executor == nil || transErr == nil || strings.TrimSpace(buildID) == "" {
		return
	}
	var build *VanadisBuild
	var chunks []VanadisChunk
	if b.store != nil {
		if bld, chks, err := b.store.GetBuild(ctx, buildID); err == nil && bld != nil {
			build = bld
			chunks = chks
		}
	}
	if build == nil && priorState != nil && strings.TrimSpace(priorState.Build.ID) != "" {
		build = &priorState.Build
		chunks = priorState.Chunks
	}
	if build == nil || strings.TrimSpace(build.ID) == "" || build.Quiet {
		return
	}
	if _, err := b.executor.SyncBuildWidget(ctx, b.store, *build, chunks, fmt.Sprintf("⚠️ Reconcile backing off (%s): %v", stepName, transErr)); err != nil {
		log.Printf("[vanadis-restate] sync widget error on transition failure: %v", err)
	}
}

func (b *BuildObject) getRuntime(ctx restate.ObjectContext) objectRuntime {
	if b != nil && b.runtimeFactory != nil {
		return b.runtimeFactory(ctx)
	}
	return restateObjectRuntime{ctx: ctx}
}

func toTerminalErr(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, ErrInvalidBuildInput):
		return restate.ToTerminalError(err, restate.WithErrorCode(400))
	case errors.Is(err, ErrBuildNotFound) || errors.Is(err, ErrChunkNotFound):
		return restate.ToTerminalError(err, restate.WithErrorCode(404))
	case errors.Is(err, ErrBuildConflict):
		return restate.ToTerminalError(err, restate.WithErrorCode(409))
	default:
		return err
	}
}

func planInputsToChunks(buildID string, inputs []CreateChunkInput) []VanadisChunk {
	chunks := make([]VanadisChunk, len(inputs))
	now := time.Now().UTC()
	for i, in := range inputs {
		cid := strings.TrimSpace(in.ID)
		if cid == "" {
			cid = fmt.Sprintf("%s-chunk-%d", buildID, i+1)
		}
		mode := strings.TrimSpace(in.Mode)
		if mode == "" {
			mode = "pr"
		}
		chunks[i] = VanadisChunk{
			ID:                 cid,
			BuildID:            buildID,
			ChunkIndex:         i,
			Mode:               mode,
			Repo:               cleanRepoName(in.Repo),
			Title:              strings.TrimSpace(in.Title),
			Instructions:       strings.TrimSpace(in.Instructions),
			TargetFiles:        in.TargetFiles,
			TargetSymbols:      in.TargetSymbols,
			Effort:             in.Effort,
			AcceptanceCriteria: in.AcceptanceCriteria,
			Status:             "pending",
			DependsOn:          in.DependsOn,
			CreatedAt:          now,
			UpdatedAt:          now,
		}
	}
	return chunks
}

func (b *BuildObject) persistBuildCreation(rc restate.RunContext, build VanadisBuild, chunks []VanadisChunk) (bool, error) {
	if err := b.store.CreateBuild(rc, build, chunks); err != nil {
		return false, err
	}
	if b.executor != nil && !build.Quiet {
		if _, sErr := b.executor.SyncBuildWidget(rc, b.store, build, chunks, ""); sErr != nil {
			log.Printf("[vanadis-restate] sync widget warning: %v", sErr)
		}
	}
	return true, nil
}

// Create handles initial build and DAG initialization.
func (b *BuildObject) Create(ctx restate.ObjectContext, req CreateBuildRequest) (resp VanadisBuild, outErr error) {
	defer func() {
		if outErr != nil {
			recordReconcileError("Create", outErr)
		}
	}()

	buildID := strings.TrimSpace(restate.Key(ctx))
	if buildID == "" {
		buildID = strings.TrimSpace(req.ID)
	}
	if buildID == "" {
		return VanadisBuild{}, restate.ToTerminalError(fmt.Errorf("%w: build id cannot be empty", ErrInvalidBuildInput), restate.WithErrorCode(400))
	}
	threadID := strings.TrimSpace(req.ThreadID)
	objective := strings.TrimSpace(req.Objective)
	if threadID == "" || objective == "" {
		return VanadisBuild{}, restate.ToTerminalError(fmt.Errorf("%w: thread_id and objective cannot be empty", ErrInvalidBuildInput), restate.WithErrorCode(400))
	}

	chunks := planInputsToChunks(buildID, req.Chunks)
	if err := validateCreateBuildInputs(buildID, threadID, objective, chunks); err != nil {
		return VanadisBuild{}, toTerminalErr(err)
	}
	validatedChunks, err := normalizeAndValidateBuildDAG(chunks)
	if err != nil {
		return VanadisBuild{}, toTerminalErr(err)
	}

	title := strings.TrimSpace(req.Title)
	if title == "" {
		title = objective
	}
	now := time.Now().UTC()
	build := VanadisBuild{
		ID:            buildID,
		ThreadID:      threadID,
		Tier:          req.Tier,
		Title:         title,
		Objective:     objective,
		SharedContext: req.SharedContext,
		Status:        statusRunning,
		Quiet:         req.Quiet,
		CreatedAt:     now,
		UpdatedAt:     now,
	}

	if b.store != nil {
		_, err := restate.Run(ctx, func(rc restate.RunContext) (bool, error) {
			return b.persistBuildCreation(rc, build, validatedChunks)
		}, restate.WithName("create-build-postgres"))
		if err != nil {
			return VanadisBuild{}, toTerminalErr(err)
		}
	}

	rt := b.getRuntime(ctx)
	rt.SetState(BuildObjectState{Build: build, Chunks: validatedChunks})
	rt.SetLaunched(make(map[string]bool))
	if _, tErr := b.executeObjectTransition(rt, "reconcile-initial", nil); tErr != nil {
		log.Printf("[vanadis-restate] initial transition warning: %v", tErr)
	}
	return build, nil
}

// UpdateBuild mutates build metadata and updates state.
func (b *BuildObject) UpdateBuild(ctx restate.ObjectContext, req UpdateBuildRequest) (resp VanadisBuild, outErr error) {
	defer func() {
		if outErr != nil {
			recordReconcileError("UpdateBuild", outErr)
		}
	}()

	buildID := restate.Key(ctx)
	if b.store == nil {
		return VanadisBuild{ID: buildID, Title: req.Title, Objective: req.Objective}, nil
	}
	res, err := restate.Run(ctx, func(rc restate.RunContext) (VanadisBuild, error) {
		updated, err := b.store.UpdateBuild(rc, buildID, req)
		if err != nil {
			return VanadisBuild{}, err
		}
		if b.executor != nil {
			_, chunks, gErr := b.store.GetBuild(rc, buildID)
			if gErr == nil {
				if _, sErr := b.executor.SyncBuildWidget(rc, b.store, *updated, chunks, "Build updated: "+updated.Title); sErr != nil {
					log.Printf("[vanadis-restate] sync widget warning: %v", sErr)
				}
			}
		}
		return *updated, nil
	}, restate.WithName("update-build-postgres"))
	if err != nil {
		return VanadisBuild{}, toTerminalErr(err)
	}
	rt := b.getRuntime(ctx)
	if st, gErr := rt.GetState(); gErr == nil && st != nil {
		st.Build = res
		rt.SetState(*st)
	}
	return res, nil
}

// Cancel cancels the active build and clears Restate state.
func (b *BuildObject) Cancel(ctx restate.ObjectContext, req CancelBuildRequest) (resp VanadisBuild, outErr error) {
	defer func() {
		if outErr != nil {
			recordReconcileError("Cancel", outErr)
		}
	}()

	buildID := restate.Key(ctx)
	rt := b.getRuntime(ctx)
	defer rt.ClearState()

	if b.store == nil {
		return VanadisBuild{ID: buildID, Status: statusCancelled}, nil
	}

	res, err := restate.Run(ctx, func(rc restate.RunContext) (VanadisBuild, error) {
		bld, chunks, err := b.store.GetBuild(rc, buildID)
		if err != nil {
			return VanadisBuild{}, err
		}
		if bld.Status == statusCompleted || bld.Status == statusFailed {
			return *bld, fmt.Errorf("%w: cannot cancel terminal build %s", ErrBuildConflict, bld.Status)
		}
		if err := b.store.UpdateBuildStatus(rc, buildID, statusCancelled); err != nil {
			return VanadisBuild{}, err
		}
		bld.Status = statusCancelled
		if b.executor != nil {
			for _, c := range chunks {
				if cErr := b.executor.CancelChunkDispatch(rc, c.ID); cErr != nil {
					log.Printf("[vanadis-restate] cancel chunk dispatch %s: %v", c.ID, cErr)
				}
			}
			if _, sErr := b.executor.SyncBuildWidget(rc, b.store, *bld, chunks, "Build cancelled"); sErr != nil {
				log.Printf("[vanadis-restate] sync widget warning: %v", sErr)
			}
			if pErr := b.executor.PublishExternalEvent(rc, "vanadis_build", "cancelled", []string{buildID}, req.Reason); pErr != nil {
				log.Printf("[vanadis-restate] publish event warning: %v", pErr)
			}
		}
		return *bld, nil
	}, restate.WithName("cancel-build-postgres"))
	if err != nil {
		return VanadisBuild{}, toTerminalErr(err)
	}
	return res, nil
}

// AppendChunks appends chunks to a running build and advances the DAG.
func (b *BuildObject) AppendChunks(ctx restate.ObjectContext, req AppendChunksRequest) (resp []VanadisChunk, outErr error) {
	defer func() {
		if outErr != nil {
			recordReconcileError("AppendChunks", outErr)
		}
	}()

	if len(req.Chunks) == 0 {
		return nil, restate.ToTerminalError(fmt.Errorf("%w: chunks cannot be empty", ErrInvalidBuildInput), restate.WithErrorCode(http.StatusBadRequest))
	}
	if b.store == nil {
		return nil, nil
	}
	return b.executeChunkAppend(ctx, restate.Key(ctx), req.Chunks)
}

func (b *BuildObject) executeChunkAppend(ctx restate.ObjectContext, buildID string, chunks []CreateChunkInput) ([]VanadisChunk, error) {
	appended, err := restate.Run(ctx, func(rc restate.RunContext) ([]VanadisChunk, error) {
		return b.store.AppendChunks(rc, buildID, chunks)
	}, restate.WithName("append-chunks-postgres"))
	if err != nil {
		return nil, toTerminalErr(err)
	}
	if _, tErr := b.executeObjectTransition(b.getRuntime(ctx), "reconcile-after-append", nil); tErr != nil {
		log.Printf("[vanadis-restate] append chunks transition warning: %v", tErr)
	}
	return appended, nil
}

// UpdateChunk modifies a pending chunk and revalidates dependencies.
func (b *BuildObject) UpdateChunk(ctx restate.ObjectContext, req UpdateChunkRequest) (resp VanadisChunk, outErr error) {
	defer func() {
		if outErr != nil {
			recordReconcileError("UpdateChunk", outErr)
		}
	}()

	buildID := restate.Key(ctx)
	chunkRef := strings.TrimSpace(req.ChunkRef)
	if chunkRef == "" {
		return VanadisChunk{}, restate.ToTerminalError(fmt.Errorf("%w: chunk_ref cannot be empty", ErrInvalidBuildInput), restate.WithErrorCode(400))
	}
	if b.store == nil {
		return VanadisChunk{}, nil
	}
	updated, err := restate.Run(ctx, func(rc restate.RunContext) (VanadisChunk, error) {
		c, err := b.store.UpdateChunk(rc, buildID, chunkRef, req)
		if err != nil {
			return VanadisChunk{}, err
		}
		return *c, nil
	}, restate.WithName("update-chunk-postgres"))
	if err != nil {
		return VanadisChunk{}, toTerminalErr(err)
	}
	rt := b.getRuntime(ctx)
	if _, tErr := b.executeObjectTransition(rt, "reconcile-after-update-chunk", nil); tErr != nil {
		log.Printf("[vanadis-restate] update chunk transition warning: %v", tErr)
	}
	return updated, nil
}

// RemoveChunk removes an unstarted chunk from the build DAG.
func (b *BuildObject) RemoveChunk(ctx restate.ObjectContext, req RemoveChunkRequest) (outErr error) {
	defer func() {
		if outErr != nil {
			recordReconcileError("RemoveChunk", outErr)
		}
	}()

	buildID := restate.Key(ctx)
	targetRef := strings.TrimSpace(req.ChunkRef)
	if targetRef == "" {
		return restate.ToTerminalError(fmt.Errorf("%w: chunk_ref cannot be empty", ErrInvalidBuildInput), restate.WithErrorCode(400))
	}
	if b.store == nil {
		return nil
	}
	rt := b.getRuntime(ctx)
	return b.executeChunkRemoval(ctx, rt, buildID, targetRef)
}

func (b *BuildObject) executeChunkRemoval(ctx restate.ObjectContext, rt objectRuntime, buildID, targetRef string) error {
	_, err := restate.Run(ctx, func(rc restate.RunContext) (bool, error) {
		removedChunk, _, rErr := b.store.RemoveChunk(rc, buildID, targetRef)
		if rErr != nil {
			return false, rErr
		}
		if b.executor != nil && removedChunk != nil {
			if cErr := b.executor.CancelChunkDispatch(rc, removedChunk.ID); cErr != nil {
				log.Printf("[vanadis-restate] cancel chunk dispatch %s: %v", removedChunk.ID, cErr)
			}
		}
		return true, nil
	}, restate.WithName("remove-chunk-postgres"))
	if err != nil {
		return toTerminalErr(err)
	}
	if _, tErr := b.executeObjectTransition(rt, "reconcile-after-remove-chunk", nil); tErr != nil {
		log.Printf("[vanadis-restate] remove chunk transition warning: %v", tErr)
	}
	return nil
}

func (b *BuildObject) notifyChunkFailure(rc restate.RunContext, bld VanadisBuild, chunkID, reason string) {
	if cErr := b.executor.CancelChunkDispatch(rc, chunkID); cErr != nil {
		log.Printf("[vanadis-restate] cancel chunk dispatch %s: %v", chunkID, cErr)
	}
	prompt := fmt.Sprintf("[VANADIS BUILD FAILED]\nBuild ID: %s\nObjective: %s\n\nChunk failed: %s", bld.ID, bld.Objective, reason)
	if dErr := b.executor.DispatchWakePrompt(rc, bld.ThreadID, prompt); dErr != nil {
		log.Printf("[vanadis-restate] dispatch wake prompt warning: %v", dErr)
	}
	if chunks, _, gErr := b.store.GetBuild(rc, bld.ID); gErr == nil && chunks != nil {
		if _, sErr := b.executor.SyncBuildWidget(rc, b.store, bld, nil, "Build failed: "+reason); sErr != nil {
			log.Printf("[vanadis-restate] sync widget warning: %v", sErr)
		}
	}
	if pErr := b.executor.PublishExternalEvent(rc, "vanadis_chunk", "failed", []string{chunkID}, reason); pErr != nil {
		log.Printf("[vanadis-restate] publish event warning: %v", pErr)
	}
	if pErr := b.executor.PublishExternalEvent(rc, "vanadis_build", "failed", []string{bld.ID}, reason); pErr != nil {
		log.Printf("[vanadis-restate] publish event warning: %v", pErr)
	}
}

// FailChunk marks a chunk failed and cancels active work if the failure is terminal.
func (b *BuildObject) FailChunk(ctx restate.ObjectContext, req FailChunkRequest) (resp FailChunkResponse, outErr error) {
	defer func() {
		if outErr != nil {
			recordReconcileError("FailChunk", outErr)
		}
	}()

	chunkID := strings.TrimSpace(req.ChunkID)
	reason := strings.TrimSpace(req.Reason)
	if chunkID == "" || reason == "" {
		return FailChunkResponse{}, restate.ToTerminalError(fmt.Errorf("%w: chunk_id and reason required", ErrInvalidBuildInput), restate.WithErrorCode(400))
	}
	if b.store == nil {
		return FailChunkResponse{ChunkID: chunkID, Failed: true}, nil
	}

	res, err := restate.Run(ctx, func(rc restate.RunContext) (FailChunkResponse, error) {
		if err := b.validateActionBuild(rc, restate.Key(ctx), chunkID); err != nil {
			return FailChunkResponse{}, err
		}
		bld, chk, failed, err := b.store.FailChunk(rc, chunkID, reason, req.DispatchID)
		if err != nil {
			return FailChunkResponse{}, err
		}
		r := FailChunkResponse{
			Failed:      failed,
			BuildID:     chk.BuildID,
			ChunkID:     chk.ID,
			BuildStatus: bld.Status,
			ChunkStatus: chk.Status,
		}
		if failed && bld.Status == statusFailed && b.executor != nil {
			b.notifyChunkFailure(rc, *bld, chunkID, reason)
		}
		return r, nil
	}, restate.WithName("fail-chunk-postgres"))
	if err != nil {
		return FailChunkResponse{}, toTerminalErr(err)
	}
	if res.Failed && res.BuildStatus == statusFailed {
		rt := b.getRuntime(ctx)
		rt.ClearState()
	}
	return res, nil
}

// AttachPR attaches a PR to a chunk or standalone build.
func (b *BuildObject) AttachPR(ctx restate.ObjectContext, req AttachChunkPRRequest) (resp AttachChunkPRResponse, outErr error) {
	defer func() {
		if outErr != nil {
			recordReconcileError("AttachPR", outErr)
		}
	}()

	store := b.store
	if store == nil {
		return AttachChunkPRResponse{Status: "attached", BuildID: restate.Key(ctx), ChunkID: req.ChunkID}, nil
	}
	return b.executePRAttachment(ctx, req)
}

func (b *BuildObject) executePRAttachment(ctx restate.ObjectContext, req AttachChunkPRRequest) (AttachChunkPRResponse, error) {
	store := b.store
	attached, attachErr := restate.Run(ctx, func(rc restate.RunContext) (AttachChunkPRResponse, error) {
		bID, cID, thID, err := store.AttachChunkPR(rc, req)
		if err != nil {
			return AttachChunkPRResponse{}, err
		}
		if exec := b.executor; exec != nil {
			if bld, chunks, gErr := store.GetBuild(rc, bID); gErr == nil && bld != nil {
				if _, sErr := exec.SyncBuildWidget(rc, store, *bld, chunks, ""); sErr != nil {
					log.Printf("[vanadis-restate] sync widget error on attach pr: %v", sErr)
				}
			}
		}
		return AttachChunkPRResponse{
			Status:   "attached",
			BuildID:  bID,
			ChunkID:  cID,
			ThreadID: thID,
		}, nil
	}, restate.WithName("attach-pr-postgres"))
	if attachErr != nil {
		return AttachChunkPRResponse{}, toTerminalErr(attachErr)
	}

	runtime := b.getRuntime(ctx)
	if _, tErr := b.executeObjectTransition(runtime, "reconcile-after-attach-pr", nil); tErr != nil {
		log.Printf("[vanadis-restate] attach pr transition warning: %v", tErr)
	}
	return attached, nil
}

func (b *BuildObject) dispatchThreadWakePrompt(rc restate.RunContext, buildID, prompt string) {
	if b.executor == nil {
		return
	}
	if bld, _, gErr := b.store.GetBuild(rc, buildID); gErr == nil && bld != nil {
		if dErr := b.executor.DispatchWakePrompt(rc, bld.ThreadID, prompt); dErr != nil {
			log.Printf("[vanadis-restate] wake prompt warning: %v", dErr)
		}
	}
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
	return b.store.RecordPRCheckStatus(ctx, chunkID, checkName, checkState)
}

func (b *BuildObject) handlePREventType(rc restate.RunContext, buildID, chunkID, event string, ev pubsub.PRLifecycleEvent) error {
	switch event {
	case "check_updated":
		return b.handlePRCheckEvent(rc, chunkID, event, ev)
	case "check_failed", "ci_failed":
		if err := b.handlePRCheckEvent(rc, chunkID, event, ev); err != nil {
			log.Printf("[vanadis-restate] record check status warning: %v", err)
		}
		if err := b.store.UpdatePRLifecycleStatus(rc, chunkID, "ci_failed"); err != nil {
			return err
		}
		if b.executor != nil {
			details := b.executor.ResolveCIFailureDetails(rc, ev.Repo, ev.PRNumber, ev.CheckName, ev.HeadSHA, ev.CheckURL, ev.Details)
			b.dispatchThreadWakePrompt(rc, buildID, FormatCIFailurePrompt(ev, details))
		}
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
		if b.executor != nil {
			if pErr := b.executor.PublishExternalEvent(rc, "vanadis_chunk", "merged", []string{chunkID}, ""); pErr != nil {
				log.Printf("[vanadis-restate] publish event warning: %v", pErr)
			}
		}
	case "closed":
		if ev.Status != "merged" {
			if _, _, _, fErr := b.store.FailChunk(rc, chunkID, "pr_closed_unmerged", ""); fErr != nil {
				log.Printf("[vanadis-restate] fail chunk warning: %v", fErr)
			}
		}
	}
	return nil
}

// OnPREvent receives PR lifecycle events for this build.
func (b *BuildObject) OnPREvent(ctx restate.ObjectContext, ev pubsub.PRLifecycleEvent) (outErr error) {
	defer func() {
		if outErr != nil {
			recordReconcileError("OnPREvent", outErr)
		}
	}()

	if b.store == nil {
		return nil
	}
	buildID := restate.Key(ctx)
	_, err := restate.Run(ctx, func(rc restate.RunContext) (bool, error) {
		lookup, err := b.store.GetChunkByPR(rc, ev.Repo, ev.PRNumber)
		if err != nil {
			return false, err
		}
		if lookup == nil || !lookup.Matched {
			return false, nil
		}
		chunkID := lookup.ChunkID
		event := strings.ToLower(strings.TrimSpace(ev.Event))

		var eventErr error
		if event == "deploy_success" {
			eventErr = b.applyMutation(rc, buildID, &BuildMutationRequest{Action: mutationChunkDeploySuccess, ChunkID: chunkID, RolloutState: ev.RolloutState})
		} else {
			eventErr = b.handlePREventType(rc, buildID, chunkID, event, ev)
		}
		if errors.Is(eventErr, errPRTransitionSkipped) {
			return false, nil
		}
		if eventErr != nil {
			return false, eventErr
		}

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
	}, restate.WithName("handle-pr-event-postgres"))
	if err != nil {
		return err
	}
	rt := b.getRuntime(ctx)
	if _, tErr := b.executeObjectTransition(rt, "reconcile-after-pr-event", nil); tErr != nil {
		if strings.EqualFold(strings.TrimSpace(ev.Event), "deploy_success") {
			return tErr
		}
		log.Printf("[vanadis-restate] pr event transition warning: %v", tErr)
	}
	return nil
}

// OnDispatchOutcome receives task execution outcomes.
func (b *BuildObject) OnDispatchOutcome(ctx restate.ObjectContext, ev pubsub.DispatchOutcomeEvent) (outErr error) {
	defer func() {
		if outErr != nil {
			recordReconcileError("OnDispatchOutcome", outErr)
		}
	}()
	if b.store == nil {
		return nil
	}
	applied, err := restate.Run(ctx, func(rc restate.RunContext) (bool, error) {
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
	}, restate.WithName("handle-dispatch-outcome-postgres"))
	if err != nil {
		return err
	}
	if !applied {
		return nil
	}
	rt := b.getRuntime(ctx)
	mutation := dispatchCompletionMutation(ev)
	if _, tErr := b.executeObjectTransition(rt, "reconcile-after-dispatch-outcome", mutation); tErr != nil {
		if mutation != nil {
			return tErr
		}
		log.Printf("[vanadis-restate] dispatch outcome transition warning: %v", tErr)
	}
	return nil
}

// Reconcile reconciles the build state on Restate.
func (b *BuildObject) Reconcile(ctx restate.ObjectContext, req ReconcileBuildRequest) (resp ReconcileBuildResult, outErr error) {
	defer func() {
		if outErr != nil {
			recordReconcileError("reconcile-build", outErr)
		}
	}()
	_ = req
	if b == nil || (b.store == nil && strings.TrimSpace(b.vanadisURL) == "") {
		return ReconcileBuildResult{}, restate.TerminalErrorf("vanadis url and store not configured")
	}
	rt := b.getRuntime(ctx)
	return b.executeObjectTransition(rt, "reconcile-build", nil)
}

// Mutate applies a mutation to the build and reconciles state.
func (b *BuildObject) Mutate(ctx restate.ObjectContext, req BuildMutationRequest) (resp ReconcileBuildResult, outErr error) {
	defer func() {
		if outErr != nil {
			recordReconcileError("mutate-build", outErr)
		}
	}()
	if b == nil || (b.store == nil && strings.TrimSpace(b.vanadisURL) == "") {
		return ReconcileBuildResult{}, restate.TerminalErrorf("vanadis url and store not configured")
	}
	rt := b.getRuntime(ctx)
	return b.executeObjectTransition(rt, "mutate-build", &req)
}

func mergePriorLaunched(priorState *BuildObjectState, priorLaunched map[string]bool) map[string]bool {
	if priorState == nil || len(priorState.Launched) == 0 {
		return priorLaunched
	}
	if priorLaunched == nil {
		priorLaunched = make(map[string]bool, len(priorState.Launched))
	}
	for k, v := range priorState.Launched {
		if v {
			priorLaunched[k] = true
		}
	}
	return priorLaunched
}

func (b *BuildObject) runTransitionStep(rt objectRuntime, stepName, buildID string, mutation *BuildMutationRequest, priorState *BuildObjectState, priorLaunched map[string]bool) (reconcileStepOutput, error) {
	notifiedInStep := false
	stepOut, err := rt.RunReconcileStep(stepName, func(ctx context.Context) (reconcileStepOutput, error) {
		var out reconcileStepOutput
		var stepErr error
		if b.store != nil {
			out, stepErr = b.executeAuthoritativeTransition(ctx, buildID, mutation, priorLaunched, terminalNotificationRecorded(priorState))
		} else {
			out, stepErr = b.callVanadisTransition(ctx, buildID, mutation, priorLaunched)
		}
		if stepErr != nil {
			notifiedInStep = true
			recordReconcileError(stepName, stepErr)
			b.notifyTransitionError(ctx, buildID, stepName, priorState, stepErr)
		}
		return out, stepErr
	})
	if err != nil && !notifiedInStep {
		recordReconcileError(stepName, err)
		b.notifyTransitionError(context.Background(), buildID, stepName, priorState, err)
	}
	return stepOut, err
}

func (b *BuildObject) executeObjectTransition(rt objectRuntime, stepName string, mutation *BuildMutationRequest) (ReconcileBuildResult, error) {
	buildID := strings.TrimSpace(rt.Key())
	if buildID == "" {
		err := restate.TerminalErrorf("build_id cannot be empty")
		recordReconcileError(stepName, err)
		return ReconcileBuildResult{}, err
	}

	priorState, err := rt.GetState()
	if err != nil {
		recordReconcileError(stepName, err)
		return ReconcileBuildResult{BuildID: buildID, Status: statusFailed, Error: err.Error()}, err
	}

	priorLaunched, err := rt.GetLaunched()
	if err != nil {
		recordReconcileError(stepName, err)
		return ReconcileBuildResult{BuildID: buildID, Status: statusFailed, Error: err.Error()}, err
	}

	priorLaunched = mergePriorLaunched(priorState, priorLaunched)
	stepOut, err := b.runTransitionStep(rt, stepName, buildID, mutation, priorState, priorLaunched)
	if err != nil {
		return ReconcileBuildResult{BuildID: buildID, Status: statusFailed, Error: err.Error()}, err
	}

	if stepOut.BuildStatus == statusCompleted || stepOut.BuildStatus == statusFailed || stepOut.BuildStatus == statusCancelled || stepOut.BuildStatus == statusNotFound {
		rt.ClearState()
		// Keep only the delivery marker across terminal invocations. The journaled
		// step result restores it on replay after a crash before SetState.
		if terminalNotificationRecorded(priorState) || terminalNotificationRecorded(stepOut.State) {
			rt.SetState(BuildObjectState{TerminalNotified: true})
		}
	} else {
		if stepOut.State != nil {
			rt.SetState(*stepOut.State)
		}
		rt.SetLaunched(stepOut.LaunchedMap)
	}

	if b.shouldScheduleReconcile(stepOut) {
		rt.ScheduleReconcile()
	}
	return ReconcileBuildResult{
		BuildID:        buildID,
		Status:         stepOut.BuildStatus,
		LaunchedChunks: stepOut.LaunchedChunks,
		Error:          stepOut.Error,
	}, nil
}

func (b *BuildObject) applyMutation(ctx context.Context, buildID string, mutation *BuildMutationRequest) error {
	if mutation == nil {
		return nil
	}
	mutation = normalizeCompletionMutation(mutation)
	action := strings.ToLower(strings.TrimSpace(mutation.Action))
	switch action {
	case "chunk_merged":
		if err := b.store.UpdatePRLifecycleStatus(ctx, mutation.ChunkID, "merged"); err != nil {
			if errors.Is(err, errPRTransitionSkipped) {
				return nil
			}
			return err
		}
		if b.executor != nil {
			if err := b.executor.PublishExternalEvent(ctx, "vanadis_chunk", "merged", []string{mutation.ChunkID}, ""); err != nil {
				log.Printf("[vanadis-restate] publish event warning: %v", err)
			}
		}
	case mutationChunkDeploySuccess:
		return b.completePRDeploymentMutation(ctx, buildID, mutation)
	case mutationActionCompleted:
		if err := b.validateActionBuild(ctx, buildID, mutation.ChunkID); err != nil {
			return err
		}
		res, err := b.store.CompleteActionChunk(ctx, mutation.ChunkID, mutation.DispatchID)
		if err != nil {
			return err
		}
		if res == nil || !res.CompletionAccepted {
			return errActionCompletionSkipped
		}
		return nil
	case "chunk_failed":
		return b.failMutation(ctx, buildID, mutation)
	case "cancelled":
		if err := b.store.UpdateBuildStatus(ctx, buildID, statusCancelled); err != nil {
			log.Printf("[vanadis-restate] update build status warning: %v", err)
		}
		if b.executor != nil {
			if err := b.executor.PublishExternalEvent(ctx, "vanadis_build", "cancelled", []string{buildID}, mutation.Reason); err != nil {
				log.Printf("[vanadis-restate] publish event warning: %v", err)
			}
		}
	}
	return nil
}

func countActiveChunks(chunks []VanadisChunk) int {
	activeCount := 0
	for _, c := range chunks {
		s := strings.ToLower(strings.TrimSpace(c.Status))
		if s == statusRunning || s == "coding" || s == "open" || s == "pr_opened" || s == "deploying" {
			activeCount++
		}
	}
	return activeCount
}

func isLaunchCandidate(chunk VanadisChunk, chunks []VanadisChunk, launchedMap map[string]bool) (candidate bool, resuming bool) {
	s := strings.ToLower(strings.TrimSpace(chunk.Status))
	res := s == statusRunning && chunk.Mode == chunkModeAction && chunk.StallNudges > 0
	unlaunched := s == statusRunning && !launchedMap[actionLaunchKey(chunk)] && chunk.PRNumber == 0
	if s != statusPending && s != "queued" && !res && !unlaunched {
		return false, false
	}
	if checkChunkDependenciesReady(chunk, chunks) != nil {
		return false, false
	}
	return true, res || unlaunched
}

func (b *BuildObject) launchEligibleChunks(ctx context.Context, build VanadisBuild, chunks []VanadisChunk, launchedMap map[string]bool) ([]string, error) {
	var launchedChunks []string
	var launchErrors []error
	activeCount := countActiveChunks(chunks)

	for idx, chunk := range chunks {
		candidate, resuming := isLaunchCandidate(chunk, chunks, launchedMap)
		if !candidate {
			continue
		}
		key := actionLaunchKey(chunk)
		if activeCount > 4 || (!resuming && activeCount >= 4) || launchedMap[key] || b.executor == nil {
			continue
		}
		if !resuming {
			activeCount++
		}
		if err := b.executor.ExecuteLaunchChunk(ctx, b.store, build, chunks, idx, ""); err != nil {
			launchErrors = append(launchErrors, err)
			continue
		}
		chunks[idx].Status = statusRunning
		if chunks[idx].StartedAt.IsZero() {
			chunks[idx].StartedAt = time.Now().UTC()
		}
		launchedMap[key] = true
		launchedChunks = append(launchedChunks, chunk.ID)
	}
	return launchedChunks, classifyLaunchErrors(launchErrors)
}

// Only active-session deferrals may complete the journaled step successfully.
// Any real failure still triggers durable invocation retry, including mixed failures.
func classifyLaunchErrors(errs []error) error {
	var failures []error
	for _, err := range errs {
		if !errors.Is(err, errDispatchActive) {
			failures = append(failures, err)
		}
	}
	if len(failures) > 0 {
		return errors.Join(failures...)
	}
	if len(errs) > 0 {
		return errDispatchActive
	}
	return nil
}

func (b *BuildObject) shouldScheduleReconcile(out reconcileStepOutput) bool {
	return b.store != nil && out.Error == errDispatchActive.Error() && out.BuildStatus == statusRunning
}

func (b *BuildObject) notifyBuildTerminal(ctx context.Context, build *VanadisBuild, chunks []VanadisChunk, status, reason, promptHeader, promptMsg string) {
	if b.executor == nil {
		return
	}
	if err := b.executor.PublishExternalEvent(ctx, "vanadis_build", status, []string{build.ID}, reason); err != nil {
		log.Printf("[vanadis-restate] publish event warning: %v", err)
	}
	if !build.Quiet {
		prompt := fmt.Sprintf("[%s]\nBuild ID: %s\nObjective: %s\n\n%s", promptHeader, build.ID, build.Objective, promptMsg)
		if err := b.executor.DispatchWakePrompt(ctx, build.ThreadID, prompt); err != nil {
			log.Printf("[vanadis-restate] wake prompt warning: %v", err)
		}
	}
	if _, err := b.executor.SyncBuildWidget(ctx, b.store, *build, chunks, "Build "+status); err != nil {
		log.Printf("[vanadis-restate] sync widget warning: %v", err)
	}
}

func (b *BuildObject) evaluateBuildTerminalStatus(ctx context.Context, build *VanadisBuild, chunks []VanadisChunk) string {
	allDone := true
	hasFailed := false
	for _, c := range chunks {
		s := strings.ToLower(strings.TrimSpace(c.Status))
		if s == statusFailed {
			hasFailed = true
		}
		if s != statusCompleted {
			allDone = false
		}
	}

	switch {
	case hasFailed:
		if err := b.store.UpdateBuildStatus(ctx, build.ID, statusFailed); err != nil {
			log.Printf("[vanadis-restate] update build status failed: %v", err)
		}
		build.Status = statusFailed
		b.notifyBuildTerminal(ctx, build, chunks, statusFailed, "chunk failed", "VANADIS BUILD FAILED", "Build encountered failures.")
		return statusFailed
	case allDone && len(chunks) > 0:
		if err := b.store.UpdateBuildStatus(ctx, build.ID, statusCompleted); err != nil {
			log.Printf("[vanadis-restate] update build status completed: %v", err)
		}
		build.Status = statusCompleted
		b.notifyBuildTerminal(ctx, build, chunks, statusCompleted, "", "VANADIS BUILD COMPLETED", "All chunks completed successfully.")
		return statusCompleted
	default:
		if b.executor != nil {
			if _, err := b.executor.SyncBuildWidget(ctx, b.store, *build, chunks, ""); err != nil {
				log.Printf("[vanadis-restate] sync widget warning: %v", err)
			}
		}
		return statusRunning
	}
}

func (b *BuildObject) executeAuthoritativeTransition(ctx context.Context, buildID string, mutation *BuildMutationRequest, priorLaunched map[string]bool, terminalNotified bool) (reconcileStepOutput, error) {
	if b.store == nil {
		return reconcileStepOutput{}, errors.New("store not configured")
	}

	mutation = normalizeCompletionMutation(mutation)

	if err := b.applyMutation(ctx, buildID, mutation); err != nil {
		if errors.Is(err, errActionCompletionSkipped) {
			build, chunks, readErr := b.store.GetBuild(ctx, buildID)
			if readErr != nil {
				return reconcileStepOutput{}, readErr
			}
			return reconcileStepOutput{BuildStatus: build.Status, State: &BuildObjectState{Build: *build, Chunks: chunks, Launched: priorLaunched}}, nil
		}
		return reconcileStepOutput{}, err
	}

	build, chunks, err := b.store.GetBuild(ctx, buildID)
	if err != nil {
		if errors.Is(err, ErrBuildNotFound) {
			return reconcileStepOutput{BuildStatus: statusNotFound}, nil
		}
		return reconcileStepOutput{}, err
	}

	if !terminalNotified && build.Status == statusCompleted && isActionCompletionMutation(mutation, chunks) {
		// The completion transaction may have persisted terminal status before this
		// journaled continuation ran. Replay its terminal effects as well.
		b.notifyBuildTerminal(ctx, build, chunks, statusCompleted, "", "VANADIS BUILD COMPLETED", "All chunks completed successfully.")
		terminalNotified = true
	}
	if build.Status == statusCompleted || build.Status == statusFailed || build.Status == statusCancelled {
		return reconcileStepOutput{
			BuildStatus: build.Status,
			State:       &BuildObjectState{Build: *build, Chunks: chunks, TerminalNotified: terminalNotified},
		}, nil
	}

	launchedMap := make(map[string]bool, len(priorLaunched))
	for k, v := range priorLaunched {
		if v {
			launchedMap[k] = true
		}
	}

	launchedChunks, err := b.launchEligibleChunks(ctx, *build, chunks, launchedMap)
	deferred := errors.Is(err, errDispatchActive)
	if err != nil && !deferred {
		return reconcileStepOutput{}, err
	}
	recoveryReason := ""
	if deferred {
		recoveryReason = errDispatchActive.Error()
	}
	finalStatus := b.evaluateBuildTerminalStatus(ctx, build, chunks)
	build.Status = finalStatus

	return reconcileStepOutput{
		BuildStatus:    finalStatus,
		Error:          recoveryReason,
		LaunchedChunks: launchedChunks,
		LaunchedMap:    launchedMap,
		State:          &BuildObjectState{Build: *build, Chunks: chunks, Launched: launchedMap, TerminalNotified: finalStatus == statusCompleted},
	}, nil
}

func (b *BuildObject) callVanadisTransition(ctx context.Context, buildID string, mutation *BuildMutationRequest, priorLaunched map[string]bool) (reconcileStepOutput, error) {
	transReq := RestateTransitionRequest{
		Mutation:      mutation,
		PriorLaunched: priorLaunched,
	}
	bodyBytes, err := json.Marshal(transReq)
	if err != nil {
		return reconcileStepOutput{}, restate.TerminalErrorf("marshal transition request: %v", err)
	}

	targetURL := fmt.Sprintf("%s/internal/restate/builds/%s/transition", b.vanadisURL, buildID)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return reconcileStepOutput{}, restate.TerminalErrorf("create transition request: %v", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := b.httpClient.Do(httpReq)
	if err != nil {
		return reconcileStepOutput{}, fmt.Errorf("transition request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, rErr := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if rErr != nil {
		return reconcileStepOutput{}, fmt.Errorf("read transition response: %w", rErr)
	}

	if resp.StatusCode >= 400 && resp.StatusCode < 500 {
		return reconcileStepOutput{}, restate.TerminalErrorf("vanadis transition rejected (status %d): %s", resp.StatusCode, string(respBody))
	}
	if resp.StatusCode >= 500 || resp.StatusCode < 200 {
		return reconcileStepOutput{}, fmt.Errorf("vanadis transition server error (status %d): %s", resp.StatusCode, string(respBody))
	}

	var stepOut reconcileStepOutput
	if err := json.Unmarshal(respBody, &stepOut); err != nil {
		return reconcileStepOutput{}, restate.TerminalErrorf("unmarshal transition response: %v", err)
	}

	return stepOut, nil
}

// SetupHealthMux creates the ServeMux for health, metrics, and coverage flush.
func SetupHealthMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", handleHealth)
	mux.HandleFunc("GET /healthz", handleHealth)
	mux.Handle("GET /metrics", promhttp.Handler())

	coverageFlush := httpkit.HandleCoverageFlush("vanadis-restate")
	mux.HandleFunc("POST /debug/coverage/flush", coverageFlush)
	mux.HandleFunc("GET /debug/coverage/flush", coverageFlush)
	return mux
}

func handleHealth(w http.ResponseWriter, _ *http.Request) {
	httpkit.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// RunApp runs both the health server and the Restate worker server.
func RunApp(ctx context.Context, cfg Config) error {
	if strings.TrimSpace(cfg.PostgresURL) == "" {
		return errors.New("postgres_url is required")
	}
	pgStore, closeStore, pErr := initPostgres(ctx, cfg.PostgresURL)
	if pErr != nil {
		return fmt.Errorf("connect to postgres: %w", pErr)
	}
	defer closeStore(context.WithoutCancel(ctx))

	if strings.TrimSpace(cfg.NATSURL) == "" {
		return errors.New("nats_url is required")
	}
	bus, bErr := newBusFn(ctx, cfg.NATSURL)
	if bErr != nil {
		return fmt.Errorf("connect to nats: %w", bErr)
	}
	defer func() {
		if cErr := bus.Close(); cErr != nil {
			log.Printf("[vanadis-restate] warning closing nats: %v", cErr)
		}
	}()
	natsCtx, natsCancel := context.WithTimeout(ctx, 5*time.Second)
	defer natsCancel()
	if sErr := bus.EnsureCoreStreams(natsCtx); sErr != nil {
		return fmt.Errorf("verify nats jetstream: %w", sErr)
	}

	healthMux := SetupHealthMux()
	healthServer := &http.Server{
		Handler:      healthMux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	var lc net.ListenConfig
	healthLn, err := lc.Listen(ctx, "tcp", ":"+cfg.HealthPort)
	if err != nil {
		return fmt.Errorf("listening on health port %s: %w", cfg.HealthPort, err)
	}

	errChan := make(chan error, 2)
	go func() {
		log.Printf("[vanadis-restate] Health server listening on :%s", cfg.HealthPort)
		if sErr := healthServer.Serve(healthLn); sErr != nil && !errors.Is(sErr, http.ErrServerClosed) {
			errChan <- sErr
		}
	}()

	eriClient := NewHTTPEriClient(cfg.EriURL, nil)
	executor := NewExecutor(cfg.ShareRoot, eriClient, bus, cfg.GitHubToken, nil)

	restateObj := NewBuildObject(cfg.VanadisURL, nil).WithAuthority(pgStore, executor)
	routerSvc := NewBuildRouterService(pgStore)
	restateServer := server.NewRestate().
		Bind(restate.Reflect(restateObj)).
		Bind(restate.Reflect(routerSvc))

	go func() {
		log.Printf("[vanadis-restate] Restate HTTP/2 server listening on :%s (VanadisURL: %s)", cfg.RestatePort, cfg.VanadisURL)
		if sErr := restateServer.Start(ctx, ":"+cfg.RestatePort); sErr != nil && !errors.Is(sErr, context.Canceled) {
			errChan <- sErr
		}
	}()

	select {
	case <-ctx.Done():
	case rErr := <-errChan:
		err = rErr
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer shutdownCancel()
	if sErr := healthServer.Shutdown(shutdownCtx); sErr != nil && !errors.Is(sErr, http.ErrServerClosed) && err == nil {
		err = sErr
	}

	log.Println("[vanadis-restate] Server stopped")
	return err
}

var (
	loadConfigFn = LoadConfigFromEnv
	runAppFn     = RunApp
	exitFn       = log.Fatalf
)

func main() {
	cfg := loadConfigFn()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := runAppFn(ctx, cfg); err != nil && !errors.Is(err, http.ErrServerClosed) {
		exitFn("[vanadis-restate] Error: %v", err)
	}
}
