package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
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
	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

const (
	labelCreate                    = "Create"
	labelUpdateBuild               = "UpdateBuild"
	labelCancel                    = "Cancel"
	labelAppendChunks              = "AppendChunks"
	labelUpdateChunk               = "UpdateChunk"
	labelRemoveChunk               = "RemoveChunk"
	labelFailChunk                 = "FailChunk"
	labelAttachPR                  = "AttachPR"
	labelOnPREvent                 = "OnPREvent"
	labelOnDispatchOutcome         = "OnDispatchOutcome"
	labelReconcileBuild            = "reconcile-build"
	labelMutateBuild               = "mutate-build"
	labelReconcileInitial          = "reconcile-initial"
	labelReconcileAfterAppend      = "reconcile-after-append"
	labelReconcileAfterUpdateChunk = "reconcile-after-update-chunk"
	labelReconcileAfterRemoveChunk = "reconcile-after-remove-chunk"
	labelReconcileAfterAttachPR    = "reconcile-after-attach-pr"
	labelReconcileAfterPREvent     = "reconcile-after-pr-event"
	labelReconcileAfterDispatch    = "reconcile-after-dispatch-outcome"
	labelReconcileAfterNoPRFail    = "reconcile-after-nopr-fail"
	labelRoutePREvent              = "RoutePREvent"
	labelRouteDispatchOutcome      = "RouteDispatchOutcome"
	labelRouteAttachPR             = "RouteAttachPR"
	labelRouteFailChunk            = "RouteFailChunk"
)

var reconcileErrorHandlerLabels = []string{
	labelCreate,
	labelUpdateBuild,
	labelCancel,
	labelAppendChunks,
	labelUpdateChunk,
	labelRemoveChunk,
	labelFailChunk,
	labelAttachPR,
	labelOnPREvent,
	labelOnDispatchOutcome,
	labelReconcileBuild,
	labelMutateBuild,
	labelReconcileInitial,
	labelReconcileAfterAppend,
	labelReconcileAfterUpdateChunk,
	labelReconcileAfterRemoveChunk,
	labelReconcileAfterAttachPR,
	labelReconcileAfterPREvent,
	labelReconcileAfterDispatch,
	labelReconcileAfterNoPRFail,
	labelRoutePREvent,
	labelRouteDispatchOutcome,
	labelRouteAttachPR,
	labelRouteFailChunk,
}

var reconcileErrorsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "vanadis_restate_reconcile_errors_total",
		Help: "Total number of Vanadis Restate handler and reconcile transition errors.",
	},
	[]string{"handler"},
)

var clientErrorsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "vanadis_restate_client_errors_total",
		Help: "Total number of Vanadis Restate terminal 4xx client and validation errors.",
	},
	[]string{"handler"},
)

func init() {
	if err := prometheus.Register(reconcileErrorsTotal); err != nil {
		if _, ok := errors.AsType[prometheus.AlreadyRegisteredError](err); !ok {
			panic(fmt.Sprintf("failed to register vanadis_restate_reconcile_errors_total: %v", err))
		}
	}
	if err := prometheus.Register(clientErrorsTotal); err != nil {
		if _, ok := errors.AsType[prometheus.AlreadyRegisteredError](err); !ok {
			panic(fmt.Sprintf("failed to register vanadis_restate_client_errors_total: %v", err))
		}
	}
	for _, h := range reconcileErrorHandlerLabels {
		reconcileErrorsTotal.WithLabelValues(h).Add(0)
		clientErrorsTotal.WithLabelValues(h).Add(0)
	}
}

func isClientReconcileError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrInvalidBuildInput) ||
		errors.Is(err, ErrBuildNotFound) ||
		errors.Is(err, ErrChunkNotFound) ||
		errors.Is(err, ErrBuildConflict) {
		return true
	}
	if termErr, ok := errors.AsType[restate.TerminalError](err); ok {
		code := int(termErr.Code())
		return code >= http.StatusBadRequest && code < http.StatusInternalServerError
	}
	return false
}

func recordReconcileError(handler string, err error) {
	if err == nil {
		return
	}
	if isClientReconcileError(err) {
		clientErrorsTotal.WithLabelValues(handler).Inc()
		return
	}
	reconcileErrorsTotal.WithLabelValues(handler).Inc()
}

func runJournaled[T any](ctx restate.Context, handlerLabel, stepName string, fn func(restate.RunContext) (T, error)) (T, error) {
	return restate.Run(ctx, func(rc restate.RunContext) (T, error) {
		out, err := runInSpan(rc, spanPrefix+handlerLabel+"/"+stepName, fn)
		if err != nil {
			recordReconcileError(handlerLabel, err)
		}
		return out, toTerminalErr(err)
	}, restate.WithName(stepName))
}

var newBusFn = func(ctx context.Context, url string) (pubsub.Bus, error) {
	return pubsub.NewJetStreamBus(ctx, url, nats.RetryOnFailedConnect(false), nats.Timeout(5*time.Second))
}

const (
	statusCompleted     = "completed"
	statusFailed        = "failed"
	statusCancelled     = "cancelled"
	statusNotFound      = "not_found"
	statusCrashed       = "crashed"
	statusTimeout       = "timeout"
	mutationChunkFailed = "chunk_failed"
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
	AgentName   string
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

	pgURL := strings.TrimSpace(store.FirstNonEmpty(os.Getenv("DATABASE_URL"), os.Getenv("POSTGRES_URL"), httpkit.BuildPostgresDSN(os.Getenv, "aerial")))
	eriURL := strings.TrimRight(strings.TrimSpace(store.FirstNonEmpty(os.Getenv("ERI_URL"), "http://eri.aerial")), "/")
	natsURL := strings.TrimSpace(store.FirstNonEmpty(os.Getenv("NATS_URL"), "nats://nats.aerial:4222"))
	ghToken := strings.TrimSpace(store.FirstNonEmpty(os.Getenv("GITHUB_TOKEN"), os.Getenv("GITHUB_PAT")))
	shareRoot := strings.TrimSpace(store.FirstNonEmpty(os.Getenv("SHARE_ROOT"), defaultShareRoot))
	agentName := strings.TrimSpace(store.FirstNonEmpty(os.Getenv("AGENT_NAME"), "aerial"))

	return Config{
		RestatePort: restatePort,
		HealthPort:  healthPort,
		VanadisURL:  vanadisURL,
		PostgresURL: pgURL,
		EriURL:      eriURL,
		NATSURL:     natsURL,
		GitHubToken: ghToken,
		ShareRoot:   shareRoot,
		AgentName:   agentName,
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

// BuildMutationRequest represents a typed mutation payload targeting BuildObject.
type BuildMutationRequest struct {
	Revision     string                 `json:"revision,omitempty"`
	Generation   int                    `json:"generation,omitempty"`
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
	RetryResult    *store.RetryActionChunkResult `json:"retry_result,omitempty"`
	BuildStatus    string                        `json:"build_status"`
	LaunchedChunks []string                      `json:"launched_chunks,omitempty"`
	LaunchedMap    map[string]bool               `json:"launched_map,omitempty"`
	Error          string                        `json:"error,omitempty"`
	State          *model.BuildObjectState       `json:"state,omitempty"`
}

type objectRuntime interface {
	Key() string
	GetLaunched() (map[string]bool, error)
	SetLaunched(launched map[string]bool)
	GetState() (*model.BuildObjectState, error)
	SetState(state model.BuildObjectState)
	ClearState()
	ScheduleReconcile()
	RunReconcileStep(name string, fn func(ctx context.Context) (reconcileStepOutput, error)) (reconcileStepOutput, error)
}

type restateObjectRuntime struct {
	ctx          restate.ObjectContext
	buildService string
}

func (r restateObjectRuntime) serviceName() string {
	if strings.TrimSpace(r.buildService) == "" {
		return legacyBuildServiceName
	}
	return r.buildService
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

func (r restateObjectRuntime) GetState() (*model.BuildObjectState, error) {
	return restate.Get[*model.BuildObjectState](r.ctx, "state")
}

func (r restateObjectRuntime) SetState(state model.BuildObjectState) {
	restate.Set(r.ctx, "state", &state)
}

func (r restateObjectRuntime) ClearState() {
	restate.ClearAll(r.ctx)
}

func (r restateObjectRuntime) RunReconcileStep(name string, fn func(ctx context.Context) (reconcileStepOutput, error)) (reconcileStepOutput, error) {
	return restate.Run(r.ctx, func(rc restate.RunContext) (reconcileStepOutput, error) {
		return runInSpan(rc, spanPrefix+"reconcile/"+name, func(trc restate.RunContext) (reconcileStepOutput, error) {
			return fn(trc)
		})
	}, restate.WithName(name))
}

// A delayed self-send releases the exclusive invocation between active-dispatch
// probes, allowing completion and cancellation handlers to acquire the object.
func (r restateObjectRuntime) ScheduleReconcile() {
	_ = restate.ObjectSend(r.ctx, r.serviceName(), restate.Key(r.ctx), "Reconcile").Send(ReconcileBuildRequest{Reason: "active dispatch recovery"}, restate.WithDelay(time.Second))
}

// BuildObject implements the Restate-authoritative Virtual Object for multi-chunk builds.
type BuildObject struct {
	store          store.VanadisStore
	executor       *Executor
	vanadisURL     string
	httpClient     *http.Client
	runtimeFactory func(restate.ObjectContext) objectRuntime
	agentName      string
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

// NewBuildObjectWithAgent creates a BuildObject with an agent-scoped Restate service name.
func NewBuildObjectWithAgent(vanadisURL, agentName string, httpClient *http.Client, runtimeFactory ...func(restate.ObjectContext) objectRuntime) *BuildObject {
	b := NewBuildObject(vanadisURL, httpClient, runtimeFactory...)
	b.agentName = strings.TrimSpace(agentName)
	return b
}

// WithAuthority wires the PostgreSQL store and side-effect executor into BuildObject.
func (b *BuildObject) WithAuthority(st store.VanadisStore, exec *Executor) *BuildObject {
	if pg, ok := st.(*PostgresVanadisStore); ok && pg == nil {
		st = nil
	}
	b.store = st
	b.executor = exec
	return b
}

// ServiceName returns the Restate service name for this Virtual Object.
func (b *BuildObject) ServiceName() string {
	if b == nil {
		return legacyBuildServiceName
	}
	return BuildServiceName(b.agentName)
}

func (b *BuildObject) notifyTransitionError(ctx context.Context, buildID, stepName string, priorState *model.BuildObjectState, transErr error) {
	if b.executor == nil {
		return
	}
	var build *model.VanadisBuild
	var chunks []model.VanadisChunk
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
		slog.ErrorContext(ctx, "[vanadis-restate] sync widget error on transition failure", "error", err)
	}
}

func (b *BuildObject) getRuntime(ctx restate.ObjectContext) objectRuntime {
	if b != nil && b.runtimeFactory != nil {
		return b.runtimeFactory(ctx)
	}
	agent := ""
	if b != nil {
		agent = b.agentName
	}
	return restateObjectRuntime{ctx: ctx, buildService: BuildServiceName(agent)}
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

func planInputsToChunks(buildID string, inputs []store.CreateChunkInput) []model.VanadisChunk {
	chunks := make([]model.VanadisChunk, len(inputs))
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
		chunks[i] = model.VanadisChunk{
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

func (b *BuildObject) persistBuildCreation(rc restate.RunContext, build model.VanadisBuild, chunks []model.VanadisChunk) (bool, error) {
	if err := b.store.CreateBuild(rc, build, chunks); err != nil {
		return false, err
	}
	if b.executor != nil && !build.Quiet {
		if _, sErr := b.executor.SyncBuildWidget(rc, b.store, build, chunks, ""); sErr != nil {
			slog.WarnContext(rc, "[vanadis-restate] sync widget warning", "error", sErr)
		}
	}
	return true, nil
}

// Create handles initial build and DAG initialization.
func (b *BuildObject) Create(ctx restate.ObjectContext, req CreateBuildRequest) (model.VanadisBuild, error) {
	buildID := strings.TrimSpace(restate.Key(ctx))
	if buildID == "" {
		buildID = strings.TrimSpace(req.ID)
	}
	if buildID == "" {
		err := fmt.Errorf("%w: build id cannot be empty", ErrInvalidBuildInput)
		recordReconcileError(labelCreate, err)
		return model.VanadisBuild{}, restate.ToTerminalError(err, restate.WithErrorCode(400))
	}
	threadID := strings.TrimSpace(req.ThreadID)
	objective := strings.TrimSpace(req.Objective)
	if threadID == "" || objective == "" {
		err := fmt.Errorf("%w: thread_id and objective cannot be empty", ErrInvalidBuildInput)
		recordReconcileError(labelCreate, err)
		return model.VanadisBuild{}, restate.ToTerminalError(err, restate.WithErrorCode(400))
	}

	chunks := planInputsToChunks(buildID, req.Chunks)
	if err := validateCreateBuildInputs(buildID, threadID, objective, chunks); err != nil {
		recordReconcileError(labelCreate, err)
		return model.VanadisBuild{}, toTerminalErr(err)
	}
	validatedChunks, err := normalizeAndValidateBuildDAG(chunks)
	if err != nil {
		recordReconcileError(labelCreate, err)
		return model.VanadisBuild{}, toTerminalErr(err)
	}

	title := strings.TrimSpace(req.Title)
	if title == "" {
		title = objective
	}
	now := time.Now().UTC()
	build := model.VanadisBuild{
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
		_, err := runJournaled(ctx, labelCreate, "create-build-postgres", func(rc restate.RunContext) (bool, error) {
			return b.persistBuildCreation(rc, build, validatedChunks)
		})
		if err != nil {
			return model.VanadisBuild{}, toTerminalErr(err)
		}
	}

	rt := b.getRuntime(ctx)
	rt.SetState(model.BuildObjectState{Build: build, Chunks: validatedChunks})
	rt.SetLaunched(make(map[string]bool))
	if _, tErr := b.executeObjectTransition(rt, labelReconcileInitial, nil); tErr != nil {
		return model.VanadisBuild{}, toTerminalErr(tErr)
	}
	return build, nil
}

// UpdateBuild mutates build metadata and updates state.
func (b *BuildObject) UpdateBuild(ctx restate.ObjectContext, req store.UpdateBuildRequest) (model.VanadisBuild, error) {
	buildID := restate.Key(ctx)
	if b.store == nil {
		return model.VanadisBuild{ID: buildID, Title: req.Title, Objective: req.Objective}, nil
	}
	res, err := runJournaled(ctx, labelUpdateBuild, "update-build-postgres", func(rc restate.RunContext) (model.VanadisBuild, error) {
		updated, err := b.store.UpdateBuild(rc, buildID, req)
		if err != nil {
			return model.VanadisBuild{}, err
		}
		if b.executor != nil {
			_, chunks, gErr := b.store.GetBuild(rc, buildID)
			if gErr == nil {
				if _, sErr := b.executor.SyncBuildWidget(rc, b.store, *updated, chunks, "Build updated: "+updated.Title); sErr != nil {
					slog.WarnContext(ctx, "[vanadis-restate] sync widget warning", "error", sErr)
				}
			}
		}
		return *updated, nil
	})
	if err != nil {
		return model.VanadisBuild{}, toTerminalErr(err)
	}
	rt := b.getRuntime(ctx)
	if st, gErr := rt.GetState(); gErr == nil && st != nil {
		st.Build = res
		rt.SetState(*st)
	}
	return res, nil
}

// Cancel cancels the active build and clears Restate state.
func (b *BuildObject) Cancel(ctx restate.ObjectContext, req CancelBuildRequest) (model.VanadisBuild, error) {
	buildID := restate.Key(ctx)
	rt := b.getRuntime(ctx)
	defer rt.ClearState()

	if b.store == nil {
		return model.VanadisBuild{ID: buildID, Status: statusCancelled}, nil
	}

	res, err := runJournaled(ctx, labelCancel, "cancel-build-postgres", func(rc restate.RunContext) (model.VanadisBuild, error) {
		bld, chunks, err := b.store.GetBuild(rc, buildID)
		if err != nil {
			return model.VanadisBuild{}, err
		}
		if bld.Status == statusCompleted || bld.Status == statusFailed {
			return *bld, fmt.Errorf("%w: cannot cancel terminal build %s", ErrBuildConflict, bld.Status)
		}
		if err := b.store.UpdateBuildStatus(rc, buildID, statusCancelled); err != nil {
			return model.VanadisBuild{}, err
		}
		bld.Status = statusCancelled
		if b.executor != nil {
			for _, c := range chunks {
				if cErr := b.executor.CancelChunkDispatch(rc, c.ID); cErr != nil {
					slog.WarnContext(ctx, "[vanadis-restate] cancel chunk dispatch failed", "chunk_id", c.ID, "error", cErr)
				}
			}
			if _, sErr := b.executor.SyncBuildWidget(rc, b.store, *bld, chunks, "Build cancelled"); sErr != nil {
				slog.WarnContext(ctx, "[vanadis-restate] sync widget warning", "error", sErr)
			}
			if pErr := b.executor.PublishExternalEvent(rc, "vanadis_build", "cancelled", []string{buildID}, req.Reason); pErr != nil {
				slog.WarnContext(ctx, "[vanadis-restate] publish event warning", "error", pErr)
			}
		}
		return *bld, nil
	})
	if err != nil {
		return model.VanadisBuild{}, toTerminalErr(err)
	}
	return res, nil
}

// AppendChunks appends chunks to a running build and advances the DAG.
func (b *BuildObject) AppendChunks(ctx restate.ObjectContext, req AppendChunksRequest) ([]model.VanadisChunk, error) {
	if len(req.Chunks) == 0 {
		err := fmt.Errorf("%w: chunks cannot be empty", ErrInvalidBuildInput)
		recordReconcileError(labelAppendChunks, err)
		return nil, restate.ToTerminalError(err, restate.WithErrorCode(http.StatusBadRequest))
	}
	if b.store == nil {
		return nil, nil
	}
	return b.executeChunkAppend(ctx, restate.Key(ctx), req.Chunks)
}

func (b *BuildObject) executeChunkAppend(ctx restate.ObjectContext, buildID string, chunks []store.CreateChunkInput) ([]model.VanadisChunk, error) {
	appended, err := runJournaled(ctx, labelAppendChunks, "append-chunks-postgres", func(rc restate.RunContext) ([]model.VanadisChunk, error) {
		return b.store.AppendChunks(rc, buildID, chunks)
	})
	if err != nil {
		return nil, toTerminalErr(err)
	}
	if _, tErr := b.executeObjectTransition(b.getRuntime(ctx), labelReconcileAfterAppend, nil); tErr != nil {
		return nil, toTerminalErr(tErr)
	}
	return appended, nil
}

// UpdateChunk modifies a pending chunk and revalidates dependencies.
func (b *BuildObject) UpdateChunk(ctx restate.ObjectContext, req store.UpdateChunkRequest) (model.VanadisChunk, error) {
	buildID := restate.Key(ctx)
	chunkRef := strings.TrimSpace(req.ChunkRef)
	if chunkRef == "" {
		err := fmt.Errorf("%w: chunk_ref cannot be empty", ErrInvalidBuildInput)
		recordReconcileError(labelUpdateChunk, err)
		return model.VanadisChunk{}, restate.ToTerminalError(err, restate.WithErrorCode(400))
	}
	if b.store == nil {
		return model.VanadisChunk{}, nil
	}
	updated, err := runJournaled(ctx, labelUpdateChunk, "update-chunk-postgres", func(rc restate.RunContext) (model.VanadisChunk, error) {
		c, err := b.store.UpdateChunk(rc, buildID, chunkRef, req)
		if err != nil {
			return model.VanadisChunk{}, err
		}
		return *c, nil
	})
	if err != nil {
		return model.VanadisChunk{}, toTerminalErr(err)
	}
	rt := b.getRuntime(ctx)
	if _, tErr := b.executeObjectTransition(rt, labelReconcileAfterUpdateChunk, nil); tErr != nil {
		return model.VanadisChunk{}, toTerminalErr(tErr)
	}
	return updated, nil
}

// RemoveChunk removes an unstarted chunk from the build DAG.
func (b *BuildObject) RemoveChunk(ctx restate.ObjectContext, req RemoveChunkRequest) error {
	buildID := restate.Key(ctx)
	targetRef := strings.TrimSpace(req.ChunkRef)
	if targetRef == "" {
		err := fmt.Errorf("%w: chunk_ref cannot be empty", ErrInvalidBuildInput)
		recordReconcileError(labelRemoveChunk, err)
		return restate.ToTerminalError(err, restate.WithErrorCode(400))
	}
	if b.store == nil {
		return nil
	}
	rt := b.getRuntime(ctx)
	return b.executeChunkRemoval(ctx, rt, buildID, targetRef)
}

func (b *BuildObject) executeChunkRemoval(ctx restate.ObjectContext, rt objectRuntime, buildID, targetRef string) error {
	_, err := runJournaled(ctx, labelRemoveChunk, "remove-chunk-postgres", func(rc restate.RunContext) (bool, error) {
		removedChunk, _, rErr := b.store.RemoveChunk(rc, buildID, targetRef)
		if rErr != nil {
			return false, rErr
		}
		if b.executor != nil && removedChunk != nil {
			if cErr := b.executor.CancelChunkDispatch(rc, removedChunk.ID); cErr != nil {
				slog.WarnContext(ctx, "[vanadis-restate] cancel chunk dispatch failed", "chunk_id", removedChunk.ID, "error", cErr)
			}
		}
		return true, nil
	})
	if err != nil {
		return toTerminalErr(err)
	}
	if _, tErr := b.executeObjectTransition(rt, labelReconcileAfterRemoveChunk, nil); tErr != nil {
		return toTerminalErr(tErr)
	}
	return nil
}

func (b *BuildObject) notifyChunkFailure(rc restate.RunContext, bld model.VanadisBuild, chunkID, reason string) error {
	if b.executor == nil {
		return nil
	}
	if cErr := b.executor.CancelChunkDispatch(rc, chunkID); cErr != nil {
		slog.WarnContext(rc, "[vanadis-restate] cancel chunk dispatch failed", "chunk_id", chunkID, "error", cErr)
	}
	buildObj := bld
	var chunks []model.VanadisChunk
	if b.store != nil {
		fetched, fetchedChunks, gErr := b.store.GetBuild(rc, bld.ID)
		if gErr != nil {
			return fmt.Errorf("get build %s for failure notification: %w", bld.ID, gErr)
		}
		if fetched != nil {
			buildObj = *fetched
			chunks = fetchedChunks
		}
	}
	var widgetErr error
	if _, sErr := b.executor.SyncBuildWidget(rc, b.store, buildObj, chunks, "Build failed: "+reason); sErr != nil {
		slog.WarnContext(rc, "[vanadis-restate] sync widget warning", "error", sErr)
		widgetErr = sErr
	}
	var wakeErr error
	if !buildObj.Quiet {
		prompt := fmt.Sprintf("[VANADIS BUILD FAILED]\nBuild ID: %s\nObjective: %s\n\nChunk failed: %s", buildObj.ID, buildObj.Objective, reason)
		if dErr := b.executor.DispatchWakePrompt(rc, buildObj.ThreadID, prompt); dErr != nil {
			slog.WarnContext(rc, "[vanadis-restate] dispatch wake prompt warning", "error", dErr)
			wakeErr = dErr
		}
	}
	if pErr := b.executor.PublishExternalEvent(rc, "vanadis_chunk", "failed", []string{chunkID}, reason); pErr != nil {
		slog.WarnContext(rc, "[vanadis-restate] publish event warning", "error", pErr)
	}
	if pErr := b.executor.PublishExternalEvent(rc, "vanadis_build", "failed", []string{buildObj.ID}, reason); pErr != nil {
		slog.WarnContext(rc, "[vanadis-restate] publish event warning", "error", pErr)
	}
	return errors.Join(widgetErr, wakeErr)
}

// FailChunk marks a chunk failed and cancels active work if the failure is terminal.
func (b *BuildObject) FailChunk(ctx restate.ObjectContext, req FailChunkRequest) (FailChunkResponse, error) {
	chunkID := strings.TrimSpace(req.ChunkID)
	reason := strings.TrimSpace(req.Reason)
	if chunkID == "" || reason == "" {
		err := fmt.Errorf("%w: chunk_id and reason required", ErrInvalidBuildInput)
		recordReconcileError(labelFailChunk, err)
		return FailChunkResponse{}, restate.ToTerminalError(err, restate.WithErrorCode(400))
	}
	if b.store == nil {
		return FailChunkResponse{ChunkID: chunkID, Failed: true}, nil
	}

	res, err := runJournaled(ctx, labelFailChunk, "fail-chunk-postgres", func(rc restate.RunContext) (FailChunkResponse, error) {
		if err := b.validateActionBuild(rc, restate.Key(ctx), chunkID); err != nil {
			return FailChunkResponse{}, err
		}
		bld, chk, failed, err := b.store.FailChunk(rc, chunkID, reason, req.DispatchID)
		if err != nil {
			return FailChunkResponse{}, err
		}
		resp := FailChunkResponse{
			Failed:      failed,
			BuildID:     chk.BuildID,
			ChunkID:     chk.ID,
			BuildStatus: bld.Status,
			ChunkStatus: chk.Status,
		}
		if bld != nil {
			resp.ThreadID = bld.ThreadID
			resp.Objective = bld.Objective
			resp.Quiet = bld.Quiet
		}
		return resp, nil
	})
	if err != nil {
		return FailChunkResponse{}, toTerminalErr(err)
	}
	if res.Failed && res.BuildID != "" && res.BuildStatus == statusFailed && res.ChunkStatus == statusFailed && b.executor != nil {
		_, nErr := runJournaled(ctx, labelFailChunk, "notify-chunk-failure", func(rc restate.RunContext) (bool, error) {
			bld := model.VanadisBuild{
				ID:        res.BuildID,
				ThreadID:  res.ThreadID,
				Objective: res.Objective,
				Quiet:     res.Quiet,
			}
			if err := b.notifyChunkFailure(rc, bld, chunkID, reason); err != nil {
				return false, err
			}
			return true, nil
		})
		if nErr != nil {
			return FailChunkResponse{}, toTerminalErr(nErr)
		}
	}
	if res.Failed && res.BuildStatus == statusFailed {
		rt := b.getRuntime(ctx)
		rt.ClearState()
	}
	return res, nil
}

// AttachPR attaches a PR to a chunk or standalone build.
func (b *BuildObject) AttachPR(ctx restate.ObjectContext, req store.AttachChunkPRRequest) (AttachChunkPRResponse, error) {
	st := b.store
	if st == nil {
		return AttachChunkPRResponse{Status: "attached", BuildID: restate.Key(ctx), ChunkID: req.ChunkID}, nil
	}
	return b.executePRAttachment(ctx, req)
}

func (b *BuildObject) executePRAttachment(ctx restate.ObjectContext, req store.AttachChunkPRRequest) (AttachChunkPRResponse, error) {
	st := b.store
	attached, attachErr := runJournaled(ctx, labelAttachPR, "attach-pr-postgres", func(rc restate.RunContext) (AttachChunkPRResponse, error) {
		bID, cID, thID, err := st.AttachChunkPR(rc, req)
		if err != nil {
			return AttachChunkPRResponse{}, toTerminalErr(err)
		}
		if exec := b.executor; exec != nil {
			if bld, chunks, gErr := st.GetBuild(rc, bID); gErr == nil && bld != nil {
				if _, sErr := exec.SyncBuildWidget(rc, st, *bld, chunks, ""); sErr != nil {
					slog.ErrorContext(ctx, "[vanadis-restate] sync widget error on attach pr", "error", sErr)
				}
			}
		}
		return AttachChunkPRResponse{
			Status:   "attached",
			BuildID:  bID,
			ChunkID:  cID,
			ThreadID: thID,
		}, nil
	})
	if attachErr != nil {
		return AttachChunkPRResponse{}, toTerminalErr(attachErr)
	}

	runtime := b.getRuntime(ctx)
	if _, tErr := b.executeObjectTransition(runtime, labelReconcileAfterAttachPR, nil); tErr != nil {
		return AttachChunkPRResponse{}, toTerminalErr(tErr)
	}
	return attached, nil
}

func (b *BuildObject) dispatchThreadWakePrompt(rc restate.RunContext, buildID, prompt string) error {
	if b.executor == nil || b.store == nil {
		return nil
	}
	bld, _, gErr := b.store.GetBuild(rc, buildID)
	if gErr != nil {
		return gErr
	}
	if bld == nil {
		return ErrBuildNotFound
	}
	if dErr := b.executor.DispatchWakePrompt(rc, bld.ThreadID, prompt); dErr != nil {
		slog.WarnContext(rc, "[vanadis-restate] wake prompt warning", "error", dErr)
		return dErr
	}
	return nil
}

// Reconcile reconciles the build state on Restate.
func (b *BuildObject) Reconcile(ctx restate.ObjectContext, req ReconcileBuildRequest) (ReconcileBuildResult, error) {
	_ = req
	if b == nil || (b.store == nil && strings.TrimSpace(b.vanadisURL) == "") {
		err := restate.TerminalErrorf("vanadis url and store not configured")
		recordReconcileError(labelReconcileBuild, err)
		return ReconcileBuildResult{}, err
	}
	rt := b.getRuntime(ctx)
	return b.executeObjectTransition(rt, labelReconcileBuild, nil)
}

// Mutate applies a mutation to the build and reconciles state.
func (b *BuildObject) Mutate(ctx restate.ObjectContext, req BuildMutationRequest) (ReconcileBuildResult, error) {
	if b == nil || (b.store == nil && strings.TrimSpace(b.vanadisURL) == "") {
		err := restate.TerminalErrorf("vanadis url and store not configured")
		recordReconcileError(labelMutateBuild, err)
		return ReconcileBuildResult{}, err
	}
	rt := b.getRuntime(ctx)
	if req.Action == "retry_deployment" {
		return b.retryDeployment(rt, req)
	}
	return b.executeObjectTransition(rt, labelMutateBuild, &req)
}

func mergePriorLaunched(priorState *model.BuildObjectState, priorLaunched map[string]bool) map[string]bool {
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

func (b *BuildObject) runTransitionStep(rt objectRuntime, stepName, buildID string, mutation *BuildMutationRequest, priorState *model.BuildObjectState, priorLaunched map[string]bool) (reconcileStepOutput, error) {
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
			rt.SetState(model.BuildObjectState{TerminalNotified: true})
		}
	} else {
		if stepOut.State != nil {
			rt.SetState(*stepOut.State)
		}
		rt.SetLaunched(stepOut.LaunchedMap)
		if err := b.syncDeploymentDeadlines(rt, stepOut.State, nil, nil); err != nil {
			return ReconcileBuildResult{}, err
		}
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
				slog.WarnContext(ctx, "[vanadis-restate] publish event warning", "error", err)
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
	case mutationChunkFailed:
		return b.failMutation(ctx, buildID, mutation)
	case "cancelled":
		if err := b.store.UpdateBuildStatus(ctx, buildID, statusCancelled); err != nil {
			slog.WarnContext(ctx, "[vanadis-restate] update build status warning", "error", err)
		}
		if b.executor != nil {
			if err := b.executor.PublishExternalEvent(ctx, "vanadis_build", "cancelled", []string{buildID}, mutation.Reason); err != nil {
				slog.WarnContext(ctx, "[vanadis-restate] publish event warning", "error", err)
			}
		}
	}
	return nil
}

func countActiveChunks(chunks []model.VanadisChunk) int {
	activeCount := 0
	for _, c := range chunks {
		s := strings.ToLower(strings.TrimSpace(c.Status))
		if s == statusRunning || s == "coding" || s == "open" || s == "pr_opened" || s == "deploying" {
			activeCount++
		}
	}
	return activeCount
}

func isLaunchCandidate(chunk model.VanadisChunk, chunks []model.VanadisChunk, launchedMap map[string]bool) (candidate bool, resuming bool) {
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

func (b *BuildObject) launchEligibleChunks(ctx context.Context, build model.VanadisBuild, chunks []model.VanadisChunk, launchedMap map[string]bool) ([]string, error) {
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

func (b *BuildObject) notifyBuildTerminal(ctx context.Context, build *model.VanadisBuild, chunks []model.VanadisChunk, status, reason, promptHeader, promptMsg string) error {
	if b.executor == nil {
		return nil
	}
	if err := b.executor.PublishExternalEvent(ctx, "vanadis_build", status, []string{build.ID}, reason); err != nil {
		slog.WarnContext(ctx, "[vanadis-restate] publish event warning", "error", err)
	}
	var wakeErr error
	if !build.Quiet {
		prompt := fmt.Sprintf("[%s]\nBuild ID: %s\nObjective: %s\n\n%s", promptHeader, build.ID, build.Objective, promptMsg)
		if err := b.executor.DispatchWakePrompt(ctx, build.ThreadID, prompt); err != nil {
			slog.WarnContext(ctx, "[vanadis-restate] wake prompt warning", "error", err)
			wakeErr = err
		}
	}
	var widgetErr error
	if _, err := b.executor.SyncBuildWidget(ctx, b.store, *build, chunks, "Build "+status); err != nil {
		slog.WarnContext(ctx, "[vanadis-restate] sync widget warning", "error", err)
		widgetErr = err
	}
	return errors.Join(wakeErr, widgetErr)
}

func (b *BuildObject) evaluateBuildTerminalStatus(ctx context.Context, build *model.VanadisBuild, chunks []model.VanadisChunk) (string, error) {
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
		build.Status = statusFailed
		if err := b.notifyBuildTerminal(ctx, build, chunks, statusFailed, "chunk failed", "VANADIS BUILD FAILED", "Build encountered failures."); err != nil {
			return "", err
		}
		if err := b.store.UpdateBuildStatus(ctx, build.ID, statusFailed); err != nil {
			slog.ErrorContext(ctx, "[vanadis-restate] update build status failed", "error", err)
			return "", err
		}
		return statusFailed, nil
	case allDone && len(chunks) > 0:
		build.Status = statusCompleted
		if err := b.notifyBuildTerminal(ctx, build, chunks, statusCompleted, "", "VANADIS BUILD COMPLETED", "All chunks completed successfully."); err != nil {
			return "", err
		}
		if err := b.store.UpdateBuildStatus(ctx, build.ID, statusCompleted); err != nil {
			slog.ErrorContext(ctx, "[vanadis-restate] update build status completed", "error", err)
			return "", err
		}
		return statusCompleted, nil
	default:
		if b.executor != nil {
			if _, err := b.executor.SyncBuildWidget(ctx, b.store, *build, chunks, ""); err != nil {
				slog.WarnContext(ctx, "[vanadis-restate] sync widget warning", "error", err)
			}
		}
		return statusRunning, nil
	}
}

func cloneLaunchedMap(priorLaunched map[string]bool) map[string]bool {
	launchedMap := make(map[string]bool, len(priorLaunched))
	for k, v := range priorLaunched {
		if v {
			launchedMap[k] = true
		}
	}
	return launchedMap
}

func (b *BuildObject) replayTerminalMutationEffects(ctx context.Context, build *model.VanadisBuild, chunks []model.VanadisChunk, mutation *BuildMutationRequest, terminalNotified bool) (bool, error) {
	if terminalNotified {
		return true, nil
	}
	if build.Status == statusCompleted && isActionCompletionMutation(mutation, chunks) {
		if nErr := b.notifyBuildTerminal(ctx, build, chunks, statusCompleted, "", "VANADIS BUILD COMPLETED", "All chunks completed successfully."); nErr != nil {
			return false, nErr
		}
		return true, nil
	}
	if build.Status == statusFailed && isFailureCompletionMutation(mutation, chunks) {
		if nErr := b.notifyBuildTerminal(ctx, build, chunks, statusFailed, store.FirstNonEmpty(mutation.Reason, "chunk failed"), "VANADIS BUILD FAILED", "Build encountered failures."); nErr != nil {
			return false, nErr
		}
		return true, nil
	}
	return false, nil
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
			return reconcileStepOutput{BuildStatus: build.Status, State: &model.BuildObjectState{Build: *build, Chunks: chunks, Launched: priorLaunched}}, nil
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

	var nErr error
	terminalNotified, nErr = b.replayTerminalMutationEffects(ctx, build, chunks, mutation, terminalNotified)
	if nErr != nil {
		return reconcileStepOutput{}, nErr
	}
	if build.Status == statusCompleted || build.Status == statusFailed || build.Status == statusCancelled {
		return reconcileStepOutput{
			BuildStatus: build.Status,
			State:       &model.BuildObjectState{Build: *build, Chunks: chunks, TerminalNotified: terminalNotified},
		}, nil
	}

	launchedMap := cloneLaunchedMap(priorLaunched)
	launchedChunks, err := b.launchEligibleChunks(ctx, *build, chunks, launchedMap)
	deferred := errors.Is(err, errDispatchActive)
	if err != nil && !deferred {
		return reconcileStepOutput{}, err
	}
	recoveryReason := ""
	if deferred {
		recoveryReason = errDispatchActive.Error()
	}
	finalStatus, err := b.evaluateBuildTerminalStatus(ctx, build, chunks)
	if err != nil {
		return reconcileStepOutput{}, err
	}
	build.Status = finalStatus

	return reconcileStepOutput{
		BuildStatus:    finalStatus,
		Error:          recoveryReason,
		LaunchedChunks: launchedChunks,
		LaunchedMap:    launchedMap,
		State:          &model.BuildObjectState{Build: *build, Chunks: chunks, Launched: launchedMap, TerminalNotified: finalStatus == statusCompleted || finalStatus == statusFailed},
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
		return reconcileStepOutput{}, restate.ToTerminalError(fmt.Errorf("vanadis transition rejected (status %d): %s", resp.StatusCode, string(respBody)), restate.WithErrorCode(restate.Code(resp.StatusCode)))
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
	return httpkit.SetupHealthMux("vanadis-restate", promhttp.Handler())
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

	// Internal clients propagate traceparent; GitHub calls record spans without leaking headers.
	eriClient := NewHTTPEriClient(cfg.EriURL, tracing.Client(&http.Client{Timeout: 30 * time.Second}))
	executor := NewExecutor(cfg.ShareRoot, eriClient, bus, cfg.GitHubToken, tracing.ExternalClient(&http.Client{Timeout: 30 * time.Second}))

	restateObj := NewBuildObjectWithAgent(cfg.VanadisURL, cfg.AgentName, tracing.Client(&http.Client{Timeout: 30 * time.Second})).WithAuthority(pgStore, executor)
	routerSvc := NewBuildRouterServiceWithAgent(pgStore, cfg.AgentName)
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
	loadConfigFn  = LoadConfigFromEnv
	runAppFn      = RunApp
	exitFn        = log.Fatalf
	initTracingFn = tracing.Init
)

func main() {
	cfg := loadConfigFn()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	_, flushTracing := initTracingFn(ctx, "vanadis-restate", tracing.DefaultConfigPath)
	defer flushTracing()
	slog.SetDefault(slog.New(tracing.SlogHandler(slog.NewJSONHandler(os.Stdout, nil))))

	if err := runAppFn(ctx, cfg); err != nil && !errors.Is(err, http.ErrServerClosed) {
		exitFn("[vanadis-restate] Error: %v", err)
	}
}
