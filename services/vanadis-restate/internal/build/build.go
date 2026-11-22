package build

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	restate "github.com/restatedev/sdk-go"

	"github.com/azylman/aerial/pkg/buildplan"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/ericlient"
	"github.com/azylman/aerial/services/vanadis-restate/internal/executor"
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
	labelReconcileAfterFailChunk   = "reconcile-after-fail-chunk"
	LabelRoutePREvent              = "RoutePREvent"
	LabelRouteDispatchOutcome      = "RouteDispatchOutcome"
	LabelRouteAttachPR             = "RouteAttachPR"
	LabelRouteFailChunk            = "RouteFailChunk"
	LabelRouteBuildReconcile       = "RouteBuildReconcile"
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
	labelReconcileAfterFailChunk,
	LabelRoutePREvent,
	LabelRouteDispatchOutcome,
	LabelRouteAttachPR,
	LabelRouteFailChunk,
	LabelRouteBuildReconcile,
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
	if errors.Is(err, store.ErrInvalidBuildInput) ||
		errors.Is(err, store.ErrBuildNotFound) ||
		errors.Is(err, ErrChunkNotFound) ||
		errors.Is(err, store.ErrBuildConflict) {
		return true
	}
	if termErr, ok := errors.AsType[restate.TerminalError](err); ok {
		code := int(termErr.Code())
		return code >= http.StatusBadRequest && code < http.StatusInternalServerError
	}
	return false
}

func RecordReconcileError(handler string, err error) {
	if err == nil {
		return
	}
	if isClientReconcileError(err) {
		clientErrorsTotal.WithLabelValues(handler).Inc()
		return
	}
	reconcileErrorsTotal.WithLabelValues(handler).Inc()
}

func RunJournaled[T any](ctx restate.Context, handlerLabel, stepName string, fn func(restate.RunContext) (T, error)) (T, error) {
	return restate.Run(ctx, func(rc restate.RunContext) (T, error) {
		out, err := runInSpan(rc, spanPrefix+handlerLabel+"/"+stepName, fn)
		if err != nil {
			RecordReconcileError(handlerLabel, err)
		}
		return out, ToTerminalErr(err)
	}, restate.WithName(stepName))
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

// ReconcileBuildRequest is the input payload for the Restate Build Virtual Object.
type ReconcileBuildRequest struct {
	Reason string `json:"reason,omitempty"`
	// DeferralSeq marks the delayed Reconcile that retries deferred launches.
	DeferralSeq int `json:"deferral_seq,omitempty"`
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

type reconcileStepOutput struct {
	RetryResult    *store.RetryActionChunkResult `json:"retry_result,omitempty"`
	BuildStatus    string                        `json:"build_status"`
	LaunchedChunks []string                      `json:"launched_chunks,omitempty"`
	LaunchedMap    map[string]bool               `json:"launched_map,omitempty"`
	Error          string                        `json:"error,omitempty"`
	State          *model.BuildObjectState       `json:"state,omitempty"`
	// ReconcileTargets are other builds this step changed; each is reconciled
	// by its own runner after the step commits.
	ReconcileTargets []store.BuildRoute `json:"reconcile_targets,omitempty"`
}

type objectRuntime interface {
	Key() string
	GetLaunched() (map[string]bool, error)
	SetLaunched(launched map[string]bool)
	GetState() (*model.BuildObjectState, error)
	SetState(state model.BuildObjectState)
	ClearState()
	ScheduleReconcile()
	ScheduleDeferredReconcile(delay time.Duration, seq int)
	InvocationID() string
	SendReconcile(service, buildID, reason string)
	RunReconcileStep(name string, fn func(ctx context.Context) (reconcileStepOutput, error)) (reconcileStepOutput, error)
}

type restateObjectRuntime struct {
	ctx          restate.ObjectContext
	buildService string
}

func (r restateObjectRuntime) serviceName() string {
	if strings.TrimSpace(r.buildService) == "" {
		return buildplan.BuildServiceName("")
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

// ScheduleDeferredReconcile sends the single delayed Reconcile that retries
// launches eri deferred; seq identifies it as the build's pending retry.
func (r restateObjectRuntime) ScheduleDeferredReconcile(delay time.Duration, seq int) {
	_ = restate.ObjectSend(r.ctx, r.serviceName(), restate.Key(r.ctx), "Reconcile").Send(ReconcileBuildRequest{Reason: "active dispatch recovery", DeferralSeq: seq}, restate.WithDelay(delay))
}

// InvocationID returns the current invocation ID: stable across replays of
// one invocation and fresh for every new invocation.
func (r restateObjectRuntime) InvocationID() string {
	return r.ctx.Request().ID
}

// SendReconcile sends a one-way Reconcile to another build's Build object.
func (r restateObjectRuntime) SendReconcile(service, buildID, reason string) {
	_ = restate.ObjectSend(r.ctx, service, buildID, "Reconcile").Send(ReconcileBuildRequest{Reason: reason})
}

// BuildObject implements the Restate-authoritative Virtual Object for multi-chunk builds.
type BuildObject struct {
	store           store.VanadisStore
	executor        *executor.Executor
	vanadisURL      string
	httpClient      *http.Client
	runtimeFactory  func(restate.ObjectContext) objectRuntime
	agentName       string
	activeStepSyncs sync.Map
}

// NewBuildObject creates a new BuildObject bound to vanadisURL with an optional HTTP client.
// vanadisURL is retained for signature compatibility and is no longer used for transitions.
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
func (b *BuildObject) WithAuthority(st store.VanadisStore, exec *executor.Executor) *BuildObject {
	if pg, ok := st.(*store.Store); ok && pg == nil {
		st = nil
	}
	b.store = st
	b.executor = exec
	return b
}

// ServiceName returns the Restate service name for this Virtual Object.
func (b *BuildObject) ServiceName() string {
	if b == nil {
		return buildplan.BuildServiceName("")
	}
	return buildplan.BuildServiceName(b.agentName)
}

func (b *BuildObject) notifyTransitionError(ctx context.Context, buildID, stepName string, priorState *model.BuildObjectState, transErr error) error {
	if b.executor == nil {
		return nil
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
		return nil
	}
	if _, err := b.executor.SyncBuildWidget(ctx, b.store, *build, chunks, fmt.Sprintf("⚠️ Reconcile backing off (%s): %v", stepName, transErr)); err != nil {
		slog.ErrorContext(ctx, "[vanadis-restate] sync widget error on transition failure", "error", err)
		return err
	}
	return nil
}

func (b *BuildObject) getRuntime(ctx restate.ObjectContext) objectRuntime {
	if b != nil && b.runtimeFactory != nil {
		return b.runtimeFactory(ctx)
	}
	agent := ""
	if b != nil {
		agent = b.agentName
	}
	return restateObjectRuntime{ctx: ctx, buildService: buildplan.BuildServiceName(agent)}
}

func ToTerminalErr(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, store.ErrInvalidBuildInput):
		return restate.ToTerminalError(err, restate.WithErrorCode(400))
	case errors.Is(err, store.ErrBuildNotFound) || errors.Is(err, ErrChunkNotFound):
		return restate.ToTerminalError(err, restate.WithErrorCode(404))
	case errors.Is(err, store.ErrBuildConflict):
		return restate.ToTerminalError(err, restate.WithErrorCode(409))
	default:
		return err
	}
}

func planInputsToChunks(buildID string, inputs []store.CreateChunkInput, now time.Time) []model.VanadisChunk {
	chunks := make([]model.VanadisChunk, len(inputs))
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
			Repo:               model.CleanRepoName(in.Repo),
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
		err := fmt.Errorf("%w: build id cannot be empty", store.ErrInvalidBuildInput)
		RecordReconcileError(labelCreate, err)
		return model.VanadisBuild{}, restate.ToTerminalError(err, restate.WithErrorCode(400))
	}
	threadID := strings.TrimSpace(req.ThreadID)
	objective := strings.TrimSpace(req.Objective)
	if threadID == "" || objective == "" {
		err := fmt.Errorf("%w: thread_id and objective cannot be empty", store.ErrInvalidBuildInput)
		RecordReconcileError(labelCreate, err)
		return model.VanadisBuild{}, restate.ToTerminalError(err, restate.WithErrorCode(400))
	}

	now, err := RunJournaled(ctx, labelCreate, "create-build-clock", func(rc restate.RunContext) (time.Time, error) {
		return time.Now().UTC(), nil
	})
	if err != nil {
		return model.VanadisBuild{}, ToTerminalErr(err)
	}
	chunks := planInputsToChunks(buildID, req.Chunks, now)
	if verr := buildplan.ValidateCreateBuildInputs(buildID, threadID, objective, chunks); verr != nil {
		RecordReconcileError(labelCreate, verr)
		return model.VanadisBuild{}, ToTerminalErr(verr)
	}
	validatedChunks, err := buildplan.NormalizeAndValidateBuildDAG(chunks)
	if err != nil {
		RecordReconcileError(labelCreate, err)
		return model.VanadisBuild{}, ToTerminalErr(err)
	}

	title := strings.TrimSpace(req.Title)
	if title == "" {
		title = objective
	}
	agent := strings.TrimSpace(req.Agent)
	if agent == "" {
		agent = "aerial"
	} else {
		agent = store.NormalizeRunnerAgent(agent)
	}
	build := model.VanadisBuild{
		ID:            buildID,
		Agent:         agent,
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
		_, err := RunJournaled(ctx, labelCreate, "create-build-postgres", func(rc restate.RunContext) (bool, error) {
			return b.persistBuildCreation(rc, build, validatedChunks)
		})
		if err != nil {
			return model.VanadisBuild{}, ToTerminalErr(err)
		}
	}

	rt := b.getRuntime(ctx)
	rt.SetState(model.BuildObjectState{Build: build, Chunks: validatedChunks})
	rt.SetLaunched(make(map[string]bool))
	if _, tErr := b.executeObjectTransition(rt, labelReconcileInitial, nil); tErr != nil {
		return model.VanadisBuild{}, ToTerminalErr(tErr)
	}
	return build, nil
}

// UpdateBuild mutates build metadata and updates state.
func (b *BuildObject) UpdateBuild(ctx restate.ObjectContext, req store.UpdateBuildRequest) (model.VanadisBuild, error) {
	buildID := restate.Key(ctx)
	if b.store == nil {
		return model.VanadisBuild{ID: buildID, Title: req.Title, Objective: req.Objective}, nil
	}
	res, err := RunJournaled(ctx, labelUpdateBuild, "update-build-postgres", func(rc restate.RunContext) (model.VanadisBuild, error) {
		updated, err := b.store.UpdateBuild(rc, buildID, req)
		if err != nil {
			return model.VanadisBuild{}, err
		}
		if b.executor != nil {
			_, chunks, gErr := b.store.GetBuild(rc, buildID)
			if gErr == nil {
				if _, sErr := b.executor.SyncBuildWidget(rc, b.store, *updated, chunks, "Build updated: "+updated.Title); sErr != nil {
					slog.WarnContext(rc, "[vanadis-restate] sync widget warning", "error", sErr)
				}
			}
		}
		return *updated, nil
	})
	if err != nil {
		return model.VanadisBuild{}, ToTerminalErr(err)
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

	if b.store == nil {
		rt.ClearState()
		return model.VanadisBuild{ID: buildID, Status: statusCancelled}, nil
	}

	res, err := RunJournaled(ctx, labelCancel, "cancel-build-postgres", func(rc restate.RunContext) (model.VanadisBuild, error) {
		bld, chunks, err := b.store.GetBuild(rc, buildID)
		if err != nil {
			return model.VanadisBuild{}, err
		}
		if bld.Status == statusCompleted || bld.Status == statusCancelled {
			return *bld, fmt.Errorf("%w: cannot cancel terminal build %s", store.ErrBuildConflict, bld.Status)
		}
		if err := b.store.UpdateBuildStatus(rc, buildID, statusCancelled); err != nil {
			return model.VanadisBuild{}, err
		}
		bld.Status = statusCancelled
		if b.executor != nil {
			var cancelCtx context.Context = rc
			if bld != nil && strings.TrimSpace(bld.Agent) != "" {
				cancelCtx = ericlient.WithAgent(rc, bld.Agent)
			}
			for _, c := range chunks {
				if cErr := b.executor.CancelChunkDispatch(cancelCtx, c.ID); cErr != nil {
					slog.WarnContext(cancelCtx, "[vanadis-restate] cancel chunk dispatch failed", "chunk_id", c.ID, "error", cErr)
				}
			}
			if _, sErr := b.executor.SyncBuildWidget(cancelCtx, b.store, *bld, chunks, "Build cancelled"); sErr != nil {
				slog.WarnContext(cancelCtx, "[vanadis-restate] sync widget warning", "error", sErr)
			}
			if pErr := b.executor.PublishExternalEvent(cancelCtx, "vanadis_build", "cancelled", []string{buildID}, req.Reason); pErr != nil {
				slog.WarnContext(cancelCtx, "[vanadis-restate] publish event warning", "error", pErr)
			}
		}
		return *bld, nil
	})
	if err != nil {
		return model.VanadisBuild{}, ToTerminalErr(err)
	}
	rt.ClearState()
	return res, nil
}

// AppendChunks appends chunks to a running build and advances the DAG.
func (b *BuildObject) AppendChunks(ctx restate.ObjectContext, req AppendChunksRequest) ([]model.VanadisChunk, error) {
	if len(req.Chunks) == 0 {
		err := fmt.Errorf("%w: chunks cannot be empty", store.ErrInvalidBuildInput)
		RecordReconcileError(labelAppendChunks, err)
		return nil, restate.ToTerminalError(err, restate.WithErrorCode(http.StatusBadRequest))
	}
	if b.store == nil {
		return nil, nil
	}
	return b.executeChunkAppend(ctx, restate.Key(ctx), req.Chunks)
}

func (b *BuildObject) executeChunkAppend(ctx restate.ObjectContext, buildID string, chunks []store.CreateChunkInput) ([]model.VanadisChunk, error) {
	appended, err := RunJournaled(ctx, labelAppendChunks, "append-chunks-postgres", func(rc restate.RunContext) ([]model.VanadisChunk, error) {
		return b.store.AppendChunks(rc, buildID, chunks)
	})
	if err != nil {
		return nil, ToTerminalErr(err)
	}
	if _, tErr := b.executeObjectTransition(b.getRuntime(ctx), labelReconcileAfterAppend, nil); tErr != nil {
		return nil, ToTerminalErr(tErr)
	}
	return appended, nil
}

// UpdateChunk modifies a pending chunk and revalidates dependencies.
func (b *BuildObject) UpdateChunk(ctx restate.ObjectContext, req store.UpdateChunkRequest) (model.VanadisChunk, error) {
	buildID := restate.Key(ctx)
	chunkRef := strings.TrimSpace(req.ChunkRef)
	if chunkRef == "" {
		err := fmt.Errorf("%w: chunk_ref cannot be empty", store.ErrInvalidBuildInput)
		RecordReconcileError(labelUpdateChunk, err)
		return model.VanadisChunk{}, restate.ToTerminalError(err, restate.WithErrorCode(400))
	}
	if b.store == nil {
		return model.VanadisChunk{}, nil
	}
	updated, err := RunJournaled(ctx, labelUpdateChunk, "update-chunk-postgres", func(rc restate.RunContext) (model.VanadisChunk, error) {
		c, err := b.store.UpdateChunk(rc, buildID, chunkRef, req)
		if err != nil {
			return model.VanadisChunk{}, err
		}
		return *c, nil
	})
	if err != nil {
		return model.VanadisChunk{}, ToTerminalErr(err)
	}
	rt := b.getRuntime(ctx)
	if _, tErr := b.executeObjectTransition(rt, labelReconcileAfterUpdateChunk, nil); tErr != nil {
		return model.VanadisChunk{}, ToTerminalErr(tErr)
	}
	return updated, nil
}

// RemoveChunk removes an unstarted chunk from the build DAG.
func (b *BuildObject) RemoveChunk(ctx restate.ObjectContext, req RemoveChunkRequest) error {
	buildID := restate.Key(ctx)
	targetRef := strings.TrimSpace(req.ChunkRef)
	if targetRef == "" {
		err := fmt.Errorf("%w: chunk_ref cannot be empty", store.ErrInvalidBuildInput)
		RecordReconcileError(labelRemoveChunk, err)
		return restate.ToTerminalError(err, restate.WithErrorCode(400))
	}
	if b.store == nil {
		return nil
	}
	rt := b.getRuntime(ctx)
	return b.executeChunkRemoval(ctx, rt, buildID, targetRef)
}

func (b *BuildObject) executeChunkRemoval(ctx restate.ObjectContext, rt objectRuntime, buildID, targetRef string) error {
	_, err := RunJournaled(ctx, labelRemoveChunk, "remove-chunk-postgres", func(rc restate.RunContext) (bool, error) {
		removedChunk, _, rErr := b.store.RemoveChunk(rc, buildID, targetRef)
		if rErr != nil {
			return false, rErr
		}
		if b.executor != nil && removedChunk != nil {
			var owner *model.VanadisBuild
			if bld, _, gErr := b.store.GetBuild(rc, buildID); gErr == nil {
				owner = bld
			}
			if cErr := b.executor.CancelChunkDispatch(withBuildOwner(rc, owner), removedChunk.ID); cErr != nil {
				slog.WarnContext(rc, "[vanadis-restate] cancel chunk dispatch failed", "chunk_id", removedChunk.ID, "error", cErr)
			}
		}
		return true, nil
	})
	if err != nil {
		return ToTerminalErr(err)
	}
	if _, tErr := b.executeObjectTransition(rt, labelReconcileAfterRemoveChunk, nil); tErr != nil {
		return ToTerminalErr(tErr)
	}
	return nil
}

func (b *BuildObject) notifyChunkFailure(rc restate.RunContext, bld model.VanadisBuild, chunkID, reason string) error {
	if b == nil {
		return nil
	}
	return b.flow().NotifyChunkFailure(rc, bld, chunkID, reason)
}

// FailChunk marks a chunk failed and records failure while preserving Restate state.
func (b *BuildObject) FailChunk(ctx restate.ObjectContext, req model.FailChunkRequest) (model.FailChunkResponse, error) {
	chunkID := strings.TrimSpace(req.ChunkID)
	reason := strings.TrimSpace(req.Reason)
	if chunkID == "" || reason == "" {
		err := fmt.Errorf("%w: chunk_id and reason required", store.ErrInvalidBuildInput)
		RecordReconcileError(labelFailChunk, err)
		return model.FailChunkResponse{}, restate.ToTerminalError(err, restate.WithErrorCode(400))
	}
	if b.store == nil {
		return model.FailChunkResponse{ChunkID: chunkID, Failed: true}, nil
	}

	res, err := RunJournaled(ctx, labelFailChunk, "fail-chunk-postgres", func(rc restate.RunContext) (model.FailChunkResponse, error) {
		if err := b.validateActionBuild(rc, restate.Key(ctx), chunkID); err != nil {
			return model.FailChunkResponse{}, err
		}
		bld, chk, failed, err := b.store.FailChunk(rc, chunkID, reason, req.DispatchID)
		if err != nil {
			return model.FailChunkResponse{}, err
		}
		resp := model.FailChunkResponse{
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
			resp.Agent = bld.Agent
		}
		return resp, nil
	})
	if err != nil {
		return model.FailChunkResponse{}, ToTerminalErr(err)
	}
	if !res.Failed || res.BuildID == "" || res.ChunkStatus != statusFailed {
		return res, nil
	}
	if b.executor != nil {
		_, nErr := RunJournaled(ctx, labelFailChunk, "notify-chunk-failure", func(rc restate.RunContext) (bool, error) {
			bld := model.VanadisBuild{
				ID:        res.BuildID,
				ThreadID:  res.ThreadID,
				Objective: res.Objective,
				Quiet:     res.Quiet,
				Agent:     res.Agent,
			}
			if err := b.notifyChunkFailure(rc, bld, chunkID, reason); err != nil {
				return false, err
			}
			return true, nil
		})
		if nErr != nil {
			return model.FailChunkResponse{}, ToTerminalErr(nErr)
		}
	}
	// Reconcile owns the build status and the failure wake: the failed chunk
	// blocks only its dependents while every other ready chunk keeps launching.
	tRes, tErr := b.executeObjectTransition(b.getRuntime(ctx), labelReconcileAfterFailChunk, nil)
	if tErr != nil {
		return model.FailChunkResponse{}, ToTerminalErr(tErr)
	}
	if tRes.Status != "" {
		res.BuildStatus = tRes.Status
	}
	return res, nil
}

// AttachPR attaches a PR to a chunk or standalone build.
func (b *BuildObject) AttachPR(ctx restate.ObjectContext, req store.AttachChunkPRRequest) (model.AttachChunkPRResponse, error) {
	st := b.store
	if st == nil {
		return model.AttachChunkPRResponse{Status: "attached", BuildID: restate.Key(ctx), ChunkID: req.ChunkID}, nil
	}
	return b.executePRAttachment(ctx, req)
}

func (b *BuildObject) executePRAttachment(ctx restate.ObjectContext, req store.AttachChunkPRRequest) (model.AttachChunkPRResponse, error) {
	st := b.store
	attached, attachErr := RunJournaled(ctx, labelAttachPR, "attach-pr-postgres", func(rc restate.RunContext) (model.AttachChunkPRResponse, error) {
		bID, cID, thID, err := st.AttachChunkPR(rc, req)
		if err != nil {
			return model.AttachChunkPRResponse{}, ToTerminalErr(err)
		}
		return model.AttachChunkPRResponse{
			Status:   "attached",
			BuildID:  bID,
			ChunkID:  cID,
			ThreadID: thID,
		}, nil
	})
	if attachErr != nil {
		return model.AttachChunkPRResponse{}, ToTerminalErr(attachErr)
	}

	runtime := b.getRuntime(ctx)
	if _, tErr := b.executeObjectTransition(runtime, labelReconcileAfterAttachPR, nil); tErr != nil {
		return model.AttachChunkPRResponse{}, ToTerminalErr(tErr)
	}
	return attached, nil
}
