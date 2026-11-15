package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"maps"
	"net/http"
	"os"
	"strings"
	"time"

	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/server"
	"gopkg.in/yaml.v3"
)

const maxParallelChunks = 4

// ErrRestateDisabled is returned when the Restate server is intentionally disabled.
var ErrRestateDisabled = errors.New("restate server is disabled")

// RestateConfig holds Restate durable workflow runtime configuration for Vanadis.
type RestateConfig struct {
	Enabled             bool   `yaml:"enabled"`
	RestatePort         string `yaml:"restate_port"`
	RestateIngressURL   string `yaml:"restate_ingress_url"`
	RestateAdminURL     string `yaml:"restate_admin_url"`
	RestateAdvertiseURL string `yaml:"restate_advertise_url"`
}

func cleanRestatePortEnv(raw string) string {
	v := strings.TrimSpace(raw)
	if strings.Contains(v, "://") {
		return ""
	}
	return v
}

// LoadRestateConfig loads Restate configuration from an optional YAML file with environment variable overrides.
func LoadRestateConfig(configPath string) RestateConfig {
	var fileCfg RestateConfig
	target := strings.TrimSpace(configPath)
	if fi, err := os.Stat(target); err != nil || fi.Size() == 0 {
		target = ""
		if fi, err := os.Stat(vanadisFallbackConfigPath); err == nil && fi.Size() > 0 {
			target = vanadisFallbackConfigPath
		}
	}
	if target != "" {
		if data, err := os.ReadFile(target); err == nil {
			if err := yaml.Unmarshal(data, &fileCfg); err != nil {
				log.Printf("[vanadis] Warning: failed to parse restate yaml config at %s: %v", target, err)
			}
		}
	}
	enabled := fileCfg.Enabled || strings.EqualFold(os.Getenv("VANADIS_RESTATE_ENABLED"), "true") || strings.EqualFold(os.Getenv("RESTATE_ENABLED"), "true")
	ingressURL := strings.TrimRight(strings.TrimSpace(firstNonEmpty(os.Getenv("RESTATE_INGRESS_URL"), fileCfg.RestateIngressURL)), "/")
	if ingressURL != "" && !strings.EqualFold(os.Getenv("VANADIS_RESTATE_ENABLED"), "false") && !strings.EqualFold(os.Getenv("RESTATE_ENABLED"), "false") {
		enabled = true
	}
	return RestateConfig{
		Enabled:             enabled,
		RestatePort:         firstNonEmpty(cleanRestatePortEnv(os.Getenv("VANADIS_RESTATE_PORT")), cleanRestatePortEnv(os.Getenv("RESTATE_PORT")), fileCfg.RestatePort, "9080"),
		RestateIngressURL:   ingressURL,
		RestateAdminURL:     strings.TrimRight(strings.TrimSpace(firstNonEmpty(os.Getenv("RESTATE_ADMIN_URL"), fileCfg.RestateAdminURL)), "/"),
		RestateAdvertiseURL: strings.TrimRight(strings.TrimSpace(firstNonEmpty(os.Getenv("VANADIS_RESTATE_ADVERTISE_URL"), fileCfg.RestateAdvertiseURL)), "/"),
	}
}

// RunBuildWorkflowRequest is the input payload for the Restate BuildWorkflow.
type RunBuildWorkflowRequest struct {
	BuildID string `json:"build_id"`
}

// RunBuildWorkflowResult is the terminal output of the Restate BuildWorkflow.
type RunBuildWorkflowResult struct {
	BuildID         string `json:"build_id"`
	Status          string `json:"status"`
	TotalChunks     int    `json:"total_chunks"`
	CompletedChunks int    `json:"completed_chunks"`
}

// CancelBuildWorkflowRequest is the input for the Cancel shared signal handler.
type CancelBuildWorkflowRequest struct {
	Reason string `json:"reason,omitempty"`
}

// ReconcileBuildRequest is the input payload for the Restate BuildReconciler Virtual Object.
type ReconcileBuildRequest struct {
	Reason string `json:"reason,omitempty"`
}

// ReconcileBuildResult is the output of the Restate BuildReconciler Virtual Object.
type ReconcileBuildResult struct {
	BuildID        string   `json:"build_id"`
	Status         string   `json:"status"`
	LaunchedChunks []string `json:"launched_chunks,omitempty"`
	Error          string   `json:"error,omitempty"`
}

// BuildReconciler implements the level-triggered Restate Virtual Object for multi-chunk builds.
type BuildReconciler struct {
	srv *VanadisServer
}

// NewBuildReconciler creates a new BuildReconciler instance bound to a VanadisServer.
func NewBuildReconciler(srv *VanadisServer) *BuildReconciler {
	return &BuildReconciler{srv: srv}
}

// ServiceName returns the Restate service name for this Virtual Object.
func (r *BuildReconciler) ServiceName() string {
	return "BuildReconciler"
}

type reconcileStepOutput struct {
	BuildStatus    string          `json:"build_status"`
	LaunchedChunks []string        `json:"launched_chunks,omitempty"`
	LaunchedMap    map[string]bool `json:"launched_map,omitempty"`
	Error          string          `json:"error,omitempty"`
}

type objectRuntime interface {
	Key() string
	GetLaunched() (map[string]bool, error)
	SetLaunched(launched map[string]bool)
	ClearState()
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

func (r restateObjectRuntime) ClearState() {
	restate.ClearAll(r.ctx)
}

func (r restateObjectRuntime) RunReconcileStep(name string, fn func(ctx context.Context) (reconcileStepOutput, error)) (reconcileStepOutput, error) {
	return restate.Run(r.ctx, func(rc restate.RunContext) (reconcileStepOutput, error) {
		return fn(rc)
	}, restate.WithName(name))
}

// Reconcile reconciles the build state on Restate.
func (r *BuildReconciler) Reconcile(ctx restate.ObjectContext, _ ReconcileBuildRequest) (ReconcileBuildResult, error) {
	if r == nil || r.srv == nil || r.srv.store == nil {
		return ReconcileBuildResult{}, restate.TerminalErrorf("vanadis store not configured")
	}
	rt := restateObjectRuntime{ctx: ctx}
	if strings.TrimSpace(rt.Key()) == "" {
		return ReconcileBuildResult{}, restate.TerminalErrorf("build_id cannot be empty")
	}
	return r.reconcileWithRuntime(rt)
}

func (r *BuildReconciler) reconcileWithRuntime(rt objectRuntime) (ReconcileBuildResult, error) {
	buildID := strings.TrimSpace(rt.Key())
	if buildID == "" {
		return ReconcileBuildResult{}, restate.TerminalErrorf("build_id cannot be empty")
	}

	priorLaunched, err := rt.GetLaunched()
	if err != nil {
		return ReconcileBuildResult{BuildID: buildID, Status: "failed", Error: err.Error()}, err
	}

	stepOut, err := rt.RunReconcileStep("reconcile-build", func(ctx context.Context) (reconcileStepOutput, error) {
		return executeReconcileBuild(ctx, r.srv, buildID, priorLaunched)
	})
	if err != nil {
		return ReconcileBuildResult{BuildID: buildID, Status: "failed", Error: err.Error()}, err
	}

	if stepOut.BuildStatus == "completed" || stepOut.BuildStatus == "failed" || stepOut.BuildStatus == "cancelled" || stepOut.BuildStatus == "not_found" {
		rt.ClearState()
	} else {
		rt.SetLaunched(stepOut.LaunchedMap)
	}

	return ReconcileBuildResult{
		BuildID:        buildID,
		Status:         stepOut.BuildStatus,
		LaunchedChunks: stepOut.LaunchedChunks,
		Error:          stepOut.Error,
	}, nil
}

func deriveReconcileChunkStates(chunks []VanadisChunk, priorLaunched map[string]bool) ([]string, map[string]*workflowChunkRuntimeState) {
	chunkStates := make(map[string]*workflowChunkRuntimeState, len(chunks))
	chunkOrder := make([]string, 0, len(chunks))
	for _, c := range chunks {
		chunkOrder = append(chunkOrder, c.ID)
		cs := &workflowChunkRuntimeState{chunk: c}
		switch statusLower := strings.ToLower(strings.TrimSpace(c.Status)); statusLower {
		case "completed":
			cs.launched = true
			cs.codeReady = true
			cs.runtimeReady = true
		case "merged", "deploying", "deploy_failed":
			cs.launched = true
			cs.codeReady = strings.ToLower(c.Mode) == "pr"
			cs.runtimeReady = false
		case "pr_opened", "ci_failed", "conflict", "failed", "cancelled", "coding":
			cs.launched = true
			cs.codeReady = false
			cs.runtimeReady = false
		case "running":
			cs.launched = (priorLaunched != nil && priorLaunched[c.ID]) || c.PRNumber > 0
			cs.codeReady = false
			cs.runtimeReady = false
		default: // "pending" or other initial states
			cs.launched = priorLaunched != nil && priorLaunched[c.ID]
			cs.codeReady = false
			cs.runtimeReady = false
		}
		chunkStates[c.ID] = cs
	}
	return chunkOrder, chunkStates
}

func executeFailBuild(ctx context.Context, srv *VanadisServer, buildID, reason string) error {
	if srv == nil || srv.store == nil {
		return errors.New("vanadis store not configured")
	}
	if err := srv.store.UpdateBuildStatus(ctx, buildID, "failed"); err != nil {
		log.Printf("[vanadis] warning: failed updating build status to failed for %s: %v", buildID, err)
		return fmt.Errorf("update build %s status to failed: %w", buildID, err)
	}
	RecordBuildTotal("failed")
	srv.syncBuildWidget(ctx, buildID, "Build failed: "+reason)
	srv.publishExternalEvent(ctx, "vanadis_build", "failed", []string{buildID}, reason)

	if b, _, err := srv.store.GetBuild(ctx, buildID); err == nil && b != nil && strings.TrimSpace(b.ThreadID) != "" {
		prompt := fmt.Sprintf("[VANADIS BUILD FAILED]\nBuild ID: %s\nObjective: %s\n\nBuild failed: %s", b.ID, b.Objective, reason)
		if dErr := srv.dispatchWakePrompt(ctx, b.ThreadID, prompt); dErr != nil {
			log.Printf("[vanadis] warning: failed dispatching fail wake prompt to thread %s: %v", b.ThreadID, dErr)
		}
	}
	return nil
}

func launchReconcileEligibleChunks(ctx context.Context, srv *VanadisServer, buildID string, chunkOrder []string, states map[string]*workflowChunkRuntimeState, priorLaunched map[string]bool) ([]string, map[string]bool, error) {
	launchedMap := make(map[string]bool, len(priorLaunched))
	for k, v := range priorLaunched {
		if v {
			launchedMap[k] = true
		}
	}

	ready := computeEligibleChunksToLaunch(chunkOrder, states, maxParallelChunks)
	var launchedChunks []string
	for _, cID := range ready {
		st := states[cID]
		if st == nil {
			continue
		}
		if err := executeLaunchChunk(ctx, srv, buildID, st.chunk, "", ""); err != nil {
			return nil, nil, fmt.Errorf("launch chunk %s: %w", cID, err)
		}
		st.launched = true
		launchedMap[cID] = true
		launchedChunks = append(launchedChunks, cID)
	}

	if len(launchedChunks) > 0 {
		srv.syncBuildWidget(ctx, buildID, "")
	}

	return launchedChunks, launchedMap, nil
}

func executeReconcileBuild(ctx context.Context, srv *VanadisServer, buildID string, priorLaunched map[string]bool) (reconcileStepOutput, error) {
	if srv == nil || srv.store == nil {
		return reconcileStepOutput{}, errors.New("vanadis store not configured")
	}
	b, chunks, err := srv.store.GetBuild(ctx, buildID)
	if err != nil {
		if errors.Is(err, ErrBuildNotFound) {
			return reconcileStepOutput{BuildStatus: "not_found"}, nil
		}
		return reconcileStepOutput{}, fmt.Errorf("get build %s: %w", buildID, err)
	}

	if b.Status == "completed" || b.Status == "failed" || b.Status == "cancelled" {
		return reconcileStepOutput{BuildStatus: b.Status}, nil
	}

	if b.Status == "pending" {
		if err := srv.store.UpdateBuildStatus(ctx, buildID, "running"); err != nil {
			return reconcileStepOutput{}, fmt.Errorf("update build %s to running: %w", buildID, err)
		}
		b.Status = "running"
	}

	if priorLaunched == nil {
		priorLaunched = reconstructLaunchedChunks(chunks)
	}

	chunkOrder, states := deriveReconcileChunkStates(chunks, priorLaunched)

	for _, c := range chunks {
		statusLower := strings.ToLower(strings.TrimSpace(c.Status))
		if statusLower == "failed" || statusLower == "cancelled" {
			failReason := fmt.Sprintf("chunk %s %s", c.ID, c.Status)
			if err := executeFailBuild(ctx, srv, buildID, failReason); err != nil {
				return reconcileStepOutput{}, err
			}
			return reconcileStepOutput{BuildStatus: "failed", Error: failReason}, nil
		}
	}

	if isWorkflowBuildComplete(chunkOrder, states) {
		if err := executeCompleteWorkflowBuild(ctx, srv, buildID); err != nil {
			return reconcileStepOutput{}, fmt.Errorf("complete build %s: %w", buildID, err)
		}
		return reconcileStepOutput{BuildStatus: "completed"}, nil
	}

	launchedChunks, launchedMap, err := launchReconcileEligibleChunks(ctx, srv, buildID, chunkOrder, states, priorLaunched)
	if err != nil {
		return reconcileStepOutput{}, err
	}

	return reconcileStepOutput{
		BuildStatus:    b.Status,
		LaunchedChunks: launchedChunks,
		LaunchedMap:    launchedMap,
	}, nil
}

// DAGMutationSignalRequest is the input for the MutateDAG shared signal handler.
type DAGMutationSignalRequest struct {
	Action string `json:"action,omitempty"`
}

// BuildWorkflow implements the durable Restate DAG workflow for multi-chunk builds.
type BuildWorkflow struct {
	srv *VanadisServer
}

// NewBuildWorkflow creates a new BuildWorkflow instance bound to a VanadisServer.
func NewBuildWorkflow(srv *VanadisServer) *BuildWorkflow {
	return &BuildWorkflow{srv: srv}
}

// ServiceName returns the Restate service name for this workflow.
func (w *BuildWorkflow) ServiceName() string {
	return "BuildWorkflow"
}

type workflowPromise interface {
	Result() (string, restate.TerminalError)
	Resolve(value string) restate.TerminalError
}

type workflowAwakeable interface {
	Result() (string, restate.TerminalError)
}

type workflowSharedRuntime interface {
	Promise(name string) workflowPromise
}

type workflowRuntime interface {
	workflowSharedRuntime
	Run(fn func(context.Context) error) restate.TerminalError
	RunSnapshot(srv *VanadisServer, buildID string) (*workflowBuildSnapshot, restate.TerminalError)
	Awakeable() (string, workflowAwakeable)
	WaitFirst(futures ...any) (any, error)
}

type restateSharedRuntime struct {
	ctx restate.WorkflowSharedContext
}

func (r restateSharedRuntime) Promise(name string) workflowPromise {
	return restate.Promise[string](r.ctx, name)
}

type restateWorkflowRuntime struct {
	ctx restate.WorkflowContext
}

func (r restateWorkflowRuntime) Promise(name string) workflowPromise {
	return restate.Promise[string](r.ctx, name)
}

func (r restateWorkflowRuntime) Awakeable() (string, workflowAwakeable) {
	awk := restate.Awakeable[string](r.ctx)
	return awk.Id(), awk
}

func (r restateWorkflowRuntime) Run(fn func(context.Context) error) restate.TerminalError {
	return restate.RunVoid(r.ctx, func(rc restate.RunContext) error {
		return fn(rc)
	})
}

func (r restateWorkflowRuntime) RunSnapshot(srv *VanadisServer, buildID string) (*workflowBuildSnapshot, restate.TerminalError) {
	return restate.Run(r.ctx, func(rc restate.RunContext) (*workflowBuildSnapshot, error) {
		return fetchWorkflowSnapshot(rc, srv, buildID)
	})
}

func (r restateWorkflowRuntime) WaitFirst(futures ...any) (any, error) {
	rf := make([]restate.Future, 0, len(futures))
	for _, f := range futures {
		if fut, ok := f.(restate.Future); ok && fut != nil {
			rf = append(rf, fut)
		}
	}
	return restate.WaitFirst(r.ctx, rf...)
}

// Cancel is a shared workflow handler allowing external callers to signal cancellation.
func (w *BuildWorkflow) Cancel(ctx restate.WorkflowSharedContext, req CancelBuildWorkflowRequest) error {
	return w.cancelWithRuntime(restateSharedRuntime{ctx: ctx}, req)
}

func (w *BuildWorkflow) cancelWithRuntime(rt workflowSharedRuntime, req CancelBuildWorkflowRequest) error {
	reason := strings.TrimSpace(req.Reason)
	if reason == "" {
		reason = "cancelled"
	}
	return rt.Promise("cancel").Resolve(reason)
}

// MutateDAG is a shared workflow handler signaling that the DAG has been modified.
func (w *BuildWorkflow) MutateDAG(ctx restate.WorkflowSharedContext, req DAGMutationSignalRequest) error {
	return w.mutateDAGWithRuntime(restateSharedRuntime{ctx: ctx}, req)
}

func (w *BuildWorkflow) mutateDAGWithRuntime(rt workflowSharedRuntime, req DAGMutationSignalRequest) error {
	action := strings.TrimSpace(req.Action)
	if action == "" {
		action = "mutated"
	}
	if err := rt.Promise("dag_mutation").Resolve(action); err == nil {
		return nil
	}
	for i := 1; i <= 100; i++ {
		if err := rt.Promise(fmt.Sprintf("dag_mutation_%d", i)).Resolve(action); err == nil {
			return nil
		}
	}
	return errors.New("failed to resolve dag mutation signal")
}

type workflowChunkRuntimeState struct {
	chunk           VanadisChunk
	launched        bool
	codeReady       bool
	runtimeReady    bool
	mergeAwakeable  workflowAwakeable
	deployAwakeable workflowAwakeable
	actionAwakeable workflowAwakeable
}

type workflowBuildSnapshot struct {
	Build  *VanadisBuild  `json:"build"`
	Chunks []VanadisChunk `json:"chunks"`
}

// Run executes the durable build DAG workflow under Restate.
func (w *BuildWorkflow) Run(ctx restate.WorkflowContext, req RunBuildWorkflowRequest) (RunBuildWorkflowResult, error) {
	return w.runWithRuntime(restateWorkflowRuntime{ctx: ctx}, req)
}

func (w *BuildWorkflow) runWithRuntime(rt workflowRuntime, req RunBuildWorkflowRequest) (RunBuildWorkflowResult, error) {
	buildID := strings.TrimSpace(req.BuildID)
	if buildID == "" {
		return RunBuildWorkflowResult{}, restate.TerminalErrorf("build_id cannot be empty")
	}

	snapshot, rErr := rt.RunSnapshot(w.srv, buildID)
	if rErr != nil {
		return RunBuildWorkflowResult{BuildID: buildID, Status: "failed"}, rErr
	}

	if isTerminalBuildStatus(snapshot.Build.Status) {
		return terminalWorkflowResult(buildID, snapshot), nil
	}

	ensureBuildRunning(rt, w.srv, buildID, snapshot.Build.Status)

	chunkStates, chunkOrder := initializeWorkflowStates(snapshot.Chunks)

	return w.executeWorkflowLoop(rt, buildID, chunkOrder, chunkStates)
}

func ensureBuildRunning(rt workflowRuntime, srv *VanadisServer, buildID string, status string) {
	if status != "pending" {
		return
	}
	_ = rt.Run(func(ctx context.Context) error {
		return srv.store.UpdateBuildStatus(ctx, buildID, "running")
	})
}

func (w *BuildWorkflow) executeWorkflowLoop(rt workflowRuntime, buildID string, chunkOrder []string, chunkStates map[string]*workflowChunkRuntimeState) (RunBuildWorkflowResult, error) {
	cancelPromise := rt.Promise("cancel")
	dagMutationPromise := rt.Promise("dag_mutation")
	mutationCount := 0

	for {
		if isWorkflowBuildComplete(chunkOrder, chunkStates) {
			return completeWorkflowBuild(rt, w.srv, buildID, chunkOrder, chunkStates)
		}

		launchEligibleChunks(rt, w.srv, buildID, chunkOrder, chunkStates)

		waitFuts := buildWorkflowWaitFutures(cancelPromise, dagMutationPromise, chunkOrder, chunkStates)
		firstDone, waitErr := rt.WaitFirst(waitFuts...)
		if waitErr != nil {
			return RunBuildWorkflowResult{BuildID: buildID, Status: "failed"}, waitErr
		}

		if firstDone == cancelPromise {
			return cancelWorkflowBuild(rt, w.srv, buildID, cancelPromise, chunkOrder, chunkStates)
		}

		if firstDone == dagMutationPromise {
			mutationCount++
			dagMutationPromise = reloadWorkflowDAG(rt, w.srv, buildID, &chunkOrder, chunkStates, mutationCount)
			continue
		}

		if handled, pErr := processChunkAwakeable(rt, w.srv, buildID, firstDone, chunkOrder, chunkStates); handled {
			if pErr != nil {
				return RunBuildWorkflowResult{
					BuildID:         buildID,
					Status:          "failed",
					TotalChunks:     len(chunkOrder),
					CompletedChunks: countCompletedChunksFromStates(chunkStates),
				}, pErr
			}
		}
	}
}

func fetchWorkflowSnapshot(ctx context.Context, srv *VanadisServer, buildID string) (*workflowBuildSnapshot, error) {
	if srv == nil || srv.store == nil {
		return nil, errors.New("vanadis store not configured")
	}
	b, chs, err := srv.store.GetBuild(ctx, buildID)
	if err != nil {
		return nil, err
	}
	return &workflowBuildSnapshot{Build: b, Chunks: chs}, nil
}

func terminalWorkflowResult(buildID string, snapshot *workflowBuildSnapshot) RunBuildWorkflowResult {
	completedCount := 0
	for _, c := range snapshot.Chunks {
		if c.Status == "completed" || c.Status == "merged" {
			completedCount++
		}
	}
	return RunBuildWorkflowResult{
		BuildID:         buildID,
		Status:          snapshot.Build.Status,
		TotalChunks:     len(snapshot.Chunks),
		CompletedChunks: completedCount,
	}
}

func initializeWorkflowStates(chunks []VanadisChunk) (map[string]*workflowChunkRuntimeState, []string) {
	chunkStates := make(map[string]*workflowChunkRuntimeState, len(chunks))
	chunkOrder := make([]string, 0, len(chunks))
	for _, c := range chunks {
		chunkOrder = append(chunkOrder, c.ID)
		cs := &workflowChunkRuntimeState{chunk: c}
		switch strings.ToLower(c.Status) {
		case "completed":
			cs.launched = true
			cs.codeReady = true
			cs.runtimeReady = true
		case "merged":
			cs.launched = true
			cs.codeReady = true
			cs.runtimeReady = false
		}
		chunkStates[c.ID] = cs
	}
	return chunkStates, chunkOrder
}

func executeCompleteWorkflowBuild(ctx context.Context, srv *VanadisServer, buildID string) error {
	if srv == nil || srv.store == nil {
		return errors.New("vanadis store not configured")
	}
	prevBuild, _, prevErr := srv.store.GetBuild(ctx, buildID)
	alreadyCompleted := prevErr == nil && prevBuild != nil && prevBuild.Status == "completed"
	if err := srv.store.UpdateBuildStatus(ctx, buildID, "completed"); err != nil {
		return err
	}
	if !alreadyCompleted {
		RecordBuildTotal("completed")
	}
	b, chs, err := srv.store.GetBuild(ctx, buildID)
	if err == nil {
		if !alreadyCompleted {
			srv.publishExternalEvent(ctx, "vanadis_build", "completed", []string{buildID}, "")
			if !b.Quiet {
				res := &VanadisAdvancementResult{
					BuildID:   b.ID,
					ThreadID:  b.ThreadID,
					Objective: b.Objective,
					Tier:      b.Tier,
					Chunks:    chunksToSummaries(chs),
				}
				srv.dispatchCompletedBuildWake(ctx, res)
			}
		}
		srv.syncBuildWidget(ctx, buildID, "Build completed successfully")
	}
	return nil
}

func completeWorkflowBuild(rt workflowRuntime, srv *VanadisServer, buildID string, chunkOrder []string, chunkStates map[string]*workflowChunkRuntimeState) (RunBuildWorkflowResult, error) {
	_ = rt.Run(func(ctx context.Context) error {
		return executeCompleteWorkflowBuild(ctx, srv, buildID)
	})
	return RunBuildWorkflowResult{
		BuildID:         buildID,
		Status:          "completed",
		TotalChunks:     len(chunkOrder),
		CompletedChunks: countCompletedChunksFromStates(chunkStates),
	}, nil
}

func executeLaunchChunk(ctx context.Context, srv *VanadisServer, buildID string, chunk VanadisChunk, mergeAwkID, deployAwkID string) error {
	if srv == nil || srv.store == nil {
		return errors.New("vanadis store not configured")
	}
	if mergeAwkID != "" || deployAwkID != "" {
		if err := srv.store.SetChunkAwakeables(ctx, chunk.ID, mergeAwkID, deployAwkID); err != nil {
			return err
		}
	}
	if chunk.PRNumber > 0 || strings.EqualFold(chunk.Status, "pr_opened") {
		return nil
	}
	if err := srv.enqueueExecuteChunk(ctx, ExecuteChunkArgs{
		BuildID:    buildID,
		ChunkIndex: chunk.ChunkIndex,
		ChunkID:    chunk.ID,
	}); err != nil {
		return err
	}
	return srv.store.ReserveChunkForLaunch(ctx, chunk.ID)
}

func launchEligibleChunks(rt workflowRuntime, srv *VanadisServer, buildID string, chunkOrder []string, chunkStates map[string]*workflowChunkRuntimeState) {
	eligibleIDs := computeEligibleChunksToLaunch(chunkOrder, chunkStates, maxParallelChunks)
	if len(eligibleIDs) == 0 {
		return
	}

	for _, cid := range eligibleIDs {
		cs := chunkStates[cid]
		if strings.ToLower(cs.chunk.Mode) == "pr" {
			launchPRChunk(rt, srv, buildID, cs)
		} else {
			launchActionChunk(rt, srv, buildID, cs)
		}
	}

	_ = rt.Run(func(ctx context.Context) error {
		srv.syncBuildWidget(ctx, buildID, "")
		return nil
	})
}

func launchPRChunk(rt workflowRuntime, srv *VanadisServer, buildID string, cs *workflowChunkRuntimeState) {
	currChunk := cs.chunk
	cs.launched = true

	mAwkID, mAwk := rt.Awakeable()
	dAwkID, dAwk := rt.Awakeable()
	cs.mergeAwakeable = mAwk
	cs.deployAwakeable = dAwk

	_ = rt.Run(func(ctx context.Context) error {
		return executeLaunchChunk(ctx, srv, buildID, currChunk, mAwkID, dAwkID)
	})
}

func launchActionChunk(rt workflowRuntime, srv *VanadisServer, buildID string, cs *workflowChunkRuntimeState) {
	currChunk := cs.chunk
	cs.launched = true

	aAwkID, aAwk := rt.Awakeable()
	cs.actionAwakeable = aAwk

	_ = rt.Run(func(ctx context.Context) error {
		return executeLaunchChunk(ctx, srv, buildID, currChunk, "", aAwkID)
	})
}

func selectChunkWaitSlot(cs *workflowChunkRuntimeState) string {
	if cs == nil || !cs.launched {
		return ""
	}
	if strings.ToLower(cs.chunk.Mode) == "pr" {
		if !cs.codeReady {
			return "merge"
		}
		if !cs.runtimeReady {
			return "deploy"
		}
		return ""
	}
	if !cs.runtimeReady {
		return "action"
	}
	return ""
}

func buildWorkflowWaitFutures(cancelPromise, dagMutationPromise any, chunkOrder []string, chunkStates map[string]*workflowChunkRuntimeState) []any {
	waitFuts := make([]any, 0, len(chunkOrder)+2)
	waitFuts = append(waitFuts, cancelPromise, dagMutationPromise)

	for _, cid := range chunkOrder {
		cs := chunkStates[cid]
		switch selectChunkWaitSlot(cs) {
		case "merge":
			if cs.mergeAwakeable != nil {
				waitFuts = append(waitFuts, cs.mergeAwakeable)
			}
		case "deploy":
			if cs.deployAwakeable != nil {
				waitFuts = append(waitFuts, cs.deployAwakeable)
			}
		case "action":
			if cs.actionAwakeable != nil {
				waitFuts = append(waitFuts, cs.actionAwakeable)
			}
		}
	}
	return waitFuts
}

func executeCancelWorkflowBuild(ctx context.Context, srv *VanadisServer, buildID, reason string) error {
	if srv == nil || srv.store == nil {
		return errors.New("vanadis store not configured")
	}
	if err := srv.store.UpdateBuildStatus(ctx, buildID, "cancelled"); err != nil {
		log.Printf("[vanadis] warning: failed updating build status: %v", err)
	}
	srv.syncBuildWidget(ctx, buildID, "Build cancelled: "+reason)
	return nil
}

func cancelWorkflowBuild(rt workflowRuntime, srv *VanadisServer, buildID string, cancelPromise workflowPromise, chunkOrder []string, chunkStates map[string]*workflowChunkRuntimeState) (RunBuildWorkflowResult, error) {
	reason := "cancelled"
	if cancelPromise != nil {
		reason, _ = cancelPromise.Result()
	}
	_ = rt.Run(func(ctx context.Context) error {
		return executeCancelWorkflowBuild(ctx, srv, buildID, reason)
	})
	return RunBuildWorkflowResult{
		BuildID:         buildID,
		Status:          "cancelled",
		TotalChunks:     len(chunkOrder),
		CompletedChunks: countCompletedChunksFromStates(chunkStates),
	}, nil
}

func reconcileWorkflowDAGSnapshot(newSnap *workflowBuildSnapshot, chunkOrder *[]string, chunkStates map[string]*workflowChunkRuntimeState) {
	if newSnap == nil || chunkOrder == nil {
		return
	}
	newOrder := make([]string, 0, len(newSnap.Chunks))
	newIDs := make(map[string]bool, len(newSnap.Chunks))
	for _, c := range newSnap.Chunks {
		newOrder = append(newOrder, c.ID)
		newIDs[c.ID] = true
		if _, exists := chunkStates[c.ID]; !exists {
			chunkStates[c.ID] = &workflowChunkRuntimeState{chunk: c}
		} else {
			chunkStates[c.ID].chunk = c
		}
	}
	for cid := range chunkStates {
		if !newIDs[cid] {
			delete(chunkStates, cid)
		}
	}
	*chunkOrder = newOrder
}

func reloadWorkflowDAG(rt workflowRuntime, srv *VanadisServer, buildID string, chunkOrder *[]string, chunkStates map[string]*workflowChunkRuntimeState, count int) workflowPromise {
	newSnap, err := rt.RunSnapshot(srv, buildID)
	if err == nil && newSnap != nil {
		reconcileWorkflowDAGSnapshot(newSnap, chunkOrder, chunkStates)
	}
	return rt.Promise(fmt.Sprintf("dag_mutation_%d", count))
}

func processChunkAwakeable(rt workflowRuntime, srv *VanadisServer, buildID string, firstDone any, chunkOrder []string, chunkStates map[string]*workflowChunkRuntimeState) (bool, error) {
	if firstDone == nil {
		return false, nil
	}
	for _, cid := range chunkOrder {
		cs := chunkStates[cid]
		if !cs.launched {
			continue
		}

		var eventType, val string
		var awkErr error
		switch {
		case cs.mergeAwakeable != nil && firstDone == cs.mergeAwakeable:
			eventType = "merge"
			val, awkErr = cs.mergeAwakeable.Result()
		case cs.deployAwakeable != nil && firstDone == cs.deployAwakeable:
			eventType = "deploy"
			val, awkErr = cs.deployAwakeable.Result()
		case cs.actionAwakeable != nil && firstDone == cs.actionAwakeable:
			eventType = "action"
			val, awkErr = cs.actionAwakeable.Result()
		default:
			continue
		}

		status, err := handleAwakeableOutcome(rt, srv, buildID, cs, eventType, val, awkErr)
		if status == "failed" {
			if err == nil {
				err = restate.TerminalErrorf("chunk %s %s awakeable resolved to %s", cs.chunk.ID, eventType, val)
			} else {
				err = restate.TerminalErrorf("chunk %s %s awakeable error: %v", cs.chunk.ID, eventType, err)
			}
			return true, err
		}
		return true, nil
	}
	return false, nil
}

func executeAwakeableStatusUpdate(ctx context.Context, srv *VanadisServer, buildID, chunkID, title, eventType, newStatus string) error {
	if srv == nil || srv.store == nil {
		return errors.New("vanadis store not configured")
	}
	if newStatus == "failed" {
		if uErr := srv.store.UpdateChunkStatus(ctx, chunkID, "failed"); uErr != nil {
			log.Printf("[vanadis] warning: failed updating chunk status: %v", uErr)
		}
		if uErr := srv.store.UpdateBuildStatus(ctx, buildID, "failed"); uErr != nil {
			log.Printf("[vanadis] warning: failed updating build status: %v", uErr)
		}
		srv.syncBuildWidget(ctx, buildID, fmt.Sprintf("Chunk %s %s failed", title, eventType))
		srv.publishExternalEvent(ctx, "vanadis_chunk", "failed", []string{chunkID}, eventType)
		srv.publishExternalEvent(ctx, "vanadis_build", "failed", []string{buildID}, eventType)
		return nil
	}
	if uErr := srv.store.UpdateChunkStatus(ctx, chunkID, newStatus); uErr != nil {
		log.Printf("[vanadis] warning: failed updating chunk status: %v", uErr)
	}
	switch newStatus {
	case "completed":
		srv.publishExternalEvent(ctx, "vanadis_chunk", "completed", []string{chunkID}, "")
	case "merged":
		srv.publishExternalEvent(ctx, "vanadis_chunk", "merged", []string{chunkID}, "")
	}
	srv.syncBuildWidget(ctx, buildID, "")
	return nil
}

func handleAwakeableOutcome(rt workflowRuntime, srv *VanadisServer, buildID string, cs *workflowChunkRuntimeState, eventType, val string, inErr error) (string, error) {
	newStatus, aErr := applyAwakeableResolution(cs, eventType, val, inErr)
	_ = rt.Run(func(ctx context.Context) error {
		return executeAwakeableStatusUpdate(ctx, srv, buildID, cs.chunk.ID, cs.chunk.Title, eventType, newStatus)
	})
	if newStatus == "failed" {
		return "failed", aErr
	}
	return newStatus, nil
}

func isWorkflowBuildComplete(chunkOrder []string, states map[string]*workflowChunkRuntimeState) bool {
	for _, cid := range chunkOrder {
		cs := states[cid]
		if cs == nil || !cs.runtimeReady {
			return false
		}
	}
	return true
}

func countActiveRunningChunks(chunkOrder []string, states map[string]*workflowChunkRuntimeState) int {
	activeCount := 0
	for _, cid := range chunkOrder {
		cs := states[cid]
		if cs != nil && cs.launched && !cs.codeReady {
			activeCount++
		}
	}
	return activeCount
}

func computeEligibleChunksToLaunch(chunkOrder []string, states map[string]*workflowChunkRuntimeState, maxParallel int) []string {
	activeCount := countActiveRunningChunks(chunkOrder, states)
	canLaunch := maxParallel - activeCount
	if canLaunch <= 0 {
		return nil
	}

	var eligibleIDs []string
	for _, cid := range chunkOrder {
		if canLaunch <= 0 {
			break
		}
		cs := states[cid]
		if cs == nil || cs.launched || cs.runtimeReady {
			continue
		}

		eligible := true
		for _, depID := range cs.chunk.DependsOn {
			depState, ok := states[depID]
			if !ok {
				eligible = false
				break
			}
			mode := strings.ToLower(cs.chunk.Mode)
			if mode == "pr" {
				if !depState.codeReady {
					eligible = false
					break
				}
			} else if !depState.runtimeReady {
				eligible = false
				break
			}
		}

		if eligible {
			eligibleIDs = append(eligibleIDs, cid)
			canLaunch--
		}
	}
	return eligibleIDs
}

func applyAwakeableResolution(cs *workflowChunkRuntimeState, eventType, val string, err error) (string, error) {
	if cs == nil {
		return "", errors.New("chunk state is nil")
	}
	if err != nil || strings.EqualFold(val, "failed") || strings.EqualFold(val, "cancelled") {
		return "failed", err
	}
	switch eventType {
	case "merge":
		cs.codeReady = true
		return "merged", nil
	case "deploy":
		cs.codeReady = true
		cs.runtimeReady = true
		return "completed", nil
	case "action":
		cs.codeReady = true
		cs.runtimeReady = true
		return "completed", nil
	default:
		return "", fmt.Errorf("unknown awakeable event type: %s", eventType)
	}
}

func countCompletedChunksFromStates(states map[string]*workflowChunkRuntimeState) int {
	count := 0
	for _, s := range states {
		if s != nil && s.runtimeReady {
			count++
		}
	}
	return count
}

func chunksToSummaries(list []VanadisChunk) []VanadisChunkSummary {
	summaries := make([]VanadisChunkSummary, len(list))
	for i, c := range list {
		summaries[i] = VanadisChunkSummary{
			ID:          c.ID,
			ChunkIndex:  c.ChunkIndex,
			Mode:        c.Mode,
			Repo:        c.Repo,
			Title:       c.Title,
			Status:      c.Status,
			StartedAt:   c.StartedAt,
			CompletedAt: c.CompletedAt,
		}
	}
	return summaries
}

func startRestateServer(ctx context.Context, srv *VanadisServer) (*server.Restate, error) {
	if srv == nil {
		return nil, errors.New("vanadis server is nil")
	}
	port := strings.TrimSpace(srv.restateCfg.RestatePort)
	if port == "disabled" || port == "-1" {
		return nil, ErrRestateDisabled
	}
	if port == "" {
		port = "9080"
	}
	if (srv.cfg.Port == "0" || srv.cfg.Port == "-1") && port == "9080" {
		port = "0"
	}
	restateServer := server.NewRestate().
		Bind(restate.Reflect(NewBuildWorkflow(srv))).
		Bind(restate.Reflect(NewBuildReconciler(srv)))
	go func() {
		log.Printf("[vanadis] Restate HTTP/2 server listening on :%s", port)
		if err := restateServer.Start(ctx, ":"+port); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("[vanadis] Restate server error: %v", err)
		}
	}()

	if srv.restateCfg.RestateAdminURL != "" && srv.restateCfg.RestateAdvertiseURL != "" {
		go autoRegisterRestateDeployment(ctx, srv.restateCfg.RestateAdminURL, srv.restateCfg.RestateAdvertiseURL)
	}

	return restateServer, nil
}

func autoRegisterRestateDeployment(ctx context.Context, adminURL, advertiseURL string) {
	backoff := 500 * time.Millisecond
	for range 10 {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if err := registerRestateDeployment(ctx, adminURL, advertiseURL); err != nil {
			log.Printf("[vanadis] Restate deployment registration attempt failed: %v", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
				backoff = min(backoff*2, 5*time.Second)
			}
		} else {
			log.Printf("[vanadis] Successfully registered Restate deployment: %s with %s", advertiseURL, adminURL)
			return
		}
	}
}

func registerRestateDeployment(ctx context.Context, adminURL, advertiseURL string) error {
	adminURL = strings.TrimRight(strings.TrimSpace(adminURL), "/")
	advertiseURL = strings.TrimSpace(advertiseURL)
	if adminURL == "" || advertiseURL == "" {
		return nil
	}
	payload, err := json.Marshal(map[string]any{
		"uri":   advertiseURL,
		"force": true,
	})
	if err != nil {
		return fmt.Errorf("marshal restate deployment payload: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, adminURL+"/deployments", bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("create restate deployment request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("post restate deployment: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, rErr := io.ReadAll(io.LimitReader(resp.Body, 1024))
		if rErr != nil {
			return fmt.Errorf("restate admin returned status %d (read error: %w)", resp.StatusCode, rErr)
		}
		return fmt.Errorf("restate admin returned status %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

func (s *VanadisServer) postRestateIngressJSON(ctx context.Context, path string, payload any) error {
	if s == nil {
		return errors.New("vanadis server is nil")
	}
	baseURL := strings.TrimRight(strings.TrimSpace(s.restateCfg.RestateIngressURL), "/")
	if baseURL == "" {
		return nil
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal restate ingress payload: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+path, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create restate ingress request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	client := s.httpClient
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("post restate ingress %s: %w", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, rErr := io.ReadAll(io.LimitReader(resp.Body, 1024))
		if rErr != nil {
			return fmt.Errorf("restate ingress %s returned status %d (read error: %w)", path, resp.StatusCode, rErr)
		}
		return fmt.Errorf("restate ingress %s returned status %d: %s", path, resp.StatusCode, string(respBody))
	}
	return nil
}

func (s *VanadisServer) startRestateBuildWorkflow(ctx context.Context, buildID string) error {
	buildID = strings.TrimSpace(buildID)
	if buildID == "" {
		return errors.New("build_id cannot be empty")
	}
	return s.postRestateIngressJSON(ctx, fmt.Sprintf("/BuildWorkflow/%s/Run/send", buildID), RunBuildWorkflowRequest{BuildID: buildID})
}

func (s *VanadisServer) cancelRestateBuildWorkflow(ctx context.Context, buildID, reason string) error {
	buildID = strings.TrimSpace(buildID)
	if buildID == "" {
		return nil
	}
	return s.postRestateIngressJSON(ctx, fmt.Sprintf("/BuildWorkflow/%s/Cancel/send", buildID), CancelBuildWorkflowRequest{Reason: reason})
}

func (s *VanadisServer) notifyRestateDAGMutation(ctx context.Context, buildID, action string) error {
	buildID = strings.TrimSpace(buildID)
	if buildID == "" {
		return nil
	}
	return s.postRestateIngressJSON(ctx, fmt.Sprintf("/BuildWorkflow/%s/MutateDAG/send", buildID), DAGMutationSignalRequest{Action: action})
}

func (s *VanadisServer) resolveRestateAwakeable(ctx context.Context, awakeableID, value string) error {
	awakeableID = strings.TrimSpace(awakeableID)
	if awakeableID == "" {
		return nil
	}
	return s.postRestateIngressJSON(ctx, fmt.Sprintf("/restate/awakeables/%s/resolve", awakeableID), value)
}

func (s *VanadisServer) launchBuildOnRestate(ctx context.Context, buildID string) error {
	return s.startRestateBuildWorkflow(ctx, buildID)
}

func (s *VanadisServer) getLocalLaunched(buildID string) map[string]bool {
	if s == nil {
		return nil
	}
	s.launchMu.Lock()
	defer s.launchMu.Unlock()
	if s.localLaunched == nil {
		return nil
	}
	prior := s.localLaunched[buildID]
	if prior == nil {
		return nil
	}
	cp := make(map[string]bool, len(prior))
	maps.Copy(cp, prior)
	return cp
}

func (s *VanadisServer) setLocalLaunched(buildID, status string, launched map[string]bool) {
	if s == nil {
		return
	}
	s.launchMu.Lock()
	defer s.launchMu.Unlock()
	if s.localLaunched == nil {
		s.localLaunched = make(map[string]map[string]bool)
	}
	if status == "completed" || status == "failed" || status == "cancelled" || status == "not_found" {
		delete(s.localLaunched, buildID)
		return
	}
	cp := make(map[string]bool, len(launched))
	maps.Copy(cp, launched)
	s.localLaunched[buildID] = cp
}

func reconstructLaunchedChunks(chunks []VanadisChunk) map[string]bool {
	launched := make(map[string]bool)
	for _, c := range chunks {
		if c.PRNumber > 0 {
			launched[c.ID] = true
			continue
		}
		statusLower := strings.ToLower(strings.TrimSpace(c.Status))
		switch statusLower {
		case "completed", "merged", "deploying", "deploy_failed", "pr_opened", "coding", "ci_failed", "conflict":
			launched[c.ID] = true
		case "running":
			if c.ChunkIndex > 0 {
				launched[c.ID] = true
				continue
			}
			if !c.UpdatedAt.IsZero() && !c.CreatedAt.IsZero() && c.UpdatedAt.After(c.CreatedAt) {
				launched[c.ID] = true
				continue
			}
			for _, other := range chunks {
				if other.ID != c.ID && (isChunkDoneStatus(other.Status) || other.PRNumber > 0) {
					launched[c.ID] = true
					break
				}
			}
		}
	}
	return launched
}

func (s *VanadisServer) launchCreatedBuild(ctx context.Context, buildID string) error {
	if s != nil && strings.TrimSpace(s.restateCfg.RestateIngressURL) != "" {
		return s.startRestateBuildWorkflow(ctx, buildID)
	}
	return s.reconcileBuild(ctx, buildID, "created")
}

func (s *VanadisServer) notifyRestateActionChunkCompleted(ctx context.Context, chunkID string) error {
	if s == nil || s.store == nil || strings.TrimSpace(chunkID) == "" {
		return nil
	}
	chunk, _, err := s.store.GetChunk(ctx, chunkID)
	if err != nil {
		if errors.Is(err, ErrChunkNotFound) {
			return nil
		}
		return err
	}
	if chunk == nil {
		return nil
	}
	if chunk.Mode != "action" || chunk.Status != "completed" {
		return nil
	}
	// Action chunk with an attached PR must not be completed at end of dispatch turn;
	// it must await deploy_success resolving its deploy_awakeable_id.
	if chunk.PRNumber > 0 {
		log.Printf("[vanadis] action chunk %s has attached PR #%d; skipping end-of-turn awakeable resolution (awaiting PR deployment)", chunk.ID, chunk.PRNumber)
		return nil
	}
	if strings.TrimSpace(chunk.DeployAwakeableID) != "" {
		if err := s.resolveRestateAwakeable(ctx, chunk.DeployAwakeableID, "completed"); err != nil {
			log.Printf("[vanadis] warning: failed resolving action awakeable for chunk %s: %v", chunk.ID, err)
			return err
		}
		return nil
	}
	if err := s.reconcileBuild(ctx, chunk.BuildID, "action_completed"); err != nil {
		log.Printf("[vanadis] warning: failed reconciling build %s on action %s completed: %v", chunk.BuildID, chunk.ID, err)
		return err
	}
	return nil
}

func (s *VanadisServer) reconcileBuild(ctx context.Context, buildID, reason string) error {
	buildID = strings.TrimSpace(buildID)
	if buildID == "" {
		return errors.New("cannot reconcile empty build id")
	}
	if s == nil || s.store == nil {
		return errors.New("vanadis store not configured")
	}
	if strings.TrimSpace(s.restateCfg.RestateIngressURL) != "" {
		return s.reconcileBuildOnRestate(ctx, buildID, reason)
	}
	priorLaunched := s.getLocalLaunched(buildID)
	stepOut, err := executeReconcileBuild(ctx, s, buildID, priorLaunched)
	if err == nil {
		s.setLocalLaunched(buildID, stepOut.BuildStatus, stepOut.LaunchedMap)
	}
	return err
}

func (s *VanadisServer) reconcileBuildOnRestate(ctx context.Context, buildID, reason string) error {
	buildID = strings.TrimSpace(buildID)
	if buildID == "" {
		return errors.New("build_id cannot be empty")
	}
	return s.postRestateIngressJSON(ctx, fmt.Sprintf("/BuildReconciler/%s/Reconcile/send", buildID), ReconcileBuildRequest{Reason: reason})
}

func (s *VanadisServer) notifyRestateChunkFailed(ctx context.Context, chunkID string) {
	if s == nil || s.store == nil || strings.TrimSpace(s.restateCfg.RestateIngressURL) == "" || strings.TrimSpace(chunkID) == "" {
		return
	}
	chunk, _, err := s.store.GetChunk(ctx, chunkID)
	if err != nil || chunk == nil {
		return
	}
	if chunk.MergeAwakeableID != "" {
		if err := s.resolveRestateAwakeable(ctx, chunk.MergeAwakeableID, "failed"); err != nil {
			log.Printf("[vanadis] warning: failed resolving merge awakeable failure for chunk %s: %v", chunk.ID, err)
		}
	}
	if chunk.DeployAwakeableID != "" {
		if err := s.resolveRestateAwakeable(ctx, chunk.DeployAwakeableID, "failed"); err != nil {
			log.Printf("[vanadis] warning: failed resolving deploy awakeable failure for chunk %s: %v", chunk.ID, err)
		}
	}
}
