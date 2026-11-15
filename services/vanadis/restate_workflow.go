package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
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
	return RestateConfig{
		RestatePort:         firstNonEmpty(cleanRestatePortEnv(os.Getenv("VANADIS_RESTATE_PORT")), cleanRestatePortEnv(os.Getenv("RESTATE_PORT")), fileCfg.RestatePort, "9080"),
		RestateIngressURL:   strings.TrimRight(strings.TrimSpace(firstNonEmpty(os.Getenv("RESTATE_INGRESS_URL"), fileCfg.RestateIngressURL)), "/"),
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
	if err := srv.store.UpdateBuildStatus(ctx, buildID, "completed"); err != nil {
		return err
	}
	RecordBuildTotal("completed")
	b, chs, err := srv.store.GetBuild(ctx, buildID)
	if err == nil {
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
	if err := srv.store.SetChunkAwakeables(ctx, chunk.ID, mergeAwkID, deployAwkID); err != nil {
		return err
	}
	if err := srv.store.UpdateChunkStatus(ctx, chunk.ID, "running"); err != nil {
		return err
	}
	return srv.enqueueExecuteChunk(ctx, ExecuteChunkArgs{
		BuildID:    buildID,
		ChunkIndex: chunk.ChunkIndex,
	})
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
		return nil
	}
	if uErr := srv.store.UpdateChunkStatus(ctx, chunkID, newStatus); uErr != nil {
		log.Printf("[vanadis] warning: failed updating chunk status: %v", uErr)
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
	restateServer := server.NewRestate().Bind(restate.Reflect(NewBuildWorkflow(srv)))
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

func (s *VanadisServer) launchCreatedBuild(ctx context.Context, buildID string, useRestate bool) error {
	if s != nil && useRestate && strings.TrimSpace(s.restateCfg.RestateIngressURL) != "" {
		return s.startRestateBuildWorkflow(ctx, buildID)
	}
	return s.enqueueExecuteChunk(ctx, ExecuteChunkArgs{BuildID: buildID, ChunkIndex: 0})
}

func findChunkByIndex(chunks []VanadisChunk, idx int) *VanadisChunk {
	for i := range chunks {
		if chunks[i].ChunkIndex == idx {
			return &chunks[i]
		}
	}
	return nil
}

func (s *VanadisServer) notifyRestateChunkPRAdvanced(ctx context.Context, res *VanadisAdvancementResult) bool {
	if s == nil || s.store == nil || strings.TrimSpace(s.restateCfg.RestateIngressURL) == "" || res == nil || !res.Matched {
		return false
	}
	_, chunks, err := s.store.GetBuild(ctx, res.BuildID)
	if err != nil {
		return false
	}
	mergedChunk := findChunkByIndex(chunks, res.MergedChunkIndex)
	if mergedChunk == nil || (mergedChunk.MergeAwakeableID == "" && mergedChunk.DeployAwakeableID == "") {
		return false
	}
	if res.Advanced && mergedChunk.MergeAwakeableID != "" {
		if err := s.resolveRestateAwakeable(ctx, mergedChunk.MergeAwakeableID, "merged"); err != nil {
			log.Printf("[vanadis] warning: failed resolving merge awakeable for chunk %s: %v", mergedChunk.ID, err)
		}
	}
	if mergedChunk.DeployAwakeableID != "" {
		if err := s.resolveRestateAwakeable(ctx, mergedChunk.DeployAwakeableID, "deploy_success"); err != nil {
			log.Printf("[vanadis] warning: failed resolving deploy awakeable for chunk %s: %v", mergedChunk.ID, err)
		}
	}
	return true
}

func (s *VanadisServer) notifyRestateActionChunkCompleted(ctx context.Context, chunkID string) bool {
	if s == nil || s.store == nil || strings.TrimSpace(s.restateCfg.RestateIngressURL) == "" || strings.TrimSpace(chunkID) == "" {
		return false
	}
	chunk, _, err := s.store.GetChunk(ctx, chunkID)
	if err != nil || chunk == nil || chunk.DeployAwakeableID == "" {
		return false
	}
	if err := s.resolveRestateAwakeable(ctx, chunk.DeployAwakeableID, "completed"); err != nil {
		log.Printf("[vanadis] warning: failed resolving action awakeable for chunk %s: %v", chunk.ID, err)
	}
	return true
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
