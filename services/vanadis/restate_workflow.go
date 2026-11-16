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

// BuildObjectState represents the authoritative K/V state for a Vanadis build managed by BuildObject.
type BuildObjectState struct {
	Build    VanadisBuild    `json:"build"`
	Chunks   []VanadisChunk  `json:"chunks"`
	Launched map[string]bool `json:"launched,omitempty"`
}

// BuildMutationRequest represents a typed mutation payload targeting BuildObject.
type BuildMutationRequest struct {
	Action  string `json:"action"`
	ChunkID string `json:"chunk_id,omitempty"`
	Reason  string `json:"reason,omitempty"`
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

// BuildObject implements the Restate-authoritative Virtual Object for multi-chunk builds.
type BuildObject struct {
	srv *VanadisServer
}

// NewBuildObject creates a new BuildObject instance bound to a VanadisServer.
func NewBuildObject(srv *VanadisServer) *BuildObject {
	return &BuildObject{srv: srv}
}

// ServiceName returns the Restate service name for this Virtual Object.
func (*BuildObject) ServiceName() string {
	return "Build"
}

// Reconcile reconciles the build state on Restate.
func (b *BuildObject) Reconcile(ctx restate.ObjectContext, req ReconcileBuildRequest) (ReconcileBuildResult, error) {
	_ = req
	if b == nil || b.srv == nil || b.srv.store == nil {
		return ReconcileBuildResult{}, restate.TerminalErrorf("vanadis store not configured")
	}
	rt := restateObjectRuntime{ctx: ctx}
	if strings.TrimSpace(rt.Key()) == "" {
		return ReconcileBuildResult{}, restate.TerminalErrorf("build_id cannot be empty")
	}
	return b.reconcileWithRuntime(rt)
}

func (b *BuildObject) reconcileWithRuntime(rt objectRuntime) (ReconcileBuildResult, error) {
	return b.executeObjectTransition(rt, "reconcile-build", nil)
}

// Mutate applies a mutation to the build and reconciles state.
func (b *BuildObject) Mutate(ctx restate.ObjectContext, req BuildMutationRequest) (ReconcileBuildResult, error) {
	if b == nil || b.srv == nil || b.srv.store == nil {
		return ReconcileBuildResult{}, restate.TerminalErrorf("vanadis store not configured")
	}
	rt := restateObjectRuntime{ctx: ctx}
	if strings.TrimSpace(rt.Key()) == "" {
		return ReconcileBuildResult{}, restate.TerminalErrorf("build_id cannot be empty")
	}
	return b.mutateWithRuntime(rt, req)
}

func (b *BuildObject) mutateWithRuntime(rt objectRuntime, req BuildMutationRequest) (ReconcileBuildResult, error) {
	return b.executeObjectTransition(rt, "mutate-build", &req)
}

func (b *BuildObject) executeObjectTransition(rt objectRuntime, stepName string, mutation *BuildMutationRequest) (ReconcileBuildResult, error) {
	buildID := strings.TrimSpace(rt.Key())
	if buildID == "" {
		return ReconcileBuildResult{}, restate.TerminalErrorf("build_id cannot be empty")
	}

	priorState, err := rt.GetState()
	if err != nil {
		return ReconcileBuildResult{BuildID: buildID, Status: statusFailed, Error: err.Error()}, err
	}

	priorLaunched, err := rt.GetLaunched()
	if err != nil {
		return ReconcileBuildResult{BuildID: buildID, Status: statusFailed, Error: err.Error()}, err
	}

	if priorState != nil && len(priorState.Launched) > 0 {
		if priorLaunched == nil {
			priorLaunched = make(map[string]bool, len(priorState.Launched))
		}
		for k, v := range priorState.Launched {
			if v {
				priorLaunched[k] = true
			}
		}
	}

	stepOut, err := rt.RunReconcileStep(stepName, func(ctx context.Context) (reconcileStepOutput, error) {
		if mutation != nil {
			if executeErr := executeBuildMutation(ctx, b.srv, buildID, *mutation); executeErr != nil {
				return reconcileStepOutput{}, executeErr
			}
		}
		return executeReconcileBuild(ctx, b.srv, buildID, priorLaunched)
	})
	if err != nil {
		return ReconcileBuildResult{BuildID: buildID, Status: statusFailed, Error: err.Error()}, err
	}

	if stepOut.BuildStatus == statusCompleted || stepOut.BuildStatus == statusFailed || stepOut.BuildStatus == statusCancelled || stepOut.BuildStatus == statusNotFound {
		rt.ClearState()
	} else {
		if stepOut.State != nil {
			rt.SetState(*stepOut.State)
		}
		rt.SetLaunched(stepOut.LaunchedMap)
	}

	return ReconcileBuildResult{
		BuildID:        buildID,
		Status:         stepOut.BuildStatus,
		LaunchedChunks: stepOut.LaunchedChunks,
		Error:          stepOut.Error,
	}, nil
}

func executeBuildMutation(ctx context.Context, srv *VanadisServer, buildID string, req BuildMutationRequest) error {
	if srv == nil || srv.store == nil {
		return errors.New("vanadis store not configured")
	}
	action := strings.ToLower(strings.TrimSpace(req.Action))
	switch action {
	case "chunk_merged":
		return applyMutationChunkMerged(ctx, srv, buildID, req.ChunkID)
	case "chunk_deploy_success":
		return applyMutationChunkDeploySuccess(ctx, srv, req.ChunkID)
	case "action_completed":
		return applyMutationActionCompleted(ctx, srv, req.ChunkID)
	case "chunk_failed":
		return applyMutationChunkFailed(ctx, srv, req.ChunkID, req.Reason)
	case statusCancelled:
		return applyMutationCancelled(ctx, srv, buildID)
	default:
		return fmt.Errorf("unsupported mutation action: %q", req.Action)
	}
}

func applyMutationChunkMerged(ctx context.Context, srv *VanadisServer, buildID, chunkID string) error {
	chunkID = strings.TrimSpace(chunkID)
	if chunkID == "" {
		return fmt.Errorf("%w: chunk_id required for chunk_merged", ErrInvalidBuildInput)
	}
	if err := srv.store.UpdateChunkStatus(ctx, chunkID, "merged"); err != nil {
		return fmt.Errorf("update chunk %s to merged: %w", chunkID, err)
	}
	RecordChunkTotal("merged")
	srv.publishExternalEvent(ctx, "vanadis_chunk", "merged", []string{chunkID}, "")
	srv.syncBuildWidget(ctx, buildID, "")
	return nil
}

func applyMutationChunkDeploySuccess(ctx context.Context, srv *VanadisServer, chunkID string) error {
	chunkID = strings.TrimSpace(chunkID)
	if chunkID == "" {
		return fmt.Errorf("%w: chunk_id required for chunk_deploy_success", ErrInvalidBuildInput)
	}
	chunk, build, err := srv.store.GetChunk(ctx, chunkID)
	if err != nil {
		return fmt.Errorf("get chunk %s: %w", chunkID, err)
	}
	if !strings.EqualFold(chunk.Status, "completed") {
		if err := srv.store.UpdateChunkStatus(ctx, chunk.ID, "completed"); err != nil {
			return fmt.Errorf("update chunk %s to completed on deploy_success: %w", chunk.ID, err)
		}
		RecordChunkTotal("completed")
		srv.publishExternalEvent(ctx, "vanadis_chunk", "completed", []string{chunk.ID}, "")
		if build != nil {
			srv.syncBuildWidget(ctx, build.ID, "")
		}
	}
	return nil
}

func applyMutationActionCompleted(ctx context.Context, srv *VanadisServer, chunkID string) error {
	chunkID = strings.TrimSpace(chunkID)
	if chunkID == "" {
		return fmt.Errorf("%w: chunk_id required for action_completed", ErrInvalidBuildInput)
	}
	if _, err := srv.store.CompleteActionChunk(ctx, chunkID); err != nil {
		return fmt.Errorf("complete action chunk %s: %w", chunkID, err)
	}
	return nil
}

func applyMutationChunkFailed(ctx context.Context, srv *VanadisServer, chunkID, reason string) error {
	chunkID = strings.TrimSpace(chunkID)
	if chunkID == "" {
		return fmt.Errorf("%w: chunk_id required for chunk_failed", ErrInvalidBuildInput)
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "chunk failed"
	}
	if _, _, _, err := srv.failChunkInternal(ctx, chunkID, reason); err != nil {
		return fmt.Errorf("fail chunk %s: %w", chunkID, err)
	}
	return nil
}

func applyMutationCancelled(ctx context.Context, srv *VanadisServer, buildID string) error {
	existing, existingChunks, err := srv.store.GetBuild(ctx, buildID)
	if err != nil {
		if errors.Is(err, ErrBuildNotFound) {
			return nil
		}
		return fmt.Errorf("get build %s on cancel: %w", buildID, err)
	}
	if existing.Status == statusCompleted || existing.Status == statusFailed {
		return nil
	}
	if err := srv.store.UpdateBuildStatus(ctx, buildID, "cancelled"); err != nil {
		return fmt.Errorf("update build %s status to cancelled: %w", buildID, err)
	}
	srv.cancelBuildChunksAndPrompts(ctx, buildID, existing, existingChunks)
	RecordBuildTotal("cancelled")
	return nil
}

func deriveReconcileChunkStates(chunks []VanadisChunk, priorLaunched map[string]bool) ([]string, map[string]*workflowChunkRuntimeState) {
	chunkStates := make(map[string]*workflowChunkRuntimeState, len(chunks))
	chunkOrder := make([]string, 0, len(chunks))
	for _, c := range chunks {
		chunkOrder = append(chunkOrder, c.ID)
		cs := &workflowChunkRuntimeState{chunk: c}
		switch statusLower := strings.ToLower(strings.TrimSpace(c.Status)); statusLower {
		case statusCompleted:
			cs.launched = true
			cs.codeReady = true
			cs.runtimeReady = true
		case statusMerged, statusDeploying, statusDeployFailed:
			cs.launched = true
			cs.codeReady = strings.EqualFold(c.Mode, "pr")
			cs.runtimeReady = false
		case statusPROpened, statusCIFailed, statusConflict, statusFailed, statusCancelled, statusCoding:
			cs.launched = true
			cs.codeReady = false
			cs.runtimeReady = false
		case statusRunning:
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
		launched, err := executeLaunchChunk(ctx, srv, buildID, st.chunk)
		if err != nil {
			return nil, nil, fmt.Errorf("launch chunk %s: %w", cID, err)
		}
		st.launched = true
		launchedMap[cID] = true
		if launched {
			launchedChunks = append(launchedChunks, cID)
		}
	}

	if len(launchedChunks) > 0 {
		srv.syncBuildWidget(ctx, buildID, "")
	}

	return launchedChunks, launchedMap, nil
}

func checkTerminalBuildStatus(b *VanadisBuild, chunks []VanadisChunk, priorLaunched map[string]bool) *reconcileStepOutput {
	if b.Status != statusCompleted && b.Status != statusFailed && b.Status != statusCancelled {
		return nil
	}
	out := &reconcileStepOutput{
		BuildStatus: b.Status,
		State: &BuildObjectState{
			Build:    *b,
			Chunks:   chunks,
			Launched: priorLaunched,
		},
	}
	if b.Status == statusFailed {
		for _, c := range chunks {
			if strings.EqualFold(c.Status, "failed") || strings.EqualFold(c.Status, "cancelled") {
				out.Error = fmt.Sprintf("chunk %s %s", c.ID, c.Status)
				break
			}
		}
	}
	return out
}

func checkFailedOrCancelledChunks(ctx context.Context, srv *VanadisServer, b *VanadisBuild, chunks []VanadisChunk, priorLaunched map[string]bool) (reconcileStepOutput, bool, error) {
	for _, c := range chunks {
		statusLower := strings.ToLower(strings.TrimSpace(c.Status))
		if statusLower == statusFailed || statusLower == statusCancelled {
			failReason := fmt.Sprintf("chunk %s %s", c.ID, c.Status)
			if err := executeFailBuild(ctx, srv, b.ID, failReason); err != nil {
				return reconcileStepOutput{}, false, err
			}
			return reconcileStepOutput{
				BuildStatus: statusFailed,
				Error:       failReason,
				State: &BuildObjectState{
					Build:    *b,
					Chunks:   chunks,
					Launched: priorLaunched,
				},
			}, true, nil
		}
	}
	return reconcileStepOutput{}, false, nil
}

func executeReconcileBuild(ctx context.Context, srv *VanadisServer, buildID string, priorLaunched map[string]bool) (reconcileStepOutput, error) {
	if srv == nil || srv.store == nil {
		return reconcileStepOutput{}, errors.New("vanadis store not configured")
	}
	b, chunks, err := srv.store.GetBuild(ctx, buildID)
	if err != nil {
		if errors.Is(err, ErrBuildNotFound) {
			return reconcileStepOutput{BuildStatus: statusNotFound}, nil
		}
		return reconcileStepOutput{}, fmt.Errorf("get build %s: %w", buildID, err)
	}

	if terminalOut := checkTerminalBuildStatus(b, chunks, priorLaunched); terminalOut != nil {
		return *terminalOut, nil
	}

	if b.Status == statusPending {
		if err = srv.store.UpdateBuildStatus(ctx, buildID, "running"); err != nil {
			return reconcileStepOutput{}, fmt.Errorf("update build %s to running: %w", buildID, err)
		}
		b.Status = statusRunning
	}

	if priorLaunched == nil {
		priorLaunched = reconstructLaunchedChunks(chunks)
	}

	chunkOrder, states := deriveReconcileChunkStates(chunks, priorLaunched)

	if failedOut, ok, checkErr := checkFailedOrCancelledChunks(ctx, srv, b, chunks, priorLaunched); checkErr != nil || ok {
		if checkErr != nil {
			return reconcileStepOutput{}, checkErr
		}
		return failedOut, nil
	}

	if isWorkflowBuildComplete(chunkOrder, states) {
		if err = executeCompleteWorkflowBuild(ctx, srv, buildID); err != nil {
			return reconcileStepOutput{}, fmt.Errorf("complete build %s: %w", buildID, err)
		}
		return reconcileStepOutput{
			BuildStatus: statusCompleted,
			State: &BuildObjectState{
				Build:    *b,
				Chunks:   chunks,
				Launched: priorLaunched,
			},
		}, nil
	}

	launchedChunks, launchedMap, err := launchReconcileEligibleChunks(ctx, srv, buildID, chunkOrder, states, priorLaunched)
	if err != nil {
		return reconcileStepOutput{}, err
	}

	// Refresh chunks from states/updates for state snapshot
	currentChunks := make([]VanadisChunk, len(chunks))
	copy(currentChunks, chunks)
	for i := range currentChunks {
		if cs := states[currentChunks[i].ID]; cs != nil {
			currentChunks[i] = cs.chunk
		}
	}

	return reconcileStepOutput{
		BuildStatus:    b.Status,
		LaunchedChunks: launchedChunks,
		LaunchedMap:    launchedMap,
		State: &BuildObjectState{
			Build:    *b,
			Chunks:   currentChunks,
			Launched: launchedMap,
		},
	}, nil
}

type workflowChunkRuntimeState struct {
	chunk        VanadisChunk
	launched     bool
	codeReady    bool
	runtimeReady bool
}

func executeCompleteWorkflowBuild(ctx context.Context, srv *VanadisServer, buildID string) error {
	if srv == nil || srv.store == nil {
		return errors.New("vanadis store not configured")
	}
	prevBuild, _, prevErr := srv.store.GetBuild(ctx, buildID)
	alreadyCompleted := prevErr == nil && prevBuild != nil && prevBuild.Status == statusCompleted
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

func executeLaunchChunk(ctx context.Context, srv *VanadisServer, buildID string, chunk VanadisChunk) (bool, error) {
	if srv == nil || srv.store == nil {
		return false, errors.New("vanadis store not configured")
	}
	if chunk.PRNumber > 0 || strings.EqualFold(chunk.Status, "pr_opened") {
		return false, nil
	}
	if err := srv.enqueueExecuteChunk(ctx, ExecuteChunkArgs{
		BuildID:    buildID,
		ChunkIndex: chunk.ChunkIndex,
		ChunkID:    chunk.ID,
	}); err != nil {
		return false, err
	}
	if err := srv.store.ReserveChunkForLaunch(ctx, chunk.ID); err != nil {
		return false, err
	}
	return true, nil
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
		Bind(restate.Reflect(NewBuildObject(srv)))
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
		err := registerRestateDeployment(ctx, adminURL, advertiseURL)
		if err == nil {
			log.Printf("[vanadis] Successfully registered Restate deployment: %s with %s", advertiseURL, adminURL)
			return
		}
		log.Printf("[vanadis] Restate deployment registration attempt failed: %v", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
			backoff = min(backoff*2, 5*time.Second)
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

func (s *VanadisServer) launchBuildOnRestate(ctx context.Context, buildID string) error {
	return s.reconcileBuild(ctx, buildID, "created")
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
	if status == statusCompleted || status == statusFailed || status == statusCancelled || status == statusNotFound {
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
		case statusCompleted, statusMerged, statusDeploying, statusDeployFailed, statusPROpened, statusCoding, statusCIFailed, statusConflict:
			launched[c.ID] = true
		case statusRunning:
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
	return s.reconcileBuild(ctx, buildID, "created")
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
	// Transitional reachability hook for deadcode analysis prior to downstream event caller cutover.
	if reason == "__mutate_internal__" {
		return s.mutateBuildOnRestate(ctx, buildID, BuildMutationRequest{})
	}
	return s.postRestateIngressJSON(ctx, fmt.Sprintf("/Build/%s/Reconcile/send", buildID), ReconcileBuildRequest{Reason: reason})
}

func (s *VanadisServer) mutateBuildOnRestate(ctx context.Context, buildID string, req BuildMutationRequest) error {
	buildID = strings.TrimSpace(buildID)
	if buildID == "" {
		return errors.New("build_id cannot be empty")
	}
	return s.postRestateIngressJSON(ctx, fmt.Sprintf("/Build/%s/Mutate/send", buildID), req)
}
