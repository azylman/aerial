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
	"strings"
	"time"

	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/server"
)

const maxParallelChunks = 4

// ErrRestateDisabled is returned when the Restate server is intentionally disabled.
var ErrRestateDisabled = errors.New("restate server is disabled")

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

// Cancel is a shared workflow handler allowing external callers to signal cancellation.
func (w *BuildWorkflow) Cancel(ctx restate.WorkflowSharedContext, req CancelBuildWorkflowRequest) error {
	reason := strings.TrimSpace(req.Reason)
	if reason == "" {
		reason = "cancelled"
	}
	if ctx == nil {
		return nil
	}
	return restate.Promise[string](ctx, "cancel").Resolve(reason)
}

// MutateDAG is a shared workflow handler signaling that the DAG has been modified.
func (w *BuildWorkflow) MutateDAG(ctx restate.WorkflowSharedContext, req DAGMutationSignalRequest) error {
	action := strings.TrimSpace(req.Action)
	if action == "" {
		action = "mutated"
	}
	if ctx == nil {
		return nil
	}
	if err := restate.Promise[string](ctx, "dag_mutation").Resolve(action); err == nil {
		return nil
	}
	for i := 1; i <= 100; i++ {
		p := restate.Promise[string](ctx, fmt.Sprintf("dag_mutation_%d", i))
		if err := p.Resolve(action); err == nil {
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
	mergeAwakeable  restate.AwakeableFuture[string]
	deployAwakeable restate.AwakeableFuture[string]
	actionAwakeable restate.AwakeableFuture[string]
}

type workflowBuildSnapshot struct {
	Build  *VanadisBuild  `json:"build"`
	Chunks []VanadisChunk `json:"chunks"`
}

// Run executes the durable build DAG workflow under Restate.
func (w *BuildWorkflow) Run(ctx restate.WorkflowContext, req RunBuildWorkflowRequest) (RunBuildWorkflowResult, error) {
	buildID := strings.TrimSpace(req.BuildID)
	if buildID == "" {
		return RunBuildWorkflowResult{}, restate.TerminalErrorf("build_id cannot be empty")
	}

	snapshot, rErr := loadWorkflowSnapshot(ctx, w.srv, buildID)
	if rErr != nil {
		return RunBuildWorkflowResult{BuildID: buildID, Status: "failed"}, rErr
	}

	if isTerminalBuildStatus(snapshot.Build.Status) {
		return terminalWorkflowResult(buildID, snapshot), nil
	}

	ensureBuildRunning(ctx, w.srv, buildID, snapshot.Build.Status)

	chunkStates, chunkOrder := initializeWorkflowStates(snapshot.Chunks)

	if ctx == nil {
		if isWorkflowBuildComplete(chunkOrder, chunkStates) {
			return completeWorkflowBuild(nil, w.srv, buildID, chunkOrder, chunkStates)
		}
		launchEligibleChunks(nil, w.srv, buildID, chunkOrder, chunkStates)
		return RunBuildWorkflowResult{
			BuildID:         buildID,
			Status:          "running",
			TotalChunks:     len(chunkOrder),
			CompletedChunks: countCompletedChunksFromStates(chunkStates),
		}, nil
	}

	return w.executeWorkflowLoop(ctx, buildID, chunkOrder, chunkStates)
}

func ensureBuildRunning(ctx restate.WorkflowContext, srv *VanadisServer, buildID string, status string) {
	if status != "pending" {
		return
	}
	if ctx == nil {
		if srv != nil && srv.store != nil {
			if err := srv.store.UpdateBuildStatus(context.Background(), buildID, "running"); err != nil {
				log.Printf("[vanadis] warning: failed updating build status to running: %v", err)
			}
		}
		return
	}
	_ = restate.RunVoid(ctx, func(rc restate.RunContext) error {
		return srv.store.UpdateBuildStatus(rc, buildID, "running")
	})
}

func (w *BuildWorkflow) executeWorkflowLoop(ctx restate.WorkflowContext, buildID string, chunkOrder []string, chunkStates map[string]*workflowChunkRuntimeState) (RunBuildWorkflowResult, error) {
	cancelPromise := restate.Promise[string](ctx, "cancel")
	dagMutationPromise := restate.Promise[string](ctx, "dag_mutation")
	mutationCount := 0

	for {
		if isWorkflowBuildComplete(chunkOrder, chunkStates) {
			return completeWorkflowBuild(ctx, w.srv, buildID, chunkOrder, chunkStates)
		}

		launchEligibleChunks(ctx, w.srv, buildID, chunkOrder, chunkStates)

		waitFuts := buildWorkflowWaitFutures(cancelPromise, dagMutationPromise, chunkOrder, chunkStates)
		firstDone, waitErr := restate.WaitFirst(ctx, waitFuts...)
		if waitErr != nil {
			return RunBuildWorkflowResult{BuildID: buildID, Status: "failed"}, waitErr
		}

		if firstDone == cancelPromise {
			return cancelWorkflowBuild(ctx, w.srv, buildID, cancelPromise, chunkOrder, chunkStates)
		}

		if firstDone == dagMutationPromise {
			mutationCount++
			dagMutationPromise = reloadWorkflowDAG(ctx, w.srv, buildID, &chunkOrder, chunkStates, mutationCount)
			continue
		}

		if handled, pErr := processChunkAwakeable(ctx, w.srv, buildID, firstDone, chunkOrder, chunkStates); handled {
			if pErr != nil {
				return RunBuildWorkflowResult{BuildID: buildID, Status: "failed"}, pErr
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

func loadWorkflowSnapshot(ctx restate.WorkflowContext, srv *VanadisServer, buildID string) (*workflowBuildSnapshot, restate.TerminalError) {
	if ctx == nil {
		snap, err := fetchWorkflowSnapshot(context.Background(), srv, buildID)
		if err != nil {
			return nil, restate.TerminalErrorf("%v", err)
		}
		return snap, nil
	}
	return restate.Run(ctx, func(rc restate.RunContext) (*workflowBuildSnapshot, error) {
		return fetchWorkflowSnapshot(rc, srv, buildID)
	})
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

func completeWorkflowBuild(ctx restate.WorkflowContext, srv *VanadisServer, buildID string, chunkOrder []string, chunkStates map[string]*workflowChunkRuntimeState) (RunBuildWorkflowResult, error) {
	if ctx == nil {
		if err := executeCompleteWorkflowBuild(context.Background(), srv, buildID); err != nil {
			return RunBuildWorkflowResult{BuildID: buildID, Status: "failed"}, err
		}
	} else {
		_ = restate.RunVoid(ctx, func(rc restate.RunContext) error {
			return executeCompleteWorkflowBuild(rc, srv, buildID)
		})
	}
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

func launchEligibleChunks(ctx restate.WorkflowContext, srv *VanadisServer, buildID string, chunkOrder []string, chunkStates map[string]*workflowChunkRuntimeState) {
	eligibleIDs := computeEligibleChunksToLaunch(chunkOrder, chunkStates, maxParallelChunks)
	if len(eligibleIDs) == 0 {
		return
	}

	for _, cid := range eligibleIDs {
		cs := chunkStates[cid]
		if strings.ToLower(cs.chunk.Mode) == "pr" {
			launchPRChunk(ctx, srv, buildID, cs)
		} else {
			launchActionChunk(ctx, srv, buildID, cs)
		}
	}

	if ctx == nil {
		if srv != nil {
			srv.syncBuildWidget(context.Background(), buildID, "")
		}
		return
	}
	_ = restate.RunVoid(ctx, func(rc restate.RunContext) error {
		srv.syncBuildWidget(rc, buildID, "")
		return nil
	})
}

func launchPRChunk(ctx restate.WorkflowContext, srv *VanadisServer, buildID string, cs *workflowChunkRuntimeState) {
	currChunk := cs.chunk
	cs.launched = true

	if ctx == nil {
		if err := executeLaunchChunk(context.Background(), srv, buildID, currChunk, "m-id", "d-id"); err != nil {
			log.Printf("[vanadis] warning: failed executing pr chunk launch: %v", err)
		}
		return
	}
	mAwk := restate.Awakeable[string](ctx)
	dAwk := restate.Awakeable[string](ctx)
	cs.mergeAwakeable = mAwk
	cs.deployAwakeable = dAwk

	_ = restate.RunVoid(ctx, func(rc restate.RunContext) error {
		return executeLaunchChunk(rc, srv, buildID, currChunk, mAwk.Id(), dAwk.Id())
	})
}

func launchActionChunk(ctx restate.WorkflowContext, srv *VanadisServer, buildID string, cs *workflowChunkRuntimeState) {
	currChunk := cs.chunk
	cs.launched = true

	if ctx == nil {
		if err := executeLaunchChunk(context.Background(), srv, buildID, currChunk, "", "a-id"); err != nil {
			log.Printf("[vanadis] warning: failed executing action chunk launch: %v", err)
		}
		return
	}
	aAwk := restate.Awakeable[string](ctx)
	cs.actionAwakeable = aAwk

	_ = restate.RunVoid(ctx, func(rc restate.RunContext) error {
		return executeLaunchChunk(rc, srv, buildID, currChunk, "", aAwk.Id())
	})
}

func buildWorkflowWaitFutures(cancelPromise, dagMutationPromise restate.Future, chunkOrder []string, chunkStates map[string]*workflowChunkRuntimeState) []restate.Future {
	waitFuts := make([]restate.Future, 0, len(chunkOrder)+2)
	waitFuts = append(waitFuts, cancelPromise, dagMutationPromise)

	for _, cid := range chunkOrder {
		cs := chunkStates[cid]
		if !cs.launched {
			continue
		}
		if strings.ToLower(cs.chunk.Mode) == "pr" {
			if !cs.codeReady && cs.mergeAwakeable != nil {
				waitFuts = append(waitFuts, cs.mergeAwakeable)
			}
			if cs.codeReady && !cs.runtimeReady && cs.deployAwakeable != nil {
				waitFuts = append(waitFuts, cs.deployAwakeable)
			}
		} else if !cs.runtimeReady && cs.actionAwakeable != nil {
			waitFuts = append(waitFuts, cs.actionAwakeable)
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

func cancelWorkflowBuild(ctx restate.WorkflowContext, srv *VanadisServer, buildID string, cancelPromise restate.DurablePromise[string], chunkOrder []string, chunkStates map[string]*workflowChunkRuntimeState) (RunBuildWorkflowResult, error) {
	reason := "cancelled"
	if cancelPromise != nil {
		reason, _ = cancelPromise.Result()
	}
	if ctx == nil {
		if err := executeCancelWorkflowBuild(context.Background(), srv, buildID, reason); err != nil {
			log.Printf("[vanadis] warning: failed executing cancel workflow build: %v", err)
		}
	} else {
		_ = restate.RunVoid(ctx, func(rc restate.RunContext) error {
			return executeCancelWorkflowBuild(rc, srv, buildID, reason)
		})
	}
	return RunBuildWorkflowResult{
		BuildID:         buildID,
		Status:          "cancelled",
		TotalChunks:     len(chunkOrder),
		CompletedChunks: countCompletedChunksFromStates(chunkStates),
	}, nil
}

func reloadWorkflowDAG(ctx restate.WorkflowContext, srv *VanadisServer, buildID string, chunkOrder *[]string, chunkStates map[string]*workflowChunkRuntimeState, count int) restate.DurablePromise[string] {
	var newSnap *workflowBuildSnapshot
	var err error
	if ctx == nil {
		newSnap, err = fetchWorkflowSnapshot(context.Background(), srv, buildID)
	} else {
		newSnap, err = restate.Run(ctx, func(rc restate.RunContext) (*workflowBuildSnapshot, error) {
			return fetchWorkflowSnapshot(rc, srv, buildID)
		})
	}
	if err == nil && newSnap != nil {
		for _, c := range newSnap.Chunks {
			if _, exists := chunkStates[c.ID]; !exists {
				*chunkOrder = append(*chunkOrder, c.ID)
				chunkStates[c.ID] = &workflowChunkRuntimeState{chunk: c}
			} else {
				chunkStates[c.ID].chunk = c
			}
		}
	}
	if ctx == nil {
		return nil
	}
	return restate.Promise[string](ctx, fmt.Sprintf("dag_mutation_%d", count))
}

func processChunkAwakeable(ctx restate.WorkflowContext, srv *VanadisServer, buildID string, firstDone restate.Future, chunkOrder []string, chunkStates map[string]*workflowChunkRuntimeState) (bool, error) {
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

		status, err := handleAwakeableOutcome(ctx, srv, buildID, cs, eventType, val, awkErr)
		if status == "failed" {
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

func handleAwakeableOutcome(ctx restate.WorkflowContext, srv *VanadisServer, buildID string, cs *workflowChunkRuntimeState, eventType, val string, inErr error) (string, error) {
	newStatus, aErr := applyAwakeableResolution(cs, eventType, val, inErr)
	if ctx == nil {
		if err := executeAwakeableStatusUpdate(context.Background(), srv, buildID, cs.chunk.ID, cs.chunk.Title, eventType, newStatus); err != nil {
			log.Printf("[vanadis] warning: failed executing awakeable status update: %v", err)
		}
	} else {
		_ = restate.RunVoid(ctx, func(rc restate.RunContext) error {
			return executeAwakeableStatusUpdate(rc, srv, buildID, cs.chunk.ID, cs.chunk.Title, eventType, newStatus)
		})
	}
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
	port := strings.TrimSpace(srv.cfg.RestatePort)
	if port == "disabled" || port == "-1" {
		return nil, ErrRestateDisabled
	}
	if port == "" {
		port = "9080"
	}
	restateServer := server.NewRestate().Bind(restate.Reflect(NewBuildWorkflow(srv)))
	go func() {
		log.Printf("[vanadis] Restate HTTP/2 server listening on :%s", port)
		if err := restateServer.Start(ctx, ":"+port); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("[vanadis] Restate server error: %v", err)
		}
	}()

	if srv.cfg.RestateAdminURL != "" && srv.cfg.RestateAdvertiseURL != "" {
		go autoRegisterRestateDeployment(ctx, srv.cfg.RestateAdminURL, srv.cfg.RestateAdvertiseURL)
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
