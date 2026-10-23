package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// NomadStreamPayload represents the top-level JSON envelope returned by Nomad's /v1/event/stream endpoint.
type NomadStreamPayload struct {
	Index  uint64       `json:"Index"`
	Events []NomadEvent `json:"Events"`
}

// NomadEvent represents an individual event within the Nomad event stream.
type NomadEvent struct {
	Topic      string          `json:"Topic"`
	Type       string          `json:"Type"`
	Key        string          `json:"Key"`
	FilterKeys []string        `json:"FilterKeys"`
	Index      uint64          `json:"Index"`
	Payload    json.RawMessage `json:"Payload"`
}

// NomadDeploymentEventPayload captures deployment status updates from Nomad.
type NomadDeploymentEventPayload struct {
	Deployment struct {
		ID                string `json:"ID"`
		JobID             string `json:"JobID"`
		Status            string `json:"Status"` // "successful", "failed", "running", "cancelled"
		StatusDescription string `json:"StatusDescription"`
	} `json:"Deployment"`
}

// NomadEvaluationEventPayload captures evaluation status updates from Nomad.
type NomadEvaluationEventPayload struct {
	Evaluation struct {
		ID                string                 `json:"ID"`
		JobID             string                 `json:"JobID"`
		Status            string                 `json:"Status"` // "complete", "failed", "blocked", "canceled"
		StatusDescription string                 `json:"StatusDescription"`
		DeploymentID      string                 `json:"DeploymentID"`
		BlockedEval       string                 `json:"BlockedEval"`
		FailedTGAllocs    map[string]interface{} `json:"FailedTGAllocs"`
		QueuedAllocations map[string]int         `json:"QueuedAllocations"`
	} `json:"Evaluation"`
}

// hasUnqueuedFailedAllocs returns true if any task group with placement failures lacks a queued allocation.
func hasUnqueuedFailedAllocs(failedTG map[string]interface{}, queued map[string]int) bool {
	if len(failedTG) == 0 {
		return false
	}
	for tg := range failedTG {
		if queued[tg] <= 0 {
			return true
		}
	}
	return false
}

// allFailedTaskGroupsQueued returns true if there is at least one failed task group and every failed task group has a queued allocation.
func allFailedTaskGroupsQueued(failedTG map[string]interface{}, queued map[string]int) bool {
	if len(failedTG) == 0 {
		return false
	}
	for tg := range failedTG {
		if queued[tg] <= 0 {
			return false
		}
	}
	return true
}

func metricInt(v interface{}) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	default:
		return 0
	}
}

func hasPositiveMetricValue(v interface{}) bool {
	if metricInt(v) > 0 {
		return true
	}
	switch mp := v.(type) {
	case map[string]interface{}:
		for _, val := range mp {
			if metricInt(val) > 0 {
				return true
			}
		}
	case []interface{}:
		return len(mp) > 0
	}
	return false
}

// hasNonTransientPlacementFailure returns true if any unqueued task group in FailedTGAllocs
// failed placement due to non-transient resource, node, constraint, class, or quota exhaustion
// (ignoring transient network/port collisions).
func hasNonTransientPlacementFailure(failedTG map[string]interface{}, queued map[string]int) bool {
	if len(failedTG) == 0 {
		return false
	}
	for tg, rawMetrics := range failedTG {
		if queued[tg] > 0 {
			continue
		}
		m, ok := rawMetrics.(map[string]interface{})
		if !ok {
			return true
		}
		if m["NodesAvailable"] != nil && !hasPositiveMetricValue(m["NodesAvailable"]) {
			return true
		}
		if hasPositiveMetricValue(m["ConstraintFiltered"]) ||
			hasPositiveMetricValue(m["ClassFiltered"]) ||
			hasPositiveMetricValue(m["QuotaExhausted"]) {
			return true
		}
		hasPositiveDim := false
		if dimMap, ok := m["DimensionExhausted"].(map[string]interface{}); ok {
			for k, count := range dimMap {
				if metricInt(count) > 0 {
					hasPositiveDim = true
					lower := strings.ToLower(k)
					if !strings.Contains(lower, "network") && !strings.Contains(lower, "port") {
						return true
					}
				}
			}
		}
		if !hasPositiveDim && (metricInt(m["NodesExhausted"]) > 0 || metricInt(m["AllocationResourceExhausted"]) > 0 || metricInt(m["ResourceExhausted"]) > 0) {
			return true
		}
	}
	return false
}

// formatFailedTGAllocs formats a human-readable evaluation failure description combining statusDesc and failedTG.
func formatFailedTGAllocs(statusDesc string, failedTG map[string]interface{}) string {
	desc := strings.TrimSpace(statusDesc)
	if len(failedTG) == 0 {
		return desc
	}
	allocSummary := fmt.Sprintf("failed task group allocations: %v", failedTG)
	if desc == "" {
		return allocSummary
	}
	return fmt.Sprintf("%s (%s)", desc, allocSummary)
}

// hasFailedTaskState returns true if any task state in the allocation has Failed == true.
func hasFailedTaskState(taskStates map[string]NomadTaskState) bool {
	for _, ts := range taskStates {
		if ts.Failed {
			return true
		}
	}
	return false
}

// NomadJobEventPayload captures job registration updates from Nomad.
type NomadJobEventPayload struct {
	Job struct {
		ID          string `json:"ID"`
		ModifyIndex uint64 `json:"ModifyIndex"`
	} `json:"Job"`
}

// NomadTaskEvent captures task lifecycle events within an allocation.
type NomadTaskEvent struct {
	Type            string            `json:"Type"`
	DisplayMessage  string            `json:"DisplayMessage"`
	DriverError     string            `json:"DriverError"`
	Message         string            `json:"Message"`
	RestartReason   string            `json:"RestartReason"`
	SetupError      string            `json:"SetupError"`
	DownloadError   string            `json:"DownloadError"`
	ValidationError string            `json:"ValidationError"`
	VaultError      string            `json:"VaultError"`
	ExitCode        int               `json:"ExitCode"`
	Details         map[string]string `json:"Details"`
	Time            int64             `json:"Time"`
}

// NomadTaskState captures the state of a task in an allocation.
type NomadTaskState struct {
	State    string           `json:"State"`
	Failed   bool             `json:"Failed"`
	Restarts int              `json:"Restarts"`
	Events   []NomadTaskEvent `json:"Events"`
}

// NomadAllocationEventPayload captures allocation updates from Nomad.
type NomadAllocationEventPayload struct {
	Allocation struct {
		ID                string                    `json:"ID"`
		JobID             string                    `json:"JobID"`
		DesiredStatus     string                    `json:"DesiredStatus"` // "run", "stop"
		ClientStatus      string                    `json:"ClientStatus"`  // "running", "pending", "failed", "complete"
		ClientDescription string                    `json:"ClientDescription"`
		TaskStates        map[string]NomadTaskState `json:"TaskStates"`
	} `json:"Allocation"`
}

// NomadJobAllocSummary represents a summary item from GET /v1/job/{job}/allocations.
type NomadJobAllocSummary struct {
	ID            string `json:"ID"`
	JobID         string `json:"JobID"`
	DesiredStatus string `json:"DesiredStatus"`
	ClientStatus  string `json:"ClientStatus"`
	JobVersion    uint64 `json:"JobVersion"`
	CreateIndex   uint64 `json:"CreateIndex"`
}

// NomadStreamSubscriber manages the persistent event streaming connection to Nomad.
type NomadStreamSubscriber struct {
	nomadAddr      string
	nomadToken     string
	httpClient     *http.Client
	server         *RouterServer
	lastIndex      uint64
	mu             sync.Mutex
	reconnectDelay time.Duration
	maxBackoff     time.Duration
}

// NewNomadStreamSubscriber constructs a new NomadStreamSubscriber.
func NewNomadStreamSubscriber(server *RouterServer) *NomadStreamSubscriber {
	nomadAddr := server.cfg.NomadAddr
	if nomadAddr == "" {
		nomadAddr = "http://127.0.0.1:4646"
	}
	return &NomadStreamSubscriber{
		nomadAddr:      strings.TrimRight(nomadAddr, "/"),
		nomadToken:     server.cfg.NomadToken,
		httpClient:     &http.Client{Timeout: 0}, // streaming connection, no client timeout
		server:         server,
		reconnectDelay: 1 * time.Second,
		maxBackoff:     30 * time.Second,
	}
}

// Start launches the background streaming worker with automatic reconnection and exponential backoff.
func (sub *NomadStreamSubscriber) Start(ctx context.Context) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[webhooks-router] [nomad-stream] PANIC recovered in stream worker: %v", r)
			}
		}()

		sub.mu.Lock()
		delay := sub.reconnectDelay
		sub.mu.Unlock()
		if delay <= 0 {
			delay = 1 * time.Second
		}

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			start := time.Now()
			err := sub.consumeStream(ctx)
			if err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("[webhooks-router] [nomad-stream] stream disconnected: %v; reconnecting in %v", err, delay)
			}

			// Reset backoff delay if connection was established and lasted more than 5s, or closed without error
			if time.Since(start) > 5*time.Second || err == nil {
				sub.mu.Lock()
				delay = sub.reconnectDelay
				sub.mu.Unlock()
			}

			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}

			// Exponential backoff with jitter
			delay = time.Duration(float64(delay) * 1.5)
			jitter := time.Duration(rand.Int63n(int64(500 * time.Millisecond)))
			delay += jitter
			sub.mu.Lock()
			maxB := sub.maxBackoff
			sub.mu.Unlock()
			if delay > maxB {
				delay = maxB
			}
		}
	}()
}

// consumeStream opens an HTTP streaming connection to Nomad and processes events line-by-line.
func (sub *NomadStreamSubscriber) consumeStream(ctx context.Context) error {
	sub.mu.Lock()
	idx := sub.lastIndex
	sub.mu.Unlock()

	streamURL := fmt.Sprintf("%s/v1/event/stream?topic=Deployment:*&topic=Allocation:*&topic=Evaluation:*&topic=Job:*", sub.nomadAddr)
	if idx > 0 {
		streamURL += fmt.Sprintf("&index=%d", idx)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, streamURL, nil)
	if err != nil {
		return fmt.Errorf("create stream request: %w", err)
	}
	if sub.nomadToken != "" {
		req.Header.Set("X-Nomad-Token", sub.nomadToken)
	}

	resp, err := sub.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("stream connection failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusBadRequest || resp.StatusCode >= 500 {
		// Detect Raft Index out-of-bounds error (Nomad GC'd historical index)
		body, err := io.ReadAll(io.LimitReader(resp.Body, 1024))
		if err != nil {
			log.Printf("[webhooks-router] [nomad-stream] error reading response body: %v", err)
		}
		if strings.Contains(strings.ToLower(string(body)), "index") || strings.Contains(strings.ToLower(string(body)), "bounds") {
			log.Printf("[webhooks-router] [nomad-stream] Nomad index %d is out of bounds; resetting to current index", idx)
			sub.mu.Lock()
			sub.lastIndex = 0
			sub.mu.Unlock()
		}
		return fmt.Errorf("nomad stream returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %d from nomad stream", resp.StatusCode)
	}

	reader := bufio.NewReader(resp.Body)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		line, err := reader.ReadBytes('\n')
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
				return err
			}
			return fmt.Errorf("read stream line: %w", err)
		}

		trimmed := bytes.TrimSpace(line)
		// Heartbeat Evasion: Nomad periodically emits empty "{}" lines to maintain keep-alive
		if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("{}")) {
			continue
		}

		sub.processStreamLine(ctx, trimmed)
	}
}

// processStreamLine parses a single JSON line from Nomad's event stream.
func (sub *NomadStreamSubscriber) processStreamLine(ctx context.Context, line []byte) {
	var payload NomadStreamPayload
	if err := json.Unmarshal(line, &payload); err != nil {
		// Log and skip malformed or unexpected frame
		return
	}

	if payload.Index > 0 {
		sub.mu.Lock()
		if payload.Index > sub.lastIndex {
			sub.lastIndex = payload.Index
		}
		sub.mu.Unlock()
	}

	for _, event := range payload.Events {
		sub.handleNomadEvent(ctx, event)
	}
}

// handleNomadEvent dispatches an individual Nomad event to its corresponding handler.
func (sub *NomadStreamSubscriber) handleNomadEvent(ctx context.Context, event NomadEvent) {
	// Pre-filter noise in memory before hitting River or DB
	if strings.EqualFold(event.Type, "PlanResult") {
		// Ignore internal scheduler planning calculations (PlanResult is not a lifecycle state transition)
		return
	}
	var idempotencyKey string
	switch event.Topic {
	case "Deployment":
		var depPayload NomadDeploymentEventPayload
		if err := json.Unmarshal(event.Payload, &depPayload); err != nil {
			return
		}
		status := depPayload.Deployment.Status
		if !strings.EqualFold(status, "successful") && !strings.EqualFold(status, "failed") && !strings.EqualFold(status, "cancelled") {
			return
		}
		if depPayload.Deployment.ID != "" && depPayload.Deployment.JobID != "" {
			idempotencyKey = fmt.Sprintf("nomad:deployment:%s:%s:%s", depPayload.Deployment.JobID, depPayload.Deployment.ID, status)
		}
	case "Allocation":
		var allocPayload NomadAllocationEventPayload
		if err := json.Unmarshal(event.Payload, &allocPayload); err != nil {
			return
		}
		alloc := allocPayload.Allocation
		isRunning := alloc.DesiredStatus == "run" && alloc.ClientStatus == "running"
		isComplete := alloc.DesiredStatus == "run" && alloc.ClientStatus == "complete" && !hasFailedTaskState(alloc.TaskStates)
		isFailed := strings.EqualFold(alloc.ClientStatus, "failed")
		isExhausted := alloc.DesiredStatus == "run" && isAllocationRestartExhausted(alloc.TaskStates)
		if !isRunning && !isComplete && !isFailed && !isExhausted {
			return
		}
		if alloc.ID != "" && alloc.JobID != "" {
			if isFailed || isExhausted {
				idempotencyKey = fmt.Sprintf("nomad:allocation:%s:%s:failed", alloc.JobID, alloc.ID)
			} else {
				idempotencyKey = fmt.Sprintf("nomad:allocation:%s:%s:%s", alloc.JobID, alloc.ID, strings.ToLower(alloc.ClientStatus))
			}
		}
	case "Evaluation":
		var evalPayload NomadEvaluationEventPayload
		if err := json.Unmarshal(event.Payload, &evalPayload); err != nil {
			return
		}
		eval := evalPayload.Evaluation
		if strings.TrimSpace(eval.JobID) == "" {
			return
		}
		if strings.EqualFold(eval.Status, "blocked") {
			if !hasNonTransientPlacementFailure(eval.FailedTGAllocs, eval.QueuedAllocations) {
				return
			}
			idempotencyKey = fmt.Sprintf("nomad:evaluation:%s:%s:blocked", eval.JobID, eval.ID)
		} else {
			hasUnqueuedFailures := hasUnqueuedFailedAllocs(eval.FailedTGAllocs, eval.QueuedAllocations)
			isExplicitFailure := strings.EqualFold(eval.Status, "failed") || strings.EqualFold(eval.Status, "canceled") || strings.EqualFold(eval.Status, "cancelled")

			if hasUnqueuedFailures || isExplicitFailure {
				// Pass through to River/ProcessNomadEvent for failure handling
				idempotencyKey = fmt.Sprintf("nomad:evaluation:%s:%s:%s", eval.JobID, eval.ID, eval.Status)
			} else if eval.BlockedEval != "" || allFailedTaskGroupsQueued(eval.FailedTGAllocs, eval.QueuedAllocations) {
				// Blocked evaluation or all unplaced allocations queued for rollout; wait for follow-up allocation/deployment events
				return
			} else if strings.EqualFold(eval.Status, "complete") {
				// In-place or restart complete evaluation; pass through for success handling
				idempotencyKey = fmt.Sprintf("nomad:evaluation:%s:%s:complete", eval.JobID, eval.ID)
			} else {
				return
			}
		}
	case "Job":
		if !strings.EqualFold(event.Type, "JobRegistered") {
			return
		}
		var jobPayload NomadJobEventPayload
		if err := json.Unmarshal(event.Payload, &jobPayload); err != nil {
			return
		}
		jobID := strings.TrimSpace(jobPayload.Job.ID)
		if jobID == "" {
			return
		}
		idempotencyKey = fmt.Sprintf("nomad:job:%s:%d:registered", jobID, jobPayload.Job.ModifyIndex)
	default:
		// Drop unhandled topics immediately
		return
	}

	if idempotencyKey == "" {
		idempotencyKey = randomNonce()
	}

	if sub.server != nil && sub.server.riverClient != nil {
		args := NomadEventArgs{
			IdempotencyKey: idempotencyKey,
			Topic:          event.Topic,
			Type:           event.Type,
			Payload:        event.Payload,
		}
		if err := sub.server.insertRiverJob(ctx, args); err != nil {
			log.Printf("[webhooks-router] [nomad-stream] error inserting nomad event into river: %v", err)
		}
		return
	}

	if sub.server != nil {
		if err := sub.server.ProcessNomadEvent(ctx, event.Topic, event.Type, event.Payload); err != nil {
			log.Printf("[webhooks-router] [nomad-stream] error processing nomad event directly: %v", err)
		}
	}
}

// ProcessNomadEvent processes a filtered Nomad stream event synchronously.
func (s *RouterServer) ProcessNomadEvent(ctx context.Context, topic, eventType string, rawPayload json.RawMessage) error {
	if strings.EqualFold(eventType, "PlanResult") {
		return nil
	}
	switch topic {
	case "Deployment":
		var depPayload NomadDeploymentEventPayload
		if err := json.Unmarshal(rawPayload, &depPayload); err != nil {
			return fmt.Errorf("unmarshal nomad deployment payload: %w", err)
		}
		dep := depPayload.Deployment
		if strings.EqualFold(dep.Status, "successful") {
			return s.HandleNomadJobSuccess(ctx, dep.JobID, dep.ID)
		} else if strings.EqualFold(dep.Status, "failed") || strings.EqualFold(dep.Status, "cancelled") {
			return s.HandleNomadJobFailure(ctx, dep.JobID, dep.ID, dep.Status, dep.StatusDescription)
		}
	case "Allocation":
		var allocPayload NomadAllocationEventPayload
		if err := json.Unmarshal(rawPayload, &allocPayload); err != nil {
			return fmt.Errorf("unmarshal nomad allocation payload: %w", err)
		}
		alloc := allocPayload.Allocation
		if strings.EqualFold(alloc.ClientStatus, "failed") {
			if s.isNomadJobHealthy(ctx, alloc.JobID) {
				return nil
			}
			return s.HandleNomadJobFailure(ctx, alloc.JobID, alloc.ID, "failed", extractAllocationErrorDetails(alloc.ClientDescription, alloc.TaskStates))
		}
		if alloc.DesiredStatus == "run" && isAllocationRestartExhausted(alloc.TaskStates) {
			return s.HandleNomadJobFailure(ctx, alloc.JobID, alloc.ID, "failed", extractAllocationErrorDetails(alloc.ClientDescription, alloc.TaskStates))
		}
		if isAllocationRestarting(alloc.TaskStates) {
			return nil
		}
		if alloc.DesiredStatus == "run" && (alloc.ClientStatus == "running" || (alloc.ClientStatus == "complete" && !hasFailedTaskState(alloc.TaskStates))) && s.isNomadJobHealthy(ctx, alloc.JobID) {
			return s.HandleNomadJobSuccess(ctx, alloc.JobID, alloc.ID)
		}
	case "Evaluation":
		var evalPayload NomadEvaluationEventPayload
		if err := json.Unmarshal(rawPayload, &evalPayload); err != nil {
			return fmt.Errorf("unmarshal nomad evaluation payload: %w", err)
		}
		eval := evalPayload.Evaluation
		if strings.EqualFold(eval.Status, "blocked") {
			if !hasNonTransientPlacementFailure(eval.FailedTGAllocs, eval.QueuedAllocations) || s.isNomadJobHealthy(ctx, eval.JobID) {
				return nil
			}
			return s.HandleNomadJobFailure(ctx, eval.JobID, eval.ID, eval.Status, formatFailedTGAllocs(eval.StatusDescription, eval.FailedTGAllocs))
		}
		hasUnqueuedFailures := hasUnqueuedFailedAllocs(eval.FailedTGAllocs, eval.QueuedAllocations)
		isExplicitFailure := strings.EqualFold(eval.Status, "failed") || strings.EqualFold(eval.Status, "canceled") || strings.EqualFold(eval.Status, "cancelled")

		if hasUnqueuedFailures || isExplicitFailure {
			if s.isNomadJobHealthy(ctx, eval.JobID) {
				return nil
			}
			return s.HandleNomadJobFailure(ctx, eval.JobID, eval.ID, eval.Status, formatFailedTGAllocs(eval.StatusDescription, eval.FailedTGAllocs))
		} else if eval.BlockedEval != "" || allFailedTaskGroupsQueued(eval.FailedTGAllocs, eval.QueuedAllocations) {
			return nil
		} else if strings.EqualFold(eval.Status, "complete") && s.isNomadJobHealthy(ctx, eval.JobID) {
			return s.HandleNomadJobSuccess(ctx, eval.JobID, eval.ID)
		}
	case "Job":
		if !strings.EqualFold(eventType, "JobRegistered") {
			return nil
		}
		var jobPayload NomadJobEventPayload
		if err := json.Unmarshal(rawPayload, &jobPayload); err != nil {
			return fmt.Errorf("unmarshal nomad job payload: %w", err)
		}
		jobID := strings.TrimSpace(jobPayload.Job.ID)
		if jobID != "" && s.isNomadBatchTemplateHealthy(ctx, jobID) {
			return s.HandleNomadJobSuccess(ctx, jobID, fmt.Sprintf("job-registered-%d", jobPayload.Job.ModifyIndex))
		}
	}
	return nil
}

// HandleNomadJobFailure processes a confirmed deployment failure or cancellation for a Nomad job.
func (s *RouterServer) HandleNomadJobFailure(ctx context.Context, jobID, refID, status, statusDesc string) error {
	cleanJob := strings.TrimSpace(jobID)
	if cleanJob == "" {
		return nil
	}
	if s.registry == nil {
		return s.HandleUnmanagedJobCrash(ctx, cleanJob, refID, status, statusDesc)
	}

	targetID, prNumber, mergeSHA, repo, updated, err := s.registry.AtomicTransitionDeployFailedByJob(ctx, cleanJob)
	if err != nil {
		log.Printf("[webhooks-router] [nomad] error transitioning job %s to deploy_failed: %v", cleanJob, err)
		return err
	}
	if !updated {
		return s.HandleUnmanagedJobCrash(ctx, cleanJob, refID, status, statusDesc)
	}

	log.Printf("[webhooks-router] [nomad] deployment failure confirmed for %s#%d (job: %s, commit: %s, target: %s, ref: %s, status: %s)",
		repo, prNumber, cleanJob, mergeSHA, targetID, refID, status)

	dmMsg := fmt.Sprintf("💥 %sNomad deployment failed for %s. Following up...", formatDirectMessagePrefix(prNumber, repo), cleanJob)
	if prNumber > 0 {
		if err := s.SyncPRWidget(ctx, repo, prNumber, "failed", fmt.Sprintf("Nomad deployment failed for %s", cleanJob), nil); err != nil {
			log.Printf("[webhooks-router] [widget] warning syncing nomad deploy_failed widget: %v", err)
		}
	}
	prompt := formatErrorDetailsBlock(
		fmt.Sprintf("Nomad deployment for job %s failed with status %q (PR #%d on %s, commit: %s, deployment ID: %s).", cleanJob, status, prNumber, repo, mergeSHA, refID),
		statusDesc, "", "Please investigate and fix the deployment failure.",
	)
	return s.dispatchAlertWithPrompt(ctx, targetID, "failure", dmMsg, prompt, s.vanadisPromptOpts(ctx, repo, prNumber))
}

// HandleNomadJobSuccess processes a confirmed successful deployment or running allocation for a Nomad job.
func (s *RouterServer) HandleNomadJobSuccess(ctx context.Context, jobID, refID string) error {
	cleanJob := strings.TrimSpace(jobID)
	if s.registry == nil || cleanJob == "" {
		return nil
	}
	targetID, prNumber, mergeSHA, repo, updated, err := s.registry.AtomicTransitionDeployedByJob(ctx, cleanJob)
	if err != nil {
		log.Printf("[webhooks-router] [nomad] error transitioning job %s to deployed: %v", cleanJob, err)
		return err
	}
	if !updated {
		return nil
	}
	log.Printf("[webhooks-router] [nomad] deployment confirmed for %s#%d (job: %s, commit: %s, target: %s, ref: %s)",
		repo, prNumber, cleanJob, mergeSHA, targetID, refID)
	return s.dispatchDeploymentSuccessPrompt(ctx, targetID, cleanJob, repo, mergeSHA, prNumber, refID)
}

// ReconcileActiveDeployments performs the startup sweep across all pending deployments in pr_registry.
func (s *RouterServer) ReconcileActiveDeployments(ctx context.Context) error {
	if s.registry == nil {
		return nil
	}
	pending, err := s.registry.ListDeployingPRs(ctx)
	if err != nil {
		return fmt.Errorf("list deploying prs for sweep: %w", err)
	}
	if len(pending) == 0 {
		return nil
	}
	log.Printf("[webhooks-router] [sweep] found %d active deployment(s) in pr_registry, reconciling with Nomad...", len(pending))
	for _, p := range pending {
		for _, job := range extractStartedJobsFromMetadata(p.Metadata) {
			if s.isNomadJobHealthy(ctx, job) {
				if err := s.HandleNomadJobSuccess(ctx, job, "startup-sweep"); err != nil {
					log.Printf("[webhooks-router] [sweep] error handling nomad job success for %s: %v", job, err)
				}
			}
		}
	}
	return nil
}

func (s *RouterServer) getNomadJSON(ctx context.Context, path string, out any) bool {
	cleanAddr := strings.TrimRight(firstNonEmpty(s.cfg.NomadAddr, "http://127.0.0.1:4646"), "/")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, cleanAddr+path, nil)
	if err != nil {
		return false
	}
	if s.cfg.NomadToken != "" {
		req.Header.Set("X-Nomad-Token", s.cfg.NomadToken)
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false
	}
	return json.NewDecoder(resp.Body).Decode(out) == nil
}

// NomadJobKind classifies a Nomad job for completion and health tracking.
type NomadJobKind int

const (
	NomadJobKindService NomadJobKind = iota
	NomadJobKindOneShotBatch
	NomadJobKindBatchTemplate
)

func (s *RouterServer) classifyNomadJob(ctx context.Context, job string) (NomadJobKind, string, uint64, bool) {
	var jobSpec struct {
		Type     string `json:"Type"`
		Status   string `json:"Status"`
		Version  uint64 `json:"Version"`
		Stop     bool   `json:"Stop"`
		Periodic *struct {
			Enabled bool `json:"Enabled"`
		} `json:"Periodic"`
		ParameterizedJob interface{} `json:"ParameterizedJob"`
	}
	if !s.getNomadJSON(ctx, "/v1/job/"+job, &jobSpec) || !strings.EqualFold(jobSpec.Type, "batch") {
		return NomadJobKindService, jobSpec.Status, jobSpec.Version, jobSpec.Stop
	}
	if (jobSpec.Periodic != nil && jobSpec.Periodic.Enabled) || jobSpec.ParameterizedJob != nil {
		return NomadJobKindBatchTemplate, jobSpec.Status, jobSpec.Version, jobSpec.Stop
	}
	return NomadJobKindOneShotBatch, jobSpec.Status, jobSpec.Version, jobSpec.Stop
}

// isNomadJobHealthy queries the Nomad REST API to verify if the job has successfully deployed.
func (s *RouterServer) isNomadJobHealthy(ctx context.Context, job string) bool {
	var deps []struct {
		ID                string `json:"ID"`
		Status            string `json:"Status"`
		StatusDescription string `json:"StatusDescription"`
		JobVersion        uint64 `json:"JobVersion"`
		CreateIndex       uint64 `json:"CreateIndex"`
	}
	if s.getNomadJSON(ctx, "/v1/job/"+job+"/deployments", &deps) && len(deps) > 0 {
		sort.SliceStable(deps, func(i, j int) bool {
			return deps[i].CreateIndex > deps[j].CreateIndex
		})
		latest := deps[0]
		desc := strings.ToLower(latest.StatusDescription)
		if !strings.EqualFold(latest.Status, "successful") || strings.Contains(desc, "roll") || strings.Contains(desc, "revert") {
			return false
		}
		for _, d := range deps[1:] {
			if d.JobVersion > latest.JobVersion {
				return false
			}
		}
		return true
	}

	var allocs []NomadJobAllocSummary
	if !s.getNomadJSON(ctx, "/v1/job/"+job+"/allocations", &allocs) {
		return false
	}
	kind, jobStatus, jobVersion, jobStop := s.classifyNomadJob(ctx, job)
	switch kind {
	case NomadJobKindBatchTemplate:
		for _, a := range allocs {
			if a.DesiredStatus == "run" && (a.ClientStatus == "failed" || a.ClientStatus == "pending") {
				return false
			}
		}
		return strings.EqualFold(jobStatus, "running") && !jobStop
	case NomadJobKindOneShotBatch:
		if jobStop || strings.EqualFold(jobStatus, "pending") {
			return false
		}
		hasComplete := false
		for _, a := range allocs {
			if a.JobVersion < jobVersion {
				continue
			}
			if a.DesiredStatus == "run" && (a.ClientStatus == "failed" || a.ClientStatus == "pending" || a.ClientStatus == "running") {
				return false
			}
			if a.DesiredStatus == "run" && a.ClientStatus == "complete" {
				hasComplete = true
			}
		}
		return hasComplete
	default:
		hasRunning := false
		for _, a := range allocs {
			if a.DesiredStatus == "run" && (a.ClientStatus == "failed" || a.ClientStatus == "pending") {
				return false
			}
			if a.DesiredStatus == "run" && a.ClientStatus == "running" {
				hasRunning = true
			}
		}
		return hasRunning
	}
}

// isNomadBatchTemplateHealthy queries Nomad for periodic or parameterized batch job templates
// that do not maintain persistent allocations between runs.
func (s *RouterServer) isNomadBatchTemplateHealthy(ctx context.Context, job string) bool {
	kind, status, _, stop := s.classifyNomadJob(ctx, job)
	return kind == NomadJobKindBatchTemplate && strings.EqualFold(status, "running") && !stop
}

// extractStringSliceFromMetadata extracts a slice of strings stored under key in metadata JSONB.
func extractStringSliceFromMetadata(meta map[string]interface{}, key string) []string {
	if meta == nil {
		return nil
	}
	var raw []string
	if rawVal, ok := meta[key]; ok {
		switch v := rawVal.(type) {
		case []interface{}:
			for _, item := range v {
				if s, ok := item.(string); ok {
					raw = append(raw, s)
				}
			}
		case []string:
			raw = append(raw, v...)
		}
	}
	return dedupeTrimmedStrings(raw)
}

// extractJobsFromMetadata extracts candidate Nomad job names stored in the PR registry metadata JSONB.
func extractJobsFromMetadata(meta map[string]interface{}) []string {
	return extractStringSliceFromMetadata(meta, "jobs")
}

// extractStartedJobsFromMetadata returns candidate Nomad jobs eligible for startup sweep reconciliation.
// When "started_jobs" is present in metadata, only pending jobs in "started_jobs" are returned;
// legacy rows without "started_jobs" fall back to extractJobsFromMetadata.
func extractStartedJobsFromMetadata(meta map[string]interface{}) []string {
	if meta == nil {
		return nil
	}
	pending := extractJobsFromMetadata(meta)
	if _, hasStarted := meta["started_jobs"]; !hasStarted {
		return pending
	}
	started := extractStringSliceFromMetadata(meta, "started_jobs")
	if len(started) == 0 || len(pending) == 0 {
		return nil
	}
	startedSet := make(map[string]struct{}, len(started))
	for _, s := range started {
		startedSet[strings.ToLower(s)] = struct{}{}
	}
	var out []string
	for _, job := range pending {
		if _, ok := startedSet[strings.ToLower(job)]; ok {
			out = append(out, job)
		}
	}
	return out
}

// isTaskRestartExhausted returns true if a task has exhausted its restart policy attempts
// and is not actively running.
func isTaskRestartExhausted(ts NomadTaskState) bool {
	if strings.EqualFold(ts.State, "running") {
		return false
	}
	for i := len(ts.Events) - 1; i >= 0; i-- {
		evt := ts.Events[i]
		if evt.Type == "Started" {
			return false
		}
		if evt.Type == "Not Restarting" || strings.HasPrefix(evt.RestartReason, "Exceeded allowed attempts") {
			return true
		}
	}
	return false
}

// isAllocationRestartExhausted returns true if any task in the allocation has exhausted its allowed restart attempts.
func isAllocationRestartExhausted(taskStates map[string]NomadTaskState) bool {
	for _, ts := range taskStates {
		if isTaskRestartExhausted(ts) {
			return true
		}
	}
	return false
}

// isAllocationRestarting returns true if any task in the allocation is actively restarting or pending restart within policy.
func isAllocationRestarting(taskStates map[string]NomadTaskState) bool {
	for _, ts := range taskStates {
		if isTaskRestartExhausted(ts) {
			continue
		}
		if strings.EqualFold(ts.State, "pending") || strings.EqualFold(ts.State, "restarting") {
			return true
		}
		if len(ts.Events) > 0 {
			lastType := ts.Events[len(ts.Events)-1].Type
			if lastType == "Restart Signaled" || lastType == "Restarting" {
				return true
			}
		}
	}
	return false
}

// extractAllocationErrorDetails extracts human-readable failure diagnostics from allocation task states and events.
func extractAllocationErrorDetails(clientDesc string, taskStates map[string]NomadTaskState) string {
	taskNames := make([]string, 0, len(taskStates))
	for name := range taskStates {
		taskNames = append(taskNames, name)
	}
	sort.Strings(taskNames)

	var taskErrors []string
	for _, name := range taskNames {
		ts := taskStates[name]
		if !ts.Failed && !strings.EqualFold(ts.State, "dead") && !isTaskRestartExhausted(ts) {
			continue
		}
		var errMsg string
		for i := len(ts.Events) - 1; i >= 0; i-- {
			evt := ts.Events[i]
			errMsg = firstNonEmpty(evt.DriverError, evt.SetupError, evt.DownloadError, evt.ValidationError, evt.VaultError, evt.Details["driver_error"])
			if errMsg == "" && (evt.Type == "Driver Failure" || evt.Type == "Task Setup" || evt.Type == "Failed Validation") {
				errMsg = evt.DisplayMessage
			}
			if errMsg == "" && evt.ExitCode != 0 {
				errMsg = fmt.Sprintf("exited with code %d", evt.ExitCode)
				if evt.DisplayMessage != "" {
					errMsg = fmt.Sprintf("%s (%s)", evt.DisplayMessage, errMsg)
				}
			}
			if errMsg != "" {
				break
			}
		}
		if errMsg == "" && len(ts.Events) > 0 {
			lastEvt := ts.Events[len(ts.Events)-1]
			errMsg = firstNonEmpty(strings.TrimSpace(lastEvt.DisplayMessage), strings.TrimSpace(lastEvt.RestartReason))
		}
		if errMsg != "" {
			taskErrors = append(taskErrors, fmt.Sprintf("task %q: %s", name, errMsg))
		}
	}
	if len(taskErrors) > 0 {
		return strings.Join(taskErrors, "\n")
	}
	return firstNonEmpty(strings.TrimSpace(clientDesc), "allocation failed")
}

// HandleUnmanagedJobCrash coordinates debounced failure reporting for non-PR unmanaged Nomad jobs.
func (s *RouterServer) HandleUnmanagedJobCrash(ctx context.Context, jobID, allocID, status, statusDesc string) error {
	cleanJob := strings.TrimSpace(jobID)
	if cleanJob == "" {
		return nil
	}
	args := NomadCrashAlertArgs{
		JobID:      cleanJob,
		AllocID:    strings.TrimSpace(allocID),
		Status:     strings.TrimSpace(status),
		StatusDesc: strings.TrimSpace(statusDesc),
	}
	if s.riverClient != nil {
		if err := s.insertRiverJob(ctx, args); err != nil {
			log.Printf("[webhooks-router] [nomad] error enqueuing crash alert into river: %v", err)
			return fmt.Errorf("enqueue crash alert: %w", err)
		}
		return nil
	}
	return s.ProcessCrashAlert(ctx, args)
}

// ProcessCrashAlert delivers direct Discord messages and AI prompt notifications for unmanaged job crashes.
func (s *RouterServer) ProcessCrashAlert(ctx context.Context, args NomadCrashAlertArgs) error {
	channelID := strings.TrimSpace(s.cfg.SystemChannelID)
	if !IsValidDiscordSnowflake(channelID) || s.dispatcher == nil {
		return nil
	}
	callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	dmContent := fmt.Sprintf("💥 Nomad job `%s` crashed or failed. Following up...", args.JobID)
	if err := s.dispatcher.DispatchDirectMessage(callCtx, DirectMessageRequest{ChannelID: channelID, Content: dmContent}); err != nil {
		if is4xxClientError(err) {
			return nil
		}
		log.Printf("[webhooks-router] [crash-alert] warning dispatching direct message: %v", err)
	}
	prompt := formatErrorDetailsBlock(
		fmt.Sprintf("Nomad job `%s` failed or crashed unexpectedly (status: %s, allocation ID: %s).", args.JobID, args.Status, args.AllocID),
		args.StatusDesc, "", "Please investigate and fix the service failure.",
	)
	if err := s.dispatcher.DispatchPrompt(callCtx, PromptRequest{ChannelID: channelID, Prompt: prompt}); err != nil {
		if is4xxClientError(err) {
			return nil
		}
		return fmt.Errorf("dispatch failure prompt for %s: %w", args.JobID, err)
	}
	return nil
}
