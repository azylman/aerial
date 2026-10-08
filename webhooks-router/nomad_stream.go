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

	"github.com/riverqueue/river"
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

// SetReconnectDelay overrides reconnection backoff parameters for fast hermetic unit tests.
func (sub *NomadStreamSubscriber) SetReconnectDelay(initial, maxDelay time.Duration) {
	sub.mu.Lock()
	defer sub.mu.Unlock()
	sub.reconnectDelay = initial
	sub.maxBackoff = maxDelay
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

	streamURL := fmt.Sprintf("%s/v1/event/stream?topic=Deployment:*&topic=Allocation:*&topic=Evaluation:*", sub.nomadAddr)
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
		isFailed := strings.EqualFold(alloc.ClientStatus, "failed")
		isExhausted := alloc.DesiredStatus == "run" && isAllocationRestartExhausted(alloc.TaskStates)
		if !isRunning && !isFailed && !isExhausted {
			return
		}
		if alloc.ID != "" && alloc.JobID != "" {
			if isFailed || isExhausted {
				idempotencyKey = fmt.Sprintf("nomad:allocation:%s:%s:failed", alloc.JobID, alloc.ID)
			} else {
				idempotencyKey = fmt.Sprintf("nomad:allocation:%s:%s", alloc.JobID, alloc.ID)
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
			// A blocked evaluation represents Nomad waiting for cluster state changes to place remaining allocations;
			// wait for follow-up allocation/deployment events.
			return
		}
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
		opts := &river.InsertOpts{
			MaxAttempts: 5,
			UniqueOpts: river.UniqueOpts{
				ByArgs:   true,
				ByPeriod: 15 * time.Minute,
			},
		}
		res, err := sub.server.riverClient.Insert(ctx, args, opts)
		if err != nil {
			log.Printf("[webhooks-router] [nomad-stream] error inserting nomad event into river: %v", err)
		}
		if res != nil && res.UniqueSkippedAsDuplicate {
			RecordUniqueSkipped("nomad_event")
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
			// Stale allocation protection: ignore failure events from superseded or stopped allocations if the job is already healthy
			if s.isNomadJobHealthy(ctx, alloc.JobID) {
				return nil
			}
			statusDesc := extractAllocationErrorDetails(alloc.ClientDescription, alloc.TaskStates)
			return s.HandleNomadJobFailure(ctx, alloc.JobID, alloc.ID, "failed", statusDesc)
		}
		if alloc.DesiredStatus == "run" && isAllocationRestartExhausted(alloc.TaskStates) {
			statusDesc := extractAllocationErrorDetails(alloc.ClientDescription, alloc.TaskStates)
			return s.HandleNomadJobFailure(ctx, alloc.JobID, alloc.ID, "failed", statusDesc)
		}
		if isAllocationRestarting(alloc.TaskStates) {
			return nil
		}
		if alloc.DesiredStatus == "run" && alloc.ClientStatus == "running" && s.isNomadJobHealthy(ctx, alloc.JobID) {
			return s.HandleNomadJobSuccess(ctx, alloc.JobID, alloc.ID)
		}
	case "Evaluation":
		var evalPayload NomadEvaluationEventPayload
		if err := json.Unmarshal(rawPayload, &evalPayload); err != nil {
			return fmt.Errorf("unmarshal nomad evaluation payload: %w", err)
		}
		eval := evalPayload.Evaluation
		if strings.EqualFold(eval.Status, "blocked") {
			// A blocked evaluation represents Nomad waiting for cluster state changes to place remaining allocations;
			// wait for follow-up allocation/deployment events.
			return nil
		}
		hasUnqueuedFailures := hasUnqueuedFailedAllocs(eval.FailedTGAllocs, eval.QueuedAllocations)
		isExplicitFailure := strings.EqualFold(eval.Status, "failed") || strings.EqualFold(eval.Status, "canceled") || strings.EqualFold(eval.Status, "cancelled")

		if hasUnqueuedFailures || isExplicitFailure {
			if s.isNomadJobHealthy(ctx, eval.JobID) {
				return nil
			}
			desc := eval.StatusDescription
			if desc == "" && len(eval.FailedTGAllocs) > 0 {
				desc = fmt.Sprintf("failed task group allocations: %v", eval.FailedTGAllocs)
			}
			return s.HandleNomadJobFailure(ctx, eval.JobID, eval.ID, eval.Status, desc)
		} else if eval.BlockedEval != "" || allFailedTaskGroupsQueued(eval.FailedTGAllocs, eval.QueuedAllocations) {
			// All unplaced allocations have queued allocations / blocked evaluation waiting to place them
			return nil
		} else if strings.EqualFold(eval.Status, "complete") {
			if s.isNomadJobHealthy(ctx, eval.JobID) {
				return s.HandleNomadJobSuccess(ctx, eval.JobID, eval.ID)
			}
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

	if IsValidDiscordSnowflake(targetID) && s.dispatcher != nil {
		dmMsg := fmt.Sprintf("💥 %sNomad deployment failed for %s. Following up...", formatDirectMessagePrefix(prNumber, repo), cleanJob)
		if err := s.dispatcher.DispatchDirectMessage(ctx, DirectMessageRequest{
			ChannelID: targetID,
			Content:   dmMsg,
		}); err != nil {
			log.Printf("[webhooks-router] [dispatcher] warning dispatching nomad failure direct message: %v", err)
		}

		lines := []string{
			fmt.Sprintf("Nomad deployment for job %s failed with status %q (PR #%d on %s, commit: %s, deployment ID: %s).", cleanJob, status, prNumber, repo, mergeSHA, refID),
		}
		if cleanDesc := strings.TrimSpace(statusDesc); cleanDesc != "" {
			lines = append(lines, fmt.Sprintf("Error details:\n```\n%s\n```", truncatePromptDetails(cleanDesc)))
		}
		lines = append(lines, "Please investigate and fix the deployment failure.")
		prompt := strings.Join(lines, "\n")

		if err := s.dispatcher.DispatchPrompt(ctx, PromptRequest{
			ChannelID: targetID,
			Prompt:    prompt,
		}); err != nil {
			log.Printf("[webhooks-router] [dispatcher] error dispatching failure prompt for %s: %v", cleanJob, err)
			return fmt.Errorf("dispatch failure prompt for %s: %w", cleanJob, err)
		}
	}
	return nil
}

// HandleNomadJobSuccess processes a confirmed successful deployment or running allocation for a Nomad job.
func (s *RouterServer) HandleNomadJobSuccess(ctx context.Context, jobID, refID string) error {
	if s.registry == nil {
		return nil
	}

	cleanJob := strings.TrimSpace(jobID)
	if cleanJob == "" {
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

	if IsValidDiscordSnowflake(targetID) && s.dispatcher != nil {
		if err := s.dispatchDeploymentSuccessPrompt(ctx, targetID, cleanJob, repo, mergeSHA, prNumber, refID); err != nil {
			log.Printf("[webhooks-router] [dispatcher] error dispatching deployment success prompt for %s: %v", cleanJob, err)
			return err
		}
	}
	return nil
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
		jobs := extractJobsFromMetadata(p.Metadata)
		for _, job := range jobs {
			if s.isNomadJobHealthy(ctx, job) {
				if err := s.HandleNomadJobSuccess(ctx, job, "startup-sweep"); err != nil {
					log.Printf("[webhooks-router] [sweep] error handling nomad job success for %s: %v", job, err)
				}
			}
		}
	}
	return nil
}

// isNomadJobHealthy queries the Nomad REST API to verify if the job has successfully deployed.
// It prioritizes /v1/job/{job}/deployments (checking if the latest deployment is "successful").
// If no deployment stanza exists, it checks /v1/job/{job}/allocations for healthy running allocations without pending/failing allocations.
func (s *RouterServer) isNomadJobHealthy(ctx context.Context, job string) bool {
	nomadAddr := s.cfg.NomadAddr
	if nomadAddr == "" {
		nomadAddr = "http://127.0.0.1:4646"
	}
	cleanAddr := strings.TrimRight(nomadAddr, "/")

	// 1. Check job deployments first
	depEndpoint := fmt.Sprintf("%s/v1/job/%s/deployments", cleanAddr, job)
	reqDep, err := http.NewRequestWithContext(ctx, http.MethodGet, depEndpoint, nil)
	if err == nil {
		if s.cfg.NomadToken != "" {
			reqDep.Header.Set("X-Nomad-Token", s.cfg.NomadToken)
		}
		respDep, err := s.httpClient.Do(reqDep)
		if err == nil {
			defer respDep.Body.Close()
			if respDep.StatusCode == http.StatusOK {
				var deps []struct {
					ID                string `json:"ID"`
					Status            string `json:"Status"`
					StatusDescription string `json:"StatusDescription"`
					JobVersion        uint64 `json:"JobVersion"`
					CreateIndex       uint64 `json:"CreateIndex"`
				}
				if err := json.NewDecoder(respDep.Body).Decode(&deps); err == nil && len(deps) > 0 {
					// Nomad does NOT sort deployments newest first! Sort by CreateIndex descending.
					sort.SliceStable(deps, func(i, j int) bool {
						return deps[i].CreateIndex > deps[j].CreateIndex
					})

					latest := deps[0]
					if !strings.EqualFold(latest.Status, "successful") {
						return false
					}

					// Auto-Revert Trap Protection:
					// 1. Check if the latest deployment status description indicates a rollback
					desc := strings.ToLower(latest.StatusDescription)
					if strings.Contains(desc, "roll") || strings.Contains(desc, "revert") {
						return false
					}

					// 2. If any deployment in the history has a higher JobVersion than latest,
					// then Nomad rolled back to an older version and the candidate deployment failed.
					for _, d := range deps[1:] {
						if d.JobVersion > latest.JobVersion {
							return false
						}
					}

					return true
				}
			}
		}
	}

	// 2. Fallback to allocations for jobs without deployment stanzas
	allocEndpoint := fmt.Sprintf("%s/v1/job/%s/allocations", cleanAddr, job)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, allocEndpoint, nil)
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

	var allocs []NomadJobAllocSummary
	if err := json.NewDecoder(resp.Body).Decode(&allocs); err != nil {
		return false
	}
	if len(allocs) == 0 {
		return s.isNomadBatchTemplateHealthy(ctx, cleanAddr, job)
	}

	hasRunning := false
	for _, a := range allocs {
		// If an allocation is actively failing or pending, the rollout is incomplete
		if a.DesiredStatus == "run" && (a.ClientStatus == "failed" || a.ClientStatus == "pending") {
			return false
		}
		if a.DesiredStatus == "run" && a.ClientStatus == "running" {
			hasRunning = true
		}
	}
	if hasRunning {
		return true
	}
	return s.isNomadBatchTemplateHealthy(ctx, cleanAddr, job)
}

// isNomadBatchTemplateHealthy queries Nomad for periodic or parameterized batch job templates
// that do not maintain persistent allocations between runs.
func (s *RouterServer) isNomadBatchTemplateHealthy(ctx context.Context, cleanAddr, job string) bool {
	jobEndpoint := fmt.Sprintf("%s/v1/job/%s", cleanAddr, job)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, jobEndpoint, nil)
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

	var jobSpec struct {
		Type             string `json:"Type"`
		Status           string `json:"Status"`
		Stop             bool   `json:"Stop"`
		Periodic         *struct {
			Enabled bool `json:"Enabled"`
		} `json:"Periodic"`
		ParameterizedJob interface{} `json:"ParameterizedJob"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&jobSpec); err != nil {
		return false
	}

	isPeriodic := jobSpec.Periodic != nil && jobSpec.Periodic.Enabled
	isParameterized := jobSpec.ParameterizedJob != nil
	isBatchTemplate := isPeriodic || isParameterized

	return strings.EqualFold(jobSpec.Type, "batch") &&
		isBatchTemplate &&
		strings.EqualFold(jobSpec.Status, "running") &&
		!jobSpec.Stop
}

// extractJobsFromMetadata extracts candidate Nomad job names stored in the PR registry metadata JSONB.
func extractJobsFromMetadata(meta map[string]interface{}) []string {
	if meta == nil {
		return nil
	}

	seen := make(map[string]struct{})
	var jobs []string
	addJob := func(s string) {
		clean := strings.TrimSpace(s)
		if clean != "" {
			if _, exists := seen[clean]; !exists {
				seen[clean] = struct{}{}
				jobs = append(jobs, clean)
			}
		}
	}

	if rawJobs, ok := meta["jobs"]; ok {
		switch v := rawJobs.(type) {
		case []interface{}:
			for _, item := range v {
				if s, ok := item.(string); ok {
					addJob(s)
				}
			}
		case []string:
			for _, s := range v {
				addJob(s)
			}
		}
	}
	if rawJob, ok := meta["job"]; ok {
		if s, ok := rawJob.(string); ok {
			addJob(s)
		}
	}
	return jobs
}

// isTaskRestartExhausted returns true if a task has exhausted its restart policy attempts
// (either entering an extended backoff delay under mode="delay" or permanently failing under mode="fail")
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
		if evt.RestartReason == "Exceeded allowed attempts, applying a delay" ||
			evt.Type == "Not Restarting" ||
			strings.HasPrefix(evt.RestartReason, "Exceeded allowed attempts") {
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
			lastEvt := ts.Events[len(ts.Events)-1]
			if lastEvt.Type == "Restart Signaled" || lastEvt.Type == "Restarting" {
				return true
			}
		}
	}
	return false
}

// extractAllocationErrorDetails extracts human-readable failure diagnostics from allocation task states and events.
func extractAllocationErrorDetails(clientDesc string, taskStates map[string]NomadTaskState) string {
	var taskErrors []string

	// Sort task names for deterministic ordering in tests and alerts
	taskNames := make([]string, 0, len(taskStates))
	for name := range taskStates {
		taskNames = append(taskNames, name)
	}
	sort.Strings(taskNames)

	for _, name := range taskNames {
		ts := taskStates[name]
		// Extract error details from tasks that failed, are dead, or have exhausted restart attempts
		if !ts.Failed && !strings.EqualFold(ts.State, "dead") && !isTaskRestartExhausted(ts) {
			continue
		}

		var errMsg string

		// Search events in reverse order to find the latest specific error diagnostic
		for i := len(ts.Events) - 1; i >= 0; i-- {
			evt := ts.Events[i]
			if evt.DriverError != "" {
				errMsg = evt.DriverError
			} else if evt.SetupError != "" {
				errMsg = evt.SetupError
			} else if evt.DownloadError != "" {
				errMsg = evt.DownloadError
			} else if evt.ValidationError != "" {
				errMsg = evt.ValidationError
			} else if evt.VaultError != "" {
				errMsg = evt.VaultError
			} else if evt.Details != nil && evt.Details["driver_error"] != "" {
				errMsg = evt.Details["driver_error"]
			} else if (evt.Type == "Driver Failure" || evt.Type == "Task Setup" || evt.Type == "Failed Validation") && evt.DisplayMessage != "" {
				errMsg = evt.DisplayMessage
			} else if evt.ExitCode != 0 {
				errMsg = fmt.Sprintf("exited with code %d", evt.ExitCode)
				if evt.DisplayMessage != "" {
					errMsg = fmt.Sprintf("%s (%s)", evt.DisplayMessage, errMsg)
				}
			}

			if errMsg != "" {
				break
			}
		}

		// Fallback to last event's DisplayMessage or RestartReason if task is failed/dead/exhausted and no specific error field was found
		if errMsg == "" && len(ts.Events) > 0 {
			lastEvt := ts.Events[len(ts.Events)-1]
			if strings.TrimSpace(lastEvt.DisplayMessage) != "" {
				errMsg = strings.TrimSpace(lastEvt.DisplayMessage)
			} else if strings.TrimSpace(lastEvt.RestartReason) != "" {
				errMsg = strings.TrimSpace(lastEvt.RestartReason)
			}
		}

		if errMsg != "" {
			taskErrors = append(taskErrors, fmt.Sprintf("task %q: %s", name, errMsg))
		}
	}

	if len(taskErrors) > 0 {
		return strings.Join(taskErrors, "\n")
	}

	if strings.TrimSpace(clientDesc) != "" {
		return strings.TrimSpace(clientDesc)
	}

	return "allocation failed"
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
		opts := args.InsertOpts()
		res, err := s.riverClient.Insert(ctx, args, &opts)
		if err != nil {
			log.Printf("[webhooks-router] [nomad] error enqueuing crash alert into river: %v", err)
			return fmt.Errorf("enqueue crash alert: %w", err)
		}
		if res != nil && res.UniqueSkippedAsDuplicate {
			RecordUniqueSkipped("nomad_crash_alert")
		}
		return nil
	}

	return s.ProcessCrashAlert(ctx, args)
}

// ProcessCrashAlert delivers direct Discord messages and AI prompt notifications for unmanaged job crashes.
func (s *RouterServer) ProcessCrashAlert(ctx context.Context, args NomadCrashAlertArgs) error {
	channelID := strings.TrimSpace(s.cfg.SystemChannelID)
	if !IsValidDiscordSnowflake(channelID) {
		log.Printf("[webhooks-router] [crash-alert] system channel ID is not configured or invalid snowflake (%q); dropping alert for job %s", channelID, args.JobID)
		return nil
	}

	if s.dispatcher == nil {
		log.Printf("[webhooks-router] [crash-alert] dispatcher not configured; cannot deliver alert for job %s", args.JobID)
		return nil
	}

	callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	dmContent := fmt.Sprintf("💥 Nomad job `%s` crashed or failed. Following up...", args.JobID)
	if err := s.dispatcher.DispatchDirectMessage(callCtx, DirectMessageRequest{
		ChannelID: channelID,
		Content:   dmContent,
	}); err != nil {
		log.Printf("[webhooks-router] [crash-alert] warning dispatching direct message: %v", err)
		if is4xxClientError(err) {
			log.Printf("[webhooks-router] [crash-alert] direct message failed with 4xx client error; dropping cleanly: %v", err)
			return nil
		}
	}

	lines := []string{
		fmt.Sprintf("Nomad job `%s` failed or crashed unexpectedly (status: %s, allocation ID: %s).", args.JobID, args.Status, args.AllocID),
	}
	if cleanDesc := strings.TrimSpace(args.StatusDesc); cleanDesc != "" {
		lines = append(lines, fmt.Sprintf("Error details:\n```\n%s\n```", truncatePromptDetails(cleanDesc)))
	}
	lines = append(lines, "Please investigate and fix the service failure.")
	prompt := strings.Join(lines, "\n")

	if err := s.dispatcher.DispatchPrompt(callCtx, PromptRequest{
		ChannelID: channelID,
		Prompt:    prompt,
	}); err != nil {
		if is4xxClientError(err) {
			log.Printf("[webhooks-router] [crash-alert] prompt dispatch failed with 4xx client error; dropping cleanly: %v", err)
			return nil
		}
		log.Printf("[webhooks-router] [crash-alert] error dispatching failure prompt for %s: %v", args.JobID, err)
		return fmt.Errorf("dispatch failure prompt for %s: %w", args.JobID, err)
	}

	return nil
}
