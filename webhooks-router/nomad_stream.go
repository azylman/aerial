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

// NomadAllocationEventPayload captures allocation updates from Nomad.
type NomadAllocationEventPayload struct {
	Allocation struct {
		ID            string `json:"ID"`
		JobID         string `json:"JobID"`
		DesiredStatus string `json:"DesiredStatus"` // "run", "stop"
		ClientStatus  string `json:"ClientStatus"`  // "running", "pending", "failed", "complete"
	} `json:"Allocation"`
}

// NomadJobAllocSummary represents a summary item from GET /v1/job/{job}/allocations.
type NomadJobAllocSummary struct {
	ID            string `json:"ID"`
	JobID         string `json:"JobID"`
	DesiredStatus string `json:"DesiredStatus"`
	ClientStatus  string `json:"ClientStatus"`
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

		delay := sub.reconnectDelay
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

	streamURL := fmt.Sprintf("%s/v1/event/stream?topic=Deployment:*&topic=Allocation:*", sub.nomadAddr)
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
	switch event.Topic {
	case "Deployment":
		var depPayload NomadDeploymentEventPayload
		if err := json.Unmarshal(event.Payload, &depPayload); err != nil {
			return
		}
		dep := depPayload.Deployment
		if strings.EqualFold(dep.Status, "successful") {
			sub.server.HandleNomadJobSuccess(ctx, dep.JobID, dep.ID)
		}

	case "Allocation":
		var allocPayload NomadAllocationEventPayload
		if err := json.Unmarshal(event.Payload, &allocPayload); err != nil {
			return
		}
		alloc := allocPayload.Allocation
		if alloc.DesiredStatus == "run" && alloc.ClientStatus == "running" {
			sub.server.HandleNomadJobSuccess(ctx, alloc.JobID, alloc.ID)
		}
	}
}

// HandleNomadJobSuccess processes a confirmed successful deployment or running allocation for a Nomad job.
func (s *RouterServer) HandleNomadJobSuccess(ctx context.Context, jobID, refID string) {
	if s.registry == nil {
		return
	}

	cleanJob := strings.TrimSpace(jobID)
	if cleanJob == "" {
		return
	}

	targetID, prNumber, mergeSHA, repo, updated, err := s.registry.AtomicTransitionDeployedByJob(ctx, cleanJob)
	if err != nil {
		log.Printf("[webhooks-router] [nomad] error transitioning job %s to deployed: %v", cleanJob, err)
		return
	}
	if !updated {
		return
	}

	log.Printf("[webhooks-router] [nomad] deployment confirmed for %s#%d (job: %s, commit: %s, target: %s, ref: %s)",
		repo, prNumber, cleanJob, mergeSHA, targetID, refID)

	if IsValidDiscordSnowflake(targetID) && s.dispatcher != nil {
		msg := fmt.Sprintf("Deployment succeeded for job %s (PR #%d, commit %s).", cleanJob, prNumber, mergeSHA)
		go func(tID, m string) {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("[webhooks-router] [dispatcher] PANIC recovered in DispatchDirectMessage (nomad stream): %v", r)
				}
			}()
			dispCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := s.dispatcher.DispatchDirectMessage(dispCtx, DirectMessageRequest{
				ChannelID: tID,
				Content:   m,
			}); err != nil {
				log.Printf("[webhooks-router] [dispatcher] error dispatching direct message for %s: %v", cleanJob, err)
			}
		}(targetID, msg)
	}
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
				s.HandleNomadJobSuccess(ctx, job, "startup-sweep")
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
					ID     string `json:"ID"`
					Status string `json:"Status"`
				}
				if err := json.NewDecoder(respDep.Body).Decode(&deps); err == nil && len(deps) > 0 {
					// Deployments are sorted newest first; return true only if the newest is successful
					return strings.EqualFold(deps[0].Status, "successful")
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
	return hasRunning
}

// extractJobsFromMetadata extracts candidate Nomad job names stored in the PR registry metadata JSONB.
func extractJobsFromMetadata(meta map[string]interface{}) []string {
	if meta == nil {
		return nil
	}

	var jobs []string
	if rawJobs, ok := meta["jobs"]; ok {
		switch v := rawJobs.(type) {
		case []interface{}:
			for _, item := range v {
				if s, ok := item.(string); ok && strings.TrimSpace(s) != "" {
					jobs = append(jobs, strings.TrimSpace(s))
				}
			}
		case []string:
			jobs = append(jobs, v...)
		}
	}
	if rawJob, ok := meta["job"]; ok {
		if s, ok := rawJob.(string); ok && strings.TrimSpace(s) != "" {
			jobs = append(jobs, strings.TrimSpace(s))
		}
	}
	return jobs
}

// jobNameFromImage maps a container image reference to its candidate Nomad job name.
func jobNameFromImage(imageRef string) string {
	clean := strings.TrimSpace(imageRef)
	for svc, img := range aerialServiceImageMap {
		if strings.EqualFold(img, clean) {
			return svc
		}
	}
	base := clean
	if idx := strings.LastIndex(base, ":"); idx != -1 {
		base = base[:idx]
	}
	if idx := strings.LastIndex(base, "/"); idx != -1 {
		base = base[idx+1:]
	}
	base = strings.TrimPrefix(base, "aerial-")
	return base
}
