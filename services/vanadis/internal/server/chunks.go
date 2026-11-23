package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/agent"
	"github.com/azylman/aerial/pkg/buildplan"
	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/k8sauth"
	"github.com/azylman/aerial/pkg/strutil"
)

const (
	chunkModeAction          = "action"
	errMissingCallerIdentity = "unauthorized: missing caller identity"
	fieldBuild               = "build"
	fieldStatus              = "status"
	fieldBuildID             = "build_id"
	fieldChunks              = "chunks"
	fieldChunk               = "chunk"
	statusRunning            = "running"
	statusCompleted          = "completed"
	statusCancelled          = "cancelled"
	statusQueued             = "queued"
)

// AttachChunkPRRequest represents the payload for POST /chunks/attach-pr.
type AttachChunkPRRequest = buildplan.AttachChunkPRRequest

func (s *VanadisServer) authorizeBuildUpdate(ctx context.Context, buildAgent string) error {
	if !s.authEnabled() {
		return nil
	}
	caller, ok := k8sauth.CallerFromContext(ctx)
	if !ok {
		return &k8sauth.UnauthorizedError{Message: errMissingCallerIdentity}
	}
	return s.auth.Authorize(ctx, caller, "builds", "update", strutil.FirstNonEmpty(buildAgent, agent.Default))
}

// authorizeBuildMutation loads the build and authorizes the caller to update
// it, returning the build so the mutation can route to its runner.
func (s *VanadisServer) authorizeBuildMutation(w http.ResponseWriter, r *http.Request, buildID string) (*VanadisBuild, bool) {
	if s.store == nil {
		writeErr(w, http.StatusInternalServerError, "vanadis store not configured")
		return nil, false
	}
	build, _, err := s.store.GetBuild(r.Context(), buildID)
	if err == nil && build == nil {
		err = ErrBuildNotFound
	}
	if err != nil {
		writeStoreErr(r.Context(), w, err)
		return nil, false
	}
	if err := s.authorizeBuildUpdate(r.Context(), build.Agent); err != nil {
		k8sauth.WriteError(w, err)
		return nil, false
	}
	return build, true
}

// mutateBuildObject calls a handler on the Build object.
func (s *VanadisServer) mutateBuildObject(ctx context.Context, build *VanadisBuild, handler string, req, out any) error {
	return s.callRestateIngress(ctx, s.restateObjectPath("", build.ID, handler), req, out)
}

const maxTrackedActiveStepBuilds = 256

func (s *VanadisServer) forwardActiveStep(ctx context.Context, build *VanadisBuild, req UpdateActiveStepRequest) error {
	path := s.restateObjectPath("", build.ID, "UpdateActiveStep")
	return s.callRestateIngress(ctx, path, req, nil)
}

func (s *VanadisServer) clearBuildStepMapLocked(buildID, chunkID string) {
	m := s.stepsByBuild[buildID]
	if m == nil {
		return
	}
	delete(m, chunkID)
	if len(m) > 0 {
		return
	}
	delete(s.stepsByBuild, buildID)
	if tm := s.trailingTimer[buildID]; tm != nil {
		tm.Stop()
		delete(s.trailingTimer, buildID)
	}
	if !s.inFlightBuilds[buildID] {
		delete(s.lastForwardedAt, buildID)
		delete(s.dirtyBuilds, buildID)
	}
}

func (s *VanadisServer) updateBuildStepMapLocked(buildID, chunkID, stepText string) {
	if stepText == "" {
		s.clearBuildStepMapLocked(buildID, chunkID)
		return
	}
	m := s.stepsByBuild[buildID]
	if m == nil {
		m = make(map[string]string)
		s.stepsByBuild[buildID] = m
	}
	m[chunkID] = stepText
}

func (s *VanadisServer) pruneStaleBuildsLocked(now time.Time) {
	if len(s.lastForwardedAt) <= maxTrackedActiveStepBuilds {
		return
	}
	for bID, ts := range s.lastForwardedAt {
		if s.inFlightBuilds[bID] || (now.Sub(ts) <= time.Hour && len(s.lastForwardedAt) <= maxTrackedActiveStepBuilds) {
			continue
		}
		delete(s.lastForwardedAt, bID)
		delete(s.stepsByBuild, bID)
		delete(s.dirtyBuilds, bID)
		if tm := s.trailingTimer[bID]; tm != nil {
			tm.Stop()
			delete(s.trailingTimer, bID)
		}
	}
}

func (s *VanadisServer) scheduleTrailingFlushLocked(ctx context.Context, build *VanadisBuild, remaining time.Duration) {
	if remaining < 10*time.Millisecond {
		remaining = 10 * time.Millisecond
	}
	if s.trailingTimer == nil {
		s.trailingTimer = make(map[string]*time.Timer)
	}
	if s.trailingTimer[build.ID] == nil {
		b := build
		trailingCtx := context.WithoutCancel(ctx)
		s.trailingTimer[build.ID] = time.AfterFunc(remaining, func() {
			s.flushTrailingActiveStep(trailingCtx, b)
		})
	}
}

func (s *VanadisServer) forwardActiveStepLoop(ctx context.Context, build *VanadisBuild) error {
	for {
		s.activeStepMu.Lock()
		now := time.Now()
		if s.nowFn != nil {
			now = s.nowFn()
		}
		s.lastForwardedAt[build.ID] = now
		delete(s.dirtyBuilds, build.ID)
		activeSteps := s.stepsByBuild[build.ID]
		isClearAll := len(activeSteps) == 0
		req := UpdateActiveStepRequest{
			ActiveSteps: maps.Clone(activeSteps),
			ClearAll:    isClearAll,
		}
		s.activeStepMu.Unlock()

		if err := s.forwardActiveStep(ctx, build, req); err != nil {
			s.activeStepMu.Lock()
			delete(s.inFlightBuilds, build.ID)
			delete(s.dirtyBuilds, build.ID)
			s.activeStepMu.Unlock()
			return err
		}

		s.activeStepMu.Lock()
		if !s.dirtyBuilds[build.ID] {
			delete(s.inFlightBuilds, build.ID)
			if len(s.stepsByBuild[build.ID]) == 0 {
				delete(s.stepsByBuild, build.ID)
				delete(s.lastForwardedAt, build.ID)
				delete(s.dirtyBuilds, build.ID)
			}
			s.activeStepMu.Unlock()
			return nil
		}
		s.activeStepMu.Unlock()
	}
}

func (s *VanadisServer) flushTrailingActiveStep(ctx context.Context, build *VanadisBuild) {
	s.activeStepMu.Lock()
	delete(s.trailingTimer, build.ID)
	if s.inFlightBuilds[build.ID] {
		s.dirtyBuilds[build.ID] = true
		s.activeStepMu.Unlock()
		return
	}
	if len(s.stepsByBuild[build.ID]) == 0 {
		s.activeStepMu.Unlock()
		return
	}
	s.inFlightBuilds[build.ID] = true
	s.activeStepMu.Unlock()

	defer func() {
		s.activeStepMu.Lock()
		delete(s.inFlightBuilds, build.ID)
		s.activeStepMu.Unlock()
	}()

	if err := s.forwardActiveStepLoop(ctx, build); err != nil {
		slog.WarnContext(ctx, "[vanadis] trailing active step forward warning", "build_id", build.ID, "error", err)
	}
}

func (s *VanadisServer) shouldDebounceActiveStepLocked(ctx context.Context, build *VanadisBuild, isClear bool, now time.Time, minInterval time.Duration) bool {
	buildID := build.ID
	if s.inFlightBuilds[buildID] {
		s.dirtyBuilds[buildID] = true
		return true
	}
	if isClear {
		return false
	}
	last := s.lastForwardedAt[buildID]
	if !last.IsZero() && now.Sub(last) < minInterval {
		s.scheduleTrailingFlushLocked(ctx, build, minInterval-now.Sub(last))
		return true
	}
	return false
}

func (s *VanadisServer) processActiveStepUpdate(w http.ResponseWriter, r *http.Request, build *VanadisBuild, req UpdateActiveStepRequest) {
	if err := s.authorizeBuildUpdate(r.Context(), build.Agent); err != nil {
		k8sauth.WriteError(w, err)
		return
	}

	chunkID := strings.TrimSpace(req.ChunkID)
	stepText := strutil.FirstNonEmpty(strings.TrimSpace(req.ActiveStep), strings.TrimSpace(req.Step))

	now := time.Now()
	if s.nowFn != nil {
		now = s.nowFn()
	}
	minInterval := s.minActiveStepInterval
	if minInterval <= 0 {
		minInterval = 2 * time.Second
	}

	s.activeStepMu.Lock()
	if s.stepsByBuild == nil {
		s.stepsByBuild = make(map[string]map[string]string)
	}
	if s.inFlightBuilds == nil {
		s.inFlightBuilds = make(map[string]bool)
	}
	if s.lastForwardedAt == nil {
		s.lastForwardedAt = make(map[string]time.Time)
	}
	if s.dirtyBuilds == nil {
		s.dirtyBuilds = make(map[string]bool)
	}

	s.pruneStaleBuildsLocked(now)

	if stepText != "" && s.stepsByBuild[build.ID] != nil && s.stepsByBuild[build.ID][chunkID] == stepText {
		s.activeStepMu.Unlock()
		httpkit.WriteJSON(w, http.StatusOK, map[string]string{fieldStatus: "ok"})
		return
	}

	s.updateBuildStepMapLocked(build.ID, chunkID, stepText)

	if s.shouldDebounceActiveStepLocked(r.Context(), build, stepText == "", now, minInterval) {
		s.activeStepMu.Unlock()
		httpkit.WriteJSON(w, http.StatusOK, map[string]string{fieldStatus: "ok"})
		return
	}

	if tm := s.trailingTimer[build.ID]; tm != nil {
		tm.Stop()
		delete(s.trailingTimer, build.ID)
	}

	s.inFlightBuilds[build.ID] = true
	s.activeStepMu.Unlock()

	defer func() {
		s.activeStepMu.Lock()
		delete(s.inFlightBuilds, build.ID)
		s.activeStepMu.Unlock()
	}()

	if err := s.forwardActiveStepLoop(r.Context(), build); err != nil {
		writeStoreErr(r.Context(), w, err)
		return
	}

	httpkit.WriteJSON(w, http.StatusOK, map[string]string{fieldStatus: "ok"})
}

func (s *VanadisServer) handleChunksActiveStep(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if s.store == nil {
		writeErr(w, http.StatusInternalServerError, "vanadis store not configured")
		return
	}
	var req UpdateActiveStepRequest
	if !s.decodeJSONBody(w, r, &req) {
		return
	}
	if strings.TrimSpace(req.ChunkID) == "" {
		writeErr(w, http.StatusBadRequest, "chunk_id is required")
		return
	}
	_, build, err := s.store.GetChunk(r.Context(), req.ChunkID)
	if err != nil {
		writeStoreErr(r.Context(), w, err)
		return
	}
	if build == nil {
		writeStoreErr(r.Context(), w, ErrBuildNotFound)
		return
	}
	s.processActiveStepUpdate(w, r, build, req)
}

func (s *VanadisServer) handleBuildActiveStep(w http.ResponseWriter, r *http.Request, id string) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if s.store == nil {
		writeErr(w, http.StatusInternalServerError, "vanadis store not configured")
		return
	}
	var req UpdateActiveStepRequest
	if !s.decodeJSONBody(w, r, &req) {
		return
	}
	if strings.TrimSpace(req.ChunkID) == "" {
		writeErr(w, http.StatusBadRequest, "chunk_id is required")
		return
	}
	build, _, err := s.store.GetBuild(r.Context(), id)
	if err != nil {
		writeStoreErr(r.Context(), w, err)
		return
	}
	s.processActiveStepUpdate(w, r, build, req)
}

func (s *VanadisServer) handleChunkByID(w http.ResponseWriter, r *http.Request) {
	if s.store == nil {
		writeErr(w, http.StatusInternalServerError, "vanadis store not configured")
		return
	}
	if r.Method != http.MethodGet {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if err := s.authorizeBuildRead(r.Context(), "get"); err != nil {
		k8sauth.WriteError(w, err)
		return
	}
	trimmed := strings.Trim(strings.TrimPrefix(r.URL.Path, "/chunks/"), "/")
	if trimmed == "" {
		writeErr(w, http.StatusBadRequest, "chunk id is required")
		return
	}
	chunk, build, err := s.store.GetChunk(r.Context(), trimmed)
	if err != nil {
		writeStoreErr(r.Context(), w, err)
		return
	}
	httpkit.WriteJSON(w, http.StatusOK, map[string]any{fieldChunk: chunk, fieldBuild: build})
}

func decodeChunkPRPost[T any](s *VanadisServer, w http.ResponseWriter, r *http.Request, missingMsg string, valid func(*T) bool) (T, bool) {
	var zero T
	if s.store == nil {
		writeErr(w, http.StatusInternalServerError, "vanadis store not configured")
		return zero, false
	}
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return zero, false
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 2<<20))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "failed to read request body")
		return zero, false
	}
	var req T
	if err := json.Unmarshal(body, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json payload")
		return zero, false
	}
	if !valid(&req) {
		writeErr(w, http.StatusBadRequest, missingMsg)
		return zero, false
	}
	return req, true
}

func (s *VanadisServer) handleAttachChunkPR(w http.ResponseWriter, r *http.Request) {
	req, ok := decodeChunkPRPost(s, w, r, "chunk_id or repo, and valid pr_number are required", func(req *AttachChunkPRRequest) bool {
		req.ChunkID = strings.TrimSpace(req.ChunkID)
		req.Repo = strings.TrimSpace(req.Repo)
		return (req.ChunkID != "" || req.Repo != "") && req.PRNumber > 0
	})
	if !ok {
		return
	}
	attachAgent, err := s.resolveAttachCallerAgent(r.Context(), req.Agent)
	if err != nil {
		k8sauth.WriteError(w, err)
		return
	}
	req.Agent = attachAgent
	if req.ChunkID != "" && req.PRNumber > 0 && s.store != nil {
		if err := s.store.PreRecordChunkPR(r.Context(), req.ChunkID, req.Repo, req.PRNumber); err != nil {
			slog.WarnContext(r.Context(), "[vanadis] prerecord chunk pr failed", "chunk_id", req.ChunkID, "repo", req.Repo, "pr_number", req.PRNumber, "error", err)
		}
	}
	var resp AttachChunkPRResponse
	idempotencyKey := fmt.Sprintf("attach-pr:%s:%s:%d", req.ChunkID, req.Repo, req.PRNumber)
	if err := s.callRestateIngressKeyed(r.Context(), s.restateRouterPath("RouteAttachPR"), idempotencyKey, req, &resp); err != nil {
		writeStoreErr(r.Context(), w, err)
		return
	}
	httpkit.WriteJSON(w, http.StatusOK, map[string]string{
		fieldStatus:  "attached",
		fieldBuildID: resp.BuildID,
		"chunk_id":   resp.ChunkID,
		"thread_id":  resp.ThreadID,
	})
}

// resolveAttachCallerAgent returns the agent that owns, and whose runner
// executes, a standalone build created by an attach-PR call. Agent callers
// are pinned to their own ServiceAccount agent; service principals and
// unauthenticated deployments may name one, defaulting to the default agent.
func (s *VanadisServer) resolveAttachCallerAgent(ctx context.Context, requested string) (string, error) {
	requested = agent.Clean(requested)
	if s.authEnabled() {
		caller, ok := k8sauth.CallerFromContext(ctx)
		if !ok {
			return "", &k8sauth.UnauthorizedError{Message: errMissingCallerIdentity}
		}
		if !caller.IsService {
			requested = agent.Clean(caller.CallerAgent)
		}
	}
	if requested != "" && !k8sauth.IsValidAgentSlug(requested) {
		return "", &k8sauth.ValidationError{Message: fmt.Sprintf("invalid agent %q", requested)}
	}
	return strutil.FirstNonEmpty(requested, agent.Default), nil
}

func (s *VanadisServer) decodeJSONBody(w http.ResponseWriter, r *http.Request, target any) bool {
	if s.store == nil {
		writeErr(w, http.StatusInternalServerError, "vanadis store not configured")
		return false
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 2<<20))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "failed to read request body")
		return false
	}
	if err = json.Unmarshal(body, target); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json payload")
		return false
	}
	return true
}
