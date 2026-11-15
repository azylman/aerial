package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"strings"

	"github.com/azylman/aerial/pkg/pubsub"
)

func (s *VanadisServer) handleCompletedDispatchOutcome(ctx context.Context, ev pubsub.DispatchOutcomeEvent) error {
	res, err := s.store.CompleteActionChunk(ctx, ev.VanadisChunkID)
	if err != nil {
		if errors.Is(err, ErrChunkNotFound) {
			log.Printf("[vanadis] chunk %s not found on dispatch outcome completed", ev.VanadisChunkID)
			return nil
		}
		log.Printf("[vanadis] complete action chunk %s error: %v", ev.VanadisChunkID, err)
		return err
	}
	if res != nil && res.ChunkMode == "pr" {
		if err := s.ProcessStalledChunkCheck(ctx, ev.VanadisChunkID, ev.DispatchID); err != nil {
			log.Printf("[vanadis] stalled chunk check for %s failed: %v", ev.VanadisChunkID, err)
		}
	}
	if res == nil || res.ChunkMode != chunkModeAction {
		return nil
	}
	if !res.Advanced {
		if res.ChunkMode == chunkModeAction && s.restateCfg.RestateIngressURL != "" {
			if c, _, gErr := s.store.GetChunk(ctx, ev.VanadisChunkID); gErr == nil && c != nil && c.Status == statusCompleted {
				return s.reconcileBuildOnRestate(ctx, res.BuildID, "action_completed")
			}
		}
		return nil
	}
	RecordChunkTotal("completed")
	s.syncBuildWidget(ctx, res.BuildID, "")
	s.publishExternalEvent(ctx, "vanadis_chunk", "completed", []string{ev.VanadisChunkID}, "")
	if res.BuildCompleted {
		RecordBuildTotal("completed")
		s.publishExternalEvent(ctx, "vanadis_build", "completed", []string{res.BuildID}, "")
		s.dispatchCompletedBuildWake(ctx, res)
	}
	if s.restateCfg.RestateIngressURL != "" {
		if err := s.reconcileBuildOnRestate(ctx, res.BuildID, "action_completed"); err != nil {
			return err
		}
	}
	return nil
}

func (s *VanadisServer) handleFailedDispatchOutcome(ctx context.Context, ev pubsub.DispatchOutcomeEvent) error {
	reason := strings.TrimSpace(ev.Error)
	if reason == "" {
		reason = "dispatch status " + ev.Status
	}
	if _, _, _, err := s.failChunkInternal(ctx, ev.VanadisChunkID, reason); err != nil {
		if errors.Is(err, ErrChunkNotFound) {
			log.Printf("[vanadis] chunk %s not found on dispatch outcome failure", ev.VanadisChunkID)
			return nil
		}
		log.Printf("[vanadis] fail chunk internal error for %s: %v", ev.VanadisChunkID, err)
		return err
	}
	return nil
}

// HandleDispatchOutcomeEvent processes a dispatch outcome event delivered from JetStream.
func (s *VanadisServer) HandleDispatchOutcomeEvent(ctx context.Context, msg pubsub.Message) error {
	if s == nil || s.store == nil {
		return errors.New("vanadis store not configured")
	}
	var ev pubsub.DispatchOutcomeEvent
	if err := json.Unmarshal(msg.Data(), &ev); err != nil {
		log.Printf("[vanadis] warning: failed to unmarshal dispatch outcome event: %v", err)
		return nil
	}
	ev.DispatchID = strings.TrimSpace(ev.DispatchID)
	ev.VanadisChunkID = strings.TrimSpace(ev.VanadisChunkID)
	if ev.DispatchID == "healthcheck-ping" || ev.VanadisChunkID == "" {
		return nil
	}

	switch strings.ToLower(strings.TrimSpace(ev.Status)) {
	case statusCompleted:
		return s.handleCompletedDispatchOutcome(ctx, ev)
	case statusFailed, "timeout":
		return s.handleFailedDispatchOutcome(ctx, ev)
	default:
		return nil
	}
}
