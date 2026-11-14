package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"strings"

	"github.com/azylman/aerial/pkg/pubsub"
)

func (s *VanadisServer) retryNextRunningChunkEnqueue(ctx context.Context, res *VanadisAdvancementResult) error {
	if res == nil || res.ChunkMode != "action" || res.BuildStatus != "running" {
		return nil
	}
	_, chunks, err := s.store.GetBuild(ctx, res.BuildID)
	if err != nil {
		log.Printf("[vanadis] get build %s on retry enqueue error: %v", res.BuildID, err)
		return err
	}
	for _, next := range chunks {
		if next.ChunkIndex > res.MergedChunkIndex && next.Status == "running" {
			if err := s.insertRiverJob(ctx, ExecuteChunkArgs{BuildID: res.BuildID, ChunkIndex: next.ChunkIndex}); err != nil {
				log.Printf("[vanadis] failed to re-enqueue running chunk %d for build %s on redelivery: %v", next.ChunkIndex, res.BuildID, err)
				return err
			}
			break
		}
	}
	return nil
}

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
	if res == nil || !res.Advanced {
		return s.retryNextRunningChunkEnqueue(ctx, res)
	}
	RecordChunkTotal("completed")
	s.syncBuildWidget(ctx, res.BuildID, "")
	if res.BuildCompleted {
		RecordBuildTotal("completed")
		s.dispatchCompletedBuildWake(ctx, res)
	} else if res.NextChunkIndex >= 0 {
		if err := s.insertRiverJob(ctx, ExecuteChunkArgs{BuildID: res.BuildID, ChunkIndex: res.NextChunkIndex}); err != nil {
			log.Printf("[vanadis] failed to enqueue next chunk %d for build %s: %v", res.NextChunkIndex, res.BuildID, err)
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
	case "completed":
		return s.handleCompletedDispatchOutcome(ctx, ev)
	case "failed", "timeout":
		return s.handleFailedDispatchOutcome(ctx, ev)
	default:
		return nil
	}
}
