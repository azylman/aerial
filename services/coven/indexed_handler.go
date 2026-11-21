package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/k8sauth"
	"github.com/azylman/aerial/pkg/pubsub"
)

// ParseCovenExtractSubject extracts agent and threadID from a 5-token NATS subject:
// aerial.coalesced.coven_extract.<agent>.<thread_id>.
func ParseCovenExtractSubject(subject string) (agent, threadID string, ok bool) {
	tokens := strings.Split(strings.TrimSpace(subject), ".")
	if len(tokens) != 5 || tokens[0] != "aerial" || tokens[1] != "coalesced" || tokens[2] != "coven_extract" {
		return "", "", false
	}
	agent = strings.TrimSpace(tokens[3])
	threadID = strings.TrimSpace(tokens[4])
	if !k8sauth.IsValidAgentSlug(strings.TrimSpace(agent)) || threadID == "" {
		return "", "", false
	}
	return agent, threadID, true
}

// covenMaxNakDelay caps the retry-forever pubsub.NakBackoff curve for indexed events.
// Coven never gives up on a decodable event: failures always NakWithDelay,
// so every delay value feeds a retry, never a drop.
const covenMaxNakDelay = 1 * time.Hour

// HandleTranscriptsIndexed routes one TranscriptsIndexedEvent to extraction or job publishing.
// When role is "all", it accepts events for any valid agent slug. When role is not "all",
// it filters strictly for ev.EffectiveAgent() == NormalizeStack(stack).
func HandleTranscriptsIndexed(ctx context.Context, msg pubsub.Message, stack, role string, extract func(ctx context.Context, agent, threadID string) error) error {
	var ev pubsub.TranscriptsIndexedEvent
	if err := json.Unmarshal(msg.Data(), &ev); err != nil {
		slog.ErrorContext(ctx, "[Coven] error unmarshaling transcripts indexed event", "error", err)
		if termErr := msg.Term(); termErr != nil {
			slog.WarnContext(ctx, "[Coven] error terming transcripts indexed event", "error", termErr)
		}
		return nil
	}
	threadID := strings.TrimSpace(ev.ThreadID)
	if threadID == "" {
		return nil
	}
	agent := ev.EffectiveAgent()
	if !k8sauth.IsValidAgentSlug(strings.TrimSpace(agent)) {
		return nil
	}
	isAll := strings.EqualFold(strings.TrimSpace(role), "all")
	if !isAll && agent != NormalizeStack(stack) {
		return nil
	}
	if err := extract(ctx, agent, threadID); err != nil {
		if nakErr := msg.NakWithDelay(pubsub.NakBackoff(msg.NumDelivered(), covenMaxNakDelay)); nakErr != nil {
			slog.WarnContext(ctx, "[Coven] error naking transcripts indexed event", "error", nakErr)
		}
		return err
	}
	return nil
}

// newTranscriptsIndexedHandler returns a pubsub.Handler that publishes a coalesced extraction job to
// pubsub.SubjectCovenExtractCoalescedForAgent. When role is "all", events for all valid agents are accepted.
func newTranscriptsIndexedHandler(b pubsub.Bus, stack string, role ...string) pubsub.Handler {
	normalizedStack := NormalizeStack(stack)
	userRole := ""
	if len(role) > 0 {
		userRole = strings.TrimSpace(role[0])
	}
	return func(ctx context.Context, msg pubsub.Message) error {
		return HandleTranscriptsIndexed(ctx, msg, normalizedStack, userRole, func(extractCtx context.Context, agent, threadID string) error {
			msgID := fmt.Sprintf("coven:extract:%s:%s:%d", agent, threadID, time.Now().UnixNano())
			args := pubsub.CovenExtractJobArgs{
				Agent:    agent,
				Stack:    agent,
				ThreadID: threadID,
			}
			return b.Publish(extractCtx, pubsub.SubjectCovenExtractCoalescedForAgent(agent, threadID), msgID, args)
		})
	}
}

// darkCovenAnchor keeps deferred watermark symbols reachable in the production call graph.
var _ = func() bool {
	_ = ScopedThreadKey("", "")
	return true
}()
