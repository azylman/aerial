package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
)

var validAgentSlugRegex = regexp.MustCompile("^[a-z0-9-]+$")

// IsValidAgentSlug reports whether an agent slug is a valid non-empty DNS-compatible slug.
func IsValidAgentSlug(slug string) bool {
	return validAgentSlugRegex.MatchString(strings.TrimSpace(slug))
}

// ParseCovenExtractSubject extracts agent and threadID from a 5-token NATS subject:
// aerial.coalesced.coven_extract.<agent>.<thread_id>.
func ParseCovenExtractSubject(subject string) (agent, threadID string, ok bool) {
	tokens := strings.Split(strings.TrimSpace(subject), ".")
	if len(tokens) != 5 || tokens[0] != "aerial" || tokens[1] != "coalesced" || tokens[2] != "coven_extract" {
		return "", "", false
	}
	agent = strings.TrimSpace(tokens[3])
	threadID = strings.TrimSpace(tokens[4])
	if !IsValidAgentSlug(agent) || threadID == "" {
		return "", "", false
	}
	return agent, threadID, true
}

// covenMaxNakDelay caps the retry-forever backoff for indexed events.
const covenMaxNakDelay = 1 * time.Hour

// covenNakDelay returns min(1s*2^(n-1), 1h) for delivery count n.
// Coven never gives up on a decodable event: failures always NakWithDelay,
// so every delay value feeds a retry, never a drop.
func covenNakDelay(numDelivered uint64) time.Duration {
	if numDelivered <= 1 {
		return 1 * time.Second
	}
	shift := min(numDelivered-1, 20)
	if d := (1 * time.Second) << shift; d > 0 && d < covenMaxNakDelay {
		return d
	}
	return covenMaxNakDelay
}

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
	if !IsValidAgentSlug(agent) {
		return nil
	}
	isAll := strings.EqualFold(strings.TrimSpace(role), "all")
	if !isAll && agent != NormalizeStack(stack) {
		return nil
	}
	if err := extract(ctx, agent, threadID); err != nil {
		if nakErr := msg.NakWithDelay(covenNakDelay(msg.NumDelivered())); nakErr != nil {
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
