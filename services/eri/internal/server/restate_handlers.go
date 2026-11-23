package server

import (
	"context"
	"strings"

	"github.com/azylman/aerial/pkg/eriwire"
	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/eri/internal/restateclient"
	"github.com/azylman/aerial/services/eri/pkg/queue"
)

// handlePromptViaRestate enqueues a prompt on the caller's channel. The idempotency key names
// the caller's request (channel + keyID), not the thread it lands in: ThreadSession.Enqueue
// resolves (and if needed creates) that thread in a journaled step. recordID is the caller's own
// message ID; when it is empty (keyID was derived from the request content) Enqueue mints the row
// ID from its invocation, so the derived key only dedupes retries inside Restate's idempotency
// retention and a later identical send is still delivered.
func handlePromptViaRestate(ctx context.Context, client *restateclient.RestateIngressClient, req PromptRequest, channelID, recordID, keyID string) (queue.ThreadEnqueueResponse, error) {
	reqEnqueue := queue.ThreadEnqueueRequest{
		Prompt:       req.Prompt,
		ChannelID:    channelID,
		Title:        strings.TrimSpace(req.Title),
		MessageID:    recordID,
		Model:        strings.TrimSpace(req.Model),
		Effort:       strings.TrimSpace(req.Effort),
		SkipIfActive: req.SkipIfActive,
		Traceparent:  tracing.Traceparent(ctx),
	}
	idempotencyKey := "prompt:" + channelID + ":" + keyID
	return client.EnqueueThread(ctx, channelID, reqEnqueue, idempotencyKey)
}

// handleDispatchViaRestate enqueues a dispatch keyed on keyID. As with prompts, an empty recordID
// (derived keyID) lets DispatchSession.Enqueue mint the row ID from its invocation.
func handleDispatchViaRestate(ctx context.Context, client *restateclient.RestateIngressClient, req DispatchRequest, sessionKey, recordID, keyID string) (eriwire.DispatchEnqueueResponse, error) {
	bReq := eriwire.BrainDispatchRequest{
		ID:             recordID,
		Source:         strings.ToLower(strings.TrimSpace(req.Source)),
		SessionID:      sessionKey,
		TargetID:       req.TargetID,
		VanadisChunkID: strings.TrimSpace(req.VanadisChunkID),
		Prompt:         req.Prompt,
		Effort:         strings.ToLower(strings.TrimSpace(req.Effort)),
		Model:          strings.TrimSpace(req.Model),
		SkipIfActive:   req.SkipIfActive,
		Traceparent:    tracing.Traceparent(ctx),
	}
	return client.EnqueueDispatch(ctx, sessionKey, bReq, keyID)
}

func handleCancelDispatchViaRestate(ctx context.Context, client *restateclient.RestateIngressClient, dispatchID, buildID, reason string) (int, []string, error) {
	resp, err := client.CancelDispatchImmediate(ctx, queue.CancelDispatchControlRequest{
		DispatchID: dispatchID,
		BuildID:    buildID,
		ChunkID:    buildID,
		Reason:     reason,
	})
	if err != nil {
		return 0, nil, err
	}
	return resp.CancelledCount, resp.CancelledIDs, nil
}
