package server

import (
	"context"
	"strings"

	"github.com/azylman/aerial/pkg/eriwire"
	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/eri/internal/restateclient"
	"github.com/azylman/aerial/services/eri/pkg/queue"
)

func handlePromptViaRestate(ctx context.Context, client *restateclient.RestateIngressClient, req PromptRequest, channelID, msgID string) (string, error) {
	reqEnqueue := queue.ThreadEnqueueRequest{
		Prompt:       req.Prompt,
		ChannelID:    channelID,
		Title:        strings.TrimSpace(req.Title),
		MessageID:    msgID,
		Model:        strings.TrimSpace(req.Model),
		Effort:       strings.TrimSpace(req.Effort),
		SkipIfActive: req.SkipIfActive,
		Traceparent:  tracing.Traceparent(ctx),
	}
	idempotencyKey := "prompt:" + channelID + ":" + msgID
	resp, err := client.EnqueueThread(ctx, channelID, reqEnqueue, idempotencyKey)
	if err != nil {
		return "", err
	}
	return resp.Status, nil
}

func handleDispatchViaRestate(ctx context.Context, client *restateclient.RestateIngressClient, req DispatchRequest, sessionKey, dispatchID string) (string, error) {
	bReq := eriwire.BrainDispatchRequest{
		ID:             dispatchID,
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
	idempotencyKey := dispatchID
	resp, err := client.EnqueueDispatch(ctx, sessionKey, bReq, idempotencyKey)
	if err != nil {
		return "", err
	}
	return resp.Status, nil
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
