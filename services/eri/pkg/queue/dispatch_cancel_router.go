package queue

import (
	"context"
	"slices"
	"strings"

	restate "github.com/restatedev/sdk-go"
)

const stepResolveDispatchSessionKeys = "resolve-dispatch-session-keys"

// dispatchSessionKeyLister resolves the session keys holding active dispatches.
type dispatchSessionKeyLister interface {
	ListActiveDispatchSessionKeys(ctx context.Context, dispatchID, buildID string) ([]string, error)
}

// DispatchCancelRouter is the coordinator-mode SessionManager service: it fans a dispatch or
// build cancel out to every DispatchSession key holding matching work, without key locks.
type DispatchCancelRouter struct {
	store     dispatchSessionKeyLister
	agentName string
}

// NewDispatchCancelRouter constructs a DispatchCancelRouter for agentName.
func NewDispatchCancelRouter(store dispatchSessionKeyLister, agentName string) *DispatchCancelRouter {
	return &DispatchCancelRouter{store: store, agentName: strings.TrimSpace(agentName)}
}

// ServiceName returns the per-agent SessionManager service name.
func (r *DispatchCancelRouter) ServiceName() string {
	return SessionManagerServiceName(r.agentName)
}

// mergeSessionKeys returns the distinct non-empty keys of listed plus extra, sorted.
func mergeSessionKeys(listed []string, extra string) []string {
	keys := normalizeStepIDs(append(slices.Clone(listed), extra))
	slices.Sort(keys)
	return keys
}

// CancelDispatch resolves the session keys holding the targeted dispatches and calls
// DispatchSession(key).Cancel on each in sorted order, aggregating cancelled IDs.
func (r *DispatchCancelRouter) CancelDispatch(ctx restate.Context, req CancelDispatchControlRequest) (CancelDispatchResponse, error) {
	dispatchID := strings.TrimSpace(req.DispatchID)
	buildID := strings.TrimSpace(req.BuildID)
	if buildID == "" {
		buildID = strings.TrimSpace(req.ChunkID)
	}
	sessionKey := strings.TrimSpace(req.SessionKey)

	keys, err := restate.Run(ctx, func(rc restate.RunContext) ([]string, error) {
		var listed []string
		if dispatchID != "" || buildID != "" {
			var lErr error
			listed, lErr = storeCall(rc, func(sc context.Context) ([]string, error) {
				return r.store.ListActiveDispatchSessionKeys(sc, dispatchID, buildID)
			})
			if lErr != nil {
				return nil, lErr
			}
		}
		return mergeSessionKeys(listed, sessionKey), nil
	}, restate.WithName(stepResolveDispatchSessionKeys))
	if err != nil {
		return CancelDispatchResponse{}, coordinatorError(err)
	}

	cancelReq := CancelDispatchRequest{
		DispatchID: dispatchID,
		ChunkID:    strings.TrimSpace(req.ChunkID),
		BuildID:    strings.TrimSpace(req.BuildID),
		Reason:     strings.TrimSpace(req.Reason),
	}
	svc := DispatchSessionServiceName(r.agentName)
	var ids []string
	for _, key := range keys {
		resp, cErr := restate.Object[CancelDispatchResponse](ctx, svc, key, HandlerCancel).Request(cancelReq)
		if cErr != nil {
			return CancelDispatchResponse{}, coordinatorError(cErr)
		}
		ids = normalizeStepIDs(append(ids, resp.CancelledIDs...))
	}
	return CancelDispatchResponse{Status: "ok", CancelledCount: len(ids), CancelledIDs: ids}, nil
}
