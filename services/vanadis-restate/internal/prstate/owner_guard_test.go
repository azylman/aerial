package prstate

import (
	"maps"
	"testing"

	dto "github.com/prometheus/client_model/go"
	restate "github.com/restatedev/sdk-go"
	"github.com/stretchr/testify/require"
)

// rejectedCount reads prstate_legacy_events_rejected_total for handler.
func rejectedCount(t *testing.T, handler string) float64 {
	t.Helper()
	var m dto.Metric
	require.NoError(t, legacyEventsRejectedTotal.WithLabelValues(handler).Write(&m))
	return m.GetCounter().GetValue()
}

// bindShared adapts a shared (read-only) handler to the sim signature.
func bindShared[I, O any](f func(restate.ObjectSharedContext, I) (O, error)) simHandler {
	return func(ctx restate.ObjectContext, input []byte) error {
		in, err := decodeInput[I](input)
		if err != nil {
			return err
		}
		_, err = f(ctx, in)
		return err
	}
}

// newGuardSim is the stale-head sim with every remaining handler that takes
// a synapse-stamped event bound, so each guard is driven through the real
// service method.
func newGuardSim(t *testing.T) *paritySim {
	t.Helper()
	sim := newStaleHeadSim(t)
	pr := NewPRObjectService()
	repo := NewRepoObjectService()
	maps.Copy(sim.handlers, map[string]simHandler{
		"PRObject/HandleCheckRerunRequested":     bindErr(pr.HandleCheckRerunRequested),
		"PRObject/HandleCollaboratorAlert":       bindErr(pr.HandleCollaboratorAlert),
		"PRObject/HandleConflictDetected":        bindShared(pr.HandleConflictDetected),
		"PRObject/HandleConflictPublished":       bindOut(pr.HandleConflictPublished),
		"PRObject/HandleCIRecovery":              bindShared(pr.HandleCIRecovery),
		"PRObject/HandleBuildMetadata":           bindErr(pr.HandleBuildMetadata),
		"PRObject/HandleDeployDispatched":        bindShared(pr.HandleDeployDispatched),
		"PRObject/HandleReconcileLost":           bindErr(pr.HandleReconcileLost),
		"PRObject/HandleConflictCheck":           bindErr(pr.HandleConflictCheck),
		"PRObject/HandleBranchConflict":          bindErr(pr.HandleBranchConflict),
		"PRObject/HandleHeadRefMissing":          bindErr(pr.HandleHeadRefMissing),
		"PRObject/RetryReadiness":                bindOut(pr.RetryReadiness),
		"RepoObject/HandleFanout":                bindErr(repo.HandleFanout),
		"RepoObject/HandleWorkflowRunBySHA":      bindErr(repo.HandleWorkflowRunBySHA),
		"RepoObject/HandleImageReadyBySHA":       bindErr(repo.HandleImageReadyBySHA),
		"RepoObject/" + RepoHandlerWorkflowRerun: bindErr(repo.HandleWorkflowRerun),
	})
	return sim
}

// Legacy-stamped payloads per input type.
const (
	legacyPREvent   = `{"repo":"azylman/aerial","pr_number":101,"head_sha":"h1","merge_sha":"m1","check_name":"ci","effects_owner":"legacy"}`
	legacyRepoEvent = `{"repo":"azylman/aerial","main_sha":"m1","head_sha":"h1","run_id":55,"ref":"refs/heads/main","images":["eri"],"effects_owner":"legacy"}`
	legacySHAEvent  = `{"repo":"azylman/aerial","sha":"h1","handler":"HandleCheckFailed","event":{"event":"check_failed","effects_owner":"legacy"}}`
	legacyRegister  = `{"repo":"azylman/aerial","pr_number":101,"branch":"feat","head_sha":"h1","effects_owner":"legacy"}`
	legacyReadiness = `{"merge_sha":"m1","effects_owner":"legacy"}`
	prKey           = "azylman/aerial#101"
	repoKey         = "azylman/aerial"
)

// TestRejectLegacyOwner pins that every handler taking a synapse-stamped
// event rejects an effects_owner=legacy stamp with a terminal 409, counts
// it under its handler label, and writes no state and sends nothing.
func TestRejectLegacyOwner(t *testing.T) {
	t.Parallel()
	tests := []struct {
		object  string
		handler string
		key     string
		input   string
	}{
		{object: "PRObject", handler: "HandleCheckFailed", key: prKey, input: legacyPREvent},
		{object: "PRObject", handler: "HandleCheckPassed", key: prKey, input: legacyPREvent},
		{object: "PRObject", handler: "HandleCheckRerunRequested", key: prKey, input: legacyPREvent},
		{object: "PRObject", handler: "HandleCollaboratorAlert", key: prKey, input: legacyPREvent},
		{object: "PRObject", handler: "HandleConflict", key: prKey, input: legacyPREvent},
		{object: "PRObject", handler: "HandleConflictDetected", key: prKey, input: legacyPREvent},
		{object: "PRObject", handler: "HandleConflictPublished", key: prKey, input: legacyPREvent},
		{object: "PRObject", handler: "HandleCIRecovered", key: prKey, input: legacyPREvent},
		{object: "PRObject", handler: "HandleCIRecovery", key: prKey, input: legacyPREvent},
		{object: "PRObject", handler: "HandleConflictResolved", key: prKey, input: legacyPREvent},
		{object: "PRObject", handler: "HandleMerged", key: prKey, input: legacyPREvent},
		{object: "PRObject", handler: "HandleClosed", key: prKey, input: legacyPREvent},
		{object: "PRObject", handler: "HandleReopened", key: prKey, input: legacyPREvent},
		{object: "PRObject", handler: "HandleOpened", key: prKey, input: legacyPREvent},
		{object: "PRObject", handler: "HandleSelfRegistered", key: prKey, input: legacyPREvent},
		{object: "PRObject", handler: "HandleRolloutProgress", key: prKey, input: legacyPREvent},
		{object: "PRObject", handler: "HandleSynchronized", key: prKey, input: legacyPREvent},
		{object: "PRObject", handler: "HandleDeployFailed", key: prKey, input: legacyPREvent},
		{object: "PRObject", handler: "HandleBuildMetadata", key: prKey, input: legacyPREvent},
		{object: "PRObject", handler: "HandleCheckStatus", key: prKey, input: legacyPREvent},
		{object: "PRObject", handler: "HandleDeploySuccess", key: prKey, input: legacyPREvent},
		{object: "PRObject", handler: "HandleDeployDispatched", key: prKey, input: legacyPREvent},
		{object: "PRObject", handler: "HandleDeploySuccessPublished", key: prKey, input: legacyPREvent},
		{object: "PRObject", handler: "HandleMainAdvanced", key: prKey, input: legacyPREvent},
		{object: "PRObject", handler: "HandleDeployObserved", key: prKey, input: legacyPREvent},
		{object: "PRObject", handler: "HandleReconcileLost", key: prKey, input: legacyPREvent},
		{object: "PRObject", handler: "HandleDeployAttemptFailed", key: prKey, input: legacyPREvent},
		{object: "PRObject", handler: "HandleKustomizationRequested", key: prKey, input: legacyPREvent},
		{object: "PRObject", handler: "HandleReadinessRetry", key: prKey, input: legacyPREvent},
		{object: "PRObject", handler: "HandleConflictCheck", key: prKey, input: legacyPREvent},
		{object: "PRObject", handler: "HandleBranchConflict", key: prKey, input: legacyPREvent},
		{object: "PRObject", handler: "HandleHeadRefMissing", key: prKey, input: legacyPREvent},
		{object: "PRObject", handler: "Register", key: prKey, input: legacyRegister},
		{object: "PRObject", handler: "RetryReadiness", key: prKey, input: legacyReadiness},
		{object: "RepoObject", handler: "HandleMainPush", key: repoKey, input: legacyRepoEvent},
		{object: "RepoObject", handler: "HandleFanout", key: repoKey, input: legacyRepoEvent},
		{object: "RepoObject", handler: "HandleKustomizationStatus", key: repoKey, input: legacyRepoEvent},
		{object: "RepoObject", handler: "HandleAutoUpdate", key: repoKey, input: legacyRepoEvent},
		{object: "RepoObject", handler: "HandleWorkflowRerun", key: repoKey, input: legacyRepoEvent},
		{object: "RepoObject", handler: "HandleGitPush", key: repoKey, input: legacyRepoEvent},
		{object: "RepoObject", handler: "HandleImagePromotion", key: repoKey, input: legacyRepoEvent},
		{object: "RepoObject", handler: "HandleCheckRunBySHA", key: repoKey, input: legacySHAEvent},
		{object: "RepoObject", handler: "HandleWorkflowRunBySHA", key: repoKey, input: legacySHAEvent},
		{object: "RepoObject", handler: "HandlePushBySHA", key: repoKey, input: legacySHAEvent},
		{object: "RepoObject", handler: "HandleImageReadyBySHA", key: repoKey, input: legacySHAEvent},
	}
	for _, tc := range tests {
		t.Run(tc.object+"/"+tc.handler, func(t *testing.T) {
			t.Parallel()
			before := rejectedCount(t, tc.handler)
			sim := newGuardSim(t)
			err := sim.run(simCall{service: tc.object, key: tc.key, handler: tc.handler, input: []byte(tc.input)})
			require.True(t, restate.IsTerminalError(err), "err = %v", err)
			require.Equal(t, restate.Code(409), restate.AsTerminalError(err).Code())
			require.Empty(t, sim.kv)
			require.Empty(t, sim.pending)
			require.GreaterOrEqual(t, rejectedCount(t, tc.handler), before+1)
		})
	}
}
