package build

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	restate "github.com/restatedev/sdk-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/eriwire"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/pubsubtest"
	"github.com/azylman/aerial/services/vanadis-restate/internal/executor"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

func TestBuildObject_LifecycleTransitionsAndFailures(t *testing.T) {
	t.Parallel()
	b1 := model.VanadisBuild{ID: "b-more", Status: "running", ThreadID: "th-1"}
	c1 := model.VanadisChunk{ID: "c1", BuildID: "b-more", ChunkIndex: 0, Title: "C1", Status: "pending", Mode: "action"}

	// 1. Cancel on already terminal build -> store.ErrBuildConflict
	bTerm := model.VanadisBuild{ID: "b-term", Status: "completed", ThreadID: "th-1"}
	storeTerm, _ := seededStore(t, []model.VanadisBuild{bTerm}, nil)
	boTerm := NewBuildObject("", nil).WithAuthority(storeTerm, nil)
	ctxTerm := makeObjectMockContext(t, "b-term")
	_, err := boTerm.Cancel(ctxTerm, CancelBuildRequest{Reason: "test"})
	if err == nil {
		t.Fatal("expected error canceling terminal build")
	}
	assert.Equal(t, "completed", lifecycleBuildStatus(t, storeTerm, "b-term"))

	// 2. Cancel with store update build status error: every build row exists
	// and every build UPDATE fails inside the database.
	storeCancelErr, poolCancelErr := seededStore(t, []model.VanadisBuild{b1, {ID: "b-fail", Status: "running", ThreadID: "th-1"}, {ID: "b-done", Status: "running", ThreadID: "th-1"}}, nil)
	mustExec(t, poolCancelErr, failStatement("vanadis_builds", "UPDATE"))
	boCancelErr := NewBuildObject("", nil).WithAuthority(storeCancelErr, nil)
	ctxCancel := makeObjectMockContext(t, "b-more")
	_, _ = boCancelErr.Cancel(ctxCancel, CancelBuildRequest{Reason: "test"})
	assert.Equal(t, "running", lifecycleBuildStatus(t, storeCancelErr, "b-more"))

	// 3. UpdateChunk error in store
	storeUpdErr := &store.Store{}
	boUpdErr := NewBuildObject("", nil).WithAuthority(storeUpdErr, nil)
	_, _ = boUpdErr.UpdateChunk(ctxCancel, store.UpdateChunkRequest{ChunkRef: "c1", Title: "new"})

	// 4. AppendChunks error in store
	_, _ = boUpdErr.AppendChunks(ctxCancel, AppendChunksRequest{Chunks: []store.CreateChunkInput{{Title: "C2"}}})

	// 5. RemoveChunk error in store
	_ = boUpdErr.RemoveChunk(ctxCancel, RemoveChunkRequest{ChunkRef: "c1"})

	// 6. FailChunk error in store
	_, _ = boUpdErr.FailChunk(ctxCancel, model.FailChunkRequest{ChunkID: "c1", Reason: "boom"})

	// 7. handlePREventType error branches
	eriErr := &mockEriClient{
		promptFn: func(context.Context, string, string) error { return errors.New("prompt err") },
		widgetFn: func(context.Context, executor.StatusWidgetRequest) (string, error) {
			return "", errors.New("widget err")
		},
	}
	execErr := executor.NewExecutor(t.TempDir(), eriErr, &pubsubtest.FaultBus{PublishErr: errors.New("pub err")}, "token", "example-owner", nil)
	boHandleErr := NewBuildObject("", nil).WithAuthority(storeUpdErr, execErr)
	_, _ = restate.Run(ctxCancel, func(rc restate.RunContext) (bool, error) {
		boHandleErr.handlePREventType(rc, "b-more", "c1", "ci_failed", pubsub.PRLifecycleEvent{Event: "ci_failed", Repo: "aerial"})
		boHandleErr.handlePREventType(rc, "b-more", "c1", "conflict", pubsub.PRLifecycleEvent{Event: "conflict"})
		boHandleErr.handlePREventType(rc, "b-more", "c1", "closed", pubsub.PRLifecycleEvent{Event: "closed", Status: "closed"})
		boHandleErr.handlePREventType(rc, "b-more", "c1", "merged", pubsub.PRLifecycleEvent{Event: "merged"})
		return true, nil
	})

	// 8. OnDispatchOutcome error branches in store
	_ = boUpdErr.OnDispatchOutcome(ctxCancel, pubsub.DispatchOutcomeEvent{VanadisChunkID: "c1", Status: "completed"})
	_ = boUpdErr.OnDispatchOutcome(ctxCancel, pubsub.DispatchOutcomeEvent{VanadisChunkID: "c1", Status: "failed", Error: "boom"})

	// 9. evaluateBuildTerminalStatus error branches and retry recovery
	// 9a. evaluateBuildTerminalStatus failing on notifyBuildTerminal (execErr)
	boTermErr := NewBuildObject("", nil).WithAuthority(storeCancelErr, execErr)
	bF := model.VanadisBuild{ID: "b-fail", Status: "running", ThreadID: "th-1"}
	cF := model.VanadisChunk{ID: "c1", Status: "failed"}
	retF, errF := boTermErr.evaluateBuildTerminalStatus(context.Background(), &bF, []model.VanadisChunk{cF}, map[string]bool{}, false)
	assert.Error(t, errF)
	assert.Empty(t, retF)

	bD := model.VanadisBuild{ID: "b-done", Status: "running", ThreadID: "th-1"}
	cD := model.VanadisChunk{ID: "c1", Status: "completed"}
	retD, errD := boTermErr.evaluateBuildTerminalStatus(context.Background(), &bD, []model.VanadisChunk{cD}, map[string]bool{}, false)
	assert.Error(t, errD)
	assert.Empty(t, retD)

	// 9b. evaluateBuildTerminalStatus failing on UpdateBuildStatus (storeCancelErr with nil executor)
	boStoreUpdateErr := NewBuildObject("", nil).WithAuthority(storeCancelErr, nil)
	bFStore := model.VanadisBuild{ID: "b-fail", Status: "running", ThreadID: "th-1"}
	retFStore, errFStore := boStoreUpdateErr.evaluateBuildTerminalStatus(context.Background(), &bFStore, []model.VanadisChunk{cF}, map[string]bool{}, false)
	assert.Error(t, errFStore)
	assert.Empty(t, retFStore)

	bDStore := model.VanadisBuild{ID: "b-done", Status: "running", ThreadID: "th-1"}
	retDStore, errDStore := boStoreUpdateErr.evaluateBuildTerminalStatus(context.Background(), &bDStore, []model.VanadisChunk{cD}, map[string]bool{}, false)
	assert.Error(t, errDStore)
	assert.Empty(t, retDStore)

	// 9c. retry recovery when notifyBuildTerminal fails on attempt 1 (vanadis_builds.status remains running) and succeeds on attempt 2
	var promptAttempt atomic.Int32
	flakyEri := &mockEriClient{
		promptFn: func(context.Context, string, string) error {
			if promptAttempt.Add(1) == 1 {
				return errors.New("transient prompt error")
			}
			return nil
		},
	}
	execFlaky := executor.NewExecutor(t.TempDir(), flakyEri, pubsub.NewMemoryBus(), "token", "example-owner", nil)
	spyStore := &terminalRetrySpyStore{}
	boRetry := NewBuildObject("", nil).WithAuthority(spyStore, execFlaky)

	bRetry := model.VanadisBuild{ID: "b-retry", Status: "running", StatusMessageID: "msg-123", ThreadID: "th-1"}
	// Attempt 1: notifyBuildTerminal fails
	retRetry1, errRetry1 := boRetry.evaluateBuildTerminalStatus(context.Background(), &bRetry, []model.VanadisChunk{cD}, map[string]bool{}, false)
	assert.Error(t, errRetry1)
	assert.Empty(t, retRetry1)
	assert.Empty(t, spyStore.updatedStatus)

	// Attempt 2: retry succeeds
	retRetry2, errRetry2 := boRetry.evaluateBuildTerminalStatus(context.Background(), &bRetry, []model.VanadisChunk{cD}, map[string]bool{}, false)
	assert.NoError(t, errRetry2)
	assert.Equal(t, statusCompleted, retRetry2)
	assert.Equal(t, statusCompleted, spyStore.updatedStatus)

	// 9d. evaluateBuildTerminalStatus default branch widget sync warning
	bRunning := model.VanadisBuild{ID: "b-run", Status: "running", ThreadID: "th-1"}
	cRunning := model.VanadisChunk{ID: "c1", Status: "running"}
	retRun, errRun := boTermErr.evaluateBuildTerminalStatus(context.Background(), &bRunning, []model.VanadisChunk{cRunning}, map[string]bool{}, false)
	assert.NoError(t, errRun)
	assert.Equal(t, statusRunning, retRun)

	// 9e. notifyBuildTerminal nil executor returns nil
	boNilExec := NewBuildObject("", nil).WithAuthority(storeCancelErr, nil)
	assert.NoError(t, boNilExec.notifyBuildTerminal(context.Background(), &bD, []model.VanadisChunk{cD}, statusCompleted, "", "H", "M"))

	// 9f. executeAuthoritativeTransition fails when notifyBuildTerminal fails during continuation
	storeCompCont := lifecycleStore(t, model.VanadisBuild{ID: "b-cont", Status: statusCompleted, ThreadID: "th-1"}, "", model.VanadisChunk{ID: "c1", Mode: "action", Status: "completed"})
	boContErr := NewBuildObject("", nil).WithAuthority(storeCompCont, execErr)
	_, errCont := boContErr.executeAuthoritativeTransition(context.Background(), "b-cont", &BuildMutationRequest{Action: "action_completed", ChunkID: "c1", DispatchID: "vanadis-c1-turn-0"}, nil, nil, false)
	assert.Error(t, errCont)

	// 9g. executeAuthoritativeTransition fails when evaluateBuildTerminalStatus fails
	bFailRunning := model.VanadisBuild{ID: "b-fail-trans", Status: statusRunning, ThreadID: "th-1"}
	storeTransErr := lifecycleStore(t, bFailRunning, "", model.VanadisChunk{ID: "c1", Status: statusFailed})
	boTransErr := NewBuildObject("", nil).WithAuthority(storeTransErr, execErr)
	_, errTrans := boTransErr.executeAuthoritativeTransition(context.Background(), "b-fail-trans", nil, nil, nil, false)
	assert.Error(t, errTrans)
	// The failed wake is sent before the status write, so the build stays running for the retry.
	assert.Equal(t, statusRunning, lifecycleBuildStatus(t, storeTransErr, "b-fail-trans"))

	// 10. UpdateBuild with widget sync error
	storeUpdOk, _ := seededStore(t, []model.VanadisBuild{b1}, []model.VanadisChunk{c1})
	boUpdWidgetErr := NewBuildObject("", nil).WithAuthority(storeUpdOk, execErr)
	_, _ = boUpdWidgetErr.UpdateBuild(ctxCancel, store.UpdateBuildRequest{Title: "new title"})

	// 11. persistBuildCreation with widget sync error
	storeCreate, _ := openStore(t)
	boCreateWidgetErr := NewBuildObject("", nil).WithAuthority(storeCreate, execErr)
	_, _ = restate.Run(ctxCancel, func(rc restate.RunContext) (bool, error) {
		return boCreateWidgetErr.persistBuildCreation(rc, b1, []model.VanadisChunk{c1})
	})

	// 12. AttachPR widget sync error
	_, _ = boUpdWidgetErr.AttachPR(ctxCancel, store.AttachChunkPRRequest{ChunkID: "c1", Repo: "aerial"})
}

type terminalRetrySpyStore struct {
	store.VanadisStore

	updatedStatus string
}

func (s *terminalRetrySpyStore) UpdateBuildStatus(_ context.Context, _ string, status string) error {
	s.updatedStatus = status
	return nil
}

func (*terminalRetrySpyStore) CompareAndSwapBuildStatusMessageID(context.Context, string, string, string) (bool, error) {
	return true, nil
}

func TestDeploymentMutationPersistence(t *testing.T) {
	t.Parallel()
	readFailure := `ALTER TABLE vanadis_builds RENAME COLUMN tier TO tier_x;`
	failUpdate := failStatement("vanadis_chunks", "UPDATE")
	tests := []struct {
		name, buildStatus, chunkStatus string
		mode                           string
		prNumber                       int
		sabotageSQL                    string
		nilEvidence                    bool
		wantErr                        bool
		// wantChunkStatus is the stored chunk status afterwards ("" when the
		// sabotage makes the build unreadable).
		wantChunkStatus string
		// wantJobs is the stored rollout evidence (nil skips the check).
		wantJobs []string
	}{
		{"failure prevents reconciliation", "running", "deploying", "pr", 0, failUpdate, false, true, "deploying", nil},
		{"success completes before reconciliation", "running", "deploying", "pr", 0, "", false, false, "completed", []string{"service"}},
		{"completed metadata under failed build", "failed", "completed", "pr", 0, "", false, false, "completed", []string{"service"}},
		{"cancelled build guarded", "cancelled", "deploying", "pr", 0, failUpdate, false, false, "deploying", nil},
		{"cancelled chunk guarded", "running", "cancelled", "pr", 0, failUpdate, false, false, "cancelled", nil},
		{"failed chunk guarded", "running", "failed", "pr", 0, failUpdate, false, false, "failed", nil},
		{"read error prevents reconciliation", "running", "deploying", "pr", 0, readFailure, false, true, "", nil},
		{"action compatibility", "running", "running", "action", 0, "", false, false, "completed", nil},
		{"action persistence failure", "running", "running", "action", 0, failUpdate, false, true, "running", nil},
		{"attached action evidence", "running", "deploying", "action", 42, "", false, false, "running", []string{"service"}},
		{"attached action nil evidence", "running", "deploying", "action", 42, "", true, false, "running", []string{"existing"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			repo := "owner/repo"
			if tc.mode == "action" && tc.prNumber == 0 {
				repo = ""
			}
			chunk := model.VanadisChunk{ID: "c1", Mode: tc.mode, Repo: repo, Status: tc.chunkStatus, PRNumber: tc.prNumber, RolloutState: &pubsub.PRRolloutState{ExpectedJobs: []string{"existing"}}}
			st, pool := seededStore(t, []model.VanadisBuild{{ID: "b1", Status: tc.buildStatus}}, lifecycleChunks("b1", chunk, model.VanadisChunk{ID: "sibling", Mode: "pr", Status: "completed"}))
			mustExec(t, pool, tc.sabotageSQL)
			bo := NewBuildObject("", nil).WithAuthority(st, executor.NewExecutor("", &mockEriClient{}, pubsub.NewMemoryBus(), "token", "example-owner", nil))
			evidence := &pubsub.PRRolloutState{ExpectedJobs: []string{"service"}}
			if tc.nilEvidence {
				evidence = nil
			}
			out, err := bo.executeAuthoritativeTransition(context.Background(), "b1", &BuildMutationRequest{Action: "chunk_deploy_success", ChunkID: "c1", RolloutState: evidence}, nil, nil, false)

			if tc.wantErr {
				assert.Error(t, err)
				assert.Empty(t, out.LaunchedChunks)
				assert.Nil(t, out.State)
			} else {
				assert.NoError(t, err)
			}
			if tc.wantChunkStatus == "" {
				return
			}
			stored := lifecycleChunk(t, st, "b1", "c1")
			assert.Equal(t, tc.wantChunkStatus, stored.Status)
			if tc.wantJobs != nil {
				assert.Equal(t, &pubsub.PRRolloutState{ExpectedJobs: tc.wantJobs}, stored.RolloutState)
			}
		})
	}
}

func TestDeploymentSuccessPREvent(t *testing.T) {
	t.Parallel()
	const lookupFailureSQL = `ALTER TABLE vanadis_chunks RENAME COLUMN pr_number TO pr_number_x;`
	tests := []struct {
		name              string
		fail              bool
		lookupFailure     bool
		event             string
		transitionFailure bool
	}{
		{"evidence persists", false, false, "deploy_success", false},
		{"transition error retries", false, false, "deploy_success", true},
		{"persistence error retries", true, false, "deploy_success", false},
		{"lookup error retries", false, true, "deploy_success", false},
		{"late rollout persistence error", true, false, "rollout_updated", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			chunk := model.VanadisChunk{ID: "c1", Mode: "pr", Repo: "owner/repo", PRNumber: 42, Status: "completed"}
			st, pool := seededStore(t, []model.VanadisBuild{{ID: "b1", Status: "failed"}}, lifecycleChunks("b1", chunk))
			if tc.fail {
				mustExec(t, pool, failStatement("vanadis_chunks", "UPDATE"))
			}
			if tc.lookupFailure {
				mustExec(t, pool, lookupFailureSQL)
			}
			bo := NewBuildObject("", nil).WithAuthority(st, nil)
			if tc.transitionFailure {
				bo.runtimeFactory = func(restate.ObjectContext) objectRuntime {
					rt := newFakeObjectRuntime("b1")
					rt.runStepErr = errors.New("reconcile unavailable")
					return rt
				}
			}
			err := bo.OnPREvent(makeObjectMockContext(t, "b1"), pubsub.PRLifecycleEvent{Event: tc.event, Repo: "owner/repo", PRNumber: 42, RolloutState: &pubsub.PRRolloutState{ExpectedJobs: []string{"service"}}})
			if tc.fail && tc.event == "deploy_success" || tc.lookupFailure || tc.transitionFailure {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				if tc.event == "deploy_success" {
					assert.Equal(t, &pubsub.PRRolloutState{ExpectedJobs: []string{"service"}}, lifecycleChunk(t, st, "b1", "c1").RolloutState)
				}
			}
		})
	}
}

type deploymentEventRecorder struct {
	pubsub.Bus

	events []any
}

func (bus *deploymentEventRecorder) Publish(_ context.Context, _, _ string, payload any) error {
	bus.events = append(bus.events, payload)
	return nil
}

func TestApplyMutation(t *testing.T) {
	t.Parallel()
	prChunk := func(status string) []model.VanadisChunk {
		return []model.VanadisChunk{{ID: "c1", Mode: "pr", Repo: "aerial", Status: status}}
	}
	nudged := func() []model.VanadisChunk {
		return []model.VanadisChunk{{ID: "c1", Mode: "action", Status: "running", StallNudges: 1}}
	}
	// action is the running action chunk FailChunk locks; generation is its stall_nudges.
	action := func(generation int) []model.VanadisChunk {
		return []model.VanadisChunk{{ID: "c1", Mode: "action", Status: "running", StallNudges: generation}}
	}
	const skipped, notFound = "action completion not applied", "vanadis chunk not found"
	const sabotaged = "sabotaged UPDATE on vanadis_chunks"
	tests := []struct {
		name, action, buildID, chunkID, dispatchID string
		sabotageSQL                                string
		chunks                                     []model.VanadisChunk
		wantErr                                    string
	}{
		{name: "cancelled during completion write", action: "chunk_deploy_success", buildID: "b1", chunkID: "c1", sabotageSQL: lifecycleSkipChunkUpdatesSQL, chunks: prChunk("deploying")},
		{name: "obsolete merge mutation", action: "chunk_merged", buildID: "b1", chunkID: "c1", chunks: prChunk("completed")},
		{name: "merge persistence error", action: "chunk_merged", buildID: "b1", chunkID: "c1", sabotageSQL: failStatement("vanadis_chunks", "UPDATE"), chunks: prChunk("deploying"), wantErr: sabotaged},
		{name: "deployment missing identity", action: "chunk_deploy_success", buildID: "b1", chunkID: " c1 ", chunks: nudged(), wantErr: skipped},
		{name: "deployment stale identity", action: "chunk_deploy_success", buildID: "b1", chunkID: " c1 ", dispatchID: "vanadis-c1-turn-0", chunks: nudged(), wantErr: skipped},
		{name: "completion missing identity", action: "action_completed", buildID: "b1", chunkID: " c1 ", chunks: nudged(), wantErr: skipped},
		{name: "completion stale identity", action: "action_completed", buildID: "b1", chunkID: " c1 ", dispatchID: "vanadis-c1-turn-0", chunks: nudged(), wantErr: skipped},
		{name: "wrong keyed build", action: "action_completed", buildID: "other", chunkID: " c1 ", dispatchID: "vanadis-c1-turn-1", wantErr: notFound},
		{name: "deployment wrong keyed build", action: "chunk_deploy_success", buildID: "other", chunkID: " c1 ", dispatchID: "vanadis-c1-turn-1", wantErr: notFound},
		{name: "failure missing generation", action: "chunk_failed", buildID: "b1", chunkID: "c1", chunks: action(1), wantErr: skipped},
		{name: "failure stale generation", action: "chunk_failed", buildID: "b1", chunkID: "c1", dispatchID: "vanadis-c1-turn-0", chunks: action(1), wantErr: skipped},
		{name: "failure current generation", action: "chunk_failed", buildID: "b1", chunkID: " c1 ", dispatchID: "vanadis-c1-turn-1", chunks: action(1)},
		{name: "failure persistence cancellation", action: "chunk_failed", buildID: "b1", chunkID: "c1", sabotageSQL: failStatement("vanadis_chunks", "UPDATE"), chunks: action(0), wantErr: sabotaged},
		{name: "failure wrong owner", action: "chunk_failed", buildID: "b1", chunkID: "other", chunks: action(0), wantErr: notFound},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := lifecycleStore(t, model.VanadisBuild{ID: tc.buildID, Status: "running"}, tc.sabotageSQL, tc.chunks...)
			bus := &deploymentEventRecorder{}
			bo := NewBuildObject("", nil).WithAuthority(st, executor.NewExecutor("", &mockEriClient{}, bus, "token", "example-owner", nil))
			err := bo.applyMutation(context.Background(), tc.buildID, &BuildMutationRequest{Action: tc.action, ChunkID: tc.chunkID, DispatchID: tc.dispatchID, Reason: "failure"})
			if tc.wantErr != "" {
				assert.ErrorContains(t, err, tc.wantErr)
			} else {
				assert.NoError(t, err)
			}
			assert.Empty(t, bus.events)
		})
	}
}

func TestDeploymentSuccessBeforeDelayedLifecycle(t *testing.T) {
	t.Parallel()
	tests := []struct{ name, delayedEvent string }{
		{"delayed merge", "merged"},
		{"delayed ci failure", "ci_failed"},
		{"delayed conflict", "conflict"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := lifecycleStore(t, model.VanadisBuild{ID: "b1", Status: "running"}, "",
				model.VanadisChunk{ID: "c1", Mode: "pr", Repo: "owner/repo", Status: "deploying", PRNumber: 42},
				model.VanadisChunk{ID: "c2", Mode: "pr", Repo: "owner/repo", Status: "deploying", PRNumber: 43},
				model.VanadisChunk{ID: "action", Mode: "action", Status: "pending", DependsOn: []string{"c1", "c2"}},
			)
			bus := &deploymentEventRecorder{}
			bo := NewBuildObject("", nil).WithAuthority(st, executor.NewExecutor("", &mockEriClient{}, bus, "token", "example-owner", nil))
			ctx := makeObjectMockContext(t, "b1")
			evidence := &pubsub.PRRolloutState{ExpectedJobs: []string{"service"}}
			assert.NoError(t, bo.OnPREvent(ctx, pubsub.PRLifecycleEvent{Event: "deploy_success", Repo: "owner/repo", PRNumber: 42, RolloutState: evidence}))
			assert.Equal(t, map[string]string{"c1": "completed", "c2": "deploying", "action": "pending"}, chunkStatuses(t, st, "b1"))
			events := append([]any(nil), bus.events...)
			assert.NoError(t, bo.OnPREvent(ctx, pubsub.PRLifecycleEvent{Event: tc.delayedEvent, Repo: "owner/repo", PRNumber: 42}))
			stored := lifecycleChunk(t, st, "b1", "c1")
			assert.Equal(t, "completed", stored.Status)
			assert.Equal(t, evidence, stored.RolloutState)
			assert.Equal(t, events, bus.events)
			out, err := bo.executeAuthoritativeTransition(context.Background(), "b1", &BuildMutationRequest{Action: "chunk_deploy_success", ChunkID: "c2"}, nil, nil, false)
			assert.NoError(t, err)
			assert.Contains(t, out.LaunchedChunks, "action")
			assert.Equal(t, map[string]string{"c1": "completed", "c2": "completed", "action": "running"}, chunkStatuses(t, st, "b1"))
		})
	}
}

func TestActionCompletionContinuation(t *testing.T) {
	t.Parallel()
	done := func(nudges int) model.VanadisChunk {
		return model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "completed", StallNudges: nudges}
	}
	next := model.VanadisChunk{ID: "next", BuildID: "b1", Mode: "pr", Status: "pending", Repo: "owner/repo", TargetFiles: []string{"main.go"}, DependsOn: []string{"c1"}}
	donePR := model.VanadisChunk{ID: "c2", BuildID: "b1", Mode: "pr", Status: "completed"}
	const turn1 = "vanadis-c1-turn-1"
	const completedPrompt = "[VANADIS BUILD COMPLETED]\nBuild ID: b1\nObjective: Ship it\n\nAll chunks completed successfully."
	// startStatus, when set, is the build status a reconcile-build transition
	// sees before the mutations run against buildStatus: a redelivered
	// completion after reconciliation already completed the build.
	tests := []struct {
		name, action, startStatus, buildStatus, chunkID string
		chunks                                          []model.VanadisChunk
		quiet                                           bool
		identities                                      []string
		wantEvents                                      int
		wantPrompts                                     []string
		wantDispatch                                    string
		wantWidget                                      bool
	}{
		{name: "mutation resumes after committed completion", action: "action_completed", buildStatus: "running", chunkID: "c1", chunks: cks(done(1), next), identities: []string{turn1}, wantDispatch: "next", wantWidget: true},
		{name: "final deployment delivers terminal event", action: "chunk_deploy_success", buildStatus: "completed", chunkID: "c1", chunks: cks(done(1)), identities: []string{turn1, "vanadis-c1-turn-0", turn1}, wantEvents: 1, wantPrompts: []string{completedPrompt}, wantWidget: true},
		{name: "final mutation delivers terminal event", action: "action_completed", buildStatus: "completed", chunkID: "c1", chunks: cks(done(1)), identities: []string{turn1, turn1}, wantEvents: 1, wantPrompts: []string{completedPrompt}, wantWidget: true},
		{name: "stale completed generation suppressed", action: "chunk_deploy_success", buildStatus: "completed", chunkID: "c1", chunks: cks(done(1)), identities: []string{"vanadis-c1-turn-0"}},
		{name: "quiet final deployment preserves event and widget", action: "chunk_deploy_success", buildStatus: "completed", chunkID: "c1", chunks: cks(done(1)), quiet: true, identities: []string{turn1}, wantEvents: 1, wantWidget: true},
		{name: "padded final action completion", action: "action_completed", buildStatus: "completed", chunkID: " c1 ", chunks: cks(done(1)), identities: []string{turn1}, wantEvents: 1, wantPrompts: []string{completedPrompt}, wantWidget: true},
		{name: "padded final deployment completion", action: "chunk_deploy_success", buildStatus: "completed", chunkID: " c1 ", chunks: cks(done(1)), identities: []string{turn1}, wantEvents: 1, wantPrompts: []string{completedPrompt}, wantWidget: true},
		{name: "mixed build completion redelivered after reconcile", action: "action_completed", startStatus: "running", buildStatus: "completed", chunkID: "c1", chunks: cks(done(0), donePR), identities: []string{""}, wantEvents: 1, wantPrompts: []string{completedPrompt}, wantWidget: true},
		{name: "quiet mixed build completion redelivered after reconcile", action: "action_completed", startStatus: "running", buildStatus: "completed", chunkID: "c1", chunks: cks(done(0), donePR), quiet: true, identities: []string{""}, wantEvents: 1, wantWidget: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := lifecycleStore(t, model.VanadisBuild{ID: "b1", Status: cmp.Or(tc.startStatus, tc.buildStatus), ThreadID: "thread", Objective: "Ship it", Quiet: tc.quiet}, "", tc.chunks...)
			bus := &deploymentEventRecorder{}
			var queuedPrompts []string
			eri := &mockEriClient{promptFn: func(_ context.Context, _, prompt string) error {
				queuedPrompts = append(queuedPrompts, prompt)
				return nil
			}}
			bo := NewBuildObject("", nil).WithAuthority(st, executor.NewExecutor("", eri, bus, "token", "example-owner", nil))
			rt := newFakeObjectRuntime("b1")
			if tc.startStatus != "" {
				_, err := bo.executeObjectTransition(rt, "reconcile-build", nil)
				require.NoError(t, err)
				// Reconciliation itself persisted the completion the mutations then observe.
				require.Equal(t, tc.buildStatus, lifecycleBuildStatus(t, st, "b1"))
			}
			for _, identity := range tc.identities {
				_, err := bo.executeObjectTransition(rt, "mutate-build", &BuildMutationRequest{Action: tc.action, ChunkID: tc.chunkID, DispatchID: identity})
				assert.NoError(t, err)
			}
			assert.Len(t, bus.events, tc.wantEvents)
			assert.Equal(t, tc.wantPrompts, queuedPrompts)
			assert.Equal(t, tc.wantWidget, eri.LastWidget != nil)
			if tc.wantDispatch == "" {
				assert.Nil(t, eri.LastDispatch)
				return
			}
			require.NotNil(t, eri.LastDispatch)
			assert.Equal(t, tc.wantDispatch, eri.LastDispatch.VanadisChunkID)
		})
	}
}

// noPRFailStore layers stateful chunk failure over another store so the
// failure transition observes its own write. Shared with pr_lifecycle_test.go.
type noPRFailStore struct {
	store.VanadisStore

	failedChunk, failReason string
	skip                    bool
}

func (s *noPRFailStore) FailChunk(_ context.Context, chunkID, reason, _ string) (*model.VanadisBuild, *model.VanadisChunk, bool, error) {
	if s.skip {
		return &model.VanadisBuild{ID: "b1", Status: statusRunning}, &model.VanadisChunk{ID: chunkID, Mode: "pr", Status: statusRunning}, false, nil
	}
	s.failedChunk, s.failReason = chunkID, reason
	return &model.VanadisBuild{ID: "b1", Status: statusRunning}, &model.VanadisChunk{ID: chunkID, Mode: "pr", Status: statusFailed}, true, nil
}

func (s *noPRFailStore) GetBuild(ctx context.Context, buildID string) (*model.VanadisBuild, []model.VanadisChunk, error) {
	bld, chunks, err := s.VanadisStore.GetBuild(ctx, buildID)
	if err != nil || s.failedChunk == "" {
		return bld, chunks, err
	}
	for i := range chunks {
		if chunks[i].ID == s.failedChunk {
			chunks[i].Status = statusFailed
		}
	}
	return bld, chunks, nil
}

func TestDispatchCompletionContinuation(t *testing.T) {
	t.Parallel()
	actionDone := model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "completed", StallNudges: 1}
	actionRunning := model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "running", StallNudges: 1, Title: "Chunk 1", Instructions: "Do step 1"}
	actionDeploying := model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "deploying", Repo: "aerial", PRNumber: 7, StallNudges: 1, Title: "Chunk 1", Instructions: "Do step 1"}
	prRunning := model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "pr", Status: "running", Repo: "aerial", Title: "Chunk 1", Instructions: "Do step 1"}
	prOpened := model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "pr", Status: "pr_opened", PRNumber: 7, Repo: "aerial", Title: "Chunk 1"}
	// prFailed is a PR chunk a timeout reaches after it already failed: FailChunk skips it.
	prFailed := model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "pr", Status: "failed", Repo: "aerial", Title: "Chunk 1"}
	const actionResume1 = "[VANADIS AUTOMATIC RESUME 1/12]\nYour previous turn for action chunk c1 ended without calling complete_chunk."
	// The failed wake lists each chunk by its persisted FailChunk banner.
	const buildFailedAction = "[VANADIS BUILD FAILED]\nBuild ID: b1\nObjective: Ship it\n\nBuild encountered failures.\nChunk 1 failed: RESUMABLE: action turn ended without complete_chunk after 12 automatic resumes [c1]"
	const buildFailedNoPR = "[VANADIS BUILD FAILED]\nBuild ID: b1\nObjective: Ship it\n\nBuild encountered failures.\nChunk 1 failed: chunk turn ended without opening a PR (after one automatic resume) [c1]"
	const buildFailedTimeout = "[VANADIS BUILD FAILED]\nBuild ID: b1\nObjective: Ship it\n\nBuild encountered failures.\nChunk 1 failed: dispatch outcome failed [c1]"
	const actionFailReason = "RESUMABLE: action turn ended without complete_chunk after 12 automatic resumes"
	tests := []struct {
		name, identity, chunkID, status string
		chunk                           model.VanadisChunk
		buildStatus                     string
		transitionFailure               bool
		dispatchErr, widgetErr          error
		wantErr                         string
		wantEvents                      int
		wantDispatchID                  string
		wantSession                     string
		wantPromptPrefix                string
		wantFailReason                  string
		wantPrompts                     []string
	}{
		{"completed action outcome never completes the chunk", "vanadis-c1-turn-1", "c1", "completed", actionDone, "completed", false, nil, nil, "", 0, "", "", "", "", nil},
		{"stale completed generation suppressed", "vanadis-c1-turn-0", "c1", "completed", actionDone, "completed", false, nil, nil, "", 0, "", "", "", "", nil},
		{"action launch turn without verdict resumes", "vanadis-c1-turn-1", "c1", "completed", actionRunning, "running", false, nil, nil, "", 0, "vanadis-c1-turn-1-resume-1", "vanadis-c1", actionResume1, "", nil},
		{"whitespace identity resumes to the last resume", "vanadis-c1-turn-1-resume-11", " c1 ", "completed", actionRunning, "running", false, nil, nil, "", 0, "vanadis-c1-turn-1-resume-12", "vanadis-c1", "[VANADIS AUTOMATIC RESUME 12/12]\n", "", nil},
		{"timeout action turn resumes like a verdictless turn", "vanadis-c1-turn-1-resume-3", "c1", "timeout", actionRunning, "running", false, nil, nil, "", 0, "vanadis-c1-turn-1-resume-4", "vanadis-c1", "[VANADIS AUTOMATIC RESUME 4/12]\n", "", nil},
		{"action in repair pr stage resumes", "vanadis-c1-turn-1", "c1", "completed", actionDeploying, "running", false, nil, nil, "", 0, "vanadis-c1-turn-1-resume-1", "vanadis-c1", actionResume1, "", nil},
		{"last action resume fails resumable and wakes thread", "vanadis-c1-turn-1-resume-12", "c1", "completed", actionRunning, "running", false, nil, nil, "", 2, "", "", "", actionFailReason, []string{buildFailedAction}},
		{"timeout on last action resume fails resumable", "vanadis-c1-turn-1-resume-12", "c1", "timeout", actionRunning, "running", false, nil, nil, "", 2, "", "", "", actionFailReason, []string{buildFailedAction}},
		{"other generation action turn ignored", "vanadis-c1-turn-0", "c1", "completed", actionRunning, "running", false, nil, nil, "", 0, "", "", "", "", nil},
		{"action resume on failed build resumes", "vanadis-c1-turn-1", "c1", "completed", actionRunning, "failed", false, nil, nil, "", 0, "vanadis-c1-turn-1-resume-1", "vanadis-c1", actionResume1, "", nil},
		{"action resume on cancelled build ignored", "vanadis-c1-turn-1", "c1", "completed", actionRunning, "cancelled", false, nil, nil, "", 0, "", "", "", "", nil},
		{"action resume dispatch failure stays retryable", "vanadis-c1-turn-1", "c1", "completed", actionRunning, "running", false, errors.New("eri unavailable"), nil, "eri unavailable", 0, "vanadis-c1-turn-1-resume-1", "vanadis-c1", actionResume1, "", nil},
		{"action resume fail transition error stays retryable", "vanadis-c1-turn-1-resume-12", "c1", "completed", actionRunning, "running", true, nil, nil, "continuation unavailable", 0, "", "", "", "", nil},
		{"pr launch turn without pr resumes once", "vanadis-c1-turn-0", "c1", "completed", prRunning, "running", false, nil, nil, "", 0, "vanadis-c1-nopr-resume-1", "vanadis-c1", "[VANADIS AUTOMATIC RESUME]\nYour previous turn for chunk c1 ended without opening a PR.", "", nil},
		{"pr resume turn without pr fails and wakes thread", "vanadis-c1-nopr-resume-1", "c1", "completed", prRunning, "running", false, nil, nil, "", 2, "", "", "", "chunk turn ended without opening a PR (after one automatic resume)", []string{buildFailedNoPR}},
		{"pr launch turn with attached pr ignored", "vanadis-c1-turn-0", "c1", "completed", prOpened, "running", false, nil, nil, "", 0, "", "", "", "", nil},
		{"pr resume dispatch failure stays retryable", "vanadis-c1-turn-0", "c1", "completed", prRunning, "running", false, errors.New("eri unavailable"), nil, "eri unavailable", 0, "vanadis-c1-nopr-resume-1", "vanadis-c1", "[VANADIS AUTOMATIC RESUME]\n", "", nil},
		{"pr resume widget failure still resumes", "vanadis-c1-turn-0", "c1", "completed", prRunning, "running", false, nil, errors.New("widget down"), "", 0, "vanadis-c1-nopr-resume-1", "vanadis-c1", "[VANADIS AUTOMATIC RESUME]\n", "", nil},
		{"pr no-pr failure transition error stays retryable", "vanadis-c1-nopr-resume-1", "c1", "completed", prRunning, "running", true, nil, nil, "continuation unavailable", 0, "", "", "", "", nil},
		{"pr timeout still fails the chunk", "vanadis-c1-turn-0", "c1", "timeout", prRunning, "running", false, nil, nil, "", 2, "", "", "", "dispatch outcome failed", []string{buildFailedTimeout}},
		{"skipped pr timeout never resumes", "vanadis-c1-turn-0", "c1", "timeout", prFailed, "running", false, nil, nil, "", 0, "", "", "", "", nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := lifecycleStore(t, model.VanadisBuild{ID: "b1", Status: tc.buildStatus, ThreadID: "thread", Objective: "Ship it"}, "", tc.chunk)
			bus := &deploymentEventRecorder{}
			var prompts []string
			eri := &mockEriClient{
				promptFn: func(_ context.Context, _, prompt string) error {
					prompts = append(prompts, prompt)
					return nil
				},
				dispatchFn: func(context.Context, eriwire.BrainDispatchRequest) error { return tc.dispatchErr },
				widgetFn:   func(context.Context, executor.StatusWidgetRequest) (string, error) { return "msg-1", tc.widgetErr },
			}
			bo := NewBuildObject("", nil).WithAuthority(st, executor.NewExecutor("", eri, bus, "token", "example-owner", nil))
			bo.runtimeFactory = func(restate.ObjectContext) objectRuntime {
				rt := newFakeObjectRuntime("b1")
				if tc.transitionFailure {
					rt.runStepErr = errors.New("continuation unavailable")
				}
				return rt
			}
			err := bo.OnDispatchOutcome(makeObjectMockContext(t, "b1"), pubsub.DispatchOutcomeEvent{VanadisChunkID: tc.chunkID, DispatchID: tc.identity, Status: tc.status})
			if tc.wantErr != "" {
				assert.ErrorContains(t, err, tc.wantErr)
			} else {
				assert.NoError(t, err)
			}
			assert.Len(t, bus.events, tc.wantEvents)
			assert.Equal(t, tc.wantPrompts, prompts)
			stored := lifecycleChunk(t, st, "b1", "c1")
			if tc.wantFailReason == "" {
				assert.Equal(t, tc.chunk.Status, stored.Status)
			} else {
				assert.Equal(t, "failed", stored.Status)
				assert.Equal(t, "Chunk 1 failed: "+tc.wantFailReason, stored.Banner)
			}
			if tc.wantDispatchID == "" {
				assert.Nil(t, eri.LastDispatch)
				return
			}
			require.NotNil(t, eri.LastDispatch)
			assert.Equal(t, tc.wantDispatchID, eri.LastDispatch.ID)
			assert.Equal(t, tc.wantSession, eri.LastDispatch.SessionID)
			assert.Equal(t, "thread", eri.LastDispatch.TargetID)
			assert.False(t, eri.LastDispatch.SkipIfActive)
			assert.True(t, strings.HasPrefix(eri.LastDispatch.Prompt, tc.wantPromptPrefix), eri.LastDispatch.Prompt)
			assert.Contains(t, eri.LastDispatch.Prompt, "Do step 1")
		})
	}
}

func TestBuildObject_AuthoritativeGenerationRecovery(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, status            string
		generation              int
		prior                   map[string]bool
		dispatchError           bool
		wantDispatch, wantError bool
		wantKey                 string
	}{
		{"legacy sibling key", "pending", 1, map[string]bool{"vanadis-c-turn-1": true}, false, true, false, "dispatch:vanadis-c-turn-1"},
		{"before reservation", "pending", 1, map[string]bool{"c": true}, false, true, false, "dispatch:vanadis-c-turn-1"},
		{"after reservation", "running", 1, map[string]bool{"c": true, "dispatch:vanadis-c-turn-0": true}, false, true, false, "dispatch:vanadis-c-turn-1"},
		{"acknowledged", "running", 1, map[string]bool{"dispatch:vanadis-c-turn-1": true}, false, false, false, "dispatch:vanadis-c-turn-1"},
		{"pending acknowledged", "pending", 1, map[string]bool{"dispatch:vanadis-c-turn-1": true}, false, false, false, "dispatch:vanadis-c-turn-1"},
		{"eri rejection", "running", 1, nil, true, true, true, ""},
		{"first run", "pending", 0, nil, false, true, false, "c"},
		{"legacy running", "running", 0, nil, false, true, false, "c"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := lifecycleStore(t, model.VanadisBuild{ID: "b", Status: "running", ThreadID: "thread"}, "", model.VanadisChunk{ID: "c", Mode: "action", Status: tc.status, StallNudges: tc.generation, Title: "action"})
			eri := &mockEriClient{}
			if tc.dispatchError {
				eri.dispatchFn = func(context.Context, eriwire.BrainDispatchRequest) error { return errors.New("rejected") }
			}
			bo := NewBuildObject("", nil).WithAuthority(st, executor.NewExecutor(t.TempDir(), eri, nil, "", "example-owner", nil))
			out, err := bo.executeAuthoritativeTransition(context.Background(), "b", nil, nil, tc.prior, false)
			assert.Equal(t, tc.wantError, err != nil)
			assert.Equal(t, tc.wantDispatch, eri.LastDispatch != nil)
			if tc.wantKey != "" {
				assert.True(t, out.LaunchedMap[tc.wantKey])
			}
			if eri.LastDispatch != nil {
				assert.Equal(t, fmt.Sprintf("vanadis-c-turn-%d", tc.generation), eri.LastDispatch.ID)
			}
		})
	}
}

func TestBuildObject_GenerationRecoveryOrdering(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name               string
		mode               string
		status             string
		nudges             int
		siblings           int
		dependencyStatus   string
		withPendingSibling bool
		wantDispatched     []string
	}{
		{"resume at capacity", "action", "running", 2, 5, "completed", true, []string{"vanadis-retry-turn-2"}},
		{"unlaunched running reserves slot", "pr", "running", 0, 5, "completed", true, []string{"vanadis-retry-turn-0"}},
		{"pending at capacity", "action", "pending", 2, 6, "completed", false, nil},
		{"dependency unfinished", "action", "running", 2, 0, "running", false, nil},
		{"dependency ready", "action", "pending", 2, 0, "completed", false, []string{"vanadis-retry-turn-2"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			chunks := []model.VanadisChunk{
				{ID: "retry", Mode: tc.mode, Repo: "aerial", Status: tc.status, StallNudges: tc.nudges, DependsOn: []string{"dependency"}},
				{ID: "dependency", Mode: "action", Status: tc.dependencyStatus},
			}
			for i := range tc.siblings {
				chunks = append(chunks, model.VanadisChunk{ID: fmt.Sprintf("sibling-%d", i), Mode: "pr", Repo: "aerial", Status: "running", PRNumber: i + 1})
			}
			if tc.withPendingSibling {
				chunks = append(chunks, model.VanadisChunk{ID: "pending-sibling", Mode: "pr", Repo: "aerial", Status: "pending"})
			}
			var dispatched []string
			eri := &mockEriClient{dispatchFn: func(_ context.Context, req eriwire.BrainDispatchRequest) error {
				dispatched = append(dispatched, req.ID)
				return nil
			}}
			bo := NewBuildObject("", nil).WithAuthority(&mockStoreForLaunch{}, executor.NewExecutor(t.TempDir(), eri, nil, "", "example-owner", nil))
			launched := map[string]bool{"dispatch:vanadis-retry-turn-1": true, "dependency": true}
			if tc.nudges > 0 {
				launched["retry"] = true
			}
			_, err := bo.launchEligibleChunks(context.Background(), model.VanadisBuild{ID: "b"}, chunks, launched)
			assert.NoError(t, err)
			assert.Equal(t, tc.wantDispatched, dispatched)
			if len(tc.wantDispatched) > 0 {
				// Repeated reconciliation after acceptance must leave the generation acknowledged
				// and keep the 6-chunk cap intact against pending siblings.
				dispatched = nil
				_, err = bo.launchEligibleChunks(context.Background(), model.VanadisBuild{ID: "b"}, chunks, launched)
				assert.NoError(t, err)
				assert.Empty(t, dispatched)
			}
		})
	}
}

type retryingObjectRuntime struct {
	*fakeObjectRuntime
}

func (r *retryingObjectRuntime) RunReconcileStep(name string, fn func(ctx context.Context) (reconcileStepOutput, error)) (reconcileStepOutput, error) {
	r.stepsRun = append(r.stepsRun, name)
	out, err := fn(context.Background())
	if err != nil {
		return fn(context.Background())
	}
	return out, nil
}

func TestBuildObject_NotifyTransitionError(t *testing.T) {
	t.Parallel()
	quiet := model.VanadisBuild{ID: "b-quiet", Quiet: true}
	active := model.VanadisBuild{ID: "b-active", Title: "Active Build", Status: "running"}
	prior := &model.BuildObjectState{Build: model.VanadisBuild{ID: "b-prior", Title: "Prior Build"}, Chunks: []model.VanadisChunk{{ID: "c-prior", Title: "Prior Chunk"}}}
	// build, when set, is the stored build (with chunks) the store snapshot reads;
	// nil means no store is configured.
	tests := []struct {
		name, buildID string
		build         *model.VanadisBuild
		chunks        []model.VanadisChunk
		prior         *model.BuildObjectState
		wantChunks    []string
	}{
		{name: "quiet build suppresses notification", buildID: "b-quiet", build: &quiet},
		{name: "store snapshot drives banner", buildID: "b-active", build: &active, chunks: []model.VanadisChunk{{ID: "c1", Title: "Chunk 1"}}, wantChunks: []string{"c1"}},
		{name: "prior state without store", buildID: "b-prior", prior: prior, wantChunks: []string{"c-prior"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var st store.VanadisStore
			if tc.build != nil {
				st = lifecycleStore(t, *tc.build, "", tc.chunks...)
			}
			eri := &mockEriClient{}
			bo := NewBuildObject("", nil).WithAuthority(st, executor.NewExecutor(t.TempDir(), eri, nil, "", "example-owner", nil))
			require.NoError(t, bo.notifyTransitionError(context.Background(), tc.buildID, "step", tc.prior, errors.New("trans err")))
			if tc.wantChunks == nil {
				assert.Nil(t, eri.LastWidget)
				return
			}
			require.NotNil(t, eri.LastWidget)
			assert.Equal(t, tc.buildID, eri.LastWidget.ID)
			assert.Equal(t, "⚠️ Reconcile backing off (step): trans err", eri.LastWidget.Banner)
			var chunks []string
			for _, c := range eri.LastWidget.Chunks {
				chunks = append(chunks, c.ID)
			}
			assert.Equal(t, tc.wantChunks, chunks)
		})
	}
}

// TestRunTransitionStep_InStepRetry pins the in-step failure path: a step
// that fails once inside RunReconcileStep records the reconcile error and
// emits the backoff banner before the retried step succeeds.
func TestRunTransitionStep_InStepRetry(t *testing.T) {
	t.Parallel()
	attempts := 0
	var banners []string
	eri := &mockEriClient{
		dispatchFn: func(context.Context, eriwire.BrainDispatchRequest) error {
			attempts++
			if attempts == 1 {
				return errors.New("transient dispatch failure")
			}
			return nil
		},
		widgetFn: func(_ context.Context, req executor.StatusWidgetRequest) (string, error) {
			banners = append(banners, req.Banner)
			return "msg-retry", nil
		},
	}
	active := model.VanadisBuild{ID: "b-active", Title: "Active Build", Status: "running"}
	st := lifecycleStore(t, active, "", model.VanadisChunk{ID: "c1", Mode: "action", Status: "pending", Title: "Chunk 1"})
	bo := NewBuildObject("", nil).WithAuthority(st, executor.NewExecutor(t.TempDir(), eri, nil, "", "example-owner", nil))
	rt := &retryingObjectRuntime{fakeObjectRuntime: newFakeObjectRuntime("b-active")}
	before := reconcileErrorsCount(t, "reconcile-retry-test")
	res, err := bo.executeObjectTransition(rt, "reconcile-retry-test", nil)
	require.NoError(t, err)
	assert.Equal(t, "running", res.Status)
	assert.Equal(t, before+1, reconcileErrorsCount(t, "reconcile-retry-test"))
	assert.Contains(t, banners, "⚠️ Reconcile backing off (reconcile-retry-test): transient dispatch failure")
	assert.Equal(t, map[string]string{"c1": "running"}, chunkStatuses(t, st, "b-active"))
}

func buildTestMetricsMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	return mux
}

func errorMetricValue(t *testing.T, metric, handler string) float64 {
	t.Helper()
	rec := httptest.NewRecorder()
	buildTestMetricsMux().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody))
	prefix := fmt.Sprintf(`%s{handler=%q} `, metric, handler)
	for line := range strings.SplitSeq(rec.Body.String(), "\n") {
		if valStr, ok := strings.CutPrefix(strings.TrimSpace(line), prefix); ok {
			val, err := strconv.ParseFloat(strings.TrimSpace(valStr), 64)
			require.NoError(t, err)
			return val
		}
	}
	return 0
}

func reconcileErrorsCount(t *testing.T, handler string) float64 {
	t.Helper()
	return errorMetricValue(t, "vanadis_restate_reconcile_errors_total", handler)
}

func clientErrorsCount(t *testing.T, handler string) float64 {
	t.Helper()
	return errorMetricValue(t, "vanadis_restate_client_errors_total", handler)
}

func TestReconcileErrorsMetric(t *testing.T) {
	t.Parallel()
	rec := httptest.NewRecorder()
	buildTestMetricsMux().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody))
	metricsBody := rec.Body.String()
	for _, label := range reconcileErrorHandlerLabels {
		assert.Contains(t, metricsBody, fmt.Sprintf(`vanadis_restate_reconcile_errors_total{handler=%q} `, label))
		assert.Contains(t, metricsBody, fmt.Sprintf(`vanadis_restate_client_errors_total{handler=%q} `, label))
	}

	tests := []struct {
		name, handler                string
		err                          error
		wantClientInc, wantServerInc float64
	}{
		{"nil error increments neither", "test-handler-nil", nil, 0, 0},
		{"store.ErrInvalidBuildInput routes to client errors", "test-handler-invalid", store.ErrInvalidBuildInput, 1, 0},
		{"wrapped store.ErrInvalidBuildInput routes to client errors", "test-handler-wrapped-invalid", fmt.Errorf("validation error: %w", store.ErrInvalidBuildInput), 1, 0},
		{"store.ErrBuildNotFound routes to client errors", "test-handler-build-not-found", store.ErrBuildNotFound, 1, 0},
		{"ErrChunkNotFound routes to client errors", "test-handler-chunk-not-found", ErrChunkNotFound, 1, 0},
		{"store.ErrBuildConflict routes to client errors", "test-handler-conflict", store.ErrBuildConflict, 1, 0},
		{"4xx restate.TerminalError routes to client errors", "test-handler-terminal-400", restate.ToTerminalError(errors.New("bad input"), restate.WithErrorCode(400)), 1, 0},
		{"404 restate.TerminalError routes to client errors", "test-handler-terminal-404", restate.ToTerminalError(errors.New("resource missing"), restate.WithErrorCode(404)), 1, 0},
		{"409 restate.TerminalError routes to client errors", "test-handler-terminal-409", restate.ToTerminalError(errors.New("state conflict"), restate.WithErrorCode(409)), 1, 0},
		{"499 restate.TerminalError routes to client errors", "test-handler-terminal-499", restate.ToTerminalError(errors.New("custom client error"), restate.WithErrorCode(499)), 1, 0},
		{"5xx restate.TerminalError routes to server errors", "test-handler-terminal-500", restate.ToTerminalError(errors.New("internal error"), restate.WithErrorCode(500)), 0, 1},
		{"uncoded restate.TerminalErrorf routes to server errors", "test-handler-terminal-uncoded", restate.TerminalErrorf("generic terminal error: %s", "boom"), 0, 1},
		{"transient error routes to server errors", "test-handler-transient", errors.New("transient database connection timeout"), 0, 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			beforeClient := clientErrorsCount(t, tc.handler)
			beforeServer := reconcileErrorsCount(t, tc.handler)

			RecordReconcileError(tc.handler, tc.err)

			assert.Equal(t, beforeClient+tc.wantClientInc, clientErrorsCount(t, tc.handler))
			assert.Equal(t, beforeServer+tc.wantServerInc, reconcileErrorsCount(t, tc.handler))
		})
	}
}

func TestExecuteLaunchChunk_HTTPGenerationAcceptance(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, mode, key, body       string
		nudges                      int
		code                        int
		wantError, wantAcknowledged bool
	}{
		{"pr accepted", "pr", "c", `{"status":"accepted","dispatch_id":"vanadis-c-turn-0"}`, 0, 202, false, true},
		{"initial duplicate", "action", "c", `{"status":"already_exists","dispatch_id":"vanadis-c-turn-0"}`, 0, 200, false, true},
		{"initial wrong dispatch id", "action", "c", `{"status":"accepted","dispatch_id":"wrong"}`, 0, 202, true, false},
		{"retry accepted", "action", "dispatch:vanadis-c-turn-1", `{"status":"accepted","dispatch_id":"vanadis-c-turn-1"}`, 1, 202, false, true},
		{"retry active", "action", "dispatch:vanadis-c-turn-1", `{"status":"skipped_active"}`, 1, 200, true, false},
		{"retry server failure", "action", "dispatch:vanadis-c-turn-1", `{}`, 1, 500, true, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.code)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()
			st := lifecycleStore(t, model.VanadisBuild{ID: "b", Status: "running"}, "", model.VanadisChunk{ID: "c", Mode: tc.mode, Status: "running", StallNudges: tc.nudges})
			bo := NewBuildObject("", nil).WithAuthority(st, executor.NewExecutor(t.TempDir(), executor.NewHTTPEriClient(srv.URL, srv.Client()), nil, "", "example-owner", nil))
			out, err := bo.executeAuthoritativeTransition(context.Background(), "b", nil, nil, nil, false)
			assert.Equal(t, tc.wantError, err != nil)
			assert.Equal(t, tc.wantAcknowledged, out.LaunchedMap[tc.key])
		})
	}
}

func TestBuildObject_HTTPRecoverySiblingProgress(t *testing.T) {
	t.Parallel()
	accepted := make(chan string, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/dispatch" {
			w.WriteHeader(http.StatusOK)
			return
		}
		var req eriwire.BrainDispatchRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		// Eri's active-session gate runs before its duplicate-ID check. Either
		// persisted generation therefore produces the same ambiguous response.
		if req.SessionID == "vanadis-retry" {
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "skipped_active", "dispatch_id": req.ID})
			return
		}
		accepted <- req.ID
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "accepted", "dispatch_id": req.ID})
	}))
	defer srv.Close()
	build := model.VanadisBuild{ID: "b", Status: "running"}
	chunks := []model.VanadisChunk{
		{ID: "retry", Mode: "action", Status: "running", StallNudges: 1},
		{ID: "sibling", Mode: "action", Status: "pending"},
		{ID: "dependent", Mode: "action", Status: "pending", DependsOn: []string{"retry"}},
	}
	bo := NewBuildObject("", nil).WithAuthority(&mockStoreForLaunch{}, executor.NewExecutor(t.TempDir(), executor.NewHTTPEriClient(srv.URL, srv.Client()), nil, "", "example-owner", nil))
	launched := map[string]bool{}
	_, err := bo.launchEligibleChunks(context.Background(), build, chunks, launched)
	assert.ErrorIs(t, err, eriwire.ErrDispatchActive)
	assert.False(t, launched["dispatch:vanadis-retry-turn-1"])
	assert.True(t, launched["sibling"])
	select {
	case id := <-accepted:
		assert.Equal(t, "vanadis-sibling-turn-0", id)
	default:
		t.Error("independent sibling was not accepted")
	}
	assert.False(t, launched["dependent"])
}

// lifecycleSkipChunkUpdatesSQL makes every chunk UPDATE match no row, the
// shape a write takes when a concurrent transition already moved the chunk.
const lifecycleSkipChunkUpdatesSQL = `CREATE FUNCTION lifecycle_skip_row() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
	RETURN NULL;
END $$;
CREATE TRIGGER lifecycle_skip_chunk_updates BEFORE UPDATE ON vanadis_chunks FOR EACH ROW EXECUTE FUNCTION lifecycle_skip_row();`

// lifecycleChunks assigns chunks to buildID in slice order.
func lifecycleChunks(buildID string, chunks ...model.VanadisChunk) []model.VanadisChunk {
	out := make([]model.VanadisChunk, len(chunks))
	for i, c := range chunks {
		c.BuildID, c.ChunkIndex = buildID, i
		out[i] = c
	}
	return out
}

// lifecycleStore seeds build plus its chunks (in slice order) into a private
// database, then applies sabotageSQL.
func lifecycleStore(t *testing.T, build model.VanadisBuild, sabotageSQL string, chunks ...model.VanadisChunk) *store.Store {
	t.Helper()
	st, pool := seededStore(t, []model.VanadisBuild{build}, lifecycleChunks(build.ID, chunks...))
	mustExec(t, pool, sabotageSQL)
	return st
}

// lifecycleChunk reads one stored chunk of buildID.
func lifecycleChunk(t *testing.T, st *store.Store, buildID, chunkID string) model.VanadisChunk {
	t.Helper()
	_, chunks := loadBuild(t, st, buildID)
	for _, c := range chunks {
		if c.ID == chunkID {
			return c
		}
	}
	t.Fatalf("chunk %s not stored in build %s", chunkID, buildID)
	return model.VanadisChunk{}
}

// lifecycleBuildStatus reads the stored status of build id.
func lifecycleBuildStatus(t *testing.T, st *store.Store, id string) string {
	t.Helper()
	b, _ := loadBuild(t, st, id)
	return b.Status
}
