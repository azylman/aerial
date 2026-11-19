package build

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/x/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/pubsub"
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
	storeTerm, _ := dbScript{buildRow: &mockRow{b: &bTerm}}.open()
	boTerm := NewBuildObject("", nil).WithAuthority(storeTerm, nil)
	ctxTerm := makeObjectMockContext(t, "b-term")
	_, err := boTerm.Cancel(ctxTerm, CancelBuildRequest{Reason: "test"})
	if err == nil {
		t.Fatal("expected error canceling terminal build")
	}

	// 2. Cancel with store update build status error
	storeCancelErr, _ := dbScript{buildRow: &mockRow{b: &b1}, execErr: errors.New("update status fail")}.open()
	boCancelErr := NewBuildObject("", nil).WithAuthority(storeCancelErr, nil)
	ctxCancel := makeObjectMockContext(t, "b-more")
	_, _ = boCancelErr.Cancel(ctxCancel, CancelBuildRequest{Reason: "test"})

	// 3. UpdateChunk error in store
	storeUpdErr, _ := dbScript{bare: nilDB}.open()
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
	execErr := executor.NewExecutor(t.TempDir(), eriErr, &mockBusError{err: errors.New("pub err")}, "token", nil)
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
	retF, errF := boTermErr.evaluateBuildTerminalStatus(context.Background(), &bF, []model.VanadisChunk{cF})
	assert.Error(t, errF)
	assert.Empty(t, retF)

	bD := model.VanadisBuild{ID: "b-done", Status: "running", ThreadID: "th-1"}
	cD := model.VanadisChunk{ID: "c1", Status: "completed"}
	retD, errD := boTermErr.evaluateBuildTerminalStatus(context.Background(), &bD, []model.VanadisChunk{cD})
	assert.Error(t, errD)
	assert.Empty(t, retD)

	// 9b. evaluateBuildTerminalStatus failing on UpdateBuildStatus (storeCancelErr with nil executor)
	boStoreUpdateErr := NewBuildObject("", nil).WithAuthority(storeCancelErr, nil)
	bFStore := model.VanadisBuild{ID: "b-fail", Status: "running", ThreadID: "th-1"}
	retFStore, errFStore := boStoreUpdateErr.evaluateBuildTerminalStatus(context.Background(), &bFStore, []model.VanadisChunk{cF})
	assert.Error(t, errFStore)
	assert.Empty(t, retFStore)

	bDStore := model.VanadisBuild{ID: "b-done", Status: "running", ThreadID: "th-1"}
	retDStore, errDStore := boStoreUpdateErr.evaluateBuildTerminalStatus(context.Background(), &bDStore, []model.VanadisChunk{cD})
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
	execFlaky := executor.NewExecutor(t.TempDir(), flakyEri, pubsub.NewMemoryBus(), "token", nil)
	spyStore := &terminalRetrySpyStore{}
	boRetry := NewBuildObject("", nil).WithAuthority(spyStore, execFlaky)

	bRetry := model.VanadisBuild{ID: "b-retry", Status: "running", StatusMessageID: "msg-123", ThreadID: "th-1"}
	// Attempt 1: notifyBuildTerminal fails
	retRetry1, errRetry1 := boRetry.evaluateBuildTerminalStatus(context.Background(), &bRetry, []model.VanadisChunk{cD})
	assert.Error(t, errRetry1)
	assert.Empty(t, retRetry1)
	assert.Empty(t, spyStore.updatedStatus)

	// Attempt 2: retry succeeds
	retRetry2, errRetry2 := boRetry.evaluateBuildTerminalStatus(context.Background(), &bRetry, []model.VanadisChunk{cD})
	assert.NoError(t, errRetry2)
	assert.Equal(t, statusCompleted, retRetry2)
	assert.Equal(t, statusCompleted, spyStore.updatedStatus)

	// 9d. evaluateBuildTerminalStatus default branch widget sync warning
	bRunning := model.VanadisBuild{ID: "b-run", Status: "running", ThreadID: "th-1"}
	cRunning := model.VanadisChunk{ID: "c1", Status: "running"}
	retRun, errRun := boTermErr.evaluateBuildTerminalStatus(context.Background(), &bRunning, []model.VanadisChunk{cRunning})
	assert.NoError(t, errRun)
	assert.Equal(t, statusRunning, retRun)

	// 9e. notifyBuildTerminal nil executor returns nil
	boNilExec := NewBuildObject("", nil).WithAuthority(storeCancelErr, nil)
	assert.NoError(t, boNilExec.notifyBuildTerminal(context.Background(), &bD, []model.VanadisChunk{cD}, statusCompleted, "", "H", "M"))

	// 9f. executeAuthoritativeTransition fails when notifyBuildTerminal fails during continuation
	storeCompCont, _ := dbScript{buildRow: &mockRow{b: &model.VanadisBuild{ID: "b-cont", Status: statusCompleted}}, chunks: []model.VanadisChunk{cD}}.open()
	boContErr := NewBuildObject("", nil).WithAuthority(storeCompCont, execErr)
	_, errCont := boContErr.executeAuthoritativeTransition(context.Background(), "b-cont", &BuildMutationRequest{Action: "action_completed", ChunkID: "c1", DispatchID: "vanadis-c1-turn-0"}, nil, nil, false)
	assert.Error(t, errCont)

	// 9g. executeAuthoritativeTransition fails when evaluateBuildTerminalStatus fails
	bFailRunning := model.VanadisBuild{ID: "b-fail-trans", Status: statusRunning, ThreadID: "th-1"}
	cFailChunk := model.VanadisChunk{ID: "c1", BuildID: "b-fail-trans", Status: statusFailed}
	storeTransErr, _ := dbScript{buildRow: &mockRow{b: &bFailRunning}, chunks: []model.VanadisChunk{cFailChunk}, execTag: "UPDATE 1"}.open()
	boTransErr := NewBuildObject("", nil).WithAuthority(storeTransErr, execErr)
	_, errTrans := boTransErr.executeAuthoritativeTransition(context.Background(), "b-fail-trans", nil, nil, nil, false)
	assert.Error(t, errTrans)

	// 10. UpdateBuild with widget sync error
	storeUpdOk, _ := dbScript{
		buildRow: &mockRow{b: &b1},
		chunks:   []model.VanadisChunk{c1},
		execTag:  "UPDATE 1",
	}.open()
	boUpdWidgetErr := NewBuildObject("", nil).WithAuthority(storeUpdOk, execErr)
	_, _ = boUpdWidgetErr.UpdateBuild(ctxCancel, store.UpdateBuildRequest{Title: "new title"})

	// 11. persistBuildCreation with widget sync error
	_, _ = restate.Run(ctxCancel, func(rc restate.RunContext) (bool, error) {
		return boUpdWidgetErr.persistBuildCreation(rc, b1, []model.VanadisChunk{c1})
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

func (*terminalRetrySpyStore) UpdateBuildStatusMessageID(_ context.Context, _, _ string) error {
	return nil
}

func TestDeploymentMutationPersistence(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, buildStatus, chunkStatus string
		fail                           bool
		wantErr                        bool
		mode                           string
		readFailure                    bool
		prNumber                       int
		nilEvidence                    bool
	}{
		{"failure prevents reconciliation", "running", "deploying", true, true, "pr", false, 0, false},
		{"success completes before reconciliation", "running", "deploying", false, false, "pr", false, 0, false},
		{"completed metadata under failed build", "failed", "completed", false, false, "pr", false, 0, false},
		{"cancelled build guarded", "cancelled", "deploying", true, false, "pr", false, 0, false},
		{"cancelled chunk guarded", "running", "cancelled", true, false, "pr", false, 0, false},
		{"failed chunk guarded", "running", "failed", true, false, "pr", false, 0, false},
		{"read error prevents reconciliation", "running", "deploying", false, true, "pr", true, 0, false},
		{"action compatibility", "running", "running", false, false, "action", false, 0, false},
		{"action persistence failure", "running", "running", true, true, "action", false, 0, false},
		{"attached action evidence", "running", "deploying", false, false, "action", false, 42, false},
		{"attached action nil evidence", "running", "deploying", false, false, "action", false, 42, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			build := model.VanadisBuild{ID: "b1", Status: tc.buildStatus}
			chunk := model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: tc.mode, Repo: "owner/repo", Status: tc.chunkStatus, PRNumber: tc.prNumber, RolloutState: &pubsub.PRRolloutState{ExpectedJobs: []string{"existing"}}}
			script := dbScript{row: &mockRow{b: &build}, countRow: valuesRow(1), chunks: []model.VanadisChunk{chunk, {ID: "sibling", BuildID: "b1", Mode: "pr", Status: "completed"}}, execTag: "UPDATE 1"}
			if tc.readFailure {
				script.row = errRow(errors.New("read unavailable"))
			}
			if tc.fail {
				script.execErr = errors.New("persist failed")
				script.failExec = "UPDATE vanadis_chunks"
			}
			raw, _ := script.openRaw()
			observed := &deploymentEvidenceDB{PgxQueryExecer: raw, chunks: script.chunks}
			st := store.New(observed)
			bo := NewBuildObject("", nil).WithAuthority(st, executor.NewExecutor("", &mockEriClient{}, pubsub.NewMemoryBus(), "token", nil))
			evidence := &pubsub.PRRolloutState{ExpectedJobs: []string{"service"}}
			if tc.nilEvidence {
				evidence = nil
			}
			out, err := bo.executeAuthoritativeTransition(context.Background(), "b1", &BuildMutationRequest{Action: "chunk_deploy_success", ChunkID: "c1", RolloutState: evidence}, nil, nil, false)

			if tc.wantErr {
				assert.Error(t, err)
				assert.Empty(t, out.LaunchedChunks)
				assert.Nil(t, out.State)
				return
			}
			assert.NoError(t, err)
			if tc.buildStatus == "cancelled" || tc.chunkStatus == "cancelled" || tc.chunkStatus == "failed" {
				return
			}
			assert.Equal(t, "completed", observed.observed.Status)
			if tc.mode != "pr" && tc.prNumber == 0 {
				return
			}
			wantJobs := []string{"service"}
			if tc.nilEvidence {
				wantJobs = []string{"existing"}
			}
			assert.Equal(t, &pubsub.PRRolloutState{ExpectedJobs: wantJobs}, observed.observed.RolloutState)
		})
	}
}

func TestDeploymentSuccessPREvent(t *testing.T) {
	t.Parallel()
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
			build := model.VanadisBuild{ID: "b1", Status: "failed"}
			chunk := model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "pr", Status: "completed"}
			script := dbScript{
				buildRow: &mockRow{b: &build}, chunks: []model.VanadisChunk{chunk},
				routes:  []sqlRoute{{match: "c.repo = ANY($1) AND c.pr_number = $2", row: valuesRow("c1", "b1", 0, "medium", true)}},
				execTag: "UPDATE 1",
			}
			if tc.fail {
				script.execErr = errors.New("persistence unavailable")
				script.failExec = "SET status = 'completed'"
			}
			if tc.lookupFailure {
				script.routes[0].row = errRow(errors.New("lookup unavailable"))
			}
			if tc.event == "rollout_updated" {
				script.failExec = "UPDATE vanadis_chunks"
			}
			raw, _ := script.openRaw()
			observed := &deploymentEvidenceDB{PgxQueryExecer: raw, chunks: script.chunks}
			st := store.New(observed)
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
					assert.Equal(t, &pubsub.PRRolloutState{ExpectedJobs: []string{"service"}}, observed.observed.RolloutState)
				}
			}
		})
	}
}

// deploymentEvidenceDB models only the external database write boundary. Reads
// retain snapshots so the tests can verify what reconciliation actually observed.
type deploymentEvidenceDB struct {
	store.PgxQueryExecer

	chunks   []model.VanadisChunk
	observed model.VanadisChunk
}

func (db *deploymentEvidenceDB) Exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error) {
	var chunk *model.VanadisChunk
	for i := range db.chunks {
		id, _ := args[len(args)-1].(string)
		if id == db.chunks[i].ID {
			chunk = &db.chunks[i]
			break
		}
	}
	if chunk != nil && (strings.Contains(query, "AND status NOT IN") || strings.Contains(query, "AND c.status NOT IN ('completed'")) && (chunk.Status == "completed" || chunk.Status == "failed" || chunk.Status == "cancelled") {
		return pgconn.NewCommandTag("UPDATE 0"), nil
	}
	tag, err := db.PgxQueryExecer.Exec(ctx, query, args...)
	if err != nil || tag.RowsAffected() == 0 || chunk == nil {
		return tag, err
	}
	if strings.Contains(query, "rollout_state = COALESCE") {
		if data, ok := args[0].([]byte); ok {
			var evidence pubsub.PRRolloutState
			if err := json.Unmarshal(data, &evidence); err != nil {
				return tag, err
			}
			chunk.RolloutState = &evidence
		}
		chunk.Status = "completed"
	}
	if strings.Contains(query, "SET status = 'completed'") {
		chunk.Status = "completed"
	} else if !strings.Contains(query, "rollout_state = COALESCE") {
		if status, ok := args[0].(string); ok {
			chunk.Status = status
		}
	}
	// Snapshot on write as well as on read: store reads increasingly bypass
	// this wrapper as methods promote to the embedded store, so a trailing
	// read snapshot is no longer guaranteed.
	db.observed = *chunk
	return tag, nil
}

func (db *deploymentEvidenceDB) Query(ctx context.Context, query string, args ...any) (pgx.Rows, error) {
	db.observed = db.chunks[0]
	return db.PgxQueryExecer.Query(ctx, query, args...)
}

type deploymentEventRecorder struct {
	pubsub.Bus

	events []any
}

func (bus *deploymentEventRecorder) Publish(_ context.Context, _, _ string, payload any) error {
	bus.events = append(bus.events, payload)
	return nil
}

func TestRejectedDeploymentMutation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, status, action string
		fail                 bool
	}{
		{"cancelled during completion write", "deploying", "chunk_deploy_success", false},
		{"obsolete merge mutation", "completed", "chunk_merged", false},
		{"merge persistence error", "deploying", "chunk_merged", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			build := model.VanadisBuild{ID: "b1", Status: "running"}
			chunk := model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "pr", Status: tc.status}
			script := dbScript{row: &mockRow{b: &build}, chunks: []model.VanadisChunk{chunk}, execTag: "UPDATE 0"}
			if tc.fail {
				script.execErr = errors.New("database unavailable")
				script.failExec = "UPDATE vanadis_chunks"
			}
			st, _ := script.open()
			bus := &deploymentEventRecorder{}
			bo := NewBuildObject("", nil).WithAuthority(st, executor.NewExecutor("", &mockEriClient{}, bus, "token", nil))
			err := bo.applyMutation(context.Background(), "b1", &BuildMutationRequest{Action: tc.action, ChunkID: "c1"})
			if tc.fail {
				assert.ErrorContains(t, err, "database unavailable")
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
			build := model.VanadisBuild{ID: "b1", Status: "running"}
			chunks := []model.VanadisChunk{
				{ID: "c1", BuildID: "b1", Mode: "pr", Status: "deploying", PRNumber: 42},
				{ID: "c2", BuildID: "b1", Mode: "pr", Status: "deploying", PRNumber: 43},
				{ID: "action", BuildID: "b1", Mode: "action", Status: "pending", DependsOn: []string{"c1", "c2"}},
			}
			script := dbScript{
				buildRow: &mockRow{b: &build}, chunks: chunks, execTag: "UPDATE 1",
				routes: []sqlRoute{{match: "c.repo = ANY($1)", row: valuesRow("c1", "b1", 0, "medium", true)}},
			}
			raw, _ := script.openRaw()
			db := &deploymentEvidenceDB{PgxQueryExecer: raw, chunks: chunks}
			st := store.New(db)
			bus := &deploymentEventRecorder{}
			bo := NewBuildObject("", nil).WithAuthority(st, executor.NewExecutor("", &mockEriClient{}, bus, "token", nil))
			ctx := makeObjectMockContext(t, "b1")
			evidence := &pubsub.PRRolloutState{ExpectedJobs: []string{"service"}}
			assert.NoError(t, bo.OnPREvent(ctx, pubsub.PRLifecycleEvent{Event: "deploy_success", Repo: "owner/repo", PRNumber: 42, RolloutState: evidence}))
			assert.Equal(t, "pending", chunks[2].Status)
			events := append([]any(nil), bus.events...)
			assert.NoError(t, bo.OnPREvent(ctx, pubsub.PRLifecycleEvent{Event: tc.delayedEvent, Repo: "owner/repo", PRNumber: 42}))
			assert.Equal(t, "completed", chunks[0].Status)
			assert.Equal(t, evidence, chunks[0].RolloutState)
			assert.Equal(t, events, bus.events)
			out, err := bo.executeAuthoritativeTransition(context.Background(), "b1", &BuildMutationRequest{Action: "chunk_deploy_success", ChunkID: "c2"}, nil, nil, false)
			assert.NoError(t, err)
			assert.Contains(t, out.LaunchedChunks, "action")
		})
	}
}

type deploymentEvidenceTx struct {
	pgx.Tx

	db *deploymentEvidenceDB
}

func (db *deploymentEvidenceDB) Begin(ctx context.Context) (pgx.Tx, error) {
	tx, err := db.PgxQueryExecer.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &deploymentEvidenceTx{Tx: tx, db: db}, nil
}

func (tx *deploymentEvidenceTx) Exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error) {
	return tx.db.Exec(ctx, query, args...)
}

func TestActionCompletionMutationIdentity(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, action, buildID, dispatchID, status string
		wantErr                                   error
	}{
		{"deployment missing identity", "chunk_deploy_success", "b1", "", "running", errActionCompletionSkipped},
		{"deployment stale identity", "chunk_deploy_success", "b1", "vanadis-c1-turn-0", "running", errActionCompletionSkipped},
		{"completion missing identity", "action_completed", "b1", "", "running", errActionCompletionSkipped},
		{"completion stale identity", "action_completed", "b1", "vanadis-c1-turn-0", "running", errActionCompletionSkipped},
		{"wrong keyed build", "action_completed", "other", "vanadis-c1-turn-1", "running", ErrChunkNotFound},
		{"deployment wrong keyed build", "chunk_deploy_success", "other", "vanadis-c1-turn-1", "running", ErrChunkNotFound},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			build := model.VanadisBuild{ID: tc.buildID, Status: "running"}
			chunk := model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: tc.status, StallNudges: 1}
			chunks := []model.VanadisChunk{chunk}
			if tc.buildID != "b1" {
				chunks = nil
			}
			st, _ := (dbScript{row: &mockRow{b: &build}, chunks: chunks, countRow: valuesRow(1)}).open()
			bus := &deploymentEventRecorder{}
			bo := NewBuildObject("", nil).WithAuthority(st, executor.NewExecutor("", &mockEriClient{}, bus, "token", nil))
			err := bo.applyMutation(context.Background(), tc.buildID, &BuildMutationRequest{Action: tc.action, ChunkID: " c1 ", DispatchID: tc.dispatchID})
			assert.ErrorIs(t, err, tc.wantErr)
			assert.Empty(t, bus.events)
		})
	}
}

func TestActionCompletionContinuation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, action, buildStatus, identity, chunkID string
		wantEvents                                   int
		quiet                                        bool
		identities                                   []string
	}{
		{"mutation resumes after committed completion", "action_completed", "running", "vanadis-c1-turn-1", "c1", 0, false, nil},
		{"final deployment delivers terminal event", "chunk_deploy_success", "completed", "vanadis-c1-turn-1", "c1", 1, false, []string{"vanadis-c1-turn-1", "vanadis-c1-turn-0", "vanadis-c1-turn-1"}},
		{"final mutation delivers terminal event", "action_completed", "completed", "vanadis-c1-turn-1", "c1", 1, false, []string{"vanadis-c1-turn-1", "vanadis-c1-turn-1"}},
		{"stale completed generation suppressed", "chunk_deploy_success", "completed", "vanadis-c1-turn-0", "c1", 0, false, nil},
		{"quiet final deployment preserves event and widget", "chunk_deploy_success", "completed", "vanadis-c1-turn-1", "c1", 1, true, nil},
		{"padded final action completion", "action_completed", "completed", "vanadis-c1-turn-1", " c1 ", 1, false, nil},
		{"padded final deployment completion", "chunk_deploy_success", "completed", "vanadis-c1-turn-1", " c1 ", 1, false, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			build := model.VanadisBuild{ID: "b1", Status: tc.buildStatus, ThreadID: "thread", Quiet: tc.quiet}
			chunks := []model.VanadisChunk{{ID: "c1", BuildID: "b1", Mode: "action", Status: "completed", StallNudges: 1}}
			if tc.buildStatus == "running" {
				chunks = append(chunks, model.VanadisChunk{ID: "next", BuildID: "b1", Mode: "pr", Status: "pending", Repo: "owner/repo", TargetFiles: []string{"main.go"}, DependsOn: []string{"c1"}})
			}
			st, _ := (dbScript{row: &mockRow{b: &build}, chunks: chunks}).open()
			bus := &deploymentEventRecorder{}
			var queuedPrompts []string
			eri := &mockEriClient{promptFn: func(_ context.Context, _, prompt string) error {
				queuedPrompts = append(queuedPrompts, prompt)
				return nil
			}}
			bo := NewBuildObject("", nil).WithAuthority(st, executor.NewExecutor("", eri, bus, "token", nil))
			rt := newFakeObjectRuntime("b1")
			identities := tc.identities
			if len(identities) == 0 {
				identities = []string{tc.identity}
			}
			for _, identity := range identities {
				_, err := bo.executeObjectTransition(rt, "mutate-build", &BuildMutationRequest{Action: tc.action, ChunkID: tc.chunkID, DispatchID: identity})
				assert.NoError(t, err)
			}
			assert.Len(t, bus.events, tc.wantEvents)
			if tc.wantEvents > 0 && !tc.quiet {
				assert.Equal(t, []string{"[VANADIS BUILD COMPLETED]\nBuild ID: b1\nObjective: \n\nAll chunks completed successfully."}, queuedPrompts)
			} else {
				assert.Empty(t, queuedPrompts)
			}
			if tc.buildStatus == "running" {
				assert.NotNil(t, eri.lastDispatch)
				if eri.lastDispatch != nil {
					assert.Equal(t, "next", eri.lastDispatch.VanadisChunkID)
				}
			} else if tc.wantEvents > 0 {
				assert.NotNil(t, eri.lastWidget)
			}
		})
	}
}

// noPRFailStore layers stateful chunk failure over the scripted store so the
// failure transition observes its own write.
type noPRFailStore struct {
	store.VanadisStore

	failedChunk, failReason string
}

func (s *noPRFailStore) FailChunk(_ context.Context, chunkID, reason, _ string) (*model.VanadisBuild, *model.VanadisChunk, bool, error) {
	s.failedChunk, s.failReason = chunkID, reason
	return &model.VanadisBuild{ID: "b1", Status: statusFailed}, &model.VanadisChunk{ID: chunkID, Mode: "pr", Status: statusFailed}, true, nil
}

func (s *noPRFailStore) GetBuild(ctx context.Context, buildID string) (*model.VanadisBuild, []model.VanadisChunk, error) {
	bld, chunks, err := s.VanadisStore.GetBuild(ctx, buildID)
	if err != nil || s.failedChunk == "" {
		return bld, chunks, err
	}
	bld.Status = statusFailed
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
	prRunning := model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "pr", Status: "running", Repo: "aerial", Title: "Chunk 1", Instructions: "Do step 1"}
	prOpened := model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "pr", Status: "pr_opened", PRNumber: 7, Repo: "aerial", Title: "Chunk 1"}
	tests := []struct {
		name, identity, chunkID string
		chunk                   model.VanadisChunk
		buildStatus             string
		transitionFailure       bool
		dispatchErr, widgetErr  error
		wantErr                 string
		wantEvents              int
		wantDispatchID          string
		wantSession             string
		wantPromptPrefix        string
		wantFailReason          string
		wantPrompts             []string
	}{
		{"current completed generation resumes", "vanadis-c1-turn-1", "c1", actionDone, "completed", false, nil, nil, "", 1, "", "", "", "", []string{"[VANADIS BUILD COMPLETED]\nBuild ID: b1\nObjective: \n\nAll chunks completed successfully."}},
		{"stale completed generation suppressed", "vanadis-c1-turn-0", "c1", actionDone, "completed", false, nil, nil, "", 0, "", "", "", "", nil},
		{"continuation error remains retryable", "vanadis-c1-turn-1", "c1", actionDone, "completed", true, nil, nil, "continuation unavailable", 0, "", "", "", "", nil},
		{"whitespace identity resumes", "vanadis-c1-turn-1", " c1 ", actionDone, "completed", false, nil, nil, "", 1, "", "", "", "", []string{"[VANADIS BUILD COMPLETED]\nBuild ID: b1\nObjective: \n\nAll chunks completed successfully."}},
		{"pr launch turn without pr resumes once", "vanadis-c1-turn-0", "c1", prRunning, "running", false, nil, nil, "", 0, "vanadis-c1-nopr-resume-1", "vanadis-c1", "[VANADIS AUTOMATIC RESUME]\nYour previous turn for chunk c1 ended without opening a PR.", "", nil},
		{"pr resume turn without pr fails and wakes thread", "vanadis-c1-nopr-resume-1", "c1", prRunning, "running", false, nil, nil, "", 1, "", "", "", "chunk turn ended without opening a PR (after one automatic resume)", []string{"[VANADIS BUILD FAILED]\nBuild ID: b1\nObjective: \n\nBuild encountered failures."}},
		{"pr launch turn with attached pr ignored", "vanadis-c1-turn-0", "c1", prOpened, "running", false, nil, nil, "", 0, "", "", "", "", nil},
		{"pr resume dispatch failure stays retryable", "vanadis-c1-turn-0", "c1", prRunning, "running", false, errors.New("eri unavailable"), nil, "eri unavailable", 0, "vanadis-c1-nopr-resume-1", "vanadis-c1", "[VANADIS AUTOMATIC RESUME]\n", "", nil},
		{"pr resume widget failure still resumes", "vanadis-c1-turn-0", "c1", prRunning, "running", false, nil, errors.New("widget down"), "", 0, "vanadis-c1-nopr-resume-1", "vanadis-c1", "[VANADIS AUTOMATIC RESUME]\n", "", nil},
		{"pr no-pr failure transition error stays retryable", "vanadis-c1-nopr-resume-1", "c1", prRunning, "running", true, nil, nil, "continuation unavailable", 0, "", "", "", "", nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			build := model.VanadisBuild{ID: "b1", Status: tc.buildStatus, ThreadID: "thread"}
			scripted, _ := (dbScript{row: &mockRow{b: &build}, chunks: []model.VanadisChunk{tc.chunk}}).open()
			st := &noPRFailStore{VanadisStore: scripted}
			bus := &deploymentEventRecorder{}
			var prompts []string
			eri := &mockEriClient{
				promptFn: func(_ context.Context, _, prompt string) error {
					prompts = append(prompts, prompt)
					return nil
				},
				dispatchFn: func(context.Context, executor.BrainDispatchRequest) error { return tc.dispatchErr },
				widgetFn:   func(context.Context, executor.StatusWidgetRequest) (string, error) { return "msg-1", tc.widgetErr },
			}
			bo := NewBuildObject("", nil).WithAuthority(st, executor.NewExecutor("", eri, bus, "token", nil))
			bo.runtimeFactory = func(restate.ObjectContext) objectRuntime {
				rt := newFakeObjectRuntime("b1")
				if tc.transitionFailure {
					rt.runStepErr = errors.New("continuation unavailable")
				}
				return rt
			}
			err := bo.OnDispatchOutcome(makeObjectMockContext(t, "b1"), pubsub.DispatchOutcomeEvent{VanadisChunkID: tc.chunkID, DispatchID: tc.identity, Status: "completed"})
			if tc.wantErr != "" {
				assert.ErrorContains(t, err, tc.wantErr)
			} else {
				assert.NoError(t, err)
			}
			assert.Len(t, bus.events, tc.wantEvents)
			assert.Equal(t, tc.wantPrompts, prompts)
			assert.Equal(t, tc.wantFailReason, st.failReason)
			if tc.wantDispatchID == "" {
				assert.Nil(t, eri.lastDispatch)
				return
			}
			require.NotNil(t, eri.lastDispatch)
			assert.Equal(t, tc.wantDispatchID, eri.lastDispatch.ID)
			assert.Equal(t, tc.wantSession, eri.lastDispatch.SessionID)
			assert.Equal(t, "thread", eri.lastDispatch.TargetID)
			assert.False(t, eri.lastDispatch.SkipIfActive)
			assert.True(t, strings.HasPrefix(eri.lastDispatch.Prompt, tc.wantPromptPrefix), eri.lastDispatch.Prompt)
			assert.Contains(t, eri.lastDispatch.Prompt, "Do step 1")
		})
	}
}

func TestMixedBuildCompletionRedelivery(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		quiet       bool
		wantPrompts int
	}{
		{"mixed build completion", false, 1},
		{"quiet mixed build completion", true, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			build := model.VanadisBuild{ID: "b1", Status: "running", ThreadID: "thread", Quiet: tc.quiet}
			chunks := []model.VanadisChunk{
				{ID: "c1", BuildID: "b1", Mode: "action", Status: "completed"},
				{ID: "c2", BuildID: "b1", Mode: "pr", Status: "completed"},
			}
			st, _ := (dbScript{row: &mockRow{b: &build}, chunks: chunks}).open()
			bus := &deploymentEventRecorder{}
			var queuedPrompts []string
			eri := &mockEriClient{promptFn: func(_ context.Context, _, prompt string) error {
				queuedPrompts = append(queuedPrompts, prompt)
				return nil
			}}
			bo := NewBuildObject("", nil).WithAuthority(st, executor.NewExecutor("", eri, bus, "token", nil))
			rt := newFakeObjectRuntime("b1")
			_, err := bo.executeObjectTransition(rt, "reconcile-build", nil)
			assert.NoError(t, err)
			// The next database snapshot reflects the completion written by reconciliation.
			build.Status = "completed"
			_, err = bo.executeObjectTransition(rt, "mutate-build", &BuildMutationRequest{Action: "action_completed", ChunkID: "c1"})
			assert.NoError(t, err)
			assert.Len(t, bus.events, 1)
			assert.Len(t, queuedPrompts, tc.wantPrompts)
		})
	}
}

func TestBuildObject_FailureMutations(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, chunkID, dispatchID string
		db                        dbScript
		wantErr                   error
	}{
		{"missing generation", "c1", "", dbScript{row: failRow("running", "running", 1)}, errActionCompletionSkipped},
		{"stale generation", "c1", "vanadis-c1-turn-0", dbScript{row: failRow("running", "running", 1)}, errActionCompletionSkipped},
		{"current generation", " c1 ", "vanadis-c1-turn-1", dbScript{row: failRow("running", "running", 1)}, nil},
		{"persistence cancellation", "c1", "", dbScript{row: failRow("running", "running"), failExec: "UPDATE vanadis_chunks", execErr: context.Canceled}, context.Canceled},
		{"wrong owner", "other", "", dbScript{}, ErrChunkNotFound},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			script := tc.db
			script.buildRow = &mockRow{b: &model.VanadisBuild{ID: "b1", Status: "running"}}
			script.chunks = []model.VanadisChunk{{ID: "c1", BuildID: "b1", Mode: "action", Status: "running"}}
			st, _ := script.open()
			bo := NewBuildObject("", nil).WithAuthority(st, nil)
			err := bo.applyMutation(context.Background(), "b1", &BuildMutationRequest{Action: "chunk_failed", ChunkID: tc.chunkID, DispatchID: tc.dispatchID, Reason: "failure"})
			if tc.wantErr != nil {
				assert.ErrorIs(t, err, tc.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestActionPRFailureFence(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, status, identity string
		wantFailed             bool
	}{
		{"closed without merging", "pr_opened", "", true},
		{"current action", "pr_opened", "vanadis-c1-turn-1", true},
		{"stale action", "pr_opened", "vanadis-c1-turn-0", false},
		{"ci remediation", "pr_opened", "vanadis-c1-cifail-1-abcdef0", true},
		{"conflict remediation", "pr_opened", "vanadis-conflict-c1", true},
		{"deploy failed fences delayed action", "deploy_failed", "vanadis-c1-turn-1", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, _ := dbScript{row: failRow(tc.status, "running", 1, 42)}.open()
			build, chunk, failed, err := st.FailChunk(context.Background(), "c1", "pr_closed_unmerged", tc.identity)
			assert.NoError(t, err)
			assert.Equal(t, tc.wantFailed, failed)
			if tc.wantFailed {
				assert.Equal(t, "failed", build.Status)
				assert.Equal(t, "failed", chunk.Status)
			}
		})
	}
}

func failRow(cStatus, bStatus string, generation ...int) *genericMockRow {
	return &genericMockRow{scanFn: func(dest ...any) error {
		now := time.Now()
		nudges, prNumber := 0, 0
		if len(generation) > 0 {
			nudges = generation[0]
		}
		if len(generation) > 1 {
			prNumber = generation[1]
		}
		return scanValues(dest,
			"c1", "b1", 0, "action", "", "Fail Chunk Title", "Instructions", []string{}, []string{}, "medium", []string{}, cStatus, prNumber, sql.NullTime{Time: now, Valid: true}, sql.NullTime{Time: now, Valid: true}, now, now, nudges,
			"b1", "thread-1", 2, "Build 1", "Objective 1", "Context", bStatus, "msg-1", now, now,
		)
	}}
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
			build := model.VanadisBuild{ID: "b", Status: "running", ThreadID: "thread"}
			chunks := []model.VanadisChunk{{ID: "c", BuildID: "b", Mode: "action", Status: tc.status, StallNudges: tc.generation, Title: "action"}}
			st, _ := dbScript{buildRow: &mockRow{b: &build}, chunks: chunks, execTag: "UPDATE 1"}.open()
			eri := &mockEriClient{}
			if tc.dispatchError {
				eri.dispatchFn = func(context.Context, executor.BrainDispatchRequest) error { return errors.New("rejected") }
			}
			bo := NewBuildObject("", nil).WithAuthority(st, executor.NewExecutor(t.TempDir(), eri, nil, "", nil))
			out, err := bo.executeAuthoritativeTransition(context.Background(), "b", nil, nil, tc.prior, false)
			assert.Equal(t, tc.wantError, err != nil)
			assert.Equal(t, tc.wantDispatch, eri.lastDispatch != nil)
			if tc.wantKey != "" {
				assert.True(t, out.LaunchedMap[tc.wantKey])
			}
			if eri.lastDispatch != nil {
				assert.Equal(t, fmt.Sprintf("vanadis-c-turn-%d", tc.generation), eri.lastDispatch.ID)
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
		{"resume at capacity", "action", "running", 2, 3, "completed", true, []string{"vanadis-retry-turn-2"}},
		{"unlaunched running reserves slot", "pr", "running", 0, 3, "completed", true, []string{"vanadis-retry-turn-0"}},
		{"pending at capacity", "action", "pending", 2, 4, "completed", false, nil},
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
			eri := &mockEriClient{dispatchFn: func(_ context.Context, req executor.BrainDispatchRequest) error {
				dispatched = append(dispatched, req.ID)
				return nil
			}}
			bo := NewBuildObject("", nil).WithAuthority(&mockStoreForLaunch{}, executor.NewExecutor(t.TempDir(), eri, nil, "", nil))
			launched := map[string]bool{"dispatch:vanadis-retry-turn-1": true, "dependency": true}
			if tc.nudges > 0 {
				launched["retry"] = true
			}
			_, err := bo.launchEligibleChunks(context.Background(), model.VanadisBuild{ID: "b"}, chunks, launched)
			assert.NoError(t, err)
			assert.Equal(t, tc.wantDispatched, dispatched)
			if len(tc.wantDispatched) > 0 {
				// Repeated reconciliation after acceptance must leave the generation acknowledged
				// and keep the 4-chunk cap intact against pending siblings.
				dispatched = nil
				_, err = bo.launchEligibleChunks(context.Background(), model.VanadisBuild{ID: "b"}, chunks, launched)
				assert.NoError(t, err)
				assert.Empty(t, dispatched)
			}
		})
	}
}

func TestBuildObject_DeferredLaunchReleasesInvocation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, mode, id, key, first string
		nudges                     int
		deferred                   bool
	}{
		{"retry active", "action", "vanadis-retry-turn-1", "dispatch:vanadis-retry-turn-1", "active", 1, true},
		{"initial active", "action", "vanadis-retry-turn-0", "retry", "active", 0, true},
		{"pr active", "pr", "vanadis-retry-turn-0", "retry", "active", 0, true},
		{"lost acceptance", "action", "vanadis-retry-turn-0", "retry", "lost", 0, false},
		{"server failure", "action", "vanadis-retry-turn-1", "dispatch:vanadis-retry-turn-1", "failure", 1, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var ready atomic.Bool
			srv, ids := launchRecoveryServer(t, tc.id, tc.first, &ready)
			defer srv.Close()
			build := model.VanadisBuild{ID: "b", Status: "running"}
			chunks := []model.VanadisChunk{{ID: "retry", Mode: tc.mode, Status: "running", StallNudges: tc.nudges}, {ID: "dependent", Mode: "action", Status: "pending", DependsOn: []string{"retry"}}}
			st, _ := dbScript{buildRow: &mockRow{b: &build}, chunks: chunks, execTag: "UPDATE 1"}.open()
			bo := NewBuildObject("", nil).WithAuthority(st, executor.NewExecutor(t.TempDir(), executor.NewHTTPEriClient(srv.URL, srv.Client()), nil, "", nil))
			rt := newFakeObjectRuntime("b")
			_, err := bo.executeObjectTransition(rt, "reconcile-build", nil)
			assert.Equal(t, !tc.deferred, err != nil)
			assert.Equal(t, tc.deferred, rt.reconcileScheduled)
			assert.False(t, rt.launched[tc.key])
			ready.Store(true)
			_, err = bo.executeObjectTransition(rt, "reconcile-build", nil)
			assert.NoError(t, err)
			assert.True(t, rt.launched[tc.key])
			_, err = bo.executeObjectTransition(rt, "reconcile-build", nil)
			assert.NoError(t, err)
			assert.False(t, rt.launched["dependent"])
			var got []string
			for len(ids) > 0 {
				got = append(got, <-ids)
			}
			assert.Equal(t, []string{tc.id, tc.id}, got)
			mc := mocks.NewMockContext(t)
			mc.EXPECT().Key().Return("b")
			mc.EXPECT().Run(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(runMockRunContext)
			bo.runtimeFactory = func(restate.ObjectContext) objectRuntime { return rt }
			cancelled, err := bo.Cancel(restate.WithMockContext(mc), CancelBuildRequest{})
			assert.NoError(t, err)
			assert.Equal(t, "cancelled", cancelled.Status)
		})
	}
}

func TestClassifyLaunchErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                    string
		failures                []error
		wantError, wantDeferred bool
	}{
		{"no failures", nil, false, false},
		{"active dispatch", []error{executor.ErrDispatchActive}, true, true},
		{"several active dispatches", []error{executor.ErrDispatchActive, executor.ErrDispatchActive}, true, true},
		{"transport failure", []error{errors.New("unavailable")}, true, false},
		{"mixed failures", []error{executor.ErrDispatchActive, errors.New("unavailable")}, true, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := classifyLaunchErrors(tc.failures)
			assert.Equal(t, tc.wantError, err != nil)
			assert.Equal(t, tc.wantDeferred, errors.Is(err, executor.ErrDispatchActive))
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

	// Quiet build suppresses error notification
	buildQuiet := model.VanadisBuild{ID: "b-quiet", Quiet: true}
	storeQuiet, _ := dbScript{buildRow: &mockRow{b: &buildQuiet}}.open()
	eriQuiet := &mockEriClient{}
	boQuiet := NewBuildObject("", nil).WithAuthority(storeQuiet, executor.NewExecutor(t.TempDir(), eriQuiet, nil, "", nil))
	require.NoError(t, boQuiet.notifyTransitionError(context.Background(), "b-quiet", "step", nil, errors.New("trans err")))
	assert.Nil(t, eriQuiet.lastWidget)

	// Non-quiet build resolves from store and emits banner
	buildActive := model.VanadisBuild{ID: "b-active", Title: "Active Build", Status: "running"}
	storeActive, _ := dbScript{buildRow: &mockRow{b: &buildActive}, chunks: []model.VanadisChunk{{ID: "c1", Title: "Chunk 1"}}}.open()
	eriActive := &mockEriClient{}
	boActive := NewBuildObject("", nil).WithAuthority(storeActive, executor.NewExecutor(t.TempDir(), eriActive, nil, "", nil))
	require.NoError(t, boActive.notifyTransitionError(context.Background(), "b-active", "step", nil, errors.New("trans err")))
	require.NotNil(t, eriActive.lastWidget)
	assert.Equal(t, "b-active", eriActive.lastWidget.ID)
	assert.Equal(t, "⚠️ Reconcile backing off (step): trans err", eriActive.lastWidget.Banner)
	require.Len(t, eriActive.lastWidget.Chunks, 1)
	assert.Equal(t, "c1", eriActive.lastWidget.Chunks[0].ID)

	// Fallback to priorState when store is nil
	eriPrior := &mockEriClient{}
	boNoStore := NewBuildObject("", nil).WithAuthority(nil, executor.NewExecutor(t.TempDir(), eriPrior, nil, "", nil))
	prior := &model.BuildObjectState{Build: model.VanadisBuild{ID: "b-prior", Title: "Prior Build"}, Chunks: []model.VanadisChunk{{ID: "c-prior", Title: "Prior Chunk"}}}
	require.NoError(t, boNoStore.notifyTransitionError(context.Background(), "b-prior", "step", prior, errors.New("trans err")))
	require.NotNil(t, eriPrior.lastWidget)
	assert.Equal(t, "b-prior", eriPrior.lastWidget.ID)
	assert.Equal(t, "⚠️ Reconcile backing off (step): trans err", eriPrior.lastWidget.Banner)
	require.Len(t, eriPrior.lastWidget.Chunks, 1)
	assert.Equal(t, "c-prior", eriPrior.lastWidget.Chunks[0].ID)

	// Retry inside RunReconcileStep records counter and emits banner before succeeding on retry
	attempts := 0
	var retryBanners []string
	eriRetry := &mockEriClient{
		dispatchFn: func(context.Context, executor.BrainDispatchRequest) error {
			attempts++
			if attempts == 1 {
				return errors.New("transient dispatch failure")
			}
			return nil
		},
		widgetFn: func(_ context.Context, req executor.StatusWidgetRequest) (string, error) {
			retryBanners = append(retryBanners, req.Banner)
			return "msg-retry", nil
		},
	}
	storeRetry, _ := dbScript{
		buildRow: &mockRow{b: &buildActive},
		chunks:   []model.VanadisChunk{{ID: "c1", BuildID: "b-active", Mode: "action", Status: "pending", Title: "Chunk 1"}},
		execTag:  "UPDATE 1",
	}.open()
	boRetry := NewBuildObject("", nil).WithAuthority(storeRetry, executor.NewExecutor(t.TempDir(), eriRetry, nil, "", nil))
	rtRetry := &retryingObjectRuntime{fakeObjectRuntime: newFakeObjectRuntime("b-active")}
	beforeCount := reconcileErrorsCount(t, "reconcile-retry-test")
	res, err := boRetry.executeObjectTransition(rtRetry, "reconcile-retry-test", nil)
	require.NoError(t, err)
	assert.Equal(t, "running", res.Status)
	assert.Equal(t, beforeCount+1, reconcileErrorsCount(t, "reconcile-retry-test"))
	assert.Contains(t, retryBanners, "⚠️ Reconcile backing off (reconcile-retry-test): transient dispatch failure")
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
		name          string
		handler       string
		err           error
		wantClientInc float64
		wantServerInc float64
	}{
		{
			name:          "nil error increments neither",
			handler:       "test-handler-nil",
			err:           nil,
			wantClientInc: 0,
			wantServerInc: 0,
		},
		{
			name:          "store.ErrInvalidBuildInput routes to client errors",
			handler:       "test-handler-invalid",
			err:           store.ErrInvalidBuildInput,
			wantClientInc: 1,
			wantServerInc: 0,
		},
		{
			name:          "wrapped store.ErrInvalidBuildInput routes to client errors",
			handler:       "test-handler-wrapped-invalid",
			err:           fmt.Errorf("validation error: %w", store.ErrInvalidBuildInput),
			wantClientInc: 1,
			wantServerInc: 0,
		},
		{
			name:          "store.ErrBuildNotFound routes to client errors",
			handler:       "test-handler-build-not-found",
			err:           store.ErrBuildNotFound,
			wantClientInc: 1,
			wantServerInc: 0,
		},
		{
			name:          "ErrChunkNotFound routes to client errors",
			handler:       "test-handler-chunk-not-found",
			err:           ErrChunkNotFound,
			wantClientInc: 1,
			wantServerInc: 0,
		},
		{
			name:          "store.ErrBuildConflict routes to client errors",
			handler:       "test-handler-conflict",
			err:           store.ErrBuildConflict,
			wantClientInc: 1,
			wantServerInc: 0,
		},
		{
			name:          "4xx restate.TerminalError routes to client errors",
			handler:       "test-handler-terminal-400",
			err:           restate.ToTerminalError(errors.New("bad input"), restate.WithErrorCode(400)),
			wantClientInc: 1,
			wantServerInc: 0,
		},
		{
			name:          "404 restate.TerminalError routes to client errors",
			handler:       "test-handler-terminal-404",
			err:           restate.ToTerminalError(errors.New("resource missing"), restate.WithErrorCode(404)),
			wantClientInc: 1,
			wantServerInc: 0,
		},
		{
			name:          "409 restate.TerminalError routes to client errors",
			handler:       "test-handler-terminal-409",
			err:           restate.ToTerminalError(errors.New("state conflict"), restate.WithErrorCode(409)),
			wantClientInc: 1,
			wantServerInc: 0,
		},
		{
			name:          "499 restate.TerminalError routes to client errors",
			handler:       "test-handler-terminal-499",
			err:           restate.ToTerminalError(errors.New("custom client error"), restate.WithErrorCode(499)),
			wantClientInc: 1,
			wantServerInc: 0,
		},
		{
			name:          "5xx restate.TerminalError routes to server errors",
			handler:       "test-handler-terminal-500",
			err:           restate.ToTerminalError(errors.New("internal error"), restate.WithErrorCode(500)),
			wantClientInc: 0,
			wantServerInc: 1,
		},
		{
			name:          "uncoded restate.TerminalErrorf routes to server errors",
			handler:       "test-handler-terminal-uncoded",
			err:           restate.TerminalErrorf("generic terminal error: %s", "boom"),
			wantClientInc: 0,
			wantServerInc: 1,
		},
		{
			name:          "transient error routes to server errors",
			handler:       "test-handler-transient",
			err:           errors.New("transient database connection timeout"),
			wantClientInc: 0,
			wantServerInc: 1,
		},
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
		{"pr duplicate", "pr", "c", `{"status":"already_exists","dispatch_id":"vanadis-c-turn-0"}`, 0, 200, false, true},
		{"pr active", "pr", "c", `{"status":"skipped_active"}`, 0, 200, false, false},
		{"pr wrong accepted", "pr", "c", `{"status":"accepted","dispatch_id":"wrong"}`, 0, 202, true, false},
		{"pr wrong duplicate", "pr", "c", `{"status":"already_exists","dispatch_id":"wrong"}`, 0, 200, true, false},
		{"pr malformed", "pr", "c", `invalid`, 0, 200, true, false},
		{"pr missing status", "pr", "c", `{}`, 0, 200, true, false},
		{"pr server failure", "pr", "c", `{}`, 0, 500, true, false},
		{"initial accepted", "action", "c", `{"status":"accepted","dispatch_id":"vanadis-c-turn-0"}`, 0, 202, false, true},
		{"initial duplicate", "action", "c", `{"status":"already_exists","dispatch_id":"vanadis-c-turn-0"}`, 0, 200, false, true},
		{"initial active", "action", "c", `{"status":"skipped_active"}`, 0, 200, false, false},
		{"initial wrong accepted", "action", "c", `{"status":"accepted","dispatch_id":"wrong"}`, 0, 202, true, false},
		{"initial wrong duplicate", "action", "c", `{"status":"already_exists","dispatch_id":"wrong"}`, 0, 200, true, false},
		{"initial malformed", "action", "c", `invalid`, 0, 200, true, false},
		{"initial missing status", "action", "c", `{}`, 0, 200, true, false},
		{"initial server failure", "action", "c", `{}`, 0, 500, true, false},
		{"retry accepted", "action", "dispatch:vanadis-c-turn-1", `{"status":"accepted","dispatch_id":"vanadis-c-turn-1"}`, 1, 202, false, true},
		{"retry duplicate", "action", "dispatch:vanadis-c-turn-1", `{"status":"already_exists","dispatch_id":"vanadis-c-turn-1"}`, 1, 200, false, true},
		{"retry active", "action", "dispatch:vanadis-c-turn-1", `{"status":"skipped_active"}`, 0, 200, false, false},
		{"retry wrong accepted", "action", "dispatch:vanadis-c-turn-1", `{"status":"accepted","dispatch_id":"wrong"}`, 1, 202, true, false},
		{"retry wrong duplicate", "action", "dispatch:vanadis-c-turn-1", `{"status":"already_exists","dispatch_id":"wrong"}`, 1, 200, true, false},
		{"retry malformed", "action", "dispatch:vanadis-c-turn-1", `invalid`, 1, 200, true, false},
		{"retry missing status", "action", "dispatch:vanadis-c-turn-1", `{}`, 1, 200, true, false},
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
			build := model.VanadisBuild{ID: "b", Status: "running"}
			st, _ := dbScript{buildRow: &mockRow{b: &build}, chunks: []model.VanadisChunk{{ID: "c", Mode: tc.mode, Status: "running", StallNudges: tc.nudges}}, execTag: "UPDATE 1"}.open()
			bo := NewBuildObject("", nil).WithAuthority(st, executor.NewExecutor(t.TempDir(), executor.NewHTTPEriClient(srv.URL, srv.Client()), nil, "", nil))
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
		var req executor.BrainDispatchRequest
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
	bo := NewBuildObject("", nil).WithAuthority(&mockStoreForLaunch{}, executor.NewExecutor(t.TempDir(), executor.NewHTTPEriClient(srv.URL, srv.Client()), nil, "", nil))
	launched := map[string]bool{}
	_, err := bo.launchEligibleChunks(context.Background(), build, chunks, launched)
	assert.ErrorIs(t, err, executor.ErrDispatchActive)
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
