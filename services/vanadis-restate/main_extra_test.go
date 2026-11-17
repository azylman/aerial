package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/x/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/pubsub"
)

func TestBuildObject_LifecycleTransitionsAndFailures(t *testing.T) {
	t.Parallel()
	b1 := VanadisBuild{ID: "b-more", Status: "running", ThreadID: "th-1"}
	c1 := VanadisChunk{ID: "c1", BuildID: "b-more", ChunkIndex: 0, Title: "C1", Status: "pending", Mode: "action"}

	// 1. Cancel on already terminal build -> ErrBuildConflict
	bTerm := VanadisBuild{ID: "b-term", Status: "completed", ThreadID: "th-1"}
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
	_, _ = boUpdErr.UpdateChunk(ctxCancel, UpdateChunkRequest{ChunkRef: "c1", Title: "new"})

	// 4. AppendChunks error in store
	_, _ = boUpdErr.AppendChunks(ctxCancel, AppendChunksRequest{Chunks: []CreateChunkInput{{Title: "C2"}}})

	// 5. RemoveChunk error in store
	_ = boUpdErr.RemoveChunk(ctxCancel, RemoveChunkRequest{ChunkRef: "c1"})

	// 6. FailChunk error in store
	_, _ = boUpdErr.FailChunk(ctxCancel, FailChunkRequest{ChunkID: "c1", Reason: "boom"})

	// 7. handlePREventType error branches
	eriErr := &mockEriClient{
		promptFn: func(context.Context, string, string) error { return errors.New("prompt err") },
		widgetFn: func(context.Context, StatusWidgetRequest) (string, error) { return "", errors.New("widget err") },
	}
	execErr := NewExecutor(t.TempDir(), eriErr, &mockBusError{err: errors.New("pub err")}, "token", nil)
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

	// 9. evaluateBuildTerminalStatus with store error
	boTermErr := NewBuildObject("", nil).WithAuthority(storeCancelErr, execErr)
	bF := VanadisBuild{ID: "b-fail", Status: "running", ThreadID: "th-1"}
	cF := VanadisChunk{ID: "c1", Status: "failed"}
	_ = boTermErr.evaluateBuildTerminalStatus(context.Background(), &bF, []VanadisChunk{cF})

	bD := VanadisBuild{ID: "b-done", Status: "running", ThreadID: "th-1"}
	cD := VanadisChunk{ID: "c1", Status: "completed"}
	_ = boTermErr.evaluateBuildTerminalStatus(context.Background(), &bD, []VanadisChunk{cD})

	// 10. UpdateBuild with widget sync error
	storeUpdOk, _ := dbScript{
		buildRow: &mockRow{b: &b1},
		chunks:   []VanadisChunk{c1},
		execTag:  "UPDATE 1",
	}.open()
	boUpdWidgetErr := NewBuildObject("", nil).WithAuthority(storeUpdOk, execErr)
	_, _ = boUpdWidgetErr.UpdateBuild(ctxCancel, UpdateBuildRequest{Title: "new title"})

	// 11. persistBuildCreation with widget sync error
	_, _ = restate.Run(ctxCancel, func(rc restate.RunContext) (bool, error) {
		return boUpdWidgetErr.persistBuildCreation(rc, b1, []VanadisChunk{c1})
	})

	// 12. AttachPR widget sync error
	_, _ = boUpdWidgetErr.AttachPR(ctxCancel, AttachChunkPRRequest{ChunkID: "c1", Repo: "aerial"})
}

type badReader struct{}

func (badReader) Read([]byte) (int, error) {
	return 0, errors.New("read error")
}

func TestHTTPEriClient_ReadError(t *testing.T) {
	t.Parallel()
	client := &http.Client{
		Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusInternalServerError,
				Body:       io.NopCloser(badReader{}),
			}, nil
		}),
	}
	c := NewHTTPEriClient("http://eri.test", client)
	err := c.Dispatch(context.Background(), BrainDispatchRequest{})
	if err == nil {
		t.Fatal("expected error")
	}

	clientBadJSON := &http.Client{
		Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader("not json")),
			}, nil
		}),
	}
	cBadJSON := NewHTTPEriClient("http://eri.test", clientBadJSON)
	_, err = cBadJSON.Widget(context.Background(), StatusWidgetRequest{})
	if err == nil {
		t.Fatal("expected unmarshal error")
	}
}

func TestDag_ExtraValidationBranches(t *testing.T) {
	t.Parallel()
	// store.go isValidVanadisChunkID empty/too long
	if isValidVanadisChunkID("") {
		t.Fatal("empty id should be invalid")
	}
	if isValidVanadisChunkID(strings.Repeat("a", 121)) {
		t.Fatal("too long id should be invalid")
	}
	// dag.go normalizeAndValidateBuildDAG duplicate ID
	_, err := normalizeAndValidateBuildDAG([]VanadisChunk{
		{ID: "c1", Status: "pending"},
		{ID: "c1", Status: "pending"},
	})
	if err == nil {
		t.Fatal("expected error on duplicate chunk ID")
	}
}

func TestDag_ExtraDisjointAndValidation(t *testing.T) {
	t.Parallel()
	// dag.go L233-L234: validateCreateBuildInputs chunkMode validation error
	err := validateCreateBuildInputs("b1", "th1", "obj", []VanadisChunk{
		{ID: "c1", Title: "t", Instructions: "i", Mode: "action", Repo: "non-empty-repo"},
	})
	if err == nil {
		t.Fatal("expected error on action mode with repo")
	}

	// dag.go L178-L178: validateConcurrentDisjointPRChunks where modeJ != "pr"
	chunks := []VanadisChunk{
		{ID: "c1", Status: "pending", Mode: "pr", Repo: "aerial", TargetFiles: []string{"a.go"}},
		{ID: "c2", Status: "pending", Mode: "action", Repo: ""},
	}
	reach := buildTransitiveClosure(chunks, map[string]int{"c1": 0, "c2": 1})
	_ = validateConcurrentDisjointPRChunks(chunks, reach)
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
			build := VanadisBuild{ID: "b1", Status: tc.buildStatus}
			chunk := VanadisChunk{ID: "c1", BuildID: "b1", Mode: tc.mode, Repo: "owner/repo", Status: tc.chunkStatus, PRNumber: tc.prNumber, RolloutState: &pubsub.PRRolloutState{ExpectedJobs: []string{"existing"}}}
			script := dbScript{row: &mockRow{b: &build}, countRow: valuesRow(1), chunks: []VanadisChunk{chunk, {ID: "sibling", BuildID: "b1", Mode: "pr", Status: "completed"}}, execTag: "UPDATE 1"}
			if tc.readFailure {
				script.row = errRow(errors.New("read unavailable"))
			}
			if tc.fail {
				script.execErr = errors.New("persist failed")
				script.failExec = "UPDATE vanadis_chunks"
			}
			store, _ := script.open()
			observed := &deploymentEvidenceDB{pgxQueryExecer: store.db, chunks: script.chunks}
			store.db = observed
			bo := NewBuildObject("", nil).WithAuthority(store, NewExecutor("", &mockEriClient{}, pubsub.NewMemoryBus(), "token", nil))
			evidence := &pubsub.PRRolloutState{ExpectedJobs: []string{"service"}}
			if tc.nilEvidence {
				evidence = nil
			}
			out, err := bo.executeAuthoritativeTransition(context.Background(), "b1", &BuildMutationRequest{Action: "chunk_deploy_success", ChunkID: "c1", RolloutState: evidence}, nil, false)

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
			build := VanadisBuild{ID: "b1", Status: "failed"}
			chunk := VanadisChunk{ID: "c1", BuildID: "b1", Mode: "pr", Status: "completed"}
			script := dbScript{
				buildRow: &mockRow{b: &build}, chunks: []VanadisChunk{chunk},
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
			store, _ := script.open()
			observed := &deploymentEvidenceDB{pgxQueryExecer: store.db, chunks: script.chunks}
			store.db = observed
			bo := NewBuildObject("", nil).WithAuthority(store, nil)
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
	pgxQueryExecer

	chunks   []VanadisChunk
	observed VanadisChunk
}

func (db *deploymentEvidenceDB) Exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error) {
	var chunk *VanadisChunk
	for i := range db.chunks {
		id, _ := args[len(args)-1].(string)
		if id == db.chunks[i].ID {
			chunk = &db.chunks[i]
			break
		}
	}
	if chunk != nil && strings.Contains(query, "AND status NOT IN") && (chunk.Status == "completed" || chunk.Status == "failed" || chunk.Status == "cancelled") {
		return pgconn.NewCommandTag("UPDATE 0"), nil
	}
	tag, err := db.pgxQueryExecer.Exec(ctx, query, args...)
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
	return tag, nil
}

func (db *deploymentEvidenceDB) Query(ctx context.Context, query string, args ...any) (pgx.Rows, error) {
	db.observed = db.chunks[0]
	return db.pgxQueryExecer.Query(ctx, query, args...)
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
			build := VanadisBuild{ID: "b1", Status: "running"}
			chunk := VanadisChunk{ID: "c1", BuildID: "b1", Mode: "pr", Status: tc.status}
			script := dbScript{row: &mockRow{b: &build}, chunks: []VanadisChunk{chunk}, execTag: "UPDATE 0"}
			if tc.fail {
				script.execErr = errors.New("database unavailable")
				script.failExec = "UPDATE vanadis_chunks"
			}
			store, _ := script.open()
			bus := &deploymentEventRecorder{}
			bo := NewBuildObject("", nil).WithAuthority(store, NewExecutor("", &mockEriClient{}, bus, "token", nil))
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
			build := VanadisBuild{ID: "b1", Status: "running"}
			chunks := []VanadisChunk{
				{ID: "c1", BuildID: "b1", Mode: "pr", Status: "deploying", PRNumber: 42},
				{ID: "c2", BuildID: "b1", Mode: "pr", Status: "deploying", PRNumber: 43},
				{ID: "action", BuildID: "b1", Mode: "action", Status: "pending", DependsOn: []string{"c1", "c2"}},
			}
			script := dbScript{
				buildRow: &mockRow{b: &build}, chunks: chunks, execTag: "UPDATE 1",
				routes: []sqlRoute{{match: "c.repo = ANY($1)", row: valuesRow("c1", "b1", 0, "medium", true)}},
			}
			store, _ := script.open()
			db := &deploymentEvidenceDB{pgxQueryExecer: store.db, chunks: chunks}
			store.db = db
			bus := &deploymentEventRecorder{}
			bo := NewBuildObject("", nil).WithAuthority(store, NewExecutor("", &mockEriClient{}, bus, "token", nil))
			ctx := makeObjectMockContext(t, "b1")
			evidence := &pubsub.PRRolloutState{ExpectedJobs: []string{"service"}}
			assert.NoError(t, bo.OnPREvent(ctx, pubsub.PRLifecycleEvent{Event: "deploy_success", Repo: "owner/repo", PRNumber: 42, RolloutState: evidence}))
			assert.Equal(t, "pending", chunks[2].Status)
			events := append([]any(nil), bus.events...)
			assert.NoError(t, bo.OnPREvent(ctx, pubsub.PRLifecycleEvent{Event: tc.delayedEvent, Repo: "owner/repo", PRNumber: 42}))
			assert.Equal(t, "completed", chunks[0].Status)
			assert.Equal(t, evidence, chunks[0].RolloutState)
			assert.Equal(t, events, bus.events)
			out, err := bo.executeAuthoritativeTransition(context.Background(), "b1", &BuildMutationRequest{Action: "chunk_deploy_success", ChunkID: "c2"}, nil, false)
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
	tx, err := db.pgxQueryExecer.Begin(ctx)
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
			build := VanadisBuild{ID: tc.buildID, Status: "running"}
			chunk := VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: tc.status, StallNudges: 1}
			chunks := []VanadisChunk{chunk}
			if tc.buildID != "b1" {
				chunks = nil
			}
			store, _ := (dbScript{row: &mockRow{b: &build}, chunks: chunks, countRow: valuesRow(1)}).open()
			bus := &deploymentEventRecorder{}
			bo := NewBuildObject("", nil).WithAuthority(store, NewExecutor("", &mockEriClient{}, bus, "token", nil))
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
			build := VanadisBuild{ID: "b1", Status: tc.buildStatus, ThreadID: "thread", Quiet: tc.quiet}
			chunks := []VanadisChunk{{ID: "c1", BuildID: "b1", Mode: "action", Status: "completed", StallNudges: 1}}
			if tc.buildStatus == "running" {
				chunks = append(chunks, VanadisChunk{ID: "next", BuildID: "b1", Mode: "pr", Status: "pending", Repo: "owner/repo", TargetFiles: []string{"main.go"}, DependsOn: []string{"c1"}})
			}
			store, _ := (dbScript{row: &mockRow{b: &build}, chunks: chunks}).open()
			bus := &deploymentEventRecorder{}
			var queuedPrompts []string
			eri := &mockEriClient{promptFn: func(_ context.Context, _, prompt string) error {
				queuedPrompts = append(queuedPrompts, prompt)
				return nil
			}}
			bo := NewBuildObject("", nil).WithAuthority(store, NewExecutor("", eri, bus, "token", nil))
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

func TestDispatchCompletionContinuation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, identity, chunkID string
		transitionFailure       bool
		wantErr                 bool
		wantEvents              int
	}{
		{"current completed generation resumes", "vanadis-c1-turn-1", "c1", false, false, 1},
		{"stale completed generation suppressed", "vanadis-c1-turn-0", "c1", false, false, 0},
		{"continuation error remains retryable", "vanadis-c1-turn-1", "c1", true, true, 0},
		{"whitespace identity resumes", "vanadis-c1-turn-1", " c1 ", false, false, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			build := VanadisBuild{ID: "b1", Status: "completed", ThreadID: "thread"}
			chunks := []VanadisChunk{{ID: "c1", BuildID: "b1", Mode: "action", Status: "completed", StallNudges: 1}}
			store, _ := (dbScript{row: &mockRow{b: &build}, chunks: chunks}).open()
			bus := &deploymentEventRecorder{}
			bo := NewBuildObject("", nil).WithAuthority(store, NewExecutor("", &mockEriClient{}, bus, "token", nil))
			bo.runtimeFactory = func(restate.ObjectContext) objectRuntime {
				rt := newFakeObjectRuntime("b1")
				if tc.transitionFailure {
					rt.runStepErr = errors.New("continuation unavailable")
				}
				return rt
			}
			err := bo.OnDispatchOutcome(makeObjectMockContext(t, "b1"), pubsub.DispatchOutcomeEvent{VanadisChunkID: tc.chunkID, DispatchID: tc.identity, Status: "completed"})
			if tc.wantErr {
				assert.ErrorContains(t, err, "continuation unavailable")
			} else {
				assert.NoError(t, err)
			}
			assert.Len(t, bus.events, tc.wantEvents)
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
			build := VanadisBuild{ID: "b1", Status: "running", ThreadID: "thread", Quiet: tc.quiet}
			chunks := []VanadisChunk{
				{ID: "c1", BuildID: "b1", Mode: "action", Status: "completed"},
				{ID: "c2", BuildID: "b1", Mode: "pr", Status: "completed"},
			}
			store, _ := (dbScript{row: &mockRow{b: &build}, chunks: chunks}).open()
			bus := &deploymentEventRecorder{}
			var queuedPrompts []string
			eri := &mockEriClient{promptFn: func(_ context.Context, _, prompt string) error {
				queuedPrompts = append(queuedPrompts, prompt)
				return nil
			}}
			bo := NewBuildObject("", nil).WithAuthority(store, NewExecutor("", eri, bus, "token", nil))
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
			script.buildRow = &mockRow{b: &VanadisBuild{ID: "b1", Status: "running"}}
			script.chunks = []VanadisChunk{{ID: "c1", BuildID: "b1", Mode: "action", Status: "running"}}
			store, _ := script.open()
			bo := NewBuildObject("", nil).WithAuthority(store, nil)
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
			store, _ := dbScript{row: failRow(tc.status, "running", 1, 42)}.open()
			build, chunk, failed, err := store.FailChunk(context.Background(), "c1", "pr_closed_unmerged", tc.identity)
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
			build := VanadisBuild{ID: "b", Status: "running", ThreadID: "thread"}
			chunks := []VanadisChunk{{ID: "c", BuildID: "b", Mode: "action", Status: tc.status, StallNudges: tc.generation, Title: "action"}}
			store, _ := dbScript{buildRow: &mockRow{b: &build}, chunks: chunks, execTag: "UPDATE 1"}.open()
			eri := &mockEriClient{}
			if tc.dispatchError {
				eri.dispatchFn = func(context.Context, BrainDispatchRequest) error { return errors.New("rejected") }
			}
			bo := NewBuildObject("", nil).WithAuthority(store, NewExecutor(t.TempDir(), eri, nil, "", nil))
			out, err := bo.executeAuthoritativeTransition(context.Background(), "b", nil, tc.prior, false)
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
			chunks := []VanadisChunk{
				{ID: "retry", Mode: tc.mode, Repo: "aerial", Status: tc.status, StallNudges: tc.nudges, DependsOn: []string{"dependency"}},
				{ID: "dependency", Mode: "action", Status: tc.dependencyStatus},
			}
			for i := range tc.siblings {
				chunks = append(chunks, VanadisChunk{ID: fmt.Sprintf("sibling-%d", i), Mode: "pr", Repo: "aerial", Status: "running", PRNumber: i + 1})
			}
			if tc.withPendingSibling {
				chunks = append(chunks, VanadisChunk{ID: "pending-sibling", Mode: "pr", Repo: "aerial", Status: "pending"})
			}
			var dispatched []string
			eri := &mockEriClient{dispatchFn: func(_ context.Context, req BrainDispatchRequest) error {
				dispatched = append(dispatched, req.ID)
				return nil
			}}
			bo := NewBuildObject("", nil).WithAuthority(&mockStoreForLaunch{}, NewExecutor(t.TempDir(), eri, nil, "", nil))
			launched := map[string]bool{"dispatch:vanadis-retry-turn-1": true, "dependency": true}
			if tc.nudges > 0 {
				launched["retry"] = true
			}
			_, err := bo.launchEligibleChunks(context.Background(), VanadisBuild{ID: "b"}, chunks, launched)
			assert.NoError(t, err)
			assert.Equal(t, tc.wantDispatched, dispatched)
			if len(tc.wantDispatched) > 0 {
				// Repeated reconciliation after acceptance must leave the generation acknowledged
				// and keep the 4-chunk cap intact against pending siblings.
				dispatched = nil
				_, err = bo.launchEligibleChunks(context.Background(), VanadisBuild{ID: "b"}, chunks, launched)
				assert.NoError(t, err)
				assert.Empty(t, dispatched)
			}
		})
	}
}

func TestBuildObject_DeferredLaunchReleasesInvocation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		dispatchErr  error
		wantDeferred bool
	}{
		{"active generation", errDispatchActive, true},
		{"transport failure", errors.New("unavailable"), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			build := VanadisBuild{ID: "b", Status: "running"}
			store, _ := dbScript{buildRow: &mockRow{b: &build}, chunks: []VanadisChunk{{ID: "retry", Mode: "action", Status: "running", StallNudges: 1}, {ID: "sibling", Mode: "action", Status: "pending"}}, execTag: "UPDATE 1"}.open()
			eri := &mockEriClient{dispatchFn: func(_ context.Context, req BrainDispatchRequest) error {
				if req.ID == "vanadis-retry-turn-1" {
					return tc.dispatchErr
				}
				return nil
			}}
			bo := NewBuildObject("", nil).WithAuthority(store, NewExecutor(t.TempDir(), eri, nil, "", nil))
			rt := newFakeObjectRuntime("b")
			_, err := bo.executeObjectTransition(rt, "reconcile-build", nil)
			assert.Equal(t, !tc.wantDeferred, err != nil)
			assert.Equal(t, tc.wantDeferred, rt.reconcileScheduled)
			if tc.wantDeferred {
				assert.True(t, rt.launched["sibling"])
				assert.False(t, rt.launched["dispatch:vanadis-retry-turn-1"])
				// Observe a sibling completion on the next invocation: its slot is
				// available to another independent chunk while retry remains active.
				nextStore, _ := dbScript{buildRow: &mockRow{b: &build}, chunks: []VanadisChunk{
					{ID: "retry", Mode: "action", Status: "running", StallNudges: 1},
					{ID: "sibling", Mode: "action", Status: "completed"},
					{ID: "next", Mode: "action", Status: "pending"},
				}, execTag: "UPDATE 1"}.open()
				bo.store = nextStore
				_, err = bo.executeObjectTransition(rt, "reconcile-build", nil)
				assert.NoError(t, err)
				assert.True(t, rt.launched["next"])
				// Cancel uses a separate exclusive invocation after deferral.
				mc := mocks.NewMockContext(t)
				mc.EXPECT().Key().Return("b")
				mc.EXPECT().Run(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(runMockRunContext)
				bo.runtimeFactory = func(restate.ObjectContext) objectRuntime { return rt }
				cancelled, err := bo.Cancel(restate.WithMockContext(mc), CancelBuildRequest{})
				assert.NoError(t, err)
				assert.Equal(t, "cancelled", cancelled.Status)
			}
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
		{"active dispatch", []error{errDispatchActive}, true, true},
		{"several active dispatches", []error{errDispatchActive, errDispatchActive}, true, true},
		{"transport failure", []error{errors.New("unavailable")}, true, false},
		{"mixed failures", []error{errDispatchActive, errors.New("unavailable")}, true, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := classifyLaunchErrors(tc.failures)
			assert.Equal(t, tc.wantError, err != nil)
			assert.Equal(t, tc.wantDeferred, errors.Is(err, errDispatchActive))
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
	buildQuiet := VanadisBuild{ID: "b-quiet", Quiet: true}
	storeQuiet, _ := dbScript{buildRow: &mockRow{b: &buildQuiet}}.open()
	eriQuiet := &mockEriClient{}
	boQuiet := NewBuildObject("", nil).WithAuthority(storeQuiet, NewExecutor(t.TempDir(), eriQuiet, nil, "", nil))
	boQuiet.notifyTransitionError(context.Background(), "b-quiet", "step", nil, errors.New("trans err"))
	assert.Nil(t, eriQuiet.lastWidget)

	// Non-quiet build resolves from store and emits banner
	buildActive := VanadisBuild{ID: "b-active", Title: "Active Build", Status: "running"}
	storeActive, _ := dbScript{buildRow: &mockRow{b: &buildActive}, chunks: []VanadisChunk{{ID: "c1", Title: "Chunk 1"}}}.open()
	eriActive := &mockEriClient{}
	boActive := NewBuildObject("", nil).WithAuthority(storeActive, NewExecutor(t.TempDir(), eriActive, nil, "", nil))
	boActive.notifyTransitionError(context.Background(), "b-active", "step", nil, errors.New("trans err"))
	require.NotNil(t, eriActive.lastWidget)
	assert.Equal(t, "b-active", eriActive.lastWidget.ID)
	assert.Equal(t, "⚠️ Reconcile backing off (step): trans err", eriActive.lastWidget.Banner)
	require.Len(t, eriActive.lastWidget.Chunks, 1)
	assert.Equal(t, "c1", eriActive.lastWidget.Chunks[0].ID)

	// Fallback to priorState when store is nil
	eriPrior := &mockEriClient{}
	boNoStore := NewBuildObject("", nil).WithAuthority(nil, NewExecutor(t.TempDir(), eriPrior, nil, "", nil))
	prior := &BuildObjectState{Build: VanadisBuild{ID: "b-prior", Title: "Prior Build"}, Chunks: []VanadisChunk{{ID: "c-prior", Title: "Prior Chunk"}}}
	boNoStore.notifyTransitionError(context.Background(), "b-prior", "step", prior, errors.New("trans err"))
	require.NotNil(t, eriPrior.lastWidget)
	assert.Equal(t, "b-prior", eriPrior.lastWidget.ID)
	assert.Equal(t, "⚠️ Reconcile backing off (step): trans err", eriPrior.lastWidget.Banner)
	require.Len(t, eriPrior.lastWidget.Chunks, 1)
	assert.Equal(t, "c-prior", eriPrior.lastWidget.Chunks[0].ID)

	// Retry inside RunReconcileStep records counter and emits banner before succeeding on retry
	attempts := 0
	var retryBanners []string
	eriRetry := &mockEriClient{
		dispatchFn: func(context.Context, BrainDispatchRequest) error {
			attempts++
			if attempts == 1 {
				return errors.New("transient dispatch failure")
			}
			return nil
		},
		widgetFn: func(_ context.Context, req StatusWidgetRequest) (string, error) {
			retryBanners = append(retryBanners, req.Banner)
			return "msg-retry", nil
		},
	}
	storeRetry, _ := dbScript{
		buildRow: &mockRow{b: &buildActive},
		chunks:   []VanadisChunk{{ID: "c1", BuildID: "b-active", Mode: "action", Status: "pending", Title: "Chunk 1"}},
		execTag:  "UPDATE 1",
	}.open()
	boRetry := NewBuildObject("", nil).WithAuthority(storeRetry, NewExecutor(t.TempDir(), eriRetry, nil, "", nil))
	rtRetry := &retryingObjectRuntime{fakeObjectRuntime: newFakeObjectRuntime("b-active")}
	beforeCount := reconcileErrorsCount(t, "reconcile-retry-test")
	res, err := boRetry.executeObjectTransition(rtRetry, "reconcile-retry-test", nil)
	require.NoError(t, err)
	assert.Equal(t, "running", res.Status)
	assert.Equal(t, beforeCount+1, reconcileErrorsCount(t, "reconcile-retry-test"))
	assert.Contains(t, retryBanners, "⚠️ Reconcile backing off (reconcile-retry-test): transient dispatch failure")
}

func reconcileErrorsCount(t *testing.T, handler string) float64 {
	t.Helper()
	rec := httptest.NewRecorder()
	SetupHealthMux().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody))
	prefix := fmt.Sprintf(`vanadis_restate_reconcile_errors_total{handler=%q} `, handler)
	for line := range strings.SplitSeq(rec.Body.String(), "\n") {
		if valStr, ok := strings.CutPrefix(strings.TrimSpace(line), prefix); ok {
			val, err := strconv.ParseFloat(strings.TrimSpace(valStr), 64)
			require.NoError(t, err)
			return val
		}
	}
	return 0
}

type mockStartupBus struct {
	pubsub.Bus

	ensureErr error
	closed    bool
}

func (m *mockStartupBus) EnsureCoreStreams(context.Context) error { return m.ensureErr }
func (m *mockStartupBus) Close() error {
	m.closed = true
	return nil
}

func TestRunApp_FailFast(t *testing.T) {
	// Missing Postgres URL
	err := RunApp(context.Background(), Config{PostgresURL: "", NATSURL: "nats://localhost:4222"})
	assert.ErrorContains(t, err, "postgres_url is required")

	// Invalid Postgres URL
	err = RunApp(context.Background(), Config{PostgresURL: "invalid://url", NATSURL: "nats://localhost:4222"})
	assert.ErrorContains(t, err, "connect to postgres")

	// Unreachable NATS via default newBusFn fails fast (RetryOnFailedConnect(false))
	_, natsDialErr := newBusFn(context.Background(), "nats://127.0.0.1:1")
	assert.Error(t, natsDialErr)

	// Missing NATS URL
	oldInitPg := initPostgres
	initPostgres = func(context.Context, string) (*PostgresVanadisStore, func(context.Context), error) {
		return &PostgresVanadisStore{}, func(context.Context) {}, nil
	}
	defer func() { initPostgres = oldInitPg }()

	err = RunApp(context.Background(), Config{PostgresURL: "postgres://mock", NATSURL: ""})
	assert.ErrorContains(t, err, "nats_url is required")

	// NATS connect failure
	oldNewBus := newBusFn
	newBusFn = func(context.Context, string) (pubsub.Bus, error) {
		return nil, errors.New("nats down")
	}
	defer func() { newBusFn = oldNewBus }()

	err = RunApp(context.Background(), Config{PostgresURL: "postgres://mock", NATSURL: "nats://mock"})
	assert.ErrorContains(t, err, "connect to nats")

	// NATS JetStream verification failure closes bus and fails fast
	startupBus := &mockStartupBus{ensureErr: errors.New("jetstream unavailable")}
	newBusFn = func(context.Context, string) (pubsub.Bus, error) {
		return startupBus, nil
	}
	err = RunApp(context.Background(), Config{PostgresURL: "postgres://mock", NATSURL: "nats://mock"})
	assert.ErrorContains(t, err, "verify nats jetstream")
	assert.True(t, startupBus.closed)
}

func TestReconcileErrorsMetric(t *testing.T) {
	t.Parallel()
	rec := httptest.NewRecorder()
	SetupHealthMux().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody))
	metricsBody := rec.Body.String()
	for _, label := range reconcileErrorHandlerLabels {
		assert.Contains(t, metricsBody, fmt.Sprintf(`vanadis_restate_reconcile_errors_total{handler=%q} `, label))
	}

	before := reconcileErrorsCount(t, "test-handler")
	recordReconcileError("test-handler", nil)
	assert.Equal(t, before, reconcileErrorsCount(t, "test-handler"))
	recordReconcileError("test-handler", errors.New("err"))
	assert.Equal(t, before+1, reconcileErrorsCount(t, "test-handler"))
}

type checkStatusSpyStore struct {
	VanadisStore

	lifecycleStatus string
	getBuildErr     error
	chunks          []VanadisChunk
	calls           []string
	resets          []string
	failed          []string
	banners         []string
	err             error
	resetErr        error
	statusErr       error
	bannerErr       error
	failErr         error
	skipFailApply   bool
}

func (s *checkStatusSpyStore) GetBuild(_ context.Context, buildID string) (*VanadisBuild, []VanadisChunk, error) {
	return &VanadisBuild{ID: buildID, Status: statusRunning, ThreadID: "th-1"}, s.chunks, s.getBuildErr
}

func (s *checkStatusSpyStore) RecordPRCheckStatus(_ context.Context, chunkID, headSHA, checkName, checkState string) error {
	s.calls = append(s.calls, chunkID+":"+headSHA+":"+checkName+":"+checkState)
	return s.err
}

func (s *checkStatusSpyStore) ResetPRCheckState(_ context.Context, chunkID, headSHA, mergeSHA string) error {
	s.resets = append(s.resets, chunkID+":"+headSHA+":"+mergeSHA)
	return s.resetErr
}

func (s *checkStatusSpyStore) UpdatePRLifecycleStatus(_ context.Context, _ string, status string) error {
	s.lifecycleStatus = status
	return s.statusErr
}

func TestBuildObject_MergeRevisionAfterLifecycle(t *testing.T) {
	t.Parallel()
	unavailable := errors.New("reset unavailable")
	tests := []struct {
		name, mergeSHA    string
		resetErr, wantErr error
	}{
		{"missing optional merge SHA", "", errPRTransitionSkipped, nil},
		{"rejected merge reset reloads lifecycle", "merge-event", errPRTransitionSkipped, errPRTransitionSkipped},
		{"failed merge reset retries", "merge-event", unavailable, unavailable},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := &checkStatusSpyStore{resetErr: tc.resetErr, chunks: []VanadisChunk{{ID: "c1", BuildID: "b1", Status: "merged", HeadSHA: "head"}}}
			bo := NewBuildObject("", nil).WithAuthority(store, nil)
			bo.setChunkRevision("c1", prRevisionState{headSHA: "head"})
			ctx := mockRunContext{Context: context.Background()}
			err := bo.handlePREventType(ctx, "b1", "c1", "merged", pubsub.PRLifecycleEvent{HeadSHA: "head", MergeSHA: tc.mergeSHA})
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("merge error=%v want=%v", err, tc.wantErr)
			}
			if err := bo.handlePREventType(ctx, "b1", "c1", "check_updated", pubsub.PRLifecycleEvent{HeadSHA: "merge-actual", CheckName: "build", CheckState: "passed"}); err != nil {
				t.Fatalf("merge check rejected: %v", err)
			}
			if err := bo.handlePREventType(ctx, "b1", "c1", "check_updated", pubsub.PRLifecycleEvent{HeadSHA: "head", CheckName: "lint", CheckState: "passed"}); !errors.Is(err, errPRTransitionSkipped) {
				t.Fatalf("head check error=%v", err)
			}
		})
	}
}

func TestBuildObject_RejectedCheckFailure(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		err  error
	}{{"skipped write", errPRTransitionSkipped}, {"failed write", errors.New("database unavailable")}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := &checkStatusSpyStore{err: tc.err, getBuildErr: errors.New("read unavailable")}
			bo := NewBuildObject("", nil).WithAuthority(store, nil)
			err := bo.handlePREventType(mockRunContext{Context: context.Background()}, "b1", "c1", "check_failed", pubsub.PRLifecycleEvent{HeadSHA: "new", CheckName: "lint"})
			if !errors.Is(err, tc.err) || store.lifecycleStatus != "" {
				t.Fatalf("error=%v lifecycle=%s", err, store.lifecycleStatus)
			}
		})
	}
}

func (s *checkStatusSpyStore) FailChunk(_ context.Context, chunkID, reason, _ string) (*VanadisBuild, *VanadisChunk, bool, error) {
	s.failed = append(s.failed, chunkID+":"+reason)
	if s.failErr != nil {
		return nil, nil, false, s.failErr
	}
	return &VanadisBuild{ID: "b1", Status: statusFailed, Objective: "Obj", ThreadID: "th-1"}, &VanadisChunk{ID: chunkID, BuildID: "b1", Status: statusFailed}, !s.skipFailApply, nil
}

func (s *checkStatusSpyStore) UpdateChunkBannerAndStatus(_ context.Context, chunkID, status, banner string) error {
	s.banners = append(s.banners, chunkID+":"+status+":"+banner)
	return s.bannerErr
}

func (*checkStatusSpyStore) UpdateBuildStatusMessageID(_ context.Context, _, _ string) error {
	return nil
}

func TestBuildObject_HandlePRCheckEvent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	spy := &checkStatusSpyStore{}
	var currentHead atomic.Value
	currentHead.Store("sha-2")
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(w, `{"state":"open","merged":false,"head":{"sha":%q}}`, currentHead.Load())
	}))
	defer api.Close()
	executor := NewExecutor("", nil, nil, "", api.Client())
	executor.gitHubAPIURL = api.URL
	bo := NewBuildObject("", nil).WithAuthority(spy, executor)

	// Empty check name is ignored
	assert.NoError(t, bo.handlePRCheckEvent(ctx, "c1", "check_updated", pubsub.PRLifecycleEvent{CheckName: ""}))
	assert.Empty(t, spy.calls)

	// Empty check state on check_updated is ignored
	assert.NoError(t, bo.handlePRCheckEvent(ctx, "c1", "check_updated", pubsub.PRLifecycleEvent{CheckName: "test", CheckState: ""}))
	assert.Empty(t, spy.calls)

	// check_updated with state records exact state on initial head sha-1
	assert.NoError(t, bo.handlePRCheckEvent(ctx, "c1", "check_updated", pubsub.PRLifecycleEvent{HeadSHA: "sha-1", CheckName: "test", CheckState: "running"}))
	assert.Equal(t, []string{"c1:sha-1:test:running"}, spy.calls)

	// check_failed defaults empty state to failed on matching head sha-1
	spy.calls = nil
	assert.NoError(t, bo.handlePRCheckEvent(ctx, "c1", "check_failed", pubsub.PRLifecycleEvent{HeadSHA: "sha-1", CheckName: "test"}))
	assert.Equal(t, []string{"c1:sha-1:test:failed"}, spy.calls)

	// synchronize advances tracked revision to sha-2 and resets check state
	t2 := time.Date(2026, 11, 16, 23, 0, 0, 0, time.UTC)
	assert.NoError(t, bo.handlePREventType(mockRunContext{Context: ctx}, "b1", "c1", "synchronize", pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 12, HeadSHA: "sha-2", OccurredAt: t2}))
	assert.Equal(t, []string{"c1:sha-2:"}, spy.resets)

	// delayed synchronize for superseded sha-1 or older timestamp is rejected without mutating revision
	spy.resets = nil
	assert.ErrorIs(t, bo.handlePREventType(mockRunContext{Context: ctx}, "b1", "c1", "synchronize", pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 12, HeadSHA: "sha-1"}), errPRTransitionSkipped)
	assert.ErrorIs(t, bo.handlePREventType(mockRunContext{Context: ctx}, "b1", "c1", "synchronize", pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 12, HeadSHA: "sha-0", OccurredAt: t2.Add(-time.Minute)}), errPRTransitionSkipped)
	assert.Empty(t, spy.resets)

	// delayed check_updated and check_failed from old head sha-1 are rejected
	spy.calls = nil
	assert.ErrorIs(t, bo.handlePRCheckEvent(ctx, "c1", "check_updated", pubsub.PRLifecycleEvent{HeadSHA: "sha-1", CheckName: "test", CheckState: "pending"}), errPRTransitionSkipped)
	assert.ErrorIs(t, bo.handlePREventType(mockRunContext{Context: ctx}, "b1", "c1", "ci_failed", pubsub.PRLifecycleEvent{HeadSHA: "sha-1", CheckName: "test"}), errPRTransitionSkipped)
	assert.Empty(t, spy.calls)

	// check_updated for new head sha-2 succeeds
	assert.NoError(t, bo.handlePRCheckEvent(ctx, "c1", "check_updated", pubsub.PRLifecycleEvent{HeadSHA: "sha-2", CheckName: "test", CheckState: "passed"}))
	assert.Equal(t, []string{"c1:sha-2:test:passed"}, spy.calls)

	// legitimate newer force-push returning branch to previous head sha-1 (with newer OccurredAt) is accepted
	spy.resets = nil
	spy.calls = nil
	t3 := t2.Add(time.Minute)
	currentHead.Store("sha-1")
	assert.NoError(t, bo.handlePREventType(mockRunContext{Context: ctx}, "b1", "c1", "synchronize", pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 12, HeadSHA: "sha-1", OccurredAt: t3}))
	assert.Equal(t, []string{"c1:sha-1:"}, spy.resets)
	assert.NoError(t, bo.handlePRCheckEvent(ctx, "c1", "check_updated", pubsub.PRLifecycleEvent{HeadSHA: "sha-1", CheckName: "test", CheckState: "passed"}))
	assert.Equal(t, []string{"c1:sha-1:test:passed"}, spy.calls)

	// advance back to sha-2 with newer timestamp t4
	spy.resets = nil
	t4 := t3.Add(time.Minute)
	currentHead.Store("sha-2")
	assert.NoError(t, bo.handlePREventType(mockRunContext{Context: ctx}, "b1", "c1", "synchronize", pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 12, HeadSHA: "sha-2", OccurredAt: t4}))
	assert.Equal(t, []string{"c1:sha-2:"}, spy.resets)

	// untimestamped synchronize on c2 clears headAt so delayed sha-a with older timestamp is rejected
	currentHead.Store("sha-a")
	assert.NoError(t, bo.handlePREventType(mockRunContext{Context: ctx}, "b1", "c2", "synchronize", pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 12, HeadSHA: "sha-a", OccurredAt: t2}))
	currentHead.Store("sha-b")
	assert.NoError(t, bo.handlePREventType(mockRunContext{Context: ctx}, "b1", "c2", "synchronize", pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 12, HeadSHA: "sha-b"}))
	assert.ErrorIs(t, bo.handlePREventType(mockRunContext{Context: ctx}, "b1", "c2", "synchronize", pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 12, HeadSHA: "sha-a", OccurredAt: t3}), errPRTransitionSkipped)
	assert.True(t, prHeadAtFromContext(nil).IsZero())
	assert.True(t, prHeadAtFromContext(context.Background()).IsZero())
	assert.Nil(t, withPRHeadAt(nil, time.Time{}))
	assert.Equal(t, t3.UTC(), prHeadAtFromContext(withPRHeadAt(nil, t3)))
	var parsedChunk VanadisChunk
	applyRawChunkCIChecks(&parsedChunk, []byte(`{"_head_sha":"sha-1","_merge_sha":"m-1","_head_at":"1700000000000000000","_seen_heads":"sha-0,sha-1","lint":"passed"}`))
	assert.Equal(t, "sha-1", parsedChunk.HeadSHA)
	assert.Equal(t, []string{"sha-0", "sha-1"}, parsedChunk.SeenHeads)
	assert.False(t, parsedChunk.HeadAt.IsZero())
	assert.Equal(t, map[string]string{"lint": "passed"}, parsedChunk.CIChecks)
	spy.resets = nil

	// failed or skipped ResetPRCheckState on synchronize does not overwrite cached revision
	currentHead.Store("sha-unapplied")
	spy.resetErr = errPRTransitionSkipped
	assert.ErrorIs(t, bo.handlePREventType(mockRunContext{Context: ctx}, "b1", "c1", "synchronize", pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 12, HeadSHA: "sha-unapplied"}), errPRTransitionSkipped)
	spy.resetErr = nil
	spy.calls = nil
	assert.NoError(t, bo.handlePRCheckEvent(ctx, "c1", "check_updated", pubsub.PRLifecycleEvent{HeadSHA: "sha-2", CheckName: "test", CheckState: "passed"}))
	assert.Equal(t, []string{"c1:sha-2:test:passed"}, spy.calls)

	// merged resets check state for merge commit and rejects delayed pre-merge head sha-2 synchronize and checks while accepting merge-sha-3 checks
	spy.calls = nil
	spy.resets = nil
	assert.NoError(t, bo.handlePREventType(mockRunContext{Context: ctx}, "b1", "c1", "merged", pubsub.PRLifecycleEvent{HeadSHA: "sha-2", MergeSHA: "merge-sha-3"}))
	assert.Equal(t, []string{"c1:sha-2:merge-sha-3"}, spy.resets)
	assert.ErrorIs(t, bo.handlePREventType(mockRunContext{Context: ctx}, "b1", "c1", "synchronize", pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 12, HeadSHA: "sha-4"}), errPRTransitionSkipped)
	assert.ErrorIs(t, bo.handlePRCheckEvent(ctx, "c1", "check_updated", pubsub.PRLifecycleEvent{HeadSHA: "sha-2", CheckName: "test", CheckState: "pending"}), errPRTransitionSkipped)
	assert.Empty(t, spy.calls)
	assert.NoError(t, bo.handlePRCheckEvent(ctx, "c1", "check_updated", pubsub.PRLifecycleEvent{HeadSHA: "merge-sha-3", CheckName: "Build & Push Images (eri)", CheckState: "passed"}))
	assert.Equal(t, []string{"c1:merge-sha-3:Build & Push Images (eri):passed"}, spy.calls)

	// store error on recordPRMergeRevision prevents cached revision overwrite; store error on check_updated propagates
	spyErr := &checkStatusSpyStore{err: errors.New("db error"), resetErr: errors.New("reset err")}
	boErr := NewBuildObject("", nil).WithAuthority(spyErr, nil)
	assert.Error(t, boErr.recordPRMergeRevision(ctx, "b1", "c1", "sha-2", "merge-sha-3"))
	assert.Error(t, boErr.handlePRCheckEvent(ctx, "c1", "check_updated", pubsub.PRLifecycleEvent{HeadSHA: "merge-sha-3", CheckName: "test", CheckState: "passed"}))
	assert.Equal(t, []string{"c1:merge-sha-3:test:passed"}, spyErr.calls)

	// restart after deploy_failed reconstructs merged revision state and rejects delayed pre-merge checks
	spyRestart := &checkStatusSpyStore{
		chunks: []VanadisChunk{
			{ID: "c-df-1", BuildID: "b1", Status: statusDeployFailed, HeadSHA: "sha-pre", MergeSHA: "sha-merge"},
			{ID: "c-df-2", BuildID: "b1", Status: statusDeployFailed, HeadSHA: "sha-pre"},
		},
	}
	boRestart := NewBuildObject("", nil).WithAuthority(spyRestart, nil)
	assert.ErrorIs(t, boRestart.handlePREventType(nil, "b1", "c-df-1", "ci_failed", pubsub.PRLifecycleEvent{HeadSHA: "sha-pre", CheckName: "test"}), errPRTransitionSkipped)
	assert.ErrorIs(t, boRestart.handlePREventType(nil, "b1", "c-df-2", "ci_failed", pubsub.PRLifecycleEvent{HeadSHA: "sha-pre", CheckName: "test"}), errPRTransitionSkipped)
	assert.NoError(t, boRestart.handlePREventType(nil, "b1", "c-df-2", "check_updated", pubsub.PRLifecycleEvent{HeadSHA: "sha-post-merge", CheckName: "test", CheckState: "passed"}))
	assert.Equal(t, []string{"c-df-2:sha-post-merge:test:passed"}, spyRestart.calls)
	// Rejected post-merge check writes must not pin an unaccepted merge SHA.
	mergedStore := &checkStatusSpyStore{err: errPRTransitionSkipped}
	mergedObject := NewBuildObject("", nil).WithAuthority(mergedStore, nil)
	mergedObject.setChunkRevision("c1", prRevisionState{headSHA: "head", merged: true})
	assert.ErrorIs(t, mergedObject.handlePRCheckEvent(ctx, "c1", "check_updated", pubsub.PRLifecycleEvent{HeadSHA: "rejected-merge", CheckName: "build", CheckState: "passed"}), errPRTransitionSkipped)
	mergedStore.err = nil
	assert.NoError(t, mergedObject.handlePRCheckEvent(ctx, "c1", "check_updated", pubsub.PRLifecycleEvent{HeadSHA: "accepted-merge", CheckName: "build", CheckState: "passed"}))
}

type mockBusError struct {
	pubsub.Bus

	err error
}

func (m *mockBusError) Publish(context.Context, string, string, any) error {
	return m.err
}

func TestBuildObject_ExecutorErrorBranches(t *testing.T) {
	t.Parallel()
	b1 := VanadisBuild{ID: "b-err", Status: "running", ThreadID: "th-1"}
	c1 := VanadisChunk{ID: "c1", BuildID: "b-err", ChunkIndex: 0, Title: "C1", Status: "running", Mode: "action"}
	store, _ := dbScript{
		buildRow: &mockRow{b: &b1},
		chunks:   []VanadisChunk{c1},
		execTag:  "UPDATE 1",
	}.open()
	eriErr := &mockEriClient{
		cancelFn:  func(context.Context, string) error { return errors.New("cancel err") },
		promptFn:  func(context.Context, string, string) error { return errors.New("prompt err") },
		widgetFn:  func(context.Context, StatusWidgetRequest) (string, error) { return "", errors.New("widget err") },
		messageFn: func(context.Context, string, string) error { return errors.New("msg err") },
	}
	mockBusErr := &mockBusError{err: errors.New("pub err")}
	execErr := NewExecutor(t.TempDir(), eriErr, mockBusErr, "token", nil)
	boErr := NewBuildObject("", nil).WithAuthority(store, execErr)

	// notifyBuildTerminal error branches
	boErr.notifyBuildTerminal(context.Background(), &b1, []VanadisChunk{c1}, "failed", "chunk failed", "TITLE", "MSG")

	// notifyChunkFailure error branches
	ctx := makeObjectMockContext(t, "b-err")
	_, _ = restate.Run(ctx, func(rc restate.RunContext) (bool, error) {
		boErr.notifyChunkFailure(rc, b1, "c1", "reason")
		return true, nil
	})

	// dispatchThreadWakePrompt error branches
	_, _ = restate.Run(ctx, func(rc restate.RunContext) (bool, error) {
		boErr.dispatchThreadWakePrompt(rc, "b-err", "prompt")
		return true, nil
	})

	// persistBuildCreation error branches
	_, _ = restate.Run(ctx, func(rc restate.RunContext) (bool, error) {
		_, _ = boErr.persistBuildCreation(rc, b1, []VanadisChunk{c1})
		return true, nil
	})

	// handlePREventType error branches
	_, _ = restate.Run(ctx, func(rc restate.RunContext) (bool, error) {
		boErr.handlePREventType(rc, "b-err", "c1", "check_failed", pubsub.PRLifecycleEvent{Event: "check_failed", Repo: "aerial"})
		boErr.handlePREventType(rc, "b-err", "c1", "conflict", pubsub.PRLifecycleEvent{Event: "conflict"})
		boErr.handlePREventType(rc, "b-err", "c1", "merged", pubsub.PRLifecycleEvent{Event: "merged"})
		return true, nil
	})

	storeErr, _ := dbScript{bare: nilDB}.open()
	boStoreErr := NewBuildObject("", nil).WithAuthority(storeErr, execErr)
	for _, act := range []string{"chunk_merged", "chunk_deploy_success", "action_completed", "chunk_failed", "cancelled"} {
		boStoreErr.applyMutation(context.Background(), "b-err", &BuildMutationRequest{Action: act, ChunkID: "c1", Reason: "test"})
	}
	_, _ = boStoreErr.AttachPR(ctx, AttachChunkPRRequest{ChunkID: "c1", Repo: "aerial"})
	_, _ = boErr.AttachPR(ctx, AttachChunkPRRequest{ChunkID: "c1", Repo: "aerial"})
	_, _ = restate.Run(ctx, func(rc restate.RunContext) (bool, error) {
		boStoreErr.dispatchThreadWakePrompt(rc, "b-err", "prompt")
		return true, nil
	})
	_ = boStoreErr.OnPREvent(ctx, pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 101, Event: "check_failed", RolloutState: &pubsub.PRRolloutState{}})
}

func TestBuildObject_HandlePREventType_Parity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		event         string
		prEvent       pubsub.PRLifecycleEvent
		bannerErr     error
		failErr       error
		skipFailApply bool
		wantErr       bool
		wantFailed    []string
		wantBanners   []string
		wantPromptIn  string
	}{
		{
			name:         "closed unmerged fails chunk and dispatches failure prompt",
			event:        "closed",
			prEvent:      pubsub.PRLifecycleEvent{Event: "closed", Status: "closed"},
			wantFailed:   []string{"c1:pr_closed_unmerged"},
			wantPromptIn: "[VANADIS BUILD FAILED]",
		},
		{
			name:       "closed with status merged skips fail chunk",
			event:      "closed",
			prEvent:    pubsub.PRLifecycleEvent{Event: "closed", Status: "merged"},
			wantFailed: nil,
		},
		{
			name:         "closed_unmerged event fails chunk and dispatches failure prompt",
			event:        "closed_unmerged",
			prEvent:      pubsub.PRLifecycleEvent{Event: "closed_unmerged"},
			wantFailed:   []string{"c1:pr_closed_unmerged"},
			wantPromptIn: "[VANADIS BUILD FAILED]",
		},
		{
			name:          "closed_unmerged already terminal skips notifications",
			event:         "closed_unmerged",
			prEvent:       pubsub.PRLifecycleEvent{Event: "closed_unmerged"},
			skipFailApply: true,
			wantFailed:    []string{"c1:pr_closed_unmerged"},
		},
		{
			name:       "closed_unmerged fail chunk error propagates",
			event:      "closed_unmerged",
			prEvent:    pubsub.PRLifecycleEvent{Event: "closed_unmerged"},
			failErr:    errors.New("db fail error"),
			wantErr:    true,
			wantFailed: []string{"c1:pr_closed_unmerged"},
		},
		{
			name:         "deploy_failed sets status banner and dispatches wake prompt",
			event:        "deploy_failed",
			prEvent:      pubsub.PRLifecycleEvent{Event: "deploy_failed", JobName: "eri", Details: "crashloop", PRNumber: 101, Repo: "aerial"},
			wantBanners:  []string{"c1:deploy_failed:Deployment failed for eri: crashloop"},
			wantPromptIn: "Deployment failed for job eri on aerial (PR #101",
		},
		{
			name:         "sync_failed sets status banner and dispatches wake prompt",
			event:        "sync_failed",
			prEvent:      pubsub.PRLifecycleEvent{Event: "sync_failed", Details: "git pull conflict", PRNumber: 102, Repo: "aerial"},
			wantBanners:  []string{"c1:deploy_failed:Git sync failed: git pull conflict"},
			wantPromptIn: "Git sync failed for PR #102 on aerial",
		},
		{
			name:        "deploy_failed banner update error propagates",
			event:       "deploy_failed",
			prEvent:     pubsub.PRLifecycleEvent{Event: "deploy_failed", JobName: "synapse"},
			bannerErr:   errors.New("db banner error"),
			wantErr:     true,
			wantBanners: []string{"c1:deploy_failed:Deployment failed for synapse"},
		},
		{
			name:        "deploy_failed on completed chunk skips wake prompt",
			event:       "deploy_failed",
			prEvent:     pubsub.PRLifecycleEvent{Event: "deploy_failed", JobName: "synapse"},
			bannerErr:   errPRTransitionSkipped,
			wantErr:     true,
			wantBanners: []string{"c1:deploy_failed:Deployment failed for synapse"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spy := &checkStatusSpyStore{bannerErr: tc.bannerErr, failErr: tc.failErr, skipFailApply: tc.skipFailApply}
			eri := &mockEriClient{}
			exec := NewExecutor(t.TempDir(), eri, nil, "token", nil)
			bo := NewBuildObject("", nil).WithAuthority(spy, exec)

			err := bo.handlePREventType(nil, "b1", "c1", tc.event, tc.prEvent)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tc.wantFailed, spy.failed)
			assert.Equal(t, tc.wantBanners, spy.banners)
			if tc.wantPromptIn != "" {
				assert.Contains(t, eri.lastPrompt, tc.wantPromptIn)
				assert.Equal(t, "th-1", eri.lastThreadID)
			} else {
				assert.Empty(t, eri.lastPrompt)
			}
		})
	}
}
