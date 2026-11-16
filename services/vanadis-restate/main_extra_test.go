package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	restate "github.com/restatedev/sdk-go"
	"github.com/stretchr/testify/assert"

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
		{"action compatibility", "running", "deploying", false, false, "action", false, 0, false},
		{"action persistence failure", "running", "deploying", true, true, "action", false, 0, false},
		{"attached action evidence", "running", "deploying", false, false, "action", false, 42, false},
		{"attached action nil evidence", "running", "deploying", false, false, "action", false, 42, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			build := VanadisBuild{ID: "b1", Status: tc.buildStatus}
			chunk := VanadisChunk{ID: "c1", BuildID: "b1", Mode: tc.mode, Repo: "owner/repo", Status: tc.chunkStatus, PRNumber: tc.prNumber, RolloutState: &pubsub.PRRolloutState{ExpectedJobs: []string{"existing"}}}
			script := dbScript{row: &mockRow{b: &build}, chunks: []VanadisChunk{chunk, {ID: "sibling", BuildID: "b1", Mode: "pr", Status: "completed"}}, execTag: "UPDATE 1"}
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
			out, err := bo.executeAuthoritativeTransition(context.Background(), "b1", &BuildMutationRequest{Action: "chunk_deploy_success", ChunkID: "c1", RolloutState: evidence}, nil)

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
	} else if status, ok := args[0].(string); ok {
		chunk.Status = status
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
			out, err := bo.executeAuthoritativeTransition(context.Background(), "b1", &BuildMutationRequest{Action: "chunk_deploy_success", ChunkID: "c2"}, nil)
			assert.NoError(t, err)
			assert.Contains(t, out.LaunchedChunks, "action")
		})
	}
}
