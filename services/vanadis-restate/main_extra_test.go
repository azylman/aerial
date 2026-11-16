package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	restate "github.com/restatedev/sdk-go"

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
