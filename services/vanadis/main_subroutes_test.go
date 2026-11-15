package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMain_PromptFormattingAndSettleHelpers(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// 1. settleFailedChunk
	msgTerm := &memoryMsg{numDelivered: 3}
	settleFailedChunk(msgTerm, true)
	if !msgTerm.termed {
		t.Error("expected msg to be termed on final attempt")
	}

	msgNak := &memoryMsg{numDelivered: 1}
	settleFailedChunk(msgNak, false)
	if msgNak.nakDelay == 0 {
		t.Error("expected msg to be nake-delayed on non-final attempt")
	}

	// 2. formatBuildCompletedWakePrompt & isShutdownInterruptionReason
	if prompt := formatBuildCompletedWakePrompt(nil); prompt != "" {
		t.Errorf("expected empty prompt for nil, got %q", prompt)
	}

	res := &VanadisAdvancementResult{
		BuildID:   "b1",
		Tier:      2,
		Objective: "Test Objective",
		Chunks: []VanadisChunkSummary{
			{ChunkIndex: 0, Title: "Chunk 1", Repo: "azylman/aerial", PRNumber: 123},
			{ChunkIndex: 1, Title: "Chunk 2", Repo: "action"},
		},
	}
	prompt := formatBuildCompletedWakePrompt(res)
	if !strings.Contains(prompt, "Chunk 1 (azylman/aerial, PR #123)") {
		t.Errorf("unexpected prompt output: %s", prompt)
	}

	if !isShutdownInterruptionReason("interrupted by shutdown") {
		t.Error("expected true for shutdown reason")
	}
	if isShutdownInterruptionReason("other failure") {
		t.Error("expected false for other reason")
	}

	// 3. postBrainJSON & dispatchWakePrompt
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/fail" {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("internal error"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer ts.Close()

	srv := &VanadisServer{
		cfg:        Config{BrainURL: ts.URL},
		httpClient: ts.Client(),
	}

	// empty inputs
	if err := srv.dispatchWakePrompt(ctx, "", "prompt"); err != nil {
		t.Error("expected nil on empty threadID")
	}
	if err := srv.dispatchWakePrompt(ctx, "th1", ""); err != nil {
		t.Error("expected nil on empty prompt")
	}

	// success
	if err := srv.dispatchWakePrompt(ctx, "th1", "hello"); err != nil {
		t.Errorf("expected success on dispatchWakePrompt, got %v", err)
	}

	// error response from brain
	_, err := srv.postBrainJSON(ctx, "/fail", map[string]string{"foo": "bar"}, 1024)
	if err == nil {
		t.Error("expected error on 500 status from postBrainJSON")
	}

	// empty BrainURL
	emptySrv := &VanadisServer{httpClient: ts.Client()}
	if _, err := emptySrv.postBrainJSON(ctx, "/test", nil, 1024); err == nil {
		t.Error("expected error on empty brain URL")
	}
}

func TestInitPostgres(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	srv := &VanadisServer{}

	// invalid postgres URL
	_, err := initPostgres(ctx, srv, ":::invalid-url")
	if err == nil {
		t.Fatal("expected error on invalid postgres url")
	}
}

func TestVanadisServer_StoreHTTPEndpointErrors(t *testing.T) {
	t.Parallel()

	sErr := newMemoryVanadisStore()
	sErr.getErr = errors.New("simulated get err")
	sErr.updateChunkErr = errors.New("simulated update err")
	sErr.updateBuildErr = errors.New("simulated build err")

	srv := &VanadisServer{store: sErr}
	mux := srv.Routes()

	doReq := func(method, path, body string) int {
		var r *http.Request
		if body != "" {
			r = httptest.NewRequest(method, path, strings.NewReader(body))
		} else {
			r = httptest.NewRequest(method, path, nil)
		}
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, r)
		return rec.Code
	}

	if code := doReq(http.MethodPost, "/chunks/attach-pr", `{"repo":"aerial","pr_number":101}`); code != http.StatusInternalServerError {
		t.Errorf("expected 500 on AttachChunkPR err, got %d", code)
	}
	if code := doReq(http.MethodGet, "/chunks/c1", ""); code != http.StatusInternalServerError {
		t.Errorf("expected 500 on GetChunk err, got %d", code)
	}
	if code := doReq(http.MethodGet, "/builds/b1", ""); code != http.StatusInternalServerError {
		t.Errorf("expected 500 on GetBuild err, got %d", code)
	}
	if code := doReq(http.MethodPost, "/builds/b1/cancel", ""); code != http.StatusInternalServerError {
		t.Errorf("expected 500 on CancelBuild err, got %d", code)
	}
}

func TestVanadisServer_ExecuteAndDispatchPlumbing(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	var nilProcessSrv *VanadisServer
	if err := nilProcessSrv.ProcessExecuteChunk(ctx, "b1", 0, false); err == nil {
		t.Fatal("expected error on nil server ProcessExecuteChunk")
	}
	memStore := newMemoryVanadisStore()
	srvProc := &VanadisServer{store: memStore}
	if err := srvProc.ProcessExecuteChunk(ctx, "b-none", 0, false); err != nil {
		t.Fatalf("expected nil on missing build in ProcessExecuteChunk, got %v", err)
	}
	bProc := VanadisBuild{ID: "b-p", Status: "running"}
	cDone := VanadisChunk{ID: "c-done", BuildID: "b-p", ChunkIndex: 0, Status: "completed"}
	_ = memStore.CreateBuild(ctx, bProc, []VanadisChunk{cDone})
	if err := srvProc.ProcessExecuteChunk(ctx, "b-p", 99, false); err == nil {
		t.Fatal("expected error on out of range chunk index")
	}
	if err := srvProc.ProcessExecuteChunk(ctx, "b-p", 0, false); err != nil {
		t.Fatalf("expected nil on completed chunk, got %v", err)
	}

	badMsg := &memoryMsg{data: []byte("bad json")}
	if err := srvProc.HandleExecuteChunkJob(ctx, badMsg); err != nil {
		t.Fatalf("expected nil on unmarshal error, got %v", err)
	}
}
