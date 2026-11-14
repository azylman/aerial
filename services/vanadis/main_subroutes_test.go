package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/nats-io/nats.go"
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
	if !strings.Contains(prompt, "Chunk 1 (azylman/aerial, PR #123)") || !strings.Contains(prompt, "Directive:") {
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

func TestInitVanadisJetStreamWithFactory(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	srv := &VanadisServer{}

	// empty nats URL -> no-op cleanup, nil error
	cleanup, err := initVanadisJetStreamWithFactory(ctx, srv, "", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	cleanup()

	// factory returns error
	_, err = initVanadisJetStreamWithFactory(ctx, srv, "nats://bad", func(ctx context.Context, url string) (pubsub.Bus, error) {
		return nil, errors.New("conn err")
	})
	if err == nil {
		t.Fatal("expected error on factory error")
	}

	// factory returns closed bus -> EnsureCoreStreams error
	_, err = initVanadisJetStreamWithFactory(ctx, srv, "nats://closed", func(ctx context.Context, url string) (pubsub.Bus, error) {
		b := pubsub.NewMemoryBus()
		_ = b.Close()
		return b, nil
	})
	if err == nil {
		t.Fatal("expected error on closed bus")
	}

	// successful initialization
	cleanupSuccess, err := initVanadisJetStreamWithFactory(ctx, srv, "nats://ok", func(ctx context.Context, url string) (pubsub.Bus, error) {
		return pubsub.NewMemoryBus(), nil
	})
	if err != nil {
		t.Fatalf("unexpected error on success: %v", err)
	}
	cleanupSuccess()
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

func TestHandleListBuilds_And_SubRoutes(t *testing.T) {
	t.Parallel()

	// store is nil
	nilSrv := &VanadisServer{}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/builds", nil)
	nilSrv.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}

	// bad request on /builds/ without id
	store := newMemoryVanadisStore()
	srv := &VanadisServer{store: store}
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodDelete, "/builds/", nil)
	srv.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}

	// subroutes error handling
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/builds/b1/unknown", nil)
	srv.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", rec.Code)
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/builds/b1/chunks", nil)
	srv.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", rec.Code)
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/builds/b1/other/sub", nil)
	srv.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", rec.Code)
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/builds/b1/chunks/c1/too/deep", nil)
	srv.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", rec.Code)
	}
}

func TestVanadisServer_BuildAndChunk_Subroutes_Extended(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	store := newMemoryVanadisStore()
	b1 := VanadisBuild{ID: "b-ext", ThreadID: "th-1", Objective: "test ext", Status: "running"}
	c1 := VanadisChunk{ID: "c-ext-1", BuildID: "b-ext", ChunkIndex: 0, Mode: "pr", Repo: "azylman/aerial", Title: "C1", Status: "pending"}
	c2 := VanadisChunk{ID: "c-ext-2", BuildID: "b-ext", ChunkIndex: 1, Mode: "action", Title: "C2", Status: "pending"}
	_ = store.CreateBuild(ctx, b1, []VanadisChunk{c1, c2})

	srv := &VanadisServer{store: store}
	mux := srv.Routes()

	// 1. handleChunkByID
	// nil store
	srvNil := &VanadisServer{}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/chunks/c-ext-1", nil)
	srvNil.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 on nil store, got %d", rec.Code)
	}

	// method not allowed
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/chunks/c-ext-1", nil)
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405 on POST /chunks/c-ext-1, got %d", rec.Code)
	}

	// empty chunk ID
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/chunks/", nil)
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 on GET /chunks/, got %d", rec.Code)
	}

	// not found
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/chunks/nonexistent", nil)
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404 on nonexistent chunk, got %d", rec.Code)
	}

	// success
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/chunks/c-ext-1", nil)
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 on valid chunk, got %d", rec.Code)
	}

	// 2. handleBuildRoot
	// method not allowed (DELETE on /builds/b-ext)
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodDelete, "/builds/b-ext", nil)
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405 on DELETE /builds/b-ext, got %d", rec.Code)
	}

	// GET build success
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/builds/b-ext", nil)
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 on GET /builds/b-ext, got %d", rec.Code)
	}

	// GET build not found
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/builds/b-none", nil)
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404 on GET /builds/b-none, got %d", rec.Code)
	}

	// 3. handleBuildCancel
	// non-POST
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/builds/b-ext/cancel", nil)
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405 on GET /builds/b-ext/cancel, got %d", rec.Code)
	}

	// cancel running build
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/builds/b-ext/cancel", nil)
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 on POST /builds/b-ext/cancel, got %d: %s", rec.Code, rec.Body.String())
	}

	// conflict on cancelling completed build
	bTerm := VanadisBuild{ID: "b-term", ThreadID: "th-1", Objective: "term", Status: "completed"}
	_ = store.CreateBuild(ctx, bTerm, []VanadisChunk{{ID: "c-term", BuildID: "b-term", Title: "ct"}})
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/builds/b-term/cancel", nil)
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusConflict {
		t.Errorf("expected 409 on cancelling completed build, got %d", rec.Code)
	}

	// 4. handleBuildChunkRoutes
	bChunk := VanadisBuild{ID: "b-chk", ThreadID: "th-1", Objective: "chk", Status: "running"}
	cChk1 := VanadisChunk{ID: "c-chk-1", BuildID: "b-chk", ChunkIndex: 0, Mode: "pr", Repo: "azylman/aerial", Title: "Chk1", Status: "pending"}
	cChk2 := VanadisChunk{ID: "c-chk-2", BuildID: "b-chk", ChunkIndex: 1, Mode: "action", Title: "Chk2", Status: "pending"}
	_ = store.CreateBuild(ctx, bChunk, []VanadisChunk{cChk1, cChk2})

	// unsupported method
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPut, "/builds/b-chk/chunks/c-chk-1", nil)
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405 on PUT /builds/b-chk/chunks/c-chk-1, got %d", rec.Code)
	}

	// GET chunk via build route
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/builds/b-chk/chunks/c-chk-1", nil)
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 on GET /builds/b-chk/chunks/c-chk-1, got %d", rec.Code)
	}

	// PATCH chunk via build route
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPatch, "/builds/b-chk/chunks/c-chk-1", strings.NewReader(`{"title":"Updated Title"}`))
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 on PATCH /builds/b-chk/chunks/c-chk-1, got %d", rec.Code)
	}

	// DELETE chunk via build route
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodDelete, "/builds/b-chk/chunks/c-chk-2", nil)
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 on DELETE /builds/b-chk/chunks/c-chk-2, got %d: %s", rec.Code, rec.Body.String())
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

	if code := doReq(http.MethodGet, "/chunks/by-pr?repo=aerial&pr_number=101", ""); code != http.StatusInternalServerError {
		t.Errorf("expected 500 on GetChunkByPR err, got %d", code)
	}
	if code := doReq(http.MethodPost, "/chunks/attach-pr", `{"repo":"aerial","pr_number":101}`); code != http.StatusInternalServerError {
		t.Errorf("expected 500 on AttachChunkPR err, got %d", code)
	}
	if code := doReq(http.MethodPost, "/chunks/advance-pr", `{"repo":"aerial","pr_number":101}`); code != http.StatusInternalServerError {
		t.Errorf("expected 500 on AdvanceChunkPR err, got %d", code)
	}
	if code := doReq(http.MethodPost, "/chunks/fail-pr", `{"repo":"aerial","pr_number":101}`); code != http.StatusInternalServerError {
		t.Errorf("expected 500 on FailChunkPR err, got %d", code)
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

type mockMsgWithErrors struct {
	termErr error
	nakErr  error
}

func (m *mockMsgWithErrors) Data() []byte                     { return []byte("bad json") }
func (m *mockMsgWithErrors) Subject() string                  { return "sub" }
func (m *mockMsgWithErrors) Header() nats.Header              { return nil }
func (m *mockMsgWithErrors) NumDelivered() uint64             { return 1 }
func (m *mockMsgWithErrors) Ack() error                       { return nil }
func (m *mockMsgWithErrors) NakWithDelay(time.Duration) error { return m.nakErr }
func (m *mockMsgWithErrors) Term() error                      { return m.termErr }
func (m *mockMsgWithErrors) Snooze(time.Duration) error       { return nil }
func (m *mockMsgWithErrors) InProgress() error                { return nil }

func TestVanadisServer_ExecuteAndDispatchPlumbing(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// 1. settleFailedChunk
	mTermErr := &mockMsgWithErrors{termErr: errors.New("term boom")}
	settleFailedChunk(mTermErr, true)

	mNakErr := &mockMsgWithErrors{nakErr: errors.New("nak boom")}
	settleFailedChunk(mNakErr, false)

	// 2. postBrainJSON
	var nilSrv *VanadisServer
	if _, err := nilSrv.postBrainJSON(ctx, "/path", "p", 0); err == nil {
		t.Fatal("expected error on nil server postBrainJSON")
	}

	ts500 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("brain 500 error"))
	}))
	defer ts500.Close()

	srvNoURL := &VanadisServer{httpClient: ts500.Client()}
	if _, err := srvNoURL.postBrainJSON(ctx, "/path", "p", 0); err == nil {
		t.Fatal("expected error on empty brain URL")
	}

	srv500 := &VanadisServer{cfg: Config{BrainURL: ts500.URL}, httpClient: ts500.Client()}
	if _, err := srv500.postBrainJSON(ctx, "/path", "payload", 0); err == nil {
		t.Fatal("expected error on status 500 postBrainJSON")
	}

	// 3. syncBuildWidget
	var nilSyncSrv *VanadisServer
	nilSyncSrv.syncBuildWidget(ctx, "b1", "banner")

	tsSynapse := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer tsSynapse.Close()
	srvSynapse := &VanadisServer{cfg: Config{SynapseURL: tsSynapse.URL}, httpClient: tsSynapse.Client()}
	srvSynapse.syncBuildWidget(ctx, "b1", "banner")

	// 4. dispatchWakePrompt
	srv500.dispatchWakePrompt(ctx, "", "prompt")
	_ = srv500.dispatchWakePrompt(ctx, "123", "prompt")

	// 5. handleFinalDispatchFailure
	store := newMemoryVanadisStore()
	store.updateChunkErr = errors.New("chunk update err")
	store.updateBuildErr = errors.New("build update err")
	srvFinal := &VanadisServer{store: store}
	b := &VanadisBuild{ID: "b-fin"}
	c := VanadisChunk{ID: "c-fin", ChunkIndex: 0, Title: "Fin"}
	srvFinal.handleFinalDispatchFailure(ctx, b, c, 1, errors.New("dispatch fail"))

	// 6. ProcessExecuteChunk
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

	// 7. HandleExecuteChunkJob unmarshal error
	badMsg := &mockMsgWithErrors{}
	if err := srvProc.HandleExecuteChunkJob(ctx, badMsg); err != nil {
		t.Fatalf("expected nil on unmarshal error, got %v", err)
	}
}
