package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestHelpers_WidgetStatus(t *testing.T) {
	normTests := []struct {
		in   string
		want string
	}{
		{"ci_failed", "failed"},
		{"deploy_failed", "failed"},
		{"RUNNING", "running"},
		{"open", "open"},
	}
	for _, tc := range normTests {
		t.Run("normalize_"+tc.in, func(t *testing.T) {
			if got := normalizeWidgetStatus(tc.in); got != tc.want {
				t.Errorf("normalizeWidgetStatus(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}

	resolveTests := []struct {
		chunkStatus string
		prStatus    string
		want        string
	}{
		{"failed", "deployed", "failed"},
		{"pending", "deployed", "deployed"},
		{"pending", "deploying", "deploying"},
		{"pending", "failed", "failed"},
		{"pending", "conflict", "conflict"},
		{"pending", "merged", "merged"},
		{"merged", "closed", "merged"},
		{"pending", "closed", "failed"},
		{"coding", "unknown", "coding"},
		{"", "", "open"},
	}
	for _, tc := range resolveTests {
		t.Run("resolve_"+tc.chunkStatus+"_"+tc.prStatus, func(t *testing.T) {
			if got := resolveVanadisChunkWidgetStatus(tc.chunkStatus, tc.prStatus); got != tc.want {
				t.Errorf("resolveVanadisChunkWidgetStatus(%q, %q) = %q, want %q", tc.chunkStatus, tc.prStatus, got, tc.want)
			}
		})
	}
}

func setupTestStoreWithBuild() (*memoryVanadisStore, string, []string) {
	s := newMemoryVanadisStore()
	buildID := "build-test-1"
	b := mkBuild(buildID, "123456789012345678", "Test Build Title", "Test Objective", "running")
	c1 := VanadisChunk{
		ID:         "chunk-1",
		BuildID:    buildID,
		ChunkIndex: 0,
		Repo:       "azylman/aerial",
		Title:      "Chunk 1",
		Status:     "pending",
		Effort:     "low",
		CreatedAt:  time.Now().UTC(),
		UpdatedAt:  time.Now().UTC(),
	}
	c2 := VanadisChunk{
		ID:         "chunk-2",
		BuildID:    buildID,
		ChunkIndex: 1,
		Repo:       "azylman/aerial",
		Title:      "Chunk 2",
		Status:     "pending",
		Effort:     "medium",
		CreatedAt:  time.Now().UTC(),
		UpdatedAt:  time.Now().UTC(),
	}
	_ = s.CreateBuild(context.Background(), b, []VanadisChunk{c1, c2})
	return s, buildID, []string{"chunk-1", "chunk-2"}
}

func TestVanadisStore_ChunkLifecycle(t *testing.T) {
	ctx := context.Background()

	t.Run("GetChunk_Success", func(t *testing.T) {
		s, bID, cIDs := setupTestStoreWithBuild()
		c, b, err := s.GetChunk(ctx, cIDs[0])
		if err != nil || c == nil || b == nil {
			t.Fatalf("GetChunk error: %v, c=%v, b=%v", err, c, b)
		}
		if c.ID != cIDs[0] || b.ID != bID {
			t.Errorf("got c.ID=%q, b.ID=%q, want %q, %q", c.ID, b.ID, cIDs[0], bID)
		}
	})

	t.Run("GetChunk_NotFound", func(t *testing.T) {
		s, _, _ := setupTestStoreWithBuild()
		c, b, err := s.GetChunk(ctx, "nonexistent")
		if !errors.Is(err, ErrChunkNotFound) || c != nil || b != nil {
			t.Errorf("expected ErrChunkNotFound, got err=%v", err)
		}
	})

	t.Run("GetChunk_BuildNotFound", func(t *testing.T) {
		s, _, cIDs := setupTestStoreWithBuild()
		delete(s.builds, "build-test-1")
		c, b, err := s.GetChunk(ctx, cIDs[0])
		if !errors.Is(err, ErrBuildNotFound) || c != nil || b != nil {
			t.Errorf("expected ErrBuildNotFound, got err=%v", err)
		}
	})

	t.Run("AttachChunkPR_Success", func(t *testing.T) {
		s, _, cIDs := setupTestStoreWithBuild()
		threadID, err := s.AttachChunkPR(ctx, cIDs[0], 101, "aerial")
		if err != nil || threadID != "123456789012345678" {
			t.Fatalf("AttachChunkPR err=%v, threadID=%q", err, threadID)
		}
		c, _, _ := s.GetChunk(ctx, cIDs[0])
		if c.PRNumber != 101 || c.Status != "pr_opened" {
			t.Errorf("expected pr_number=101, status=pr_opened, got %+v", c)
		}

		threadID, err = s.AttachChunkPR(ctx, cIDs[0], 102, "aerial")
		if err != nil || threadID != "123456789012345678" {
			t.Fatalf("AttachChunkPR overwrite err=%v, threadID=%q", err, threadID)
		}
		c, _, _ = s.GetChunk(ctx, cIDs[0])
		if c.PRNumber != 102 {
			t.Errorf("expected overwritten pr_number=102, got %d", c.PRNumber)
		}
	})

	t.Run("AttachChunkPR_Errors", func(t *testing.T) {
		s, _, cIDs := setupTestStoreWithBuild()
		_, _ = s.AttachChunkPR(ctx, cIDs[0], 101, "aerial")

		_, err := s.AttachChunkPR(ctx, cIDs[0], 101, "other-repo")
		if err == nil {
			t.Error("expected error attaching mismatched repo to chunk")
		}

		_, err = s.AttachChunkPR(ctx, "missing", 101, "aerial")
		if !errors.Is(err, ErrChunkNotFound) {
			t.Errorf("expected ErrChunkNotFound, got %v", err)
		}

		_, err = s.AttachChunkPR(ctx, "", 101, "aerial")
		if err == nil {
			t.Error("expected error on empty chunk ID")
		}
	})

	t.Run("GetChunkByPR_Lifecycle", func(t *testing.T) {
		s, bID, cIDs := setupTestStoreWithBuild()
		_, _ = s.AttachChunkPR(ctx, cIDs[0], 101, "azylman/aerial")

		res, err := s.GetChunkByPR(ctx, "aerial", 101)
		if err != nil || res == nil || !res.Matched || !res.HasRemainingChunks || res.BuildID != bID || res.ChunkID != cIDs[0] {
			t.Fatalf("unexpected GetChunkByPR result: %+v, err: %v", res, err)
		}

		emptyRes, err := s.GetChunkByPR(ctx, "aerial", 999)
		if err != nil || emptyRes.Matched {
			t.Fatalf("expected unmatched, got %+v", emptyRes)
		}

		b := s.builds[bID]
		b.Status = "cancelled"
		s.builds[bID] = b
		cancRes, err := s.GetChunkByPR(ctx, "aerial", 101)
		if err != nil || cancRes.Matched {
			t.Errorf("expected cancelled build to be unmatchable, got %+v", cancRes)
		}

		invRes, err := s.GetChunkByPR(ctx, "", 0)
		if err != nil || invRes.Matched {
			t.Errorf("expected empty lookup for invalid params, got %+v", invRes)
		}
	})

	t.Run("AdvanceChunkByPR_Lifecycle", func(t *testing.T) {
		s, bID, cIDs := setupTestStoreWithBuild()
		_, _ = s.AttachChunkPR(ctx, cIDs[0], 101, "aerial")
		_, _ = s.AttachChunkPR(ctx, cIDs[1], 102, "aerial")

		adv1, err := s.AdvanceChunkByPR(ctx, "aerial", 101)
		if err != nil || adv1 == nil || !adv1.Matched || !adv1.Advanced || adv1.BuildCompleted || adv1.NextChunkIndex != 1 {
			t.Fatalf("unexpected adv1: %+v, err: %v", adv1, err)
		}

		adv1Repeat, err := s.AdvanceChunkByPR(ctx, "aerial", 101)
		if err != nil || adv1Repeat == nil || !adv1Repeat.Matched || adv1Repeat.Advanced {
			t.Fatalf("expected adv1Repeat to not advance, got %+v", adv1Repeat)
		}

		adv2, err := s.AdvanceChunkByPR(ctx, "aerial", 102)
		if err != nil || adv2 == nil || !adv2.Matched || !adv2.Advanced || !adv2.BuildCompleted || adv2.NextChunkIndex != -1 || adv2.BuildStatus != "completed" {
			t.Fatalf("unexpected adv2: %+v, err: %v", adv2, err)
		}

		b, _, _ := s.GetBuild(ctx, bID)
		if b.Status != "completed" {
			t.Errorf("expected build status completed, got %q", b.Status)
		}

		advUnmatched, err := s.AdvanceChunkByPR(ctx, "aerial", 999)
		if err != nil || advUnmatched.Matched {
			t.Errorf("expected unmatched advance, got %+v", advUnmatched)
		}

		advInvalid, err := s.AdvanceChunkByPR(ctx, "", 0)
		if err != nil || advInvalid.Matched {
			t.Errorf("expected unmatched advance for invalid params, got %+v", advInvalid)
		}
	})

	t.Run("FailChunkByPR_Lifecycle", func(t *testing.T) {
		s, bID, cIDs := setupTestStoreWithBuild()
		_, _ = s.AttachChunkPR(ctx, cIDs[0], 101, "aerial")

		bIDGot, cIDGot, failed, err := s.FailChunkByPR(ctx, "aerial", 101)
		if err != nil || !failed || bIDGot != bID || cIDGot != cIDs[0] {
			t.Fatalf("FailChunkByPR got (%s, %s, %v, %v)", bIDGot, cIDGot, failed, err)
		}
		c, b, _ := s.GetChunk(ctx, cIDs[0])
		if c.Status != "failed" || b.Status != "failed" {
			t.Errorf("expected failed status, got c=%s, b=%s", c.Status, b.Status)
		}

		_, _, repeatFailed, _ := s.FailChunkByPR(ctx, "aerial", 101)
		if repeatFailed {
			t.Error("expected second fail call to not fail already failed chunk")
		}

		_, _, unkFailed, _ := s.FailChunkByPR(ctx, "aerial", 999)
		if unkFailed {
			t.Error("expected fail on unknown PR to return false")
		}

		_, _, invFailed, _ := s.FailChunkByPR(ctx, "", 0)
		if invFailed {
			t.Error("expected fail on invalid args to return false")
		}
	})
}

func TestServer_ChunkEndpoints(t *testing.T) {
	memStore, bID, cIDs := setupTestStoreWithBuild()
	_, _ = memStore.AttachChunkPR(context.Background(), cIDs[0], 101, "azylman/aerial")

	var widgetDispatched []StatusWidgetRequest
	tsBrain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/discord/widget" {
			var wr StatusWidgetRequest
			_ = json.NewDecoder(r.Body).Decode(&wr)
			widgetDispatched = append(widgetDispatched, wr)
			httpkit.WriteJSON(w, http.StatusOK, map[string]string{"message_id": "msg-new-widget-1"})
			return
		}
		http.NotFound(w, r)
	}))
	defer tsBrain.Close()

	inserter := &mockRiverInserter{}
	srv := NewVanadisServer(Config{BrainURL: tsBrain.URL}, memStore, inserter, tsBrain.Client())
	mux := SetupMux(srv)

	doReq := func(method, path string, body string) *httptest.ResponseRecorder {
		var r *http.Request
		if body != "" {
			r = httptest.NewRequest(method, path, strings.NewReader(body))
			r.Header.Set("Content-Type", "application/json")
		} else {
			r = httptest.NewRequest(method, path, nil)
		}
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, r)
		return rec
	}

	t.Run("GET_ChunkByID", func(t *testing.T) {
		rec := doReq(http.MethodGet, "/chunks/"+cIDs[0], "")
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d: %s", rec.Code, rec.Body.String())
		}
		var res map[string]any
		_ = json.Unmarshal(rec.Body.Bytes(), &res)
		if res["chunk"] == nil || res["build"] == nil {
			t.Errorf("missing chunk or build in response: %+v", res)
		}

		rec404 := doReq(http.MethodGet, "/chunks/missing-chunk-id", "")
		if rec404.Code != http.StatusNotFound {
			t.Errorf("expected 404, got %d", rec404.Code)
		}

		rec400 := doReq(http.MethodGet, "/chunks/", "")
		if rec400.Code != http.StatusBadRequest {
			t.Errorf("expected 400 for empty id, got %d", rec400.Code)
		}

		rec405 := doReq(http.MethodPost, "/chunks/"+cIDs[0], "")
		if rec405.Code != http.StatusMethodNotAllowed {
			t.Errorf("expected 405, got %d", rec405.Code)
		}

		srvNil := NewVanadisServer(Config{}, nil, nil, nil)
		muxNil := SetupMux(srvNil)
		rec500 := httptest.NewRecorder()
		muxNil.ServeHTTP(rec500, httptest.NewRequest(http.MethodGet, "/chunks/"+cIDs[0], nil))
		if rec500.Code != http.StatusInternalServerError {
			t.Errorf("expected 500 when store is nil, got %d", rec500.Code)
		}
	})

	t.Run("GET_ChunkByPR", func(t *testing.T) {
		rec := doReq(http.MethodGet, "/chunks/by-pr?repo=aerial&pr_number=101", "")
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d: %s", rec.Code, rec.Body.String())
		}
		var lookup VanadisChunkPRLookup
		_ = json.Unmarshal(rec.Body.Bytes(), &lookup)
		if !lookup.Matched || lookup.BuildID != bID || lookup.ChunkID != cIDs[0] {
			t.Errorf("unexpected lookup: %+v", lookup)
		}

		recUnmatched := doReq(http.MethodGet, "/chunks/by-pr?repo=aerial&pr_number=999", "")
		if recUnmatched.Code != http.StatusOK {
			t.Errorf("expected 200 OK for unmatched, got %d", recUnmatched.Code)
		}

		for _, bad := range []string{
			"/chunks/by-pr",
			"/chunks/by-pr?repo=aerial",
			"/chunks/by-pr?pr_number=101",
			"/chunks/by-pr?repo=aerial&pr_number=abc",
			"/chunks/by-pr?repo=aerial&pr_number=-1",
		} {
			recBad := doReq(http.MethodGet, bad, "")
			if recBad.Code != http.StatusBadRequest {
				t.Errorf("expected 400 for %s, got %d", bad, recBad.Code)
			}
		}

		rec405 := doReq(http.MethodPost, "/chunks/by-pr", "")
		if rec405.Code != http.StatusMethodNotAllowed {
			t.Errorf("expected 405, got %d", rec405.Code)
		}

		srvNil := NewVanadisServer(Config{}, nil, nil, nil)
		muxNil := SetupMux(srvNil)
		rec500 := httptest.NewRecorder()
		muxNil.ServeHTTP(rec500, httptest.NewRequest(http.MethodGet, "/chunks/by-pr?repo=aerial&pr_number=101", nil))
		if rec500.Code != http.StatusInternalServerError {
			t.Errorf("expected 500 when store is nil, got %d", rec500.Code)
		}
	})

	t.Run("POST_AttachChunkPR", func(t *testing.T) {
		sAttach, _, cAttachIDs := setupTestStoreWithBuild()
		srvAttach := NewVanadisServer(Config{}, sAttach, nil, nil)
		muxAttach := SetupMux(srvAttach)
		doAttachReq := func(method, path string, body string) *httptest.ResponseRecorder {
			var r *http.Request
			if body != "" {
				r = httptest.NewRequest(method, path, strings.NewReader(body))
			} else {
				r = httptest.NewRequest(method, path, nil)
			}
			rec := httptest.NewRecorder()
			muxAttach.ServeHTTP(rec, r)
			return rec
		}

		rec := doAttachReq(http.MethodPost, "/chunks/attach-pr", fmt.Sprintf(`{"chunk_id":%q,"pr_number":103,"repo":"aerial"}`, cAttachIDs[0]))
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d: %s", rec.Code, rec.Body.String())
		}
		var res map[string]string
		_ = json.Unmarshal(rec.Body.Bytes(), &res)
		if res["status"] != "attached" || res["thread_id"] == "" {
			t.Errorf("unexpected attach response: %+v", res)
		}

		rec404 := doAttachReq(http.MethodPost, "/chunks/attach-pr", `{"chunk_id":"missing-chunk","pr_number":101,"repo":"aerial"}`)
		if rec404.Code != http.StatusNotFound {
			t.Errorf("expected 404 for missing chunk, got %d: %s", rec404.Code, rec404.Body.String())
		}

		recErrReader := httptest.NewRecorder()
		muxAttach.ServeHTTP(recErrReader, httptest.NewRequest(http.MethodPost, "/chunks/attach-pr", errorReader{}))
		if recErrReader.Code != http.StatusBadRequest {
			t.Errorf("expected 400 on errorReader, got %d", recErrReader.Code)
		}

		for _, bad := range []string{
			`{invalid-json`,
			`{"chunk_id":"","pr_number":101}`,
			`{"chunk_id":"chunk-1","pr_number":0}`,
		} {
			recBad := doAttachReq(http.MethodPost, "/chunks/attach-pr", bad)
			if recBad.Code != http.StatusBadRequest {
				t.Errorf("expected 400 for bad body %s, got %d", bad, recBad.Code)
			}
		}

		rec405 := doAttachReq(http.MethodGet, "/chunks/attach-pr", "")
		if rec405.Code != http.StatusMethodNotAllowed {
			t.Errorf("expected 405, got %d", rec405.Code)
		}

		srvNil := NewVanadisServer(Config{}, nil, nil, nil)
		muxNil := SetupMux(srvNil)
		rec500 := httptest.NewRecorder()
		muxNil.ServeHTTP(rec500, httptest.NewRequest(http.MethodPost, "/chunks/attach-pr", strings.NewReader(`{}`)))
		if rec500.Code != http.StatusInternalServerError {
			t.Errorf("expected 500 when store is nil, got %d", rec500.Code)
		}
	})

	t.Run("POST_AdvanceChunkPR", func(t *testing.T) {
		sAdv := newMemoryVanadisStore()
		bAdvID := "build-adv-1"
		b := mkBuild(bAdvID, "123456789012345678", "Adv Build Title", "Objective", "running")
		c1 := VanadisChunk{ID: "c-adv-1", BuildID: bAdvID, ChunkIndex: 0, Repo: "azylman/aerial", Title: "C1", Status: "pr_opened", PRNumber: 101}
		c2 := VanadisChunk{ID: "c-adv-2", BuildID: bAdvID, ChunkIndex: 1, Repo: "azylman/aerial", Title: "C2", Status: "pr_opened", PRNumber: 102}
		c3 := VanadisChunk{ID: "c-adv-3", BuildID: bAdvID, ChunkIndex: 2, Repo: "azylman/aerial", Title: "C3", Status: "pr_opened", PRNumber: 103}
		_ = sAdv.CreateBuild(context.Background(), b, []VanadisChunk{c1, c2, c3})

		inserterAdv := &mockRiverInserter{}
		srvAdv := NewVanadisServer(Config{}, sAdv, inserterAdv, nil)
		muxAdv := SetupMux(srvAdv)

		doAdvReq := func(method, path string, body string) *httptest.ResponseRecorder {
			var r *http.Request
			if body != "" {
				r = httptest.NewRequest(method, path, strings.NewReader(body))
			} else {
				r = httptest.NewRequest(method, path, nil)
			}
			rec := httptest.NewRecorder()
			muxAdv.ServeHTTP(rec, r)
			return rec
		}

		advRec := doAdvReq(http.MethodPost, "/chunks/advance-pr", `{"repo":"aerial","pr_number":101}`)
		if advRec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d: %s", advRec.Code, advRec.Body.String())
		}
		var advRes VanadisAdvancementResult
		_ = json.Unmarshal(advRec.Body.Bytes(), &advRes)
		if !advRes.Matched || !advRes.Advanced || advRes.NextChunkIndex != 1 {
			t.Errorf("unexpected advancement result: %+v", advRes)
		}
		if len(inserterAdv.inserted) != 1 || inserterAdv.inserted[0].ChunkIndex != 1 {
			t.Errorf("expected river job inserted for chunk 1, got %+v", inserterAdv.inserted)
		}

		// River insert error on chunk 2 advancement
		inserterAdv.err = errors.New("river boom")
		advErrRec := doAdvReq(http.MethodPost, "/chunks/advance-pr", `{"repo":"aerial","pr_number":102}`)
		if advErrRec.Code != http.StatusInternalServerError {
			t.Errorf("expected 500 on river error, got %d", advErrRec.Code)
		}
		inserterAdv.err = nil

		// Repeat chunk 2 advance succeeding
		advRec2 := doAdvReq(http.MethodPost, "/chunks/advance-pr", `{"repo":"aerial","pr_number":102}`)
		if advRec2.Code != http.StatusOK {
			t.Fatalf("expected 200 OK on repeat chunk 2 advance, got %d", advRec2.Code)
		}
		if len(inserterAdv.inserted) != 2 || inserterAdv.inserted[1].ChunkIndex != 2 {
			t.Errorf("expected river job inserted for chunk 2 on repeat, got %+v", inserterAdv.inserted)
		}

		// Final advance completing build
		advCompRec := doAdvReq(http.MethodPost, "/chunks/advance-pr", `{"repo":"aerial","pr_number":103}`)
		if advCompRec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK on complete, got %d: %s", advCompRec.Code, advCompRec.Body.String())
		}
		var advCompRes VanadisAdvancementResult
		_ = json.Unmarshal(advCompRec.Body.Bytes(), &advCompRes)
		if !advCompRes.BuildCompleted || advCompRes.NextChunkIndex != -1 {
			t.Errorf("expected build completed, got %+v", advCompRes)
		}

		recErrReader := httptest.NewRecorder()
		muxAdv.ServeHTTP(recErrReader, httptest.NewRequest(http.MethodPost, "/chunks/advance-pr", errorReader{}))
		if recErrReader.Code != http.StatusBadRequest {
			t.Errorf("expected 400 on errorReader, got %d", recErrReader.Code)
		}

		for _, bad := range []string{
			`{invalid-json`,
			`{"repo":"","pr_number":101}`,
			`{"repo":"aerial","pr_number":0}`,
		} {
			recBad := doAdvReq(http.MethodPost, "/chunks/advance-pr", bad)
			if recBad.Code != http.StatusBadRequest {
				t.Errorf("expected 400 for bad body %s, got %d", bad, recBad.Code)
			}
		}

		rec405 := doAdvReq(http.MethodGet, "/chunks/advance-pr", "")
		if rec405.Code != http.StatusMethodNotAllowed {
			t.Errorf("expected 405, got %d", rec405.Code)
		}

		srvNil := NewVanadisServer(Config{}, nil, nil, nil)
		muxNil := SetupMux(srvNil)
		rec500 := httptest.NewRecorder()
		muxNil.ServeHTTP(rec500, httptest.NewRequest(http.MethodPost, "/chunks/advance-pr", strings.NewReader(`{}`)))
		if rec500.Code != http.StatusInternalServerError {
			t.Errorf("expected 500 when store is nil, got %d", rec500.Code)
		}
	})

	t.Run("POST_FailChunkPR", func(t *testing.T) {
		sFail, _, cFailIDs := setupTestStoreWithBuild()
		_, _ = sFail.AttachChunkPR(context.Background(), cFailIDs[0], 201, "aerial")
		srvFail := NewVanadisServer(Config{}, sFail, nil, nil)
		muxFail := SetupMux(srvFail)

		rec := httptest.NewRecorder()
		muxFail.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/chunks/fail-pr", strings.NewReader(`{"repo":"aerial","pr_number":201}`)))
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d: %s", rec.Code, rec.Body.String())
		}
		var res map[string]any
		_ = json.Unmarshal(rec.Body.Bytes(), &res)
		if res["failed"] != true || res["chunk_id"] != cFailIDs[0] {
			t.Errorf("unexpected fail-pr response: %+v", res)
		}

		recUnm := httptest.NewRecorder()
		muxFail.ServeHTTP(recUnm, httptest.NewRequest(http.MethodPost, "/chunks/fail-pr", strings.NewReader(`{"repo":"aerial","pr_number":999}`)))
		if recUnm.Code != http.StatusOK {
			t.Errorf("expected 200 OK for unmatched, got %d", recUnm.Code)
		}

		recErrReader := httptest.NewRecorder()
		muxFail.ServeHTTP(recErrReader, httptest.NewRequest(http.MethodPost, "/chunks/fail-pr", errorReader{}))
		if recErrReader.Code != http.StatusBadRequest {
			t.Errorf("expected 400 on errorReader, got %d", recErrReader.Code)
		}

		for _, bad := range []string{
			`{invalid-json`,
			`{"repo":"","pr_number":201}`,
			`{"repo":"aerial","pr_number":0}`,
		} {
			recBad := httptest.NewRecorder()
			muxFail.ServeHTTP(recBad, httptest.NewRequest(http.MethodPost, "/chunks/fail-pr", strings.NewReader(bad)))
			if recBad.Code != http.StatusBadRequest {
				t.Errorf("expected 400 for %s, got %d", bad, recBad.Code)
			}
		}

		rec405 := httptest.NewRecorder()
		muxFail.ServeHTTP(rec405, httptest.NewRequest(http.MethodGet, "/chunks/fail-pr", nil))
		if rec405.Code != http.StatusMethodNotAllowed {
			t.Errorf("expected 405, got %d", rec405.Code)
		}

		srvNil := NewVanadisServer(Config{}, nil, nil, nil)
		muxNil := SetupMux(srvNil)
		rec500 := httptest.NewRecorder()
		muxNil.ServeHTTP(rec500, httptest.NewRequest(http.MethodPost, "/chunks/fail-pr", strings.NewReader(`{}`)))
		if rec500.Code != http.StatusInternalServerError {
			t.Errorf("expected 500 when store is nil, got %d", rec500.Code)
		}
	})

	t.Run("POST_SyncPRWidget", func(t *testing.T) {
		sWidget, bWidgetID, cWidgetIDs := setupTestStoreWithBuild()
		_, _ = sWidget.AttachChunkPR(context.Background(), cWidgetIDs[0], 301, "aerial")

		srvWidget := NewVanadisServer(Config{BrainURL: tsBrain.URL}, sWidget, nil, tsBrain.Client())
		muxWidget := SetupMux(srvWidget)

		payload := `{"repo":"aerial","pr_number":301,"pr_status":"deploying","pr_banner":"","pr_found":true}`
		rec := httptest.NewRecorder()
		muxWidget.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/chunks/sync-pr-widget", strings.NewReader(payload)))
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d: %s", rec.Code, rec.Body.String())
		}
		var res map[string]any
		_ = json.Unmarshal(rec.Body.Bytes(), &res)
		if res["matched"] != true || res["build_id"] != bWidgetID {
			t.Errorf("unexpected sync widget response: %+v", res)
		}
		b, _, _ := sWidget.GetBuild(context.Background(), bWidgetID)
		if b.StatusMessageID != "msg-new-widget-1" {
			t.Errorf("expected status_message_id updated to msg-new-widget-1, got %q", b.StatusMessageID)
		}

		// Conflict banner override
		conflictPayload := `{"repo":"aerial","pr_number":301,"pr_status":"conflict","pr_banner":"Merge conflict detected","pr_found":true}`
		recConflict := httptest.NewRecorder()
		muxWidget.ServeHTTP(recConflict, httptest.NewRequest(http.MethodPost, "/chunks/sync-pr-widget", strings.NewReader(conflictPayload)))
		if recConflict.Code != http.StatusOK {
			t.Errorf("expected 200 OK for conflict, got %d", recConflict.Code)
		}
		if len(widgetDispatched) > 0 {
			last := widgetDispatched[len(widgetDispatched)-1]
			if last.Banner != "Merge conflict detected" || last.Status != "failed" {
				t.Errorf("expected widget banner and status override, got banner=%q status=%q", last.Banner, last.Status)
			}
		}

		// Unmatched PR
		unmPayload := `{"repo":"aerial","pr_number":999,"pr_status":"open","pr_found":false}`
		recUnm := httptest.NewRecorder()
		muxWidget.ServeHTTP(recUnm, httptest.NewRequest(http.MethodPost, "/chunks/sync-pr-widget", strings.NewReader(unmPayload)))
		if recUnm.Code != http.StatusOK {
			t.Errorf("expected 200 OK for unmatched, got %d", recUnm.Code)
		}
		var unmRes map[string]any
		_ = json.Unmarshal(recUnm.Body.Bytes(), &unmRes)
		if unmRes["matched"] != false {
			t.Errorf("expected matched=false, got %+v", unmRes)
		}

		// Brain error
		tsBadBrain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "brain failure", http.StatusInternalServerError)
		}))
		defer tsBadBrain.Close()
		srvBadBrain := NewVanadisServer(Config{BrainURL: tsBadBrain.URL}, sWidget, nil, tsBadBrain.Client())
		muxBadBrain := SetupMux(srvBadBrain)
		recBrainErr := httptest.NewRecorder()
		muxBadBrain.ServeHTTP(recBrainErr, httptest.NewRequest(http.MethodPost, "/chunks/sync-pr-widget", strings.NewReader(payload)))
		if recBrainErr.Code != http.StatusInternalServerError {
			t.Errorf("expected 500 when brain fails, got %d", recBrainErr.Code)
		}

		recErrReader := httptest.NewRecorder()
		muxWidget.ServeHTTP(recErrReader, httptest.NewRequest(http.MethodPost, "/chunks/sync-pr-widget", errorReader{}))
		if recErrReader.Code != http.StatusBadRequest {
			t.Errorf("expected 400 on errorReader, got %d", recErrReader.Code)
		}

		for _, bad := range []string{
			`{invalid-json`,
			`{"repo":"","pr_number":301}`,
			`{"repo":"aerial","pr_number":0}`,
		} {
			recBad := httptest.NewRecorder()
			muxWidget.ServeHTTP(recBad, httptest.NewRequest(http.MethodPost, "/chunks/sync-pr-widget", strings.NewReader(bad)))
			if recBad.Code != http.StatusBadRequest {
				t.Errorf("expected 400 for %s, got %d", bad, recBad.Code)
			}
		}

		rec405 := httptest.NewRecorder()
		muxWidget.ServeHTTP(rec405, httptest.NewRequest(http.MethodGet, "/chunks/sync-pr-widget", nil))
		if rec405.Code != http.StatusMethodNotAllowed {
			t.Errorf("expected 405, got %d", rec405.Code)
		}

		srvNil := NewVanadisServer(Config{}, nil, nil, nil)
		muxNil := SetupMux(srvNil)
		rec500 := httptest.NewRecorder()
		muxNil.ServeHTTP(rec500, httptest.NewRequest(http.MethodPost, "/chunks/sync-pr-widget", strings.NewReader(`{}`)))
		if rec500.Code != http.StatusInternalServerError {
			t.Errorf("expected 500 when store is nil, got %d", rec500.Code)
		}
	})
}

func TestPostgresVanadisStore_ChunkMethods(t *testing.T) {
	ctx := context.Background()

	t.Run("NilReceiverAndDB", func(t *testing.T) {
		var nilStore *PostgresVanadisStore
		emptyStore := &PostgresVanadisStore{}

		for _, s := range []*PostgresVanadisStore{nilStore, emptyStore} {
			if _, _, err := s.GetChunk(ctx, "c1"); err == nil {
				t.Error("expected error on GetChunk")
			}
			if _, err := s.GetChunkByPR(ctx, "aerial", 1); err == nil {
				t.Error("expected error on GetChunkByPR")
			}
			if _, err := s.AttachChunkPR(ctx, "c1", 1, "aerial"); err == nil {
				t.Error("expected error on AttachChunkPR")
			}
			if _, err := s.AdvanceChunkByPR(ctx, "aerial", 1); err == nil {
				t.Error("expected error on AdvanceChunkByPR")
			}
			if _, _, _, err := s.FailChunkByPR(ctx, "aerial", 1); err == nil {
				t.Error("expected error on FailChunkByPR")
			}
		}
	})

	t.Run("GetChunk_MockDB", func(t *testing.T) {
		// Empty chunk ID
		s := newMockStore(mockDB{})
		if _, _, err := s.GetChunk(ctx, "  "); err == nil {
			t.Error("expected error on empty chunk ID")
		}

		// Query error
		sQueryErr := newMockStore(mockDB{
			queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
				return nil, errors.New("query err")
			},
		})
		if _, _, err := sQueryErr.GetChunk(ctx, "c1"); err == nil {
			t.Error("expected error on query failure")
		}

		// Rows error
		sRowsErr := newMockStore(mockDB{
			queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
				return &mockRows{rowsErr: errors.New("rows err"), items: []VanadisChunk{{ID: "c1"}}}, nil
			},
		})
		if _, _, err := sRowsErr.GetChunk(ctx, "c1"); err == nil {
			t.Error("expected error on rows error")
		}

		// Chunk not found (0 items)
		sNotFound := newMockStore(mockDB{
			queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
				return &mockRows{items: []VanadisChunk{}}, nil
			},
		})
		if _, _, err := sNotFound.GetChunk(ctx, "c1"); !errors.Is(err, ErrChunkNotFound) {
			t.Errorf("expected ErrChunkNotFound, got %v", err)
		}

		// Build not found
		sBuildNotFound := newMockStore(mockDB{
			queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
				return &mockRows{items: []VanadisChunk{{ID: "c1", BuildID: "b1"}}}, nil
			},
			queryRowFn: func(context.Context, string, ...any) pgx.Row {
				return &mockRow{err: pgx.ErrNoRows}
			},
		})
		if _, _, err := sBuildNotFound.GetChunk(ctx, "c1"); !errors.Is(err, ErrBuildNotFound) {
			t.Errorf("expected ErrBuildNotFound, got %v", err)
		}

		// Build query error
		sBuildErr := newMockStore(mockDB{
			queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
				return &mockRows{items: []VanadisChunk{{ID: "c1", BuildID: "b1"}}}, nil
			},
			queryRowFn: func(context.Context, string, ...any) pgx.Row {
				return &mockRow{err: errors.New("build query err")}
			},
		})
		if _, _, err := sBuildErr.GetChunk(ctx, "c1"); err == nil {
			t.Error("expected error on build query error")
		}

		// Success
		sSuccess := newMockStore(mockDB{
			queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
				return &mockRows{items: []VanadisChunk{{ID: "c1", BuildID: "b1", Title: "Chunk Title"}}}, nil
			},
			queryRowFn: func(context.Context, string, ...any) pgx.Row {
				return &mockRow{b: &VanadisBuild{ID: "b1", Title: "Build Title"}}
			},
		})
		c, b, err := sSuccess.GetChunk(ctx, "c1")
		if err != nil || c.ID != "c1" || b.ID != "b1" {
			t.Errorf("unexpected GetChunk success: c=%+v b=%+v err=%v", c, b, err)
		}
	})

	t.Run("GetChunkByPR_MockDB", func(t *testing.T) {
		s := newMockStore(mockDB{})
		// Invalid args
		res, err := s.GetChunkByPR(ctx, "", 0)
		if err != nil || res.Matched {
			t.Errorf("expected unmatched for empty args, got %+v", res)
		}

		// No rows
		sNoRows := newMockStore(mockDB{
			queryRowFn: func(context.Context, string, ...any) pgx.Row {
				return &mockRow{err: pgx.ErrNoRows}
			},
		})
		resNoRows, err := sNoRows.GetChunkByPR(ctx, "aerial", 101)
		if err != nil || resNoRows.Matched {
			t.Errorf("expected unmatched on NoRows, got %+v", resNoRows)
		}

		// QueryRow error
		sErr := newMockStore(mockDB{
			queryRowFn: func(context.Context, string, ...any) pgx.Row {
				return &mockRow{err: errors.New("db error")}
			},
		})
		if _, err := sErr.GetChunkByPR(ctx, "aerial", 101); err == nil {
			t.Error("expected error on query row error")
		}

		// Success
		sSuccess := newMockStore(mockDB{
			queryRowFn: func(context.Context, string, ...any) pgx.Row {
				return &genericMockRow{scanFn: func(dest ...any) error {
					*dest[0].(*string) = "c1"
					*dest[1].(*string) = "b1"
					*dest[2].(*string) = "low"
					*dest[3].(*bool) = true
					return nil
				}}
			},
		})
		resSuccess, err := sSuccess.GetChunkByPR(ctx, "aerial", 101)
		if err != nil || !resSuccess.Matched || resSuccess.ChunkID != "c1" || resSuccess.Effort != "low" || !resSuccess.HasRemainingChunks {
			t.Errorf("unexpected success lookup: %+v, err=%v", resSuccess, err)
		}
	})

	t.Run("AttachChunkPR_MockDB", func(t *testing.T) {
		s := newMockStore(mockDB{})
		// Invalid args
		if _, err := s.AttachChunkPR(ctx, "", 0, "aerial"); err == nil {
			t.Error("expected error on empty chunk ID")
		}

		// No rows: chunk does not exist
		sNotExist := newMockStore(mockDB{
			queryRowFn: func(_ context.Context, sql string, _ ...any) pgx.Row {
				if strings.Contains(sql, "SELECT EXISTS") {
					return &genericMockRow{scanFn: func(dest ...any) error {
						*dest[0].(*bool) = false
						return nil
					}}
				}
				return &mockRow{err: pgx.ErrNoRows}
			},
		})
		if _, err := sNotExist.AttachChunkPR(ctx, "c1", 101, "aerial"); !errors.Is(err, ErrChunkNotFound) {
			t.Errorf("expected ErrChunkNotFound, got %v", err)
		}

		// No rows: chunk exists but condition failed (e.g. mismatched)
		sMismatched := newMockStore(mockDB{
			queryRowFn: func(_ context.Context, sql string, _ ...any) pgx.Row {
				if strings.Contains(sql, "SELECT EXISTS") {
					return &genericMockRow{scanFn: func(dest ...any) error {
						*dest[0].(*bool) = true
						return nil
					}}
				}
				return &mockRow{err: pgx.ErrNoRows}
			},
		})
		if _, err := sMismatched.AttachChunkPR(ctx, "c1", 101, "aerial"); err == nil || errors.Is(err, ErrChunkNotFound) {
			t.Errorf("expected mismatch error, got %v", err)
		}

		// General error
		sGeneralErr := newMockStore(mockDB{
			queryRowFn: func(_ context.Context, _ string, _ ...any) pgx.Row {
				return &mockRow{err: errors.New("attach db err")}
			},
		})
		if _, err := sGeneralErr.AttachChunkPR(ctx, "c1", 101, "aerial"); err == nil {
			t.Error("expected error on general queryRow error")
		}

		// Success without repo
		sNoRepo := newMockStore(mockDB{
			queryRowFn: func(_ context.Context, _ string, _ ...any) pgx.Row {
				return &genericMockRow{scanFn: func(dest ...any) error {
					*dest[0].(*string) = "thread-123"
					return nil
				}}
			},
		})
		threadID, err := sNoRepo.AttachChunkPR(ctx, "c1", 101, "")
		if err != nil || threadID != "thread-123" {
			t.Errorf("unexpected AttachChunkPR result: threadID=%q err=%v", threadID, err)
		}
	})

	t.Run("AdvanceChunkByPR_MockDB", func(t *testing.T) {
		s := newMockStore(mockDB{})
		// Invalid args
		res, err := s.AdvanceChunkByPR(ctx, "", 0)
		if err != nil || res.Matched {
			t.Errorf("expected unmatched for empty args, got %+v", res)
		}

		// Begin error
		sBeginErr := newMockStore(mockDB{
			beginFn: func(context.Context) (pgx.Tx, error) {
				return nil, errors.New("begin tx err")
			},
		})
		if _, err := sBeginErr.AdvanceChunkByPR(ctx, "aerial", 101); err == nil {
			t.Error("expected error on begin tx failure")
		}

		// Select chunk no rows
		sNoRows := newMockStore(mockDB{
			beginFn: func(context.Context) (pgx.Tx, error) {
				return &mockTx{
					queryRowFn: func(context.Context, string, ...any) pgx.Row {
						return &mockRow{err: pgx.ErrNoRows}
					},
				}, nil
			},
		})
		resNoRows, err := sNoRows.AdvanceChunkByPR(ctx, "aerial", 101)
		if err != nil || resNoRows.Matched {
			t.Errorf("expected unmatched on no rows, got %+v", resNoRows)
		}

		// Select chunk db error
		sSelectErr := newMockStore(mockDB{
			beginFn: func(context.Context) (pgx.Tx, error) {
				return &mockTx{
					queryRowFn: func(context.Context, string, ...any) pgx.Row {
						return &mockRow{err: errors.New("select chunk err")}
					},
				}, nil
			},
		})
		if _, err := sSelectErr.AdvanceChunkByPR(ctx, "aerial", 101); err == nil {
			t.Error("expected error on select chunk error")
		}

		// Already terminal
		sTerminal := newMockStore(mockDB{
			beginFn: func(context.Context) (pgx.Tx, error) {
				return &mockTx{
					queryRowFn: func(context.Context, string, ...any) pgx.Row {
						return &genericMockRow{scanFn: func(dest ...any) error {
							*dest[0].(*string) = "c1"
							*dest[1].(*string) = "b1"
							*dest[2].(*int) = 0
							*dest[3].(*string) = "Chunk 1"
							*dest[4].(*string) = "merged" // already merged
							*dest[5].(*string) = "thread-1"
							*dest[6].(*int) = 2
							*dest[7].(*string) = "Build 1"
							*dest[8].(*string) = "Objective 1"
							*dest[9].(*string) = "cancelled"
							*dest[10].(*string) = "msg-1"
							return nil
						}}
					},
				}, nil
			},
		})
		resTerm, err := sTerminal.AdvanceChunkByPR(ctx, "aerial", 101)
		if err != nil || !resTerm.Matched || resTerm.Advanced || resTerm.NextChunkIndex != -1 {
			t.Errorf("expected matched but not advanced for already terminal build, got %+v", resTerm)
		}

		// Already merged chunk on running build computes next chunk
		sMergedRunning := newMockStore(mockDB{
			beginFn: func(context.Context) (pgx.Tx, error) {
				return &mockTx{
					queryRowFn: func(context.Context, string, ...any) pgx.Row {
						return &genericMockRow{scanFn: func(dest ...any) error {
							*dest[0].(*string) = "c1"
							*dest[1].(*string) = "b1"
							*dest[2].(*int) = 0
							*dest[3].(*string) = "Chunk 1"
							*dest[4].(*string) = "merged"
							*dest[5].(*string) = "thread-1"
							*dest[6].(*int) = 2
							*dest[7].(*string) = "Build 1"
							*dest[8].(*string) = "Objective 1"
							*dest[9].(*string) = "running"
							*dest[10].(*string) = "msg-1"
							return nil
						}}
					},
					queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
						return &mockRows{items: []VanadisChunk{
							{ID: "c1", ChunkIndex: 0, Status: "merged"},
							{ID: "c2", ChunkIndex: 1, Status: "pending"},
						}}, nil
					},
				}, nil
			},
		})
		resMergedRunning, err := sMergedRunning.AdvanceChunkByPR(ctx, "aerial", 101)
		if err != nil || !resMergedRunning.Matched || resMergedRunning.Advanced || resMergedRunning.NextChunkIndex != 1 {
			t.Errorf("expected matched, unadvanced, nextChunkIdx 1 for already merged chunk on running build, got %+v", resMergedRunning)
		}

		// Update chunk error
		sUpdateErr := newMockStore(mockDB{
			beginFn: func(context.Context) (pgx.Tx, error) {
				return &mockTx{
					queryRowFn: func(context.Context, string, ...any) pgx.Row {
						return &genericMockRow{scanFn: func(dest ...any) error {
							*dest[0].(*string) = "c1"
							*dest[1].(*string) = "b1"
							*dest[2].(*int) = 0
							*dest[3].(*string) = "Chunk 1"
							*dest[4].(*string) = "pr_opened"
							*dest[5].(*string) = "thread-1"
							*dest[6].(*int) = 2
							*dest[7].(*string) = "Build 1"
							*dest[8].(*string) = "Objective 1"
							*dest[9].(*string) = "running"
							*dest[10].(*string) = "msg-1"
							return nil
						}}
					},
					execFn: func(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
						if strings.Contains(sql, "UPDATE vanadis_chunks") {
							return pgconn.CommandTag{}, errors.New("update chunk err")
						}
						return pgconn.NewCommandTag("UPDATE 1"), nil
					},
				}, nil
			},
		})
		if _, err := sUpdateErr.AdvanceChunkByPR(ctx, "aerial", 101); err == nil {
			t.Error("expected error on update chunk error")
		}

		// Query chunks error
		sQueryChunksErr := newMockStore(mockDB{
			beginFn: func(context.Context) (pgx.Tx, error) {
				return &mockTx{
					queryRowFn: func(context.Context, string, ...any) pgx.Row {
						return &genericMockRow{scanFn: func(dest ...any) error {
							*dest[0].(*string) = "c1"
							*dest[1].(*string) = "b1"
							*dest[2].(*int) = 0
							*dest[3].(*string) = "Chunk 1"
							*dest[4].(*string) = "pr_opened"
							*dest[5].(*string) = "thread-1"
							*dest[6].(*int) = 2
							*dest[7].(*string) = "Build 1"
							*dest[8].(*string) = "Objective 1"
							*dest[9].(*string) = "running"
							*dest[10].(*string) = "msg-1"
							return nil
						}}
					},
					queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
						return nil, errors.New("query chunks err")
					},
				}, nil
			},
		})
		if _, err := sQueryChunksErr.AdvanceChunkByPR(ctx, "aerial", 101); err == nil {
			t.Error("expected error on query chunks error")
		}

		// Iterate chunks error
		sIterErr := newMockStore(mockDB{
			beginFn: func(context.Context) (pgx.Tx, error) {
				return &mockTx{
					queryRowFn: func(context.Context, string, ...any) pgx.Row {
						return &genericMockRow{scanFn: func(dest ...any) error {
							*dest[0].(*string) = "c1"
							*dest[1].(*string) = "b1"
							*dest[2].(*int) = 0
							*dest[3].(*string) = "Chunk 1"
							*dest[4].(*string) = "pr_opened"
							*dest[5].(*string) = "thread-1"
							*dest[6].(*int) = 2
							*dest[7].(*string) = "Build 1"
							*dest[8].(*string) = "Objective 1"
							*dest[9].(*string) = "running"
							*dest[10].(*string) = "msg-1"
							return nil
						}}
					},
					queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
						return &mockRows{rowsErr: errors.New("chunks iter err"), items: []VanadisChunk{{ID: "c1"}}}, nil
					},
				}, nil
			},
		})
		if _, err := sIterErr.AdvanceChunkByPR(ctx, "aerial", 101); err == nil {
			t.Error("expected error on chunks iter error")
		}

		// Commit error with next chunk
		sCommitErr := newMockStore(mockDB{
			beginFn: func(context.Context) (pgx.Tx, error) {
				return &mockTx{
					commitErr: errors.New("commit err"),
					queryRowFn: func(context.Context, string, ...any) pgx.Row {
						return &genericMockRow{scanFn: func(dest ...any) error {
							*dest[0].(*string) = "c1"
							*dest[1].(*string) = "b1"
							*dest[2].(*int) = 0
							*dest[3].(*string) = "Chunk 1"
							*dest[4].(*string) = "pr_opened"
							*dest[5].(*string) = "thread-1"
							*dest[6].(*int) = 2
							*dest[7].(*string) = "Build 1"
							*dest[8].(*string) = "Objective 1"
							*dest[9].(*string) = "running"
							*dest[10].(*string) = "msg-1"
							return nil
						}}
					},
					queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
						return &mockRows{items: []VanadisChunk{
							{ID: "c1", ChunkIndex: 0, Status: "merged", StartedAt: time.Now()},
							{ID: "c2", ChunkIndex: 1, Status: "pending"},
						}}, nil
					},
				}, nil
			},
		})
		if _, err := sCommitErr.AdvanceChunkByPR(ctx, "aerial", 101); err == nil {
			t.Error("expected error on commit error")
		}

		// Build completion: update build error
		sBuildUpdateErr := newMockStore(mockDB{
			beginFn: func(context.Context) (pgx.Tx, error) {
				return &mockTx{
					queryRowFn: func(context.Context, string, ...any) pgx.Row {
						return &genericMockRow{scanFn: func(dest ...any) error {
							*dest[0].(*string) = "c1"
							*dest[1].(*string) = "b1"
							*dest[2].(*int) = 0
							*dest[3].(*string) = "Chunk 1"
							*dest[4].(*string) = "pr_opened"
							*dest[5].(*string) = "thread-1"
							*dest[6].(*int) = 2
							*dest[7].(*string) = "Build 1"
							*dest[8].(*string) = "Objective 1"
							*dest[9].(*string) = "running"
							*dest[10].(*string) = "msg-1"
							return nil
						}}
					},
					queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
						// Only 1 chunk, which is merged -> complete build
						return &mockRows{items: []VanadisChunk{
							{ID: "c1", ChunkIndex: 0, Status: "merged"},
						}}, nil
					},
					execFn: func(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
						if strings.Contains(sql, "UPDATE vanadis_builds") {
							return pgconn.CommandTag{}, errors.New("update build err")
						}
						return pgconn.NewCommandTag("UPDATE 1"), nil
					},
				}, nil
			},
		})
		if _, err := sBuildUpdateErr.AdvanceChunkByPR(ctx, "aerial", 101); err == nil {
			t.Error("expected error on build update error")
		}

		// Build completion: commit error
		sBuildCommitErr := newMockStore(mockDB{
			beginFn: func(context.Context) (pgx.Tx, error) {
				return &mockTx{
					commitErr: errors.New("build commit err"),
					queryRowFn: func(context.Context, string, ...any) pgx.Row {
						return &genericMockRow{scanFn: func(dest ...any) error {
							*dest[0].(*string) = "c1"
							*dest[1].(*string) = "b1"
							*dest[2].(*int) = 0
							*dest[3].(*string) = "Chunk 1"
							*dest[4].(*string) = "pr_opened"
							*dest[5].(*string) = "thread-1"
							*dest[6].(*int) = 2
							*dest[7].(*string) = "Build 1"
							*dest[8].(*string) = "Objective 1"
							*dest[9].(*string) = "running"
							*dest[10].(*string) = "msg-1"
							return nil
						}}
					},
					queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
						return &mockRows{items: []VanadisChunk{
							{ID: "c1", ChunkIndex: 0, Status: "merged"},
						}}, nil
					},
				}, nil
			},
		})
		if _, err := sBuildCommitErr.AdvanceChunkByPR(ctx, "aerial", 101); err == nil {
			t.Error("expected error on build commit error")
		}

		// Success advancing to next chunk
		sAdvanceNextSuccess := newMockStore(mockDB{
			beginFn: func(context.Context) (pgx.Tx, error) {
				return &mockTx{
					queryRowFn: func(context.Context, string, ...any) pgx.Row {
						return &genericMockRow{scanFn: func(dest ...any) error {
							*dest[0].(*string) = "c1"
							*dest[1].(*string) = "b1"
							*dest[2].(*int) = 0
							*dest[3].(*string) = "Chunk 1"
							*dest[4].(*string) = "pr_opened"
							*dest[5].(*string) = "thread-1"
							*dest[6].(*int) = 2
							*dest[7].(*string) = "Build 1"
							*dest[8].(*string) = "Objective 1"
							*dest[9].(*string) = "running"
							*dest[10].(*string) = "msg-1"
							return nil
						}}
					},
					queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
						return &mockRows{items: []VanadisChunk{
							{ID: "c1", ChunkIndex: 0, Status: "merged"},
							{ID: "c2", ChunkIndex: 1, Status: "pending"},
						}}, nil
					},
				}, nil
			},
		})
		resNext, err := sAdvanceNextSuccess.AdvanceChunkByPR(ctx, "aerial", 101)
		if err != nil || !resNext.Matched || !resNext.Advanced || resNext.NextChunkIndex != 1 {
			t.Errorf("expected success advancing to next chunk, got %+v err=%v", resNext, err)
		}

		// Success completing build
		sCompleteSuccess := newMockStore(mockDB{
			beginFn: func(context.Context) (pgx.Tx, error) {
				return &mockTx{
					queryRowFn: func(context.Context, string, ...any) pgx.Row {
						return &genericMockRow{scanFn: func(dest ...any) error {
							*dest[0].(*string) = "c1"
							*dest[1].(*string) = "b1"
							*dest[2].(*int) = 0
							*dest[3].(*string) = "Chunk 1"
							*dest[4].(*string) = "pr_opened"
							*dest[5].(*string) = "thread-1"
							*dest[6].(*int) = 2
							*dest[7].(*string) = "Build 1"
							*dest[8].(*string) = "Objective 1"
							*dest[9].(*string) = "running"
							*dest[10].(*string) = "msg-1"
							return nil
						}}
					},
					queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
						return &mockRows{items: []VanadisChunk{
							{ID: "c1", ChunkIndex: 0, Status: "merged"},
						}}, nil
					},
				}, nil
			},
		})
		resComp, err := sCompleteSuccess.AdvanceChunkByPR(ctx, "aerial", 101)
		if err != nil || !resComp.Matched || !resComp.Advanced || !resComp.BuildCompleted || resComp.NextChunkIndex != -1 {
			t.Errorf("expected success completing build, got %+v err=%v", resComp, err)
		}
	})

	t.Run("FailChunkByPR_MockDB", func(t *testing.T) {
		s := newMockStore(mockDB{})
		// Invalid args
		_, _, failed, err := s.FailChunkByPR(ctx, "", 0)
		if err != nil || failed {
			t.Errorf("expected not failed for invalid args, got failed=%v, err=%v", failed, err)
		}

		// No rows
		sNoRows := newMockStore(mockDB{
			queryRowFn: func(context.Context, string, ...any) pgx.Row {
				return &mockRow{err: pgx.ErrNoRows}
			},
		})
		_, _, failedNoRows, err := sNoRows.FailChunkByPR(ctx, "aerial", 101)
		if err != nil || failedNoRows {
			t.Errorf("expected not failed on NoRows, got failed=%v, err=%v", failedNoRows, err)
		}

		// Query row error
		sErr := newMockStore(mockDB{
			queryRowFn: func(context.Context, string, ...any) pgx.Row {
				return &mockRow{err: errors.New("fail query err")}
			},
		})
		if _, _, _, err := sErr.FailChunkByPR(ctx, "aerial", 101); err == nil {
			t.Error("expected error on query row failure")
		}

		// Update build error
		sUpdateBuildErr := newMockStore(mockDB{
			queryRowFn: func(context.Context, string, ...any) pgx.Row {
				return &genericMockRow{scanFn: func(dest ...any) error {
					*dest[0].(*string) = "b1"
					*dest[1].(*string) = "c1"
					return nil
				}}
			},
			execFn: func(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
				if strings.Contains(sql, "UPDATE vanadis_builds") {
					return pgconn.CommandTag{}, errors.New("update build err")
				}
				return pgconn.NewCommandTag("UPDATE 1"), nil
			},
		})
		if _, _, _, err := sUpdateBuildErr.FailChunkByPR(ctx, "aerial", 101); err == nil {
			t.Error("expected error on update build failure")
		}

		// Success
		sFailSuccess := newMockStore(mockDB{
			queryRowFn: func(context.Context, string, ...any) pgx.Row {
				return &genericMockRow{scanFn: func(dest ...any) error {
					*dest[0].(*string) = "b1"
					*dest[1].(*string) = "c1"
					return nil
				}}
			},
		})
		bID, cID, failed, err := sFailSuccess.FailChunkByPR(ctx, "aerial", 101)
		if err != nil || !failed || bID != "b1" || cID != "c1" {
			t.Errorf("unexpected FailChunkByPR success: got (%s, %s, %v, %v)", bID, cID, failed, err)
		}
	})
}

type genericMockRow struct {
	scanFn func(dest ...any) error
}

func (r *genericMockRow) Scan(dest ...any) error {
	if r.scanFn != nil {
		return r.scanFn(dest...)
	}
	return nil
}
