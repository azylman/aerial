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
)

func TestServer_ChunkEndpoints(t *testing.T) {
	memStore, bID, cIDs := setupTestStoreWithBuild()
	_, _ = memStore.AttachChunkPR(context.Background(), cIDs[0], 101, "aerial")

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
		c1 := VanadisChunk{ID: "c-adv-1", BuildID: bAdvID, ChunkIndex: 0, Repo: "aerial", Title: "C1", Status: "pr_opened", PRNumber: 101}
		c2 := VanadisChunk{ID: "c-adv-2", BuildID: bAdvID, ChunkIndex: 1, Repo: "aerial", Title: "C2", Status: "pr_opened", PRNumber: 102}
		c3 := VanadisChunk{ID: "c-adv-3", BuildID: bAdvID, ChunkIndex: 2, Repo: "aerial", Title: "C3", Status: "pr_opened", PRNumber: 103}
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
		if len(widgetDispatched) == 0 || len(widgetDispatched[len(widgetDispatched)-1].Chunks) == 0 || widgetDispatched[len(widgetDispatched)-1].Chunks[0].ID != cWidgetIDs[0] {
			t.Errorf("expected dispatched widget to have chunk ID %q, got %+v", cWidgetIDs[0], widgetDispatched)
		}
		b, _, _ := sWidget.GetBuild(context.Background(), bWidgetID)
		if b.StatusMessageID != "msg-new-widget-1" {
			t.Errorf("expected status_message_id updated to msg-new-widget-1, got %q", b.StatusMessageID)
		}

		// ActiveStep and SkipSink propagation on open PR sync
		ciPayload := `{"repo":"aerial","pr_number":301,"pr_status":"open","active_step":"Waiting on CI (2/4 passed)","pr_found":true}`
		recCI := httptest.NewRecorder()
		muxWidget.ServeHTTP(recCI, httptest.NewRequest(http.MethodPost, "/chunks/sync-pr-widget", strings.NewReader(ciPayload)))
		if recCI.Code != http.StatusOK {
			t.Fatalf("expected 200 OK for CI sync, got %d: %s", recCI.Code, recCI.Body.String())
		}
		if len(widgetDispatched) > 0 {
			last := widgetDispatched[len(widgetDispatched)-1]
			if last.SkipSink {
				t.Errorf("expected SkipSink to be false on sync-pr-widget so buried cards can sink")
			}
			if len(last.Chunks) == 0 || last.Chunks[0].ActiveStep != "Waiting on CI (2/4 passed)" {
				t.Errorf("expected chunk ActiveStep to be %q, got %+v", "Waiting on CI (2/4 passed)", last.Chunks)
			}
		}

		// Recovery from failed state on open PR sync
		_, _, failed, err := sWidget.FailChunkByPR(context.Background(), "aerial", 301)
		if err != nil || !failed {
			t.Fatalf("FailChunkByPR got failed=%v, err=%v", failed, err)
		}
		cFailed, bFailed, _ := sWidget.GetChunk(context.Background(), cWidgetIDs[0])
		if cFailed.Status != "failed" || bFailed.Status != "failed" {
			t.Fatalf("expected chunk and build status failed before recovery, got chunk=%s build=%s", cFailed.Status, bFailed.Status)
		}

		recoverPayload := `{"repo":"aerial","pr_number":301,"pr_status":"open","active_step":"Waiting on CI (1/4 passed)","pr_found":true}`
		recRecover := httptest.NewRecorder()
		muxWidget.ServeHTTP(recRecover, httptest.NewRequest(http.MethodPost, "/chunks/sync-pr-widget", strings.NewReader(recoverPayload)))
		if recRecover.Code != http.StatusOK {
			t.Fatalf("expected 200 OK for recover sync, got %d: %s", recRecover.Code, recRecover.Body.String())
		}
		cRecovered, bRecovered, _ := sWidget.GetChunk(context.Background(), cWidgetIDs[0])
		if cRecovered.Status != "pr_opened" {
			t.Errorf("expected recovered chunk status pr_opened, got %q", cRecovered.Status)
		}
		if bRecovered.Status != "running" {
			t.Errorf("expected recovered build status running, got %q", bRecovered.Status)
		}
		if !cRecovered.CompletedAt.IsZero() {
			t.Errorf("expected recovered chunk CompletedAt to be zero, got %v", cRecovered.CompletedAt)
		}

		// Build ID direct sync
		buildPayload := fmt.Sprintf(`{"build_id":"%s","pr_banner":"Custom banner test"}`, bWidgetID)
		recBuild := httptest.NewRecorder()
		muxWidget.ServeHTTP(recBuild, httptest.NewRequest(http.MethodPost, "/chunks/sync-pr-widget", strings.NewReader(buildPayload)))
		if recBuild.Code != http.StatusOK {
			t.Fatalf("expected 200 OK for build_id sync, got %d: %s", recBuild.Code, recBuild.Body.String())
		}
		var buildRes map[string]any
		_ = json.Unmarshal(recBuild.Body.Bytes(), &buildRes)
		if buildRes["matched"] != true || buildRes["build_id"] != bWidgetID {
			t.Errorf("unexpected build_id sync response: %+v", buildRes)
		}
		if len(widgetDispatched) > 0 {
			last := widgetDispatched[len(widgetDispatched)-1]
			if last.Banner != "Custom banner test" || last.ID != bWidgetID {
				t.Errorf("expected widget banner %q and ID %q, got banner=%q ID=%q", "Custom banner test", bWidgetID, last.Banner, last.ID)
			}
		}

		// Build ID direct sync: unmatched ErrBuildNotFound
		unmatchedBuildPayload := `{"build_id":"nonexistent-build-id"}`
		recUnmatchedBuild := httptest.NewRecorder()
		muxWidget.ServeHTTP(recUnmatchedBuild, httptest.NewRequest(http.MethodPost, "/chunks/sync-pr-widget", strings.NewReader(unmatchedBuildPayload)))
		if recUnmatchedBuild.Code != http.StatusOK {
			t.Fatalf("expected 200 OK for nonexistent build_id, got %d", recUnmatchedBuild.Code)
		}
		var unRes map[string]any
		_ = json.Unmarshal(recUnmatchedBuild.Body.Bytes(), &unRes)
		if unRes["matched"] != false {
			t.Errorf("expected matched=false for nonexistent build, got %+v", unRes)
		}

		// Build ID direct sync: store error
		sWidget.getErr = errors.New("simulated store error")
		recBuildStoreErr := httptest.NewRecorder()
		muxWidget.ServeHTTP(recBuildStoreErr, httptest.NewRequest(http.MethodPost, "/chunks/sync-pr-widget", strings.NewReader(buildPayload)))
		if recBuildStoreErr.Code != http.StatusInternalServerError {
			t.Errorf("expected 500 on store error for build_id sync, got %d", recBuildStoreErr.Code)
		}
		sWidget.getErr = nil

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

		// POST /chunks/complete tests
		t.Run("POST_ChunksComplete", func(t *testing.T) {
			sAct := newMemoryVanadisStore()
			bActID := "build-act-complete"
			bAct := mkBuild(bActID, "123456789012345678", "Action Build", "Test Action Completion", "running")
			cAct1 := VanadisChunk{
				ID:         "chunk-act-1",
				BuildID:    bActID,
				ChunkIndex: 0,
				Mode:       "action",
				Title:      "Action Step 1",
				Status:     "running",
				Effort:     "low",
				CreatedAt:  time.Now().UTC(),
				UpdatedAt:  time.Now().UTC(),
			}
			cAct2 := VanadisChunk{
				ID:         "chunk-act-2",
				BuildID:    bActID,
				ChunkIndex: 1,
				Mode:       "action",
				Title:      "Action Step 2",
				Status:     "pending",
				Effort:     "low",
				CreatedAt:  time.Now().UTC(),
				UpdatedAt:  time.Now().UTC(),
			}
			_ = sAct.CreateBuild(context.Background(), bAct, []VanadisChunk{cAct1, cAct2})

			var wakePrompts []map[string]string
			tsBrainAct := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/internal/widget/sync":
					httpkit.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
				case "/prompt":
					var p map[string]string
					_ = json.NewDecoder(r.Body).Decode(&p)
					wakePrompts = append(wakePrompts, p)
					httpkit.WriteJSON(w, http.StatusAccepted, map[string]string{"status": "accepted"})
				default:
					http.NotFound(w, r)
				}
			}))
			defer tsBrainAct.Close()

			insAct := &mockRiverInserter{}
			srvAct := NewVanadisServer(Config{BrainURL: tsBrainAct.URL, SynapseURL: tsBrainAct.URL}, sAct, insAct, tsBrainAct.Client())
			muxAct := SetupMux(srvAct)

			// 1. Advance first action chunk -> next chunk queued in River
			rec1 := httptest.NewRecorder()
			muxAct.ServeHTTP(rec1, httptest.NewRequest(http.MethodPost, "/chunks/complete", strings.NewReader(`{"chunk_id":"chunk-act-1"}`)))
			if rec1.Code != http.StatusOK {
				t.Fatalf("expected 200 OK advancing action chunk, got %d: %s", rec1.Code, rec1.Body.String())
			}
			var res1 VanadisAdvancementResult
			_ = json.Unmarshal(rec1.Body.Bytes(), &res1)
			if !res1.Matched || !res1.Advanced || res1.NextChunkIndex != 1 || res1.BuildCompleted {
				t.Errorf("unexpected advancement result for chunk 1: %+v", res1)
			}
			if len(insAct.inserted) != 1 || insAct.inserted[0].ChunkIndex != 1 {
				t.Errorf("expected chunk 1 queued in river, got %+v", insAct.inserted)
			}

			// 2. Complete second (final) action chunk -> build completed & wake prompt dispatched
			rec2 := httptest.NewRecorder()
			muxAct.ServeHTTP(rec2, httptest.NewRequest(http.MethodPost, "/chunks/complete", strings.NewReader(`{"chunk_id":"chunk-act-2"}`)))
			if rec2.Code != http.StatusOK {
				t.Fatalf("expected 200 OK completing build, got %d: %s", rec2.Code, rec2.Body.String())
			}
			var res2 VanadisAdvancementResult
			_ = json.Unmarshal(rec2.Body.Bytes(), &res2)
			if !res2.Matched || !res2.Advanced || !res2.BuildCompleted || res2.BuildStatus != "completed" {
				t.Errorf("unexpected completion result for chunk 2: %+v", res2)
			}
			if len(wakePrompts) != 1 || wakePrompts[0]["channel_id"] != "123456789012345678" || !strings.Contains(wakePrompts[0]["prompt"], "Vanadis build build-act-complete completed") {
				t.Errorf("unexpected wake prompt dispatched: %+v", wakePrompts)
			}

			// 3. Repeat complete -> noop
			recNoop := httptest.NewRecorder()
			muxAct.ServeHTTP(recNoop, httptest.NewRequest(http.MethodPost, "/chunks/complete", strings.NewReader(`{"chunk_id":"chunk-act-2"}`)))
			if recNoop.Code != http.StatusOK {
				t.Fatalf("expected 200 OK for noop, got %d: %s", recNoop.Code, recNoop.Body.String())
			}
			var resNoop map[string]any
			_ = json.Unmarshal(recNoop.Body.Bytes(), &resNoop)
			if resNoop["status"] != "noop" || resNoop["build_id"] != bActID {
				t.Errorf("expected noop result, got %+v", resNoop)
			}

			// 4. Missing chunk_id -> 400
			recMissing := httptest.NewRecorder()
			muxAct.ServeHTTP(recMissing, httptest.NewRequest(http.MethodPost, "/chunks/complete", strings.NewReader(`{"chunk_id":""}`)))
			if recMissing.Code != http.StatusBadRequest {
				t.Errorf("expected 400 on empty chunk_id, got %d", recMissing.Code)
			}

			// 5. Invalid JSON -> 400
			recBadJSON := httptest.NewRecorder()
			muxAct.ServeHTTP(recBadJSON, httptest.NewRequest(http.MethodPost, "/chunks/complete", strings.NewReader(`{invalid`)))
			if recBadJSON.Code != http.StatusBadRequest {
				t.Errorf("expected 400 on invalid JSON, got %d", recBadJSON.Code)
			}

			// 6. Non-POST -> 405
			recMethod := httptest.NewRecorder()
			muxAct.ServeHTTP(recMethod, httptest.NewRequest(http.MethodGet, "/chunks/complete", nil))
			if recMethod.Code != http.StatusMethodNotAllowed {
				t.Errorf("expected 405 on GET, got %d", recMethod.Code)
			}

			// 7. Non-existent chunk -> 404
			rec404 := httptest.NewRecorder()
			muxAct.ServeHTTP(rec404, httptest.NewRequest(http.MethodPost, "/chunks/complete", strings.NewReader(`{"chunk_id":"nonexistent"}`)))
			if rec404.Code != http.StatusNotFound {
				t.Errorf("expected 404 on nonexistent chunk, got %d", rec404.Code)
			}

			// 8. Nil store -> 500
			srvNil := NewVanadisServer(Config{}, nil, nil, nil)
			recNil := httptest.NewRecorder()
			SetupMux(srvNil).ServeHTTP(recNil, httptest.NewRequest(http.MethodPost, "/chunks/complete", strings.NewReader(`{"chunk_id":"c1"}`)))
			if recNil.Code != http.StatusInternalServerError {
				t.Errorf("expected 500 on nil store, got %d", recNil.Code)
			}
		})

		// POST /chunks/fail tests
		t.Run("POST_ChunksFail", func(t *testing.T) {
			sFail := newMemoryVanadisStore()
			bFailID := "build-act-fail"
			bFail := mkBuild(bFailID, "123456789012345678", "Fail Build", "Test Action Failure", "running")
			cFail1 := VanadisChunk{
				ID:         "chunk-fail-1",
				BuildID:    bFailID,
				ChunkIndex: 0,
				Mode:       "action",
				Title:      "Action Step Fail",
				Status:     "running",
				Effort:     "low",
				CreatedAt:  time.Now().UTC(),
				UpdatedAt:  time.Now().UTC(),
			}
			_ = sFail.CreateBuild(context.Background(), bFail, []VanadisChunk{cFail1})

			var failPrompts []map[string]string
			var widgetBanners []string
			tsBrainFail := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/internal/widget/sync":
					var wr map[string]string
					_ = json.NewDecoder(r.Body).Decode(&wr)
					widgetBanners = append(widgetBanners, wr["banner"])
					httpkit.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
				case "/prompt":
					var p map[string]string
					_ = json.NewDecoder(r.Body).Decode(&p)
					failPrompts = append(failPrompts, p)
					httpkit.WriteJSON(w, http.StatusAccepted, map[string]string{"status": "accepted"})
				default:
					http.NotFound(w, r)
				}
			}))
			defer tsBrainFail.Close()

			srvFail := NewVanadisServer(Config{BrainURL: tsBrainFail.URL, SynapseURL: tsBrainFail.URL}, sFail, nil, tsBrainFail.Client())
			muxFail := SetupMux(srvFail)

			// 1. Success failing chunk
			rec1 := httptest.NewRecorder()
			muxFail.ServeHTTP(rec1, httptest.NewRequest(http.MethodPost, "/chunks/fail", strings.NewReader(`{"chunk_id":"chunk-fail-1","reason":"hardware timeout"}`)))
			if rec1.Code != http.StatusOK {
				t.Fatalf("expected 200 OK on chunk fail, got %d: %s", rec1.Code, rec1.Body.String())
			}
			var res1 map[string]any
			_ = json.Unmarshal(rec1.Body.Bytes(), &res1)
			if res1["status"] != "failed" || res1["failed"] != true || res1["chunk_id"] != "chunk-fail-1" || res1["build_id"] != bFailID {
				t.Errorf("unexpected fail response: %+v", res1)
			}
			if len(widgetBanners) == 0 || !strings.Contains(widgetBanners[len(widgetBanners)-1], "hardware timeout") {
				t.Errorf("expected failure banner in widget sync, got %+v", widgetBanners)
			}
			if len(failPrompts) != 1 || !strings.Contains(failPrompts[0]["prompt"], "[VANADIS BUILD FAILED]") || !strings.Contains(failPrompts[0]["prompt"], "hardware timeout") {
				t.Errorf("expected failure wake prompt, got %+v", failPrompts)
			}

			// 1b. Shutdown interruption reasons return 200 with failed: false and preserve statuses
			sFailSD := newMemoryVanadisStore()
			bFailSDID := "build-act-fail-shutdown"
			bFailSD := mkBuild(bFailSDID, "123456789012345678", "Fail Build SD", "Test Shutdown Interruption", "running")
			cFailSD := VanadisChunk{
				ID:         "chunk-fail-sd",
				BuildID:    bFailSDID,
				ChunkIndex: 0,
				Mode:       "action",
				Title:      "Action Step Shutdown",
				Status:     "in_progress",
				Effort:     "low",
				CreatedAt:  time.Now().UTC(),
				UpdatedAt:  time.Now().UTC(),
			}
			_ = sFailSD.CreateBuild(context.Background(), bFailSD, []VanadisChunk{cFailSD})
			srvFailSD := NewVanadisServer(Config{BrainURL: tsBrainFail.URL, SynapseURL: tsBrainFail.URL}, sFailSD, nil, tsBrainFail.Client())
			muxFailSD := SetupMux(srvFailSD)

			shutdownReasons := []string{"interrupted by shutdown", "context cancelled during execution", "  Interrupted By Shutdown  "}
			for _, rReason := range shutdownReasons {
				recSD := httptest.NewRecorder()
				payload := fmt.Sprintf(`{"chunk_id":"chunk-fail-sd","reason":%q}`, rReason)
				muxFailSD.ServeHTTP(recSD, httptest.NewRequest(http.MethodPost, "/chunks/fail", strings.NewReader(payload)))
				if recSD.Code != http.StatusOK {
					t.Fatalf("expected 200 OK on shutdown fail for reason %q, got %d: %s", rReason, recSD.Code, recSD.Body.String())
				}
				var resSD map[string]any
				_ = json.Unmarshal(recSD.Body.Bytes(), &resSD)
				if resSD["failed"] != false || resSD["chunk_id"] != "chunk-fail-sd" || resSD["build_id"] != bFailSDID {
					t.Errorf("expected failed=false for shutdown reason %q, got %+v", rReason, resSD)
				}
				cCheck, bCheck, err := sFailSD.GetChunk(context.Background(), "chunk-fail-sd")
				if err != nil {
					t.Fatalf("GetChunk error: %v", err)
				}
				if cCheck.Status != "in_progress" {
					t.Errorf("expected chunk status in_progress for reason %q, got %q", rReason, cCheck.Status)
				}
				if bCheck.Status != "running" {
					t.Errorf("expected build status running for reason %q, got %q", rReason, bCheck.Status)
				}
			}

			// 2. Repeat fail -> already failed (failed: false)
			recRepeat := httptest.NewRecorder()
			muxFail.ServeHTTP(recRepeat, httptest.NewRequest(http.MethodPost, "/chunks/fail", strings.NewReader(`{"chunk_id":"chunk-fail-1","reason":"hardware timeout"}`)))
			if recRepeat.Code != http.StatusOK {
				t.Fatalf("expected 200 OK on repeat fail, got %d", recRepeat.Code)
			}
			var resRepeat map[string]any
			_ = json.Unmarshal(recRepeat.Body.Bytes(), &resRepeat)
			if resRepeat["failed"] != false {
				t.Errorf("expected failed=false on already failed chunk, got %+v", resRepeat)
			}

			// 3. Validation errors
			for _, bad := range []string{
				`{}`,
				`{"chunk_id":""}`,
				`{"chunk_id":"chunk-fail-1","reason":""}`,
				`{invalid-json`,
			} {
				recBad := httptest.NewRecorder()
				muxFail.ServeHTTP(recBad, httptest.NewRequest(http.MethodPost, "/chunks/fail", strings.NewReader(bad)))
				if recBad.Code != http.StatusBadRequest {
					t.Errorf("expected 400 for payload %s, got %d", bad, recBad.Code)
				}
			}

			// 4. Method not allowed (GET) -> 405
			recMethod := httptest.NewRecorder()
			muxFail.ServeHTTP(recMethod, httptest.NewRequest(http.MethodGet, "/chunks/fail", nil))
			if recMethod.Code != http.StatusMethodNotAllowed {
				t.Errorf("expected 405 on GET, got %d", recMethod.Code)
			}

			// 5. Non-existent chunk -> 404
			rec404 := httptest.NewRecorder()
			muxFail.ServeHTTP(rec404, httptest.NewRequest(http.MethodPost, "/chunks/fail", strings.NewReader(`{"chunk_id":"nonexistent","reason":"err"}`)))
			if rec404.Code != http.StatusNotFound {
				t.Errorf("expected 404 on nonexistent chunk, got %d", rec404.Code)
			}

			// 6. Nil store -> 500
			srvNil := NewVanadisServer(Config{}, nil, nil, nil)
			recNil := httptest.NewRecorder()
			SetupMux(srvNil).ServeHTTP(recNil, httptest.NewRequest(http.MethodPost, "/chunks/fail", strings.NewReader(`{"chunk_id":"c1","reason":"err"}`)))
			if recNil.Code != http.StatusInternalServerError {
				t.Errorf("expected 500 on nil store, got %d", recNil.Code)
			}
		})
	})
}

func TestServer_MutationEndpoints(t *testing.T) {
	tests := []struct {
		name       string
		nilStore   bool
		useErrBody bool
		seedBuild  *VanadisBuild
		seedChunks []VanadisChunk
		method     string
		path       string
		body       string
		wantCode   int
		wantWidget bool
	}{
		// PATCH /builds/{id}
		{"patch_build_nil_store", true, false, nil, nil, http.MethodPatch, "/builds/b1", `{"title":"T"}`, http.StatusInternalServerError, false},
		{"patch_build_err_reader", false, true, &VanadisBuild{ID: "b1", Status: "running"}, nil, http.MethodPatch, "/builds/b1", "", http.StatusBadRequest, false},
		{"patch_build_invalid_json", false, false, nil, nil, http.MethodPatch, "/builds/b1", `{invalid-json`, http.StatusBadRequest, false},
		{"patch_build_empty_fields", false, false, nil, nil, http.MethodPatch, "/builds/b1", `{"title":""}`, http.StatusBadRequest, false},
		{"patch_build_not_found", false, false, nil, nil, http.MethodPatch, "/builds/b-missing", `{"title":"T"}`, http.StatusNotFound, false},
		{"patch_build_conflict", false, false, &VanadisBuild{ID: "b-done", Status: "completed"}, nil, http.MethodPatch, "/builds/b-done", `{"title":"T"}`, http.StatusConflict, false},
		{"patch_build_ok_no_title", false, false, &VanadisBuild{ID: "b1", Status: "running"}, nil, http.MethodPatch, "/builds/b1", `{"objective":"New Obj"}`, http.StatusOK, false},
		{"patch_build_ok_with_title_syncs_widget", false, false, &VanadisBuild{ID: "b1", Status: "running"}, nil, http.MethodPatch, "/builds/b1", `{"title":"New Title"}`, http.StatusOK, true},

		// GET /builds/{id}/chunks/{chunk_ref}
		{"get_chunk_nil_store", true, false, nil, nil, http.MethodGet, "/builds/b1/chunks/c1", "", http.StatusInternalServerError, false},
		{"get_chunk_build_not_found", false, false, nil, nil, http.MethodGet, "/builds/b-missing/chunks/c1", "", http.StatusNotFound, false},
		{"get_chunk_not_found", false, false, &VanadisBuild{ID: "b1", Status: "running"}, []VanadisChunk{{ID: "c1"}}, http.MethodGet, "/builds/b1/chunks/c999", "", http.StatusNotFound, false},
		{"get_chunk_ok_by_id", false, false, &VanadisBuild{ID: "b1", Status: "running"}, []VanadisChunk{{ID: "c1", ChunkIndex: 0, Title: "C1"}}, http.MethodGet, "/builds/b1/chunks/c1", "", http.StatusOK, false},
		{"get_chunk_ok_by_index", false, false, &VanadisBuild{ID: "b1", Status: "running"}, []VanadisChunk{{ID: "c1", ChunkIndex: 0, Title: "C1"}}, http.MethodGet, "/builds/b1/chunks/0", "", http.StatusOK, false},

		// PATCH /builds/{id}/chunks/{chunk_ref}
		{"patch_chunk_nil_store", true, false, nil, nil, http.MethodPatch, "/builds/b1/chunks/c1", `{"title":"T"}`, http.StatusInternalServerError, false},
		{"patch_chunk_err_reader", false, true, &VanadisBuild{ID: "b1", Status: "running"}, []VanadisChunk{{ID: "c1", Status: "pending"}}, http.MethodPatch, "/builds/b1/chunks/c1", "", http.StatusBadRequest, false},
		{"patch_chunk_invalid_json", false, false, nil, nil, http.MethodPatch, "/builds/b1/chunks/c1", `{invalid-json`, http.StatusBadRequest, false},
		{"patch_chunk_empty_update", false, false, nil, nil, http.MethodPatch, "/builds/b1/chunks/c1", `{"title":""}`, http.StatusBadRequest, false},
		{"patch_chunk_not_found", false, false, &VanadisBuild{ID: "b1", Status: "running"}, nil, http.MethodPatch, "/builds/b1/chunks/c1", `{"title":"T"}`, http.StatusNotFound, false},
		{"patch_chunk_conflict", false, false, &VanadisBuild{ID: "b1", Status: "running"}, []VanadisChunk{{ID: "c1", Status: "running"}}, http.MethodPatch, "/builds/b1/chunks/c1", `{"title":"T"}`, http.StatusConflict, false},
		{"patch_chunk_ok_syncs_widget", false, false, &VanadisBuild{ID: "b1", Status: "running"}, []VanadisChunk{{ID: "c1", Status: "pending", ChunkIndex: 0}}, http.MethodPatch, "/builds/b1/chunks/c1", `{"title":"New Chunk Title"}`, http.StatusOK, true},

		// POST /builds/{id}/chunks
		{"append_chunks_nil_store", true, false, nil, nil, http.MethodPost, "/builds/b1/chunks", `{"chunks":[{"title":"T","instructions":"I"}]}`, http.StatusInternalServerError, false},
		{"append_chunks_err_reader", false, true, &VanadisBuild{ID: "b1", Status: "running"}, nil, http.MethodPost, "/builds/b1/chunks", "", http.StatusBadRequest, false},
		{"append_chunks_invalid_json", false, false, nil, nil, http.MethodPost, "/builds/b1/chunks", `{invalid-json`, http.StatusBadRequest, false},
		{"append_chunks_empty_list", false, false, nil, nil, http.MethodPost, "/builds/b1/chunks", `{"chunks":[]}`, http.StatusBadRequest, false},
		{"append_chunks_build_not_found", false, false, nil, nil, http.MethodPost, "/builds/b-missing/chunks", `{"chunks":[{"repo":"aerial","title":"T","instructions":"I"}]}`, http.StatusNotFound, false},
		{"append_chunks_conflict", false, false, &VanadisBuild{ID: "b-done", Status: "completed"}, nil, http.MethodPost, "/builds/b-done/chunks", `{"chunks":[{"repo":"aerial","title":"T","instructions":"I"}]}`, http.StatusConflict, false},
		{"append_chunks_ok_syncs_widget", false, false, &VanadisBuild{ID: "b1", Status: "running"}, nil, http.MethodPost, "/builds/b1/chunks", `{"chunks":[{"repo":"aerial","title":"T","instructions":"I"}]}`, http.StatusOK, true},

		// DELETE /builds/{id}/chunks/{chunk_ref}
		{"delete_chunk_nil_store", true, false, nil, nil, http.MethodDelete, "/builds/b1/chunks/c1", "", http.StatusInternalServerError, false},
		{"delete_chunk_not_found", false, false, &VanadisBuild{ID: "b1", Status: "running"}, nil, http.MethodDelete, "/builds/b1/chunks/c1", "", http.StatusNotFound, false},
		{"delete_chunk_conflict_only_chunk", false, false, &VanadisBuild{ID: "b1", Status: "running"}, []VanadisChunk{{ID: "c1", Status: "pending"}}, http.MethodDelete, "/builds/b1/chunks/c1", "", http.StatusConflict, false},
		{"delete_chunk_ok_syncs_widget", false, false, &VanadisBuild{ID: "b1", Status: "running"}, []VanadisChunk{
			{ID: "c1", Status: "running", ChunkIndex: 0},
			{ID: "c2", Status: "pending", ChunkIndex: 1},
		}, http.MethodDelete, "/builds/b1/chunks/c2", "", http.StatusOK, true},

		// Method and path errors
		{"method_not_allowed_put_build", false, false, &VanadisBuild{ID: "b1", Status: "running"}, nil, http.MethodPut, "/builds/b1", "", http.StatusMethodNotAllowed, false},
		{"method_not_allowed_get_chunks_subpath", false, false, &VanadisBuild{ID: "b1", Status: "running"}, nil, http.MethodGet, "/builds/b1/chunks", "", http.StatusMethodNotAllowed, false},
		{"method_not_allowed_post_chunk_ref", false, false, &VanadisBuild{ID: "b1", Status: "running"}, []VanadisChunk{{ID: "c1"}}, http.MethodPost, "/builds/b1/chunks/c1", "", http.StatusMethodNotAllowed, false},
		{"bad_request_empty_chunk_ref_with_slash_get", false, false, &VanadisBuild{ID: "b1", Status: "running"}, nil, http.MethodGet, "/builds/b1/chunks/", "", http.StatusBadRequest, false},
		{"bad_request_empty_chunk_ref_space", false, false, &VanadisBuild{ID: "b1", Status: "running"}, nil, http.MethodGet, "/builds/b1/chunks/%20", "", http.StatusBadRequest, false},
		{"bad_request_empty_build_id_chunks", false, false, nil, nil, http.MethodPost, "/builds/%20/chunks", `{"chunks":[{"title":"T","instructions":"I"}]}`, http.StatusBadRequest, false},
		{"not_found_unknown_subpath", false, false, &VanadisBuild{ID: "b1", Status: "running"}, nil, http.MethodGet, "/builds/b1/unknown", "", http.StatusNotFound, false},
		{"not_found_deep_unknown_subpath", false, false, &VanadisBuild{ID: "b1", Status: "running"}, nil, http.MethodGet, "/builds/b1/chunks/c1/deep", "", http.StatusNotFound, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var widgetSyncCalls int
			tsWidget := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/internal/widget/sync" {
					widgetSyncCalls++
					httpkit.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
					return
				}
				http.NotFound(w, r)
			}))
			defer tsWidget.Close()

			var store VanadisStore
			if !tc.nilStore {
				mem := newMemoryVanadisStore()
				if tc.seedBuild != nil {
					mem.builds[tc.seedBuild.ID] = *tc.seedBuild
				}
				if len(tc.seedChunks) > 0 {
					mem.chunks[tc.seedBuild.ID] = tc.seedChunks
				}
				store = mem
			}

			srv := NewVanadisServer(Config{SynapseURL: tsWidget.URL}, store, nil, tsWidget.Client())
			mux := SetupMux(srv)

			var req *http.Request
			switch {
			case tc.useErrBody:
				req = httptest.NewRequest(tc.method, tc.path, errorReader{})
			case tc.body != "":
				req = httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
				req.Header.Set("Content-Type", "application/json")
			default:
				req = httptest.NewRequest(tc.method, tc.path, nil)
			}

			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			if rec.Code != tc.wantCode {
				t.Fatalf("got status %d, want %d: %s", rec.Code, tc.wantCode, rec.Body.String())
			}
			if tc.wantWidget && widgetSyncCalls == 0 {
				t.Errorf("expected widget sync call, got 0")
			}
			if !tc.wantWidget && widgetSyncCalls > 0 {
				t.Errorf("unexpected widget sync call, got %d", widgetSyncCalls)
			}
		})
	}
}
