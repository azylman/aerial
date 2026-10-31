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
		{"pending", "synced", "synced"},
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

	streamingTests := []struct {
		in   string
		want bool
	}{
		{"open", true},
		{"pr_opened", true},
		{"merged", true},
		{"synced", true},
		{"deploying", true},
		{"deployed", false},
		{"failed", false},
		{"pending", false},
		{"running", false},
		{"", false},
		{"  OPEN  ", true},
	}
	for _, tc := range streamingTests {
		t.Run("streaming_"+tc.in, func(t *testing.T) {
			if got := isVanadisChunkStreamingPhase(tc.in); got != tc.want {
				t.Errorf("isVanadisChunkStreamingPhase(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}

	syncWidgetTests := []struct {
		name           string
		build          VanadisBuild
		chunks         []VanadisChunk
		req            SyncChunkPRWidgetRequest
		wantStatus     string
		wantSkipSink   bool
		wantActiveStep string
	}{
		{
			name: "preserves active step and allows sink when chunk is open and build running with message",
			build: VanadisBuild{
				ID:              "build-1",
				Status:          "running",
				StatusMessageID: "msg-123",
				Title:           "Test Build",
			},
			chunks: []VanadisChunk{
				{ID: "c1", ChunkIndex: 0, PRNumber: 101, Status: "open"},
			},
			req: SyncChunkPRWidgetRequest{
				BuildID:    "build-1",
				Repo:       "azylman/aerial",
				PRNumber:   101,
				PRStatus:   "open",
				ActiveStep: "Waiting on CI (2/5 passed)",
				PRFound:    true,
			},
			wantStatus:     "running",
			wantSkipSink:   false,
			wantActiveStep: "Waiting on CI (2/5 passed)",
		},
		{
			name: "sets active step on pr_opened chunk",
			build: VanadisBuild{
				ID:              "build-2",
				Status:          "running",
				StatusMessageID: "msg-456",
				Title:           "Test Build 2",
			},
			chunks: []VanadisChunk{
				{ID: "c2", ChunkIndex: 0, PRNumber: 102, Status: "pr_opened"},
			},
			req: SyncChunkPRWidgetRequest{
				BuildID:    "build-2",
				Repo:       "azylman/aerial",
				PRNumber:   102,
				PRStatus:   "open",
				ActiveStep: "Waiting on CI (3/3 passed)",
				PRFound:    true,
			},
			wantStatus:     "running",
			wantSkipSink:   false,
			wantActiveStep: "Waiting on CI (3/3 passed)",
		},
		{
			name: "omits skipsink when build status message ID is empty",
			build: VanadisBuild{
				ID:              "build-3",
				Status:          "running",
				StatusMessageID: "",
				Title:           "Test Build 3",
			},
			chunks: []VanadisChunk{
				{ID: "c3", ChunkIndex: 0, PRNumber: 103, Status: "open"},
			},
			req: SyncChunkPRWidgetRequest{
				BuildID:    "build-3",
				Repo:       "azylman/aerial",
				PRNumber:   103,
				PRStatus:   "open",
				ActiveStep: "Waiting on CI (1/2 passed)",
				PRFound:    true,
			},
			wantStatus:     "running",
			wantSkipSink:   false,
			wantActiveStep: "Waiting on CI (1/2 passed)",
		},
		{
			name: "omits skipsink and active step when pr status is failed",
			build: VanadisBuild{
				ID:              "build-4",
				Status:          "running",
				StatusMessageID: "msg-789",
				Title:           "Test Build 4",
			},
			chunks: []VanadisChunk{
				{ID: "c4", ChunkIndex: 0, PRNumber: 104, Status: "pr_opened"},
			},
			req: SyncChunkPRWidgetRequest{
				BuildID:    "build-4",
				Repo:       "azylman/aerial",
				PRNumber:   104,
				PRStatus:   "failed",
				ActiveStep: "Waiting on CI (0/2 passed)",
				PRFound:    true,
			},
			wantStatus:     "failed",
			wantSkipSink:   false,
			wantActiveStep: "",
		},
		{
			name: "omits skipsink when active step is whitespace",
			build: VanadisBuild{
				ID:              "build-5",
				Status:          "running",
				StatusMessageID: "msg-999",
				Title:           "Test Build 5",
			},
			chunks: []VanadisChunk{
				{ID: "c5", ChunkIndex: 0, PRNumber: 105, Status: "open"},
			},
			req: SyncChunkPRWidgetRequest{
				BuildID:    "build-5",
				Repo:       "azylman/aerial",
				PRNumber:   105,
				PRStatus:   "open",
				ActiveStep: "   ",
				PRFound:    true,
			},
			wantStatus:     "running",
			wantSkipSink:   false,
			wantActiveStep: "",
		},
		{
			name: "preserves active step and allows sink when chunk is merged",
			build: VanadisBuild{
				ID:              "build-merged",
				Status:          "running",
				StatusMessageID: "msg-merged",
				Title:           "Test Build Merged",
			},
			chunks: []VanadisChunk{
				{ID: "c-merged", ChunkIndex: 0, PRNumber: 106, Status: "open"},
			},
			req: SyncChunkPRWidgetRequest{
				BuildID:    "build-merged",
				Repo:       "azylman/aerial",
				PRNumber:   106,
				PRStatus:   "merged",
				ActiveStep: "Building container images (1/2 completed)",
				PRFound:    true,
			},
			wantStatus:     "running",
			wantSkipSink:   false,
			wantActiveStep: "Building container images (1/2 completed)",
		},
		{
			name: "preserves active step and allows sink when chunk is synced",
			build: VanadisBuild{
				ID:              "build-synced",
				Status:          "running",
				StatusMessageID: "msg-synced",
				Title:           "Test Build Synced",
			},
			chunks: []VanadisChunk{
				{ID: "c-synced", ChunkIndex: 0, PRNumber: 107, Status: "merged"},
			},
			req: SyncChunkPRWidgetRequest{
				BuildID:    "build-synced",
				Repo:       "azylman/aerial",
				PRNumber:   107,
				PRStatus:   "synced",
				ActiveStep: "Building container images (2/2 completed)",
				PRFound:    true,
			},
			wantStatus:     "running",
			wantSkipSink:   false,
			wantActiveStep: "Building container images (2/2 completed)",
		},
		{
			name: "preserves active step and allows sink when chunk is deploying",
			build: VanadisBuild{
				ID:              "build-deploying",
				Status:          "running",
				StatusMessageID: "msg-deploying",
				Title:           "Test Build Deploying",
			},
			chunks: []VanadisChunk{
				{ID: "c-deploying", ChunkIndex: 0, PRNumber: 108, Status: "synced"},
			},
			req: SyncChunkPRWidgetRequest{
				BuildID:    "build-deploying",
				Repo:       "azylman/aerial",
				PRNumber:   108,
				PRStatus:   "deploying",
				ActiveStep: "`eri`: 🔄 starting",
				PRFound:    true,
			},
			wantStatus:     "running",
			wantSkipSink:   false,
			wantActiveStep: "`eri`: 🔄 starting",
		},
		{
			name: "clears active step on deployed chunk",
			build: VanadisBuild{
				ID:              "build-deployed",
				Status:          "running",
				StatusMessageID: "msg-deployed",
				Title:           "Test Build Deployed",
			},
			chunks: []VanadisChunk{
				{ID: "c-deployed", ChunkIndex: 0, PRNumber: 109, Status: "deploying"},
			},
			req: SyncChunkPRWidgetRequest{
				BuildID:    "build-deployed",
				Repo:       "azylman/aerial",
				PRNumber:   109,
				PRStatus:   "deployed",
				ActiveStep: "All jobs healthy",
				PRFound:    true,
			},
			wantStatus:     "running",
			wantSkipSink:   false,
			wantActiveStep: "",
		},
	}
	for _, tc := range syncWidgetTests {
		t.Run("sync_widget_"+tc.name, func(t *testing.T) {
			got := buildSyncWidgetRequest(tc.build, tc.chunks, tc.req)
			if got.Status != tc.wantStatus {
				t.Errorf("buildSyncWidgetRequest Status = %q, want %q", got.Status, tc.wantStatus)
			}
			if got.SkipSink != tc.wantSkipSink {
				t.Errorf("buildSyncWidgetRequest SkipSink = %v, want %v", got.SkipSink, tc.wantSkipSink)
			}
			if len(got.Chunks) > 0 && got.Chunks[0].ActiveStep != tc.wantActiveStep {
				t.Errorf("buildSyncWidgetRequest Chunk[0].ActiveStep = %q, want %q", got.Chunks[0].ActiveStep, tc.wantActiveStep)
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
			if _, err := s.UpdateBuild(ctx, "b1", UpdateBuildRequest{Title: "T"}); err == nil {
				t.Error("expected error on UpdateBuild")
			}
			if _, err := s.UpdateChunk(ctx, "b1", "c1", UpdateChunkRequest{Title: "T"}); err == nil {
				t.Error("expected error on UpdateChunk")
			}
			if _, err := s.AppendChunks(ctx, "b1", []CreateChunkInput{{Title: "T", Instructions: "I"}}); err == nil {
				t.Error("expected error on AppendChunks")
			}
			if _, _, err := s.RemoveChunk(ctx, "b1", "c1"); err == nil {
				t.Error("expected error on RemoveChunk")
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

		// Update next chunk error
		sUpdateNextErr := newMockStore(mockDB{
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
							{ID: "c1", ChunkIndex: 0, Status: "merged", StartedAt: time.Now()},
							{ID: "c2", ChunkIndex: 1, Status: "pending"},
						}}, nil
					},
					execFn: func(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
						if strings.Contains(sql, "UPDATE vanadis_chunks") && strings.Contains(sql, "status = 'running'") {
							return pgconn.CommandTag{}, errors.New("update next chunk err")
						}
						return pgconn.NewCommandTag("UPDATE 1"), nil
					},
				}, nil
			},
		})
		if _, err := sUpdateNextErr.AdvanceChunkByPR(ctx, "aerial", 101); err == nil {
			t.Error("expected error on update next chunk error")
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

func TestResolveChunkRef(t *testing.T) {
	chunks := []VanadisChunk{
		{ID: "chunk-a", ChunkIndex: 0},
		{ID: "chunk-b", ChunkIndex: 1},
		{ID: "10", ChunkIndex: 2},
	}
	tests := []struct {
		name     string
		chunks   []VanadisChunk
		chunkRef string
		wantIdx  int
	}{
		{"empty_ref", chunks, "", -1},
		{"whitespace_ref", chunks, "   ", -1},
		{"match_id_exact", chunks, "chunk-a", 0},
		{"match_id_with_whitespace", chunks, "  chunk-b  ", 1},
		{"match_index_string", chunks, "0", 0},
		{"match_index_second", chunks, "1", 1},
		{"match_id_takes_precedence_over_index", chunks, "10", 2},
		{"no_match_index_out_of_bounds", chunks, "99", -1},
		{"no_match_unknown_id", chunks, "unknown", -1},
		{"empty_slice", nil, "chunk-a", -1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := resolveChunkRef(tc.chunks, tc.chunkRef)
			if got != tc.wantIdx {
				t.Errorf("resolveChunkRef() = %d, want %d", got, tc.wantIdx)
			}
		})
	}
}

func TestVanadisStore_UpdateBuild(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name        string
		storeErr    error
		seedBuild   *VanadisBuild
		buildID     string
		req         UpdateBuildRequest
		wantErrIs   error
		wantErr     bool
		wantTitle   string
		wantObj     string
		wantContext string
	}{
		{
			name:      "store_error",
			storeErr:  errors.New("boom"),
			buildID:   "b1",
			req:       UpdateBuildRequest{Title: "New"},
			wantErr:   true,
			wantErrIs: nil,
		},
		{
			name:      "empty_build_id",
			buildID:   "",
			req:       UpdateBuildRequest{Title: "New"},
			wantErr:   true,
			wantErrIs: ErrInvalidBuildInput,
		},
		{
			name:      "all_empty_fields",
			buildID:   "b1",
			req:       UpdateBuildRequest{Title: "   ", Objective: "  ", SharedContext: ""},
			wantErr:   true,
			wantErrIs: ErrInvalidBuildInput,
		},
		{
			name:      "build_not_found",
			buildID:   "missing-build",
			req:       UpdateBuildRequest{Title: "New Title"},
			wantErr:   true,
			wantErrIs: ErrBuildNotFound,
		},
		{
			name:      "build_not_running_conflict",
			seedBuild: &VanadisBuild{ID: "b-conflict", Status: "completed"},
			buildID:   "b-conflict",
			req:       UpdateBuildRequest{Title: "New Title"},
			wantErr:   true,
			wantErrIs: ErrBuildConflict,
		},
		{
			name:        "success_update_title_only",
			seedBuild:   &VanadisBuild{ID: "b-ok1", Title: "Old Title", Objective: "Original Obj", SharedContext: "Original Ctx", Status: "running"},
			buildID:     "b-ok1",
			req:         UpdateBuildRequest{Title: "Updated Title"},
			wantTitle:   "Updated Title",
			wantObj:     "Original Obj",
			wantContext: "Original Ctx",
		},
		{
			name:        "success_update_all_fields",
			seedBuild:   &VanadisBuild{ID: "b-ok2", Title: "Old Title", Objective: "Old Obj", Status: "running"},
			buildID:     "b-ok2",
			req:         UpdateBuildRequest{Title: "New Title", Objective: "New Obj", SharedContext: "New Ctx"},
			wantTitle:   "New Title",
			wantObj:     "New Obj",
			wantContext: "New Ctx",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newMemoryVanadisStore()
			if tc.storeErr != nil {
				s.updateBuildErr = tc.storeErr
			}
			if tc.seedBuild != nil {
				s.builds[tc.seedBuild.ID] = *tc.seedBuild
			}
			got, err := s.UpdateBuild(ctx, tc.buildID, tc.req)
			if tc.wantErrIs != nil {
				if !errors.Is(err, tc.wantErrIs) {
					t.Fatalf("expected error %v, got %v", tc.wantErrIs, err)
				}
				return
			}
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.Title != tc.wantTitle || got.Objective != tc.wantObj || got.SharedContext != tc.wantContext {
				t.Errorf("got title=%q obj=%q ctx=%q, want title=%q obj=%q ctx=%q", got.Title, got.Objective, got.SharedContext, tc.wantTitle, tc.wantObj, tc.wantContext)
			}
		})
	}
}

func TestVanadisStore_UpdateChunk(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name         string
		storeErr     error
		seedBuild    *VanadisBuild
		seedChunks   []VanadisChunk
		buildID      string
		chunkRef     string
		req          UpdateChunkRequest
		wantErrIs    error
		wantErr      bool
		wantRepo     string
		wantTitle    string
		wantInstr    string
		wantEffort   string
		wantFilesLen int
		wantSymsLen  int
	}{
		{
			name:      "store_error",
			storeErr:  errors.New("boom"),
			buildID:   "b1",
			chunkRef:  "c1",
			req:       UpdateChunkRequest{Title: "T"},
			wantErr:   true,
			wantErrIs: nil,
		},
		{
			name:      "empty_build_id",
			buildID:   "",
			chunkRef:  "c1",
			req:       UpdateChunkRequest{Title: "T"},
			wantErr:   true,
			wantErrIs: ErrInvalidBuildInput,
		},
		{
			name:      "empty_chunk_ref",
			buildID:   "b1",
			chunkRef:  "",
			req:       UpdateChunkRequest{Title: "T"},
			wantErr:   true,
			wantErrIs: ErrInvalidBuildInput,
		},
		{
			name:      "no_updated_fields",
			buildID:   "b1",
			chunkRef:  "c1",
			req:       UpdateChunkRequest{Title: "  ", Repo: "  ", Effort: ""},
			wantErr:   true,
			wantErrIs: ErrInvalidBuildInput,
		},
		{
			name:      "invalid_effort",
			buildID:   "b1",
			chunkRef:  "c1",
			req:       UpdateChunkRequest{Effort: "ultra-high"},
			wantErr:   true,
			wantErrIs: ErrInvalidBuildInput,
		},
		{
			name:      "build_not_found",
			buildID:   "missing-build",
			chunkRef:  "c1",
			req:       UpdateChunkRequest{Title: "New Title"},
			wantErr:   true,
			wantErrIs: ErrBuildNotFound,
		},
		{
			name:       "build_not_running_conflict",
			seedBuild:  &VanadisBuild{ID: "b-stopped", Status: "completed"},
			seedChunks: []VanadisChunk{{ID: "c1", BuildID: "b-stopped", Status: "pending"}},
			buildID:    "b-stopped",
			chunkRef:   "c1",
			req:        UpdateChunkRequest{Title: "New Title"},
			wantErr:    true,
			wantErrIs:  ErrBuildConflict,
		},
		{
			name:       "chunk_not_found",
			seedBuild:  &VanadisBuild{ID: "b-running", Status: "running"},
			seedChunks: []VanadisChunk{{ID: "c1", BuildID: "b-running", Status: "pending"}},
			buildID:    "b-running",
			chunkRef:   "c999",
			req:        UpdateChunkRequest{Title: "New Title"},
			wantErr:    true,
			wantErrIs:  ErrChunkNotFound,
		},
		{
			name:       "chunk_not_pending_conflict",
			seedBuild:  &VanadisBuild{ID: "b-running", Status: "running"},
			seedChunks: []VanadisChunk{{ID: "c1", BuildID: "b-running", Status: "running"}},
			buildID:    "b-running",
			chunkRef:   "c1",
			req:        UpdateChunkRequest{Title: "New Title"},
			wantErr:    true,
			wantErrIs:  ErrBuildConflict,
		},
		{
			name:       "success_partial_update",
			seedBuild:  &VanadisBuild{ID: "b-running", Status: "running"},
			seedChunks: []VanadisChunk{{ID: "c1", BuildID: "b-running", ChunkIndex: 0, Repo: "azylman/aerial", Title: "Old Title", Instructions: "Old Instr", Status: "pending", Effort: "medium"}},
			buildID:    "b-running",
			chunkRef:   "0",
			req: UpdateChunkRequest{
				Repo:          "mirrormere",
				Title:         "Updated Title",
				Instructions:  "Updated Instr",
				Effort:        "low",
				TargetFiles:   []string{"foo.go"},
				TargetSymbols: []string{"Bar"},
			},
			wantRepo:     "azylman/mirrormere",
			wantTitle:    "Updated Title",
			wantInstr:    "Updated Instr",
			wantEffort:   "low",
			wantFilesLen: 1,
			wantSymsLen:  1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newMemoryVanadisStore()
			if tc.storeErr != nil {
				s.updateChunkErr = tc.storeErr
			}
			if tc.seedBuild != nil {
				s.builds[tc.seedBuild.ID] = *tc.seedBuild
			}
			if len(tc.seedChunks) > 0 {
				s.chunks[tc.seedBuild.ID] = tc.seedChunks
			}
			got, err := s.UpdateChunk(ctx, tc.buildID, tc.chunkRef, tc.req)
			if tc.wantErrIs != nil {
				if !errors.Is(err, tc.wantErrIs) {
					t.Fatalf("expected error %v, got %v", tc.wantErrIs, err)
				}
				return
			}
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.Repo != tc.wantRepo || got.Title != tc.wantTitle || got.Instructions != tc.wantInstr || got.Effort != tc.wantEffort || len(got.TargetFiles) != tc.wantFilesLen || len(got.TargetSymbols) != tc.wantSymsLen {
				t.Errorf("unexpected updated chunk: %+v", got)
			}
		})
	}
}

func TestVanadisStore_AppendChunks(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name          string
		storeErr      error
		seedBuild     *VanadisBuild
		seedChunks    []VanadisChunk
		buildID       string
		inputs        []CreateChunkInput
		wantErrIs     error
		wantErr       bool
		wantCount     int
		wantFirstID   string
		wantFirstIdx  int
		wantFirstRepo string
	}{
		{
			name:      "store_error",
			storeErr:  errors.New("boom"),
			buildID:   "b1",
			inputs:    []CreateChunkInput{{Title: "T", Instructions: "I"}},
			wantErr:   true,
			wantErrIs: nil,
		},
		{
			name:      "empty_build_id",
			buildID:   "",
			inputs:    []CreateChunkInput{{Title: "T", Instructions: "I"}},
			wantErr:   true,
			wantErrIs: ErrInvalidBuildInput,
		},
		{
			name:      "empty_inputs",
			buildID:   "b1",
			inputs:    nil,
			wantErr:   true,
			wantErrIs: ErrInvalidBuildInput,
		},
		{
			name:      "empty_title",
			buildID:   "b1",
			inputs:    []CreateChunkInput{{Title: "", Instructions: "I"}},
			wantErr:   true,
			wantErrIs: ErrInvalidBuildInput,
		},
		{
			name:      "empty_instructions",
			buildID:   "b1",
			inputs:    []CreateChunkInput{{Title: "T", Instructions: "  "}},
			wantErr:   true,
			wantErrIs: ErrInvalidBuildInput,
		},
		{
			name:      "invalid_chunk_id",
			buildID:   "b1",
			inputs:    []CreateChunkInput{{ID: "bad:id", Title: "T", Instructions: "I"}},
			wantErr:   true,
			wantErrIs: ErrInvalidBuildInput,
		},
		{
			name:      "invalid_effort",
			buildID:   "b1",
			inputs:    []CreateChunkInput{{Title: "T", Instructions: "I", Effort: "gigantic"}},
			wantErr:   true,
			wantErrIs: ErrInvalidBuildInput,
		},
		{
			name:      "build_not_found",
			buildID:   "missing-b",
			inputs:    []CreateChunkInput{{Title: "T", Instructions: "I"}},
			wantErr:   true,
			wantErrIs: ErrBuildNotFound,
		},
		{
			name:       "build_not_running_conflict",
			seedBuild:  &VanadisBuild{ID: "b-done", Status: "completed"},
			seedChunks: nil,
			buildID:    "b-done",
			inputs:     []CreateChunkInput{{Title: "T", Instructions: "I"}},
			wantErr:    true,
			wantErrIs:  ErrBuildConflict,
		},
		{
			name:       "duplicate_explicit_id_conflict",
			seedBuild:  &VanadisBuild{ID: "b-run", Status: "running"},
			seedChunks: []VanadisChunk{{ID: "existing-c1", BuildID: "b-run", ChunkIndex: 0, Status: "pending"}},
			buildID:    "b-run",
			inputs:     []CreateChunkInput{{ID: "existing-c1", Title: "T", Instructions: "I"}},
			wantErr:    true,
			wantErrIs:  ErrBuildConflict,
		},
		{
			name:      "success_append_to_empty_build",
			seedBuild: &VanadisBuild{ID: "b-empty", Status: "running"},
			buildID:   "b-empty",
			inputs: []CreateChunkInput{
				{Title: "C1", Instructions: "Do C1", Repo: "aerial"},
				{Title: "C2", Instructions: "Do C2"},
			},
			wantCount:     2,
			wantFirstID:   "b-empty-chunk-1",
			wantFirstIdx:  0,
			wantFirstRepo: "azylman/aerial",
		},
		{
			name:      "success_append_inheriting_previous_repo_and_index",
			seedBuild: &VanadisBuild{ID: "b-exist", Status: "running"},
			seedChunks: []VanadisChunk{
				{ID: "b-exist-chunk-1", BuildID: "b-exist", ChunkIndex: 0, Repo: "azylman/mirrormere", Status: "running"},
			},
			buildID: "b-exist",
			inputs: []CreateChunkInput{
				{Title: "C2", Instructions: "Do C2"},
			},
			wantCount:     1,
			wantFirstID:   "b-exist-chunk-2",
			wantFirstIdx:  1,
			wantFirstRepo: "azylman/mirrormere",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newMemoryVanadisStore()
			if tc.storeErr != nil {
				s.appendChunksErr = tc.storeErr
			}
			if tc.seedBuild != nil {
				s.builds[tc.seedBuild.ID] = *tc.seedBuild
			}
			if len(tc.seedChunks) > 0 {
				s.chunks[tc.seedBuild.ID] = tc.seedChunks
			}
			got, err := s.AppendChunks(ctx, tc.buildID, tc.inputs)
			if tc.wantErrIs != nil {
				if !errors.Is(err, tc.wantErrIs) {
					t.Fatalf("expected error %v, got %v", tc.wantErrIs, err)
				}
				return
			}
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(got) != tc.wantCount {
				t.Fatalf("got %d chunks, want %d", len(got), tc.wantCount)
			}
			if got[0].ID != tc.wantFirstID || got[0].ChunkIndex != tc.wantFirstIdx || got[0].Repo != tc.wantFirstRepo {
				t.Errorf("chunk[0] mismatch: got %+v, want ID=%q Idx=%d Repo=%q", got[0], tc.wantFirstID, tc.wantFirstIdx, tc.wantFirstRepo)
			}
		})
	}
}

func TestVanadisStore_RemoveChunk(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name          string
		storeErr      error
		seedBuild     *VanadisBuild
		seedChunks    []VanadisChunk
		buildID       string
		chunkRef      string
		wantErrIs     error
		wantErr       bool
		wantRemovedID string
		wantRemainLen int
	}{
		{
			name:      "store_error",
			storeErr:  errors.New("boom"),
			buildID:   "b1",
			chunkRef:  "c1",
			wantErr:   true,
			wantErrIs: nil,
		},
		{
			name:      "empty_build_id",
			buildID:   "",
			chunkRef:  "c1",
			wantErr:   true,
			wantErrIs: ErrInvalidBuildInput,
		},
		{
			name:      "empty_chunk_ref",
			buildID:   "b1",
			chunkRef:  "",
			wantErr:   true,
			wantErrIs: ErrInvalidBuildInput,
		},
		{
			name:      "build_not_found",
			buildID:   "missing-b",
			chunkRef:  "c1",
			wantErr:   true,
			wantErrIs: ErrBuildNotFound,
		},
		{
			name:       "build_not_running_conflict",
			seedBuild:  &VanadisBuild{ID: "b-done", Status: "completed"},
			seedChunks: []VanadisChunk{{ID: "c1", BuildID: "b-done", ChunkIndex: 0, Status: "pending"}},
			buildID:    "b-done",
			chunkRef:   "c1",
			wantErr:    true,
			wantErrIs:  ErrBuildConflict,
		},
		{
			name:       "chunk_not_found",
			seedBuild:  &VanadisBuild{ID: "b-run", Status: "running"},
			seedChunks: []VanadisChunk{{ID: "c1", BuildID: "b-run", ChunkIndex: 0, Status: "pending"}},
			buildID:    "b-run",
			chunkRef:   "missing-c",
			wantErr:    true,
			wantErrIs:  ErrChunkNotFound,
		},
		{
			name:      "chunk_not_pending_conflict",
			seedBuild: &VanadisBuild{ID: "b-run", Status: "running"},
			seedChunks: []VanadisChunk{
				{ID: "c1", BuildID: "b-run", ChunkIndex: 0, Status: "running"},
				{ID: "c2", BuildID: "b-run", ChunkIndex: 1, Status: "pending"},
			},
			buildID:   "b-run",
			chunkRef:  "c1",
			wantErr:   true,
			wantErrIs: ErrBuildConflict,
		},
		{
			name:       "cannot_remove_only_chunk_conflict",
			seedBuild:  &VanadisBuild{ID: "b-run", Status: "running"},
			seedChunks: []VanadisChunk{{ID: "c1", BuildID: "b-run", ChunkIndex: 0, Status: "pending"}},
			buildID:    "b-run",
			chunkRef:   "c1",
			wantErr:    true,
			wantErrIs:  ErrBuildConflict,
		},
		{
			name:      "success_remove_middle_chunk_and_compact",
			seedBuild: &VanadisBuild{ID: "b-run", Status: "running"},
			seedChunks: []VanadisChunk{
				{ID: "c1", BuildID: "b-run", ChunkIndex: 0, Status: "running"},
				{ID: "c2", BuildID: "b-run", ChunkIndex: 1, Status: "pending"},
				{ID: "c3", BuildID: "b-run", ChunkIndex: 2, Status: "pending"},
			},
			buildID:       "b-run",
			chunkRef:      "1", // resolves to c2 by index
			wantRemovedID: "c2",
			wantRemainLen: 2,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newMemoryVanadisStore()
			if tc.storeErr != nil {
				s.removeChunkErr = tc.storeErr
			}
			if tc.seedBuild != nil {
				s.builds[tc.seedBuild.ID] = *tc.seedBuild
			}
			if len(tc.seedChunks) > 0 {
				s.chunks[tc.seedBuild.ID] = tc.seedChunks
			}
			removed, remaining, err := s.RemoveChunk(ctx, tc.buildID, tc.chunkRef)
			if tc.wantErrIs != nil {
				if !errors.Is(err, tc.wantErrIs) {
					t.Fatalf("expected error %v, got %v", tc.wantErrIs, err)
				}
				return
			}
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if removed.ID != tc.wantRemovedID {
				t.Errorf("removed.ID = %q, want %q", removed.ID, tc.wantRemovedID)
			}
			if len(remaining) != tc.wantRemainLen {
				t.Errorf("len(remaining) = %d, want %d", len(remaining), tc.wantRemainLen)
			}
		})
	}
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
		{"append_chunks_build_not_found", false, false, nil, nil, http.MethodPost, "/builds/b-missing/chunks", `{"chunks":[{"title":"T","instructions":"I"}]}`, http.StatusNotFound, false},
		{"append_chunks_conflict", false, false, &VanadisBuild{ID: "b-done", Status: "completed"}, nil, http.MethodPost, "/builds/b-done/chunks", `{"chunks":[{"title":"T","instructions":"I"}]}`, http.StatusConflict, false},
		{"append_chunks_ok_syncs_widget", false, false, &VanadisBuild{ID: "b1", Status: "running"}, nil, http.MethodPost, "/builds/b1/chunks", `{"chunks":[{"title":"T","instructions":"I"}]}`, http.StatusOK, true},

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

			srv := NewVanadisServer(Config{WebhooksRouterURL: tsWidget.URL}, store, nil, tsWidget.Client())
			mux := SetupMux(srv)

			var req *http.Request
			if tc.useErrBody {
				req = httptest.NewRequest(tc.method, tc.path, errorReader{})
			} else if tc.body != "" {
				req = httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
				req.Header.Set("Content-Type", "application/json")
			} else {
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

func TestPostgresVanadisStore_UpdateBuild(t *testing.T) {
	tests := []struct {
		name        string
		buildID     string
		req         UpdateBuildRequest
		beginErr    error
		scanErr     error
		buildStatus string
		execErr     error
		commitErr   error
		wantErrIs   error
		wantErr     bool
	}{
		{"empty_build_id", "", UpdateBuildRequest{Title: "T"}, nil, nil, "", nil, nil, ErrInvalidBuildInput, true},
		{"all_empty_fields", "b1", UpdateBuildRequest{Title: "   "}, nil, nil, "", nil, nil, ErrInvalidBuildInput, true},
		{"begin_error", "b1", UpdateBuildRequest{Title: "T"}, errors.New("begin err"), nil, "", nil, nil, nil, true},
		{"build_not_found", "b1", UpdateBuildRequest{Title: "T"}, nil, pgx.ErrNoRows, "", nil, nil, ErrBuildNotFound, true},
		{"build_query_error", "b1", UpdateBuildRequest{Title: "T"}, nil, errors.New("query err"), "", nil, nil, nil, true},
		{"build_conflict_status", "b1", UpdateBuildRequest{Title: "T"}, nil, nil, "completed", nil, nil, ErrBuildConflict, true},
		{"exec_error", "b1", UpdateBuildRequest{Title: "T"}, nil, nil, "running", errors.New("exec err"), nil, nil, true},
		{"commit_error", "b1", UpdateBuildRequest{Title: "T"}, nil, nil, "running", nil, errors.New("commit err"), nil, true},
		{"success", "b1", UpdateBuildRequest{Title: "New Title", Objective: "New Obj", SharedContext: "New Ctx"}, nil, nil, "running", nil, nil, nil, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tx := &mockTx{
				commitErr: tc.commitErr,
				queryRowFn: func(context.Context, string, ...any) pgx.Row {
					if tc.scanErr != nil {
						return &mockRow{err: tc.scanErr}
					}
					return &mockRow{b: &VanadisBuild{ID: tc.buildID, Status: tc.buildStatus}}
				},
				execFn: func(context.Context, string, ...any) (pgconn.CommandTag, error) {
					if tc.execErr != nil {
						return pgconn.CommandTag{}, tc.execErr
					}
					return pgconn.NewCommandTag("UPDATE 1"), nil
				},
			}
			db := mockDB{
				beginFn: func(context.Context) (pgx.Tx, error) {
					if tc.beginErr != nil {
						return nil, tc.beginErr
					}
					return tx, nil
				},
			}
			store := NewPostgresVanadisStore(&db)
			got, err := store.UpdateBuild(context.Background(), tc.buildID, tc.req)
			if tc.wantErrIs != nil {
				if !errors.Is(err, tc.wantErrIs) {
					t.Fatalf("expected error %v, got %v", tc.wantErrIs, err)
				}
				return
			}
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got == nil || got.Title != tc.req.Title {
				t.Fatalf("unexpected updated build: %+v", got)
			}
		})
	}
}

func TestPostgresVanadisStore_UpdateChunk(t *testing.T) {
	tests := []struct {
		name          string
		buildID       string
		chunkRef      string
		req           UpdateChunkRequest
		beginErr      error
		buildScanErr  error
		buildStatus   string
		chunksErr     error
		scanChunksErr error
		chunks        []VanadisChunk
		execErr       error
		commitErr     error
		wantErrIs     error
		wantErr       bool
	}{
		{"empty_build_id", "", "c1", UpdateChunkRequest{Title: "T"}, nil, nil, "", nil, nil, nil, nil, nil, ErrInvalidBuildInput, true},
		{"empty_chunk_ref", "b1", "", UpdateChunkRequest{Title: "T"}, nil, nil, "", nil, nil, nil, nil, nil, ErrInvalidBuildInput, true},
		{"no_fields", "b1", "c1", UpdateChunkRequest{}, nil, nil, "", nil, nil, nil, nil, nil, ErrInvalidBuildInput, true},
		{"invalid_effort", "b1", "c1", UpdateChunkRequest{Effort: "extreme"}, nil, nil, "", nil, nil, nil, nil, nil, ErrInvalidBuildInput, true},
		{"begin_error", "b1", "c1", UpdateChunkRequest{Title: "T"}, errors.New("begin err"), nil, "", nil, nil, nil, nil, nil, nil, true},
		{"build_not_found", "b1", "c1", UpdateChunkRequest{Title: "T"}, nil, pgx.ErrNoRows, "", nil, nil, nil, nil, nil, ErrBuildNotFound, true},
		{"build_query_error", "b1", "c1", UpdateChunkRequest{Title: "T"}, nil, errors.New("query err"), "", nil, nil, nil, nil, nil, nil, true},
		{"build_conflict_status", "b1", "c1", UpdateChunkRequest{Title: "T"}, nil, nil, "completed", nil, nil, nil, nil, nil, ErrBuildConflict, true},
		{"chunks_query_error", "b1", "c1", UpdateChunkRequest{Title: "T"}, nil, nil, "running", errors.New("chunks query err"), nil, nil, nil, nil, nil, true},
		{"chunks_scan_error", "b1", "c1", UpdateChunkRequest{Title: "T"}, nil, nil, "running", nil, errors.New("chunks scan err"), []VanadisChunk{{ID: "c1"}}, nil, nil, nil, true},
		{"chunk_not_found", "b1", "c999", UpdateChunkRequest{Title: "T"}, nil, nil, "running", nil, nil, []VanadisChunk{{ID: "c1", Status: "pending"}}, nil, nil, ErrChunkNotFound, true},
		{"chunk_conflict_status", "b1", "c1", UpdateChunkRequest{Title: "T"}, nil, nil, "running", nil, nil, []VanadisChunk{{ID: "c1", Status: "running"}}, nil, nil, ErrBuildConflict, true},
		{"exec_error", "b1", "c1", UpdateChunkRequest{Title: "T"}, nil, nil, "running", nil, nil, []VanadisChunk{{ID: "c1", Status: "pending"}}, errors.New("exec err"), nil, nil, true},
		{"commit_error", "b1", "c1", UpdateChunkRequest{Title: "T"}, nil, nil, "running", nil, nil, []VanadisChunk{{ID: "c1", Status: "pending"}}, nil, errors.New("commit err"), nil, true},
		{"success", "b1", "c1", UpdateChunkRequest{Repo: "mirrormere", Title: "New Title", Instructions: "New Instr", Effort: "low", TargetFiles: []string{"a.go"}, TargetSymbols: []string{"A"}}, nil, nil, "running", nil, nil, []VanadisChunk{{ID: "c1", Status: "pending"}}, nil, nil, nil, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tx := &mockTx{
				commitErr: tc.commitErr,
				queryRowFn: func(context.Context, string, ...any) pgx.Row {
					if tc.buildScanErr != nil {
						return &mockRow{err: tc.buildScanErr}
					}
					return &mockRow{b: &VanadisBuild{ID: tc.buildID, Status: tc.buildStatus}}
				},
				queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
					if tc.chunksErr != nil {
						return nil, tc.chunksErr
					}
					return &mockRows{items: tc.chunks, scanErr: tc.scanChunksErr}, nil
				},
				execFn: func(context.Context, string, ...any) (pgconn.CommandTag, error) {
					if tc.execErr != nil {
						return pgconn.CommandTag{}, tc.execErr
					}
					return pgconn.NewCommandTag("UPDATE 1"), nil
				},
			}
			db := mockDB{
				beginFn: func(context.Context) (pgx.Tx, error) {
					if tc.beginErr != nil {
						return nil, tc.beginErr
					}
					return tx, nil
				},
			}
			store := NewPostgresVanadisStore(&db)
			got, err := store.UpdateChunk(context.Background(), tc.buildID, tc.chunkRef, tc.req)
			if tc.wantErrIs != nil {
				if !errors.Is(err, tc.wantErrIs) {
					t.Fatalf("expected error %v, got %v", tc.wantErrIs, err)
				}
				return
			}
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got == nil || got.Title != tc.req.Title {
				t.Fatalf("unexpected updated chunk: %+v", got)
			}
		})
	}
}

func TestPostgresVanadisStore_AppendChunks(t *testing.T) {
	tests := []struct {
		name          string
		buildID       string
		inputs        []CreateChunkInput
		beginErr      error
		buildScanErr  error
		buildStatus   string
		chunksErr     error
		scanChunksErr error
		existing      []VanadisChunk
		execErr       error
		commitErr     error
		wantErrIs     error
		wantErr       bool
		wantCount     int
	}{
		{"empty_build_id", "", []CreateChunkInput{{Title: "T", Instructions: "I"}}, nil, nil, "", nil, nil, nil, nil, nil, ErrInvalidBuildInput, true, 0},
		{"empty_inputs", "b1", nil, nil, nil, "", nil, nil, nil, nil, nil, ErrInvalidBuildInput, true, 0},
		{"empty_title", "b1", []CreateChunkInput{{Title: "", Instructions: "I"}}, nil, nil, "", nil, nil, nil, nil, nil, ErrInvalidBuildInput, true, 0},
		{"empty_instructions", "b1", []CreateChunkInput{{Title: "T", Instructions: ""}}, nil, nil, "", nil, nil, nil, nil, nil, ErrInvalidBuildInput, true, 0},
		{"invalid_id", "b1", []CreateChunkInput{{ID: "bad:id", Title: "T", Instructions: "I"}}, nil, nil, "", nil, nil, nil, nil, nil, ErrInvalidBuildInput, true, 0},
		{"invalid_effort", "b1", []CreateChunkInput{{Title: "T", Instructions: "I", Effort: "super"}}, nil, nil, "", nil, nil, nil, nil, nil, ErrInvalidBuildInput, true, 0},
		{"begin_error", "b1", []CreateChunkInput{{Title: "T", Instructions: "I"}}, errors.New("begin err"), nil, "", nil, nil, nil, nil, nil, nil, true, 0},
		{"build_not_found", "b1", []CreateChunkInput{{Title: "T", Instructions: "I"}}, nil, pgx.ErrNoRows, "", nil, nil, nil, nil, nil, ErrBuildNotFound, true, 0},
		{"build_query_error", "b1", []CreateChunkInput{{Title: "T", Instructions: "I"}}, nil, errors.New("query err"), "", nil, nil, nil, nil, nil, nil, true, 0},
		{"build_conflict_status", "b1", []CreateChunkInput{{Title: "T", Instructions: "I"}}, nil, nil, "completed", nil, nil, nil, nil, nil, ErrBuildConflict, true, 0},
		{"chunks_query_error", "b1", []CreateChunkInput{{Title: "T", Instructions: "I"}}, nil, nil, "running", errors.New("chunks err"), nil, nil, nil, nil, nil, true, 0},
		{"chunks_scan_error", "b1", []CreateChunkInput{{Title: "T", Instructions: "I"}}, nil, nil, "running", nil, errors.New("scan err"), []VanadisChunk{{ID: "c1"}}, nil, nil, nil, true, 0},
		{"duplicate_id_conflict", "b1", []CreateChunkInput{{ID: "c1", Title: "T", Instructions: "I"}}, nil, nil, "running", nil, nil, []VanadisChunk{{ID: "c1"}}, nil, nil, ErrBuildConflict, true, 0},
		{"exec_insert_error", "b1", []CreateChunkInput{{Title: "T", Instructions: "I"}}, nil, nil, "running", nil, nil, nil, errors.New("insert err"), nil, nil, true, 0},
		{"commit_error", "b1", []CreateChunkInput{{Title: "T", Instructions: "I"}}, nil, nil, "running", nil, nil, nil, nil, errors.New("commit err"), nil, true, 0},
		{"success_with_id_collision_resolution", "b1", []CreateChunkInput{{Title: "T", Instructions: "I"}}, nil, nil, "running", nil, nil, []VanadisChunk{{ID: "b1-chunk-1", ChunkIndex: 0, Repo: "azylman/mirrormere"}}, nil, nil, nil, false, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tx := &mockTx{
				commitErr: tc.commitErr,
				queryRowFn: func(context.Context, string, ...any) pgx.Row {
					if tc.buildScanErr != nil {
						return &mockRow{err: tc.buildScanErr}
					}
					return &mockRow{b: &VanadisBuild{ID: tc.buildID, Status: tc.buildStatus}}
				},
				queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
					if tc.chunksErr != nil {
						return nil, tc.chunksErr
					}
					return &mockRows{items: tc.existing, scanErr: tc.scanChunksErr}, nil
				},
				execFn: func(context.Context, string, ...any) (pgconn.CommandTag, error) {
					if tc.execErr != nil {
						return pgconn.CommandTag{}, tc.execErr
					}
					return pgconn.NewCommandTag("INSERT 0 1"), nil
				},
			}
			db := mockDB{
				beginFn: func(context.Context) (pgx.Tx, error) {
					if tc.beginErr != nil {
						return nil, tc.beginErr
					}
					return tx, nil
				},
			}
			store := NewPostgresVanadisStore(&db)
			got, err := store.AppendChunks(context.Background(), tc.buildID, tc.inputs)
			if tc.wantErrIs != nil {
				if !errors.Is(err, tc.wantErrIs) {
					t.Fatalf("expected error %v, got %v", tc.wantErrIs, err)
				}
				return
			}
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(got) != tc.wantCount {
				t.Fatalf("got %d chunks, want %d", len(got), tc.wantCount)
			}
		})
	}
}

func TestPostgresVanadisStore_RemoveChunk(t *testing.T) {
	tests := []struct {
		name           string
		buildID        string
		chunkRef       string
		beginErr       error
		buildScanErr   error
		buildStatus    string
		chunksErr      error
		scanChunksErr  error
		existing       []VanadisChunk
		delExecErr     error
		compactExecErr error
		commitErr      error
		wantErrIs      error
		wantErr        bool
		wantRemainLen  int
	}{
		{"empty_build_id", "", "c1", nil, nil, "", nil, nil, nil, nil, nil, nil, ErrInvalidBuildInput, true, 0},
		{"empty_chunk_ref", "b1", "", nil, nil, "", nil, nil, nil, nil, nil, nil, ErrInvalidBuildInput, true, 0},
		{"begin_error", "b1", "c1", errors.New("begin err"), nil, "", nil, nil, nil, nil, nil, nil, nil, true, 0},
		{"build_not_found", "b1", "c1", nil, pgx.ErrNoRows, "", nil, nil, nil, nil, nil, nil, ErrBuildNotFound, true, 0},
		{"build_query_error", "b1", "c1", nil, errors.New("query err"), "", nil, nil, nil, nil, nil, nil, nil, true, 0},
		{"build_conflict_status", "b1", "c1", nil, nil, "completed", nil, nil, nil, nil, nil, nil, ErrBuildConflict, true, 0},
		{"chunks_query_error", "b1", "c1", nil, nil, "running", errors.New("chunks err"), nil, nil, nil, nil, nil, nil, true, 0},
		{"chunks_scan_error", "b1", "c1", nil, nil, "running", nil, errors.New("scan err"), []VanadisChunk{{ID: "c1"}}, nil, nil, nil, nil, true, 0},
		{"chunk_not_found", "b1", "c999", nil, nil, "running", nil, nil, []VanadisChunk{{ID: "c1", Status: "pending"}}, nil, nil, nil, ErrChunkNotFound, true, 0},
		{"chunk_conflict_status", "b1", "c1", nil, nil, "running", nil, nil, []VanadisChunk{{ID: "c1", Status: "running"}, {ID: "c2", Status: "pending"}}, nil, nil, nil, ErrBuildConflict, true, 0},
		{"cannot_remove_only_chunk", "b1", "c1", nil, nil, "running", nil, nil, []VanadisChunk{{ID: "c1", Status: "pending"}}, nil, nil, nil, ErrBuildConflict, true, 0},
		{"delete_exec_error", "b1", "c2", nil, nil, "running", nil, nil, []VanadisChunk{{ID: "c1", ChunkIndex: 0, Status: "running"}, {ID: "c2", ChunkIndex: 1, Status: "pending"}}, errors.New("del err"), nil, nil, nil, true, 0},
		{"compact_exec_error", "b1", "c1", nil, nil, "running", nil, nil, []VanadisChunk{{ID: "c1", ChunkIndex: 0, Status: "pending"}, {ID: "c2", ChunkIndex: 1, Status: "pending"}}, nil, errors.New("compact err"), nil, nil, true, 0},
		{"commit_error", "b1", "c2", nil, nil, "running", nil, nil, []VanadisChunk{{ID: "c1", ChunkIndex: 0, Status: "running"}, {ID: "c2", ChunkIndex: 1, Status: "pending"}}, nil, nil, errors.New("commit err"), nil, true, 0},
		{"success", "b1", "1", nil, nil, "running", nil, nil, []VanadisChunk{{ID: "c1", ChunkIndex: 0, Status: "running"}, {ID: "c2", ChunkIndex: 1, Status: "pending"}, {ID: "c3", ChunkIndex: 2, Status: "pending"}}, nil, nil, nil, nil, false, 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tx := &mockTx{
				commitErr: tc.commitErr,
				queryRowFn: func(context.Context, string, ...any) pgx.Row {
					if tc.buildScanErr != nil {
						return &mockRow{err: tc.buildScanErr}
					}
					return &mockRow{b: &VanadisBuild{ID: tc.buildID, Status: tc.buildStatus}}
				},
				queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
					if tc.chunksErr != nil {
						return nil, tc.chunksErr
					}
					return &mockRows{items: tc.existing, scanErr: tc.scanChunksErr}, nil
				},
				execFn: func(context.Context, string, ...any) (pgconn.CommandTag, error) {
					if tc.delExecErr != nil {
						return pgconn.CommandTag{}, tc.delExecErr
					}
					if tc.compactExecErr != nil {
						return pgconn.CommandTag{}, tc.compactExecErr
					}
					return pgconn.NewCommandTag("DELETE 1"), nil
				},
			}
			db := mockDB{
				beginFn: func(context.Context) (pgx.Tx, error) {
					if tc.beginErr != nil {
						return nil, tc.beginErr
					}
					return tx, nil
				},
			}
			store := NewPostgresVanadisStore(&db)
			removed, remaining, err := store.RemoveChunk(context.Background(), tc.buildID, tc.chunkRef)
			if tc.wantErrIs != nil {
				if !errors.Is(err, tc.wantErrIs) {
					t.Fatalf("expected error %v, got %v", tc.wantErrIs, err)
				}
				return
			}
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if removed == nil {
				t.Fatal("expected removed chunk, got nil")
			}
			if len(remaining) != tc.wantRemainLen {
				t.Fatalf("got %d remaining chunks, want %d", len(remaining), tc.wantRemainLen)
			}
		})
	}
}
