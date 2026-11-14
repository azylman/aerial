package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/db/dbtest"
	"github.com/azylman/aerial/services/eri/pkg/delivery"
	"github.com/azylman/aerial/services/eri/pkg/discord"
	"github.com/azylman/aerial/services/eri/pkg/metrics"
	"github.com/azylman/aerial/services/eri/pkg/queue"
	"github.com/azylman/aerial/services/eri/pkg/runner"
	"github.com/azylman/aerial/services/eri/pkg/transcript"
	"github.com/bwmarrin/discordgo"
)

func runHTTP(h http.HandlerFunc, method, target string, body io.Reader, headers ...string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, body)
	for i := 0; i+1 < len(headers); i += 2 {
		if headers[i] != "" && headers[i+1] != "" {
			req.Header.Set(headers[i], headers[i+1])
		}
	}
	w := httptest.NewRecorder()
	h(w, req)
	return w
}

type promptPayloadCase struct {
	name          string
	pool          *queue.WorkerPool
	payload       string
	msgID         string
	wantThreadID  string
	wantChannelID string
	wantEffort    string
	wantModel     string
	wantContent   string
}

func assertPromptPayloadCase(t *testing.T, store db.Store, tc promptPayloadCase) {
	t.Helper()
	w := runHTTP(handlePrompt(store, tc.pool), http.MethodPost, "/prompt", strings.NewReader(tc.payload))
	if w.Code != http.StatusAccepted {
		t.Fatalf("got status=%d body=%q, want 202", w.Code, w.Body.String())
	}
	if tc.wantThreadID != "" && !strings.Contains(w.Body.String(), fmt.Sprintf(`"session_id":"%s"`, tc.wantThreadID)) {
		t.Errorf("response body missing session_id %q: %s", tc.wantThreadID, w.Body.String())
	}
	if tc.wantChannelID != "" && !strings.Contains(w.Body.String(), fmt.Sprintf(`"channel_id":"%s"`, tc.wantChannelID)) {
		t.Errorf("response body missing channel_id %q: %s", tc.wantChannelID, w.Body.String())
	}
	if tc.msgID == "" {
		return
	}
	msg, err := store.GetMessage(context.Background(), tc.msgID)
	if err != nil || msg == nil {
		t.Fatalf("failed to retrieve persisted message %s: %v", tc.msgID, err)
	}
	if tc.wantThreadID != "" && msg.ThreadID != tc.wantThreadID {
		t.Errorf("thread_id=%q, want %q", msg.ThreadID, tc.wantThreadID)
	}
	if tc.wantChannelID != "" && msg.ChannelID != tc.wantChannelID {
		t.Errorf("channel_id=%q, want %q", msg.ChannelID, tc.wantChannelID)
	}
	if tc.wantContent != "" && msg.Content != tc.wantContent {
		t.Errorf("content=%q, want %q", msg.Content, tc.wantContent)
	}
	if tc.wantEffort != "" && msg.Effort != tc.wantEffort {
		t.Errorf("effort=%q, want %q", msg.Effort, tc.wantEffort)
	}
	if tc.wantModel != "" && msg.Model != tc.wantModel {
		t.Errorf("model=%q, want %q", msg.Model, tc.wantModel)
	}
	if !msg.IsAdmin {
		t.Errorf("expected is_admin=true for /prompt message, got false")
	}
}

func TestHandlePromptValidation(t *testing.T) {
	store := dbtest.NewFakeStore()
	pool := newTestWorkerPool(store)
	pool.Start()
	defer pool.Stop()

	closedStore := dbtest.NewFakeStore()
	_ = closedStore.Close()

	mockSess, _ := discordgo.New("Bot fake-token")
	mockSess.State = discordgo.NewState()
	mockSess.State.GuildAdd(&discordgo.Guild{
		ID: "guild-1",
		Channels: []*discordgo.Channel{
			{ID: "1564150557705248879", Name: "prospera"},
		},
	})
	prosperaPromptPool := queue.New(nil, queue.WorkerPoolConfig{
		Store:          store,
		DiscordSession: mockSess,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			return `{"event":"result","result":{"status":"SUCCESS","response":"mock"}}`, "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error { return nil },
	})
	defer prosperaPromptPool.Stop()

	for _, tc := range []struct {
		name       string
		method     string
		store      db.Store
		pool       *queue.WorkerPool
		body       io.Reader
		wantStatus int
		wantSubstr string
	}{
		{"method not allowed", http.MethodGet, store, pool, nil, http.StatusMethodNotAllowed, "Method not allowed"},
		{"read error", http.MethodPost, store, pool, &errReadCloser{readErr: errors.New("simulated network read error")}, http.StatusBadRequest, "Failed to read request body"},
		{"invalid JSON", http.MethodPost, store, pool, strings.NewReader("{invalid-json"), http.StatusBadRequest, "Invalid JSON payload"},
		{"empty prompt", http.MethodPost, store, pool, strings.NewReader(`{"prompt":""}`), http.StatusBadRequest, "Invalid payload: 'prompt' field is required and cannot be empty"},
		{"whitespace prompt", http.MethodPost, store, pool, strings.NewReader(`{"prompt":"   "}`), http.StatusBadRequest, "Invalid payload: 'prompt' field is required and cannot be empty"},
		{"missing channel_id", http.MethodPost, store, pool, strings.NewReader(`{"prompt":"hello"}`), http.StatusBadRequest, "Invalid payload: 'channel_id' field is required and cannot be empty"},
		{"non-snowflake channel_id", http.MethodPost, store, pool, strings.NewReader(`{"prompt":"hello","channel_id":"not-a-snowflake"}`), http.StatusBadRequest, "Invalid payload: 'channel_id' must be a valid numeric Discord snowflake"},
		{"channel name resolution prospera", http.MethodPost, store, prosperaPromptPool, strings.NewReader(`{"prompt":"hello","channel_id":"prospera"}`), http.StatusAccepted, "1564150557705248879"},
		{"channel name resolution hash prospera", http.MethodPost, store, prosperaPromptPool, strings.NewReader(`{"prompt":"hello","channel_id":"#prospera"}`), http.StatusAccepted, "1564150557705248879"},
		{"channel name unresolvable falls back to snowflake validation", http.MethodPost, store, prosperaPromptPool, strings.NewReader(`{"prompt":"hello","channel_id":"unknown-channel"}`), http.StatusBadRequest, "must be a valid numeric Discord snowflake"},
		{"nil store", http.MethodPost, nil, pool, strings.NewReader(`{"prompt":"hello","channel_id":"1542423172400291873"}`), http.StatusInternalServerError, "Database store not configured"},
		{"closed store", http.MethodPost, closedStore, pool, strings.NewReader(`{"prompt":"hello","channel_id":"1542423172400291873"}`), http.StatusInternalServerError, "Failed to persist prompt message"},
		{"body close error", http.MethodPost, store, pool, &errReadCloser{reader: strings.NewReader(`{"prompt":"hello","channel_id":"1542423172400291873"}`), closeErr: errors.New("close error")}, http.StatusAccepted, "channel_id"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := runHTTP(handlePrompt(tc.store, tc.pool), tc.method, "/prompt", tc.body)
			if w.Code != tc.wantStatus || !strings.Contains(w.Body.String(), tc.wantSubstr) {
				t.Fatalf("got status=%d body=%q, want status=%d substr=%q", w.Code, w.Body.String(), tc.wantStatus, tc.wantSubstr)
			}
		})
	}

	threadPool := queue.New(nil, queue.WorkerPoolConfig{
		Store: store,
		CreateThreadFunc: func(channelID, title string) (string, error) {
			return "1542999999999999999", nil
		},
	})
	defer threadPool.Stop()

	redirectCfg := config.NewFromData(&config.ConfigData{
		SystemChannel: "1564150557705248879",
		Channels: map[string]config.ChannelPolicy{
			"1542423172400291873": {
				Mode:           "channel",
				NoBuildUpdates: true,
			},
		},
	})
	redirectPool := queue.New(redirectCfg, queue.WorkerPoolConfig{
		Store: store,
	})
	defer redirectPool.Stop()

	for _, tc := range []promptPayloadCase{
		{"default effort", pool, `{"prompt":"Test valid prompt","channel_id":"1542423172400291873","message_id":"msg-123"}`, "msg-123", "1542423172400291873", "1542423172400291873", "", "", "Test valid prompt"},
		{"explicit effort", pool, `{"prompt":"Test explicit effort","channel_id":"1542423172400291873","effort":"high","message_id":"msg-effort-high"}`, "msg-effort-high", "1542423172400291873", "1542423172400291873", "high", "", "Test explicit effort"},
		{"inferred medium effort", pool, `{"prompt":"Test medium effort","channel_id":"1542423172400291873","model":"model-medium","message_id":"msg-effort-med"}`, "msg-effort-med", "1542423172400291873", "1542423172400291873", "medium", "model-medium", "Test medium effort"},
		{"inferred low effort", pool, `{"prompt":"Test low effort","channel_id":"1542423172400291873","model":"model-flash","message_id":"msg-effort-flash"}`, "msg-effort-flash", "1542423172400291873", "1542423172400291873", "low", "model-flash", "Test low effort"},
		{"nil pool", nil, `{"prompt":"hello nil pool","channel_id":"1542423172400291873","message_id":"msg-nil-pool"}`, "msg-nil-pool", "1542423172400291873", "1542423172400291873", "", "", "hello nil pool"},
		{"auto-generated uuid when message_id omitted", pool, `{"prompt":"auto uuid prompt","channel_id":"1542423172400291873"}`, "", "", "", "", "", "auto uuid prompt"},
		{"resolve target thread", threadPool, `{"prompt":"create thread prompt","channel_id":"1542423172400291873","title":"Heartbeat Alert","message_id":"msg-thread-create"}`, "msg-thread-create", "1542999999999999999", "1542999999999999999", "", "", "create thread prompt"},
		{
			"cd build completion redirects to system channel when no_build_updates",
			redirectPool,
			`{"prompt":"Continuous Delivery deployment completed for job aerial-eri","channel_id":"1542423172400291873","message_id":"msg-cd-redirect"}`,
			"msg-cd-redirect",
			"1564150557705248879",
			"1564150557705248879",
			"",
			"",
			"Continuous Delivery deployment completed for job aerial-eri",
		},
		{
			"vanadis build completion redirects to system channel when no_build_updates",
			redirectPool,
			`{"prompt":"Vanadis build build-123 completed successfully","channel_id":"1542423172400291873","message_id":"msg-vanadis-redirect"}`,
			"msg-vanadis-redirect",
			"1564150557705248879",
			"1564150557705248879",
			"",
			"",
			"Vanadis build build-123 completed successfully",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertPromptPayloadCase(t, store, tc)
		})
	}
}

func TestHandlePrompt_SkipIfActive(t *testing.T) {
	activeStore := dbtest.NewFakeStore()
	_ = activeStore.InsertMessage(context.Background(), db.Message{
		ID:       "msg-active",
		ThreadID: "1542423172400291873",
		Status:   db.StatusProcessing,
	})

	inactiveStore := dbtest.NewFakeStore()

	errStore := dbtest.NewFakeStore()
	errStore.FailNext("HasActiveThreadTurn", errors.New("simulated db failure"))

	redirectCfg := config.NewFromData(&config.ConfigData{
		SystemChannel: "1564150557705248879",
		Channels: map[string]config.ChannelPolicy{
			"1542423172400291873": {
				Mode:           "channel",
				NoBuildUpdates: true,
			},
		},
	})
	redirectPool := queue.New(redirectCfg, queue.WorkerPoolConfig{
		Store: activeStore,
	})
	defer redirectPool.Stop()

	tests := []struct {
		name       string
		store      db.Store
		pool       *queue.WorkerPool
		body       string
		wantStatus int
		wantSubstr string
	}{
		{
			name:       "active turn exists returns 200 skipped_active",
			store:      activeStore,
			body:       `{"prompt":"check","channel_id":"1542423172400291873","skip_if_active":true}`,
			wantStatus: http.StatusOK,
			wantSubstr: `"status":"skipped_active"`,
		},
		{
			name:       "no active turn exists returns 202 accepted",
			store:      inactiveStore,
			body:       `{"prompt":"check","channel_id":"1542423172400291873","skip_if_active":true}`,
			wantStatus: http.StatusAccepted,
			wantSubstr: `"status":"accepted"`,
		},
		{
			name:       "redirected build completion prompt on active channel succeeds if system channel idle",
			store:      activeStore,
			pool:       redirectPool,
			body:       `{"prompt":"Vanadis build build-1 completed","channel_id":"1542423172400291873","skip_if_active":true}`,
			wantStatus: http.StatusAccepted,
			wantSubstr: `"status":"accepted"`,
		},
		{
			name:       "store error returns 500",
			store:      errStore,
			body:       `{"prompt":"check","channel_id":"1542423172400291873","skip_if_active":true}`,
			wantStatus: http.StatusInternalServerError,
			wantSubstr: "Failed to check active turn",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := runHTTP(handlePrompt(tc.store, tc.pool), http.MethodPost, "/prompt", strings.NewReader(tc.body))
			if w.Code != tc.wantStatus || !strings.Contains(w.Body.String(), tc.wantSubstr) {
				t.Fatalf("got status=%d body=%q, want status=%d substr=%q", w.Code, w.Body.String(), tc.wantStatus, tc.wantSubstr)
			}
		})
	}
}

func TestHandleDispatch(t *testing.T) {
	for _, tc := range []struct {
		name               string
		method             string
		store              db.Store
		body               string
		wantStatus         int
		wantSubstr         string
		checkID            string
		wantTargetID       string
		wantVanadisChunkID string
	}{
		{"method not allowed", http.MethodGet, dbtest.NewFakeStore(), "", http.StatusMethodNotAllowed, "Method not allowed", "", "", ""},
		{"nil store", http.MethodPost, nil, `{"source":"heartbeat","prompt":"test"}`, http.StatusInternalServerError, "Database store not configured", "", "", ""},
		{"invalid json", http.MethodPost, dbtest.NewFakeStore(), `{bad`, http.StatusBadRequest, "Invalid JSON payload", "", "", ""},
		{"missing source", http.MethodPost, dbtest.NewFakeStore(), `{"prompt":"test"}`, http.StatusBadRequest, "'source' must match", "", "", ""},
		{"invalid source", http.MethodPost, dbtest.NewFakeStore(), `{"source":"unknown!bad","prompt":"test"}`, http.StatusBadRequest, "'source' must match", "", "", ""},
		{"empty prompt", http.MethodPost, dbtest.NewFakeStore(), `{"source":"heartbeat","prompt":" "}`, http.StatusBadRequest, "'prompt' field is required", "", "", ""},
		{"numeric session_id rejected", http.MethodPost, dbtest.NewFakeStore(), `{"source":"heartbeat","prompt":"test","session_id":"1234567890"}`, http.StatusBadRequest, "cannot be a numeric snowflake", "", "", ""},
		{"discord prefix session_id rejected", http.MethodPost, dbtest.NewFakeStore(), `{"source":"heartbeat","prompt":"test","session_id":"discord-123"}`, http.StatusBadRequest, "cannot be a numeric snowflake", "", "", ""},
		{"invalid chars session_id rejected", http.MethodPost, dbtest.NewFakeStore(), `{"source":"heartbeat","prompt":"test","session_id":"bad:session"}`, http.StatusBadRequest, "cannot be a numeric snowflake", "", "", ""},
		{"invalid effort rejected", http.MethodPost, dbtest.NewFakeStore(), `{"source":"heartbeat","prompt":"test","effort":"ultra"}`, http.StatusBadRequest, "'effort' must be 'low', 'medium', or 'high'", "", "", ""},
		{"valid stateful accepted", http.MethodPost, dbtest.NewFakeStore(), `{"id":"disp-1","source":"heartbeat","session_id":"heartbeat-1","prompt":"run check","effort":"low"}`, http.StatusAccepted, `"status":"accepted"`, "disp-1", "", ""},
		{"valid ephemeral accepted", http.MethodPost, dbtest.NewFakeStore(), `{"id":"disp-2","source":"vanadis","prompt":"execute build"}`, http.StatusAccepted, `"status":"accepted"`, "disp-2", "", ""},
		{"valid with target_id normalized and chunk_id propagated", http.MethodPost, dbtest.NewFakeStore(), `{"id":"disp-norm-1","source":"vanadis","prompt":"execute chunk","target_id":"discord-987654321012345678","vanadis_chunk_id":"  build-42-chunk-1  "}`, http.StatusAccepted, `"status":"accepted"`, "disp-norm-1", "987654321012345678", "build-42-chunk-1"},
		{"valid with pooled target_id normalized", http.MethodPost, dbtest.NewFakeStore(), `{"id":"disp-norm-2","source":"vanadis","prompt":"execute chunk","target_id":"stateful_medium:discord-11223344","vanadis_chunk_id":"build-42-chunk-2"}`, http.StatusAccepted, `"status":"accepted"`, "disp-norm-2", "11223344", "build-42-chunk-2"},
		{"headless target_id blanked", http.MethodPost, dbtest.NewFakeStore(), `{"id":"disp-norm-3","source":"heartbeat","prompt":"pulse","target_id":"heartbeat-hb-1","vanadis_chunk_id":""}`, http.StatusAccepted, `"status":"accepted"`, "disp-norm-3", "", ""},
		{"has active dispatch turn error", http.MethodPost, func() db.Store {
			s := dbtest.NewFakeStore()
			s.FailNext("HasActiveDispatchTurn", errors.New("simulated error"))
			return s
		}(), `{"source":"heartbeat","prompt":"test","skip_if_active":true}`, http.StatusInternalServerError, "Failed to check active turn", "", "", ""},
		{"insert dispatch error", http.MethodPost, func() db.Store {
			s := dbtest.NewFakeStore()
			s.FailNext("InsertDispatch", errors.New("simulated error"))
			return s
		}(), `{"source":"heartbeat","prompt":"test"}`, http.StatusInternalServerError, "Failed to persist dispatch", "", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := runHTTP(handleDispatch(tc.store, nil), tc.method, "/dispatch", strings.NewReader(tc.body))
			if w.Code != tc.wantStatus || !strings.Contains(w.Body.String(), tc.wantSubstr) {
				t.Fatalf("got status=%d body=%q, want status=%d substr=%q", w.Code, w.Body.String(), tc.wantStatus, tc.wantSubstr)
			}
			if tc.checkID != "" && tc.store != nil {
				d, err := tc.store.GetDispatch(context.Background(), tc.checkID)
				if err != nil || d == nil {
					t.Fatalf("failed to retrieve dispatch %s: %v", tc.checkID, err)
				}
				if d.TargetID != tc.wantTargetID {
					t.Errorf("got target_id %q, want %q", d.TargetID, tc.wantTargetID)
				}
				if d.VanadisChunkID != tc.wantVanadisChunkID {
					t.Errorf("got vanadis_chunk_id %q, want %q", d.VanadisChunkID, tc.wantVanadisChunkID)
				}
			}
		})
	}
}

func TestHandleDispatch_SkipIfActive(t *testing.T) {
	s := dbtest.NewFakeStore()
	_, _ = s.InsertDispatch(context.Background(), db.Dispatch{
		ID:        "active-disp-1",
		Source:    "heartbeat",
		SessionID: "heartbeat-test",
		Prompt:    "ongoing run",
		Status:    db.StatusProcessing,
	})

	for _, tc := range []struct {
		name       string
		body       string
		wantStatus int
		wantSubstr string
	}{
		{"skip active session", `{"source":"heartbeat","session_id":"heartbeat-test","prompt":"test","skip_if_active":true}`, http.StatusOK, `"status":"skipped_active"`},
		{"do not skip when skip_if_active false", `{"source":"heartbeat","session_id":"heartbeat-test","prompt":"test","skip_if_active":false}`, http.StatusAccepted, `"status":"accepted"`},
		{"do not skip different session", `{"source":"heartbeat","session_id":"heartbeat-other","prompt":"test","skip_if_active":true}`, http.StatusAccepted, `"status":"accepted"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := runHTTP(handleDispatch(s, nil), http.MethodPost, "/dispatch", strings.NewReader(tc.body))
			if w.Code != tc.wantStatus || !strings.Contains(w.Body.String(), tc.wantSubstr) {
				t.Fatalf("got status=%d body=%q, want status=%d substr=%q", w.Code, w.Body.String(), tc.wantStatus, tc.wantSubstr)
			}
		})
	}
}

func TestHandleCancelDispatch(t *testing.T) {
	seedStore := func(failOp string) *dbtest.FakeStore {
		s := dbtest.NewFakeStore()
		ctx := context.Background()
		for _, d := range []db.Dispatch{
			{ID: "disp-b1-c1", Source: "vanadis", Prompt: "[VANADIS BUILD CHUNK EXECUTION]\nBuild ID: build-99\n", Status: db.StatusPending},
			{ID: "disp-b1-c2", Source: "vanadis", Prompt: "[VANADIS BUILD CHUNK EXECUTION]\nBuild ID: build-99\n", Status: db.StatusPending},
			{ID: "disp-active", Source: "vanadis", SessionID: "vanadis-build-99-chunk-1", Prompt: "[VANADIS BUILD CHUNK EXECUTION]\nBuild ID: build-99\n", Status: db.StatusProcessing},
			{ID: "disp-active-sess", Source: "vanadis", SessionID: "vanadis-build-99-chunk-2", Prompt: "Action chunk without explicit prompt marker", Status: db.StatusProcessing},
			{ID: "disp-single", Source: "heartbeat", Prompt: "hourly pulse", Status: db.StatusPending},
		} {
			_, _ = s.InsertDispatch(ctx, d)
		}
		if failOp != "" {
			s.FailNext(failOp, errors.New("simulated "+failOp+" error"))
		}
		return s
	}

	for _, tc := range []struct {
		name       string
		method     string
		store      db.Store
		pool       *queue.WorkerPool
		body       string
		wantStatus int
		wantSubstr string
	}{
		{"method not allowed", http.MethodGet, seedStore(""), nil, "", http.StatusMethodNotAllowed, "Method not allowed"},
		{"nil store", http.MethodPost, nil, nil, `{"build_id":"build-99"}`, http.StatusInternalServerError, "Database store not configured"},
		{"invalid json", http.MethodPost, seedStore(""), nil, `{invalid`, http.StatusBadRequest, "Invalid JSON payload"},
		{"missing ids", http.MethodPost, seedStore(""), nil, `{"reason":"no ids"}`, http.StatusBadRequest, "'dispatch_id' (or 'message_id') or 'build_id' is required"},
		{"store error", http.MethodPost, seedStore("CancelPendingDispatches"), nil, `{"build_id":"build-99"}`, http.StatusInternalServerError, "Failed to fetch cancelling dispatches"},
		{"cancel by build_id", http.MethodPost, seedStore(""), nil, `{"build_id":"build-99"}`, http.StatusOK, `"cancelled_count":4`},
		{"cancel by dispatch_id", http.MethodPost, seedStore(""), nil, `{"dispatch_id":"disp-single"}`, http.StatusOK, `"cancelled_ids":["disp-single"]`},
		{"cancel by dispatch_id custom reason", http.MethodPost, seedStore(""), nil, `{"dispatch_id":"disp-single","reason":"superseded"}`, http.StatusOK, `"cancelled_ids":["disp-single"]`},
		{"cancel by message_id alias", http.MethodPost, seedStore(""), nil, `{"message_id":"disp-single"}`, http.StatusOK, `"cancelled_ids":["disp-single"]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var pools []*queue.WorkerPool
			if tc.pool != nil {
				pools = append(pools, tc.pool)
			}
			w := runHTTP(handleCancelDispatch(tc.store, pools...), tc.method, "/dispatch/cancel", strings.NewReader(tc.body))
			if w.Code != tc.wantStatus || !strings.Contains(w.Body.String(), tc.wantSubstr) {
				t.Fatalf("got status=%d body=%q, want status=%d substr=%q", w.Code, w.Body.String(), tc.wantStatus, tc.wantSubstr)
			}
		})
	}
}

type directMessageCase struct {
	name       string
	method     string
	pool       *queue.WorkerPool
	body       io.Reader
	wantStatus int
	wantSubstr string
}

func assertDirectMessageCase(t *testing.T, tc directMessageCase) {
	t.Helper()
	w := runHTTP(handleDirectMessage(tc.pool), tc.method, "/discord/message", tc.body)
	if w.Code != tc.wantStatus || !strings.Contains(w.Body.String(), tc.wantSubstr) {
		t.Fatalf("got status=%d body=%q, want status=%d substr=%q", w.Code, w.Body.String(), tc.wantStatus, tc.wantSubstr)
	}
}

func runDirectMessageDeliveryCases(t *testing.T) {
	t.Helper()
	var capturedChannelID, capturedText string
	handler := handleDirectMessage(queue.New(nil, queue.WorkerPoolConfig{
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			capturedChannelID, capturedText = channelID, text
			return nil
		},
	}))

	w := runHTTP(handler, http.MethodPost, "/discord/message", strings.NewReader(`{"channel_id":"1542423172400291873","content":"Deploy succeeded!"}`))
	if w.Code != http.StatusOK || capturedChannelID != "1542423172400291873" || capturedText != "Deploy succeeded!" {
		t.Fatalf("unexpected delivery: code=%d, ch=%q, txt=%q", w.Code, capturedChannelID, capturedText)
	}
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["status"] != "sent" || resp["channel_id"] != "1542423172400291873" {
		t.Fatalf("unexpected response body: %+v", resp)
	}

	w2 := runHTTP(handler, http.MethodPost, "/discord/message", strings.NewReader(`{"thread_id":"1542423172400291874","text":"Deploy failed!"}`))
	if w2.Code != http.StatusOK || capturedChannelID != "1542423172400291874" || capturedText != "Deploy failed!" {
		t.Fatalf("unexpected fallback: code=%d, ch=%q, txt=%q", w2.Code, capturedChannelID, capturedText)
	}

	w3 := runHTTP(handler, http.MethodPost, "/discord/message", strings.NewReader(`{"channel_id":"1542423172400291873","thread_id":"1542423172400291875","content":"Priority check"}`))
	if w3.Code != http.StatusOK || capturedChannelID != "1542423172400291875" || capturedText != "Priority check" {
		t.Fatalf("unexpected thread priority: code=%d, ch=%q, txt=%q", w3.Code, capturedChannelID, capturedText)
	}

	var capturedAtts []*delivery.Attachment
	attHandler := handleDirectMessage(queue.New(nil, queue.WorkerPoolConfig{
		DeliveryWithAttachmentsFunc: func(s *discordgo.Session, channelID, text string, atts []*delivery.Attachment) error {
			capturedChannelID, capturedText, capturedAtts = channelID, text, atts
			return nil
		},
	}))
	wAtt := runHTTP(attHandler, http.MethodPost, "/discord/message", strings.NewReader(`{"channel_id":"1542423172400291873","content":"Voicemail received","attachments":[{"filename":"test.wav","content_type":"audio/wav","content_base64":"UklGRg=="}]}`))
	if wAtt.Code != http.StatusOK || len(capturedAtts) != 1 || capturedAtts[0].Filename != "test.wav" || string(capturedAtts[0].Data) != "RIFF" {
		t.Fatalf("unexpected attachment delivery: code=%d, atts=%+v", wAtt.Code, capturedAtts)
	}
}

func runDirectMessageMuxCases(t *testing.T) {
	t.Helper()
	pool := queue.New(nil, queue.WorkerPoolConfig{
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error { return nil },
	})
	mux := SetupBrainMuxWithEmbedder(dbtest.NewFakeStore(), dbtest.NewFakeStore(), pool, nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/discord/message", strings.NewReader(`{"channel_id":"1542423172400291873","content":"test"}`)))
	if w.Code != http.StatusOK {
		t.Errorf("expected 200 for /discord/message, got %d", w.Code)
	}
	wInternal := httptest.NewRecorder()
	mux.ServeHTTP(wInternal, httptest.NewRequest(http.MethodPost, "/internal/message", strings.NewReader(`{"channel_id":"1542423172400291873","content":"test"}`)))
	if wInternal.Code != http.StatusNotFound {
		t.Errorf("expected 404 for dropped /internal/message, got %d", wInternal.Code)
	}
}

func TestHandleDirectMessage(t *testing.T) {
	mockSess, _ := discordgo.New("Bot fake-token")
	mockSess.State = discordgo.NewState()
	mockSess.State.GuildAdd(&discordgo.Guild{
		ID: "guild-1",
		Channels: []*discordgo.Channel{
			{ID: "1564150557705248879", Name: "prospera"},
		},
	})

	prosperaPool := queue.New(nil, queue.WorkerPoolConfig{
		DiscordSession: mockSess,
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			return nil
		},
		DeliveryWithAttachmentsFunc: func(s *discordgo.Session, channelID, text string, atts []*delivery.Attachment) error {
			return nil
		},
	})
	restErrPool := queue.New(nil, queue.WorkerPoolConfig{
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			return &discordgo.RESTError{Response: &http.Response{StatusCode: http.StatusForbidden}}
		},
	})
	sendErrPool := queue.New(nil, queue.WorkerPoolConfig{
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			return errors.New("cannot send messages to this thread")
		},
	})

	oversizedB64 := base64.StdEncoding.EncodeToString(make([]byte, 8*1024*1024+1))

	for _, tc := range []directMessageCase{
		{"method not allowed", http.MethodGet, &queue.WorkerPool{}, nil, http.StatusMethodNotAllowed, "Method not allowed"},
		{"nil pool", http.MethodPost, nil, strings.NewReader(`{"channel_id":"1542423172400291873","content":"hello"}`), http.StatusInternalServerError, "Worker pool not available"},
		{"read and close error", http.MethodPost, &queue.WorkerPool{}, &errReadCloser{readErr: errors.New("read err"), closeErr: errors.New("close err")}, http.StatusBadRequest, "Failed to read request body"},
		{"invalid JSON", http.MethodPost, &queue.WorkerPool{}, strings.NewReader("{invalid-json"), http.StatusBadRequest, "Invalid JSON payload"},
		{"missing channel_id", http.MethodPost, &queue.WorkerPool{}, strings.NewReader(`{"content":"hello"}`), http.StatusBadRequest, "Invalid payload: 'channel_id' field is required and cannot be empty"},
		{"non-snowflake channel_id", http.MethodPost, &queue.WorkerPool{}, strings.NewReader(`{"channel_id":"not-a-snowflake","content":"hello"}`), http.StatusBadRequest, "Invalid payload: 'channel_id' must be a valid numeric Discord snowflake"},
		{"unresolvable channel name", http.MethodPost, prosperaPool, strings.NewReader(`{"channel_id":"unknown-channel","content":"hello"}`), http.StatusBadRequest, "Invalid payload: 'channel_id' must be a valid numeric Discord snowflake"},
		{"resolve channel name prospera", http.MethodPost, prosperaPool, strings.NewReader(`{"channel_id":"prospera","content":"hello"}`), http.StatusOK, "1564150557705248879"},
		{"resolve hash channel name prospera", http.MethodPost, prosperaPool, strings.NewReader(`{"channel_id":"#prospera","content":"hello"}`), http.StatusOK, "1564150557705248879"},
		{"empty content without attachments", http.MethodPost, &queue.WorkerPool{}, strings.NewReader(`{"channel_id":"1542423172400291873","content":"  "}`), http.StatusBadRequest, "Invalid payload: 'content' field is required and cannot be empty"},
		{"too many attachments", http.MethodPost, prosperaPool, strings.NewReader(`{"channel_id":"1542423172400291873","content":"hi","attachments":[{"filename":"1","content_base64":"YQ=="},{"filename":"2","content_base64":"YQ=="},{"filename":"3","content_base64":"YQ=="},{"filename":"4","content_base64":"YQ=="},{"filename":"5","content_base64":"YQ=="},{"filename":"6","content_base64":"YQ=="},{"filename":"7","content_base64":"YQ=="},{"filename":"8","content_base64":"YQ=="},{"filename":"9","content_base64":"YQ=="},{"filename":"10","content_base64":"YQ=="},{"filename":"11","content_base64":"YQ=="}]}`), http.StatusBadRequest, "Too many attachments"},
		{"empty attachment filename", http.MethodPost, prosperaPool, strings.NewReader(`{"channel_id":"1542423172400291873","attachments":[{"filename":"   ","content_base64":"YQ=="}]}`), http.StatusBadRequest, "Invalid attachment: 'filename' is required"},
		{"invalid base64", http.MethodPost, prosperaPool, strings.NewReader(`{"channel_id":"1542423172400291873","attachments":[{"filename":"a.txt","content_base64":"not@base64"}]}`), http.StatusBadRequest, "Invalid attachment base64"},
		{"empty base64 decoded data", http.MethodPost, prosperaPool, strings.NewReader(`{"channel_id":"1542423172400291873","attachments":[{"filename":"a.txt","content_base64":""}]}`), http.StatusBadRequest, "Invalid attachment: payload is empty"},
		{"oversized attachment", http.MethodPost, prosperaPool, strings.NewReader(`{"channel_id":"1542423172400291873","attachments":[{"filename":"huge.bin","content_base64":"` + oversizedB64 + `"}]}`), http.StatusBadRequest, "exceeds maximum size"},
		{"attachment with empty content allowed", http.MethodPost, prosperaPool, strings.NewReader(`{"channel_id":"1542423172400291873","attachments":[{"filename":"audio.wav","content_base64":"UklGRg=="}]}`), http.StatusOK, "sent"},
		{"discord session not connected", http.MethodPost, &queue.WorkerPool{}, strings.NewReader(`{"channel_id":"1542423172400291873","content":"hello"}`), http.StatusServiceUnavailable, "Discord session not connected"},
		{"discord client RESTError", http.MethodPost, restErrPool, strings.NewReader(`{"channel_id":"1542423172400291873","content":"hello"}`), http.StatusForbidden, "Discord client error"},
		{"discord client cannot send", http.MethodPost, sendErrPool, strings.NewReader(`{"channel_id":"1542423172400291873","content":"hello"}`), http.StatusBadRequest, "Discord client error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertDirectMessageCase(t, tc)
		})
	}

	t.Run("delivery success and fallback", func(t *testing.T) {
		runDirectMessageDeliveryCases(t)
	})

	t.Run("mux route mounting for /discord/message", func(t *testing.T) {
		runDirectMessageMuxCases(t)
	})
}

type statusWidgetCase struct {
	name              string
	method            string
	urlPath           string
	pool              *queue.WorkerPool
	body              string
	wantStatus        int
	wantSubstr        string
	useMux            bool
	wantMessageID     string
	wantDeliveredID   string
	wantDeliveredChan string
	wantChunkCount    int
}

func assertStatusWidgetCase(t *testing.T, tc statusWidgetCase, capturedReq *discord.StatusWidgetRequest) {
	t.Helper()
	var r io.Reader
	if tc.body != "" {
		r = strings.NewReader(tc.body)
	}
	path := tc.urlPath
	if path == "" {
		path = "/discord/widget"
	}
	var w *httptest.ResponseRecorder
	if tc.useMux {
		mux := SetupBrainMuxWithEmbedder(dbtest.NewFakeStore(), dbtest.NewFakeStore(), tc.pool, nil)
		w = httptest.NewRecorder()
		mux.ServeHTTP(w, httptest.NewRequest(tc.method, path, r))
	} else {
		w = runHTTP(handleStatusWidget(tc.pool), tc.method, path, r)
	}

	if w.Code != tc.wantStatus {
		t.Fatalf("got status=%d body=%q, want status=%d", w.Code, w.Body.String(), tc.wantStatus)
	}
	if tc.wantSubstr != "" && !strings.Contains(w.Body.String(), tc.wantSubstr) {
		t.Fatalf("got body=%q, want substr=%q", w.Body.String(), tc.wantSubstr)
	}
	if tc.wantMessageID != "" {
		var resp map[string]any
		_ = json.Unmarshal(w.Body.Bytes(), &resp)
		if resp["status"] != "ok" || resp["message_id"] != tc.wantMessageID {
			t.Fatalf("unexpected response body: %+v", resp)
		}
	}
	if tc.wantDeliveredID != "" && (capturedReq.ID != tc.wantDeliveredID || capturedReq.ChannelID != tc.wantDeliveredChan || len(capturedReq.Chunks) != tc.wantChunkCount) {
		t.Fatalf("unexpected captured request: %+v", capturedReq)
	}
}

func TestHandleStatusWidget(t *testing.T) {
	restErrPool := queue.New(nil, queue.WorkerPoolConfig{
		WidgetDeliveryFunc: func(req discord.StatusWidgetRequest) (string, error) {
			return "", &discordgo.RESTError{Response: &http.Response{StatusCode: http.StatusForbidden}}
		},
	})
	sendErrPool := queue.New(nil, queue.WorkerPoolConfig{
		WidgetDeliveryFunc: func(req discord.StatusWidgetRequest) (string, error) {
			return "", errors.New("cannot send messages to this thread")
		},
	})
	failErrPool := queue.New(nil, queue.WorkerPoolConfig{
		WidgetDeliveryFunc: func(req discord.StatusWidgetRequest) (string, error) {
			return "", errors.New("database connection reset")
		},
	})

	var capturedReq discord.StatusWidgetRequest
	successPool := queue.New(nil, queue.WorkerPoolConfig{
		WidgetDeliveryFunc: func(req discord.StatusWidgetRequest) (string, error) {
			capturedReq = req
			return "created-widget-msg-789", nil
		},
	})

	muxPool := queue.New(nil, queue.WorkerPoolConfig{
		WidgetDeliveryFunc: func(req discord.StatusWidgetRequest) (string, error) { return "widget-99", nil },
	})

	tests := []statusWidgetCase{
		{name: "method not allowed", method: http.MethodGet, pool: &queue.WorkerPool{}, wantStatus: http.StatusMethodNotAllowed, wantSubstr: "Method not allowed"},
		{name: "nil pool", method: http.MethodPost, pool: nil, body: `{"channel_id":"1542423172400291873","chunks":[{"title":"task"}]}`, wantStatus: http.StatusInternalServerError, wantSubstr: "Worker pool not available"},
		{name: "invalid JSON", method: http.MethodPost, pool: &queue.WorkerPool{}, body: "{invalid-json", wantStatus: http.StatusBadRequest, wantSubstr: "Invalid JSON payload"},
		{name: "missing channel_id", method: http.MethodPost, pool: &queue.WorkerPool{}, body: `{"chunks":[{"title":"task"}]}`, wantStatus: http.StatusBadRequest, wantSubstr: "Invalid payload: 'channel_id' field is required and cannot be empty"},
		{name: "non-snowflake channel_id", method: http.MethodPost, pool: &queue.WorkerPool{}, body: `{"channel_id":"invalid-snow","chunks":[{"title":"task"}]}`, wantStatus: http.StatusBadRequest, wantSubstr: "Invalid payload: 'channel_id' must be a valid numeric Discord snowflake"},
		{name: "empty chunks", method: http.MethodPost, pool: &queue.WorkerPool{}, body: `{"channel_id":"1542423172400291873","chunks":[]}`, wantStatus: http.StatusBadRequest, wantSubstr: "Invalid payload: 'chunks' field is required and cannot be empty"},
		{name: "discord session not connected", method: http.MethodPost, pool: &queue.WorkerPool{}, body: `{"channel_id":"1542423172400291873","chunks":[{"title":"task"}]}`, wantStatus: http.StatusServiceUnavailable, wantSubstr: "Discord session not connected"},
		{name: "discord client RESTError", method: http.MethodPost, pool: restErrPool, body: `{"channel_id":"1542423172400291873","chunks":[{"title":"task"}]}`, wantStatus: http.StatusForbidden, wantSubstr: "Discord client error"},
		{name: "discord client cannot send", method: http.MethodPost, pool: sendErrPool, body: `{"channel_id":"1542423172400291873","chunks":[{"title":"task"}]}`, wantStatus: http.StatusBadRequest, wantSubstr: "Discord client error"},
		{name: "generic delivery failure", method: http.MethodPost, pool: failErrPool, body: `{"channel_id":"1542423172400291873","chunks":[{"title":"task"}]}`, wantStatus: http.StatusInternalServerError, wantSubstr: "Failed to deliver widget"},
		{
			name:              "success returns 200 with message_id and delivers widget",
			method:            http.MethodPost,
			pool:              successPool,
			body:              `{"channel_id":"1542423172400291873","id":"build-123","title":"Build Title","status":"running","chunks":[{"index":1,"repo":"aerial","title":"Chunk 1","status":"completed"}]}`,
			wantStatus:        http.StatusOK,
			wantMessageID:     "created-widget-msg-789",
			wantDeliveredID:   "build-123",
			wantDeliveredChan: "1542423172400291873",
			wantChunkCount:    1,
		},
		{
			name:       "mux route mounting for /discord/widget",
			method:     http.MethodPost,
			urlPath:    "/discord/widget",
			pool:       muxPool,
			body:       `{"channel_id":"1542423172400291873","chunks":[{"title":"t"}]}`,
			useMux:     true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "mux route dropped for /internal/widget",
			method:     http.MethodPost,
			urlPath:    "/internal/widget",
			pool:       muxPool,
			body:       `{"channel_id":"1542423172400291873","chunks":[{"title":"t"}]}`,
			useMux:     true,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assertStatusWidgetCase(t, tc, &capturedReq)
		})
	}
}

func runTranscriptSearchSuccessCases(t *testing.T, fake *dbtest.FakeStore, embedder func(context.Context, string) ([]float32, error), embedderCalled *bool) {
	t.Helper()
	t.Run("search success and embedder", func(t *testing.T) {
		w := runHTTP(handleTranscriptSearch(fake, embedder), http.MethodGet, "/api/transcripts/search?q=container&mode=auto&limit=5&tool=run_command&session=sess-mux-1", nil)
		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d: %s", w.Code, w.Body.String())
		}
		if !*embedderCalled {
			t.Error("expected embedder to be invoked")
		}
		var res transcript.SearchResult
		if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil || len(res.Sessions) != 1 || res.Sessions[0].SessionID != "sess-mux-1" {
			t.Fatalf("unexpected search result: %+v, err: %v", res, err)
		}
	})

	t.Run("search with since and empty query steps mode", func(t *testing.T) {
		sinceTime := time.Now().Add(-10 * time.Minute).Format(time.RFC3339)
		w := runHTTP(handleTranscriptSearch(fake, nil), http.MethodGet, "/api/transcripts/search?mode=steps&session=sess-mux-1&since="+url.QueryEscape(sinceTime), nil)
		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d: %s", w.Code, w.Body.String())
		}
	})

	t.Run("search tool kind matches native name", func(t *testing.T) {
		w := runHTTP(handleTranscriptSearch(fake, nil), http.MethodGet, "/api/transcripts/search?mode=steps&session=sess-mux-1&tool=shell", nil)
		var resTool transcript.SearchResult
		if err := json.Unmarshal(w.Body.Bytes(), &resTool); err != nil || len(resTool.Steps) != 1 || resTool.Steps[0].ToolName != "Bash" || resTool.Steps[0].ToolKind != "shell" || resTool.Steps[0].Stack != "fake" {
			t.Fatalf("unexpected steps: %d %+v err=%v", w.Code, resTool, err)
		}
	})

	t.Run("search filtered to other stack", func(t *testing.T) {
		w := runHTTP(handleTranscriptSearch(fake, nil), http.MethodGet, "/api/transcripts/search?q=container&stack=elsewhere", nil)
		var resOther transcript.SearchResult
		if err := json.Unmarshal(w.Body.Bytes(), &resOther); err != nil || w.Code != http.StatusOK || resOther.Total != 0 {
			t.Fatalf("expected empty 200 result, got %d %+v err=%v", w.Code, resOther, err)
		}
	})
}

func runTranscriptStatsAndStepsCases(t *testing.T, fake *dbtest.FakeStore) {
	t.Helper()
	for _, tc := range []struct {
		name   string
		target string
		want   string
	}{
		{"stats all stacks", "/api/transcripts/stats", `{"stacks":[{"stack":"fake","sessions":1,"steps":1}],"status":"ok","total_sessions":1,"total_steps":1}`},
		{"stats filtered to other stack", "/api/transcripts/stats?stack=elsewhere", `{"stacks":[],"status":"ok","total_sessions":0,"total_steps":0}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := runHTTP(handleTranscriptStats(fake), http.MethodGet, tc.target, nil)
			if w.Code != http.StatusOK {
				t.Fatalf("expected 200 OK, got %d: %s", w.Code, w.Body.String())
			}
			if got := strings.TrimSpace(w.Body.String()); got != tc.want {
				t.Fatalf("stats body = %s, want %s", got, tc.want)
			}
		})
	}

	for _, tc := range []struct {
		name       string
		target     string
		wantStacks []string
	}{
		{"steps all stacks", "/api/transcripts/steps?session=sess-mux-1&step=0", []string{"fake"}},
		{"steps own stack", "/api/transcripts/steps?session=sess-mux-1&step=0&stack=fake", []string{"fake"}},
		{"steps other stack is empty", "/api/transcripts/steps?session=sess-mux-1&step=0&stack=elsewhere", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := runHTTP(handleTranscriptSteps(fake), http.MethodGet, tc.target, nil)
			var res transcript.StepWindowResult
			if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil || w.Code != http.StatusOK {
				t.Fatalf("got %d %s err=%v", w.Code, w.Body.String(), err)
			}
			gotStacks := make([]string, 0, len(res.Steps))
			for _, st := range res.Steps {
				gotStacks = append(gotStacks, st.Stack)
			}
			if !slices.Equal(gotStacks, tc.wantStacks) {
				t.Errorf("step stacks = %v, want %v", gotStacks, tc.wantStacks)
			}
		})
	}
}

func TestHandleTranscriptSearchAndStats(t *testing.T) {
	fake := dbtest.NewFakeStore()
	_ = fake.UpsertSessionSummary(context.Background(), db.SessionSummary{
		SessionID: "sess-mux-1",
		Summary:   "Triaged container restart in aerial",
	})
	_ = fake.BatchInsertTranscriptSteps(context.Background(), []db.TranscriptStep{
		{SessionID: "sess-mux-1", StepIndex: 0, StepType: "tool_call", ToolName: "Bash", ToolKind: "shell", Content: "kubectl rollout restart"},
	})
	errStore := &errTranscriptStore{FakeStore: dbtest.NewFakeStore()}
	embedderCalled := false
	embedder := func(ctx context.Context, text string) ([]float32, error) {
		embedderCalled = true
		return make([]float32, 384), nil
	}

	for _, tc := range []struct {
		name       string
		handler    http.HandlerFunc
		method     string
		target     string
		wantStatus int
	}{
		{"search method not allowed", handleTranscriptSearch(fake, nil), http.MethodPost, "/api/transcripts/search", http.StatusMethodNotAllowed},
		{"search nil store", handleTranscriptSearch(nil, nil), http.MethodGet, "/api/transcripts/search?q=test", http.StatusInternalServerError},
		{"search store error", handleTranscriptSearch(errStore, nil), http.MethodGet, "/api/transcripts/search?q=test&mode=semantic", http.StatusInternalServerError},
		{"stats method not allowed", handleTranscriptStats(fake), http.MethodPost, "/api/transcripts/stats", http.StatusMethodNotAllowed},
		{"stats nil store", handleTranscriptStats(nil), http.MethodGet, "/api/transcripts/stats", http.StatusInternalServerError},
		{"stats store error", handleTranscriptStats(errStore), http.MethodGet, "/api/transcripts/stats", http.StatusInternalServerError},
		{"search query param alias", handleTranscriptSearch(fake, nil), http.MethodGet, "/api/transcripts/search?query=fallback-query&limit=abc", http.StatusOK},
		{"steps method not allowed", handleTranscriptSteps(fake), http.MethodPost, "/api/transcripts/steps?session=sess-1&step=0", http.StatusMethodNotAllowed},
		{"steps nil store", handleTranscriptSteps(nil), http.MethodGet, "/api/transcripts/steps?session=sess-1&step=0", http.StatusInternalServerError},
		{"steps missing session", handleTranscriptSteps(fake), http.MethodGet, "/api/transcripts/steps?step=0", http.StatusBadRequest},
		{"steps empty session", handleTranscriptSteps(fake), http.MethodGet, "/api/transcripts/steps?session=%20%20&step=0", http.StatusBadRequest},
		{"steps missing step", handleTranscriptSteps(fake), http.MethodGet, "/api/transcripts/steps?session=sess-1", http.StatusBadRequest},
		{"steps invalid step non-integer", handleTranscriptSteps(fake), http.MethodGet, "/api/transcripts/steps?session=sess-1&step=xyz", http.StatusBadRequest},
		{"steps invalid step negative", handleTranscriptSteps(fake), http.MethodGet, "/api/transcripts/steps?session=sess-1&step=-1", http.StatusBadRequest},
		{"steps invalid context non-integer", handleTranscriptSteps(fake), http.MethodGet, "/api/transcripts/steps?session=sess-1&step=0&context=abc", http.StatusBadRequest},
		{"steps store error", handleTranscriptSteps(errStore), http.MethodGet, "/api/transcripts/steps?session=sess-1&step=0", http.StatusInternalServerError},
		{"steps success default context", handleTranscriptSteps(fake), http.MethodGet, "/api/transcripts/steps?session=sess-mux-1&step=0", http.StatusOK},
		{"steps success custom context", handleTranscriptSteps(fake), http.MethodGet, "/api/transcripts/steps?session=sess-mux-1&step=0&context=5", http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if w := runHTTP(tc.handler, tc.method, tc.target, nil); w.Code != tc.wantStatus {
				t.Fatalf("got status=%d, want %d (body: %s)", w.Code, tc.wantStatus, w.Body.String())
			}
		})
	}

	runTranscriptSearchSuccessCases(t, fake, embedder, &embedderCalled)

	runTranscriptStatsAndStepsCases(t, fake)

	t.Run("mux endpoints", func(t *testing.T) {
		mux := SetupBrainMuxWithEmbedder(fake, fake, nil, nil)
		for _, path := range []string{"/api/transcripts/search?q=test", "/api/transcripts/stats", "/api/transcripts/steps?session=sess-mux-1&step=0"} {
			if w := runHTTP(mux.ServeHTTP, http.MethodGet, path, nil); w.Code != http.StatusOK {
				t.Errorf("expected 200 for %s, got %d", path, w.Code)
			}
		}
	})
}

type errTranscriptStore struct {
	*dbtest.FakeStore
}

func (e *errTranscriptStore) GetTranscriptStats(ctx context.Context, stack string) ([]db.TranscriptStackStats, error) {
	return nil, errors.New("simulated transcript stats error")
}

func (e *errTranscriptStore) SearchTranscriptSteps(ctx context.Context, stack, query, sessionFilter, toolFilter string, limit int, since ...time.Time) ([]db.TranscriptStep, error) {
	return nil, errors.New("simulated search transcript steps error")
}

func (e *errTranscriptStore) SearchSessionSummaries(ctx context.Context, stack string, queryEmbedding []float32, textQuery string, limit int, threshold float64) ([]db.SessionSummary, error) {
	return nil, errors.New("simulated search session summaries error")
}

func (e *errTranscriptStore) GetTranscriptStepRange(ctx context.Context, stack, sessionID string, startStep, endStep int) ([]db.TranscriptStep, error) {
	return nil, errors.New("simulated get transcript step range error")
}

func runHandleTasksFilterCases(t *testing.T, handler http.HandlerFunc, now time.Time) {
	t.Helper()
	for _, tc := range []struct {
		name string
		path string
	}{
		{name: "status filter", path: "/tasks?status=PENDING,PROCESSING"},
		{name: "thread_id filter", path: "/tasks?thread_id=thread-1"},
		{name: "is_admin filter", path: "/tasks?is_admin=true"},
		{name: "since filter", path: "/tasks?since=" + url.QueryEscape(now.Add(-time.Hour).Format(time.RFC3339))},
		{name: "after_row_id filter", path: "/tasks?after_row_id=0"},
		{name: "before_row_id filter", path: "/tasks?before_row_id=100"},
		{name: "limit filter", path: "/tasks?limit=10"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rr := runHTTP(handler, http.MethodGet, tc.path, nil)
			if rr.Code != http.StatusOK {
				t.Fatalf("expected 200 OK, got %d: %s", rr.Code, rr.Body.String())
			}
		})
	}
}

func runHandleTasksErrorCases(t *testing.T, handler http.HandlerFunc) {
	t.Helper()
	if code := runHTTP(handler, http.MethodPost, "/tasks", nil).Code; code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 Method Not Allowed, got %d", code)
	}

	closedStore := dbtest.NewFakeStore()
	_ = closedStore.Close()
	for _, tc := range []struct {
		name    string
		handler http.HandlerFunc
	}{
		{"nil store", handleTasks(nil)},
		{"closed store", handleTasks(closedStore)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if code := runHTTP(tc.handler, http.MethodGet, "/tasks", nil).Code; code != http.StatusInternalServerError {
				t.Fatalf("got status=%d, want 500", code)
			}
		})
	}
}

func runHandleTasksEphemeralCase(t *testing.T, now time.Time) {
	t.Helper()
	ephemStore := dbtest.NewFakeStore()
	_ = ephemStore.SaveSessionID(context.Background(), "1564704264645902626", "parent-thread-sess-uuid")
	turnStarted := make(chan struct{})
	releaseTurn := make(chan struct{})
	ephemPool := queue.New(nil, queue.WorkerPoolConfig{
		Store: ephemStore,
		MediumEffortProcessPool: &blockingTaskTestPool{
			sessionID:   "88888888-4444-4444-4444-121212121212",
			turnStarted: turnStarted,
			releaseTurn: releaseTurn,
		},
	})
	defer ephemPool.Stop()

	ephemDisp := db.Dispatch{
		ID:        "msg-vanadis-chunk",
		Source:    "vanadis",
		Prompt:    "[VANADIS BUILD CHUNK EXECUTION]",
		Effort:    "medium",
		Status:    db.StatusPending,
		CreatedAt: now,
		UpdatedAt: now,
	}
	_, _ = ephemStore.InsertDispatch(context.Background(), ephemDisp)
	ephemPool.EnqueueDispatch(ephemDisp)
	<-turnStarted

	liveRR := runHTTP(handleTasks(ephemStore, ephemPool), http.MethodGet, "/tasks", nil)
	close(releaseTurn)
	if liveRR.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from live tasks handler, got %d", liveRR.Code)
	}
	var liveResp TasksResponse
	_ = json.NewDecoder(liveRR.Body).Decode(&liveResp)
	if len(liveResp.Tasks) != 1 || liveResp.Tasks[0].SessionID != "88888888-4444-4444-4444-121212121212" {
		t.Fatalf("expected live ephemeral session ID instead of parent thread session, got %+v", liveResp.Tasks)
	}
}

func TestHandleTasks(t *testing.T) {
	store := dbtest.NewFakeStore()
	now := time.Now().UTC()
	longPrompt := strings.Repeat("A", 600) + " ghp_123456789012345678901234567890123456"
	_ = store.InsertMessage(context.Background(), db.Message{
		ID: "msg-test-task", ThreadID: "thread-1", AuthorID: "user-1",
		AuthorName: "Tester with token ghp_999999999999999999999999999999999999",
		Content:    "Secret token ghp_123456789012345678901234567890123456 in prompt " + longPrompt,
		Status:     db.StatusProcessing, CreatedAt: now, UpdatedAt: now,
	})

	handler := handleTasks(store)
	rr := runHTTP(handler, http.MethodGet, "/tasks", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", rr.Code, rr.Body.String())
	}

	var resp struct {
		Status string          `json:"status"`
		Total  int             `json:"total"`
		Tasks  []db.ActiveTask `json:"tasks"`
	}
	_ = json.NewDecoder(rr.Body).Decode(&resp)
	if resp.Status != "ok" || resp.Total != 1 || len(resp.Tasks) != 1 {
		t.Fatalf("unexpected response payload: %+v", resp)
	}

	task := resp.Tasks[0]
	if strings.Contains(task.Prompt, "ghp_123456") || strings.Contains(task.Summary, "ghp_123456") || task.Summary == "" || strings.Contains(task.AuthorName, "ghp_999999") {
		t.Errorf("token leaked or summary empty: %+v", task)
	}
	if len(task.Prompt) < 600 {
		t.Errorf("prompt unexpectedly truncated: len=%d, text=%s", len(task.Prompt), task.Prompt)
	}

	_ = store.InsertMessage(context.Background(), db.Message{
		ID: "msg-test-task-2", ThreadID: "thread-2", AuthorID: "user-2", AuthorName: "Tester 2",
		Content: "Second prompt", Status: db.StatusPending, CreatedAt: now.Add(time.Second), UpdatedAt: now.Add(time.Second),
	})
	rrCached := runHTTP(handler, http.MethodGet, "/tasks", nil)
	var cachedResp struct {
		Status string          `json:"status"`
		Total  int             `json:"total"`
		Tasks  []db.ActiveTask `json:"tasks"`
	}
	_ = json.NewDecoder(rrCached.Body).Decode(&cachedResp)
	if cachedResp.Total != 1 || len(cachedResp.Tasks) != 1 {
		t.Errorf("expected 1 cached task within 1s TTL, got %d", cachedResp.Total)
	}

	runHandleTasksFilterCases(t, handler, now)
	runHandleTasksErrorCases(t, handler)

	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			handler(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/tasks", nil))
		})
	}
	wg.Wait()

	runHandleTasksEphemeralCase(t, now)
}

type blockingTaskTestPool struct {
	sessionID   string
	turnStarted chan struct{}
	releaseTurn chan struct{}
}

func (p *blockingTaskTestPool) AcquireLease(ctx context.Context, targetKey string) (runner.SessionLease, error) {
	return &blockingTaskTestLease{pool: p}, nil
}
func (p *blockingTaskTestPool) Initialize(ctx context.Context) error { return nil }
func (p *blockingTaskTestPool) Close() error                         { return nil }

type blockingTaskTestLease struct {
	pool *blockingTaskTestPool
}

func (l *blockingTaskTestLease) SessionID() string         { return l.pool.sessionID }
func (l *blockingTaskTestLease) PreviousSessionID() string { return "" }
func (l *blockingTaskTestLease) IsCold() bool              { return false }
func (l *blockingTaskTestLease) IsEphemeral() bool         { return true }
func (l *blockingTaskTestLease) TurnCount() int            { return 0 }
func (l *blockingTaskTestLease) MarkDirty()                {}
func (l *blockingTaskTestLease) Release() error            { return nil }
func (l *blockingTaskTestLease) Execute(ctx context.Context, turn *runner.TurnContext) (*runner.TurnResult, error) {
	close(l.pool.turnStarted)
	<-l.pool.releaseTurn
	return &runner.TurnResult{ConversationID: l.pool.sessionID, Response: "done"}, nil
}

func TestMetricsEndpoint(t *testing.T) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())

	// Record sample metrics to instantiate metric vectors
	metrics.RecordTurnCompleted("success", "discord", "gemini-3.8-flash-low", 2*time.Second)
	metrics.RecordClassifierRun("success", "gemini-3.8-flash-low", 500*time.Millisecond, 0.95, "wake")
	metrics.DiscordEventsTotal.WithLabelValues("ready").Inc()
	metrics.DiscordMessagesProcessedTotal.WithLabelValues("false", "enqueued").Inc()
	metrics.SchedulerExecutionsTotal.WithLabelValues("cron", "enqueued").Inc()

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200 OK from /metrics, got %d", rec.Code)
	}

	body := rec.Body.String()
	requiredSubstrings := []string{
		"aerial_brain_turns_total",
		"aerial_brain_turn_duration_seconds",
		"aerial_brain_active_workers",
		"aerial_brain_queue_depth",
		"aerial_brain_classifier_duration_seconds",
		"aerial_brain_classifier_decisions_total",
		"aerial_brain_classifier_confidence_score",
		"aerial_brain_discord_events_total",
		"aerial_brain_discord_messages_processed_total",
		"aerial_brain_scheduler_executions_total",
		"aerial_brain_build_info",
	}

	for _, sub := range requiredSubstrings {
		if !strings.Contains(body, sub) {
			t.Errorf("expected /metrics output to contain %q, body:\n%s", sub, body)
		}
	}
}

func TestNormalizeRoute(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input, expected string
	}{
		{"/prompt", "/prompt"},
		{"/dispatch", "/dispatch"},
		{"/dispatch/cancel", "/dispatch/cancel"},
		{"/transcripts/abc-123", "/transcripts"},
		{"/api/transcripts/steps", "/transcripts"},
		{"/api/transcripts/search", "/transcripts"},
		{"/tasks", "/tasks"},
		{"/tasks/456", "/tasks"},
		{"/internal/llm", "/internal/llm"},
		{"/internal/discord/message", "/internal/discord/message"},
		{"/internal/discord/interaction", "/internal/discord/interaction"},
		{"/internal/guard/scan", "/internal/guard/scan"},
		{"/schedules", "unmatched"},
		{"/schedules/runs", "unmatched"},
		{"/transcripts", "unmatched"},
		{"/health", "/health"},
		{"/healthz", "/health"},
		{"/metrics", "/metrics"},
		{"/voice/ask", "/voice/ask"},
		{"/api/voice/ask", "/voice/ask"},
		{"/internal/github-api/repos/azylman/aerial/pulls", "/internal/github-api"},
		{"/internal/github-git/azylman/aerial.git/info/refs", "/internal/github-git"},
		{"/unknown/path", "unmatched"},
		{"/random", "unmatched"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			if got := normalizeRoute(tt.input); got != tt.expected {
				t.Errorf("normalizeRoute(%q) = %q; want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestMetricsMiddleware(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("/unknown", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	mux.HandleFunc("/implicit", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})

	handler := metricsMiddleware(mux)

	if w := runHTTP(handler.ServeHTTP, http.MethodGet, "/health", nil); w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", w.Code)
	}
	if w := runHTTP(handler.ServeHTTP, http.MethodGet, "/unknown", nil); w.Code != http.StatusNotFound {
		t.Fatalf("expected status 404, got %d", w.Code)
	}
	if w := runHTTP(handler.ServeHTTP, http.MethodGet, "/implicit", nil); w.Code != http.StatusOK {
		t.Errorf("expected 200 for implicit write, got %d", w.Code)
	}

	emptyMethodHandler := metricsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	reqEmpty := httptest.NewRequest("", "/metrics", nil)
	reqEmpty.Method = ""
	recEmpty := httptest.NewRecorder()
	emptyMethodHandler.ServeHTTP(recEmpty, reqEmpty)
	if recEmpty.Code != http.StatusOK {
		t.Errorf("expected 200 OK for empty method, got %d", recEmpty.Code)
	}
}

type mockFlusherRecorder struct {
	*httptest.ResponseRecorder

	flushed bool
}

func (m *mockFlusherRecorder) Flush() {
	m.flushed = true
}

func TestStatusRecorder_Flush(t *testing.T) {
	t.Parallel()

	// With flusher
	rec := &mockFlusherRecorder{ResponseRecorder: httptest.NewRecorder()}
	sr := &statusRecorder{ResponseWriter: rec, statusCode: http.StatusOK}
	sr.Flush()
	if !rec.flushed {
		t.Errorf("Expected Flush() to be forwarded to underlying flusher")
	}

	// Without flusher
	plainRec := httptest.NewRecorder()
	srPlain := &statusRecorder{ResponseWriter: struct{ http.ResponseWriter }{plainRec}, statusCode: http.StatusOK}
	srPlain.Flush() // should not panic
}

func runHTTPMemoryRetrieverCases(t *testing.T) {
	t.Helper()
	memSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("q") == "fail" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if r.URL.Query().Get("q") == "badjson" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("{bad"))
			return
		}
		httpkit.WriteJSON(w, http.StatusOK, map[string]string{"context": "<retrieved_memory>ok</retrieved_memory>"})
	}))
	defer memSrv.Close()

	ret := newHTTPMemoryRetriever(memSrv.URL)
	if out, err := ret(context.Background(), ""); err != nil || out != "" {
		t.Errorf("expected empty on blank query, got %q, %v", out, err)
	}
	if out, err := ret(context.Background(), "hello world"); err != nil || !strings.Contains(out, "<retrieved_memory>") {
		t.Errorf("expected retrieved memory, got %q, %v", out, err)
	}
	if _, err := ret(context.Background(), "fail"); err == nil {
		t.Errorf("expected error on 500 status")
	}
	if _, err := ret(context.Background(), "badjson"); err == nil {
		t.Errorf("expected error on bad json")
	}

	retEmpty := newHTTPMemoryRetriever("")
	if out, err := retEmpty(context.Background(), "hello"); err != nil || out != "" {
		t.Errorf("expected empty string and nil error on empty base URL, got %q, %v", out, err)
	}
}

func TestHandleInternalLLM_And_MemoryRetriever(t *testing.T) {
	attSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/file.txt":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("attachment content"))
		case "/notfound":
			http.NotFound(w, r)
		case "/servererror":
			w.WriteHeader(http.StatusInternalServerError)
		default:
			http.NotFound(w, r)
		}
	}))
	defer attSrv.Close()

	attSrvURL, _ := url.Parse(attSrv.URL)
	attSrvHost := strings.ToLower(attSrvURL.Hostname())
	delivery.AllowedInternalHosts[attSrvHost] = true
	defer delete(delivery.AllowedInternalHosts, attSrvHost)

	var observedPrompt string
	var observedDir string
	var stagedFileContent []byte
	var stagedFileExistsDuringRun bool
	var observedDirPerm os.FileMode
	var stagedFilePerm os.FileMode

	stagingLLM := handleInternalLLM(func(ctx context.Context, model, p string) (string, error) {
		observedPrompt = p
		if _, after, ok := strings.Cut(p, "Staged directory: "); ok {
			rest := after
			lines := strings.Split(rest, "\n")
			observedDir = strings.TrimSpace(lines[0])
			if info, err := os.Stat(observedDir); err == nil {
				observedDirPerm = info.Mode().Perm()
			}
			stagedPath := filepath.Join(observedDir, "test.txt")
			if info, err := os.Stat(stagedPath); err == nil {
				stagedFilePerm = info.Mode().Perm()
			}
			if data, err := os.ReadFile(stagedPath); err == nil {
				stagedFileExistsDuringRun = true
				stagedFileContent = data
			}
		}
		return `[{"category":"system","fact":"ok"}]`, nil
	})

	okLLM := handleInternalLLM(func(ctx context.Context, model, p string) (string, error) {
		return `[{"category":"system","fact":"ok"}]`, nil
	})
	errLLM := handleInternalLLM(func(ctx context.Context, model, p string) (string, error) {
		return "", errors.New("llm failed")
	})
	nilLLM := handleInternalLLM(nil)

	attsJSON := func(n int) string {
		atts := make([]string, 0, n)
		for i := range n {
			atts = append(atts, fmt.Sprintf(`{"url":"%s/file.txt","filename":"file%d.txt"}`, attSrv.URL, i))
		}
		return fmt.Sprintf(`{"prompt":"inspect","attachments":[%s]}`, strings.Join(atts, ","))
	}
	elevenAttsJSON := attsJSON(11)
	tooManyAttsJSON := attsJSON(33)

	for _, tc := range []struct {
		name       string
		method     string
		handler    http.HandlerFunc
		body       string
		wantStatus int
	}{
		{"method not allowed", http.MethodGet, okLLM, "", http.StatusMethodNotAllowed},
		{"nil llm runner", http.MethodPost, nilLLM, `{"prompt":"hi"}`, http.StatusServiceUnavailable},
		{"invalid json", http.MethodPost, okLLM, `{bad`, http.StatusBadRequest},
		{"empty prompt", http.MethodPost, okLLM, `{"prompt":"   "}`, http.StatusBadRequest},
		{"llm error", http.MethodPost, errLLM, `{"prompt":"extract"}`, http.StatusInternalServerError},
		{"valid request", http.MethodPost, okLLM, `{"prompt":"extract"}`, http.StatusOK},
		{"valid attachment staging", http.MethodPost, stagingLLM, fmt.Sprintf(`{"prompt":"inspect","attachments":[{"url":"%s/file.txt","filename":"test.txt"}]}`, attSrv.URL), http.StatusOK},
		{"empty attachment url", http.MethodPost, okLLM, `{"prompt":"test","attachments":[{"url":"","filename":"test.txt"}]}`, http.StatusBadRequest},
		{"empty attachment filename", http.MethodPost, okLLM, fmt.Sprintf(`{"prompt":"test","attachments":[{"url":"%s/file.txt","filename":""}]}`, attSrv.URL), http.StatusBadRequest},
		{"duplicate attachment filename", http.MethodPost, okLLM, fmt.Sprintf(`{"prompt":"test","attachments":[{"url":"%s/file.txt","filename":"dup.txt"},{"url":"%s/file.txt","filename":"DUP.TXT"}]}`, attSrv.URL, attSrv.URL), http.StatusBadRequest},
		{"disallowed attachment url", http.MethodPost, okLLM, `{"prompt":"test","attachments":[{"url":"http://evil.com/file.txt","filename":"test.txt"}]}`, http.StatusBadRequest},
		{"invalid traversal attachment filename", http.MethodPost, okLLM, fmt.Sprintf(`{"prompt":"test","attachments":[{"url":"%s/file.txt","filename":"../test.txt"}]}`, attSrv.URL), http.StatusBadRequest},
		{"eleven attachments accepted", http.MethodPost, okLLM, elevenAttsJSON, http.StatusOK},
		{"exceeds max attachments", http.MethodPost, okLLM, tooManyAttsJSON, http.StatusBadRequest},
		{"upstream 404 fetch error", http.MethodPost, okLLM, fmt.Sprintf(`{"prompt":"test","attachments":[{"url":"%s/notfound","filename":"nf.txt"}]}`, attSrv.URL), http.StatusBadGateway},
		{"upstream 500 fetch error", http.MethodPost, okLLM, fmt.Sprintf(`{"prompt":"test","attachments":[{"url":"%s/servererror","filename":"err.txt"}]}`, attSrv.URL), http.StatusBadGateway},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if code := runHTTP(tc.handler, tc.method, "/internal/llm", strings.NewReader(tc.body)).Code; code != tc.wantStatus {
				t.Fatalf("got status=%d, want %d", code, tc.wantStatus)
			}
		})
	}

	if !stagedFileExistsDuringRun {
		t.Errorf("expected staged attachment file to exist during llmFn execution")
	}
	if string(stagedFileContent) != "attachment content" {
		t.Errorf("expected staged content %q, got %q", "attachment content", string(stagedFileContent))
	}
	if observedDirPerm != 0o755 {
		t.Errorf("expected staged directory perm 0755, got %#o", observedDirPerm)
	}
	if stagedFilePerm != 0o644 {
		t.Errorf("expected staged file perm 0644, got %#o", stagedFilePerm)
	}
	if !strings.Contains(observedPrompt, "<ATTACHMENTS>") {
		t.Errorf("expected observed prompt to contain <ATTACHMENTS>, got %q", observedPrompt)
	}
	if _, err := os.Stat(observedDir); !os.IsNotExist(err) {
		t.Errorf("expected staged tmpDir %q to be removed after handleInternalLLM returned", observedDir)
	}

	runHTTPMemoryRetrieverCases(t)
}

func TestSetupBrainMux_And_Endpoints(t *testing.T) {
	store := dbtest.NewFakeStore()
	pool := newTestWorkerPool(store)
	tmpHome := t.TempDir()
	mux := SetupBrainMuxWithEmbedder(store, store, pool, nil, tmpHome)

	if w := runHTTP(mux.ServeHTTP, http.MethodGet, "/health", nil); w.Code != http.StatusOK {
		t.Errorf("Expected 200 from /health, got %d", w.Code)
	}
	if w := runHTTP(mux.ServeHTTP, http.MethodGet, "/healthz", nil); w.Code != http.StatusOK {
		t.Errorf("Expected 200 from /healthz, got %d", w.Code)
	}
	mux.ServeHTTP(&flusherErrWriter{}, httptest.NewRequest(http.MethodGet, "/health", nil))

	if w := runHTTP(mux.ServeHTTP, http.MethodGet, "/tasks", nil); w.Code != http.StatusOK {
		t.Errorf("Expected 200 from /tasks, got %d", w.Code)
	}
	if w := runHTTP(mux.ServeHTTP, http.MethodPost, "/dispatch", strings.NewReader(`{"source":"heartbeat","prompt":"test"}`)); w.Code != http.StatusAccepted {
		t.Errorf("Expected 202 from /dispatch, got %d", w.Code)
	}
	if w := runHTTP(mux.ServeHTTP, http.MethodPost, "/dispatch/cancel", strings.NewReader(`{"dispatch_id":"disp-1"}`)); w.Code != http.StatusOK {
		t.Errorf("Expected 200 from /dispatch/cancel, got %d", w.Code)
	}
	if w := runHTTP(mux.ServeHTTP, http.MethodGet, "/internal/guard/scan", nil); w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected 405 from GET /internal/guard/scan, got %d", w.Code)
	}
	if w := runHTTP(mux.ServeHTTP, http.MethodPost, "/internal/guard/scan", strings.NewReader(`{"bad json`)); w.Code != http.StatusBadRequest {
		t.Errorf("Expected 400 from bad JSON to /internal/guard/scan, got %d", w.Code)
	}
	if w := runHTTP(mux.ServeHTTP, http.MethodPost, "/internal/guard/scan", strings.NewReader(`{"diff":"diff --git a/a.go b/a.go\n+var x = 1\n","text":"echo ok","ignore_internal_domain":true}`)); w.Code != http.StatusOK {
		t.Errorf("Expected 200 from valid /internal/guard/scan, got %d", w.Code)
	}
	if w := runHTTP(mux.ServeHTTP, http.MethodPost, "/internal/guard/scan", strings.NewReader(`{"text":"export TOKEN=ghp_111111111122222222223333333333444444"}`)); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"decision":"deny"`) {
		t.Errorf("Expected deny from secret /internal/guard/scan, got code=%d body=%s", w.Code, w.Body.String())
	}
	if w := runHTTP(mux.ServeHTTP, http.MethodGet, "/transcripts", nil); w.Code != http.StatusNotFound {
		t.Errorf("Expected 404 from deleted /transcripts, got %d", w.Code)
	}

	httpkit.WriteJSON(httptest.NewRecorder(), http.StatusOK, make(chan int))

	oldFn := httpkit.WriteCountersDirFn
	defer func() { httpkit.WriteCountersDirFn = oldFn }()
	oldMetaFn := httpkit.WriteMetaDirFn
	defer func() { httpkit.WriteMetaDirFn = oldMetaFn }()

	called, metaCalled := false, false
	httpkit.WriteCountersDirFn = func(dir string) error {
		called = true
		return nil
	}
	httpkit.WriteMetaDirFn = func(dir string) error {
		metaCalled = true
		return nil
	}

	if w := runHTTP(mux.ServeHTTP, http.MethodGet, "/debug/coverage/flush", nil); w.Code != http.StatusOK || !called || !metaCalled {
		t.Errorf("expected 200 OK with mocks called, got %d", w.Code)
	}

	httpkit.WriteMetaDirFn = func(dir string) error { return errors.New("simulated meta flush error") }
	if w := runHTTP(mux.ServeHTTP, http.MethodGet, "/debug/coverage/flush", nil); w.Code != http.StatusOK {
		t.Errorf("expected 200 OK when meta flush errors, got %d", w.Code)
	}

	httpkit.WriteCountersDirFn = func(dir string) error { return errors.New("simulated flush error") }
	if w := runHTTP(mux.ServeHTTP, http.MethodGet, "/debug/coverage/flush", nil); w.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 on flush error, got %d", w.Code)
	}
}
