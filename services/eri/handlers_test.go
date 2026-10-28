package main

import (
	"bytes"
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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
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
		{"invalid session_id", http.MethodPost, store, pool, strings.NewReader(`{"prompt":"hello","channel_id":"1542423172400291873","session_id":"invalid:colon"}`), http.StatusBadRequest, "session_id"},
		{"ephemeral true with high effort rejected", http.MethodPost, store, pool, strings.NewReader(`{"prompt":"hello","channel_id":"1542423172400291873","ephemeral":true,"effort":"high"}`), http.StatusBadRequest, "ephemeral: true cannot be combined with high effort"},
		{"ephemeral true with default high effort rejected", http.MethodPost, store, pool, strings.NewReader(`{"prompt":"hello","channel_id":"1542423172400291873","ephemeral":true}`), http.StatusBadRequest, "ephemeral: true cannot be combined with high effort"},
		{"nil store", http.MethodPost, nil, pool, strings.NewReader(`{"prompt":"hello","channel_id":"1542423172400291873"}`), http.StatusInternalServerError, "Database store not configured"},
		{"closed store", http.MethodPost, closedStore, pool, strings.NewReader(`{"prompt":"hello","channel_id":"1542423172400291873"}`), http.StatusInternalServerError, "Failed to persist prompt message"},
		{"nil store silent", http.MethodPost, nil, pool, strings.NewReader(`{"prompt":"hello silent","channel_id":"1542423172400291873","silent":true}`), http.StatusInternalServerError, "Database store not configured"},
		{"closed store silent", http.MethodPost, closedStore, pool, strings.NewReader(`{"prompt":"hello silent","channel_id":"1542423172400291873","silent":true}`), http.StatusInternalServerError, "Failed to persist prompt dispatch"},
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

	falseVal := false
	trueVal := true
	for _, tc := range []struct {
		name          string
		pool          *queue.WorkerPool
		payload       string
		msgID         string
		wantThreadID  string
		wantChannelID string
		wantEphemeral *bool
		wantEffort    string
		wantModel     string
		wantSilent    bool
		wantContent   string
	}{
		{"default effort", pool, `{"prompt":"Test valid prompt","channel_id":"1542423172400291873","message_id":"msg-123"}`, "msg-123", "1542423172400291873", "1542423172400291873", nil, "", "", false, "Test valid prompt"},
		{"gemini-4-flash silent", pool, `{"prompt":"Execute chunk 1","channel_id":"1542423172400291873","message_id":"msg-silent-1","model":"gemini-4-flash","silent":true}`, "msg-silent-1", "1542423172400291873", "1542423172400291873", nil, "low", "gemini-4-flash", true, ""},
		{"gemini-3.8-flash-medium", pool, `{"prompt":"Execute medium chunk","channel_id":"1542423172400291873","message_id":"msg-med-1","model":"gemini-3.8-flash-medium","silent":true}`, "msg-med-1", "1542423172400291873", "1542423172400291873", nil, "medium", "gemini-3.8-flash-medium", true, ""},
		{"silent heartbeat source and session", pool, `{"prompt":"heartbeat check","channel_id":"1542423172400291873","session_id":"heartbeat-svc","message_id":"msg-hb-1","silent":true}`, "msg-hb-1", "heartbeat-svc", "1542423172400291873", nil, "", "", true, ""},
		{"silent vanadis source and nil pool", nil, `{"prompt":"[VANADIS BUILD CHUNK EXECUTION]\nBuild ID: b1","channel_id":"1542423172400291873","message_id":"msg-vanadis-1","silent":true}`, "msg-vanadis-1", "1542423172400291873", "1542423172400291873", nil, "", "", true, ""},
		{"nil pool", nil, `{"prompt":"hello nil pool","channel_id":"1542423172400291873","message_id":"msg-nil-pool"}`, "msg-nil-pool", "1542423172400291873", "1542423172400291873", nil, "", "", false, "hello nil pool"},
		{"auto-generated uuid when message_id omitted", pool, `{"prompt":"auto uuid prompt","channel_id":"1542423172400291873"}`, "", "", "", nil, "", "", false, "auto uuid prompt"},
		{"custom session_id and explicit ephemeral false", pool, `{"prompt":"custom session prompt","channel_id":"1542423172400291873","session_id":"custom-session-123","ephemeral":false,"message_id":"msg-sess-false"}`, "msg-sess-false", "custom-session-123", "1542423172400291873", &falseVal, "", "", false, "custom session prompt"},
		{"custom session_id and explicit ephemeral true with low effort", pool, `{"prompt":"custom session prompt 2","channel_id":"1542423172400291873","session_id":"custom-session-456","ephemeral":true,"effort":"low","message_id":"msg-sess-true"}`, "msg-sess-true", "custom-session-456", "1542423172400291873", &trueVal, "low", "", false, "custom session prompt 2"},
		{"resolve target thread when session_id omitted and not silent", threadPool, `{"prompt":"create thread prompt","channel_id":"1542423172400291873","title":"Heartbeat Alert","message_id":"msg-thread-create"}`, "msg-thread-create", "1542999999999999999", "1542999999999999999", nil, "", "", false, "create thread prompt"},
	} {
		t.Run(tc.name, func(t *testing.T) {
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
			if tc.msgID != "" {
				if tc.wantSilent {
					d, err := store.GetDispatch(context.Background(), tc.msgID)
					if err != nil || d == nil {
						t.Fatalf("failed to retrieve persisted dispatch %s: %v", tc.msgID, err)
					}
					if tc.wantEffort != "" && d.Effort != tc.wantEffort {
						t.Errorf("effort=%q, want %q", d.Effort, tc.wantEffort)
					}
					if tc.wantModel != "" && d.Model != tc.wantModel {
						t.Errorf("model=%q, want %q", d.Model, tc.wantModel)
					}
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
				if tc.wantEphemeral != nil {
					if msg.Ephemeral == nil || *msg.Ephemeral != *tc.wantEphemeral {
						t.Errorf("ephemeral=%v, want %v", msg.Ephemeral, *tc.wantEphemeral)
					}
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
				if tc.wantSilent && !msg.Silent {
					t.Errorf("expected silent=true, got %v", msg.Silent)
				}
				if !msg.IsAdmin {
					t.Errorf("expected is_admin=true for /prompt message, got false")
				}
			}
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

	tests := []struct {
		name       string
		store      db.Store
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
			name:       "store error returns 500",
			store:      errStore,
			body:       `{"prompt":"check","channel_id":"1542423172400291873","skip_if_active":true}`,
			wantStatus: http.StatusInternalServerError,
			wantSubstr: "Failed to check active turn",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := runHTTP(handlePrompt(tc.store, nil), http.MethodPost, "/prompt", strings.NewReader(tc.body))
			if w.Code != tc.wantStatus || !strings.Contains(w.Body.String(), tc.wantSubstr) {
				t.Fatalf("got status=%d body=%q, want status=%d substr=%q", w.Code, w.Body.String(), tc.wantStatus, tc.wantSubstr)
			}
		})
	}
}

func TestHandleCancelPrompt(t *testing.T) {
	t.Parallel()
	seedStore := func(failOp string) *dbtest.FakeStore {
		s := dbtest.NewFakeStore()
		ctx := context.Background()
		for _, m := range []db.Message{
			{ID: "msg-b1-c2", ThreadID: "1542423172400291873", Content: "[VANADIS BUILD CHUNK EXECUTION]\nBuild ID: build-1\nChunk ID: build-1-chunk-2\n", Status: db.StatusPending},
			{ID: "msg-b10-c1", ThreadID: "1542423172400291873", Content: "[VANADIS BUILD CHUNK EXECUTION]\nBuild ID: build-10\nChunk ID: build-10-chunk-1\n", Status: db.StatusPending},
			{ID: "msg-b1-run", ThreadID: "1542423172400291873", Content: "[VANADIS BUILD CHUNK EXECUTION]\nBuild ID: build-1\nChunk ID: build-1-chunk-1\n", Status: db.StatusProcessing},
			{ID: "msg-direct-1", ThreadID: "1542423172400291873", Content: "Direct prompt", Status: db.StatusPending},
		} {
			_ = s.InsertMessage(ctx, m)
		}
		if failOp != "" {
			s.FailNext(failOp, errors.New("simulated "+failOp+" error"))
		}
		return s
	}

	for _, tc := range []struct {
		name, method  string
		store         db.Store
		body          string
		wantStatus    int
		wantSubstr    string
		checkID, wErr string
	}{
		{"method not allowed", http.MethodGet, seedStore(""), "", http.StatusMethodNotAllowed, "Method not allowed", "", ""},
		{"nil store", http.MethodPost, nil, `{"build_id":"build-1"}`, http.StatusInternalServerError, "Database store not configured", "", ""},
		{"invalid json", http.MethodPost, seedStore(""), `{invalid`, http.StatusBadRequest, "Invalid JSON payload", "", ""},
		{"missing ids", http.MethodPost, seedStore(""), `{"reason":"no ids"}`, http.StatusBadRequest, "'message_id' or 'build_id' is required", "", ""},
		{"store query error", http.MethodPost, seedStore("GetPendingOrProcessingMessages"), `{"build_id":"build-1"}`, http.StatusInternalServerError, "Failed to fetch pending messages", "", ""},
		{"claim error skips message", http.MethodPost, seedStore("ClaimPendingMessage"), `{"build_id":"build-1"}`, http.StatusOK, `"cancelled_count":0`, "", ""},
		{"update status error", http.MethodPost, seedStore("UpdateMessageStatus"), `{"build_id":"build-1"}`, http.StatusOK, `"cancelled_count":1`, "", ""},
		{"cancel by build_id", http.MethodPost, seedStore(""), `{"build_id":"build-1"}`, http.StatusOK, `"cancelled_ids":["msg-b1-c2"]`, "msg-b1-c2", "cancelled vanadis build build-1"},
		{"cancel by message_id custom reason", http.MethodPost, seedStore(""), `{"message_id":"msg-direct-1","reason":"superseded"}`, http.StatusOK, `"cancelled_ids":["msg-direct-1"]`, "msg-direct-1", "superseded"},
		{"cancel by message_id default reason", http.MethodPost, seedStore(""), `{"message_id":"msg-direct-1"}`, http.StatusOK, `"cancelled_ids":["msg-direct-1"]`, "msg-direct-1", "cancelled prompt msg-direct-1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			w := runHTTP(handleCancelPrompt(tc.store), tc.method, "/prompt/cancel", strings.NewReader(tc.body))
			if w.Code != tc.wantStatus || !strings.Contains(w.Body.String(), tc.wantSubstr) {
				t.Fatalf("got status=%d body=%q, want status=%d substr=%q", w.Code, w.Body.String(), tc.wantStatus, tc.wantSubstr)
			}
			if tc.checkID != "" && tc.store != nil {
				m, err := tc.store.GetMessage(context.Background(), tc.checkID)
				if err != nil || m == nil || m.Status != db.StatusFailed || !strings.Contains(m.ErrorMessage, tc.wErr) {
					t.Fatalf("message %s got=%+v err=%v, want FAILED with %q", tc.checkID, m, err, tc.wErr)
				}
				if other, _ := tc.store.GetMessage(context.Background(), "msg-b10-c1"); other != nil && other.Status != db.StatusPending {
					t.Fatalf("expected build-10 prompt to remain PENDING, got %q", other.Status)
				}
			}
		})
	}
}

func TestHandleDispatch(t *testing.T) {
	for _, tc := range []struct {
		name       string
		method     string
		store      db.Store
		body       string
		wantStatus int
		wantSubstr string
		checkID    string
	}{
		{"method not allowed", http.MethodGet, dbtest.NewFakeStore(), "", http.StatusMethodNotAllowed, "Method not allowed", ""},
		{"nil store", http.MethodPost, nil, `{"source":"heartbeat","prompt":"test"}`, http.StatusInternalServerError, "Database store not configured", ""},
		{"invalid json", http.MethodPost, dbtest.NewFakeStore(), `{bad`, http.StatusBadRequest, "Invalid JSON payload", ""},
		{"missing source", http.MethodPost, dbtest.NewFakeStore(), `{"prompt":"test"}`, http.StatusBadRequest, "'source' must be 'heartbeat', 'vanadis', or 'webhook'", ""},
		{"invalid source", http.MethodPost, dbtest.NewFakeStore(), `{"source":"unknown","prompt":"test"}`, http.StatusBadRequest, "'source' must be 'heartbeat', 'vanadis', or 'webhook'", ""},
		{"empty prompt", http.MethodPost, dbtest.NewFakeStore(), `{"source":"heartbeat","prompt":" "}`, http.StatusBadRequest, "'prompt' field is required", ""},
		{"numeric session_id rejected", http.MethodPost, dbtest.NewFakeStore(), `{"source":"heartbeat","prompt":"test","session_id":"1234567890"}`, http.StatusBadRequest, "cannot be a numeric snowflake", ""},
		{"discord prefix session_id rejected", http.MethodPost, dbtest.NewFakeStore(), `{"source":"heartbeat","prompt":"test","session_id":"discord-123"}`, http.StatusBadRequest, "cannot be a numeric snowflake", ""},
		{"invalid chars session_id rejected", http.MethodPost, dbtest.NewFakeStore(), `{"source":"heartbeat","prompt":"test","session_id":"bad:session"}`, http.StatusBadRequest, "cannot be a numeric snowflake", ""},
		{"invalid effort rejected", http.MethodPost, dbtest.NewFakeStore(), `{"source":"heartbeat","prompt":"test","effort":"ultra"}`, http.StatusBadRequest, "'effort' must be 'low', 'medium', or 'high'", ""},
		{"valid stateful accepted", http.MethodPost, dbtest.NewFakeStore(), `{"id":"disp-1","source":"heartbeat","session_id":"heartbeat-1","prompt":"run check","effort":"low"}`, http.StatusAccepted, `"status":"accepted"`, "disp-1"},
		{"valid ephemeral accepted", http.MethodPost, dbtest.NewFakeStore(), `{"id":"disp-2","source":"vanadis","prompt":"execute build"}`, http.StatusAccepted, `"status":"accepted"`, "disp-2"},
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
			}
		})
	}
}

func TestHandleDispatch_SkipIfActive(t *testing.T) {
	s := dbtest.NewFakeStore()
	_ = s.InsertDispatch(context.Background(), db.Dispatch{
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
	seedStore := func() *dbtest.FakeStore {
		s := dbtest.NewFakeStore()
		ctx := context.Background()
		for _, d := range []db.Dispatch{
			{ID: "disp-b1-c1", Source: "vanadis", Prompt: "[VANADIS BUILD CHUNK EXECUTION]\nBuild ID: build-99\n", Status: db.StatusPending},
			{ID: "disp-b1-c2", Source: "vanadis", Prompt: "[VANADIS BUILD CHUNK EXECUTION]\nBuild ID: build-99\n", Status: db.StatusPending},
			{ID: "disp-active", Source: "vanadis", Prompt: "[VANADIS BUILD CHUNK EXECUTION]\nBuild ID: build-99\n", Status: db.StatusProcessing},
			{ID: "disp-single", Source: "heartbeat", Prompt: "hourly pulse", Status: db.StatusPending},
		} {
			_ = s.InsertDispatch(ctx, d)
		}
		return s
	}

	for _, tc := range []struct {
		name       string
		method     string
		store      db.Store
		body       string
		wantStatus int
		wantSubstr string
	}{
		{"method not allowed", http.MethodGet, seedStore(), "", http.StatusMethodNotAllowed, "Method not allowed"},
		{"nil store", http.MethodPost, nil, `{"build_id":"build-99"}`, http.StatusInternalServerError, "Database store not configured"},
		{"invalid json", http.MethodPost, seedStore(), `{invalid`, http.StatusBadRequest, "Invalid JSON payload"},
		{"missing ids", http.MethodPost, seedStore(), `{"reason":"no ids"}`, http.StatusBadRequest, "'dispatch_id' (or 'message_id') or 'build_id' is required"},
		{"cancel by build_id", http.MethodPost, seedStore(), `{"build_id":"build-99"}`, http.StatusOK, `"cancelled_count":2`},
		{"cancel by dispatch_id", http.MethodPost, seedStore(), `{"dispatch_id":"disp-single"}`, http.StatusOK, `"cancelled_ids":["disp-single"]`},
		{"cancel by message_id alias", http.MethodPost, seedStore(), `{"message_id":"disp-single"}`, http.StatusOK, `"cancelled_ids":["disp-single"]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := runHTTP(handleCancelDispatch(tc.store), tc.method, "/dispatch/cancel", strings.NewReader(tc.body))
			if w.Code != tc.wantStatus || !strings.Contains(w.Body.String(), tc.wantSubstr) {
				t.Fatalf("got status=%d body=%q, want status=%d substr=%q", w.Code, w.Body.String(), tc.wantStatus, tc.wantSubstr)
			}
		})
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
			return &discordgo.RESTError{Response: &http.Response{StatusCode: 403}}
		},
	})
	sendErrPool := queue.New(nil, queue.WorkerPoolConfig{
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			return errors.New("cannot send messages to this thread")
		},
	})

	oversizedB64 := base64.StdEncoding.EncodeToString(make([]byte, 8*1024*1024+1))

	for _, tc := range []struct {
		name       string
		method     string
		pool       *queue.WorkerPool
		body       io.Reader
		wantStatus int
		wantSubstr string
	}{
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
			w := runHTTP(handleDirectMessage(tc.pool), tc.method, "/discord/message", tc.body)
			if w.Code != tc.wantStatus || !strings.Contains(w.Body.String(), tc.wantSubstr) {
				t.Fatalf("got status=%d body=%q, want status=%d substr=%q", w.Code, w.Body.String(), tc.wantStatus, tc.wantSubstr)
			}
		})
	}

	t.Run("delivery success and fallback", func(t *testing.T) {
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
		var resp map[string]interface{}
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
	})

	t.Run("mux route mounting for /discord/message", func(t *testing.T) {
		pool := queue.New(nil, queue.WorkerPoolConfig{
			DeliveryFunc: func(s *discordgo.Session, channelID, text string) error { return nil },
		})
		mux := SetupBrainMuxWithEmbedder(dbtest.NewFakeStore(), pool, nil)
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
	})
}

func TestHandleStatusWidget(t *testing.T) {
	restErrPool := queue.New(nil, queue.WorkerPoolConfig{
		WidgetDeliveryFunc: func(req discord.StatusWidgetRequest) (string, error) {
			return "", &discordgo.RESTError{Response: &http.Response{StatusCode: 403}}
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

	for _, tc := range []struct {
		name       string
		method     string
		pool       *queue.WorkerPool
		body       string
		wantStatus int
		wantSubstr string
	}{
		{"method not allowed", http.MethodGet, &queue.WorkerPool{}, "", http.StatusMethodNotAllowed, "Method not allowed"},
		{"nil pool", http.MethodPost, nil, `{"channel_id":"1542423172400291873","chunks":[{"title":"task"}]}`, http.StatusInternalServerError, "Worker pool not available"},
		{"invalid JSON", http.MethodPost, &queue.WorkerPool{}, "{invalid-json", http.StatusBadRequest, "Invalid JSON payload"},
		{"missing channel_id", http.MethodPost, &queue.WorkerPool{}, `{"chunks":[{"title":"task"}]}`, http.StatusBadRequest, "Invalid payload: 'channel_id' field is required and cannot be empty"},
		{"non-snowflake channel_id", http.MethodPost, &queue.WorkerPool{}, `{"channel_id":"invalid-snow","chunks":[{"title":"task"}]}`, http.StatusBadRequest, "Invalid payload: 'channel_id' must be a valid numeric Discord snowflake"},
		{"empty chunks", http.MethodPost, &queue.WorkerPool{}, `{"channel_id":"1542423172400291873","chunks":[]}`, http.StatusBadRequest, "Invalid payload: 'chunks' field is required and cannot be empty"},
		{"discord session not connected", http.MethodPost, &queue.WorkerPool{}, `{"channel_id":"1542423172400291873","chunks":[{"title":"task"}]}`, http.StatusServiceUnavailable, "Discord session not connected"},
		{"discord client RESTError", http.MethodPost, restErrPool, `{"channel_id":"1542423172400291873","chunks":[{"title":"task"}]}`, http.StatusForbidden, "Discord client error"},
		{"discord client cannot send", http.MethodPost, sendErrPool, `{"channel_id":"1542423172400291873","chunks":[{"title":"task"}]}`, http.StatusBadRequest, "Discord client error"},
		{"generic delivery failure", http.MethodPost, failErrPool, `{"channel_id":"1542423172400291873","chunks":[{"title":"task"}]}`, http.StatusInternalServerError, "Failed to deliver widget"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var r io.Reader
			if tc.body != "" {
				r = strings.NewReader(tc.body)
			}
			w := runHTTP(handleStatusWidget(tc.pool), tc.method, "/discord/widget", r)
			if w.Code != tc.wantStatus || !strings.Contains(w.Body.String(), tc.wantSubstr) {
				t.Fatalf("got status=%d body=%q, want status=%d substr=%q", w.Code, w.Body.String(), tc.wantStatus, tc.wantSubstr)
			}
		})
	}

	t.Run("success returns 200 with message_id and delivers widget", func(t *testing.T) {
		var capturedReq discord.StatusWidgetRequest
		handler := handleStatusWidget(queue.New(nil, queue.WorkerPoolConfig{
			WidgetDeliveryFunc: func(req discord.StatusWidgetRequest) (string, error) {
				capturedReq = req
				return "created-widget-msg-789", nil
			},
		}))

		payload := `{"channel_id":"1542423172400291873","id":"build-123","title":"Build Title","status":"running","chunks":[{"index":1,"repo":"aerial","title":"Chunk 1","status":"completed"}]}`
		w := runHTTP(handler, http.MethodPost, "/discord/widget", strings.NewReader(payload))
		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d (body: %s)", w.Code, w.Body.String())
		}
		if capturedReq.ID != "build-123" || capturedReq.ChannelID != "1542423172400291873" || len(capturedReq.Chunks) != 1 {
			t.Fatalf("unexpected captured request: %+v", capturedReq)
		}
		var resp map[string]interface{}
		_ = json.Unmarshal(w.Body.Bytes(), &resp)
		if resp["status"] != "ok" || resp["message_id"] != "created-widget-msg-789" {
			t.Fatalf("unexpected response body: %+v", resp)
		}
	})

	t.Run("mux route mounting for /discord/widget", func(t *testing.T) {
		pool := queue.New(nil, queue.WorkerPoolConfig{
			WidgetDeliveryFunc: func(req discord.StatusWidgetRequest) (string, error) { return "widget-99", nil },
		})
		mux := SetupBrainMuxWithEmbedder(dbtest.NewFakeStore(), pool, nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/discord/widget", strings.NewReader(`{"channel_id":"1542423172400291873","chunks":[{"title":"t"}]}`)))
		if w.Code != http.StatusOK {
			t.Errorf("expected 200 for /discord/widget, got %d", w.Code)
		}
		wInternal := httptest.NewRecorder()
		mux.ServeHTTP(wInternal, httptest.NewRequest(http.MethodPost, "/internal/widget", strings.NewReader(`{"channel_id":"1542423172400291873","chunks":[{"title":"t"}]}`)))
		if wInternal.Code != http.StatusNotFound {
			t.Errorf("expected 404 for dropped /internal/widget, got %d", wInternal.Code)
		}
	})
}

func TestHandleTranscriptSearchAndStats(t *testing.T) {
	fake := dbtest.NewFakeStore()
	_ = fake.UpsertSessionSummary(context.Background(), db.SessionSummary{
		SessionID: "sess-mux-1",
		Summary:   "Triaged container restart in aerial",
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

	t.Run("search success and embedder", func(t *testing.T) {
		w := runHTTP(handleTranscriptSearch(fake, embedder), http.MethodGet, "/api/transcripts/search?q=container&mode=auto&limit=5&tool=run_command&session=sess-mux-1", nil)
		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d: %s", w.Code, w.Body.String())
		}
		if !embedderCalled {
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

	t.Run("stats success", func(t *testing.T) {
		w := runHTTP(handleTranscriptStats(fake), http.MethodGet, "/api/transcripts/stats", nil)
		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d: %s", w.Code, w.Body.String())
		}
		var stats map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &stats); err != nil || stats["total_sessions"] != float64(1) || stats["status"] != "ok" {
			t.Fatalf("unexpected stats: %+v, err: %v", stats, err)
		}
	})

	t.Run("mux endpoints", func(t *testing.T) {
		mux := SetupBrainMuxWithEmbedder(fake, nil, nil)
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

func (e *errTranscriptStore) GetSessionSyncStates(ctx context.Context) (map[string]db.SessionSyncState, error) {
	return nil, errors.New("simulated sync states db error")
}

func (e *errTranscriptStore) SearchTranscriptSteps(ctx context.Context, query, sessionFilter, toolFilter string, limit int, since ...time.Time) ([]db.TranscriptStep, error) {
	return nil, errors.New("simulated search transcript steps error")
}

func (e *errTranscriptStore) SearchSessionSummaries(ctx context.Context, queryEmbedding []float32, textQuery string, limit int, threshold float64) ([]db.SessionSummary, error) {
	return nil, errors.New("simulated search session summaries error")
}

func (e *errTranscriptStore) GetTranscriptStepRange(ctx context.Context, sessionID string, startStep, endStep int) ([]db.TranscriptStep, error) {
	return nil, errors.New("simulated get transcript step range error")
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

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			handler(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/tasks", nil))
		}()
	}
	wg.Wait()

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

	ephemMsg := db.Message{
		ID: "msg-vanadis-chunk", ThreadID: "1564704264645902626", AuthorID: "http-client",
		AuthorName: "HTTP Client", Content: "[VANADIS BUILD CHUNK EXECUTION]", Effort: "medium",
		Silent: true, Status: db.StatusPending, CreatedAt: now, UpdatedAt: now,
	}
	_ = ephemStore.InsertMessage(context.Background(), ephemMsg)
	ephemPool.Enqueue(ephemMsg)
	<-turnStarted

	liveRR := runHTTP(handleTasks(ephemStore, ephemPool), http.MethodGet, "/tasks?status=PROCESSING", nil)
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
		{"/prompt/cancel", "/prompt/cancel"},
		{"/transcripts/abc-123", "/transcripts"},
		{"/api/transcripts/steps", "/transcripts"},
		{"/api/transcripts/search", "/transcripts"},
		{"/tasks", "/tasks"},
		{"/tasks/456", "/tasks"},
		{"/internal/llm", "/internal/llm"},
		{"/schedules", "unmatched"},
		{"/schedules/runs", "unmatched"},
		{"/transcripts", "unmatched"},
		{"/health", "/health"},
		{"/metrics", "/metrics"},
		{"/voice/ask", "/voice/ask"},
		{"/api/voice/ask", "/voice/ask"},
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

func TestHandleInternalLLM_And_MemoryRetriever(t *testing.T) {
	okLLM := handleInternalLLM(func(ctx context.Context, model, p string) (string, error) {
		return `[{"category":"system","fact":"ok"}]`, nil
	})
	errLLM := handleInternalLLM(func(ctx context.Context, model, p string) (string, error) {
		return "", errors.New("llm failed")
	})
	nilLLM := handleInternalLLM(nil)

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
	} {
		t.Run(tc.name, func(t *testing.T) {
			if code := runHTTP(tc.handler, tc.method, "/internal/llm", strings.NewReader(tc.body)).Code; code != tc.wantStatus {
				t.Fatalf("got status=%d, want %d", code, tc.wantStatus)
			}
		})
	}

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

	os.Setenv("MEMORY_MCP_URL", memSrv.URL)
	retEnv := newHTTPMemoryRetriever("")
	os.Unsetenv("MEMORY_MCP_URL")
	if out, err := retEnv(context.Background(), "hello"); err != nil || !strings.Contains(out, "<retrieved_memory>") {
		t.Errorf("expected retrieved memory with env var fallback, got %q, %v", out, err)
	}
	retDef := newHTTPMemoryRetriever("")
	if retDef == nil {
		t.Errorf("expected non-nil retriever with default URL")
	}
}

func TestSetupBrainMux_And_Endpoints(t *testing.T) {
	store := dbtest.NewFakeStore()
	pool := newTestWorkerPool(store)
	tmpHome := t.TempDir()
	mux := SetupBrainMuxWithEmbedder(store, pool, nil, tmpHome)

	if w := runHTTP(mux.ServeHTTP, http.MethodGet, "/health", nil); w.Code != http.StatusOK {
		t.Errorf("Expected 200 from /health, got %d", w.Code)
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

type errReadCloser struct {
	reader   io.Reader
	readErr  error
	closeErr error
}

func (e *errReadCloser) Read(p []byte) (int, error) {
	if e.readErr != nil {
		return 0, e.readErr
	}
	if e.reader != nil {
		return e.reader.Read(p)
	}
	return 0, errors.New("simulated network read error")
}

func (e *errReadCloser) Close() error {
	if e.closeErr != nil {
		return e.closeErr
	}
	return nil
}

type mockHTTPResponseWriter struct {
	header http.Header
	code   int
	body   bytes.Buffer
}

func (w *mockHTTPResponseWriter) Header() http.Header {
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}

func (w *mockHTTPResponseWriter) Write(b []byte) (int, error) {
	return w.body.Write(b)
}

func (w *mockHTTPResponseWriter) WriteHeader(statusCode int) {
	w.code = statusCode
}

type flusherErrWriter struct {
	header http.Header
	code   int
}

func (e *flusherErrWriter) Header() http.Header {
	if e.header == nil {
		e.header = make(http.Header)
	}
	return e.header
}

func (e *flusherErrWriter) Write(p []byte) (int, error) {
	return 0, errors.New("simulated write error")
}

func (e *flusherErrWriter) WriteHeader(statusCode int) {
	e.code = statusCode
}

func (e *flusherErrWriter) Flush() {}

func TestHandleVoiceAsk_Validation(t *testing.T) {
	store := dbtest.NewFakeStore()
	pool := newTestWorkerPool(store)
	pool.Start()
	defer pool.Stop()

	handler := handleVoiceAsk(pool)

	tests := []struct {
		name       string
		method     string
		body       io.Reader
		accept     string
		wantStatus int
		wantSubstr string
	}{
		{"GET not allowed", http.MethodGet, nil, "", http.StatusMethodNotAllowed, "Method not allowed"},
		{"empty prompt", http.MethodPost, strings.NewReader(`{"prompt":""}`), "", http.StatusBadRequest, "'prompt' field is required"},
		{"malformed JSON", http.MethodPost, strings.NewReader(`{invalid-json`), "", http.StatusBadRequest, "Invalid JSON payload"},
		{"body read error", http.MethodPost, &errReadCloser{readErr: errors.New("simulated body read error")}, "", http.StatusBadRequest, "Failed to read request body"},
		{"conversation_id fallback", http.MethodPost, strings.NewReader(`{"conversation_id":"conv-123","prompt":"hello"}`), "", http.StatusOK, ""},
		{"missing session_id", http.MethodPost, strings.NewReader(`{"prompt":"hello"}`), "", http.StatusBadRequest, "session_id"},
		{"node_id provided", http.MethodPost, strings.NewReader(`{"prompt":"hello","node_id":"touch-kiosk-kitchen"}`), "", http.StatusOK, ""},
		{"node_id priority", http.MethodPost, strings.NewReader(`{"prompt":"hello","node_id":"touch-kiosk-kitchen","session_id":"turn-123","conversation_id":"conv-456"}`), "", http.StatusOK, ""},
		{"oversized device ID", http.MethodPost, strings.NewReader(fmt.Sprintf(`{"prompt":"hello","node_id":%q}`, strings.Repeat("a", 129))), "", http.StatusBadRequest, "exceeds 128 characters"},
		{"control characters in device ID", http.MethodPost, strings.NewReader(`{"prompt":"hello","node_id":"dock\ninvalid"}`), "", http.StatusBadRequest, "contains invalid characters"},
		{"body close error handled", http.MethodPost, &errReadCloser{reader: strings.NewReader(`{"prompt":"hello","session_id":"kiosk"}`), closeErr: errors.New("simulated close error")}, "", http.StatusOK, ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := runHTTP(handler, tc.method, "/voice/ask", tc.body, "Accept", tc.accept)
			if w.Code != tc.wantStatus {
				t.Fatalf("got status %d, want %d (body: %s)", w.Code, tc.wantStatus, w.Body.String())
			}
			if tc.wantSubstr != "" && !strings.Contains(w.Body.String(), tc.wantSubstr) {
				t.Errorf("expected substring %q in body, got: %s", tc.wantSubstr, w.Body.String())
			}
			if tc.method != http.MethodPost && w.Header().Get("Allow") != "POST" {
				t.Errorf("expected Allow: POST header, got %q", w.Header().Get("Allow"))
			}
		})
	}

	t.Run("non-flusher response writer for SSE", func(t *testing.T) {
		reqSSE := httptest.NewRequest(http.MethodPost, "/voice/ask", strings.NewReader(`{"prompt":"hello","session_id":"kiosk"}`))
		reqSSE.Header.Set("Accept", "text/event-stream")
		nonFlusher := &mockHTTPResponseWriter{}
		handler(nonFlusher, reqSSE)
		if nonFlusher.code != http.StatusInternalServerError {
			t.Errorf("expected 500 for non-flusher SSE, got %d", nonFlusher.code)
		}
	})
}

func newVoiceHandler(cfg queue.WorkerPoolConfig) (http.HandlerFunc, func()) {
	if cfg.Store == nil {
		cfg.Store = dbtest.NewFakeStore()
	}
	pool := queue.New(nil, cfg)
	pool.Start()
	return handleVoiceAsk(pool), pool.Stop
}

func TestHandleVoiceAsk_SSE(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		handler, stop := newVoiceHandler(queue.WorkerPoolConfig{
			VoiceRunnerFunc: func(ctx context.Context, prompt, sessionID string, onStatus func(string)) (string, string, error) {
				if onStatus != nil {
					onStatus("⚡ Checking smart lights...")
				}
				return "Lights have been turned off.", "sess-test-456", nil
			},
		})
		defer stop()

		w := runHTTP(handler, http.MethodPost, "/voice/ask", strings.NewReader(`{"prompt":"turn off the lights","session_id":"sess-test-456"}`), "Accept", "text/event-stream")
		if w.Code != http.StatusOK || !strings.Contains(w.Header().Get("Content-Type"), "text/event-stream") || w.Header().Get("X-Accel-Buffering") != "no" {
			t.Fatalf("unexpected headers or status: code=%d, ct=%s", w.Code, w.Header().Get("Content-Type"))
		}
		body := w.Body.String()
		for _, want := range []string{
			"event: status\ndata: {\"status\":\"⚡ Checking smart lights...\"}\n\n",
			"event: reply\ndata: {\"conversation_id\":\"sess-test-456\",\"reply\":\"Lights have been turned off.\"}\n\n",
			"event: done\ndata: {\"conversation_id\":\"sess-test-456\"}\n\n",
		} {
			if !strings.Contains(body, want) {
				t.Errorf("missing SSE event %q in body:\n%s", want, body)
			}
		}
	})

	t.Run("SentenceStreaming", func(t *testing.T) {
		handler, stop := newVoiceHandler(queue.WorkerPoolConfig{
			VoiceStreamRunnerFunc: func(ctx context.Context, prompt, sessionID string, onStatus func(string), onSentence func(string)) (string, string, error) {
				if onStatus != nil {
					onStatus("⚡ Checking status...")
				}
				if onSentence != nil {
					onSentence("Here is sentence one.")
					onSentence("And here is sentence two.")
				}
				return "Here is sentence one. And here is sentence two.", "sess-stream-789", nil
			},
		})
		defer stop()

		w := runHTTP(handler, http.MethodPost, "/voice/ask", strings.NewReader(`{"prompt":"tell me two things","session_id":"sess-stream-789"}`), "Accept", "text/event-stream")
		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", w.Code)
		}
		body := w.Body.String()
		for _, want := range []string{
			"event: status\ndata: {\"status\":\"⚡ Checking status...\"}\n\n",
			"event: sentence\ndata: {\"text\":\"Here is sentence one.\"}\n\n",
			"event: sentence\ndata: {\"text\":\"And here is sentence two.\"}\n\n",
			"event: reply\ndata: {\"conversation_id\":\"sess-stream-789\",\"reply\":\"Here is sentence one. And here is sentence two.\"}\n\n",
			"event: done\ndata: {\"conversation_id\":\"sess-stream-789\"}\n\n",
		} {
			if !strings.Contains(body, want) {
				t.Errorf("missing SSE event %q in body:\n%s", want, body)
			}
		}
		idxSent1 := strings.Index(body, "event: sentence\ndata: {\"text\":\"Here is sentence one.\"}")
		idxSent2 := strings.Index(body, "event: sentence\ndata: {\"text\":\"And here is sentence two.\"}")
		idxReply := strings.Index(body, "event: reply\ndata: {\"conversation_id\":\"sess-stream-789\"")
		if idxSent1 == -1 || idxSent2 == -1 || idxReply == -1 || idxSent1 > idxSent2 || idxSent2 > idxReply {
			t.Errorf("invalid event ordering: idxSent1=%d, idxSent2=%d, idxReply=%d", idxSent1, idxSent2, idxReply)
		}
	})

	t.Run("CancelledContext", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		handler, stop := newVoiceHandler(queue.WorkerPoolConfig{
			VoiceRunnerFunc: func(c context.Context, prompt, sessionID string, onStatus func(string)) (string, string, error) {
				cancel()
				if onStatus != nil {
					onStatus("status after cancel")
				}
				return "done", "sess-1", nil
			},
		})
		defer stop()

		req := httptest.NewRequest(http.MethodPost, "/voice/ask", strings.NewReader(`{"prompt":"hello","session_id":"kiosk"}`)).WithContext(ctx)
		req.Header.Set("Accept", "text/event-stream")
		handler(httptest.NewRecorder(), req)
	})

	t.Run("VoiceRunnerError", func(t *testing.T) {
		handler, stop := newVoiceHandler(queue.WorkerPoolConfig{
			VoiceRunnerFunc: func(c context.Context, prompt, sessionID string, onStatus func(string)) (string, string, error) {
				return "", "", errors.New("synthetic voice failure")
			},
		})
		defer stop()

		w := runHTTP(handler, http.MethodPost, "/voice/ask", strings.NewReader(`{"prompt":"hello","session_id":"kiosk"}`), "Accept", "text/event-stream")
		if !strings.Contains(w.Body.String(), "event: error") {
			t.Errorf("expected SSE error event, got %q", w.Body.String())
		}
	})

	t.Run("WriteError", func(t *testing.T) {
		handler, stop := newVoiceHandler(queue.WorkerPoolConfig{
			VoiceRunnerFunc: func(c context.Context, prompt, sessionID string, onStatus func(string)) (string, string, error) {
				return "done", "sess-1", nil
			},
		})
		defer stop()

		req := httptest.NewRequest(http.MethodPost, "/voice/ask", strings.NewReader(`{"prompt":"hello","session_id":"kiosk"}`))
		req.Header.Set("Accept", "text/event-stream")
		handler(&flusherErrWriter{}, req)
	})

	t.Run("JSON_Success", func(t *testing.T) {
		handler, stop := newVoiceHandler(queue.WorkerPoolConfig{
			VoiceRunnerFunc: func(ctx context.Context, prompt, sessionID string, onStatus func(string)) (string, string, error) {
				return "The temperature is 72 degrees.", "sess-json-789", nil
			},
		})
		defer stop()

		w := runHTTP(handler, http.MethodPost, "/voice/ask", strings.NewReader(`{"prompt":"what is the temperature?","session_id":"sess-json-789"}`), "Accept", "application/json")
		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", w.Code)
		}
		var resp VoiceAskResponse
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil || resp.Reply != "The temperature is 72 degrees." || resp.SessionID != "sess-json-789" {
			t.Fatalf("unexpected JSON response: %+v, err: %v", resp, err)
		}
	})

	t.Run("Errors", func(t *testing.T) {
		hErr, stop := newVoiceHandler(queue.WorkerPoolConfig{
			VoiceRunnerFunc: func(ctx context.Context, prompt, sessionID string, onStatus func(string)) (string, string, error) {
				return "", "", errors.New("deliberation timed out")
			},
		})
		defer stop()

		wJSON := runHTTP(hErr, http.MethodPost, "/voice/ask", strings.NewReader(`{"prompt":"fail","session_id":"sess-err-2"}`), "Accept", "application/json")
		if wJSON.Code != http.StatusInternalServerError || !strings.Contains(wJSON.Body.String(), "deliberation timed out") {
			t.Errorf("expected 500 error, got %d: %s", wJSON.Code, wJSON.Body.String())
		}

		hNil := handleVoiceAsk(nil)
		if w := runHTTP(hNil, http.MethodPost, "/voice/ask", strings.NewReader(`{"prompt":"hello","session_id":"sess-nil"}`)); w.Code != http.StatusInternalServerError {
			t.Errorf("expected 500 for nil pool, got %d", w.Code)
		}
		if w := runHTTP(hNil, http.MethodPost, "/voice/ask", strings.NewReader(`{"prompt":"hello","session_id":"sess-nil-sse"}`), "Accept", "text/event-stream"); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "event: error") {
			t.Errorf("expected 200 SSE error for nil pool, got %d: %s", w.Code, w.Body.String())
		}

		mRec := httptest.NewRecorder()
		metrics.Handler().ServeHTTP(mRec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
		for _, metric := range []string{
			`aerial_brain_voice_ttfr_duration_seconds_bucket{mode="sse",status="error"`,
			`aerial_brain_voice_ttfr_duration_seconds_bucket{mode="json",status="error"`,
		} {
			if !strings.Contains(mRec.Body.String(), metric) {
				t.Errorf("missing metric %q", metric)
			}
		}
	})
}

func TestHandlePrompt_SilentSkipIfActive(t *testing.T) {
	s := dbtest.NewFakeStore()
	_ = s.InsertDispatch(context.Background(), db.Dispatch{
		ID:        "active-disp-prompt",
		Source:    "heartbeat",
		SessionID: "heartbeat-prompt-test",
		Prompt:    "ongoing run",
		Status:    db.StatusProcessing,
	})

	for _, tc := range []struct {
		name       string
		body       string
		wantStatus int
		wantSubstr string
	}{
		{"skip active silent prompt", `{"prompt":"test","channel_id":"1542423172400291873","session_id":"heartbeat-prompt-test","silent":true,"skip_if_active":true}`, http.StatusOK, `"status":"skipped_active"`},
		{"do not skip when skip_if_active false", `{"prompt":"test","channel_id":"1542423172400291873","session_id":"heartbeat-prompt-test","silent":true,"skip_if_active":false}`, http.StatusAccepted, `"status":"accepted"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := runHTTP(handlePrompt(s, nil), http.MethodPost, "/prompt", strings.NewReader(tc.body))
			if w.Code != tc.wantStatus || !strings.Contains(w.Body.String(), tc.wantSubstr) {
				t.Fatalf("got status=%d body=%q, want status=%d substr=%q", w.Code, w.Body.String(), tc.wantStatus, tc.wantSubstr)
			}
		})
	}
}
