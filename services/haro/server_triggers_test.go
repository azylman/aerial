package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestDirectDB_SummaryAndRunsErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	closedDB, err := OpenDB(&Config{DatabaseURL: ":memory:"})
	if err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}
	_ = closedDB.Close()

	tests := []struct {
		name    string
		ctx     context.Context
		db      *sql.DB
		wantErr bool
	}{
		{"canceled ctx", ctx, closedDB, true},
		{"nil db", context.Background(), nil, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := GetScheduleSummaryMetrics(tc.ctx, tc.db); (err != nil) != tc.wantErr {
				t.Errorf("GetScheduleSummaryMetrics() error = %v, wantErr %v", err, tc.wantErr)
			}
			if _, _, err := GetScheduleRunsPaginated(tc.ctx, tc.db, 50, 0, "", ""); (err != nil) != tc.wantErr {
				t.Errorf("GetScheduleRunsPaginated() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestServer_ScheduleRoutesDispatchViaMCP(t *testing.T) {
	server, ts := setupTestServer(t)
	defer ts.Close()

	tests := []struct {
		name       string
		method     string
		path       string
		body       string
		wantStatus int
	}{
		{"mcp dispatch schedules", http.MethodGet, "/schedules", "", http.StatusOK},
		{"mcp dispatch metrics", http.MethodGet, "/metrics", "", http.StatusOK},
		{"mcp dispatch schedules runs", http.MethodGet, "/schedules/runs", "", http.StatusOK},
		{"mcp dispatch schedules oneshot invalid", http.MethodPost, "/schedules/oneshot", `{}`, http.StatusBadRequest},
		{"mcp dispatch event triggers check invalid", http.MethodPost, "/event-triggers/check", `{}`, http.StatusBadRequest},
		{"mcp dispatch event triggers claim invalid", http.MethodPost, "/event-triggers/claim", `{}`, http.StatusBadRequest},
		{"mcp dispatch event triggers fire invalid", http.MethodPost, "/event-triggers/fire", `{}`, http.StatusBadRequest},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			server.handleMCP(rec, req)
			if rec.Code != tc.wantStatus {
				t.Fatalf("handleMCP path %s got status %d, want %d", tc.path, rec.Code, tc.wantStatus)
			}
		})
	}
}

func decodeJSON[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var resp T
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	return resp
}

func runEventTriggerEndpoint(t *testing.T, handler http.HandlerFunc, name, method, path, body string, wantStatus int) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	handler(rec, req)
	if rec.Code != wantStatus {
		t.Fatalf("%s got status %d, want %d (%s)", name, rec.Code, wantStatus, rec.Body.String())
	}
	return rec
}

func TestHandleClaimEventTriggers_Table(t *testing.T) {
	server, ts := setupTestServer(t)
	defer ts.Close()

	_ = InsertEventTrigger(context.Background(), server.handler.db, EventTrigger{
		ID:        "trig-claim-1",
		ChannelID: "1555405874565091380",
		Source:    "github",
		Target:    "azylman/aerial",
		EventType: "ci_passed",
		Prompt:    "check passed",
		Effort:    "medium",
	})
	_ = InsertEventTrigger(context.Background(), server.handler.db, EventTrigger{
		ID:         "trig-claim-persistent-1",
		ChannelID:  "1555405874565091380",
		Source:     "camera",
		Target:     "garage",
		EventType:  "person_arrived",
		Prompt:     "check garage person",
		Effort:     "medium",
		Persistent: true,
	})

	closedDB, err := OpenDB(&Config{DatabaseURL: ":memory:"})
	if err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}
	_ = closedDB.Close()
	serverClosed := NewServer(NewToolHandler(&Config{DatabaseURL: ":memory:"}, closedDB))

	tests := []struct {
		name       string
		method     string
		body       string
		useServer  *Server
		wantStatus int
		wantCount  int
	}{
		{"method not allowed", http.MethodGet, "", server, http.StatusMethodNotAllowed, 0},
		{"nil server db", http.MethodPost, `{"source":"github","targets":["azylman/aerial"],"event_type":"ci_passed"}`, &Server{}, http.StatusInternalServerError, 0},
		{"malformed json", http.MethodPost, `{bad`, server, http.StatusBadRequest, 0},
		{"missing fields", http.MethodPost, `{"source":"github"}`, server, http.StatusBadRequest, 0},
		{"closed db error", http.MethodPost, `{"source":"github","targets":["azylman/aerial"],"event_type":"ci_passed"}`, serverClosed, http.StatusInternalServerError, 0},
		{"claim matching trigger", http.MethodPost, `{"source":"github","targets":["azylman/aerial","azylman/aerial:main"],"event_type":"ci_passed"}`, server, http.StatusOK, 1},
		{"second claim returns empty", http.MethodPost, `{"source":"github","targets":["azylman/aerial"],"event_type":"ci_passed"}`, server, http.StatusOK, 0},
		{"claim matching persistent trigger", http.MethodPost, `{"source":"camera","targets":["garage"],"event_type":"person_arrived"}`, server, http.StatusOK, 1},
		{"second claim on persistent trigger debounces duplicate within 5s", http.MethodPost, `{"source":"camera","targets":["garage"],"event_type":"person_arrived"}`, server, http.StatusOK, 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := runEventTriggerEndpoint(t, tc.useServer.handleClaimEventTriggers, tc.name, tc.method, "/event-triggers/claim", tc.body, tc.wantStatus)
			if tc.wantStatus == http.StatusOK {
				type claimResp struct{ Triggers []EventTrigger }
				if got := len(decodeJSON[claimResp](t, rec).Triggers); got != tc.wantCount {
					t.Fatalf("got %d triggers, want %d", got, tc.wantCount)
				}
			}
		})
	}
}

func TestHandleCheckEventTriggers_Table(t *testing.T) {
	server, ts := setupTestServer(t)
	defer ts.Close()

	_ = InsertEventTrigger(context.Background(), server.handler.db, EventTrigger{
		ID:        "trig-check-1",
		ChannelID: "1555405874565091380",
		Source:    "custom_source",
		Target:    "device.alpha",
		EventType: "ready",
		Prompt:    "Device alpha ready",
		Effort:    "low",
	})

	closedDB, err := OpenDB(&Config{DatabaseURL: ":memory:"})
	if err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}
	_ = closedDB.Close()
	serverClosed := NewServer(NewToolHandler(&Config{DatabaseURL: ":memory:"}, closedDB))

	tests := []struct {
		name        string
		method      string
		body        string
		useServer   *Server
		wantStatus  int
		wantMatched bool
	}{
		{"check method not allowed", http.MethodGet, "", server, http.StatusMethodNotAllowed, false},
		{"check closed db error", http.MethodPost, `{"source":"custom_source","target":"device.alpha","event_type":"ready"}`, serverClosed, http.StatusInternalServerError, false},
		{"check unmatched target", http.MethodPost, `{"source":"custom_source","target":"device.beta","event_type":"ready"}`, server, http.StatusOK, false},
		{"check matched target", http.MethodPost, `{"source":"custom_source","target":"device.alpha","event_type":"ready"}`, server, http.StatusOK, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := runEventTriggerEndpoint(t, tc.useServer.handleCheckEventTriggers, tc.name, tc.method, "/event-triggers/check", tc.body, tc.wantStatus)
			if tc.wantStatus == http.StatusOK {
				type matchResp struct{ Matched bool }
				if got := decodeJSON[matchResp](t, rec).Matched; got != tc.wantMatched {
					t.Fatalf("got matched=%v, want %v", got, tc.wantMatched)
				}
			}
		})
	}
}

func TestHandleFireEventTriggers_Table(t *testing.T) {
	server, ts := setupTestServer(t)
	defer ts.Close()

	_ = InsertEventTrigger(context.Background(), server.handler.db, EventTrigger{
		ID:        "trig-fire-1",
		ChannelID: "1555405874565091380",
		Source:    "custom_source",
		Target:    "device.alpha",
		EventType: "ready",
		Prompt:    "Device alpha ready",
		Effort:    "low",
	})
	_ = InsertEventTrigger(context.Background(), server.handler.db, EventTrigger{
		ID:         "trig-fire-persistent-1",
		ChannelID:  "1555405874565091380",
		Source:     "camera",
		Target:     "entryway",
		EventType:  "package_delivered",
		Prompt:     "package arrived",
		Effort:     "low",
		Persistent: true,
	})
	_ = InsertEventTrigger(context.Background(), server.handler.db, EventTrigger{
		ID:         "trig-fire-persistent-error",
		ChannelID:  "1555405874565091380",
		Source:     "camera",
		Target:     "porch",
		EventType:  "package_delivered",
		Prompt:     "porch package",
		Effort:     "low",
		Persistent: true,
	})

	closedDB, err := OpenDB(&Config{DatabaseURL: ":memory:"})
	if err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}
	_ = closedDB.Close()
	serverClosed := NewServer(NewToolHandler(&Config{DatabaseURL: ":memory:"}, closedDB))

	mockDispFail := &mockDispatcher{err: errors.New("eri unavailable")}
	mockDispOK := &mockDispatcher{response: PromptDispatchResponse{Status: "ok", MessageID: "msg-1"}}
	serverNoDisp := NewServer(server.handler)
	serverNoDisp.dispatcher = nil
	serverFailDisp := NewServer(server.handler, mockDispFail)
	serverOKDisp := NewServer(server.handler, mockDispOK)

	tests := []struct {
		name       string
		method     string
		body       string
		useServer  *Server
		wantStatus int
		wantFired  int
	}{
		{"fire method not allowed", http.MethodGet, "", serverOKDisp, http.StatusMethodNotAllowed, 0},
		{"fire nil dispatcher", http.MethodPost, `{"source":"custom_source","target":"device.alpha","event_type":"ready"}`, serverNoDisp, http.StatusInternalServerError, 0},
		{"fire closed db error", http.MethodPost, `{"source":"custom_source","target":"device.alpha","event_type":"ready"}`, serverClosed, http.StatusInternalServerError, 0},
		{"fire dispatch error re-inserts trigger and returns 502", http.MethodPost, `{"source":"custom_source","target":"device.alpha","event_type":"ready","context_header":"External state change"}`, serverFailDisp, http.StatusBadGateway, 0},
		{"fire retry succeeds after re-insert", http.MethodPost, `{"source":"custom_source","target":"device.alpha","event_type":"ready","context_header":"External state change"}`, serverOKDisp, http.StatusOK, 1},
		{"fire subsequent call returns 0 fired", http.MethodPost, `{"source":"custom_source","target":"device.alpha","event_type":"ready"}`, serverOKDisp, http.StatusOK, 0},
		{"fire persistent trigger first time", http.MethodPost, `{"source":"camera","targets":["entryway"],"event_type":"package_delivered"}`, serverOKDisp, http.StatusOK, 1},
		{"fire persistent trigger second time within 5s debounce window returns 0 fired", http.MethodPost, `{"source":"camera","targets":["entryway"],"event_type":"package_delivered"}`, serverOKDisp, http.StatusOK, 0},
		{"fire dispatch error does not reinsert duplicate for persistent trigger", http.MethodPost, `{"source":"camera","targets":["porch"],"event_type":"package_delivered"}`, serverFailDisp, http.StatusBadGateway, 0},
		{"fire retry succeeds for persistent trigger after failed dispatch", http.MethodPost, `{"source":"camera","targets":["porch"],"event_type":"package_delivered"}`, serverOKDisp, http.StatusOK, 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := runEventTriggerEndpoint(t, tc.useServer.handleFireEventTriggers, tc.name, tc.method, "/event-triggers/fire", tc.body, tc.wantStatus)
			if tc.wantStatus == http.StatusOK {
				type fireResp struct{ Fired int }
				if got := decodeJSON[fireResp](t, rec).Fired; got != tc.wantFired {
					t.Fatalf("got fired=%d, want %d", got, tc.wantFired)
				}
			}
		})
	}
}

func TestServer_Metrics_Table(t *testing.T) {
	server, ts := setupTestServer(t)
	defer ts.Close()

	ctx := context.Background()
	_ = InsertCronSchedule(ctx, server.handler.db, CronSchedule{
		ID:        "m-cron-1",
		TargetID:  "chan-1",
		CronExpr:  "0 0 * * *",
		Prompt:    "daily",
		NextRunAt: time.Now().UTC(),
		Enabled:   true,
	})
	_ = InsertHeartbeatSchedule(ctx, server.handler.db, HeartbeatSchedule{
		ID:        "m-hb-1",
		TargetID:  "chan-2",
		CronExpr:  "0 0 * * *",
		Prompt:    "heartbeat",
		NextRunAt: time.Now().UTC(),
		Enabled:   true,
	})
	_ = InsertOneShotSchedule(ctx, server.handler.db, OneShotSchedule{
		ID:       "m-once-1",
		ThreadID: "chan-3",
		Prompt:   "once",
		RunAt:    time.Now().UTC(),
	})
	_ = InsertEventTrigger(ctx, server.handler.db, EventTrigger{
		ID:        "m-trig-1",
		ChannelID: "chan-4",
		Source:    "github",
		Target:    "repo",
		EventType: "push",
		Prompt:    "p",
	})

	RecordScheduleRun("cron", "completed")
	RecordScheduleRun("one_shot", "stale")
	RecordScheduleRun("heartbeat", "skipped_active")
	RecordScheduleRun("event_watch", "failed")
	RecordScheduleDispatchDuration("cron", 0.42)
	RecordScheduleDispatchDuration("event_watch", 1.25)

	nilServer := &Server{}
	serverNilDB := &Server{handler: &ToolHandler{}}
	registerHaroCollector(SchedulesActive)

	tests := []struct {
		name           string
		server         *Server
		method         string
		url            string
		wantStatus     int
		wantSubstrings []string
	}{
		{
			name:       "valid metrics with active counts and runs",
			server:     server,
			method:     http.MethodGet,
			url:        "/metrics",
			wantStatus: http.StatusOK,
			wantSubstrings: []string{
				"haro_schedules_active",
				`haro_schedules_active{schedule_type="cron"} 1`,
				`haro_schedules_active{schedule_type="heartbeat"} 1`,
				`haro_schedules_active{schedule_type="one_shot"} 1`,
				`haro_schedules_active{schedule_type="event_watch"} 1`,
				"haro_schedule_runs_total",
				`haro_schedule_runs_total{schedule_type="cron",status="completed"}`,
				`haro_schedule_runs_total{schedule_type="one_shot",status="stale"}`,
				`haro_schedule_runs_total{schedule_type="heartbeat",status="skipped_active"}`,
				`haro_schedule_runs_total{schedule_type="event_watch",status="failed"}`,
				"haro_schedule_dispatch_duration_seconds",
			},
		},
		{
			name:       "nil server db still serves metrics ok",
			server:     nilServer,
			method:     http.MethodGet,
			url:        "/metrics",
			wantStatus: http.StatusOK,
			wantSubstrings: []string{
				"haro_schedules_active",
				"haro_schedule_runs_total",
			},
		},
		{
			name:       "handler with nil db serves metrics ok",
			server:     serverNilDB,
			method:     http.MethodGet,
			url:        "/metrics",
			wantStatus: http.StatusOK,
			wantSubstrings: []string{
				"haro_schedules_active",
				"haro_schedule_runs_total",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(tc.method, tc.url, nil)
			tc.server.handleMetrics(rec, req)
			if rec.Code != tc.wantStatus {
				t.Fatalf("handleMetrics() status = %d, want %d", rec.Code, tc.wantStatus)
			}
			body := rec.Body.String()
			for _, sub := range tc.wantSubstrings {
				if !strings.Contains(body, sub) {
					t.Errorf("expected body to contain %q, body:\n%s", sub, body)
				}
			}
		})
	}
}
