package main

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestDirectDB_SummaryAndRunsErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	closedDB, err := OpenDB(t.Context(), &Config{DatabaseURL: ":memory:"})
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
			req := httptest.NewRequest(tc.method, tc.url, http.NoBody)
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

func TestServer_RecoverFailedEventTrigger(t *testing.T) {
	server, ts := setupTestServer(t)
	defer ts.Close()
	ctx := context.Background()

	closedDB, _ := OpenDB(t.Context(), &Config{DatabaseURL: ":memory:"})
	_ = closedDB.Close()
	sClosed := NewServer(NewToolHandler(&Config{DatabaseURL: ":memory:"}, closedDB))

	tests := []struct {
		name   string
		server *Server
		trig   EventTrigger
	}{
		{
			name:   "persistent trigger with valid db",
			server: server,
			trig: EventTrigger{
				ID:         "trig-p-1",
				ChannelID:  "chan-1",
				Source:     "github",
				Target:     "repo",
				EventType:  "push",
				Prompt:     "test prompt",
				Persistent: true,
			},
		},
		{
			name:   "non persistent trigger with closed db logs error",
			server: sClosed,
			trig: EventTrigger{
				ID:         "trig-np-err",
				Persistent: false,
			},
		},
		{
			name:   "persistent trigger with closed db logs error",
			server: sClosed,
			trig: EventTrigger{
				ID:         "trig-p-err",
				Persistent: true,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.server.recoverFailedEventTrigger(ctx, tc.trig)
		})
	}
}
