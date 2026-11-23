package server

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/pubsubtest"
	"github.com/azylman/aerial/services/haro/internal/config"
	"github.com/azylman/aerial/services/haro/internal/metrics"
	"github.com/azylman/aerial/services/haro/internal/model"
	"github.com/azylman/aerial/services/haro/internal/store"
	"github.com/azylman/aerial/services/haro/internal/testutil"
	"github.com/azylman/aerial/services/haro/internal/tools"
)

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
	if err := store.InsertCronSchedule(ctx, server.handler.DB, model.CronSchedule{
		ID:        "m-cron-1",
		TargetID:  "chan-1",
		CronExpr:  "0 0 * * *",
		Prompt:    "daily",
		NextRunAt: time.Now().UTC(),
		Enabled:   true,
	}); err != nil {
		t.Fatalf("InsertCronSchedule failed: %v", err)
	}
	if err := store.InsertHeartbeatSchedule(ctx, server.handler.DB, model.HeartbeatSchedule{
		ID:        "m-hb-1",
		TargetID:  "chan-2",
		CronExpr:  "0 0 * * *",
		Prompt:    "heartbeat",
		NextRunAt: time.Now().UTC(),
		Enabled:   true,
	}); err != nil {
		t.Fatalf("InsertHeartbeatSchedule failed: %v", err)
	}
	if err := store.InsertOneShotSchedule(ctx, server.handler.DB, model.OneShotSchedule{
		ID:       "m-once-1",
		ThreadID: "chan-3",
		Prompt:   "once",
		RunAt:    time.Now().UTC(),
	}); err != nil {
		t.Fatalf("InsertOneShotSchedule failed: %v", err)
	}
	if err := store.InsertEventTrigger(ctx, server.handler.DB, model.EventTrigger{
		ID:        "m-trig-1",
		ChannelID: "chan-4",
		Source:    "github",
		Target:    "repo",
		EventType: "push",
		Prompt:    "p",
	}); err != nil {
		t.Fatalf("InsertEventTrigger failed: %v", err)
	}

	metrics.RecordScheduleRun("cron", "completed")
	metrics.RecordScheduleRun("one_shot", "stale")
	metrics.RecordScheduleRun("heartbeat", "skipped_active")
	metrics.RecordScheduleRun("event_watch", "failed")
	metrics.RecordScheduleDispatchDuration("cron", 0.42)
	metrics.RecordScheduleDispatchDuration("event_watch", 1.25)

	serverNilDB := &Server{handler: &tools.ToolHandler{}}
	metrics.RegisterHaroCollector(metrics.SchedulesActive)

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

func TestHandleOneShotJob_Scenarios(t *testing.T) {
	db := testutil.NewDB(t)
	ctx := context.Background()
	now := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name    string
		server  *Server
		db      *sql.DB
		msg     pubsub.Message
		wantErr bool
	}{
		{"nil message returns error", nil, db, nil, true},
		{"nil database returns error", nil, nil, &pubsubtest.Message{Payload: []byte(`{"id":"os-valid-1","thread_id":"th-1","prompt":"p1","run_at":"2026-11-14T12:00:00Z"}`)}, true},
		{"invalid json drops cleanly", nil, db, &pubsubtest.Message{Payload: []byte(`not-json`)}, false},
		{"missing fields drops cleanly", nil, db, &pubsubtest.Message{Payload: []byte(`{"thread_id":""}`)}, false},
		{"valid insert succeeds", nil, db, &pubsubtest.Message{Payload: []byte(`{"id":"os-valid-1","thread_id":"th-1","prompt":"p1","run_at":"2026-11-14T12:00:00Z"}`)}, false},
		{"duplicate insert stays valid", nil, db, &pubsubtest.Message{Payload: []byte(`{"id":"os-valid-1","thread_id":"th-1","prompt":"p1","run_at":"2026-11-14T12:00:00Z"}`)}, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if gotErr := HandleOneShotJob(ctx, tc.server, tc.db, tc.msg); (gotErr != nil) != tc.wantErr {
				t.Errorf("HandleOneShotJob() err = %v, wantErr %v", gotErr, tc.wantErr)
			}
		})
	}

	sched, err := store.GetOneShotSchedule(ctx, db, "os-valid-1")
	if err != nil {
		t.Fatalf("GetOneShotSchedule failed: %v", err)
	}
	if sched.Prompt != "p1" || sched.ThreadID != "th-1" {
		t.Errorf("unexpected schedule: %+v", sched)
	}

	toolHandler := tools.NewToolHandler(&config.Config{DatabaseURL: ":memory:"}, db)
	srv := NewServer(toolHandler)
	msgWithServer := &pubsubtest.Message{Payload: []byte(fmt.Sprintf(`{"thread_id":"th-2","prompt":"p2","run_at":%q}`, now.Format(time.RFC3339)))}
	if err := HandleOneShotJob(ctx, srv, nil, msgWithServer); err != nil {
		t.Fatalf("HandleOneShotJob with server failed: %v", err)
	}
}

func TestHandleOneShotJob_ArmsSchedule(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		payload      string
		deliveries   int
		ingress      int
		wantErr      bool
		wantRequest  string
		wantSameArms bool
	}{
		{
			name:        "inserted one-shot is armed for its due tick",
			payload:     `{"id":"os-arm-ok","thread_id":"th-1","prompt":"p1","run_at":"2026-11-14T12:00:00Z"}`,
			deliveries:  1,
			ingress:     http.StatusAccepted,
			wantRequest: "/HaroSchedule/os-arm-ok/Arm/send key=haro:arm:one_shot:os-arm-ok:2026-11-14T12:00:00Z",
		},
		{
			name:        "arm failure returns error so the job is redelivered",
			payload:     `{"id":"os-arm-fail","thread_id":"th-1","prompt":"p1","run_at":"2026-11-14T12:00:00Z"}`,
			deliveries:  1,
			ingress:     http.StatusServiceUnavailable,
			wantErr:     true,
			wantRequest: "/HaroSchedule/os-arm-fail/Arm/send key=haro:arm:one_shot:os-arm-fail:2026-11-14T12:00:00Z",
		},
		{
			name:         "redelivered job without id arms the same schedule",
			payload:      `{"thread_id":"th-9","prompt":"retry later","run_at":"2026-11-14T13:00:00Z"}`,
			deliveries:   2,
			ingress:      http.StatusAccepted,
			wantSameArms: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := testutil.NewDB(t)
			var requests []string
			ingress := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests = append(requests, r.URL.Path+" key="+r.Header.Get("Idempotency-Key"))
				w.WriteHeader(tc.ingress)
			}))
			t.Cleanup(ingress.Close)
			srv := NewServer(tools.NewToolHandler(&config.Config{DatabaseURL: ":memory:", RestateIngressURL: ingress.URL}, db))

			var err error
			for range tc.deliveries {
				err = HandleOneShotJob(context.Background(), srv, nil, &pubsubtest.Message{Payload: []byte(tc.payload)})
			}
			if (err != nil) != tc.wantErr {
				t.Fatalf("HandleOneShotJob() err = %v, wantErr %v", err, tc.wantErr)
			}
			if len(requests) != tc.deliveries {
				t.Fatalf("arm requests = %v, want %d", requests, tc.deliveries)
			}
			if tc.wantRequest != "" && requests[0] != tc.wantRequest {
				t.Errorf("arm request = %q, want %q", requests[0], tc.wantRequest)
			}
			if tc.wantSameArms && requests[0] != requests[1] {
				t.Errorf("redelivered arm = %q, want %q", requests[1], requests[0])
			}
		})
	}
}

func TestHandleOneShotJob_ClosedDB(t *testing.T) {
	ctx := context.Background()
	closedDB := testutil.NewClosedDB(t)

	tests := []struct {
		name    string
		msg     pubsub.Message
		wantErr bool
	}{
		{"closed database returns error", &pubsubtest.Message{Payload: []byte(`{"id":"os-closed-1","thread_id":"th-1","prompt":"p1","run_at":"2026-11-14T12:00:00Z"}`)}, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := HandleOneShotJob(ctx, nil, closedDB, tc.msg); (err != nil) != tc.wantErr {
				t.Errorf("HandleOneShotJob() err = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}
