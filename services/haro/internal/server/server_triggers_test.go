package server

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/haro/internal/config"
	"github.com/azylman/aerial/services/haro/internal/dispatch"
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
	_ = store.InsertCronSchedule(ctx, server.handler.DB, model.CronSchedule{
		ID:        "m-cron-1",
		TargetID:  "chan-1",
		CronExpr:  "0 0 * * *",
		Prompt:    "daily",
		NextRunAt: time.Now().UTC(),
		Enabled:   true,
	})
	_ = store.InsertHeartbeatSchedule(ctx, server.handler.DB, model.HeartbeatSchedule{
		ID:        "m-hb-1",
		TargetID:  "chan-2",
		CronExpr:  "0 0 * * *",
		Prompt:    "heartbeat",
		NextRunAt: time.Now().UTC(),
		Enabled:   true,
	})
	_ = store.InsertOneShotSchedule(ctx, server.handler.DB, model.OneShotSchedule{
		ID:       "m-once-1",
		ThreadID: "chan-3",
		Prompt:   "once",
		RunAt:    time.Now().UTC(),
	})
	_ = store.InsertEventTrigger(ctx, server.handler.DB, model.EventTrigger{
		ID:        "m-trig-1",
		ChannelID: "chan-4",
		Source:    "github",
		Target:    "repo",
		EventType: "push",
		Prompt:    "p",
	})

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

// stubMessage is a minimal pubsub.Message carrying only a payload; the
// one-shot ingestion path reads Data and nothing else.
type stubMessage struct {
	data         []byte
	numDelivered uint64
}

func (m *stubMessage) Data() []byte  { return m.data }
func (*stubMessage) Subject() string { return "" }
func (*stubMessage) Header() nats.Header {
	return nil
}
func (m *stubMessage) NumDelivered() uint64             { return m.numDelivered }
func (*stubMessage) Ack() error                         { return nil }
func (*stubMessage) NakWithDelay(_ time.Duration) error { return nil }
func (*stubMessage) Term() error                        { return nil }
func (*stubMessage) Snooze(_ time.Duration) error       { return nil }
func (*stubMessage) InProgress() error                  { return nil }

func TestHandleOneShotJob_Scenarios(t *testing.T) {
	db, err := testutil.OpenSQLiteTestDB(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLiteTestDB failed: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
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
		{"nil database returns error", nil, nil, &stubMessage{data: []byte(`{"id":"os-valid-1","thread_id":"th-1","prompt":"p1","run_at":"2026-11-14T12:00:00Z"}`)}, true},
		{"invalid json drops cleanly", nil, db, &stubMessage{data: []byte(`not-json`)}, false},
		{"missing fields drops cleanly", nil, db, &stubMessage{data: []byte(`{"thread_id":""}`)}, false},
		{"valid insert succeeds", nil, db, &stubMessage{data: []byte(`{"id":"os-valid-1","thread_id":"th-1","prompt":"p1","run_at":"2026-11-14T12:00:00Z"}`)}, false},
		{"duplicate insert stays valid", nil, db, &stubMessage{data: []byte(`{"id":"os-valid-1","thread_id":"th-1","prompt":"p1","run_at":"2026-11-14T12:00:00Z"}`)}, false},
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
	msgWithServer := &stubMessage{data: []byte(fmt.Sprintf(`{"thread_id":"th-2","prompt":"p2","run_at":%q}`, now.Format(time.RFC3339)))}
	if err := HandleOneShotJob(ctx, srv, nil, msgWithServer); err != nil {
		t.Fatalf("HandleOneShotJob with server failed: %v", err)
	}
}

func TestHandleOneShotJob_ClosedDB(t *testing.T) {
	ctx := context.Background()
	closedDB, err := testutil.OpenSQLiteTestDB(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLiteTestDB failed: %v", err)
	}
	_ = closedDB.Close()

	tests := []struct {
		name    string
		msg     pubsub.Message
		wantErr bool
	}{
		{"closed database returns error", &stubMessage{data: []byte(`{"id":"os-closed-1","thread_id":"th-1","prompt":"p1","run_at":"2026-11-14T12:00:00Z"}`)}, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := HandleOneShotJob(ctx, nil, closedDB, tc.msg); (err != nil) != tc.wantErr {
				t.Errorf("HandleOneShotJob() err = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

// stubScenarioDispatcher records dispatched prompts for external-event tests.
type stubScenarioDispatcher struct {
	dispatched []dispatch.PromptDispatchRequest
	response   dispatch.PromptDispatchResponse
	err        error
}

func (m *stubScenarioDispatcher) DispatchPrompt(_ context.Context, req dispatch.PromptDispatchRequest) (dispatch.PromptDispatchResponse, error) {
	m.dispatched = append(m.dispatched, req)
	return m.response, m.err
}

func (*stubScenarioDispatcher) Dispatch(_ context.Context, _ dispatch.DispatchRequest) (dispatch.DispatchResponse, error) {
	return dispatch.DispatchResponse{}, nil
}

func seedExternalEventTriggers(ctx context.Context, db *sql.DB) {
	_ = store.InsertEventTrigger(ctx, db, model.EventTrigger{
		ID:        "trig-ext-1",
		ChannelID: "100000000000000001",
		Source:    "camera",
		Target:    "driveway",
		EventType: "vehicle_detected",
		Prompt:    "Vehicle in driveway",
		Effort:    "low",
	})
	_ = store.InsertEventTrigger(ctx, db, model.EventTrigger{
		ID:        "trig-ext-fail",
		ChannelID: "100000000000000001",
		Source:    "camera",
		Target:    "backyard",
		EventType: "motion",
		Prompt:    "Motion in backyard",
		Effort:    "low",
	})
	_ = store.InsertEventTrigger(ctx, db, model.EventTrigger{
		ID:        "trig-ext-healthcheck",
		ChannelID: "100000000000000001",
		Source:    "healthcheck",
		Target:    "ping",
		EventType: "ping",
		Prompt:    "Healthcheck ping",
		Effort:    "low",
	})
	_ = store.InsertEventTrigger(ctx, db, model.EventTrigger{
		ID:         "trig-ext-fail-persistent",
		ChannelID:  "100000000000000001",
		Source:     "camera",
		Target:     "sideyard",
		EventType:  "motion",
		Prompt:     "Motion in sideyard",
		Effort:     "low",
		Persistent: true,
	})
}

func verifyTriggerState(ctx context.Context, t *testing.T, db *sql.DB, ev pubsub.ExternalEvent, wantInDB bool) {
	t.Helper()
	placeholders, args, ok := store.BuildEventTriggerFilterArgs(ev.Source, ev.Targets, ev.EventType)
	if !ok {
		t.Fatal("invalid event trigger filter args")
	}
	rawQuery := "SELECT EXISTS(SELECT 1 FROM event_triggers WHERE source = $1 AND event_type = $2 AND target IN (" + placeholders + "))"
	var matched bool
	if err := db.QueryRowContext(ctx, rawQuery, args...).Scan(&matched); err != nil {
		t.Fatalf("query event_triggers failed: %v", err)
	}
	if matched != wantInDB {
		t.Errorf("trigger in db = %v, want %v", matched, wantInDB)
	}
}

func verifyDispatchedPrompt(t *testing.T, disp *stubScenarioDispatcher, wantChannel, wantPrompt, wantMessageID string) {
	t.Helper()
	if len(disp.dispatched) != 1 {
		t.Fatalf("expected 1 dispatched prompt, got %d", len(disp.dispatched))
	}
	if disp.dispatched[0].ChannelID != wantChannel {
		t.Errorf("ChannelID = %s, want %s", disp.dispatched[0].ChannelID, wantChannel)
	}
	if disp.dispatched[0].Prompt != wantPrompt {
		t.Errorf("Prompt = %q, want %q", disp.dispatched[0].Prompt, wantPrompt)
	}
	if wantMessageID != "" && disp.dispatched[0].MessageID != wantMessageID {
		t.Errorf("MessageID = %s, want %s", disp.dispatched[0].MessageID, wantMessageID)
	}
}

func TestHandleExternalEvent_Scenarios(t *testing.T) {
	db, err := testutil.OpenSQLiteTestDB(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLiteTestDB failed: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()

	seedExternalEventTriggers(ctx, db)

	closedDB, err := testutil.OpenSQLiteTestDB(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLiteTestDB failed: %v", err)
	}
	_ = closedDB.Close()
	serverClosed := NewServer(tools.NewToolHandler(&config.Config{DatabaseURL: ":memory:"}, closedDB))

	mockDispFail := &stubScenarioDispatcher{err: errors.New("eri unavailable")}
	mockDispOK := &stubScenarioDispatcher{response: dispatch.PromptDispatchResponse{Status: "ok", MessageID: "msg-ext-1"}}
	serverOK := NewServer(tools.NewToolHandler(&config.Config{DatabaseURL: ":memory:"}, db), mockDispOK)
	serverFail := NewServer(tools.NewToolHandler(&config.Config{DatabaseURL: ":memory:"}, db), mockDispFail)
	serverNoDisp := NewServer(tools.NewToolHandler(&config.Config{DatabaseURL: ":memory:"}, db))

	tests := []struct {
		name                 string
		server               *Server
		ev                   pubsub.ExternalEvent
		wantErr              bool
		checkDBID            string
		wantInDB             bool
		wantImmediateReclaim bool
	}{
		{
			name:      "healthcheck source is no-op and leaves trigger unclaimed",
			server:    serverOK,
			ev:        pubsub.ExternalEvent{Source: "healthcheck", EventType: "ping", Targets: []string{"ping"}},
			wantErr:   false,
			checkDBID: "trig-ext-healthcheck",
			wantInDB:  true,
		},
		{
			name:    "nil db returns error from ClaimEventTriggers",
			server:  NewServer(tools.NewToolHandler(&config.Config{DatabaseURL: ":memory:"}, nil)),
			ev:      pubsub.ExternalEvent{Source: "camera", EventType: "vehicle_detected", Targets: []string{"driveway"}},
			wantErr: true,
		},
		{
			name:    "closed db returns error from ClaimEventTriggers",
			server:  serverClosed,
			ev:      pubsub.ExternalEvent{Source: "camera", EventType: "vehicle_detected", Targets: []string{"driveway"}},
			wantErr: true,
		},
		{
			name:    "no matching triggers succeeds without firing",
			server:  serverOK,
			ev:      pubsub.ExternalEvent{Source: "camera", EventType: "vehicle_detected", Targets: []string{"front_door"}},
			wantErr: false,
		},
		{
			name:      "dispatch failure recovers trigger to db and returns nil to avoid redelivery storm",
			server:    serverFail,
			ev:        pubsub.ExternalEvent{Source: "camera", EventType: "motion", Targets: []string{"backyard"}},
			wantErr:   false,
			checkDBID: "trig-ext-fail",
			wantInDB:  true,
		},
		{
			name:      "nil dispatcher recovers trigger to db and returns nil to avoid redelivery storm",
			server:    serverNoDisp,
			ev:        pubsub.ExternalEvent{Source: "camera", EventType: "motion", Targets: []string{"backyard"}},
			wantErr:   false,
			checkDBID: "trig-ext-fail",
			wantInDB:  true,
		},
		{
			name:                 "dispatch failure recovers persistent trigger to db",
			server:               serverFail,
			ev:                   pubsub.ExternalEvent{Source: "camera", EventType: "motion", Targets: []string{"sideyard"}},
			wantErr:              false,
			checkDBID:            "trig-ext-fail-persistent",
			wantInDB:             true,
			wantImmediateReclaim: true,
		},
		{
			name:      "successful dispatch claims and fires trigger",
			server:    serverOK,
			ev:        pubsub.ExternalEvent{EventID: "evt-cam-123", Source: "camera", EventType: "vehicle_detected", Targets: []string{"driveway"}, ContextHeader: "Camera Alert"},
			wantErr:   false,
			checkDBID: "trig-ext-1",
			wantInDB:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotErr := tc.server.HandleExternalEvent(ctx, tc.ev)
			if (gotErr != nil) != tc.wantErr {
				t.Fatalf("HandleExternalEvent() err = %v, wantErr = %v", gotErr, tc.wantErr)
			}
			if tc.checkDBID != "" {
				verifyTriggerState(ctx, t, db, tc.ev, tc.wantInDB)
			}
			if tc.wantImmediateReclaim {
				reclaimed, rErr := store.ClaimEventTriggers(ctx, db, tc.ev.Source, tc.ev.Targets, tc.ev.EventType)
				if rErr != nil {
					t.Fatalf("ClaimEventTriggers failed: %v", rErr)
				}
				if len(reclaimed) != 1 || reclaimed[0].ID != tc.checkDBID {
					t.Fatalf("expected persistent trigger %s to be immediately reclaimable, got %v", tc.checkDBID, reclaimed)
				}
			}
			if tc.name == "successful dispatch claims and fires trigger" {
				wantMsgID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("event_watch:trig-ext-1:evt-cam-123")).String()
				verifyDispatchedPrompt(t, mockDispOK, "100000000000000001", "Camera Alert\n\nVehicle in driveway", wantMsgID)
			}
		})
	}
}
