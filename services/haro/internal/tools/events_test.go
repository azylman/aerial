package tools

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/services/haro/internal/model"
	"github.com/azylman/aerial/services/haro/internal/store"
)

func TestGetMapString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		args map[string]any
		keys []string
		want string
	}{
		{name: "nil map", args: nil, keys: []string{"a"}, want: ""},
		{name: "missing key", args: map[string]any{"b": "val"}, keys: []string{"a"}, want: ""},
		{name: "empty string", args: map[string]any{"a": "   "}, keys: []string{"a"}, want: ""},
		{name: "non string", args: map[string]any{"a": 123}, keys: []string{"a"}, want: ""},
		{name: "valid string trimmed", args: map[string]any{"a": "  hello  "}, keys: []string{"a"}, want: "hello"},
		{name: "fallback key", args: map[string]any{"b": "fallback"}, keys: []string{"a", "b"}, want: "fallback"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := getMapString(tc.args, tc.keys...); got != tc.want {
				t.Errorf("getMapString() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestGetMapBool(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		args      map[string]any
		key       string
		wantVal   bool
		wantFound bool
	}{
		{name: "nil map", args: nil, key: "k", wantVal: false, wantFound: false},
		{name: "missing key", args: map[string]any{"other": true}, key: "k", wantVal: false, wantFound: false},
		{name: "bool true", args: map[string]any{"k": true}, key: "k", wantVal: true, wantFound: true},
		{name: "bool false", args: map[string]any{"k": false}, key: "k", wantVal: false, wantFound: true},
		{name: "string true", args: map[string]any{"k": "true"}, key: "k", wantVal: true, wantFound: true},
		{name: "string 1", args: map[string]any{"k": "1"}, key: "k", wantVal: true, wantFound: true},
		{name: "string false", args: map[string]any{"k": "false"}, key: "k", wantVal: false, wantFound: true},
		{name: "string 0", args: map[string]any{"k": "0"}, key: "k", wantVal: false, wantFound: true},
		{name: "string other", args: map[string]any{"k": "maybe"}, key: "k", wantVal: false, wantFound: false},
		{name: "non-bool type", args: map[string]any{"k": 123}, key: "k", wantVal: false, wantFound: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			valPtr, found := getMapBool(tc.args, tc.key)
			if found != tc.wantFound {
				t.Fatalf("getMapBool() found = %v, want %v", found, tc.wantFound)
			}
			if found && (valPtr == nil || *valPtr != tc.wantVal) {
				t.Errorf("getMapBool() val = %v, want %v", valPtr, tc.wantVal)
			}
		})
	}
}

// newWatchVanadisServer fakes Vanadis build/chunk status lookups and points the store at it until
// cleanup. build-toctou-1 reports running on the first lookup and completed afterwards.
func newWatchVanadisServer(t *testing.T) {
	t.Helper()
	builds := map[string]string{"build-running-1": "running", "build-term-1": "completed"}
	chunks := map[string]string{"chunk-pending-1": "pending", "chunk-term-1": "merged", "build-running-1-chunk-1": "pending"}
	var toctouCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if id, ok := strings.CutPrefix(r.URL.Path, "/builds/"); ok {
			if id == "build-toctou-1" {
				status := "completed"
				if toctouCalls.Add(1) == 1 {
					status = "running"
				}
				httpkit.WriteJSON(w, http.StatusOK, map[string]any{"build": map[string]string{"status": status}})
				return
			}
			if st, ok := builds[id]; ok {
				httpkit.WriteJSON(w, http.StatusOK, map[string]any{"build": map[string]string{"status": st}})
				return
			}
		}
		if id, ok := strings.CutPrefix(r.URL.Path, "/chunks/"); ok {
			if st, ok := chunks[id]; ok {
				httpkit.WriteJSON(w, http.StatusOK, map[string]any{"chunk": map[string]string{"status": st}})
				return
			}
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(ts.Close)
	old := store.GetVanadisURL()
	store.SetVanadisURL(ts.URL)
	t.Cleanup(func() { store.SetVanadisURL(old) })
}

func TestHandleWatchEvent(t *testing.T) {
	newWatchVanadisServer(t)
	db := openToolsTestDB(t)
	ctx := context.Background()
	const ch = "1562847268833337377"
	tests := []struct {
		name           string
		args           map[string]any
		extraSources   []string
		wantErr        bool
		wantPersistent bool
		wantNoTarget   string
	}{
		{
			name: "missing channel_id",
			args: map[string]any{"source": "flux", "target": "aerial", "event_type": "sync_success", "prompt": "ok"}, wantErr: true,
		},
		{
			name:    "non-numeric snowflake channel_id",
			args:    map[string]any{"channel_id": "bad-id", "source": "flux", "target": "aerial", "event_type": "sync_success", "prompt": "ok"},
			wantErr: true,
		},
		{name: "missing prompt", args: map[string]any{"channel_id": ch, "source": "flux", "target": "aerial", "event_type": "sync_success"}, wantErr: true},
		{
			name:    "invalid effort",
			args:    map[string]any{"channel_id": ch, "source": "flux", "target": "aerial", "event_type": "sync_success", "prompt": "ok", "effort": "extreme"},
			wantErr: true,
		},
		{
			name:    "invalid spec",
			args:    map[string]any{"channel_id": ch, "source": "unknown", "target": "aerial", "event_type": "sync_success", "prompt": "ok"},
			wantErr: true,
		},
		{
			name:    "live validation failure build not found",
			args:    map[string]any{"channel_id": ch, "source": "vanadis_build", "target": "build-not-found", "event_type": "completed", "prompt": "ok"},
			wantErr: true,
		},
		{
			name:    "live validation failure build in terminal state",
			args:    map[string]any{"channel_id": ch, "source": "vanadis_build", "target": "build-term-1", "event_type": "completed", "prompt": "ok"},
			wantErr: true,
		},
		{
			name:         "terminal race after insert deletes the inserted row",
			args:         map[string]any{"channel_id": ch, "source": "vanadis_build", "target": "build-toctou-1", "event_type": "completed", "prompt": "racing build"},
			wantErr:      true,
			wantNoTarget: "build-toctou-1",
		},
		{
			name: "success with defaults",
			args: map[string]any{"channel_id": ch, "source": "flux", "target": "aerial", "event_type": "sync_success", "prompt": "deploy ready"},
		},
		{
			name: "success with explicit fields and vanadis chunk",
			args: map[string]any{
				"channel_id": "1562847268833337378", "source": "vanadis_chunk", "target": "build-running-1-chunk-1",
				"event_type": "merged", "prompt": "chunk 1 merged", "effort": "high",
			},
		},
		{
			name: "success with extra source and persistent true",
			args: map[string]any{
				"channel_id": "1562847268833337378", "source": "camera", "target": "garage",
				"event_type": "person_arrived", "prompt": "garage person detected", "persistent": true,
			},
			extraSources: []string{"camera"}, wantPersistent: true,
		},
		{
			name: "success with thread_id fallback",
			args: map[string]any{"thread_id": "1562847268833337379", "source": "flux", "target": "aerial", "event_type": "sync_success", "prompt": "thread fallback test"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := HandleWatchEvent(ctx, db, tc.args, tc.extraSources...)
			if (err != nil) != tc.wantErr {
				t.Fatalf("HandleWatchEvent() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantNoTarget != "" {
				var count int
				if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM event_triggers WHERE target = ?", tc.wantNoTarget).Scan(&count); err != nil {
					t.Fatalf("failed to count event triggers: %v", err)
				}
				if count != 0 {
					t.Errorf("event triggers for %q = %d, want 0 after race cleanup", tc.wantNoTarget, count)
				}
			}
			if tc.wantErr {
				return
			}
			if res == nil || len(res.Content) == 0 {
				t.Fatalf("expected non-empty CallToolResult")
			}
			txt, isTxt := res.Content[0].(*mcp.TextContent)
			if tc.wantPersistent && (!isTxt || !strings.Contains(txt.Text, `"persistent":true`)) {
				t.Errorf("expected persistent:true in output, got %v", res.Content[0])
			}
		})
	}
}

func seedWatchTriggers(ctx context.Context, t *testing.T, db *sql.DB, triggers ...model.EventTrigger) {
	t.Helper()
	for _, trig := range triggers {
		if err := store.InsertEventTrigger(ctx, db, trig); err != nil {
			t.Fatalf("InsertEventTrigger(%s) err = %v", trig.ID, err)
		}
	}
}

func TestHandleListEventWatches(t *testing.T) {
	db := openToolsTestDB(t)
	ctx := context.Background()
	seedWatchTriggers(ctx, t, db,
		model.EventTrigger{ID: "watch-list-1", ChannelID: "1562847268833337377", Source: "flux", Target: "repo-alpha", EventType: "sync_success", Prompt: "p1"},
		model.EventTrigger{ID: "watch-list-2", ChannelID: "1562847268833337378", Source: "flux", Target: "repo-alpha", EventType: "sync_success", Prompt: "p2"},
	)
	tests := []struct {
		name      string
		args      map[string]any
		wantCount int
	}{
		{"list all watches", map[string]any{}, 2},
		{"list filtered channel", map[string]any{"channel_id": "1562847268833337377"}, 1},
		{"list filtered target_id alias", map[string]any{"target_id": "1562847268833337378"}, 1},
		{"list empty channel", map[string]any{"channel_id": "999999999999999999"}, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := HandleListEventWatches(ctx, db, tc.args)
			if err != nil {
				t.Fatalf("HandleListEventWatches failed: %v", err)
			}
			if res == nil || len(res.Content) == 0 {
				t.Fatalf("expected non-empty CallToolResult")
			}
			text, ok := res.Content[0].(*mcp.TextContent)
			if !ok {
				t.Fatalf("expected *mcp.TextContent")
			}
			var out struct {
				EventWatches []model.EventTrigger `json:"event_watches"`
				Count        int                  `json:"count"`
			}
			if err := json.Unmarshal([]byte(text.Text), &out); err != nil {
				t.Fatalf("failed to unmarshal output: %v", err)
			}
			if len(out.EventWatches) != tc.wantCount {
				t.Errorf("got %d event watches, want %d", len(out.EventWatches), tc.wantCount)
			}
		})
	}
}

func TestHandleCancelEventWatch(t *testing.T) {
	db := openToolsTestDB(t)
	ctx := context.Background()
	seedWatchTriggers(ctx, t, db,
		model.EventTrigger{ID: "watch-key-1", ChannelID: "1562847268833337377", Source: "flux", Target: "example-org/repo", EventType: "sync_success", Prompt: "p1"},
		model.EventTrigger{ID: "watch-key-2", ChannelID: "1562847268833337377", Source: "flux", Target: "example-org/repo", EventType: "sync_success", Prompt: "p2"},
		model.EventTrigger{ID: "watch-key-3", ChannelID: "1562847268833337377", Source: "flux", Target: "example-org/repo", EventType: "sync_success", Prompt: "p3"},
	)
	tests := []struct {
		name    string
		args    map[string]any
		wantErr bool
	}{
		{"missing watch_id", map[string]any{}, true},
		{"non-existent watch_id", map[string]any{"watch_id": "not-found-id"}, true},
		{"cancel via watch_id", map[string]any{"watch_id": "watch-key-1"}, false},
		{"cancel via id alias", map[string]any{"id": "watch-key-2"}, false},
		{"cancel via schedule_id alias", map[string]any{"schedule_id": "watch-key-3"}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := HandleCancelEventWatch(ctx, db, tc.args)
			if (err != nil) != tc.wantErr {
				t.Fatalf("HandleCancelEventWatch err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr && (res == nil || len(res.Content) == 0) {
				t.Fatalf("expected non-empty CallToolResult")
			}
		})
	}
}

func TestArgs_ToMap_Table(t *testing.T) {
	t.Parallel()
	enabled := true
	persistent := true
	tests := []struct {
		name string
		got  map[string]any
		want map[string]any
	}{
		{
			"schedule heartbeat full",
			ScheduleHeartbeatArgs{ChannelID: "ch", TargetID: "t", TitlePrefix: "T", CronExpression: "0 * * * *", CronExpr: "*/5 * * * *", Prompt: "p", Timezone: "UTC", Effort: "high"}.ToMap(),
			map[string]any{"channel_id": "ch", "target_id": "t", "title_prefix": "T", "cron_expression": "0 * * * *", "cron_expr": "*/5 * * * *", "prompt": "p", "timezone": "UTC", "effort": "high"},
		},
		{"schedule heartbeat empty", ScheduleHeartbeatArgs{}.ToMap(), map[string]any{}},
		{
			"update heartbeat full",
			UpdateHeartbeatArgs{ScheduleID: "s", ID: "i", ChannelID: "ch", TargetID: "t", CronExpression: "0 * * * *", CronExpr: "*/5 * * * *", Prompt: "p", TitlePrefix: "T", Timezone: "UTC", Effort: "low", Enabled: &enabled}.ToMap(),
			map[string]any{"schedule_id": "s", "id": "i", "channel_id": "ch", "target_id": "t", "cron_expression": "0 * * * *", "cron_expr": "*/5 * * * *", "prompt": "p", "title_prefix": "T", "timezone": "UTC", "effort": "low", "enabled": true},
		},
		{"update heartbeat empty", UpdateHeartbeatArgs{}.ToMap(), map[string]any{}},
		{
			"watch event full",
			WatchEventArgs{ChannelID: "ch", TargetID: "t", ThreadID: "th", Source: "flux", Target: "aerial", EventType: "sync_success", Prompt: "p", Effort: "high", Persistent: &persistent}.ToMap(),
			map[string]any{"channel_id": "ch", "target_id": "t", "thread_id": "th", "source": "flux", "target": "aerial", "event_type": "sync_success", "prompt": "p", "effort": "high", "persistent": true},
		},
		{"watch event empty", WatchEventArgs{}.ToMap(), map[string]any{}},
		{
			"list watches channel filter",
			ListEventWatchesArgs{ChannelID: "ch", TargetID: "t"}.ToMap(),
			map[string]any{"channel_id": "ch", "target_id": "t"},
		},
		{"list watches empty", ListEventWatchesArgs{}.ToMap(), map[string]any{}},
		{
			"cancel watch aliases",
			CancelEventWatchArgs{WatchID: "w", ID: "i", ScheduleID: "s"}.ToMap(),
			map[string]any{"watch_id": "w", "id": "i", "schedule_id": "s"},
		},
		{"cancel watch empty", CancelEventWatchArgs{}.ToMap(), map[string]any{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if len(tc.got) != len(tc.want) {
				t.Fatalf("ToMap() = %v (len %d), want %d entries", tc.got, len(tc.got), len(tc.want))
			}
			for k, want := range tc.want {
				if got := tc.got[k]; got != want {
					t.Errorf("ToMap()[%q] = %v, want %v", k, got, want)
				}
			}
		})
	}
}

func TestHeartbeatTools_HandleScheduleHeartbeat(t *testing.T) {
	db := openToolsTestDB(t)
	ctx := context.Background()

	tests := []struct {
		name      string
		defaultTZ string
		args      map[string]any
		wantErr   bool
	}{
		{name: "missing channel_id", defaultTZ: "UTC", args: map[string]any{"cron_expr": "0 * * * *", "prompt": "check"}, wantErr: true},
		{
			name:      "non-numeric snowflake channel_id",
			defaultTZ: "UTC",
			args:      map[string]any{"channel_id": "invalid-snowflake", "cron_expr": "0 * * * *", "prompt": "check"},
			wantErr:   true,
		},
		{name: "missing cron_expr", defaultTZ: "UTC", args: map[string]any{"channel_id": "1562847268833337377", "prompt": "check"}, wantErr: true},
		{
			name:      "invalid cron_expr",
			defaultTZ: "UTC",
			args:      map[string]any{"channel_id": "1562847268833337377", "cron_expr": "not-a-cron", "prompt": "check"},
			wantErr:   true,
		},
		{name: "missing prompt", defaultTZ: "UTC", args: map[string]any{"channel_id": "1562847268833337377", "cron_expr": "0 * * * *"}, wantErr: true},
		{
			name:      "invalid effort",
			defaultTZ: "UTC",
			args:      map[string]any{"channel_id": "1562847268833337377", "cron_expr": "0 * * * *", "prompt": "check", "effort": "ultra"},
			wantErr:   true,
		},
		{
			name:      "success with defaults",
			defaultTZ: "UTC",
			args:      map[string]any{"channel_id": "1562847268833337377", "cron_expr": "0 0 * * *", "prompt": "Monitor system health"},
			wantErr:   false,
		},
		{
			name:      "success with explicit fields",
			defaultTZ: "America/Los_Angeles",
			args: map[string]any{
				"channel_id":   "1562847268833337378",
				"cron_expr":    "*/10 * * * *",
				"prompt":       "Check memory leaks",
				"title_prefix": "Memory Alert",
				"timezone":     "UTC",
				"effort":       "high",
			},
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := HandleScheduleHeartbeat(ctx, db, tc.defaultTZ, tc.args)
			if (err != nil) != tc.wantErr {
				t.Fatalf("HandleScheduleHeartbeat() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr {
				if res == nil || len(res.Content) == 0 {
					t.Fatalf("expected non-empty CallToolResult content")
				}
				textContent, ok := res.Content[0].(*mcp.TextContent)
				if !ok || textContent.Text == "" {
					t.Fatalf("expected TextContent, got %+v", res.Content[0])
				}
			}
		})
	}
}

func TestHeartbeatTools_HandleUpdateHeartbeat(t *testing.T) {
	db := openToolsTestDB(t)
	ctx := context.Background()

	// Seed an existing heartbeat
	seedRes, err := HandleScheduleHeartbeat(ctx, db, "UTC", map[string]any{
		"channel_id": "1562847268833337377",
		"cron_expr":  "0 0 * * *",
		"prompt":     "Original Prompt",
		"effort":     "low",
	})
	if err != nil {
		t.Fatalf("failed to seed heartbeat: %v", err)
	}
	var seedMap map[string]any
	tc, ok := seedRes.Content[0].(*mcp.TextContent)
	if !ok {
		t.Fatalf("seed result content is %T, want *mcp.TextContent", seedRes.Content[0])
	}
	if err := json.Unmarshal([]byte(tc.Text), &seedMap); err != nil {
		t.Fatalf("unmarshal seed map failed: %v", err)
	}
	hbID, ok := seedMap["schedule_id"].(string)
	if !ok {
		t.Fatalf("schedule_id is %T, want string", seedMap["schedule_id"])
	}

	// Seed a heartbeat with a stored cron expression that no longer parses,
	// bypassing tool validation via direct insert.
	badCronHB := model.HeartbeatSchedule{ID: "hb-bad-cron", TargetID: "1562847268833337377", CronExpr: "bad cron", Prompt: "p", Timezone: "UTC", Effort: "low", NextRunAt: time.Now().UTC(), Enabled: true, CreatedAt: time.Now().UTC()}
	if err := store.InsertHeartbeatSchedule(ctx, db, badCronHB); err != nil {
		t.Fatalf("failed to seed bad-cron heartbeat: %v", err)
	}

	tests := []struct {
		name    string
		args    map[string]any
		wantErr bool
	}{
		{name: "missing id", args: map[string]any{"prompt": "new prompt"}, wantErr: true},
		{name: "not found id", args: map[string]any{"id": "missing-uuid", "prompt": "new"}, wantErr: true},
		{name: "invalid channel snowflake", args: map[string]any{"id": hbID, "channel_id": "abc"}, wantErr: true},
		{name: "invalid cron_expr", args: map[string]any{"id": hbID, "cron_expr": "bad cron"}, wantErr: true},
		{name: "invalid timezone", args: map[string]any{"id": hbID, "timezone": "Invalid/Zone"}, wantErr: true},
		{name: "invalid effort", args: map[string]any{"id": hbID, "effort": "extreme"}, wantErr: true},
		{name: "recompute fails on stored invalid cron", args: map[string]any{"id": "hb-bad-cron", "timezone": "UTC"}, wantErr: true},
		{
			name: "valid full update",
			args: map[string]any{
				"id":           hbID,
				"channel_id":   "1562847268833337999",
				"cron_expr":    "*/30 * * * *",
				"prompt":       "Updated Prompt",
				"title_prefix": "New Prefix",
				"timezone":     "America/New_York",
				"effort":       "medium",
				"enabled":      true,
			},
			wantErr: false,
		},
		{
			name: "disable heartbeat",
			args: map[string]any{
				"id":      hbID,
				"enabled": false,
			},
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := HandleUpdateHeartbeat(ctx, db, tc.args)
			if (err != nil) != tc.wantErr {
				t.Fatalf("HandleUpdateHeartbeat() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr {
				if res == nil || len(res.Content) == 0 {
					t.Fatalf("expected non-empty CallToolResult content")
				}
			}
		})
	}
}
