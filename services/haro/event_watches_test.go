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

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestPureHelpers_MapArgExtractors(t *testing.T) {
	stringTests := []struct {
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
	for _, tc := range stringTests {
		t.Run("string_"+tc.name, func(t *testing.T) {
			if got := getMapString(tc.args, tc.keys...); got != tc.want {
				t.Errorf("getMapString() = %q, want %q", got, tc.want)
			}
		})
	}

	boolTests := []struct {
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
	for _, tc := range boolTests {
		t.Run("bool_"+tc.name, func(t *testing.T) {
			valPtr, found := getMapBool(tc.args, tc.key)
			if found != tc.wantFound {
				t.Fatalf("getMapBool() found = %v, want %v", found, tc.wantFound)
			}
			if found {
				if valPtr == nil || *valPtr != tc.wantVal {
					t.Errorf("getMapBool() val = %v, want %v", valPtr, tc.wantVal)
				}
			}
		})
	}
}

func TestPureHelpers_ValidateEventWatchSpec(t *testing.T) {
	tests := []struct {
		name          string
		source        string
		target        string
		eventType     string
		extraSources  []string
		wantSource    string
		wantTarget    string
		wantEventType string
		wantErr       bool
	}{
		// empty target
		{"empty target", "vanadis_build", "", "completed", nil, "", "", "", true},
		// vanadis_build
		{"vanadis_build completed", "vanadis_build", "build-123-abc", "completed", nil, "vanadis_build", "build-123-abc", "completed", false},
		{"vanadis_build failed", "VANADIS_BUILD", "build-456", "FAILED", nil, "vanadis_build", "build-456", "failed", false},
		{"vanadis_build invalid event_type", "vanadis_build", "build-123", "running", nil, "", "", "", true},
		{"vanadis_build target with chunk rejected", "vanadis_build", "build-123-chunk-1", "completed", nil, "", "", "", true},
		{"vanadis_build invalid target regex", "vanadis_build", "invalid_build", "completed", nil, "", "", "", true},
		// vanadis_chunk
		{"vanadis_chunk merged", "vanadis_chunk", "build-123-chunk-1", "merged", nil, "vanadis_chunk", "build-123-chunk-1", "merged", false},
		{"vanadis_chunk failed", "vanadis_chunk", "anyprefix-chunk-99", "failed", nil, "vanadis_chunk", "anyprefix-chunk-99", "failed", false},
		{"vanadis_chunk invalid event_type", "vanadis_chunk", "build-123-chunk-1", "completed", nil, "", "", "", true},
		{"vanadis_chunk invalid target", "vanadis_chunk", "build-123", "merged", nil, "", "", "", true},
		// flux
		{"flux bare repo sync_success", "flux", "aerial", "sync_success", nil, "flux", "azylman/aerial", "sync_success", false},
		{"flux full repo sync_failed", "flux", "azylman/aerial-config", "sync_failed", nil, "flux", "azylman/aerial-config", "sync_failed", false},
		{"flux invalid event_type", "flux", "aerial", "deployed", nil, "", "", "", true},
		{"flux unmanaged repo", "flux", "other/repo", "sync_success", nil, "", "", "", true},
		// github
		{"github repo only pr_merged", "github", "aerial", "pr_merged", nil, "github", "azylman/aerial", "pr_merged", false},
		{"github repo with branch ci_passed", "github", "azylman/aerial:main", "ci_passed", nil, "github", "azylman/aerial:main", "ci_passed", false},
		{"github refs heads stripped ci_failed", "github", "aerial:refs/heads/feature/test", "ci_failed", nil, "github", "azylman/aerial:feature/test", "ci_failed", false},
		{"github PR number with hash rejected", "github", "aerial:#123", "ci_passed", nil, "", "", "", true},
		{"github PR number numeric rejected", "github", "aerial:123", "ci_passed", nil, "", "", "", true},
		{"github 40-char SHA rejected", "github", "aerial:0123456789abcdef0123456789abcdef01234567", "ci_passed", nil, "", "", "", true},
		{"github empty branch rejected", "github", "aerial:", "ci_passed", nil, "", "", "", true},
		{"github unmanaged repo rejected", "github", "unmanaged/repo:main", "ci_passed", nil, "", "", "", true},
		{"github invalid event_type", "github", "aerial", "completed", nil, "", "", "", true},
		// unknown source vs configured extra_event_sources
		{"unknown source without extraSources", "custom_iot", "device.washer", "idle", nil, "", "", "", true},
		{"configured extra source valid", "Custom_IoT", "device.washer", "IDLE", []string{"custom_iot"}, "custom_iot", "device.washer", "idle", false},
		{"configured extra source empty event_type", "custom_iot", "device.washer", "   ", []string{"custom_iot"}, "", "", "", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, target, ev, err := ValidateEventWatchSpec(tc.source, tc.target, tc.eventType, tc.extraSources...)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ValidateEventWatchSpec() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr {
				if s != tc.wantSource || target != tc.wantTarget || ev != tc.wantEventType {
					t.Errorf("got (%q, %q, %q), want (%q, %q, %q)", s, target, ev, tc.wantSource, tc.wantTarget, tc.wantEventType)
				}
			}
		})
	}
}

func TestPureHelpers_FormatEventTriggerPrompt(t *testing.T) {
	tests := []struct {
		name    string
		header  string
		prompt  string
		wantStr string
	}{
		{"both header and prompt present", "Event trigger: test", "Proceed with task", "Event trigger: test\n\nProceed with task"},
		{"empty header returns prompt", "   ", "Proceed with task", "Proceed with task"},
		{"empty prompt returns header", "Event trigger: test", "   ", "Event trigger: test"},
		{"both empty returns empty", "", "", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := FormatEventTriggerPrompt(tc.header, tc.prompt); got != tc.wantStr {
				t.Fatalf("FormatEventTriggerPrompt() = %q, want %q", got, tc.wantStr)
			}
		})
	}
}

func handleMockEntityStatus(w http.ResponseWriter, key, id string, mockMap map[string]string) {
	if strings.HasSuffix(id, "-500") {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if strings.HasSuffix(id, "-badjson") {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("{bad-json"))
		return
	}
	if st, ok := mockMap[id]; ok {
		payload := map[string]any{key: map[string]string{"status": st}}
		if key == "chunk" {
			payload["build"] = map[string]string{"status": "running"}
		}
		httpkit.WriteJSON(w, http.StatusOK, payload)
		return
	}
	http.Error(w, "not found", http.StatusNotFound)
}

func testValidateLiveEventTarget(ctx context.Context, t *testing.T, db *sql.DB) {
	t.Helper()
	validateTests := []struct {
		name            string
		source          string
		target          string
		wantErr         bool
		wantErrContains string
	}{
		{"vanadis_build running", "vanadis_build", "b-running", false, ""},
		{"vanadis_build completed", "vanadis_build", "b-completed", true, ""},
		{"vanadis_build failed", "vanadis_build", "b-failed", true, ""},
		{"vanadis_build cancelled", "vanadis_build", "b-cancelled", true, ""},
		{"vanadis_build canceled", "vanadis_build", "b-canceled", true, ""},
		{"vanadis_build not found", "vanadis_build", "b-missing", true, ""},
		{"vanadis_build 500 error", "vanadis_build", "b-500", true, ""},
		{"vanadis_build bad json", "vanadis_build", "b-badjson", true, ""},
		{"vanadis_chunk pending", "vanadis_chunk", "c-pending", false, ""},
		{"vanadis_chunk merged", "vanadis_chunk", "c-merged", true, ""},
		{"vanadis_chunk completed", "vanadis_chunk", "c-completed", true, ""},
		{"vanadis_chunk failed", "vanadis_chunk", "c-failed", true, ""},
		{"vanadis_chunk not found", "vanadis_chunk", "c-missing", true, ""},
		{"vanadis_chunk 500 error", "vanadis_chunk", "c-500", true, ""},
		{"vanadis_chunk bad json", "vanadis_chunk", "c-badjson", true, ""},
		{"empty target", "vanadis_build", "   ", true, ""},
		{"other source flux", "flux", "azylman/aerial", false, ""},
		{"other source github", "github", "azylman/aerial:main", false, ""},
		{"other source nomad", "nomad", "aerial-brain", false, ""},
		{"vanadis URL not configured build", "vanadis_build", "b-running", true, "vanadis URL is not configured"},
		{"vanadis URL not configured chunk", "vanadis_chunk", "c-pending", true, "vanadis URL is not configured"},
	}
	for _, tc := range validateTests {
		t.Run("validate_"+tc.name, func(t *testing.T) {
			if strings.HasPrefix(tc.name, "vanadis URL not configured") {
				old := GetVanadisURL()
				SetVanadisURL("")
				defer SetVanadisURL(old)
			}
			err := ValidateLiveEventTarget(ctx, db, tc.source, tc.target)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ValidateLiveEventTarget(%q, %q) err = %v, wantErr = %v", tc.source, tc.target, err, tc.wantErr)
			}
			if tc.wantErrContains != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErrContains)) {
				t.Fatalf("ValidateLiveEventTarget(%q, %q) err = %v, want error containing %q", tc.source, tc.target, err, tc.wantErrContains)
			}
		})
	}
}

func testEventTriggersCRUD(ctx context.Context, t *testing.T, db *sql.DB) {
	t.Helper()
	now := time.Now().UTC()
	triggers := []EventTrigger{
		{ID: "et-1", ChannelID: "111111111111111111", Source: "flux", Target: "azylman/aerial", EventType: "sync_success", Prompt: "Flux sync finished", Effort: "low", CreatedAt: now},
		{ID: "et-2", ChannelID: "222222222222222222", Source: "vanadis_build", Target: "b-running", EventType: "completed", Prompt: "Build completed", CreatedAt: now},
		{ID: "et-3", ChannelID: "111111111111111111", Source: "nomad", Target: "aerial-brain", EventType: "deploy_success", Prompt: "Brain deployed", CreatedAt: now},
		{ID: "et-4", ChannelID: "111111111111111111", Source: "nomad", Target: "aerial-brain", EventType: "deploy_success", Prompt: "Brain deployed with defaults", Effort: "invalid-effort"},
		{ID: "et-5", ChannelID: "111111111111111111", Source: "camera", Target: "garage", EventType: "person_arrived", Prompt: "Standing camera watch garage", Effort: "medium", CreatedAt: now, Persistent: true},
		{ID: "et-6", ChannelID: "111111111111111111", Source: "camera", Target: "garage", EventType: "person_arrived", Prompt: "One-shot camera watch", Effort: "low", CreatedAt: now, Persistent: false},
	}
	for i, trig := range triggers {
		if err := InsertEventTrigger(ctx, db, trig); err != nil {
			t.Fatalf("InsertEventTrigger %d failed: %v", i+1, err)
		}
	}

	listTests := []struct {
		name      string
		channelID string
		wantCount int
	}{
		{"list all", "", 6},
		{"list channel 1", "111111111111111111", 5},
		{"list channel 2", "222222222222222222", 1},
		{"list non-existent channel", "999999999999999999", 0},
	}
	for _, tc := range listTests {
		t.Run("list_"+tc.name, func(t *testing.T) {
			res, err := ListEventTriggers(ctx, db, tc.channelID)
			if err != nil || len(res) != tc.wantCount {
				t.Fatalf("ListEventTriggers failed: err=%v, count=%d, want=%d", err, len(res), tc.wantCount)
			}
		})
	}

	delTests := []struct {
		name       string
		id         string
		wantDelete bool
		wantErr    bool
	}{
		{"delete existing", "et-1", true, false},
		{"delete already deleted", "et-1", false, false},
		{"delete non-existent", "et-missing", false, false},
		{"delete empty id", "", false, true},
	}
	for _, tc := range delTests {
		t.Run("delete_"+tc.name, func(t *testing.T) {
			deleted, err := DeleteEventTrigger(ctx, db, tc.id)
			if (err != nil) != tc.wantErr || deleted != tc.wantDelete {
				t.Fatalf("DeleteEventTrigger(%q) err=%v, wantErr=%v, deleted=%v, want=%v", tc.id, err, tc.wantErr, deleted, tc.wantDelete)
			}
		})
	}

	delSchedTests := []struct {
		name       string
		id         string
		wantDelete bool
	}{
		{"delete trigger via DeleteSchedule", "et-2", true},
		{"delete non-existent via DeleteSchedule", "et-missing-2", false},
	}
	for _, tc := range delSchedTests {
		t.Run("delete_sched_"+tc.name, func(t *testing.T) {
			deleted, err := DeleteSchedule(ctx, db, tc.id)
			if err != nil || deleted != tc.wantDelete {
				t.Fatalf("DeleteSchedule failed: err=%v, deleted=%v, want=%v", err, deleted, tc.wantDelete)
			}
		})
	}
}

func testClaimEventTriggers(ctx context.Context, t *testing.T, db *sql.DB) {
	t.Helper()
	claimTests := []struct {
		name      string
		source    string
		targets   []string
		eventType string
		wantCount int
		wantIDs   []string
	}{
		{"first claim returns both persistent and non-persistent", "camera", []string{"garage"}, "person_arrived", 2, []string{"et-5", "et-6"}},
		{"second claim within debounce window suppresses duplicate persistent trigger", "camera", []string{"garage"}, "person_arrived", 0, nil},
	}
	for _, tc := range claimTests {
		t.Run("claim_"+tc.name, func(t *testing.T) {
			claimed, err := ClaimEventTriggers(ctx, db, tc.source, tc.targets, tc.eventType)
			if err != nil {
				t.Fatalf("ClaimEventTriggers failed: %v", err)
			}
			if len(claimed) != tc.wantCount {
				t.Fatalf("ClaimEventTriggers count = %d, want %d", len(claimed), tc.wantCount)
			}
			for i, id := range tc.wantIDs {
				if claimed[i].ID != id {
					t.Errorf("claimed[%d].ID = %s, want %s", i, claimed[i].ID, id)
				}
			}
		})
	}

	if _, err := db.ExecContext(ctx, "UPDATE event_triggers SET updated_at = datetime('now', '-10 seconds') WHERE id = 'et-5'"); err != nil {
		t.Fatalf("failed to age updated_at: %v", err)
	}
	claimed, err := ClaimEventTriggers(ctx, db, "camera", []string{"garage"}, "person_arrived")
	if err != nil {
		t.Fatalf("ClaimEventTriggers after debounce failed: %v", err)
	}
	if len(claimed) != 1 || claimed[0].ID != "et-5" {
		t.Fatalf("ClaimEventTriggers after debounce count = %d, want 1 with et-5", len(claimed))
	}
}

func TestDB_EventTriggers(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()

	mockBuilds := map[string]string{
		"b-running":   "running",
		"b-completed": "completed",
		"b-failed":    "failed",
		"b-cancelled": "cancelled",
		"b-canceled":  "canceled",
	}
	mockChunks := map[string]string{
		"c-pending":   "pending",
		"c-merged":    "merged",
		"c-completed": "completed",
		"c-failed":    "failed",
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if after, ok := strings.CutPrefix(r.URL.Path, "/builds/"); ok {
			handleMockEntityStatus(w, "build", after, mockBuilds)
			return
		}
		if after, ok := strings.CutPrefix(r.URL.Path, "/chunks/"); ok {
			handleMockEntityStatus(w, "chunk", after, mockChunks)
			return
		}
		http.NotFound(w, r)
	}))
	defer ts.Close()
	oldVanadis := GetVanadisURL()
	SetVanadisURL(ts.URL)
	defer SetVanadisURL(oldVanadis)

	testValidateLiveEventTarget(ctx, t, db)
	testEventTriggersCRUD(ctx, t, db)
	testClaimEventTriggers(ctx, t, db)
}

func testEventWatchHandleWatchEvent(ctx context.Context, t *testing.T, db *sql.DB) {
	t.Helper()
	watchTests := []struct {
		name         string
		args         map[string]any
		extraSources []string
		wantErr      bool
	}{
		{
			name:    "missing channel_id",
			args:    map[string]any{"source": "flux", "target": "aerial", "event_type": "sync_success", "prompt": "ok"},
			wantErr: true,
		},
		{
			name:    "non-numeric snowflake channel_id",
			args:    map[string]any{"channel_id": "bad-id", "source": "flux", "target": "aerial", "event_type": "sync_success", "prompt": "ok"},
			wantErr: true,
		},
		{
			name:    "missing prompt",
			args:    map[string]any{"channel_id": "1562847268833337377", "source": "flux", "target": "aerial", "event_type": "sync_success"},
			wantErr: true,
		},
		{
			name:    "invalid effort",
			args:    map[string]any{"channel_id": "1562847268833337377", "source": "flux", "target": "aerial", "event_type": "sync_success", "prompt": "ok", "effort": "extreme"},
			wantErr: true,
		},
		{
			name:    "invalid spec",
			args:    map[string]any{"channel_id": "1562847268833337377", "source": "unknown", "target": "aerial", "event_type": "sync_success", "prompt": "ok"},
			wantErr: true,
		},
		{
			name:    "live validation failure build not found",
			args:    map[string]any{"channel_id": "1562847268833337377", "source": "vanadis_build", "target": "build-not-found", "event_type": "completed", "prompt": "ok"},
			wantErr: true,
		},
		{
			name:    "live validation failure build in terminal state",
			args:    map[string]any{"channel_id": "1562847268833337377", "source": "vanadis_build", "target": "build-term-1", "event_type": "completed", "prompt": "ok"},
			wantErr: true,
		},
		{
			name:    "success with defaults",
			args:    map[string]any{"channel_id": "1562847268833337377", "source": "flux", "target": "aerial", "event_type": "sync_success", "prompt": "deploy ready"},
			wantErr: false,
		},
		{
			name: "success with explicit fields and vanadis chunk",
			args: map[string]any{
				"channel_id": "1562847268833337378",
				"source":     "vanadis_chunk",
				"target":     "build-running-1-chunk-1",
				"event_type": "merged",
				"prompt":     "chunk 1 merged",
				"effort":     "high",
			},
			wantErr: false,
		},
		{
			name: "success with extra source and persistent true",
			args: map[string]any{
				"channel_id": "1562847268833337378",
				"source":     "camera",
				"target":     "garage",
				"event_type": "person_arrived",
				"prompt":     "garage person detected",
				"persistent": true,
			},
			extraSources: []string{"camera"},
			wantErr:      false,
		},
		{
			name: "success with thread_id fallback",
			args: map[string]any{
				"thread_id":  "1562847268833337379",
				"source":     "flux",
				"target":     "aerial",
				"event_type": "sync_success",
				"prompt":     "thread fallback test",
			},
			wantErr: false,
		},
	}

	for _, tc := range watchTests {
		t.Run("watch_"+tc.name, func(t *testing.T) {
			res, err := HandleWatchEvent(ctx, db, tc.args, tc.extraSources...)
			if (err != nil) != tc.wantErr {
				t.Fatalf("HandleWatchEvent() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr {
				if res == nil || len(res.Content) == 0 {
					t.Fatalf("expected non-empty CallToolResult")
				}
				if wantPers, ok := tc.args["persistent"].(bool); ok && wantPers {
					txt, isTxt := res.Content[0].(*mcp.TextContent)
					if !isTxt || !strings.Contains(txt.Text, `"persistent":true`) {
						t.Errorf("expected persistent:true in output, got %v", res.Content[0])
					}
				}
			}
		})
	}
}

func testEventWatchHandleToctou(ctx context.Context, t *testing.T, db *sql.DB) {
	t.Helper()
	toctouTests := []struct {
		name       string
		setupBuild string
		args       map[string]any
		wantErr    bool
	}{
		{
			name:       "trigger deletes inserted row on terminal race",
			setupBuild: "build-toctou-1",
			args: map[string]any{
				"channel_id": "1562847268833337377",
				"source":     "vanadis_build",
				"target":     "build-toctou-1",
				"event_type": "completed",
				"prompt":     "racing build",
			},
			wantErr: true,
		},
	}

	for _, tc := range toctouTests {
		t.Run("toctou_"+tc.name, func(t *testing.T) {
			_, err := HandleWatchEvent(ctx, db, tc.args)
			if (err != nil) != tc.wantErr {
				t.Fatalf("HandleWatchEvent TOCTOU err = %v, wantErr = %v", err, tc.wantErr)
			}
			var count int
			if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM event_triggers WHERE target = ?", tc.setupBuild).Scan(&count); err != nil {
				t.Fatalf("failed to count event triggers: %v", err)
			}
			if count != 0 {
				t.Errorf("expected 0 triggers remaining after TOCTOU cleanup, got %d", count)
			}
		})
	}
}

func testEventWatchHandleListAndCancel(ctx context.Context, t *testing.T, db *sql.DB) {
	t.Helper()
	listTests := []struct {
		name        string
		args        map[string]any
		wantAtLeast int
	}{
		{"list all watches", map[string]any{}, 2},
		{"list filtered channel", map[string]any{"channel_id": "1562847268833337377"}, 1},
		{"list filtered target_id alias", map[string]any{"target_id": "1562847268833337378"}, 1},
		{"list empty channel", map[string]any{"channel_id": "999999999999999999"}, 0},
	}
	for _, tc := range listTests {
		t.Run("list_"+tc.name, func(t *testing.T) {
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
				EventWatches []EventTrigger `json:"event_watches"`
				Count        int            `json:"count"`
			}
			if err := json.Unmarshal([]byte(text.Text), &out); err != nil {
				t.Fatalf("failed to unmarshal output: %v", err)
			}
			if len(out.EventWatches) < tc.wantAtLeast {
				t.Errorf("got %d event watches, want at least %d", len(out.EventWatches), tc.wantAtLeast)
			}
		})
	}

	_ = InsertEventTrigger(ctx, db, EventTrigger{ID: "watch-key-1", ChannelID: "1562847268833337377", Source: "flux", Target: "azylman/aerial", EventType: "sync_success", Prompt: "p1"})
	_ = InsertEventTrigger(ctx, db, EventTrigger{ID: "watch-key-2", ChannelID: "1562847268833337377", Source: "flux", Target: "azylman/aerial", EventType: "sync_success", Prompt: "p2"})
	_ = InsertEventTrigger(ctx, db, EventTrigger{ID: "watch-key-3", ChannelID: "1562847268833337377", Source: "flux", Target: "azylman/aerial", EventType: "sync_success", Prompt: "p3"})

	cancelTests := []struct {
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
	for _, tc := range cancelTests {
		t.Run("cancel_"+tc.name, func(t *testing.T) {
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

func TestEventWatchTools(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()

	mockBuilds := map[string]string{
		"build-running-1": "running",
		"build-term-1":    "completed",
	}
	mockChunks := map[string]string{
		"chunk-pending-1":         "pending",
		"chunk-term-1":            "merged",
		"build-running-1-chunk-1": "pending",
	}
	var toctouCalls int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if after, ok := strings.CutPrefix(r.URL.Path, "/builds/"); ok {
			id := after
			if id == "build-toctou-1" {
				toctouCalls++
				if toctouCalls == 1 {
					httpkit.WriteJSON(w, http.StatusOK, map[string]any{"build": map[string]string{"status": "running"}})
					return
				}
				httpkit.WriteJSON(w, http.StatusOK, map[string]any{"build": map[string]string{"status": "completed"}})
				return
			}
			if st, ok := mockBuilds[id]; ok {
				httpkit.WriteJSON(w, http.StatusOK, map[string]any{"build": map[string]string{"status": st}})
				return
			}
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		if after, ok := strings.CutPrefix(r.URL.Path, "/chunks/"); ok {
			id := after
			if st, ok := mockChunks[id]; ok {
				httpkit.WriteJSON(w, http.StatusOK, map[string]any{"chunk": map[string]string{"status": st}})
				return
			}
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		http.NotFound(w, r)
	}))
	defer ts.Close()
	oldVanadis := GetVanadisURL()
	SetVanadisURL(ts.URL)
	defer SetVanadisURL(oldVanadis)

	testEventWatchHandleWatchEvent(ctx, t, db)
	testEventWatchHandleToctou(ctx, t, db)
	testEventWatchHandleListAndCancel(ctx, t, db)
}

func verifyTriggerState(ctx context.Context, t *testing.T, db *sql.DB, ev pubsub.ExternalEvent, wantInDB bool) {
	t.Helper()
	matched, err := CheckEventTriggers(ctx, db, ev.Source, ev.Targets, ev.EventType)
	if err != nil {
		t.Fatalf("CheckEventTriggers failed: %v", err)
	}
	if matched != wantInDB {
		t.Errorf("trigger in db = %v, want %v", matched, wantInDB)
	}
}

func verifyDispatchedPrompt(t *testing.T, disp *mockDispatcher, wantChannel, wantPrompt string) {
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
}

func TestHandleExternalEvent_Scenarios(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()

	_ = InsertEventTrigger(ctx, db, EventTrigger{
		ID:        "trig-ext-1",
		ChannelID: "100000000000000001",
		Source:    "camera",
		Target:    "driveway",
		EventType: "vehicle_detected",
		Prompt:    "Vehicle in driveway",
		Effort:    "low",
	})

	_ = InsertEventTrigger(ctx, db, EventTrigger{
		ID:        "trig-ext-fail",
		ChannelID: "100000000000000001",
		Source:    "camera",
		Target:    "backyard",
		EventType: "motion",
		Prompt:    "Motion in backyard",
		Effort:    "low",
	})

	_ = InsertEventTrigger(ctx, db, EventTrigger{
		ID:        "trig-ext-healthcheck",
		ChannelID: "100000000000000001",
		Source:    "healthcheck",
		Target:    "ping",
		EventType: "ping",
		Prompt:    "Healthcheck ping",
		Effort:    "low",
	})

	closedDB, err := OpenDB(&Config{DatabaseURL: ":memory:"})
	if err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}
	_ = closedDB.Close()
	serverClosed := NewServer(NewToolHandler(&Config{DatabaseURL: ":memory:"}, closedDB))

	mockDispFail := &mockDispatcher{err: errors.New("eri unavailable")}
	mockDispOK := &mockDispatcher{response: PromptDispatchResponse{Status: "ok", MessageID: "msg-ext-1"}}
	serverOK := NewServer(NewToolHandler(&Config{DatabaseURL: ":memory:"}, db), mockDispOK)
	serverFail := NewServer(NewToolHandler(&Config{DatabaseURL: ":memory:"}, db), mockDispFail)

	tests := []struct {
		name      string
		server    *Server
		ev        pubsub.ExternalEvent
		wantErr   bool
		checkDBID string
		wantInDB  bool
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
			name:      "successful dispatch claims and fires trigger",
			server:    serverOK,
			ev:        pubsub.ExternalEvent{Source: "camera", EventType: "vehicle_detected", Targets: []string{"driveway"}, ContextHeader: "Camera Alert"},
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
			if tc.name == "successful dispatch claims and fires trigger" {
				verifyDispatchedPrompt(t, mockDispOK, "100000000000000001", "Camera Alert\n\nVehicle in driveway")
			}
		})
	}
}

type ackSubscribeBus struct {
	pubsub.Bus

	lastErr   error
	onHandled func()
}

func (a *ackSubscribeBus) Subscribe(ctx context.Context, subject, durable string, handler pubsub.Handler) (pubsub.Subscription, error) {
	return a.Bus.Subscribe(ctx, subject, durable, func(mCtx context.Context, msg pubsub.Message) error {
		defer a.onHandled()
		err := handler(mCtx, msg)
		a.lastErr = err
		return err
	})
}

func TestHaro_EventsConsumerIntegration(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()

	_ = InsertEventTrigger(ctx, db, EventTrigger{
		ID:        "trig-int-1",
		ChannelID: "100000000000000001",
		Source:    "flux",
		Target:    "aerial",
		EventType: "sync_success",
		Prompt:    "Deploy succeeded",
		Effort:    "low",
	})

	done := make(chan struct{})
	mockDisp := &mockDispatcher{response: PromptDispatchResponse{Status: "ok", MessageID: "msg-ext-int"}}
	server := NewServer(NewToolHandler(&Config{DatabaseURL: ":memory:"}, db), mockDisp)

	memBus := pubsub.NewMemoryBus()
	defer memBus.Close()

	if err := pubsub.EnsureCoreStreams(ctx, memBus); err != nil {
		t.Fatalf("EnsureCoreStreams: %v", err)
	}

	wrapped := &ackSubscribeBus{Bus: memBus, onHandled: func() { close(done) }}
	bus := initHaroJetStreamWithFactory(ctx, server, db, "nats://mock", func(_ context.Context, _ string) (pubsub.Bus, error) {
		return wrapped, nil
	})
	if bus == nil {
		t.Fatal("expected non-nil bus")
	}

	ev := pubsub.ExternalEvent{
		Source:        "flux",
		EventType:     "sync_success",
		Targets:       []string{"aerial"},
		ContextHeader: "Deployment update",
	}
	if err := memBus.Publish(ctx, "aerial.events.flux.sync_success", "ev-1", ev); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for event delivery")
	}

	matched, err := CheckEventTriggers(ctx, db, "flux", []string{"aerial"}, "sync_success")
	if err != nil {
		t.Fatalf("CheckEventTriggers failed: %v", err)
	}
	if matched {
		t.Error("expected trigger to have been claimed and deleted from db")
	}
	if wrapped.lastErr != nil {
		t.Errorf("expected nil error on successful event handling, got %v", wrapped.lastErr)
	}
}

type handlerCaptureBus struct {
	pubsub.Bus

	captured pubsub.Handler
}

func (c *handlerCaptureBus) Subscribe(ctx context.Context, subject, durable string, handler pubsub.Handler) (pubsub.Subscription, error) {
	c.captured = handler
	return c.Bus.Subscribe(ctx, subject, durable, handler)
}

func TestHaro_InitJetStreamEdgeCases(t *testing.T) {
	ctx := context.Background()

	memBus := pubsub.NewMemoryBus()
	defer memBus.Close()

	// Test handler delivery retry capping
	closedDB, err := OpenDB(&Config{DatabaseURL: ":memory:"})
	if err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}
	_ = closedDB.Close()
	serverClosed := NewServer(NewToolHandler(&Config{DatabaseURL: ":memory:"}, closedDB))

	captureBus := &handlerCaptureBus{Bus: memBus}
	bus := initHaroJetStreamWithFactory(ctx, serverClosed, nil, "nats://mock", func(_ context.Context, _ string) (pubsub.Bus, error) {
		return captureBus, nil
	})
	if bus == nil || captureBus.captured == nil {
		t.Fatal("expected captured subscribe handler")
	}

	evBytes, _ := json.Marshal(pubsub.ExternalEvent{
		EventID:   "ev-retry-test",
		Source:    "camera",
		EventType: "motion",
		Targets:   []string{"driveway"},
	})

	// When attempts < 5 and handler fails on DB error, returns error (NAK)
	if err := captureBus.captured(ctx, &memoryMsg{data: evBytes, numDelivered: 1}); err == nil {
		t.Error("expected error for numDelivered < 5 on DB failure")
	}

	// When attempts >= 5 and handler fails, drops and returns nil
	if err := captureBus.captured(ctx, &memoryMsg{data: evBytes, numDelivered: 5}); err != nil {
		t.Errorf("expected nil error for numDelivered >= 5 to drop poison event, got %v", err)
	}

	// Bad JSON payload returns nil to avoid poison redelivery loop
	if err := captureBus.captured(ctx, &memoryMsg{data: []byte("invalid-json"), numDelivered: 1}); err != nil {
		t.Errorf("expected nil error for bad json to avoid poison loop, got %v", err)
	}
}

func TestVanadisURL_Precedence(t *testing.T) {
	oldVanadis := GetVanadisURL()
	defer SetVanadisURL(oldVanadis)

	tests := []struct {
		name    string
		setURL  string
		wantURL string
	}{
		{"trailing slash stripped from explicit", "http://explicit.vanadis/api/", "http://explicit.vanadis/api"},
		{"empty when not configured", "", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			SetVanadisURL(tc.setURL)
			got := GetVanadisURL()
			if got != tc.wantURL {
				t.Errorf("GetVanadisURL() = %q, want %q", got, tc.wantURL)
			}
		})
	}
}
