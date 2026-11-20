package store

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/services/haro/internal/model"
)

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

func setupTestTargetEnv(t *testing.T, tcName, token string) {
	t.Helper()
	if token != "" {
		tokenPath := filepath.Join(t.TempDir(), "token")
		if err := os.WriteFile(tokenPath, []byte(token), 0o600); err != nil {
			t.Fatalf("failed to write mock token file: %v", err)
		}
		oldTS := GetVanadisTokenSource()
		SetVanadisTokenPath(tokenPath)
		t.Cleanup(func() { SetVanadisTokenSource(oldTS) })
	}
	switch {
	case strings.HasPrefix(tcName, "vanadis URL not configured"):
		old := GetVanadisURL()
		SetVanadisURL("")
		t.Cleanup(func() { SetVanadisURL(old) })
	case strings.HasPrefix(tcName, "vanadis unreachable"):
		old := GetVanadisURL()
		SetVanadisURL("http://127.0.0.1:1")
		t.Cleanup(func() { SetVanadisURL(old) })
	case strings.HasPrefix(tcName, "vanadis malformed URL"):
		old := GetVanadisURL()
		SetVanadisURL("://bad-url")
		t.Cleanup(func() { SetVanadisURL(old) })
	}
}

func testValidateLiveEventTarget(ctx context.Context, t *testing.T, db *sql.DB, getLastAuth ...func() string) {
	t.Helper()
	var authGetter func() string
	if len(getLastAuth) > 0 {
		authGetter = getLastAuth[0]
	}
	validateTests := []struct {
		name            string
		source          string
		target          string
		token           string
		wantAuth        string
		wantErr         bool
		wantErrContains string
	}{
		{"vanadis_build running", "vanadis_build", "b-running", "", "", false, ""},
		{"vanadis_build bearer auth", "vanadis_build", "b-running", "mock-token-build", "Bearer mock-token-build", false, ""},
		{"vanadis_build completed", "vanadis_build", "b-completed", "", "", true, ""},
		{"vanadis_build failed", "vanadis_build", "b-failed", "", "", true, ""},
		{"vanadis_build cancelled", "vanadis_build", "b-cancelled", "", "", true, ""},
		{"vanadis_build canceled", "vanadis_build", "b-canceled", "", "", true, ""},
		{"vanadis_build not found", "vanadis_build", "b-missing", "", "", true, ""},
		{"vanadis_build 500 error", "vanadis_build", "b-500", "", "", true, ""},
		{"vanadis_build bad json", "vanadis_build", "b-badjson", "", "", true, ""},
		{"vanadis_chunk pending", "vanadis_chunk", "c-pending", "", "", false, ""},
		{"vanadis_chunk bearer auth", "vanadis_chunk", "c-pending", "mock-token-chunk", "Bearer mock-token-chunk", false, ""},
		{"vanadis_chunk merged", "vanadis_chunk", "c-merged", "", "", true, ""},
		{"vanadis_chunk completed", "vanadis_chunk", "c-completed", "", "", true, ""},
		{"vanadis_chunk failed", "vanadis_chunk", "c-failed", "", "", true, ""},
		{"vanadis_chunk not found", "vanadis_chunk", "c-missing", "", "", true, ""},
		{"vanadis_chunk 500 error", "vanadis_chunk", "c-500", "", "", true, ""},
		{"vanadis_chunk bad json", "vanadis_chunk", "c-badjson", "", "", true, ""},
		{"empty target", "vanadis_build", "   ", "", "", true, ""},
		{"other source flux", "flux", "azylman/aerial", "", "", false, ""},
		{"other source github", "github", "azylman/aerial:main", "", "", false, ""},
		{"other source nomad", "nomad", "aerial-brain", "", "", false, ""},
		{"vanadis URL not configured build", "vanadis_build", "b-running", "", "", true, "vanadis URL is not configured"},
		{"vanadis URL not configured chunk", "vanadis_chunk", "c-pending", "", "", true, "vanadis URL is not configured"},
		{"vanadis unreachable build", "vanadis_build", "b-running", "", "", true, "failed to query"},
		{"vanadis malformed URL build", "vanadis_build", "b-running", "", "", true, "create vanadis build request"},
	}
	for _, tc := range validateTests {
		t.Run("validate_"+tc.name, func(t *testing.T) {
			setupTestTargetEnv(t, tc.name, tc.token)
			err := ValidateLiveEventTarget(ctx, db, tc.source, tc.target)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ValidateLiveEventTarget(%q, %q) err = %v, wantErr = %v", tc.source, tc.target, err, tc.wantErr)
			}
			if tc.wantErrContains != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErrContains)) {
				t.Fatalf("ValidateLiveEventTarget(%q, %q) err = %v, want error containing %q", tc.source, tc.target, err, tc.wantErrContains)
			}
			if tc.wantAuth != "" && authGetter != nil {
				if got := authGetter(); got != tc.wantAuth {
					t.Fatalf("ValidateLiveEventTarget(%q, %q) auth header = %q, want %q", tc.source, tc.target, got, tc.wantAuth)
				}
			}
		})
	}
}

func testEventTriggersCRUD(ctx context.Context, t *testing.T, db *sql.DB) {
	t.Helper()
	now := time.Now().UTC()
	triggers := []model.EventTrigger{
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
	db := openStoreTestDB(t)
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
	var (
		authMu         sync.Mutex
		lastAuthHeader string
	)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authMu.Lock()
		lastAuthHeader = r.Header.Get("Authorization")
		authMu.Unlock()
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

	testValidateLiveEventTarget(ctx, t, db, func() string {
		authMu.Lock()
		defer authMu.Unlock()
		return lastAuthHeader
	})
	testEventTriggersCRUD(ctx, t, db)
	testClaimEventTriggers(ctx, t, db)
}

func TestInsertEventTriggerErrors(t *testing.T) {
	tests := []struct {
		name    string
		trigger model.EventTrigger
		nilDB   bool
		wantErr string
	}{
		{"nil db errors", model.EventTrigger{ID: "e-1"}, true, "database is nil"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openStoreTestDB(t)
			database := db
			if tc.nilDB {
				database = nil
			}
			err := InsertEventTrigger(context.Background(), database, tc.trigger)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("InsertEventTrigger err = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

func TestListEventTriggersErrors(t *testing.T) {
	tests := []struct {
		name      string
		channelID string
		nilDB     bool
		dropTable bool
		wantErr   string
	}{
		{"nil db errors", "", true, false, "database is nil"},
		{"missing table fails query", "", false, true, "no such table"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openStoreTestDB(t)
			ctx := context.Background()
			if tc.dropTable {
				if _, err := db.ExecContext(ctx, `DROP TABLE event_triggers`); err != nil {
					t.Fatalf("drop table err = %v", err)
				}
			}
			database := db
			if tc.nilDB {
				database = nil
			}
			_, err := ListEventTriggers(ctx, database, tc.channelID)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("ListEventTriggers err = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

func TestClaimEventTriggersErrors(t *testing.T) {
	tests := []struct {
		name      string
		source    string
		targets   []string
		eventType string
		nilDB     bool
		wantErr   string
	}{
		{"nil db errors", "camera", []string{"garage"}, "person_arrived", true, "database is nil"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openStoreTestDB(t)
			ctx := context.Background()
			database := db
			if tc.nilDB {
				database = nil
			}
			_, err := ClaimEventTriggers(ctx, database, tc.source, tc.targets, tc.eventType)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("ClaimEventTriggers err = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

func TestBuildEventTriggerFilterArgs(t *testing.T) {
	tests := []struct {
		name             string
		source           string
		targets          []string
		eventType        string
		wantPlaceholders string
		wantArgs         []any
		wantOK           bool
	}{
		{"empty source rejects", "", []string{"garage"}, "person_arrived", "", nil, false},
		{"empty event type rejects", "camera", []string{"garage"}, "", "", nil, false},
		{"nil targets rejects", "camera", nil, "person_arrived", "", nil, false},
		{"blank targets reject", "camera", []string{"", "   "}, "person_arrived", "", nil, false},
		{"single target builds", "camera", []string{"garage"}, "person_arrived", "$3", []any{"camera", "person_arrived", "garage"}, true},
		{"trims lowercases dedups", "Camera", []string{" garage ", "garage", "", "driveway"}, " Person_Arrived ", "$3, $4", []any{"camera", "person_arrived", "garage", "driveway"}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			placeholders, args, ok := BuildEventTriggerFilterArgs(tc.source, tc.targets, tc.eventType)
			if placeholders != tc.wantPlaceholders || !reflect.DeepEqual(args, tc.wantArgs) || ok != tc.wantOK {
				t.Errorf("BuildEventTriggerFilterArgs(%q, %q, %q) = (%q, %v, %v), want (%q, %v, %v)",
					tc.source, tc.targets, tc.eventType, placeholders, args, ok,
					tc.wantPlaceholders, tc.wantArgs, tc.wantOK)
			}
		})
	}
}

func TestClaimEventTriggersStorageErrors(t *testing.T) {
	tests := []struct {
		name      string
		dropTable bool
		wantErr   string
	}{
		{"missing table fails select", true, "no such table"},
		{"bad row fails scan", false, "Scan error"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openStoreTestDB(t)
			ctx := context.Background()
			if tc.dropTable {
				if _, err := db.ExecContext(ctx, `DROP TABLE event_triggers`); err != nil {
					t.Fatalf("drop table err = %v", err)
				}
			} else {
				_, err := db.ExecContext(ctx, `INSERT INTO event_triggers (id, channel_id, source, target, event_type, prompt, effort, created_at, persistent) VALUES ('bad-1', 'c', 'camera', 'garage', 'person_arrived', 'p', 'low', 'not-a-date', FALSE)`)
				if err != nil {
					t.Fatalf("insert bad row err = %v", err)
				}
			}
			_, err := ClaimEventTriggers(ctx, db, "camera", []string{"garage"}, "person_arrived")
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("ClaimEventTriggers err = %v, want %q", err, tc.wantErr)
			}
		})
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

func TestGetEventTrigger(t *testing.T) {
	now := time.Now().UTC()
	tests := []struct {
		name      string
		id        string
		seed      bool
		seedAgent string
		nilDB     bool
		wantAgent string
		wantErr   string
	}{
		{name: "found trigger", id: "et-found", seed: true, seedAgent: "agent-alpha", wantAgent: "agent-alpha"},
		{name: "empty id errors", id: "   ", wantErr: "id cannot be empty"},
		{name: "nil db errors", id: "et-1", nilDB: true, wantErr: "database is nil"},
		{name: "not found trigger", id: "et-missing", wantErr: `event watch "et-missing" not found`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openStoreTestDB(t)
			ctx := context.Background()
			if tc.seed {
				if err := InsertEventTrigger(ctx, db, model.EventTrigger{ID: tc.id, ChannelID: "ch", Source: "flux", Target: "t", EventType: "ev", Prompt: "p", CreatedAt: now, Agent: tc.seedAgent}); err != nil {
					t.Fatalf("InsertEventTrigger err = %v", err)
				}
			}
			database := db
			if tc.nilDB {
				database = nil
			}
			trig, err := GetEventTrigger(ctx, database, tc.id)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("GetEventTrigger err = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("GetEventTrigger err = %v", err)
			}
			if trig.Agent != tc.wantAgent {
				t.Errorf("trig.Agent = %q, want %q", trig.Agent, tc.wantAgent)
			}
		})
	}
}

func TestListEventTriggers_MultiAgent(t *testing.T) {
	now := time.Now().UTC()
	db := openStoreTestDB(t)
	ctx := context.Background()

	_ = InsertEventTrigger(ctx, db, model.EventTrigger{ID: "et-a", ChannelID: "ch", Source: "flux", Target: "t1", EventType: "ev", Prompt: "p1", CreatedAt: now, Agent: "agent-alpha"})
	_ = InsertEventTrigger(ctx, db, model.EventTrigger{ID: "et-b", ChannelID: "ch", Source: "flux", Target: "t2", EventType: "ev", Prompt: "p2", CreatedAt: now, Agent: "agent-beta"})

	tests := []struct {
		name        string
		agentFilter string
		wantCount   int
	}{
		{"filter agent-alpha", "agent-alpha", 1},
		{"filter agent-beta", "agent-beta", 1},
		{"wildcard all", "*", 2},
		{"empty all", "", 2},
		{"non-existent", "agent-none", 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ListEventTriggers(ctx, db, "ch", tc.agentFilter)
			if err != nil {
				t.Fatalf("ListEventTriggers err = %v", err)
			}
			if len(got) != tc.wantCount {
				t.Errorf("ListEventTriggers count = %d, want %d", len(got), tc.wantCount)
			}
		})
	}
}
