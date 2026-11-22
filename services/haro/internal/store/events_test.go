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

// vanadisStatusServer serves /builds/<id> and /chunks/<id> status lookups and records the last
// Authorization header. Ids ending in -500 or -badjson return an error status or a broken body.
type vanadisStatusServer struct {
	URL      string
	mu       sync.Mutex
	lastAuth string
}

func (s *vanadisStatusServer) LastAuth() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastAuth
}

// newVanadisStatusServer starts the fake and points the package's Vanadis URL at it until cleanup.
func newVanadisStatusServer(t *testing.T) *vanadisStatusServer {
	t.Helper()
	builds := map[string]string{"b-running": "running", "b-completed": "completed", "b-failed": "failed", "b-cancelled": "cancelled", "b-canceled": "canceled"}
	chunks := map[string]string{"c-pending": "pending", "c-merged": "merged", "c-completed": "completed", "c-failed": "failed"}
	s := &vanadisStatusServer{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.lastAuth = r.Header.Get("Authorization")
		s.mu.Unlock()
		if after, ok := strings.CutPrefix(r.URL.Path, "/builds/"); ok {
			handleMockEntityStatus(w, "build", after, builds)
			return
		}
		if after, ok := strings.CutPrefix(r.URL.Path, "/chunks/"); ok {
			handleMockEntityStatus(w, "chunk", after, chunks)
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(ts.Close)
	s.URL = ts.URL
	setVanadisTestURL(t, ts.URL)
	return s
}

// closedVanadisURL in a row's vanadisURL expands to the URL of a server that was started and closed,
// so the lookup gets connection refused without a fixed port.
const closedVanadisURL = "{closed}"

func newClosedVanadisURL(t *testing.T) string {
	t.Helper()
	ts := httptest.NewServer(http.NotFoundHandler())
	ts.Close()
	return ts.URL
}

func setVanadisTestURL(t *testing.T, u string) {
	t.Helper()
	old := GetVanadisURL()
	SetVanadisURL(u)
	t.Cleanup(func() { SetVanadisURL(old) })
}

func setVanadisTestToken(t *testing.T, token string) {
	t.Helper()
	tokenPath := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenPath, []byte(token), 0o600); err != nil {
		t.Fatalf("failed to write mock token file: %v", err)
	}
	oldTS := GetVanadisTokenSource()
	SetVanadisTokenPath(tokenPath)
	t.Cleanup(func() { SetVanadisTokenSource(oldTS) })
}

func TestValidateLiveEventTarget(t *testing.T) {
	db := openStoreTestDB(t)
	ctx := context.Background()
	tests := []struct {
		name            string
		source          string
		target          string
		token           string
		vanadisURL      string
		noVanadis       bool
		wantAuth        string
		wantErr         bool
		wantErrContains string
	}{
		{name: "vanadis_build running", source: "vanadis_build", target: "b-running"},
		{name: "vanadis_build bearer auth", source: "vanadis_build", target: "b-running", token: "mock-token-build", wantAuth: "Bearer mock-token-build"},
		{name: "vanadis_build completed", source: "vanadis_build", target: "b-completed", wantErr: true},
		{name: "vanadis_build failed", source: "vanadis_build", target: "b-failed", wantErr: true},
		{name: "vanadis_build cancelled", source: "vanadis_build", target: "b-cancelled", wantErr: true},
		{name: "vanadis_build canceled", source: "vanadis_build", target: "b-canceled", wantErr: true},
		{name: "vanadis_build not found", source: "vanadis_build", target: "b-missing", wantErr: true},
		{name: "vanadis_build 500 error", source: "vanadis_build", target: "b-500", wantErr: true},
		{name: "vanadis_build bad json", source: "vanadis_build", target: "b-badjson", wantErr: true},
		{name: "vanadis_chunk pending", source: "vanadis_chunk", target: "c-pending"},
		{name: "vanadis_chunk bearer auth", source: "vanadis_chunk", target: "c-pending", token: "mock-token-chunk", wantAuth: "Bearer mock-token-chunk"},
		{name: "vanadis_chunk merged", source: "vanadis_chunk", target: "c-merged", wantErr: true},
		{name: "vanadis_chunk completed", source: "vanadis_chunk", target: "c-completed", wantErr: true},
		{name: "vanadis_chunk failed", source: "vanadis_chunk", target: "c-failed", wantErr: true},
		{name: "vanadis_chunk not found", source: "vanadis_chunk", target: "c-missing", wantErr: true},
		{name: "vanadis_chunk 500 error", source: "vanadis_chunk", target: "c-500", wantErr: true},
		{name: "vanadis_chunk bad json", source: "vanadis_chunk", target: "c-badjson", wantErr: true},
		{name: "empty target", source: "vanadis_build", target: "   ", wantErr: true},
		{name: "other source flux", source: "flux", target: "example-org/repo"},
		{name: "other source github", source: "github", target: "example-org/repo:main"},
		{name: "other source nomad", source: "nomad", target: "app-alpha"},
		{name: "vanadis URL not configured build", source: "vanadis_build", target: "b-running", noVanadis: true, wantErr: true, wantErrContains: "vanadis URL is not configured"},
		{name: "vanadis URL not configured chunk", source: "vanadis_chunk", target: "c-pending", noVanadis: true, wantErr: true, wantErrContains: "vanadis URL is not configured"},
		{name: "vanadis unreachable build", source: "vanadis_build", target: "b-running", vanadisURL: closedVanadisURL, wantErr: true, wantErrContains: "failed to query"},
		{name: "vanadis malformed URL build", source: "vanadis_build", target: "b-running", vanadisURL: "://bad-url", wantErr: true, wantErrContains: "create vanadis build request"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := newVanadisStatusServer(t)
			if tc.token != "" {
				setVanadisTestToken(t, tc.token)
			}
			if tc.vanadisURL != "" || tc.noVanadis {
				setVanadisTestURL(t, strings.ReplaceAll(tc.vanadisURL, closedVanadisURL, newClosedVanadisURL(t)))
			}
			err := ValidateLiveEventTarget(ctx, db, tc.source, tc.target)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ValidateLiveEventTarget(%q, %q) err = %v, wantErr = %v", tc.source, tc.target, err, tc.wantErr)
			}
			if tc.wantErrContains != "" && !strings.Contains(err.Error(), tc.wantErrContains) {
				t.Fatalf("ValidateLiveEventTarget(%q, %q) err = %v, want error containing %q", tc.source, tc.target, err, tc.wantErrContains)
			}
			if got := srv.LastAuth(); tc.wantAuth != "" && got != tc.wantAuth {
				t.Fatalf("ValidateLiveEventTarget(%q, %q) auth header = %q, want %q", tc.source, tc.target, got, tc.wantAuth)
			}
		})
	}
}

func seedEventTriggers(ctx context.Context, t *testing.T, db *sql.DB) {
	t.Helper()
	now := time.Now().UTC()
	triggers := []model.EventTrigger{
		{ID: "et-1", ChannelID: "111111111111111111", Source: "flux", Target: "example-org/repo", EventType: "sync_success", Prompt: "Flux sync finished", Effort: "low", CreatedAt: now},
		{ID: "et-2", ChannelID: "222222222222222222", Source: "vanadis_build", Target: "b-running", EventType: "completed", Prompt: "Build completed", CreatedAt: now},
		{ID: "et-3", ChannelID: "111111111111111111", Source: "nomad", Target: "app-alpha", EventType: "deploy_success", Prompt: "App deployed", CreatedAt: now},
		{ID: "et-4", ChannelID: "111111111111111111", Source: "nomad", Target: "app-alpha", EventType: "deploy_success", Prompt: "App deployed with defaults", Effort: "invalid-effort"},
		{ID: "et-5", ChannelID: "111111111111111111", Source: "camera", Target: "garage", EventType: "person_arrived", Prompt: "Standing camera watch garage", Effort: "medium", CreatedAt: now, Persistent: true},
		{ID: "et-6", ChannelID: "111111111111111111", Source: "camera", Target: "garage", EventType: "person_arrived", Prompt: "One-shot camera watch", Effort: "low", CreatedAt: now},
	}
	for i, trig := range triggers {
		if err := InsertEventTrigger(ctx, db, trig); err != nil {
			t.Fatalf("InsertEventTrigger %d failed: %v", i+1, err)
		}
	}
}

func TestListEventTriggersByChannel(t *testing.T) {
	db := openStoreTestDB(t)
	ctx := context.Background()
	seedEventTriggers(ctx, t, db)
	tests := []struct {
		name      string
		channelID string
		wantCount int
	}{
		{"list all", "", 6},
		{"list channel 1", "111111111111111111", 5},
		{"list channel 2", "222222222222222222", 1},
		{"list non-existent channel", "999999999999999999", 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := ListEventTriggers(ctx, db, tc.channelID)
			if err != nil || len(res) != tc.wantCount {
				t.Fatalf("ListEventTriggers failed: err=%v, count=%d, want=%d", err, len(res), tc.wantCount)
			}
		})
	}
}

// TestDeleteEventTrigger rows run in order against one database ("already deleted" follows "existing").
func TestDeleteEventTrigger(t *testing.T) {
	db := openStoreTestDB(t)
	ctx := context.Background()
	seedEventTriggers(ctx, t, db)
	tests := []struct {
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
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			deleted, err := DeleteEventTrigger(ctx, db, tc.id)
			if (err != nil) != tc.wantErr || deleted != tc.wantDelete {
				t.Fatalf("DeleteEventTrigger(%q) err=%v, wantErr=%v, deleted=%v, want=%v", tc.id, err, tc.wantErr, deleted, tc.wantDelete)
			}
		})
	}
}

func TestDeleteScheduleEventTrigger(t *testing.T) {
	db := openStoreTestDB(t)
	ctx := context.Background()
	seedEventTriggers(ctx, t, db)
	tests := []struct {
		name       string
		id         string
		wantDelete bool
	}{
		{"delete trigger via DeleteSchedule", "et-2", true},
		{"delete non-existent via DeleteSchedule", "et-missing-2", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			deleted, err := DeleteSchedule(ctx, db, tc.id)
			if err != nil || deleted != tc.wantDelete {
				t.Fatalf("DeleteSchedule failed: err=%v, deleted=%v, want=%v", err, deleted, tc.wantDelete)
			}
		})
	}
}

// TestClaimEventTriggers rows run in order against one database: the debounce window depends on the
// previous claim, and ageSQL moves a trigger's updated_at out of the window before claiming.
func TestClaimEventTriggers(t *testing.T) {
	db := openStoreTestDB(t)
	ctx := context.Background()
	seedEventTriggers(ctx, t, db)
	tests := []struct {
		name    string
		ageSQL  string
		wantIDs []string
	}{
		{name: "first claim returns both persistent and non-persistent", wantIDs: []string{"et-5", "et-6"}},
		{name: "second claim within debounce window suppresses duplicate persistent trigger"},
		{
			name:    "claim after debounce window returns persistent trigger again",
			ageSQL:  "UPDATE event_triggers SET updated_at = datetime('now', '-10 seconds') WHERE id = 'et-5'",
			wantIDs: []string{"et-5"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.ageSQL != "" {
				if _, err := db.ExecContext(ctx, tc.ageSQL); err != nil {
					t.Fatalf("failed to age updated_at: %v", err)
				}
			}
			claimed, err := ClaimEventTriggers(ctx, db, "camera", []string{"garage"}, "person_arrived")
			if err != nil {
				t.Fatalf("ClaimEventTriggers failed: %v", err)
			}
			ids := make([]string, 0, len(claimed))
			for _, c := range claimed {
				ids = append(ids, c.ID)
			}
			if strings.Join(ids, ",") != strings.Join(tc.wantIDs, ",") {
				t.Fatalf("ClaimEventTriggers IDs = %v, want %v", ids, tc.wantIDs)
			}
		})
	}
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
		name    string
		seedSQL string
		nilDB   bool
		wantErr string
	}{
		{name: "nil db errors", nilDB: true, wantErr: "database is nil"},
		{name: "missing table fails select", seedSQL: `DROP TABLE event_triggers`, wantErr: "no such table"},
		{
			name:    "bad row fails scan",
			seedSQL: `INSERT INTO event_triggers (id, channel_id, source, target, event_type, prompt, effort, created_at, persistent) VALUES ('bad-1', 'c', 'camera', 'garage', 'person_arrived', 'p', 'low', 'not-a-date', FALSE)`,
			wantErr: "Scan error",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openStoreTestDB(t)
			ctx := context.Background()
			if tc.seedSQL != "" {
				if _, err := db.ExecContext(ctx, tc.seedSQL); err != nil {
					t.Fatalf("seed SQL err = %v", err)
				}
			}
			_, err := ClaimEventTriggers(ctx, scheduleCaseDB(db, tc.nilDB, false), "camera", []string{"garage"}, "person_arrived")
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
