package interactions

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"

	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/db/dbtest"
	"github.com/azylman/aerial/services/eri/pkg/metrics"
	"github.com/azylman/aerial/services/eri/pkg/queue"
)

type capturedRequest struct {
	Method string
	URL    string
	Body   []byte
}

type mockTransport struct {
	mu       sync.Mutex
	requests []capturedRequest
	respCode int
	respBody string
}

func (m *mockTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var bodyBytes []byte
	if req.Body != nil {
		bodyBytes, _ = io.ReadAll(req.Body)
		req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
	}
	m.requests = append(m.requests, capturedRequest{Method: req.Method, URL: req.URL.String(), Body: bodyBytes})
	code, body := m.respCode, m.respBody
	if code == 0 {
		code = http.StatusOK
	}
	if body == "" {
		if strings.Contains(req.URL.Path, "commands") {
			body = `[]`
		} else {
			body = `{"id":"dummy-id"}`
		}
	}
	if code >= 400 {
		return &http.Response{
			StatusCode: code,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"message":"simulated error","code":500}`)),
		}, nil
	}
	return &http.Response{
		StatusCode: code,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}, nil
}

func (m *mockTransport) Requests() []capturedRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]capturedRequest, len(m.requests))
	copy(cp, m.requests)
	return cp
}

func newMockDiscordSession(transport *mockTransport) *discordgo.Session {
	s, _ := discordgo.New("Bot mock-token")
	s.Client = &http.Client{Transport: transport}
	s.State = discordgo.NewState()
	s.State.User = &discordgo.User{ID: "bot-123", Username: "Aerial"}
	return s
}

func setupTestRouter(adminID string, httpStatus int) (*Router, *config.Config, *mockTransport, *discordgo.Session) {
	transport := &mockTransport{respCode: httpStatus}
	cfg := config.NewFromData(&config.ConfigData{
		AdminUsers: []string{adminID},
		Channels:   map[string]config.ChannelPolicy{"chan-1": {Mode: "thread"}, "chan-2": {Mode: "mention"}},
	})
	return NewRouter(InteractionDeps{Config: cfg, MemoryURL: "http://127.0.0.1:1"}), cfg, transport, newMockDiscordSession(transport)
}

func mkOpt(name string, optType discordgo.ApplicationCommandOptionType, val any) *discordgo.ApplicationCommandInteractionDataOption {
	return &discordgo.ApplicationCommandInteractionDataOption{Name: name, Type: optType, Value: val}
}

func mkCmd(id, channelID, userID, name string, opts ...*discordgo.ApplicationCommandInteractionDataOption) *discordgo.InteractionCreate {
	ic := &discordgo.InteractionCreate{
		Interaction: &discordgo.Interaction{
			ID: id, ChannelID: channelID, Type: discordgo.InteractionApplicationCommand, Token: "token-" + id,
			Data: discordgo.ApplicationCommandInteractionData{Name: name, Options: opts},
		},
	}
	if userID != "" {
		ic.Member = &discordgo.Member{User: &discordgo.User{ID: userID, Username: "user-" + userID}}
	}
	return ic
}

func mkDMCmd(id, userID, name string, opts ...*discordgo.ApplicationCommandInteractionDataOption) *discordgo.InteractionCreate {
	return &discordgo.InteractionCreate{
		Interaction: &discordgo.Interaction{
			ID: id, Type: discordgo.InteractionApplicationCommand, Token: "token-" + id,
			User: &discordgo.User{ID: userID, Username: "user-" + userID},
			Data: discordgo.ApplicationCommandInteractionData{Name: name, Options: opts},
		},
	}
}

// ResetRegisteredGuilds clears cached guild registrations.
func ResetRegisteredGuilds() {
	registeredGuilds.Range(func(key, value any) bool {
		registeredGuilds.Delete(key)
		return true
	})
}

func mockGitOpsUpdater(cfg *config.Config, shouldErr bool) GitOpsChannelUpdater {
	return func(ctx context.Context, channelKey string, mutateFn func(p *config.ChannelPolicy)) (string, error) {
		if shouldErr {
			return "", errors.New("gitops commit failed: branch protected")
		}
		p := cfg.Current().Channels[channelKey]
		mutateFn(&p)
		cfg.Current().Channels[channelKey] = p
		return "gitops-sha-123", nil
	}
}

func seedStatusFixtures(store *dbtest.FakeStore, active bool) {
	ctx, now := context.Background(), time.Now().UTC()
	if active {
		_ = store.InsertMessage(ctx, db.Message{ID: "task-12345", Status: db.StatusProcessing, CreatedAt: now.Add(-45 * time.Second)})
	}
}

// TestCommandDefinitions checks the catalog Discord would reject or render blank: every
// command needs a unique, non-empty name and a description.
func TestCommandDefinitions(t *testing.T) {
	t.Parallel()
	seen := make(map[string]bool)
	for _, c := range CommandDefinitions() {
		if c.Name == "" || c.Description == "" || seen[c.Name] {
			t.Errorf("command %q: want a unique non-empty name and a description (got %q)", c.Name, c.Description)
		}
		seen[c.Name] = true
	}
}

// helperSampleOpts is the option list shared by the option-lookup helper tables.
var helperSampleOpts = []*discordgo.ApplicationCommandInteractionDataOption{
	mkOpt("opt_true", discordgo.ApplicationCommandOptionBoolean, true),
	mkOpt("opt_false", discordgo.ApplicationCommandOptionBoolean, false),
	mkOpt("opt_str", discordgo.ApplicationCommandOptionString, "hello"),
	mkOpt("opt_ch", discordgo.ApplicationCommandOptionChannel, "channel-999"),
	mkOpt("opt_val_str", discordgo.ApplicationCommandOptionChannel, "channel-raw-val"),
}

func TestGetBoolOption(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		key  string
		def  bool
		want bool
	}{
		{"true", "opt_true", false, true},
		{"false", "opt_false", false, false},
		{"missing falls back", "missing", true, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := getBoolOption(helperSampleOpts, tc.key, tc.def); got != tc.want {
				t.Errorf("getBoolOption(%s) = %v, want %v", tc.key, got, tc.want)
			}
		})
	}
}

func TestGetStringOption(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		key  string
		def  string
		want string
	}{
		{"found", "opt_str", "", "hello"},
		{"missing falls back", "missing", "default", "default"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := getStringOption(helperSampleOpts, tc.key, tc.def); got != tc.want {
				t.Errorf("getStringOption(%s) = %q, want %q", tc.key, got, tc.want)
			}
		})
	}
}

func TestGetChannelOption(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		options []*discordgo.ApplicationCommandInteractionDataOption
		key     string
		want    string
	}{
		{"found", helperSampleOpts, "opt_ch", "channel-999"},
		{"raw value", helperSampleOpts, "opt_val_str", "channel-raw-val"},
		{"missing in sample falls back", helperSampleOpts, "missing", "fallback"},
		{"missing", nil, "channel", "fallback"},
		{"nil option", []*discordgo.ApplicationCommandInteractionDataOption{nil}, "channel", "fallback"},
		{"different option", []*discordgo.ApplicationCommandInteractionDataOption{mkOpt("other", discordgo.ApplicationCommandOptionChannel, "channel-123")}, "channel", "fallback"},
		{"selected channel", []*discordgo.ApplicationCommandInteractionDataOption{mkOpt("channel", discordgo.ApplicationCommandOptionChannel, "channel-123")}, "channel", "channel-123"},
		{"empty selection", []*discordgo.ApplicationCommandInteractionDataOption{mkOpt("channel", discordgo.ApplicationCommandOptionChannel, "")}, "channel", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := getChannelOption(tc.options, tc.key, "fallback"); got != tc.want {
				t.Errorf("getChannelOption(%s) = %q, want %q", tc.key, got, tc.want)
			}
		})
	}
}

func TestHasOption(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		key  string
		want bool
	}{
		{"present", "opt_true", true},
		{"absent", "nonexistent", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := hasOption(helperSampleOpts, tc.key); got != tc.want {
				t.Errorf("hasOption(%s) = %v, want %v", tc.key, got, tc.want)
			}
		})
	}
}

func TestTruncateRunes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   string
		max  int
		want string
	}{
		{"long", "hello world", 5, "hello..."},
		{"short", "hi", 5, "hi"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := truncateRunes(tc.in, tc.max); got != tc.want {
				t.Errorf("truncateRunes(%q, %d) = %q, want %q", tc.in, tc.max, got, tc.want)
			}
		})
	}
}

func TestFormatRelativeDuration(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   time.Duration
		want string
	}{
		{"negative", -10 * time.Second, "10s"},
		{"seconds", 45 * time.Second, "45s"},
		{"minutes", 5*time.Minute + 30*time.Second, "5m"},
		{"hours and minutes", 2*time.Hour + 10*time.Minute, "2h 10m"},
		{"days and hours", 25 * time.Hour, "1d 1h"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := formatRelativeDuration(tc.in); got != tc.want {
				t.Errorf("formatRelativeDuration(%v) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestFormatRelativeTime(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   time.Duration
		want string
	}{
		{"just now", 10 * time.Second, "just now"},
		{"minutes", 5 * time.Minute, "5m"},
		{"hours", 3 * time.Hour, "3h"},
		{"days", 48 * time.Hour, "2d"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := formatRelativeTime(tc.in); got != tc.want {
				t.Errorf("formatRelativeTime(%v) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestRouter_AuthAndDispatch(t *testing.T) {
	mockPool := queue.New(nil, queue.WorkerPoolConfig{})

	tests := []struct {
		name          string
		interaction   *discordgo.InteractionCreate
		nilSession    bool
		bareSession   bool
		httpStatus    int
		withProviders bool
		wantSubstr    string
		wantRequests  int
	}{
		{"NonAdminMember", mkCmd("1", "chan-1", "attacker-999", "status"), false, false, 0, false, "Unauthorized", 1},
		{"NonAdminDM", mkDMCmd("2", "attacker-888", "status"), false, false, 0, false, "Unauthorized", 1},
		{"UnrecognizedCommand", mkCmd("3", "chan-1", "admin-123", "nonexistent_cmd"), false, false, 0, false, "Unrecognized command", 1},
		{"NilSession", mkCmd("4", "chan-1", "admin-123", "status"), true, false, 0, false, "", 0},
		{"BareSessionNoRatelimiter", mkCmd("5", "chan-1", "admin-123", "status"), false, true, 0, false, "", 0},
		{"NilInteraction", nil, false, false, 0, false, "", 0},
		{"NilInnerInteraction", &discordgo.InteractionCreate{}, false, false, 0, false, "", 0},
		{"EmptyInteractionID", &discordgo.InteractionCreate{Interaction: &discordgo.Interaction{ID: "", Type: discordgo.InteractionApplicationCommand}}, false, false, 0, false, "", 0},
		{"NonApplicationCommandType", &discordgo.InteractionCreate{Interaction: &discordgo.Interaction{ID: "6", Type: discordgo.InteractionMessageComponent}}, false, false, 0, false, "", 0},
		{"NilUserAndMember", &discordgo.InteractionCreate{Interaction: &discordgo.Interaction{ID: "7", Type: discordgo.InteractionApplicationCommand, Token: "tok-7", Data: discordgo.ApplicationCommandInteractionData{Name: "status"}}}, false, false, 0, false, "", 0},
		{"DynamicConfigAndPoolProviders", mkCmd("8", "chan-1", "admin-123", "nonexistent_cmd"), false, false, 0, true, "Unrecognized command", 1},
		{"ErrorLogging_NonAdminHttp500", mkDMCmd("err-1", "non-admin", "status"), false, false, http.StatusInternalServerError, false, "", 1},
		{"ErrorLogging_UnknownCmdHttp500", mkCmd("err-2", "chan-1", "admin-123", "unknown_xyz"), false, false, http.StatusInternalServerError, false, "", 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			router, cfg, transport, session := setupTestRouter("admin-123", tc.httpStatus)
			if tc.nilSession {
				session = nil
			} else if tc.bareSession {
				session = &discordgo.Session{}
			}

			if tc.withProviders {
				var cfgCalled, poolCalled bool
				router.deps.ConfigProvider = func() *config.Config {
					cfgCalled = true
					return cfg
				}
				router.deps.PoolProvider = func() *queue.WorkerPool {
					poolCalled = true
					return mockPool
				}
				if got := router.getConfig(); got != cfg || !cfgCalled {
					t.Fatalf("expected getConfig to invoke ConfigProvider")
				}
				if got := router.getPool(); got != mockPool || !poolCalled {
					t.Fatalf("expected getPool to invoke PoolProvider")
				}
			}
			router.Handle(t.Context(), session, tc.interaction)

			reqs := transport.Requests()
			if len(reqs) != tc.wantRequests {
				t.Fatalf("expected %d requests, got %d", tc.wantRequests, len(reqs))
			}
			if tc.wantSubstr != "" && len(reqs) > 0 {
				if !strings.Contains(string(reqs[0].Body), tc.wantSubstr) {
					t.Errorf("expected response to contain %q, got: %s", tc.wantSubstr, string(reqs[0].Body))
				}
			}
		})
	}
}

type statusTestCase struct {
	name          string
	gitCommitEnv  string
	deployStatus  string
	hasDeployFn   bool
	seedFacts     bool
	seedCrons     bool
	seedOneShots  bool
	seedActive    bool
	failStore     bool
	httpStatus    int
	wantSubstrs   []string
	haroInConfig  bool // haro URL comes from config instead of deps
	covenInConfig bool // memory URL comes from config CovenURL instead of deps
}

func buildStatusHaroFixtures(tc statusTestCase, now time.Time) ([]map[string]any, []map[string]any) {
	var crons []map[string]any
	var oneShots []map[string]any
	if tc.seedCrons && tc.seedFacts {
		crons = append(crons, map[string]any{
			"title_prefix": "Morning briefing",
			"prompt":       "Morning system briefing",
			"next_run_at":  now.Add(2 * time.Hour),
		})
	} else if tc.seedCrons {
		crons = append(crons,
			map[string]any{"title_prefix": "", "prompt": "Hourly health check", "next_run_at": now.Add(30 * time.Minute)},
			map[string]any{"title_prefix": "", "prompt": "Past cron", "next_run_at": now.Add(-10 * time.Minute)},
		)
	}
	if tc.seedOneShots {
		oneShots = append(oneShots,
			map[string]any{"prompt": "Follow up on PR deployment", "run_at": now.Add(15 * time.Minute)},
			map[string]any{"prompt": "Past oneshot", "run_at": now.Add(-15 * time.Minute)},
		)
	}
	return crons, oneShots
}

func setupStatusRouter(tc statusTestCase, memSrvURL string, haroSrvURL string) (*Router, *config.Config, *mockTransport, *discordgo.Session) {
	router, cfg, transport, session := setupTestRouter("admin-123", tc.httpStatus)
	if tc.seedFacts {
		router.deps.MemoryURL = memSrvURL
	} else if tc.failStore {
		router.deps.MemoryURL = "http://127.0.0.1:1"
	}
	store := dbtest.NewFakeStore()
	seedStatusFixtures(store, tc.seedActive)

	if tc.failStore {
		router.deps.HaroURL = "http://127.0.0.1:1"
		store.FailNext("GetActiveTasks", errors.New("db error"))
		router.deps.Pool = queue.New(nil, queue.WorkerPoolConfig{})
	} else {
		router.deps.HaroURL = haroSrvURL
		if tc.haroInConfig {
			router.deps.HaroURL = ""
			cfg.Current().HaroURL = haroSrvURL
		}
		if tc.covenInConfig {
			router.deps.MemoryURL = ""
			cfg.Current().CovenURL = memSrvURL
		}
	}

	router.deps.Store = store
	if tc.hasDeployFn {
		ds := tc.deployStatus
		router.deps.DeployStatusProvider = func() string { return ds }
	}
	return router, cfg, transport, session
}

func runHandleStatusCase(t *testing.T, tc statusTestCase, memSrvURL string) {
	t.Helper()
	if tc.gitCommitEnv != "" {
		t.Setenv("GIT_COMMIT", tc.gitCommitEnv)
	}

	haroSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		crons, oneShots := buildStatusHaroFixtures(tc, time.Now().UTC())
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":    "ok",
			"crons":     crons,
			"one_shots": oneShots,
		})
	}))
	defer haroSrv.Close()

	router, _, transport, session := setupStatusRouter(tc, memSrvURL, haroSrv.URL)
	router.Handle(t.Context(), session, mkCmd("int-status", "chan-1", "admin-123", "status"))

	reqs := transport.Requests()
	if len(reqs) != 1 {
		t.Fatalf("expected 1 request, got %d", len(reqs))
	}
	body := string(reqs[0].Body)
	for _, want := range tc.wantSubstrs {
		if !strings.Contains(body, want) {
			t.Errorf("expected response to contain %q, got: %s", want, body)
		}
	}
}

func TestRouter_HandleStatus(t *testing.T) {
	memSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"total": 1,
			"facts": []map[string]any{{"fact_text": "Aerial runs on PostgreSQL 16 with pgvector", "created_at": time.Now().UTC().Format(time.RFC3339)}},
		})
	}))
	defer memSrv.Close()

	tests := []statusTestCase{
		{name: "StandardWithFactsAndCron", deployStatus: "`aerial@bfed897` (Live, healthy)", hasDeployFn: true, seedFacts: true, seedCrons: true, wantSubstrs: []string{"**Runtime**:", "`aerial@bfed897`", "1 facts recorded", "Morning briefing"}},
		{name: "FallbackEnvAndIdle", gitCommitEnv: "abcdef123456", wantSubstrs: []string{"`aerial@abcdef1`", "`Idle`"}},
		{name: "AdvancedBranchesAndSchedules", hasDeployFn: true, seedCrons: true, seedOneShots: true, seedActive: true, wantSubstrs: []string{"Follow up on PR deployment"}},
		{name: "HaroConfigFallback", seedFacts: true, seedCrons: true, haroInConfig: true, wantSubstrs: []string{"Morning briefing"}},
		{name: "CovenConfigFallback", seedCrons: true, covenInConfig: true, wantSubstrs: []string{"Hourly health check", "1 facts recorded"}},
		{name: "StoreFailuresHandledGracefully", failStore: true, wantSubstrs: []string{"0 facts recorded"}},
		{name: "ErrorLoggingHttp500", httpStatus: http.StatusInternalServerError},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runHandleStatusCase(t, tc, memSrv.URL)
		})
	}
}

func TestRouter_HandleQuota(t *testing.T) {
	now := time.Now().UTC()
	resetStr := now.Add(2*time.Hour + 30*time.Minute).Format(time.RFC3339)
	refreshedAt := now.Add(-4 * time.Minute)
	sampleGroups := []metrics.QuotaGroup{
		{
			Name: "Gemini Models",
			Buckets: []metrics.QuotaBucket{
				{ID: "gemini-weekly", Window: "weekly", RemainingFraction: 0.869, ResetTime: resetStr},
				{ID: "gemini-5h", Window: "5h", RemainingFraction: 0.944, Disabled: true},
				{ID: "", Window: "ignored", RemainingFraction: 0.5},
			},
		},
	}
	tests := []struct {
		name          string
		groups        []metrics.QuotaGroup
		updatedAt     time.Time
		useProvider   bool
		httpStatus    int
		wantSubstrs   []string
		absentSubstrs []string
	}{
		{"WithActiveQuotaBuckets", sampleGroups, refreshedAt, true, 0, []string{"**Gemini Models**", "**weekly**: `87%` remaining", "resets in 2h", "Last refreshed: 4m ago", `\u003ct:`}, nil},
		{"CodexWeeklyOnly", []metrics.QuotaGroup{{Name: "Codex", Buckets: []metrics.QuotaBucket{{ID: "codex-seven-day", Window: "weekly", RemainingFraction: 0.98}}}}, refreshedAt, true, 0, []string{"**Codex**", "**weekly**: `98%` remaining"}, []string{"**5h**"}},
		{"ZeroUpdatedAtOmitsLine", sampleGroups, time.Time{}, true, 0, []string{"**Gemini Models**"}, []string{"Last refreshed"}},
		{"EmptyQuotaFallback", nil, time.Time{}, false, 0, []string{"No quota telemetry recorded yet"}, nil},
		{"ErrorLoggingHttp500", nil, time.Time{}, true, http.StatusInternalServerError, nil, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			router, _, transport, session := setupTestRouter("admin-123", tc.httpStatus)
			if tc.useProvider {
				snap := metrics.QuotaSnapshot{Groups: tc.groups, UpdatedAt: tc.updatedAt}
				router.deps.QuotaProvider = func() metrics.QuotaSnapshot { return snap }
			}
			router.Handle(t.Context(), session, mkCmd("int-quota", "chan-1", "admin-123", "quota"))
			reqs := transport.Requests()
			if len(reqs) != 1 {
				t.Fatalf("expected 1 request, got %d", len(reqs))
			}
			body := string(reqs[0].Body)
			for _, absent := range tc.absentSubstrs {
				if strings.Contains(body, absent) {
					t.Errorf("unexpected %q in %s", absent, body)
				}
			}
			for _, want := range tc.wantSubstrs {
				if !strings.Contains(body, want) {
					t.Errorf("expected response to contain %q, got: %s", want, body)
				}
			}
		})
	}
}

type fakeTurnCanceller struct {
	resp      queue.CancelTurnResponse
	err       error
	mu        sync.Mutex
	threadIDs []string
	reasons   []string
}

func (f *fakeTurnCanceller) CancelThreadTurn(_ context.Context, threadID string, req queue.CancelTurnRequest) (queue.CancelTurnResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.threadIDs = append(f.threadIDs, threadID)
	f.reasons = append(f.reasons, req.Reason)
	return f.resp, f.err
}

func TestRouter_HandleInterrupt(t *testing.T) {
	t.Parallel()
	const reason = "cancelled by operator via /interrupt"
	tests := []struct {
		name            string
		userID          string
		noProvider      bool
		providerNil     bool
		httpStatus      int
		resp            queue.CancelTurnResponse
		err             error
		wantDeferred    bool
		wantContent     string
		wantCancelCalls int
	}{
		{name: "admin active-turn cancel", userID: "admin-123", resp: queue.CancelTurnResponse{CancelledActiveTurn: true, CancelledCount: 2}, wantDeferred: true, wantContent: "🛑 Interrupted the active turn in <#chan-int> and dropped 2 queued messages", wantCancelCalls: 1},
		{name: "admin active-turn cancel without queue", userID: "admin-123", resp: queue.CancelTurnResponse{CancelledActiveTurn: true}, wantDeferred: true, wantContent: "🛑 Interrupted the active turn in <#chan-int>", wantCancelCalls: 1},
		{name: "admin queued-only cancel", userID: "admin-123", resp: queue.CancelTurnResponse{CancelledCount: 3}, wantDeferred: true, wantContent: "No active turn in <#chan-int>; dropped 3 queued messages.", wantCancelCalls: 1},
		{name: "admin nothing running", userID: "admin-123", wantDeferred: true, wantContent: "Nothing was running in <#chan-int>.", wantCancelCalls: 1},
		{name: "admin canceller error", userID: "admin-123", err: errors.New("restate down"), wantDeferred: true, wantContent: "Interrupt failed: restate down", wantCancelCalls: 1},
		{name: "non-admin rejection", userID: "attacker-999", wantContent: "Unauthorized"},
		{name: "nil provider", userID: "admin-123", noProvider: true, wantDeferred: true, wantContent: "interrupt unavailable"},
		{name: "provider returning nil", userID: "admin-123", providerNil: true, wantDeferred: true, wantContent: "interrupt unavailable"},
		{name: "discord defer failure skips cancel", userID: "admin-123", httpStatus: http.StatusInternalServerError, wantContent: `"type":5`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			router, _, transport, session := setupTestRouter("admin-123", tc.httpStatus)
			fake := &fakeTurnCanceller{resp: tc.resp, err: tc.err}
			router.deps.TurnCancellerProvider = interruptProvider(fake, tc.noProvider, tc.providerNil)

			router.Handle(t.Context(), session, mkCmd("int-"+tc.name, "chan-int", tc.userID, "interrupt"))

			assertInterruptResponses(t, transport.Requests(), tc.wantDeferred, tc.wantContent)
			assertCancelCalls(t, fake, tc.wantCancelCalls, "chan-int", reason)
		})
	}
}

func interruptProvider(fake *fakeTurnCanceller, noProvider, providerNil bool) TurnCancellerProvider {
	if noProvider {
		return nil
	}
	if providerNil {
		return func() TurnCanceller { return nil }
	}
	return func() TurnCanceller { return fake }
}

func assertInterruptResponses(t *testing.T, reqs []capturedRequest, wantDeferred bool, wantContent string) {
	t.Helper()
	if !wantDeferred {
		if len(reqs) != 1 || !strings.Contains(string(reqs[0].Body), wantContent) {
			t.Fatalf("expected one response containing %q, got %+v", wantContent, reqs)
		}
		return
	}
	if len(reqs) != 2 {
		t.Fatalf("expected deferred response plus edit, got %d requests", len(reqs))
	}
	var deferred discordgo.InteractionResponse
	if err := json.Unmarshal(reqs[0].Body, &deferred); err != nil {
		t.Fatalf("decode deferred response: %v", err)
	}
	if deferred.Type != discordgo.InteractionResponseDeferredChannelMessageWithSource || deferred.Data == nil || deferred.Data.Flags != discordgo.MessageFlagsEphemeral {
		t.Errorf("expected deferred ephemeral response, got %s", reqs[0].Body)
	}
	var edit discordgo.WebhookEdit
	if err := json.Unmarshal(reqs[1].Body, &edit); err != nil {
		t.Fatalf("decode edit: %v", err)
	}
	if reqs[1].Method != http.MethodPatch || edit.Content == nil || *edit.Content != wantContent {
		t.Errorf("expected PATCH edit %q, got %s %s", wantContent, reqs[1].Method, reqs[1].Body)
	}
}

func assertCancelCalls(t *testing.T, fake *fakeTurnCanceller, wantCalls int, wantThreadID, wantReason string) {
	t.Helper()
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.threadIDs) != wantCalls {
		t.Fatalf("expected %d cancel calls, got %d", wantCalls, len(fake.threadIDs))
	}
	for idx := range fake.threadIDs {
		if fake.threadIDs[idx] != wantThreadID || fake.reasons[idx] != wantReason {
			t.Errorf("cancel call %d: got thread %q reason %q", idx, fake.threadIDs[idx], fake.reasons[idx])
		}
	}
}

type configMutationTestCase struct {
	name            string
	cmdName         string
	opts            []*discordgo.ApplicationCommandInteractionDataOption
	withGitOps      bool
	gitOpsErr       bool
	noGitOpsUpdater bool
	httpStatus      int
	wantSubstrs     []string
	wantModeVal     string
	wantIgnoreBots  *bool
	runDeployFnTest bool
	testTimeout     bool
	testMutateKey   string
	testMutateVal   string
}

func handleDeployProgressTest(t *testing.T, router *Router, session *discordgo.Session, tc configMutationTestCase) {
	t.Helper()
	interaction := &discordgo.Interaction{ID: "int-deploy", Token: "tok-deploy"}
	if tc.testTimeout {
		oldTimeout, oldD := pollerTimeout, deployProgressDelay
		pollerTimeout, deployProgressDelay = 5*time.Millisecond, 100*time.Millisecond
		router.trackDeployProgress(t.Context(), session, interaction, "mode", "chan-1", "thread")
		router.trackDeployProgress(t.Context(), newMockDiscordSession(&mockTransport{respCode: http.StatusInternalServerError}), interaction, "mode", "chan-1", "thread")
		pollerTimeout, deployProgressDelay = oldTimeout, oldD
		return
	}
	var captured config.ChannelPolicy
	router.deps.GitOpsChannelUpdater = func(_ context.Context, _ string, fn func(p *config.ChannelPolicy)) (string, error) {
		fn(&captured)
		return "sha", nil
	}
	router.trackDeployProgress(t.Context(), session, interaction, tc.testMutateKey, "chan-1", tc.testMutateVal)
	if tc.testMutateKey == "mode" && captured.Mode != tc.testMutateVal {
		t.Errorf("expected Mode %q, got %q", tc.testMutateVal, captured.Mode)
	}
	if tc.testMutateKey == "ignore_bots" && (captured.IgnoreBots == nil || !*captured.IgnoreBots) {
		t.Errorf("expected IgnoreBots true")
	}
}

func runConfigMutationCase(t *testing.T, tc configMutationTestCase) {
	t.Helper()
	router, cfg, transport, session := setupTestRouter("admin-123", tc.httpStatus)
	if tc.withGitOps {
		router.deps.GitOpsChannelUpdater = mockGitOpsUpdater(cfg, tc.gitOpsErr)
	}

	if tc.runDeployFnTest {
		handleDeployProgressTest(t, router, session, tc)
		return
	}

	chID := "chan-1"
	if tc.cmdName == "ignore_bots" {
		chID = "chan-2"
	}
	router.Handle(t.Context(), session, mkCmd("int-"+tc.name, chID, "admin-123", tc.cmdName, tc.opts...))
	router.bgWg.Wait()

	reqs := transport.Requests()
	for _, want := range tc.wantSubstrs {
		var found bool
		for _, req := range reqs {
			if strings.Contains(string(req.Body), want) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected request body to contain %q", want)
		}
	}

	if tc.wantModeVal != "" {
		if pol := cfg.ResolveChannelPolicy("chan-1", ""); pol.Mode != tc.wantModeVal {
			t.Errorf("expected mode %q, got %q", tc.wantModeVal, pol.Mode)
		}
	}
	if tc.wantIgnoreBots != nil {
		if pol := cfg.ResolveChannelPolicy("chan-2", ""); pol.IsBotIgnored() != *tc.wantIgnoreBots {
			t.Errorf("expected ignore_bots %v, got %v", *tc.wantIgnoreBots, pol.IsBotIgnored())
		}
	}
}

func TestRouter_ConfigMutationCommands(t *testing.T) {
	oldDelay := deployProgressDelay
	deployProgressDelay = 5 * time.Millisecond
	defer func() { deployProgressDelay = oldDelay }()

	tests := []configMutationTestCase{
		{"Mode_BareQuery", "mode", nil, false, false, false, 0, []string{"**Mode**: `thread`"}, "", nil, false, false, "", ""},
		{"Mode_SetWithGitOps", "mode", []*discordgo.ApplicationCommandInteractionDataOption{mkOpt("mode", discordgo.ApplicationCommandOptionString, "classifier")}, true, false, false, 0, nil, "classifier", nil, false, false, "", ""},
		{"Mode_SetInMemoryFallback", "mode", []*discordgo.ApplicationCommandInteractionDataOption{mkOpt("mode", discordgo.ApplicationCommandOptionString, "mention")}, false, false, true, 0, nil, "mention", nil, false, false, "", ""},
		{"Mode_GitOpsError", "mode", []*discordgo.ApplicationCommandInteractionDataOption{mkOpt("mode", discordgo.ApplicationCommandOptionString, "mention")}, true, true, false, 0, []string{"Failed to apply GitOps update"}, "", nil, false, false, "", ""},
		{"IgnoreBots_BareQuery", "ignore_bots", nil, false, false, false, 0, []string{"**Ignore Bots**:"}, "", nil, false, false, "", ""},
		{"IgnoreBots_SetWithGitOps", "ignore_bots", []*discordgo.ApplicationCommandInteractionDataOption{mkOpt("enabled", discordgo.ApplicationCommandOptionBoolean, true)}, true, false, false, 0, nil, "", new(true), false, false, "", ""},
		{"IgnoreBots_SetInMemoryFallback", "ignore_bots", []*discordgo.ApplicationCommandInteractionDataOption{mkOpt("enabled", discordgo.ApplicationCommandOptionBoolean, true)}, false, false, true, 0, nil, "", new(true), false, false, "", ""},
		{"IgnoreBots_GitOpsError", "ignore_bots", []*discordgo.ApplicationCommandInteractionDataOption{mkOpt("enabled", discordgo.ApplicationCommandOptionBoolean, true)}, true, true, false, 0, []string{"Failed to apply GitOps update"}, "", nil, false, false, "", ""},
		{"DeployProgress_TimeoutAndEditErrors", "", nil, false, false, false, 0, nil, "", nil, true, true, "", ""},
		{"DeployProgress_MutateFnMode", "", nil, false, false, false, 0, nil, "", nil, true, false, "mode", "mention"},
		{"DeployProgress_MutateFnIgnoreBots", "", nil, false, false, false, 0, nil, "", nil, true, false, "ignore_bots", "true"},
		{"ErrorLogging_ModeBareHttp500", "mode", nil, false, false, false, http.StatusInternalServerError, nil, "", nil, false, false, "", ""},
		{"ErrorLogging_ModeDeferHttp500", "mode", []*discordgo.ApplicationCommandInteractionDataOption{mkOpt("mode", discordgo.ApplicationCommandOptionString, "mention")}, false, false, false, http.StatusInternalServerError, nil, "", nil, false, false, "", ""},
		{"ErrorLogging_IgnoreBotsBareHttp500", "ignore_bots", nil, false, false, false, http.StatusInternalServerError, nil, "", nil, false, false, "", ""},
		{"ErrorLogging_IgnoreBotsDeferHttp500", "ignore_bots", []*discordgo.ApplicationCommandInteractionDataOption{mkOpt("enabled", discordgo.ApplicationCommandOptionBoolean, true)}, false, false, false, http.StatusInternalServerError, nil, "", nil, false, false, "", ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runConfigMutationCase(t, tc)
		})
	}
}

// newSyncSession builds the session shape a register-commands row asks for.
func newSyncSession(transport *mockTransport, nilSession, bareSession, noStateUser bool) *discordgo.Session {
	switch {
	case nilSession:
		return nil
	case bareSession:
		return &discordgo.Session{}
	}
	session := newMockDiscordSession(transport)
	if noStateUser {
		session.State = nil
	}
	return session
}

func TestRegisterGuildCommands(t *testing.T) {
	tests := []struct {
		name         string
		appID        string
		guildID      string
		nilSession   bool
		bareSession  bool
		noStateUser  bool
		httpStatus   int
		wantErr      bool
		wantRequests int
		wantMethod   string
	}{
		{name: "Success", appID: "app-123", guildID: "guild-456", wantRequests: 1, wantMethod: http.MethodPut},
		{name: "EmptyAppIDUsesState", guildID: "guild-valid", wantRequests: 1, wantMethod: http.MethodPut},
		{name: "NilSession", nilSession: true, wantErr: true},
		{name: "BareSessionNoRatelimiter", bareSession: true, wantErr: true},
		{name: "EmptyAppIDNoStateUser", guildID: "guild-nostate", noStateUser: true, wantErr: true},
		{name: "EmptyGuildID", appID: "app-123", wantErr: true},
		{name: "Http500", appID: "app-123", guildID: "guild-err", httpStatus: http.StatusInternalServerError, wantErr: true, wantRequests: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ResetRegisteredGuilds()
			defer ResetRegisteredGuilds()
			transport := &mockTransport{respCode: tc.httpStatus}
			session := newSyncSession(transport, tc.nilSession, tc.bareSession, tc.noStateUser)
			_, err := RegisterGuildCommands(session, tc.appID, tc.guildID)
			if (err != nil) != tc.wantErr {
				t.Fatalf("RegisterGuildCommands() error = %v, wantErr %v", err, tc.wantErr)
			}
			reqs := transport.Requests()
			if len(reqs) != tc.wantRequests {
				t.Fatalf("expected %d requests, got %d", tc.wantRequests, len(reqs))
			}
			if tc.wantMethod != "" && reqs[0].Method != tc.wantMethod {
				t.Errorf("expected method %s, got %s", tc.wantMethod, reqs[0].Method)
			}
		})
	}
}

// TestRegisterGuildCommandsOnce registers three times per row: the second call must hit the
// cache, and the third (after ResetRegisteredGuilds) must reach Discord again.
func TestRegisterGuildCommandsOnce(t *testing.T) {
	tests := []struct {
		name         string
		guildID      string
		httpStatus   int
		wantErr      bool
		wantRequests []int // transport request count after each of the three calls
	}{
		{name: "CachingAndReset", guildID: "guild-once", wantRequests: []int{1, 1, 2}},
		{name: "ErrorCleansCache", guildID: "guild-once-err", httpStatus: http.StatusInternalServerError, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ResetRegisteredGuilds()
			defer ResetRegisteredGuilds()
			transport := &mockTransport{respCode: tc.httpStatus}
			session := newMockDiscordSession(transport)
			_, err := RegisterGuildCommandsOnce(session, "app-123", tc.guildID)
			if (err != nil) != tc.wantErr {
				t.Fatalf("RegisterGuildCommandsOnce() error = %v, wantErr %v", err, tc.wantErr)
			}
			for i, want := range tc.wantRequests {
				if i == 2 {
					ResetRegisteredGuilds()
				}
				if i > 0 {
					if _, err := RegisterGuildCommandsOnce(session, "app-123", tc.guildID); err != nil {
						t.Fatalf("call %d: RegisterGuildCommandsOnce() failed: %v", i+1, err)
					}
				}
				if got := len(transport.Requests()); got != want {
					t.Errorf("after call %d: %d requests, want %d", i+1, got, want)
				}
			}
		})
	}
}
