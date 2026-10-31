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

	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/db/dbtest"
	"github.com/azylman/aerial/services/eri/pkg/metrics"
	"github.com/azylman/aerial/services/eri/pkg/queue"
	"github.com/azylman/aerial/services/eri/pkg/runner"
	"github.com/bwmarrin/discordgo"
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

type mockInteractionsProcessHandle struct{}

func (mockInteractionsProcessHandle) Pid() int    { return 12345 }
func (mockInteractionsProcessHandle) Kill() error { return nil }
func (mockInteractionsProcessHandle) Wait() error { return nil }

type mockInteractionsDaemonSpawner struct{}

func (mockInteractionsDaemonSpawner) Spawn(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	errR, errW := io.Pipe()
	go func() {
		_, _ = outW.Write([]byte("{\"event\":\"init\",\"conversation_id\":\"550e8400-e29b-41d4-a716-446655440000\"}\n"))
		_ = errW.Close()
		_, _ = io.Copy(io.Discard, inR)
	}()
	return inW, outR, errR, mockInteractionsProcessHandle{}, nil
}

func newMockProcessPools(threadID string) *queue.WorkerPool {
	spawner := mockInteractionsDaemonSpawner{}
	mk := func(m string) *runner.UnifiedProcessPool {
		p := runner.NewUnifiedProcessPool(runner.PoolConfig{DefaultModel: m}, spawner)
		_, _ = p.GetOrCreate(context.Background(), threadID, "")
		return p
	}
	return queue.New(nil, queue.WorkerPoolConfig{
		ProcessPool:             mk("gemini-2.5-flash"),
		LowEffortProcessPool:    mk("gemini-2.5-flash"),
		MediumEffortProcessPool: mk("gemini-3.8-flash-medium"),
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

func boolPtr(b bool) *bool { return &b }

func TestApplicationCommandsAndHelpers(t *testing.T) {
	sampleOpts := []*discordgo.ApplicationCommandInteractionDataOption{
		mkOpt("opt_true", discordgo.ApplicationCommandOptionBoolean, true),
		mkOpt("opt_false", discordgo.ApplicationCommandOptionBoolean, false),
		mkOpt("opt_str", discordgo.ApplicationCommandOptionString, "hello"),
		mkOpt("opt_ch", discordgo.ApplicationCommandOptionChannel, "channel-999"),
		mkOpt("opt_val_str", discordgo.ApplicationCommandOptionChannel, "channel-raw-val"),
	}

	tests := []struct {
		name       string
		helperType string
		strInput   string
		intInput   int
		durInput   time.Duration
		wantStr    string
		wantBool   bool
		wantInt    int
	}{
		{"CmdDefCount", "cmd_len", "", 0, 0, "", false, 6},
		{"CmdDef_status", "cmd_opts", "status", 0, 0, "", false, 0},
		{"CmdDef_quota", "cmd_opts", "quota", 0, 0, "", false, 0},
		{"CmdDef_rotate", "cmd_opts", "rotate", 0, 0, "", false, 2},
		{"CmdDef_interrupt", "cmd_opts", "interrupt", 0, 0, "", false, 2},
		{"CmdDef_mode", "cmd_opts", "mode", 0, 0, "", false, 2},
		{"CmdDef_ignore_bots", "cmd_opts", "ignore_bots", 0, 0, "", false, 2},
		{"GetBool_True", "bool", "opt_true", 0, 0, "", true, 0},
		{"GetBool_False", "bool", "opt_false", 0, 0, "", false, 0},
		{"GetBool_MissingFallback", "bool", "missing", 0, 0, "", true, 0},
		{"GetString_Found", "string", "opt_str", 0, 0, "hello", false, 0},
		{"GetString_MissingFallback", "string", "missing", 0, 0, "default", false, 0},
		{"GetChannel_Found", "channel", "opt_ch", 0, 0, "channel-999", false, 0},
		{"GetChannel_RawVal", "channel", "opt_val_str", 0, 0, "channel-raw-val", false, 0},
		{"GetChannel_MissingFallback", "channel", "missing", 0, 0, "fallback", false, 0},
		{"HasOption_True", "has_opt", "opt_true", 0, 0, "", true, 0},
		{"HasOption_False", "has_opt", "nonexistent", 0, 0, "", false, 0},
		{"Truncate_Long", "truncate", "hello world", 5, 0, "hello...", false, 0},
		{"Truncate_Short", "truncate", "hi", 5, 0, "hi", false, 0},
		{"FormatDur_Negative", "duration", "", 0, -10 * time.Second, "10s", false, 0},
		{"FormatDur_Seconds", "duration", "", 0, 45 * time.Second, "45s", false, 0},
		{"FormatDur_Minutes", "duration", "", 0, 5*time.Minute + 30*time.Second, "5m", false, 0},
		{"FormatDur_HoursMinutes", "duration", "", 0, 2*time.Hour + 10*time.Minute, "2h 10m", false, 0},
		{"FormatDur_DaysHours", "duration", "", 0, 25 * time.Hour, "1d 1h", false, 0},
		{"FormatRel_JustNow", "relative_time", "", 0, 10 * time.Second, "just now", false, 0},
		{"FormatRel_Minutes", "relative_time", "", 0, 5 * time.Minute, "5m", false, 0},
		{"FormatRel_Hours", "relative_time", "", 0, 3 * time.Hour, "3h", false, 0},
		{"FormatRel_Days", "relative_time", "", 0, 48 * time.Hour, "2d", false, 0},
	}

	cmds := CommandDefinitions()
	cmdMap := make(map[string]int)
	for _, c := range cmds {
		cmdMap[c.Name] = len(c.Options)
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			switch tc.helperType {
			case "cmd_len":
				if len(cmds) != tc.wantInt {
					t.Fatalf("expected %d commands, got %d", tc.wantInt, len(cmds))
				}
			case "cmd_opts":
				if opts, ok := cmdMap[tc.strInput]; !ok || opts != tc.wantInt {
					t.Fatalf("cmd %s: expected %d options, got %d (found=%v)", tc.strInput, tc.wantInt, opts, ok)
				}
			case "bool":
				if got := getBoolOption(sampleOpts, tc.strInput, tc.strInput == "missing"); got != tc.wantBool {
					t.Errorf("getBoolOption(%s)=%v, want %v", tc.strInput, got, tc.wantBool)
				}
			case "string":
				def := ""
				if tc.strInput == "missing" {
					def = "default"
				}
				if got := getStringOption(sampleOpts, tc.strInput, def); got != tc.wantStr {
					t.Errorf("getStringOption(%s)=%q, want %q", tc.strInput, got, tc.wantStr)
				}
			case "channel":
				if got := getChannelOption(sampleOpts, tc.strInput, "fallback"); got != tc.wantStr {
					t.Errorf("getChannelOption(%s)=%q, want %q", tc.strInput, got, tc.wantStr)
				}
			case "has_opt":
				if got := hasOption(sampleOpts, tc.strInput); got != tc.wantBool {
					t.Errorf("hasOption(%s)=%v, want %v", tc.strInput, got, tc.wantBool)
				}
			case "truncate":
				if got := truncateRunes(tc.strInput, tc.intInput); got != tc.wantStr {
					t.Errorf("truncateRunes(%s, %d)=%q, want %q", tc.strInput, tc.intInput, got, tc.wantStr)
				}
			case "duration":
				if got := formatRelativeDuration(tc.durInput); got != tc.wantStr {
					t.Errorf("formatRelativeDuration(%v)=%q, want %q", tc.durInput, got, tc.wantStr)
				}
			case "relative_time":
				if got := formatRelativeTime(tc.durInput); got != tc.wantStr {
					t.Errorf("formatRelativeTime(%v)=%q, want %q", tc.durInput, got, tc.wantStr)
				}
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
			router.Handle(session, tc.interaction)

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

func TestRouter_HandleStatus(t *testing.T) {
	memSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"total": 1,
			"facts": []map[string]any{{"fact_text": "Aerial runs on PostgreSQL 16 with pgvector", "created_at": time.Now().UTC().Format(time.RFC3339)}},
		})
	}))
	defer memSrv.Close()

	tests := []struct {
		name         string
		gitCommitEnv string
		deployStatus string
		hasDeployFn  bool
		seedFacts    bool
		seedCrons    bool
		seedOneShots bool
		seedActive   bool
		failStore    bool
		httpStatus   int
		wantSubstrs  []string
		envMode      string
	}{
		{"StandardWithFactsAndCron", "", "`aerial@bfed897` (Live, healthy)", true, true, true, false, false, false, 0, []string{"**Runtime**:", "`aerial@bfed897`", "1 facts recorded", "Morning briefing"}, ""},
		{"FallbackEnvAndIdle", "abcdef123456", "", false, false, false, false, false, false, 0, []string{"`aerial@abcdef1`", "`Idle`"}, ""},
		{"AdvancedBranchesAndSchedules", "", "", true, false, true, true, true, false, 0, []string{"Follow up on PR deployment"}, ""},
		{"HaroEnvFallback", "", "", false, true, true, false, false, false, 0, []string{"Morning briefing"}, "haro_env"},
		{"CovenEnvFallback", "", "", false, false, true, false, false, false, 0, []string{"Hourly health check", "1 facts recorded"}, "coven_env"},
		{"StoreFailuresHandledGracefully", "", "", false, false, false, false, false, true, 0, []string{"0 facts recorded"}, ""},
		{"ErrorLoggingHttp500", "", "", false, false, false, false, false, false, http.StatusInternalServerError, nil, ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.gitCommitEnv != "" {
				t.Setenv("GIT_COMMIT", tc.gitCommitEnv)
			}

			router, _, transport, session := setupTestRouter("admin-123", tc.httpStatus)
			if tc.seedFacts {
				router.deps.MemoryURL = memSrv.URL
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
				haroSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					now := time.Now().UTC()
					var crons []map[string]any
					var oneShots []map[string]any
					if tc.seedCrons {
						if tc.seedFacts {
							crons = append(crons, map[string]any{
								"title_prefix": "Morning briefing",
								"prompt":       "Morning system briefing",
								"next_run_at":  now.Add(2 * time.Hour),
							})
						} else {
							crons = append(crons,
								map[string]any{"title_prefix": "", "prompt": "Hourly health check", "next_run_at": now.Add(30 * time.Minute)},
								map[string]any{"title_prefix": "", "prompt": "Past cron", "next_run_at": now.Add(-10 * time.Minute)},
							)
						}
					}
					if tc.seedOneShots {
						oneShots = append(oneShots,
							map[string]any{"prompt": "Follow up on PR deployment", "run_at": now.Add(15 * time.Minute)},
							map[string]any{"prompt": "Past oneshot", "run_at": now.Add(-15 * time.Minute)},
						)
					}
					_ = json.NewEncoder(w).Encode(map[string]any{
						"status":    "ok",
						"crons":     crons,
						"one_shots": oneShots,
					})
				}))
				defer haroSrv.Close()
				switch tc.envMode {
				case "haro_env":
					t.Setenv("HARO_URL", haroSrv.URL)
				case "coven_env":
					t.Setenv("COVEN_URL", memSrv.URL)
					router.deps.MemoryURL = ""
					router.deps.HaroURL = haroSrv.URL
				default:
					router.deps.HaroURL = haroSrv.URL
				}
			}

			router.deps.Store = store
			if tc.hasDeployFn {
				ds := tc.deployStatus
				router.deps.DeployStatusProvider = func() string { return ds }
			}

			router.Handle(session, mkCmd("int-status", "chan-1", "admin-123", "status"))

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
		name        string
		groups      []metrics.QuotaGroup
		updatedAt   time.Time
		useProvider bool
		httpStatus  int
		wantSubstrs []string
	}{
		{"WithActiveQuotaBuckets", sampleGroups, refreshedAt, true, 0, []string{"**Gemini Models**", "**weekly**: `87%` remaining", "resets in 2h", "Last refreshed: 4m ago", `\u003ct:`}},
		{"EmptyQuotaFallback", nil, time.Time{}, false, 0, []string{"No quota telemetry recorded yet"}},
		{"ErrorLoggingHttp500", nil, time.Time{}, true, http.StatusInternalServerError, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			router, _, transport, session := setupTestRouter("admin-123", tc.httpStatus)
			if tc.useProvider {
				g, u := tc.groups, tc.updatedAt
				router.deps.QuotaProvider = func() []metrics.QuotaGroup { return g }
				router.deps.QuotaTimeProvider = func() time.Time { return u }
			}
			router.Handle(session, mkCmd("int-quota", "chan-1", "admin-123", "quota"))
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
		})
	}
}

func TestRouter_SessionAndQueueCommands(t *testing.T) {
	tests := []struct {
		name         string
		cmdName      string
		opts         []*discordgo.ApplicationCommandInteractionDataOption
		seedPending  bool
		failStore    bool
		withPools    bool
		httpStatus   int
		wantRequests int
		wantSubstrs  []string
		avoidSubstrs []string
	}{
		{"Rotate_BareAndTopic", "rotate", []*discordgo.ApplicationCommandInteractionDataOption{mkOpt("bare", discordgo.ApplicationCommandOptionBoolean, true), mkOpt("topic", discordgo.ApplicationCommandOptionString, "refactor queue")}, false, false, false, 0, 2, []string{"Bare session wipe complete", "refactor queue"}, nil},
		{"Rotate_NormalFresh", "rotate", nil, false, false, false, 0, 2, []string{"Fresh context initialized"}, nil},
		{"Rotate_StoreFailureAndPools", "rotate", nil, false, true, true, 0, 2, nil, nil},
		{"Interrupt_DrainAndReset", "interrupt", []*discordgo.ApplicationCommandInteractionDataOption{mkOpt("drain_queue", discordgo.ApplicationCommandOptionBoolean, true), mkOpt("reset", discordgo.ApplicationCommandOptionBoolean, true)}, true, false, false, 0, 1, []string{"Interrupted active turn execution", "Pending message backlog purged", "Session rotated"}, nil},
		{"Interrupt_NoFlagsAndFailures", "interrupt", []*discordgo.ApplicationCommandInteractionDataOption{mkOpt("drain_queue", discordgo.ApplicationCommandOptionBoolean, false), mkOpt("reset", discordgo.ApplicationCommandOptionBoolean, false)}, false, true, true, 0, 1, []string{"Interrupted active turn execution"}, []string{"Pending message backlog purged", "Session rotated"}},
		{"ErrorLogging_RotateHttp500", "rotate", nil, false, false, false, http.StatusInternalServerError, 1, nil, nil},
		{"ErrorLogging_InterruptHttp500", "interrupt", nil, false, false, false, http.StatusInternalServerError, 1, nil, nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			router, _, transport, session := setupTestRouter("admin-123", tc.httpStatus)
			store := dbtest.NewFakeStore()
			targetChan := "chan-" + tc.name

			if tc.seedPending {
				_ = store.InsertMessage(context.Background(), db.Message{ID: "msg-pending-1", ThreadID: targetChan, Status: db.StatusPending, CreatedAt: time.Now().UTC()})
			}
			if tc.failStore {
				store.FailNext("RotateSessionID", errors.New("db error"))
				store.FailNext("GetPendingOrProcessingMessages", errors.New("db error"))
				store.FailNext("RotateSessionID", errors.New("db error"))
			}
			router.deps.Store = store

			if tc.withPools {
				router.deps.Pool = newMockProcessPools(targetChan)
			}

			router.Handle(session, mkCmd("int-"+tc.name, targetChan, "admin-123", tc.cmdName, tc.opts...))

			reqs := transport.Requests()
			if len(reqs) != tc.wantRequests {
				t.Fatalf("expected %d requests, got %d", tc.wantRequests, len(reqs))
			}

			body := ""
			if len(reqs) == 2 {
				var deferredResp discordgo.InteractionResponse
				_ = json.Unmarshal(reqs[0].Body, &deferredResp)
				if deferredResp.Type != discordgo.InteractionResponseDeferredChannelMessageWithSource {
					t.Errorf("expected deferred response type, got %d", deferredResp.Type)
				}
				body = string(reqs[1].Body)
			} else if len(reqs) == 1 {
				body = string(reqs[0].Body)
			}

			for _, want := range tc.wantSubstrs {
				if !strings.Contains(body, want) {
					t.Errorf("expected body to contain %q, got: %s", want, body)
				}
			}
			for _, avoid := range tc.avoidSubstrs {
				if strings.Contains(body, avoid) {
					t.Errorf("expected body NOT to contain %q, got: %s", avoid, body)
				}
			}

			if tc.seedPending {
				msg, err := store.GetMessage(context.Background(), "msg-pending-1")
				if err != nil || msg.Status != db.StatusFailed {
					t.Errorf("expected message status failed, got %v (err=%v)", msg.Status, err)
				}
			}
		})
	}
}

func TestRouter_ConfigMutationCommands(t *testing.T) {
	oldDelay := deployProgressDelay
	deployProgressDelay = 5 * time.Millisecond
	defer func() { deployProgressDelay = oldDelay }()

	tests := []struct {
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
	}{
		{"Mode_BareQuery", "mode", nil, false, false, false, 0, []string{"**Mode**: `thread`"}, "", nil, false, false, "", ""},
		{"Mode_SetWithGitOps", "mode", []*discordgo.ApplicationCommandInteractionDataOption{mkOpt("mode", discordgo.ApplicationCommandOptionString, "classifier")}, true, false, false, 0, nil, "classifier", nil, false, false, "", ""},
		{"Mode_SetInMemoryFallback", "mode", []*discordgo.ApplicationCommandInteractionDataOption{mkOpt("mode", discordgo.ApplicationCommandOptionString, "mention")}, false, false, true, 0, nil, "mention", nil, false, false, "", ""},
		{"Mode_GitOpsError", "mode", []*discordgo.ApplicationCommandInteractionDataOption{mkOpt("mode", discordgo.ApplicationCommandOptionString, "mention")}, true, true, false, 0, []string{"Failed to apply GitOps update"}, "", nil, false, false, "", ""},
		{"IgnoreBots_BareQuery", "ignore_bots", nil, false, false, false, 0, []string{"**Ignore Bots**:"}, "", nil, false, false, "", ""},
		{"IgnoreBots_SetWithGitOps", "ignore_bots", []*discordgo.ApplicationCommandInteractionDataOption{mkOpt("enabled", discordgo.ApplicationCommandOptionBoolean, true)}, true, false, false, 0, nil, "", boolPtr(true), false, false, "", ""},
		{"IgnoreBots_SetInMemoryFallback", "ignore_bots", []*discordgo.ApplicationCommandInteractionDataOption{mkOpt("enabled", discordgo.ApplicationCommandOptionBoolean, true)}, false, false, true, 0, nil, "", boolPtr(true), false, false, "", ""},
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
			router, cfg, transport, session := setupTestRouter("admin-123", tc.httpStatus)
			if tc.withGitOps {
				router.deps.GitOpsChannelUpdater = mockGitOpsUpdater(cfg, tc.gitOpsErr)
			}

			if tc.runDeployFnTest {
				interaction := &discordgo.Interaction{ID: "int-deploy", Token: "tok-deploy"}
				if tc.testTimeout {
					oldTimeout, oldD := pollerTimeout, deployProgressDelay
					pollerTimeout, deployProgressDelay = 5*time.Millisecond, 100*time.Millisecond
					router.trackDeployProgress(session, interaction, "mode", "chan-1", "thread")
					router.trackDeployProgress(newMockDiscordSession(&mockTransport{respCode: http.StatusInternalServerError}), interaction, "mode", "chan-1", "thread")
					pollerTimeout, deployProgressDelay = oldTimeout, oldD
					return
				}
				var captured config.ChannelPolicy
				router.deps.GitOpsChannelUpdater = func(ctx context.Context, key string, fn func(p *config.ChannelPolicy)) (string, error) {
					fn(&captured)
					return "sha", nil
				}
				router.trackDeployProgress(session, interaction, tc.testMutateKey, "chan-1", tc.testMutateVal)
				if tc.testMutateKey == "mode" && captured.Mode != tc.testMutateVal {
					t.Errorf("expected Mode %q, got %q", tc.testMutateVal, captured.Mode)
				}
				if tc.testMutateKey == "ignore_bots" && (captured.IgnoreBots == nil || !*captured.IgnoreBots) {
					t.Errorf("expected IgnoreBots true")
				}
				return
			}

			chID := "chan-1"
			if tc.cmdName == "ignore_bots" {
				chID = "chan-2"
			}
			router.Handle(session, mkCmd("int-"+tc.name, chID, "admin-123", tc.cmdName, tc.opts...))
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
		})
	}
}

func TestRouter_HandleSync(t *testing.T) {
	tests := []struct {
		name         string
		appID        string
		guildID      string
		nilSession   bool
		bareSession  bool
		noStateUser  bool
		httpStatus   int
		testOnce     bool
		wantErr      bool
		wantRequests int
		wantMethod   string
	}{
		{"RegisterGuildCommands_Success", "app-123", "guild-456", false, false, false, 0, false, false, 1, http.MethodPut},
		{"RegisterGuildCommands_EmptyAppIDUsesState", "", "guild-valid", false, false, false, 0, false, false, 1, http.MethodPut},
		{"RegisterGuildCommands_NilSession", "", "", true, false, false, 0, false, true, 0, ""},
		{"RegisterGuildCommands_BareSessionNoRatelimiter", "", "", false, true, false, 0, false, true, 0, ""},
		{"RegisterGuildCommands_EmptyAppIDNoStateUser", "", "guild-nostate", false, false, true, 0, false, true, 0, ""},
		{"RegisterGuildCommands_EmptyGuildID", "app-123", "", false, false, false, 0, false, true, 0, ""},
		{"RegisterGuildCommands_Http500", "app-123", "guild-err", false, false, false, http.StatusInternalServerError, false, true, 1, ""},
		{"RegisterGuildCommandsOnce_CachingAndReset", "app-123", "guild-once", false, false, false, 0, true, false, 1, ""},
		{"RegisterGuildCommandsOnce_ErrorCleansCache", "app-123", "guild-once-err", false, false, false, http.StatusInternalServerError, true, true, 0, ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ResetRegisteredGuilds()
			defer ResetRegisteredGuilds()

			transport := &mockTransport{respCode: tc.httpStatus}
			var session *discordgo.Session
			if !tc.nilSession {
				if tc.bareSession {
					session = &discordgo.Session{}
				} else {
					session = newMockDiscordSession(transport)
					if tc.noStateUser {
						session.State = nil
					}
				}
			}

			if tc.testOnce {
				cmds, err := RegisterGuildCommandsOnce(session, tc.appID, tc.guildID)
				if (err != nil) != tc.wantErr {
					t.Fatalf("RegisterGuildCommandsOnce() error = %v, wantErr %v", err, tc.wantErr)
				}
				if !tc.wantErr {
					if _, err = RegisterGuildCommandsOnce(session, tc.appID, tc.guildID); err != nil {
						t.Fatalf("second RegisterGuildCommandsOnce() failed: %v", err)
					}
					if len(transport.Requests()) != 1 {
						t.Errorf("expected 1 request due to cache, got %d", len(transport.Requests()))
					}
					ResetRegisteredGuilds()
					if _, err = RegisterGuildCommandsOnce(session, tc.appID, tc.guildID); err != nil {
						t.Fatalf("third RegisterGuildCommandsOnce() failed: %v", err)
					}
					if len(transport.Requests()) != 2 {
						t.Errorf("expected 2 requests after reset, got %d", len(transport.Requests()))
					}
				}
				_ = cmds
				return
			}

			cmds, err := RegisterGuildCommands(session, tc.appID, tc.guildID)
			if (err != nil) != tc.wantErr {
				t.Fatalf("RegisterGuildCommands() error = %v, wantErr %v", err, tc.wantErr)
			}
			reqs := transport.Requests()
			if len(reqs) != tc.wantRequests {
				t.Fatalf("expected %d requests, got %d", tc.wantRequests, len(reqs))
			}
			if tc.wantMethod != "" && len(reqs) > 0 && reqs[0].Method != tc.wantMethod {
				t.Errorf("expected method %s, got %s", tc.wantMethod, reqs[0].Method)
			}
			_ = cmds
		})
	}
}
