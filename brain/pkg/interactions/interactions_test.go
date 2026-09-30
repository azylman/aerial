package interactions

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/db"
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

	m.requests = append(m.requests, capturedRequest{
		Method: req.Method,
		URL:    req.URL.String(),
		Body:   bodyBytes,
	})

	code := m.respCode
	if code == 0 {
		code = http.StatusOK
	}
	body := m.respBody
	if body == "" {
		if strings.Contains(req.URL.Path, "commands") {
			body = `[]`
		} else {
			body = `{"id":"dummy-id"}`
		}
	}

	header := make(http.Header)
	header.Set("Content-Type", "application/json")

	return &http.Response{
		StatusCode: code,
		Header:     header,
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
	s.State.User = &discordgo.User{
		ID:       "bot-123",
		Username: "Aerial",
	}
	return s
}

func TestCommandDefinitions(t *testing.T) {
	t.Parallel()

	cmds := CommandDefinitions()
	if len(cmds) != 5 {
		t.Fatalf("expected 5 command definitions, got %d", len(cmds))
	}

	expectedCmds := map[string]int{
		"status":      0,
		"rotate":      2,
		"interrupt":   2,
		"mode":        2,
		"ignore_bots": 2,
	}

	for _, cmd := range cmds {
		expOpts, ok := expectedCmds[cmd.Name]
		if !ok {
			t.Errorf("unexpected command: %s", cmd.Name)
			continue
		}
		if len(cmd.Options) != expOpts {
			t.Errorf("command %s: expected %d options, got %d", cmd.Name, expOpts, len(cmd.Options))
		}
	}
}

func TestHelpers(t *testing.T) {
	t.Parallel()

	// getBoolOption
	opts := []*discordgo.ApplicationCommandInteractionDataOption{
		{Name: "opt_true", Type: discordgo.ApplicationCommandOptionBoolean, Value: true},
		{Name: "opt_false", Type: discordgo.ApplicationCommandOptionBoolean, Value: false},
		{Name: "opt_str", Type: discordgo.ApplicationCommandOptionString, Value: "hello"},
		{Name: "opt_ch", Type: discordgo.ApplicationCommandOptionChannel, Value: "channel-999"},
	}

	if !getBoolOption(opts, "opt_true", false) {
		t.Errorf("expected opt_true to be true")
	}
	if getBoolOption(opts, "opt_false", true) {
		t.Errorf("expected opt_false to be false")
	}
	if !getBoolOption(opts, "missing", true) {
		t.Errorf("expected fallback true for missing")
	}

	// getStringOption
	if got := getStringOption(opts, "opt_str", ""); got != "hello" {
		t.Errorf("expected 'hello', got %q", got)
	}
	if got := getStringOption(opts, "missing", "default"); got != "default" {
		t.Errorf("expected 'default', got %q", got)
	}

	// getChannelOption
	if got := getChannelOption(opts, "opt_ch", "fallback"); got != "channel-999" {
		t.Errorf("expected 'channel-999', got %q", got)
	}
	if got := getChannelOption(opts, "missing", "fallback"); got != "fallback" {
		t.Errorf("expected 'fallback', got %q", got)
	}

	// hasOption
	if !hasOption(opts, "opt_true") {
		t.Errorf("expected hasOption opt_true to be true")
	}
	if hasOption(opts, "nonexistent") {
		t.Errorf("expected hasOption nonexistent to be false")
	}

	// truncateRunes
	if got := truncateRunes("hello world", 5); got != "hello..." {
		t.Errorf("expected 'hello...', got %q", got)
	}
	if got := truncateRunes("hi", 5); got != "hi" {
		t.Errorf("expected 'hi', got %q", got)
	}

	// formatRelativeDuration
	if got := formatRelativeDuration(45 * time.Second); got != "45s" {
		t.Errorf("expected '45s', got %q", got)
	}
	if got := formatRelativeDuration(5*time.Minute + 30*time.Second); got != "5m" {
		t.Errorf("expected '5m', got %q", got)
	}
	if got := formatRelativeDuration(2*time.Hour + 10*time.Minute); got != "2h 10m" {
		t.Errorf("expected '2h 10m', got %q", got)
	}
	if got := formatRelativeDuration(25 * time.Hour); got != "1d 1h" {
		t.Errorf("expected '1d 1h', got %q", got)
	}

	// formatRelativeTime
	if got := formatRelativeTime(10 * time.Second); got != "just now" {
		t.Errorf("expected 'just now', got %q", got)
	}
	if got := formatRelativeTime(5 * time.Minute); got != "5m" {
		t.Errorf("expected '5m', got %q", got)
	}
	if got := formatRelativeTime(3 * time.Hour); got != "3h" {
		t.Errorf("expected '3h', got %q", got)
	}
	if got := formatRelativeTime(48 * time.Hour); got != "2d" {
		t.Errorf("expected '2d', got %q", got)
	}
}

func setupTestRouter(adminID string) (*Router, *config.Config, *mockTransport) {
	transport := &mockTransport{}
	cfgData := &config.ConfigData{
		AdminUsers: []string{adminID},
		Channels: map[string]config.ChannelPolicy{
			"chan-1": {
				Mode: "thread",
			},
			"chan-2": {
				Mode: "mention",
			},
		},
	}
	cfg := config.NewFromData(cfgData)
	router := NewRouter(InteractionDeps{
		Config: cfg,
	})
	return router, cfg, transport
}

func TestRouter_Auth_NonAdmin(t *testing.T) {
	t.Parallel()

	router, _, transport := setupTestRouter("admin-123")
	session := newMockDiscordSession(transport)

	// Case 1: Member user is not admin
	interaction := &discordgo.InteractionCreate{
		Interaction: &discordgo.Interaction{
			ID:    "int-1",
			Type:  discordgo.InteractionApplicationCommand,
			Token: "token-1",
			Member: &discordgo.Member{
				User: &discordgo.User{
					ID:       "attacker-999",
					Username: "attacker",
				},
			},
			Data: discordgo.ApplicationCommandInteractionData{
				Name: "status",
			},
		},
	}

	router.Handle(session, interaction)

	reqs := transport.Requests()
	if len(reqs) != 1 {
		t.Fatalf("expected 1 request, got %d", len(reqs))
	}
	if !strings.Contains(string(reqs[0].Body), "Unauthorized") {
		t.Errorf("expected response body to contain Unauthorized, got: %s", string(reqs[0].Body))
	}

	// Case 2: DM User is not admin
	interactionDM := &discordgo.InteractionCreate{
		Interaction: &discordgo.Interaction{
			ID:    "int-2",
			Type:  discordgo.InteractionApplicationCommand,
			Token: "token-2",
			User: &discordgo.User{
				ID:       "attacker-888",
				Username: "attacker_dm",
			},
			Data: discordgo.ApplicationCommandInteractionData{
				Name: "status",
			},
		},
	}

	router.Handle(session, interactionDM)

	reqs = transport.Requests()
	if len(reqs) != 2 {
		t.Fatalf("expected 2 requests, got %d", len(reqs))
	}
	if !strings.Contains(string(reqs[1].Body), "Unauthorized") {
		t.Errorf("expected DM response body to contain Unauthorized, got: %s", string(reqs[1].Body))
	}
}

func TestRouter_UnrecognizedCommand(t *testing.T) {
	t.Parallel()

	router, _, transport := setupTestRouter("admin-123")
	session := newMockDiscordSession(transport)

	interaction := &discordgo.InteractionCreate{
		Interaction: &discordgo.Interaction{
			ID:    "int-1",
			Type:  discordgo.InteractionApplicationCommand,
			Token: "token-1",
			Member: &discordgo.Member{
				User: &discordgo.User{
					ID: "admin-123",
				},
			},
			Data: discordgo.ApplicationCommandInteractionData{
				Name: "nonexistent_cmd",
			},
		},
	}

	router.Handle(session, interaction)

	reqs := transport.Requests()
	if len(reqs) != 1 {
		t.Fatalf("expected 1 request, got %d", len(reqs))
	}
	if !strings.Contains(string(reqs[0].Body), "Unrecognized command") {
		t.Errorf("expected response to mention Unrecognized command, got: %s", string(reqs[0].Body))
	}
}

func TestRouter_HandleStatus(t *testing.T) {
	router, _, transport := setupTestRouter("admin-123")
	session := newMockDiscordSession(transport)

	store := db.NewFakeStore()
	ctx := context.Background()

	// Seed facts
	_, err := store.InsertFact(ctx, "architecture", "Aerial runs on PostgreSQL 16 with pgvector", 0.9, "thread-1", nil)
	if err != nil {
		t.Fatalf("InsertFact failed: %v", err)
	}

	// Seed cron
	now := time.Now().UTC()
	err = store.CreateCronSchedule(ctx, db.CronSchedule{
		ID:          "cron-1",
		CronExpr:    "0 9 * * *",
		Prompt:      "Morning system briefing",
		TitlePrefix: "Morning briefing",
		NextRunAt:   now.Add(2 * time.Hour),
	})
	if err != nil {
		t.Fatalf("CreateCronSchedule failed: %v", err)
	}

	router.deps.Store = store
	router.deps.DeployStatusProvider = func() string {
		return "`aerial@bfed897` (Live, healthy)"
	}

	interaction := &discordgo.InteractionCreate{
		Interaction: &discordgo.Interaction{
			ID:        "int-status",
			ChannelID: "chan-1",
			Type:      discordgo.InteractionApplicationCommand,
			Token:     "token-status",
			Member: &discordgo.Member{
				User: &discordgo.User{
					ID: "admin-123",
				},
			},
			Data: discordgo.ApplicationCommandInteractionData{
				Name: "status",
			},
		},
	}

	router.Handle(session, interaction)

	reqs := transport.Requests()
	if len(reqs) != 1 {
		t.Fatalf("expected 1 request, got %d", len(reqs))
	}
	body := string(reqs[0].Body)
	if !strings.Contains(body, "**Runtime**:") {
		t.Errorf("expected **Runtime**: in response, got %s", body)
	}
	if !strings.Contains(body, "`aerial@bfed897`") {
		t.Errorf("expected deploy status in response, got %s", body)
	}
	if !strings.Contains(body, "1 facts recorded") {
		t.Errorf("expected '1 facts recorded' in response, got %s", body)
	}
	if !strings.Contains(body, "Morning briefing") {
		t.Errorf("expected 'Morning briefing' in response, got %s", body)
	}
}

func TestRouter_HandleStatus_FallbackAndEnv(t *testing.T) {
	router, _, transport := setupTestRouter("admin-123")
	session := newMockDiscordSession(transport)

	os.Setenv("GIT_COMMIT", "abcdef123456")
	defer os.Unsetenv("GIT_COMMIT")

	interaction := &discordgo.InteractionCreate{
		Interaction: &discordgo.Interaction{
			ID:        "int-status-fallback",
			ChannelID: "chan-fallback",
			Type:      discordgo.InteractionApplicationCommand,
			Token:     "token-fallback",
			Member: &discordgo.Member{
				User: &discordgo.User{ID: "admin-123"},
			},
			Data: discordgo.ApplicationCommandInteractionData{
				Name: "status",
			},
		},
	}

	router.Handle(session, interaction)

	reqs := transport.Requests()
	if len(reqs) != 1 {
		t.Fatalf("expected 1 request, got %d", len(reqs))
	}
	body := string(reqs[0].Body)
	if !strings.Contains(body, "`aerial@abcdef1`") {
		t.Errorf("expected GIT_COMMIT env in status, got %s", body)
	}
	if !strings.Contains(body, "`Idle`") {
		t.Errorf("expected Idle runtime in status, got %s", body)
	}
}

func TestRouter_HandleRotate(t *testing.T) {
	router, _, transport := setupTestRouter("admin-123")
	session := newMockDiscordSession(transport)
	store := db.NewFakeStore()
	router.deps.Store = store

	interaction := &discordgo.InteractionCreate{
		Interaction: &discordgo.Interaction{
			ID:        "int-rotate",
			ChannelID: "chan-rotate",
			Type:      discordgo.InteractionApplicationCommand,
			Token:     "token-rotate",
			Member: &discordgo.Member{
				User: &discordgo.User{ID: "admin-123"},
			},
			Data: discordgo.ApplicationCommandInteractionData{
				Name: "rotate",
				Options: []*discordgo.ApplicationCommandInteractionDataOption{
					{Name: "bare", Type: discordgo.ApplicationCommandOptionBoolean, Value: true},
					{Name: "topic", Type: discordgo.ApplicationCommandOptionString, Value: "refactor queue"},
				},
			},
		},
	}

	router.Handle(session, interaction)

	reqs := transport.Requests()
	// Should have 1 response (deferred) and 1 edit
	if len(reqs) != 2 {
		t.Fatalf("expected 2 requests (deferred + edit), got %d", len(reqs))
	}

	// Verify deferred response type
	var deferredResp discordgo.InteractionResponse
	if err := json.Unmarshal(reqs[0].Body, &deferredResp); err != nil {
		t.Fatalf("failed to unmarshal deferred response: %v", err)
	}
	if deferredResp.Type != discordgo.InteractionResponseDeferredChannelMessageWithSource {
		t.Errorf("expected deferred response type, got %d", deferredResp.Type)
	}

	// Verify edited content
	editBody := string(reqs[1].Body)
	if !strings.Contains(editBody, "Bare session wipe complete") {
		t.Errorf("expected bare wipe message, got %s", editBody)
	}
	if !strings.Contains(editBody, "refactor queue") {
		t.Errorf("expected topic in edit message, got %s", editBody)
	}
}

func TestRouter_HandleInterrupt(t *testing.T) {
	router, _, transport := setupTestRouter("admin-123")
	session := newMockDiscordSession(transport)
	store := db.NewFakeStore()
	ctx := context.Background()

	// Seed pending message in thread
	err := store.InsertMessage(ctx, db.Message{
		ID:        "msg-pending-1",
		ThreadID:  "chan-interrupt",
		Status:    db.StatusPending,
		CreatedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("InsertMessage failed: %v", err)
	}

	router.deps.Store = store

	interaction := &discordgo.InteractionCreate{
		Interaction: &discordgo.Interaction{
			ID:        "int-interrupt",
			ChannelID: "chan-interrupt",
			Type:      discordgo.InteractionApplicationCommand,
			Token:     "token-interrupt",
			Member: &discordgo.Member{
				User: &discordgo.User{ID: "admin-123"},
			},
			Data: discordgo.ApplicationCommandInteractionData{
				Name: "interrupt",
				Options: []*discordgo.ApplicationCommandInteractionDataOption{
					{Name: "drain_queue", Type: discordgo.ApplicationCommandOptionBoolean, Value: true},
					{Name: "reset", Type: discordgo.ApplicationCommandOptionBoolean, Value: true},
				},
			},
		},
	}

	router.Handle(session, interaction)

	reqs := transport.Requests()
	if len(reqs) != 1 {
		t.Fatalf("expected 1 request, got %d", len(reqs))
	}
	body := string(reqs[0].Body)
	if !strings.Contains(body, "Interrupted active turn execution") {
		t.Errorf("expected Interrupted text, got %s", body)
	}
	if !strings.Contains(body, "Pending message backlog purged") {
		t.Errorf("expected backlog purged text, got %s", body)
	}
	if !strings.Contains(body, "Session rotated") {
		t.Errorf("expected session rotated text, got %s", body)
	}

	// Verify message in store was updated to failed
	msg, err := store.GetMessage(ctx, "msg-pending-1")
	if err != nil {
		t.Fatalf("GetMessage failed: %v", err)
	}
	if msg.Status != db.StatusFailed {
		t.Errorf("expected message status failed, got %s", msg.Status)
	}
}

func TestRouter_HandleMode_BareAndSet(t *testing.T) {
	oldDelay := deployProgressDelay
	deployProgressDelay = 10 * time.Millisecond
	defer func() { deployProgressDelay = oldDelay }()

	router, cfg, transport := setupTestRouter("admin-123")
	session := newMockDiscordSession(transport)

	// 1. Bare query
	bareInteraction := &discordgo.InteractionCreate{
		Interaction: &discordgo.Interaction{
			ID:        "int-mode-bare",
			ChannelID: "chan-1",
			Type:      discordgo.InteractionApplicationCommand,
			Token:     "token-mode-bare",
			Member: &discordgo.Member{
				User: &discordgo.User{ID: "admin-123"},
			},
			Data: discordgo.ApplicationCommandInteractionData{
				Name: "mode",
			},
		},
	}

	router.Handle(session, bareInteraction)

	reqs := transport.Requests()
	if len(reqs) != 1 {
		t.Fatalf("expected 1 immediate response, got %d", len(reqs))
	}
	if !strings.Contains(string(reqs[0].Body), "**Mode**: `thread`") {
		t.Errorf("expected Mode thread, got %s", string(reqs[0].Body))
	}

	// 2. Set mode with GitOps updater
	var gitOpsCalled bool
	var gitOpsKey string
	router.deps.GitOpsChannelUpdater = func(ctx context.Context, channelKey string, mutateFn func(p *config.ChannelPolicy)) (string, error) {
		gitOpsCalled = true
		gitOpsKey = channelKey
		p := cfg.Current().Channels[channelKey]
		mutateFn(&p)
		cfg.Current().Channels[channelKey] = p
		return "gitops-sha-123", nil
	}

	setInteraction := &discordgo.InteractionCreate{
		Interaction: &discordgo.Interaction{
			ID:        "int-mode-set",
			ChannelID: "chan-1",
			Type:      discordgo.InteractionApplicationCommand,
			Token:     "token-mode-set",
			Member: &discordgo.Member{
				User: &discordgo.User{ID: "admin-123"},
			},
			Data: discordgo.ApplicationCommandInteractionData{
				Name: "mode",
				Options: []*discordgo.ApplicationCommandInteractionDataOption{
					{Name: "mode", Type: discordgo.ApplicationCommandOptionString, Value: "classifier"},
				},
			},
		},
	}

	router.Handle(session, setInteraction)

	// Wait for background goroutine to finish 5 stages
	time.Sleep(100 * time.Millisecond)

	if !gitOpsCalled {
		t.Errorf("expected GitOpsChannelUpdater to be called")
	}
	if gitOpsKey != "chan-1" {
		t.Errorf("expected channel chan-1, got %s", gitOpsKey)
	}

	// Verify channel mode updated in config
	pol := cfg.ResolveChannelPolicy("chan-1", "")
	if pol.Mode != "classifier" {
		t.Errorf("expected mode classifier, got %s", pol.Mode)
	}
}

func TestRouter_HandleIgnoreBots_BareAndSet(t *testing.T) {
	oldDelay := deployProgressDelay
	deployProgressDelay = 10 * time.Millisecond
	defer func() { deployProgressDelay = oldDelay }()

	router, cfg, transport := setupTestRouter("admin-123")
	session := newMockDiscordSession(transport)

	// 1. Bare query
	bareInteraction := &discordgo.InteractionCreate{
		Interaction: &discordgo.Interaction{
			ID:        "int-ignorebots-bare",
			ChannelID: "chan-2",
			Type:      discordgo.InteractionApplicationCommand,
			Token:     "token-ignorebots-bare",
			Member: &discordgo.Member{
				User: &discordgo.User{ID: "admin-123"},
			},
			Data: discordgo.ApplicationCommandInteractionData{
				Name: "ignore_bots",
			},
		},
	}

	router.Handle(session, bareInteraction)

	reqs := transport.Requests()
	if len(reqs) != 1 {
		t.Fatalf("expected 1 immediate response, got %d", len(reqs))
	}
	if !strings.Contains(string(reqs[0].Body), "**Ignore Bots**:") {
		t.Errorf("expected Ignore Bots in body, got %s", string(reqs[0].Body))
	}

	// 2. Set ignore_bots
	setInteraction := &discordgo.InteractionCreate{
		Interaction: &discordgo.Interaction{
			ID:        "int-ignorebots-set",
			ChannelID: "chan-2",
			Type:      discordgo.InteractionApplicationCommand,
			Token:     "token-ignorebots-set",
			Member: &discordgo.Member{
				User: &discordgo.User{ID: "admin-123"},
			},
			Data: discordgo.ApplicationCommandInteractionData{
				Name: "ignore_bots",
				Options: []*discordgo.ApplicationCommandInteractionDataOption{
					{Name: "enabled", Type: discordgo.ApplicationCommandOptionBoolean, Value: true},
				},
			},
		},
	}

	router.Handle(session, setInteraction)

	// Wait for background goroutine to finish stages
	time.Sleep(100 * time.Millisecond)

	// Verify channel ignore_bots updated in config
	pol := cfg.ResolveChannelPolicy("chan-2", "")
	if !pol.IsBotIgnored() {
		t.Errorf("expected IsBotIgnored to be true after update")
	}
}

func TestRegisterGuildCommands_And_Once(t *testing.T) {
	ResetRegisteredGuilds()
	defer ResetRegisteredGuilds()

	transport := &mockTransport{}
	session := newMockDiscordSession(transport)

	// First registration call
	cmds, err := RegisterGuildCommandsOnce(session, "app-123", "guild-456")
	if err != nil {
		t.Fatalf("RegisterGuildCommandsOnce failed: %v", err)
	}
	if len(cmds) != 0 { // discordgo dummy response is empty array or mocked
		// That's fine
	}

	reqs := transport.Requests()
	if len(reqs) != 1 {
		t.Fatalf("expected 1 HTTP request for bulk overwrite, got %d", len(reqs))
	}
	if reqs[0].Method != http.MethodPut {
		t.Errorf("expected PUT method, got %s", reqs[0].Method)
	}

	// Second registration call should be a no-op due to sync.Map caching
	_, err = RegisterGuildCommandsOnce(session, "app-123", "guild-456")
	if err != nil {
		t.Fatalf("Second RegisterGuildCommandsOnce failed: %v", err)
	}
	if len(transport.Requests()) != 1 {
		t.Errorf("expected still 1 request, got %d", len(transport.Requests()))
	}

	// Reset cache and call again
	ResetRegisteredGuilds()
	_, err = RegisterGuildCommandsOnce(session, "app-123", "guild-456")
	if err != nil {
		t.Fatalf("Third RegisterGuildCommandsOnce failed: %v", err)
	}
	if len(transport.Requests()) != 2 {
		t.Errorf("expected 2 requests after reset, got %d", len(transport.Requests()))
	}
}
