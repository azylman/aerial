package discord

import (
	"context"
	"errors"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
)

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func jsonHTTPResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

type hydrateRESTCase struct {
	name          string
	token         string
	userStatus    int
	userBody      string
	guildsStatus  int
	guildsBody    string
	rolesStatus   int
	chansStatus   int
	threadsStatus int
	wantErr       bool
	wantGuildID   string
	wantChanID    string
	wantThreadID  string
	wantRoleID    string
}

func runHydrateRoundTrip(req *http.Request, tc hydrateRESTCase) (*http.Response, error) {
	p := req.URL.Path
	switch {
	case strings.Contains(p, "/users/@me/guilds"):
		if tc.guildsStatus != 0 {
			return jsonHTTPResponse(tc.guildsStatus, tc.guildsBody), nil
		}
		return jsonHTTPResponse(http.StatusOK, `[]`), nil
	case strings.Contains(p, "/users/@me"):
		if tc.userStatus != 0 {
			return jsonHTTPResponse(tc.userStatus, tc.userBody), nil
		}
		return jsonHTTPResponse(http.StatusOK, `{"id":"bot-1","username":"Aerial"}`), nil
	case strings.Contains(p, "/roles"):
		if tc.rolesStatus != 0 {
			return jsonHTTPResponse(tc.rolesStatus, `{"message":"roles error"}`), nil
		}
		return jsonHTTPResponse(http.StatusOK, `[{"id":"role-hydrated-1","name":"Admins"},null]`), nil
	case strings.Contains(p, "/threads/active"):
		if tc.threadsStatus != 0 {
			return jsonHTTPResponse(tc.threadsStatus, `{"message":"threads error"}`), nil
		}
		return jsonHTTPResponse(http.StatusOK, `{"threads":[{"id":"th-hydrated-1","name":"Active Thread","type":11},null]}`), nil
	case strings.Contains(p, "/channels"):
		if tc.chansStatus != 0 {
			return jsonHTTPResponse(tc.chansStatus, `{"message":"channels error"}`), nil
		}
		return jsonHTTPResponse(http.StatusOK, `[{"id":"ch-hydrated-1","name":"general","type":0},null]`), nil
	case strings.Contains(p, "/members/"):
		return jsonHTTPResponse(http.StatusOK, `{"user":{"id":"bot-hydrate-1"},"roles":["role-bot-1"]}`), nil
	default:
		return jsonHTTPResponse(http.StatusOK, `{}`), nil
	}
}

func TestHydrateSessionREST_Guards(t *testing.T) {
	t.Parallel()

	if err := HydrateSessionREST(context.Background(), nil, HydrateOptions{}); err != nil {
		t.Fatalf("HydrateSessionREST(nil) error = %v", err)
	}

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	sCanceled, err := discordgo.New("Bot mock-token")
	if err != nil {
		t.Fatalf("discordgo.New() error = %v", err)
	}
	if hErr := HydrateSessionREST(canceledCtx, sCanceled, HydrateOptions{}); !errors.Is(hErr, context.Canceled) {
		t.Fatalf("HydrateSessionREST(canceled) = %v, want context.Canceled", hErr)
	}

	sNilState, err := discordgo.New("Bot mock-token")
	if err != nil {
		t.Fatalf("discordgo.New() error = %v", err)
	}
	sNilState.State = nil
	sNilState.Client = &http.Client{
		Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			return runHydrateRoundTrip(req, hydrateRESTCase{userStatus: http.StatusOK, userBody: `{"id":"bot-nil-state"}`, guildsStatus: http.StatusOK, guildsBody: `[]`})
		}),
	}
	if err := HydrateSessionREST(context.Background(), sNilState, HydrateOptions{}); err != nil {
		t.Fatalf("HydrateSessionREST(nil state) error = %v", err)
	}
	if sNilState.State == nil || sNilState.State.User == nil || sNilState.State.User.ID != "bot-nil-state" {
		t.Fatalf("expected auto-initialized State.User, got %+v", sNilState.State)
	}
}

func assertHydrateRESTCase(t *testing.T, tc hydrateRESTCase) {
	t.Helper()
	s, err := discordgo.New("Bot " + tc.token)
	if err != nil {
		t.Fatalf("discordgo.New() error = %v", err)
	}
	s.StateEnabled = true
	s.State = discordgo.NewState()
	s.Client = &http.Client{
		Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			return runHydrateRoundTrip(req, tc)
		}),
	}

	var mu sync.Mutex
	var seenChannels, seenGuilds []string
	opts := HydrateOptions{
		OnChannel: func(ch *discordgo.Channel) {
			mu.Lock()
			seenChannels = append(seenChannels, ch.ID)
			mu.Unlock()
		},
		OnGuild: func(_ *discordgo.Session, appID, guildID string) {
			mu.Lock()
			seenGuilds = append(seenGuilds, appID+":"+guildID)
			mu.Unlock()
		},
	}

	err = HydrateSessionREST(context.Background(), s, opts)
	if (err != nil) != tc.wantErr {
		t.Fatalf("HydrateSessionREST() error = %v, wantErr = %v", err, tc.wantErr)
	}
	if tc.wantGuildID != "" {
		if _, gErr := s.State.Guild(tc.wantGuildID); gErr != nil {
			t.Errorf("expected guild %s in state: %v", tc.wantGuildID, gErr)
		}
	}
	for _, chID := range []string{tc.wantChanID, tc.wantThreadID} {
		if chID == "" {
			continue
		}
		if !slices.Contains(seenChannels, chID) {
			t.Errorf("expected OnChannel(%s), got %v", chID, seenChannels)
		}
		if _, cErr := s.State.Channel(chID); cErr != nil {
			t.Errorf("expected channel %s in state: %v", chID, cErr)
		}
	}
	if tc.wantRoleID != "" {
		if _, rErr := s.State.Role(tc.wantGuildID, tc.wantRoleID); rErr != nil {
			t.Errorf("expected role %s in state: %v", tc.wantRoleID, rErr)
		}
		if !slices.Contains(seenGuilds, "bot-hydrate-1:"+tc.wantGuildID) {
			t.Errorf("expected OnGuild for %s, got %v", tc.wantGuildID, seenGuilds)
		}
	}
}

func TestHydrateSessionREST(t *testing.T) {
	t.Parallel()

	tests := []hydrateRESTCase{
		{name: "EmptyToken", token: ""},
		{name: "UserMeError", token: "mock-token", userStatus: http.StatusInternalServerError, userBody: `{"message":"err"}`, wantErr: true},
		{name: "UserGuildsError", token: "mock-token", userStatus: http.StatusOK, userBody: `{"id":"bot-hydrate-1"}`, guildsStatus: http.StatusBadGateway, guildsBody: `{"message":"bad gateway"}`, wantErr: true},
		{name: "EmptyGuildIDSkipped", token: "mock-token", userStatus: http.StatusOK, userBody: `{"id":"bot-hydrate-1"}`, guildsStatus: http.StatusOK, guildsBody: `[null,{"id":""}]`},
		{name: "GuildSubcallErrorsIgnored", token: "mock-token", userStatus: http.StatusOK, userBody: `{"id":"bot-hydrate-1"}`, guildsStatus: http.StatusOK, guildsBody: `[{"id":"g-err-1","name":"ErrGuild"}]`, rolesStatus: http.StatusInternalServerError, chansStatus: http.StatusInternalServerError, threadsStatus: http.StatusInternalServerError, wantGuildID: "g-err-1"},
		{name: "SuccessfulHydration", token: "mock-token", userStatus: http.StatusOK, userBody: `{"id":"bot-hydrate-1","username":"Aerial"}`, guildsStatus: http.StatusOK, guildsBody: `[{"id":"g-hydrated-1","name":"Main Guild"}]`, wantGuildID: "g-hydrated-1", wantChanID: "ch-hydrated-1", wantThreadID: "th-hydrated-1", wantRoleID: "role-hydrated-1"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertHydrateRESTCase(t, tc)
		})
	}
}

func TestHydrateSessionREST_UnknownGuildStateErrorsAndNilContext(t *testing.T) {
	t.Parallel()

	s, err := discordgo.New("Bot mock-token")
	if err != nil {
		t.Fatalf("discordgo.New() error = %v", err)
	}
	s.State = discordgo.NewState()
	s.Client = &http.Client{
		Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			return runHydrateRoundTrip(req, hydrateRESTCase{})
		}),
	}

	var chCount int
	hydrateGuildRoles(context.Background(), s, "g-unknown", "[test] ")
	hydrateGuildChannels(context.Background(), s, "g-unknown", func(*discordgo.Channel) { chCount++ }, "[test] ")
	hydrateGuildThreads(context.Background(), s, "g-unknown", func(*discordgo.Channel) { chCount++ }, "[test] ")
	if chCount != 2 {
		t.Errorf("expected 2 OnChannel calls even when state insert fails, got %d", chCount)
	}
}

func TestHydrateSessionREST_CancelMidGuildLoop(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	s, err := discordgo.New("Bot mock-token")
	if err != nil {
		t.Fatalf("discordgo.New() error = %v", err)
	}
	s.Client = &http.Client{
		Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			if strings.Contains(req.URL.Path, "/users/@me/guilds") {
				cancel()
				return jsonHTTPResponse(http.StatusOK, `[{"id":"g-1","name":"Guild 1"}]`), nil
			}
			return jsonHTTPResponse(http.StatusOK, `{"id":"bot-1"}`), nil
		}),
	}
	if err := HydrateSessionREST(ctx, s, HydrateOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestSyncBotMemberRoles(t *testing.T) {
	t.Parallel()

	var fetchCalls atomic.Int32
	var shouldFail atomic.Bool
	opts := HydrateOptions{
		MemberFetcher: func(_ *discordgo.Session, guildID, userID string) (*discordgo.Member, error) {
			fetchCalls.Add(1)
			if shouldFail.Load() {
				return nil, errors.New("simulated fetch failure")
			}
			return &discordgo.Member{
				User:  &discordgo.User{ID: userID},
				Roles: []string{"mock-role-robot-123"},
			}, nil
		},
	}

	s := &discordgo.Session{State: discordgo.NewState()}
	if err := s.State.GuildAdd(&discordgo.Guild{ID: "guild-1"}); err != nil {
		t.Fatalf("GuildAdd error = %v", err)
	}

	SyncBotMemberRoles(context.Background(), nil, "guild-1", "bot-1", opts)
	SyncBotMemberRoles(context.Background(), s, "", "bot-1", opts)
	SyncBotMemberRoles(context.Background(), s, "guild-1", "", opts)
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	SyncBotMemberRoles(canceledCtx, s, "guild-1", "bot-1", opts)
	if got := fetchCalls.Load(); got != 0 {
		t.Fatalf("expected 0 fetch calls for invalid/canceled args, got %d", got)
	}

	shouldFail.Store(true)
	SyncBotMemberRoles(context.Background(), s, "guild-1", "bot-1", opts)
	if got := fetchCalls.Load(); got != 1 {
		t.Fatalf("expected 1 fetch call on failure, got %d", got)
	}

	shouldFail.Store(false)
	SyncBotMemberRoles(context.Background(), s, "guild-1", "bot-1", opts)
	if got := fetchCalls.Load(); got != 2 {
		t.Fatalf("expected 2 fetch calls total, got %d", got)
	}

	m, err := s.State.Member("guild-1", "bot-1")
	if err != nil || len(m.Roles) != 1 || m.Roles[0] != "mock-role-robot-123" || m.GuildID != "guild-1" {
		t.Fatalf("unexpected cached member: %+v, err=%v", m, err)
	}

	// Subsequent call hits state cache.
	SyncBotMemberRoles(context.Background(), s, "guild-1", "bot-1", opts)
	if got := fetchCalls.Load(); got != 2 {
		t.Fatalf("expected cached hit without fetch, got %d", got)
	}

	// Unknown guild logs warning without panic.
	SyncBotMemberRoles(context.Background(), s, "guild-not-in-state", "bot-1", opts)

	// Default REST MemberFetcher with nil ctx.
	restSess, err := discordgo.New("Bot mock-token")
	if err != nil {
		t.Fatalf("discordgo.New() error = %v", err)
	}
	restSess.State = discordgo.NewState()
	if err := restSess.State.GuildAdd(&discordgo.Guild{ID: "guild-rest"}); err != nil {
		t.Fatalf("GuildAdd error = %v", err)
	}
	restSess.Client = &http.Client{
		Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			return jsonHTTPResponse(http.StatusOK, `{"user":{"id":"bot-rest"},"roles":["role-rest-1"]}`), nil
		}),
	}
	SyncBotMemberRoles(context.Background(), restSess, "guild-rest", "bot-rest", HydrateOptions{})
	if rm, rErr := restSess.State.Member("guild-rest", "bot-rest"); rErr != nil || !slices.Equal(rm.Roles, []string{"role-rest-1"}) {
		t.Fatalf("expected REST-fetched member in state, got %+v (err=%v)", rm, rErr)
	}
}

func TestOpenSessionWithRetry_ShortCircuits(t *testing.T) {
	t.Parallel()

	s := &discordgo.Session{}
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	var called atomic.Int32
	opts := HydrateOptions{
		HydrateFunc: func(context.Context, *discordgo.Session) error {
			called.Add(1)
			return nil
		},
	}

	OpenSessionWithRetry(canceledCtx, s, opts)()
	OpenSessionWithRetry(context.Background(), nil, opts)()
	if got := called.Load(); got != 0 {
		t.Fatalf("expected 0 HydrateFunc calls on short-circuit, got %d", got)
	}
}

func TestOpenSessionWithRetry_RetriesAndStops(t *testing.T) {
	t.Parallel()

	s := &discordgo.Session{}
	var attempts atomic.Int32
	done := make(chan struct{})
	stop := OpenSessionWithRetry(context.Background(), s, HydrateOptions{
		RetryBackoff: time.Millisecond,
		LogPrefix:    "[test] ",
		HydrateFunc: func(context.Context, *discordgo.Session) error {
			if attempts.Add(1) < 3 {
				return errors.New("transient error")
			}
			close(done)
			return nil
		},
	})
	defer stop()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for retry loop")
	}
	stop()
	if got := attempts.Load(); got != 3 {
		t.Fatalf("attempts = %d, want 3", got)
	}
}

func TestOpenSessionWithRetry_DefaultHydrator(t *testing.T) {
	t.Parallel()

	s, err := discordgo.New("Bot mock-token")
	if err != nil {
		t.Fatalf("discordgo.New() error = %v", err)
	}
	guildsRequested := make(chan struct{})
	var once sync.Once
	s.Client = &http.Client{
		Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			if strings.Contains(req.URL.Path, "/users/@me/guilds") {
				once.Do(func() { close(guildsRequested) })
				return jsonHTTPResponse(http.StatusOK, `[]`), nil
			}
			return jsonHTTPResponse(http.StatusOK, `{"id":"bot-default-1"}`), nil
		}),
	}

	stop := OpenSessionWithRetry(context.Background(), s, HydrateOptions{})
	defer stop()

	select {
	case <-guildsRequested:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for default hydrator")
	}
	stop()
	if s.State == nil || s.State.User == nil || s.State.User.ID != "bot-default-1" {
		t.Fatalf("expected default hydrator to populate State.User, got %+v", s.State)
	}
}

func TestOpenSessionWithRetry_StopCancelsBackoff(t *testing.T) {
	t.Parallel()

	s := &discordgo.Session{}
	firstAttempt := make(chan struct{})
	var once sync.Once
	stop := OpenSessionWithRetry(context.Background(), s, HydrateOptions{
		RetryBackoff: 10 * time.Second,
		HydrateFunc: func(context.Context, *discordgo.Session) error {
			once.Do(func() { close(firstAttempt) })
			return errors.New("simulated failure")
		},
	})
	select {
	case <-firstAttempt:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for initial hydration attempt")
	}

	stopped := make(chan struct{})
	go func() {
		stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("stop() did not cancel 10s backoff promptly")
	}
}
