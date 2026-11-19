package discord

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"

	"github.com/azylman/aerial/services/synapse/internal/model"
)

func TestChannelWatermarks(t *testing.T) {
	ResetWatermarksForTest()
	t.Cleanup(ResetWatermarksForTest)

	tests := []struct {
		name       string
		channelID  string
		advanceID  string
		wantBefore string
		wantAfter  string
	}{
		{
			name:       "empty channel returns empty",
			channelID:  "chan-new",
			advanceID:  "",
			wantBefore: "",
			wantAfter:  "",
		},
		{
			name:       "first advance sets watermark",
			channelID:  "chan-new",
			advanceID:  "100",
			wantBefore: "",
			wantAfter:  "100",
		},
		{
			name:       "smaller advance does not regress watermark",
			channelID:  "chan-new",
			advanceID:  "50",
			wantBefore: "100",
			wantAfter:  "100",
		},
		{
			name:       "larger advance advances watermark",
			channelID:  "chan-new",
			advanceID:  "200",
			wantBefore: "100",
			wantAfter:  "200",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			before := GetChannelWatermark(tc.channelID)
			if before != tc.wantBefore {
				t.Errorf("before = %q, want %q", before, tc.wantBefore)
			}
			AdvanceChannelWatermark(tc.channelID, tc.advanceID)
			after := GetChannelWatermark(tc.channelID)
			if after != tc.wantAfter {
				t.Errorf("after = %q, want %q", after, tc.wantAfter)
			}
		})
	}
}

type sweepTestCase struct {
	name                string
	nilSession          bool
	nilServer           bool
	sweepAlreadyRunning bool
	canceledCtx         bool
	now                 time.Time
	botUserID           string
	initialWatermark    string
	channelID           string
	lastMessageID       string
	channelGuildID      string
	guildID             string
	nilGuildInState     bool
	nilChannelInGuild   bool
	threadsInState      []*discordgo.Channel
	restChannels        []*discordgo.Channel
	restThreads         []*discordgo.Channel
	restChannelsErr     bool
	restThreadsErr      bool
	userGuildsResp      []*discordgo.UserGuild
	userGuildsErr       bool
	messagesResp        []*discordgo.Message
	messagesErrCode     int
	hasBus              bool
	runSecondSweep      bool

	wantFetchCalls  int
	wantAfterQuery  string
	wantEnqueuedIDs []string
	wantWatermark   string
}

func buildCatchUpGuildChannels(tc sweepTestCase) []*discordgo.Channel {
	var chs []*discordgo.Channel
	if tc.nilChannelInGuild {
		chs = append(chs, nil, &discordgo.Channel{ID: ""})
	}
	if tc.channelID != "" {
		chs = append(chs, &discordgo.Channel{
			ID:            tc.channelID,
			GuildID:       tc.channelGuildID,
			LastMessageID: tc.lastMessageID,
		})
	}
	return chs
}

func setupCatchUpSweepSession(t *testing.T, tc sweepTestCase) (*discordgo.Session, *mockDiscordTransport) {
	t.Helper()
	if tc.nilSession {
		return nil, nil
	}
	session, err := discordgo.New("Bot dummy-token")
	if err != nil {
		t.Fatalf("discordgo.New() err = %v", err)
	}
	gID := tc.guildID
	if gID == "" {
		gID = "default-guild"
	}
	transport := &mockDiscordTransport{
		messagesResp:    map[string][]*discordgo.Message{tc.channelID: tc.messagesResp},
		messagesErrCode: map[string]int{tc.channelID: tc.messagesErrCode},
		channelsResp:    map[string][]*discordgo.Channel{gID: tc.restChannels},
		channelsErr:     map[string]bool{gID: tc.restChannelsErr},
		threadsResp:     map[string]*discordgo.ThreadsList{gID: {Threads: tc.restThreads}},
		threadsErr:      map[string]bool{gID: tc.restThreadsErr},
		userGuildsResp:  tc.userGuildsResp,
		userGuildsErr:   tc.userGuildsErr,
	}
	session.Client = &http.Client{Transport: transport}
	if tc.botUserID != "" {
		session.State.User = &discordgo.User{ID: tc.botUserID}
	}
	switch {
	case tc.userGuildsResp != nil || tc.userGuildsErr:
		session.State.Guilds = nil
	case tc.channelID != "" || tc.nilGuildInState:
		chs := buildCatchUpGuildChannels(tc)
		if tc.nilGuildInState {
			session.State.Guilds = append(session.State.Guilds, nil, &discordgo.Guild{ID: ""})
		}
		session.State.Guilds = append(session.State.Guilds, &discordgo.Guild{
			ID:       gID,
			Channels: chs,
			Threads:  tc.threadsInState,
		})
	}
	return session, transport
}

func assertCatchUpSweepTransport(t *testing.T, tc sweepTestCase, transport *mockDiscordTransport) {
	t.Helper()
	if transport == nil {
		return
	}
	if len(transport.messageCalls) != tc.wantFetchCalls {
		t.Errorf("fetch calls = %d, want %d", len(transport.messageCalls), tc.wantFetchCalls)
	}
	if tc.wantAfterQuery != "" && len(transport.afterQueries) > 0 {
		if transport.afterQueries[0] != tc.wantAfterQuery {
			t.Errorf("after query = %q, want %q", transport.afterQueries[0], tc.wantAfterQuery)
		}
	}
}

func assertCatchUpSweepBus(t *testing.T, tc sweepTestCase, bus *testEventBus) {
	t.Helper()
	if len(bus.insertedJobs) != len(tc.wantEnqueuedIDs) {
		t.Errorf("insertedJobs = %d, want %d", len(bus.insertedJobs), len(tc.wantEnqueuedIDs))
	}
	for idx, wantID := range tc.wantEnqueuedIDs {
		if idx < len(bus.insertedJobs) {
			if args, ok := bus.insertedJobs[idx].(model.DiscordIngressArgs); ok && args.MessageID != wantID {
				t.Errorf("job[%d] MessageID = %q, want %q", idx, args.MessageID, wantID)
			}
		}
	}
}

func assertCatchUpSweepServer(t *testing.T, tc sweepTestCase, srv *testIngressServer) {
	t.Helper()
	if srv == nil {
		return
	}
	calls := srv.processedIngress
	if len(calls) != len(tc.wantEnqueuedIDs) {
		t.Errorf("processedIngress = %d, want %d", len(calls), len(tc.wantEnqueuedIDs))
	}
	for idx, wantID := range tc.wantEnqueuedIDs {
		if idx < len(calls) && calls[idx].Message != nil && calls[idx].Message.ID != wantID {
			t.Errorf("call[%d] MessageID = %q, want %q", idx, calls[idx].Message.ID, wantID)
		}
	}
	if len(calls) > 1 && tc.guildID != "" {
		if calls[1].Message != nil && calls[1].Message.GuildID != tc.guildID {
			t.Errorf("call[1] GuildID = %q, want %q", calls[1].Message.GuildID, tc.guildID)
		}
		if calls[1].Message != nil && calls[1].Message.ReferencedMessage != nil && calls[1].Message.ReferencedMessage.GuildID != tc.guildID {
			t.Errorf("call[1] RefMessage GuildID = %q, want %q", calls[1].Message.ReferencedMessage.GuildID, tc.guildID)
		}
	}
}

func assertCatchUpSweepResults(t *testing.T, tc sweepTestCase, srv *testIngressServer, bus *testEventBus, transport *mockDiscordTransport) {
	t.Helper()
	assertCatchUpSweepTransport(t, tc, transport)
	if tc.hasBus {
		assertCatchUpSweepBus(t, tc, bus)
		return
	}
	assertCatchUpSweepServer(t, tc, srv)
}

func runDiscordCatchUpSweepCase(ctx context.Context, t *testing.T, tc sweepTestCase) {
	t.Helper()
	ResetWatermarksForTest()
	defer ResetWatermarksForTest()

	if tc.initialWatermark != "" && tc.channelID != "" {
		AdvanceChannelWatermark(tc.channelID, tc.initialWatermark)
	}

	srv := &testIngressServer{}
	bus := &testEventBus{}
	if tc.hasBus {
		srv.bus = bus
	}

	targetCtx := ctx
	if tc.canceledCtx {
		cCtx, cancel := context.WithCancel(ctx)
		cancel()
		targetCtx = cCtx
	}

	session, transport := setupCatchUpSweepSession(t, tc)

	if tc.sweepAlreadyRunning {
		cleanup := SetSweepRunningForTest(true)
		defer cleanup()
	}

	var targetServer IngressServer
	if !tc.nilServer {
		targetServer = srv
	}

	RunCatchUpSweep(targetCtx, session, targetServer, tc.now)

	if tc.runSecondSweep {
		RunCatchUpSweep(targetCtx, session, targetServer, tc.now)
	}

	assertCatchUpSweepResults(t, tc, srv, bus, transport)

	if tc.channelID != "" && tc.wantWatermark != "" {
		wm := GetChannelWatermark(tc.channelID)
		if wm != tc.wantWatermark {
			t.Errorf("channel watermark = %q, want %q", wm, tc.wantWatermark)
		}
	}
}

func discordCatchUpSweepCases(baseNow time.Time, minSnow, staleSnow, freshSnow1, freshSnow2, botSnow string, freshMsg1, freshMsg2, botMsg, staleMsg *discordgo.Message) []sweepTestCase {
	return []sweepTestCase{
		{
			name:       "nil session returns immediately",
			nilSession: true,
		},
		{
			name:      "nil server returns immediately",
			nilServer: true,
		},
		{
			name:                "concurrent sweep already running returns immediately",
			sweepAlreadyRunning: true,
		},
		{
			name:        "canceled context halts sweep early",
			now:         baseNow,
			canceledCtx: true,
			channelID:   "chan-canceled",
		},
		{
			name:          "zero now defaults to UTC now",
			now:           time.Time{},
			channelID:     "chan-zero-now",
			lastMessageID: "",
		},
		{
			name:          "fast-path skip on empty LastMessageID",
			now:           baseNow,
			channelID:     "chan-sweep-1",
			lastMessageID: "",
		},
		{
			name:          "fast-path skip when LastMessageID <= minSnowflake",
			now:           baseNow,
			channelID:     "chan-sweep-2",
			lastMessageID: staleSnow,
		},
		{
			name:             "fast-path skip when LastMessageID <= watermark",
			now:              baseNow,
			channelID:        "chan-sweep-3",
			lastMessageID:    freshSnow1,
			initialWatermark: freshSnow1,
		},
		{
			name:            "ChannelMessages fetch error logged and channel skipped",
			now:             baseNow,
			channelID:       "chan-sweep-4",
			lastMessageID:   freshSnow2,
			messagesErrCode: http.StatusInternalServerError,
			wantFetchCalls:  1,
			wantAfterQuery:  minSnow,
		},
		{
			name:          "chronological enqueue skipping stale and bot messages, advancing watermark",
			now:           baseNow,
			botUserID:     "bot-123",
			channelID:     "chan-sweep-5",
			guildID:       "guild-123",
			lastMessageID: freshSnow2,
			// Discord REST returns descending (newest first)
			messagesResp:    []*discordgo.Message{freshMsg2, freshMsg1, botMsg, staleMsg},
			wantFetchCalls:  1,
			wantAfterQuery:  minSnow,
			wantEnqueuedIDs: []string{freshSnow1, freshSnow2},
			wantWatermark:   freshSnow2,
		},
		{
			name:            "subsequent sweep makes zero calls once watermark advanced",
			now:             baseNow,
			botUserID:       "bot-123",
			channelID:       "chan-sweep-6",
			guildID:         "guild-123",
			lastMessageID:   freshSnow2,
			messagesResp:    []*discordgo.Message{freshMsg2, freshMsg1},
			runSecondSweep:  true,
			wantFetchCalls:  1,
			wantAfterQuery:  minSnow,
			wantEnqueuedIDs: []string{freshSnow1, freshSnow2},
			wantWatermark:   freshSnow2,
		},
		{
			name:            "bus client enqueue in chronological order",
			now:             baseNow,
			botUserID:       "bot-123",
			channelID:       "chan-sweep-7",
			guildID:         "guild-123",
			lastMessageID:   freshSnow2,
			hasBus:          true,
			messagesResp:    []*discordgo.Message{freshMsg2, freshMsg1},
			wantFetchCalls:  1,
			wantAfterQuery:  minSnow,
			wantEnqueuedIDs: []string{freshSnow1, freshSnow2},
			wantWatermark:   freshSnow2,
		},
		{
			name:           "empty state guilds fetches user guilds from REST",
			now:            baseNow,
			userGuildsResp: []*discordgo.UserGuild{{ID: "user-guild-1"}},
			restChannels:   []*discordgo.Channel{{ID: "ch-rest-1", LastMessageID: ""}},
			wantFetchCalls: 0,
		},
		{
			name:          "user guilds fetch error handled gracefully",
			now:           baseNow,
			userGuildsErr: true,
		},
		{
			name:            "guild channels and active threads error handled gracefully",
			now:             baseNow,
			guildID:         "guild-err",
			channelID:       "chan-err-fallback",
			lastMessageID:   "",
			restChannelsErr: true,
			restThreadsErr:  true,
		},
		{
			name:           "REST active threads and state threads merged and processed",
			now:            baseNow,
			guildID:        "guild-threads",
			channelID:      "chan-main",
			lastMessageID:  "",
			restThreads:    []*discordgo.Channel{{ID: "thread-rest", LastMessageID: ""}},
			threadsInState: []*discordgo.Channel{{ID: "thread-state", LastMessageID: ""}},
		},
		{
			name:              "nil guild in state and nil channel ignored safely",
			now:               baseNow,
			guildID:           "guild-valid",
			channelID:         "chan-valid",
			lastMessageID:     "",
			nilGuildInState:   true,
			nilChannelInGuild: true,
		},
		{
			name:            "nil and empty ID messages in fetched batch skipped",
			now:             baseNow,
			guildID:         "guild-nil-msg",
			channelID:       "chan-nil-msg",
			lastMessageID:   freshSnow2,
			messagesResp:    []*discordgo.Message{nil, {ID: ""}, freshMsg2},
			wantFetchCalls:  1,
			wantAfterQuery:  minSnow,
			wantEnqueuedIDs: []string{freshSnow2},
			wantWatermark:   freshSnow2,
		},
	}
}

func TestRunDiscordCatchUpSweep(t *testing.T) {
	ctx := context.Background()

	baseNow := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	minSnow := DiscordSnowflakeFromTime(baseNow.Add(-30 * time.Minute))
	staleSnow := DiscordSnowflakeFromTime(baseNow.Add(-45 * time.Minute))
	freshSnow1 := DiscordSnowflakeFromTime(baseNow.Add(-15 * time.Minute))
	freshSnow2 := DiscordSnowflakeFromTime(baseNow.Add(-5 * time.Minute))
	botSnow := DiscordSnowflakeFromTime(baseNow.Add(-10 * time.Minute))

	freshMsg1 := &discordgo.Message{
		ID:        freshSnow1,
		ChannelID: "chan-sweep-5",
		Author:    &discordgo.User{ID: "user-alpha"},
		Content:   "older fresh message",
		Timestamp: baseNow.Add(-15 * time.Minute),
	}
	freshMsg2 := &discordgo.Message{
		ID:        freshSnow2,
		ChannelID: "chan-sweep-5",
		Author:    &discordgo.User{ID: "user-beta"},
		Content:   "newer fresh message",
		Timestamp: baseNow.Add(-5 * time.Minute),
		ReferencedMessage: &discordgo.Message{
			ID: "ref-parent",
		},
	}
	botMsg := &discordgo.Message{
		ID:        botSnow,
		ChannelID: "chan-sweep-5",
		Author:    &discordgo.User{ID: "bot-123"},
		Content:   "bot response",
		Timestamp: baseNow.Add(-10 * time.Minute),
	}
	staleMsg := &discordgo.Message{
		ID:        staleSnow,
		ChannelID: "chan-sweep-5",
		Author:    &discordgo.User{ID: "user-gamma"},
		Content:   "stale message older than 30m",
		Timestamp: baseNow.Add(-45 * time.Minute),
	}

	tests := discordCatchUpSweepCases(baseNow, minSnow, staleSnow, freshSnow1, freshSnow2, botSnow, freshMsg1, freshMsg2, botMsg, staleMsg)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runDiscordCatchUpSweepCase(ctx, t, tc)
		})
	}
}

type mockDiscordTransport struct {
	mu              sync.Mutex
	messageCalls    []string
	afterQueries    []string
	channelsResp    map[string][]*discordgo.Channel
	channelsErr     map[string]bool
	threadsResp     map[string]*discordgo.ThreadsList
	threadsErr      map[string]bool
	userGuildsResp  []*discordgo.UserGuild
	userGuildsErr   bool
	messagesResp    map[string][]*discordgo.Message
	messagesErrCode map[string]int
}

func extractPathSegmentAfter(path, segment string) string {
	parts := strings.Split(path, "/")
	for idx, p := range parts {
		if p == segment && idx+1 < len(parts) {
			return parts[idx+1]
		}
	}
	return ""
}

func (m *mockDiscordTransport) roundTripGuilds() (int, []byte) {
	if m.userGuildsErr {
		return http.StatusInternalServerError, []byte(`{"message": "error"}`)
	}
	bodyBytes, _ := json.Marshal(m.userGuildsResp)
	return http.StatusOK, bodyBytes
}

func (m *mockDiscordTransport) roundTripThreads(path string) (int, []byte) {
	guildID := extractPathSegmentAfter(path, "guilds")
	if m.threadsErr != nil && m.threadsErr[guildID] {
		return http.StatusInternalServerError, []byte(`{"message": "error"}`)
	}
	thList := &discordgo.ThreadsList{}
	if m.threadsResp != nil && m.threadsResp[guildID] != nil {
		thList = m.threadsResp[guildID]
	}
	bodyBytes, _ := json.Marshal(thList)
	return http.StatusOK, bodyBytes
}

func (m *mockDiscordTransport) roundTripChannels(path string) (int, []byte) {
	guildID := extractPathSegmentAfter(path, "guilds")
	if m.channelsErr != nil && m.channelsErr[guildID] {
		return http.StatusInternalServerError, []byte(`{"message": "error"}`)
	}
	var chList []*discordgo.Channel
	if m.channelsResp != nil && m.channelsResp[guildID] != nil {
		chList = m.channelsResp[guildID]
	}
	bodyBytes, _ := json.Marshal(chList)
	return http.StatusOK, bodyBytes
}

func (m *mockDiscordTransport) roundTripMessages(req *http.Request, path string) (int, []byte) {
	channelID := extractPathSegmentAfter(path, "channels")
	m.messageCalls = append(m.messageCalls, channelID)
	m.afterQueries = append(m.afterQueries, req.URL.Query().Get("after"))

	if m.messagesErrCode != nil && m.messagesErrCode[channelID] > 0 {
		return m.messagesErrCode[channelID], []byte(`{"message": "error"}`)
	}
	var msgList []*discordgo.Message
	if m.messagesResp != nil && m.messagesResp[channelID] != nil {
		msgList = m.messagesResp[channelID]
	}
	bodyBytes, _ := json.Marshal(msgList)
	return http.StatusOK, bodyBytes
}

func (m *mockDiscordTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	path := req.URL.Path
	var bodyBytes []byte
	statusCode := http.StatusOK

	switch {
	case strings.HasSuffix(path, "/users/@me/guilds"):
		statusCode, bodyBytes = m.roundTripGuilds()
	case strings.Contains(path, "/threads/active"):
		statusCode, bodyBytes = m.roundTripThreads(path)
	case strings.HasSuffix(path, "/channels"):
		statusCode, bodyBytes = m.roundTripChannels(path)
	case strings.HasSuffix(path, "/messages"):
		statusCode, bodyBytes = m.roundTripMessages(req, path)
	default:
		bodyBytes = []byte(`{}`)
	}

	resp := &http.Response{
		StatusCode: statusCode,
		Body:       io.NopCloser(bytes.NewReader(bodyBytes)),
		Header:     http.Header{"Content-Type": {"application/json"}},
		Request:    req,
	}
	return resp, nil
}

func TestEnqueueDiscordMessageDirect(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name          string
		nilServer     bool
		nilMsg        bool
		nilAuthor     bool
		botUserID     string
		authorID      string
		refGuildID    string
		msgGuildID    string
		hasBus        bool
		wantProcessed int
	}{
		{
			name:      "nil server returns",
			nilServer: true,
		},
		{
			name:   "nil message returns",
			nilMsg: true,
		},
		{
			name:      "nil author returns",
			nilAuthor: true,
		},
		{
			name:      "author is bot returns",
			botUserID: "bot-1",
			authorID:  "bot-1",
		},
		{
			name:          "valid direct message dispatch with ref message guild ID",
			botUserID:     "bot-1",
			authorID:      "user-1",
			msgGuildID:    "guild-1",
			refGuildID:    "guild-1",
			wantProcessed: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var srv *testIngressServer
			if !tc.nilServer {
				srv = &testIngressServer{}
			}
			var m *discordgo.Message
			if !tc.nilMsg {
				m = &discordgo.Message{
					ID:        "msg-direct-1",
					ChannelID: "chan-direct-1",
					GuildID:   tc.msgGuildID,
				}
				if !tc.nilAuthor {
					m.Author = &discordgo.User{ID: tc.authorID}
				}
				if tc.refGuildID != "" {
					m.ReferencedMessage = &discordgo.Message{
						ID:      "ref-direct-1",
						GuildID: tc.refGuildID,
					}
				}
			}

			var targetServer IngressServer
			if srv != nil {
				targetServer = srv
			}
			EnqueueDiscordMessage(ctx, targetServer, nil, m, tc.botUserID)

			if srv != nil && len(srv.processedIngress) != tc.wantProcessed {
				t.Errorf("processedIngress = %d, want %d", len(srv.processedIngress), tc.wantProcessed)
			}
		})
	}
}

func TestFetchActiveGuildThreads(t *testing.T) {
	t.Parallel()

	// nil session
	threads, err := FetchActiveGuildThreads(nil, "guild-1")
	if err != nil || threads != nil {
		t.Errorf("expected nil, nil; got %v, %v", threads, err)
	}

	// empty guild ID
	sess, err := discordgo.New("Bot dummy")
	if err != nil {
		t.Fatalf("discordgo.New() err = %v", err)
	}
	threads, err = FetchActiveGuildThreads(sess, "")
	if err != nil || threads != nil {
		t.Errorf("expected nil, nil; got %v, %v", threads, err)
	}

	// threads error
	transportErr := &mockDiscordTransport{threadsErr: map[string]bool{"g-err": true}}
	sessErr, _ := discordgo.New("Bot dummy")
	sessErr.Client = &http.Client{Transport: transportErr}
	if _, err := FetchActiveGuildThreads(sessErr, "g-err"); err == nil {
		t.Error("expected error for threadsErr")
	}
}

func TestResolveCatchUpGuilds(t *testing.T) {
	t.Parallel()

	if guilds := resolveCatchUpGuilds(nil); guilds != nil {
		t.Errorf("resolveCatchUpGuilds(nil) = %v", guilds)
	}
	if guilds := fetchFallbackUserGuilds(nil); guilds != nil {
		t.Errorf("fetchFallbackUserGuilds(nil) = %v", guilds)
	}
	if guilds := fetchFallbackUserGuilds(&discordgo.Session{}); guilds != nil {
		t.Errorf("fetchFallbackUserGuilds(empty token) = %v", guilds)
	}
}

func TestCollectCandidateChannels(t *testing.T) {
	t.Parallel()

	if chs := collectCandidateChannels(nil, nil); chs != nil {
		t.Errorf("collectCandidateChannels(nil) = %v", chs)
	}
	if chs := collectCandidateChannels(nil, &discordgo.Guild{}); chs != nil {
		t.Errorf("collectCandidateChannels(empty guild) = %v", chs)
	}

	// Duplicate channels deduplication
	gDup := &discordgo.Guild{
		ID: "g-dup",
		Channels: []*discordgo.Channel{
			{ID: "c1"},
			{ID: "c1"},
		},
	}
	if chs := collectCandidateChannels(nil, gDup); len(chs) != 1 {
		t.Errorf("expected 1 unique channel, got %d", len(chs))
	}
}

func TestIsCatchUpMessageEligible(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	minSnow := DiscordSnowflakeFromTime(now.Add(-30 * time.Minute))
	if isCatchUpMessageEligible(nil, "bot", minSnow, now) {
		t.Error("nil message should not be eligible")
	}
	if isCatchUpMessageEligible(&discordgo.Message{}, "bot", minSnow, now) {
		t.Error("empty ID message should not be eligible")
	}
	if isCatchUpMessageEligible(&discordgo.Message{ID: "1"}, "bot", minSnow, now) {
		t.Error("nil author message should not be eligible")
	}
	if isCatchUpMessageEligible(&discordgo.Message{ID: minSnow, Author: &discordgo.User{ID: "u1"}}, "bot", minSnow, now) {
		t.Error("message <= minSnowflake should not be eligible")
	}
}

func TestCatchUpChannelMessages(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Now().UTC()

	sess, err := discordgo.New("Bot dummy")
	if err != nil {
		t.Fatalf("discordgo.New: %v", err)
	}
	sess.Client = &http.Client{Transport: &mockDiscordTransport{}}

	CatchUpChannelMessages(ctx, nil, nil, nil, "", "", "", now)
	CatchUpChannelMessages(ctx, sess, &testIngressServer{}, &discordgo.Channel{}, "", "", "", now)

	cCtx, cancel := context.WithCancel(ctx)
	cancel()
	CatchUpChannelMessages(cCtx, sess, &testIngressServer{}, &discordgo.Channel{ID: "c1", LastMessageID: "999"}, "", "", "1", now)
}
