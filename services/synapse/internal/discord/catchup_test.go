package discord

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"slices"
	"strconv"
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

	// Rows run in order against one shared watermark map.
	tests := []struct {
		name       string
		agent      string
		channelID  string
		advanceID  string
		wantBefore string
		wantAfter  string
	}{
		{name: "empty channel returns empty", channelID: "chan-new"},
		{name: "first advance sets watermark", channelID: "chan-new", advanceID: "100", wantAfter: "100"},
		{name: "smaller advance does not regress watermark", channelID: "chan-new", advanceID: "50", wantBefore: "100", wantAfter: "100"},
		{name: "larger advance advances watermark", channelID: "chan-new", advanceID: "200", wantBefore: "100", wantAfter: "200"},
		{name: "empty channel ID never advances", advanceID: "300"},
		{name: "agent advance sets agent watermark", agent: "agent-alpha", channelID: "chan-agent-2", advanceID: "250", wantAfter: "250"},
		{name: "other agent has empty watermark for same channel", agent: "agent-beta", channelID: "chan-agent-2"},
		{name: "unscoped has empty watermark for agent-advanced channel", channelID: "chan-agent-2"},
		{name: "agent watermark is separate from unscoped", agent: "agent-alpha", channelID: "chan-new", wantBefore: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if before := GetChannelWatermarkForAgent(tc.agent, tc.channelID); before != tc.wantBefore {
				t.Errorf("GetChannelWatermarkForAgent(%q, %q) before = %q, want %q", tc.agent, tc.channelID, before, tc.wantBefore)
			}
			AdvanceChannelWatermarkForAgent(tc.agent, tc.channelID, tc.advanceID)
			if after := GetChannelWatermarkForAgent(tc.agent, tc.channelID); after != tc.wantAfter {
				t.Errorf("GetChannelWatermarkForAgent(%q, %q) after = %q, want %q", tc.agent, tc.channelID, after, tc.wantAfter)
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
	agent               string

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
		AdvanceChannelWatermarkForAgent(tc.agent, tc.channelID, tc.initialWatermark)
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

	RunCatchUpSweepForAgent(targetCtx, session, targetServer, tc.now, tc.agent)

	if tc.runSecondSweep {
		RunCatchUpSweepForAgent(targetCtx, session, targetServer, tc.now, tc.agent)
	}

	assertCatchUpSweepResults(t, tc, srv, bus, transport)
	for idx, call := range srv.processedIngress {
		if call.Agent != tc.agent {
			t.Errorf("call[%d] Agent = %q, want %q", idx, call.Agent, tc.agent)
		}
	}

	if tc.channelID != "" && tc.wantWatermark != "" {
		if wm := GetChannelWatermarkForAgent(tc.agent, tc.channelID); wm != tc.wantWatermark {
			t.Errorf("channel watermark = %q, want %q", wm, tc.wantWatermark)
		}
		if wm := GetChannelWatermarkForAgent("unrelated-agent", tc.channelID); wm != "" {
			t.Errorf("unrelated agent watermark = %q, want empty", wm)
		}
	}
}

// Catch-up sweep fixtures: snowflakes and messages relative to sweepNow (stale TTL is 30m).
var (
	sweepNow        = time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	sweepMinSnow    = DiscordSnowflakeFromTime(sweepNow.Add(-30 * time.Minute))
	sweepStaleSnow  = DiscordSnowflakeFromTime(sweepNow.Add(-45 * time.Minute))
	sweepFreshSnow1 = DiscordSnowflakeFromTime(sweepNow.Add(-15 * time.Minute))
	sweepFreshSnow2 = DiscordSnowflakeFromTime(sweepNow.Add(-5 * time.Minute))
	sweepBotSnow    = DiscordSnowflakeFromTime(sweepNow.Add(-10 * time.Minute))

	sweepFreshMsg1 = sweepMessage(sweepFreshSnow1, "user-alpha", 15*time.Minute)
	sweepFreshMsg2 = &discordgo.Message{
		ID: sweepFreshSnow2, Author: &discordgo.User{ID: "user-beta"}, Timestamp: sweepNow.Add(-5 * time.Minute),
		ReferencedMessage: &discordgo.Message{ID: "ref-parent"},
	}
	sweepBotMsg   = sweepMessage(sweepBotSnow, "bot-123", 10*time.Minute)
	sweepStaleMsg = sweepMessage(sweepStaleSnow, "user-gamma", 45*time.Minute)
)

func sweepMessage(id, authorID string, age time.Duration) *discordgo.Message {
	return &discordgo.Message{ID: id, Author: &discordgo.User{ID: authorID}, Content: "catch-up message", Timestamp: sweepNow.Add(-age)}
}

func TestRunDiscordCatchUpSweep(t *testing.T) {
	ctx := context.Background()
	fresh := []string{sweepFreshSnow1, sweepFreshSnow2}

	tests := []sweepTestCase{
		{name: "nil session returns immediately", nilSession: true},
		{name: "nil server returns immediately", nilServer: true},
		{name: "concurrent sweep already running returns immediately", sweepAlreadyRunning: true},
		{name: "canceled context halts sweep early", now: sweepNow, canceledCtx: true, channelID: "chan-canceled"},
		{name: "zero now defaults to UTC now", channelID: "chan-zero-now"},
		{name: "fast-path skip on empty LastMessageID", now: sweepNow, channelID: "chan-sweep-1"},
		{name: "fast-path skip when LastMessageID <= minSnowflake", now: sweepNow, channelID: "chan-sweep-2", lastMessageID: sweepStaleSnow},
		{
			name: "fast-path skip when LastMessageID <= watermark", now: sweepNow, channelID: "chan-sweep-3", lastMessageID: sweepFreshSnow1,
			initialWatermark: sweepFreshSnow1,
		},
		{
			name: "ChannelMessages fetch error logged and channel skipped", now: sweepNow, channelID: "chan-sweep-4", lastMessageID: sweepFreshSnow2,
			messagesErrCode: http.StatusInternalServerError, wantFetchCalls: 1, wantAfterQuery: sweepMinSnow,
		},
		// Discord REST returns messages newest first; the sweep enqueues oldest first.
		{
			name: "chronological enqueue skipping stale and bot messages, advancing watermark", now: sweepNow, botUserID: "bot-123", channelID: "chan-sweep-5",
			guildID: "guild-123", lastMessageID: sweepFreshSnow2,
			messagesResp: []*discordgo.Message{sweepFreshMsg2, sweepFreshMsg1, sweepBotMsg, sweepStaleMsg}, wantFetchCalls: 1, wantAfterQuery: sweepMinSnow,
			wantEnqueuedIDs: fresh, wantWatermark: sweepFreshSnow2,
		},
		{
			name: "subsequent sweep makes zero calls once watermark advanced", now: sweepNow, botUserID: "bot-123", channelID: "chan-sweep-6",
			guildID: "guild-123", lastMessageID: sweepFreshSnow2, messagesResp: []*discordgo.Message{sweepFreshMsg2, sweepFreshMsg1}, runSecondSweep: true,
			wantFetchCalls: 1, wantAfterQuery: sweepMinSnow, wantEnqueuedIDs: fresh, wantWatermark: sweepFreshSnow2,
		},
		{
			name: "bus client enqueue in chronological order", now: sweepNow, botUserID: "bot-123", channelID: "chan-sweep-7", guildID: "guild-123",
			lastMessageID: sweepFreshSnow2, hasBus: true, messagesResp: []*discordgo.Message{sweepFreshMsg2, sweepFreshMsg1}, wantFetchCalls: 1,
			wantAfterQuery: sweepMinSnow, wantEnqueuedIDs: fresh, wantWatermark: sweepFreshSnow2,
		},
		{
			name: "agent-scoped sweep enqueues for the agent and advances only its watermark", agent: "agent-alpha", now: sweepNow, channelID: "chan-agent-1",
			guildID: "guild-agent-1", lastMessageID: sweepFreshSnow2, messagesResp: []*discordgo.Message{sweepFreshMsg2}, wantFetchCalls: 1,
			wantAfterQuery: sweepMinSnow, wantEnqueuedIDs: []string{sweepFreshSnow2}, wantWatermark: sweepFreshSnow2,
		},
		{
			name: "empty state guilds fetches user guilds from REST", now: sweepNow, userGuildsResp: []*discordgo.UserGuild{{ID: "user-guild-1"}},
			restChannels: []*discordgo.Channel{{ID: "ch-rest-1"}},
		},
		{name: "user guilds fetch error handled gracefully", now: sweepNow, userGuildsErr: true},
		{
			name: "guild channels and active threads error handled gracefully", now: sweepNow, guildID: "guild-err", channelID: "chan-err-fallback",
			restChannelsErr: true, restThreadsErr: true,
		},
		{
			name: "REST active threads and state threads merged and processed", now: sweepNow, guildID: "guild-threads", channelID: "chan-main",
			restThreads: []*discordgo.Channel{{ID: "thread-rest"}}, threadsInState: []*discordgo.Channel{{ID: "thread-state"}},
		},
		{
			name: "nil guild in state and nil channel ignored safely", now: sweepNow, guildID: "guild-valid", channelID: "chan-valid", nilGuildInState: true,
			nilChannelInGuild: true,
		},
		{
			name: "nil and empty ID messages in fetched batch skipped", now: sweepNow, guildID: "guild-nil-msg", channelID: "chan-nil-msg",
			lastMessageID: sweepFreshSnow2, messagesResp: []*discordgo.Message{nil, {ID: ""}, sweepFreshMsg2}, wantFetchCalls: 1, wantAfterQuery: sweepMinSnow,
			wantEnqueuedIDs: []string{sweepFreshSnow2}, wantWatermark: sweepFreshSnow2,
		},
	}
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
	thList, ok := m.threadsResp[guildID]
	if ok && thList == nil {
		return http.StatusOK, []byte(`null`) // explicit nil entry: Discord returned no thread list
	}
	if thList == nil {
		thList = &discordgo.ThreadsList{}
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
		Status:     strconv.Itoa(statusCode) + " " + http.StatusText(statusCode),
		Body:       io.NopCloser(bytes.NewReader(bodyBytes)),
		Header:     http.Header{"Content-Type": {"application/json"}},
		Request:    req,
	}
	return resp, nil
}

func TestEnqueueDiscordMessageGuards(t *testing.T) {
	t.Parallel()
	author := func(id string) *discordgo.Message {
		return &discordgo.Message{ID: "msg-direct-1", ChannelID: "chan-direct-1", Author: &discordgo.User{ID: id}}
	}

	tests := []struct {
		name      string
		srv       *testIngressServer
		msg       *discordgo.Message
		botUserID string
	}{
		{"nil server returns", nil, author("user-1"), "bot-1"},
		{"nil message returns", &testIngressServer{}, nil, "bot-1"},
		{"nil author returns", &testIngressServer{}, &discordgo.Message{ID: "msg-direct-1"}, "bot-1"},
		{"author is bot returns", &testIngressServer{}, author("bot-1"), "bot-1"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			EnqueueDiscordMessageForAgent(context.Background(), tc.srv, nil, tc.msg, tc.botUserID, "")
			if tc.srv != nil && (len(tc.srv.processedIngress) != 0 || len(tc.srv.publishedJobs) != 0) {
				t.Errorf("processed = %d, published = %d, want none", len(tc.srv.processedIngress), len(tc.srv.publishedJobs))
			}
		})
	}
}

// newCatchupSession builds a REST-capable session whose HTTP calls go to tr and whose state holds guilds.
func newCatchupSession(tr *mockDiscordTransport, guilds ...*discordgo.Guild) *discordgo.Session {
	sess, err := discordgo.New("Bot dummy")
	if err != nil {
		panic(err)
	}
	sess.Client = &http.Client{Transport: tr}
	sess.State.Guilds = guilds
	return sess
}

func channelIDs(chs []*discordgo.Channel) []string {
	ids := make([]string, 0, len(chs))
	for _, ch := range chs {
		ids = append(ids, ch.ID)
	}
	return ids
}

func guildIDs(gs []*discordgo.Guild) []string {
	ids := make([]string, 0, len(gs))
	for _, g := range gs {
		ids = append(ids, g.ID)
	}
	return ids
}

func TestFetchActiveGuildThreads(t *testing.T) {
	t.Parallel()
	threads := &discordgo.ThreadsList{Threads: []*discordgo.Channel{{ID: "th-1"}, {ID: "th-2"}}}

	tests := []struct {
		name    string
		dg      *discordgo.Session
		guildID string
		want    []string
		wantErr string
	}{
		{name: "nil session returns nothing", guildID: "g-1"},
		{name: "empty guild ID returns nothing", dg: newCatchupSession(&mockDiscordTransport{})},
		{
			name: "REST error returned", dg: newCatchupSession(&mockDiscordTransport{threadsErr: map[string]bool{"g-err": true}}), guildID: "g-err",
			wantErr: `HTTP 500 Internal Server Error, {"message": "error"}`,
		},
		{
			name: "null thread list returns nothing",
			dg:   newCatchupSession(&mockDiscordTransport{threadsResp: map[string]*discordgo.ThreadsList{"g-null": nil}}), guildID: "g-null",
		},
		{
			name: "active threads returned", dg: newCatchupSession(&mockDiscordTransport{threadsResp: map[string]*discordgo.ThreadsList{"g-1": threads}}),
			guildID: "g-1", want: []string{"th-1", "th-2"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := FetchActiveGuildThreads(tc.dg, tc.guildID)
			if gotErr := errString(err); gotErr != tc.wantErr {
				t.Fatalf("FetchActiveGuildThreads() err = %q, want %q", gotErr, tc.wantErr)
			}
			if ids := channelIDs(got); !slices.Equal(ids, tc.want) {
				t.Errorf("FetchActiveGuildThreads() = %v, want %v", ids, tc.want)
			}
		})
	}
}

func TestResolveCatchUpGuilds(t *testing.T) {
	t.Parallel()
	stateGuild := &discordgo.Guild{
		ID:       "g-state",
		Channels: []*discordgo.Channel{nil, {ID: "c-1", GuildID: "g-state", LastMessageID: "9"}},
		Threads:  []*discordgo.Channel{nil, {ID: "t-1"}},
	}

	tests := []struct {
		name string
		dg   *discordgo.Session
		want []string
	}{
		{name: "nil session returns nothing"},
		{name: "nil state and empty token returns nothing", dg: &discordgo.Session{}},
		{
			name: "state guilds snapshotted, nil and empty IDs skipped", dg: newCatchupSession(&mockDiscordTransport{}, nil, &discordgo.Guild{}, stateGuild),
			want: []string{"g-state"},
		},
		{
			name: "empty state falls back to user guilds",
			dg:   newCatchupSession(&mockDiscordTransport{userGuildsResp: []*discordgo.UserGuild{nil, {ID: ""}, {ID: "g-rest"}}}), want: []string{"g-rest"},
		},
		{name: "user guilds error returns nothing", dg: newCatchupSession(&mockDiscordTransport{userGuildsErr: true})},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := guildIDs(resolveCatchUpGuilds(tc.dg)); !slices.Equal(got, tc.want) {
				t.Errorf("resolveCatchUpGuilds() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestCollectCandidateChannels(t *testing.T) {
	t.Parallel()
	restTransport := &mockDiscordTransport{
		channelsResp: map[string][]*discordgo.Channel{"g-1": {{ID: "c-rest"}, {ID: "c-state"}}},
		threadsResp:  map[string]*discordgo.ThreadsList{"g-1": {Threads: []*discordgo.Channel{{ID: "t-rest"}}}},
	}
	failing := &mockDiscordTransport{channelsErr: map[string]bool{"g-1": true}, threadsErr: map[string]bool{"g-1": true}}
	stateGuild := &discordgo.Guild{
		ID:       "g-1",
		Channels: []*discordgo.Channel{nil, {ID: ""}, {ID: "c-state"}, {ID: "c-state"}},
		Threads:  []*discordgo.Channel{{ID: "t-rest"}, {ID: "t-state"}},
	}

	tests := []struct {
		name  string
		dg    *discordgo.Session
		guild *discordgo.Guild
		want  []string
	}{
		{name: "nil guild returns nothing"},
		{name: "empty guild ID returns nothing", guild: &discordgo.Guild{}},
		{name: "state only dedupes and skips nil or empty", guild: stateGuild, want: []string{"c-state", "t-rest", "t-state"}},
		{
			name: "REST channels then threads then state, deduped", dg: newCatchupSession(restTransport), guild: stateGuild,
			want: []string{"c-rest", "c-state", "t-rest", "t-state"},
		},
		{name: "REST errors fall back to state", dg: newCatchupSession(failing), guild: stateGuild, want: []string{"c-state", "t-rest", "t-state"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := channelIDs(collectCandidateChannels(tc.dg, tc.guild)); !slices.Equal(got, tc.want) {
				t.Errorf("collectCandidateChannels() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestIsCatchUpMessageEligible(t *testing.T) {
	t.Parallel()
	user := &discordgo.User{ID: "user-1"}

	tests := []struct {
		name string
		msg  *discordgo.Message
		want bool
	}{
		{"nil message", nil, false},
		{"empty ID", &discordgo.Message{Author: user}, false},
		{"nil author", &discordgo.Message{ID: sweepFreshSnow1}, false},
		{"bot author", &discordgo.Message{ID: sweepFreshSnow1, Author: &discordgo.User{ID: "bot"}}, false},
		{"timestamp older than stale TTL", &discordgo.Message{ID: sweepFreshSnow1, Author: user, Timestamp: sweepNow.Add(-31 * time.Minute)}, false},
		{"ID at minSnowflake", &discordgo.Message{ID: sweepMinSnow, Author: user}, false},
		{"fresh message without timestamp", &discordgo.Message{ID: sweepFreshSnow1, Author: user}, true},
		{"fresh message with timestamp", &discordgo.Message{ID: sweepFreshSnow1, Author: user, Timestamp: sweepNow.Add(-15 * time.Minute)}, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := isCatchUpMessageEligible(tc.msg, "bot", sweepMinSnow, sweepNow); got != tc.want {
				t.Errorf("isCatchUpMessageEligible() = %v, want %v", got, tc.want)
			}
		})
	}
}

func canceledCatchupContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func TestCatchUpChannelMessages(t *testing.T) {
	t.Parallel()
	ref := &discordgo.Message{ID: sweepFreshSnow2, Author: &discordgo.User{ID: "user-2"}, ReferencedMessage: &discordgo.Message{ID: "ref-1"}}
	msgs := func(channelID string) *mockDiscordTransport {
		older := &discordgo.Message{ID: sweepFreshSnow1, Author: &discordgo.User{ID: "user-1"}}
		return &mockDiscordTransport{messagesResp: map[string][]*discordgo.Message{channelID: {ref, older}}}
	}
	chanWith := func(id, guildID string) *discordgo.Channel {
		return &discordgo.Channel{ID: id, GuildID: guildID, LastMessageID: sweepFreshSnow2}
	}

	// Each row uses its own channel ID so the shared watermark map stays row-local.
	tests := []struct {
		name          string
		ctx           context.Context
		tr            *mockDiscordTransport
		srv           *testIngressServer
		ch            *discordgo.Channel
		guildID       string
		wantFetches   int
		wantEnqueued  []string
		wantGuild     string
		wantWatermark string
	}{
		{name: "nil channel ignored", ctx: context.Background(), tr: &mockDiscordTransport{}, srv: &testIngressServer{}},
		{name: "empty channel ID ignored", ctx: context.Background(), tr: &mockDiscordTransport{}, srv: &testIngressServer{}, ch: &discordgo.Channel{}},
		{name: "nil server ignored", ctx: context.Background(), tr: msgs("c-nosrv"), ch: chanWith("c-nosrv", "")},
		{
			name: "empty LastMessageID skips fetch", ctx: context.Background(), tr: msgs("c-idle"), srv: &testIngressServer{},
			ch: &discordgo.Channel{ID: "c-idle"},
		},
		{
			name: "canceled context stops before enqueue or watermark", ctx: canceledCatchupContext(), tr: msgs("c-cancel"), srv: &testIngressServer{},
			ch: chanWith("c-cancel", ""), wantFetches: 1,
		},
		{
			name: "channel guild ID backfilled oldest first", ctx: context.Background(), tr: msgs("c-ok"), srv: &testIngressServer{},
			ch: chanWith("c-ok", "g-chan"), guildID: "g-arg", wantFetches: 1, wantEnqueued: []string{sweepFreshSnow1, sweepFreshSnow2}, wantGuild: "g-chan",
			wantWatermark: sweepFreshSnow2,
		},
		{
			name: "guild argument used when channel has none", ctx: context.Background(), tr: msgs("c-arg"), srv: &testIngressServer{},
			ch: chanWith("c-arg", ""), guildID: "g-arg", wantFetches: 1, wantEnqueued: []string{sweepFreshSnow1, sweepFreshSnow2}, wantGuild: "g-arg",
			wantWatermark: sweepFreshSnow2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			CatchUpChannelMessagesForAgent(tc.ctx, newCatchupSession(tc.tr), tc.srv, tc.ch, tc.guildID, "bot", sweepMinSnow, sweepNow, "catchup-agent")

			if got := len(tc.tr.messageCalls); got != tc.wantFetches {
				t.Errorf("fetches = %d, want %d", got, tc.wantFetches)
			}
			if tc.srv == nil {
				return
			}
			var gotIDs []string
			for _, call := range tc.srv.processedIngress {
				gotIDs = append(gotIDs, call.MessageID)
				if call.Message.GuildID != tc.wantGuild {
					t.Errorf("message %s GuildID = %q, want %q", call.MessageID, call.Message.GuildID, tc.wantGuild)
				}
				if ref := call.Message.ReferencedMessage; ref != nil && ref.GuildID != tc.wantGuild {
					t.Errorf("message %s referenced GuildID = %q, want %q", call.MessageID, ref.GuildID, tc.wantGuild)
				}
			}
			if !slices.Equal(gotIDs, tc.wantEnqueued) {
				t.Errorf("enqueued = %v, want %v", gotIDs, tc.wantEnqueued)
			}
			if tc.ch != nil && tc.ch.ID != "" {
				if got := GetChannelWatermarkForAgent("catchup-agent", tc.ch.ID); got != tc.wantWatermark {
					t.Errorf("watermark = %q, want %q", got, tc.wantWatermark)
				}
			}
		})
	}
}
