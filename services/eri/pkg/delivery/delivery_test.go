package delivery

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/bwmarrin/discordgo"

	"github.com/azylman/aerial/pkg/sanitizer"
)

type splitMessageTestCase struct {
	name       string
	text       string
	limit      int
	wantChunks int
	wantFirst  string
	wantLast   string
	checkCode  bool
}

func runSplitMessageCase(t *testing.T, tc splitMessageTestCase) {
	t.Helper()
	res := SplitMessage(tc.text, tc.limit)
	effLimit := tc.limit
	if effLimit <= 0 {
		effLimit = 2000
	}
	if tc.text == "" {
		if len(res) != 0 {
			t.Fatalf("expected 0 chunks for empty input, got %d", len(res))
		}
		return
	}
	if len(res) < tc.wantChunks {
		t.Fatalf("expected at least %d chunks, got %d", tc.wantChunks, len(res))
	}
	for i, chunk := range res {
		if len([]rune(chunk)) > effLimit {
			t.Errorf("chunk %d exceeds limit %d: %d runes", i, effLimit, len([]rune(chunk)))
		}
		if tc.checkCode {
			fenceCount := strings.Count(chunk, "```")
			if fenceCount%2 != 0 {
				t.Errorf("chunk %d has unclosed/odd code fences (%d): %q", i, fenceCount, chunk)
			}
		}
	}
	if tc.wantFirst != "" && len(res) > 0 && res[0] != tc.wantFirst {
		t.Errorf("want first chunk %q, got %q", tc.wantFirst, res[0])
	}
	if tc.wantLast != "" && len(res) > 0 && res[len(res)-1] != tc.wantLast {
		t.Errorf("want last chunk %q, got %q", tc.wantLast, res[len(res)-1])
	}
}

func TestSplitMessage(t *testing.T) {
	t.Parallel()
	p1 := strings.Repeat("A", 1200)
	p2 := strings.Repeat("B", 1200)

	tests := []splitMessageTestCase{
		{name: "EmptyString", text: "", limit: 2000, wantChunks: 0},
		{
			name:       "ShortTextWithinLimit",
			text:       "Hello world! This is a simple test message.",
			limit:      2000,
			wantChunks: 1,
			wantFirst:  "Hello world! This is a simple test message.",
		},
		{name: "MultiParagraphExceedingLimit", text: p1 + "\n\n" + p2, limit: 2000, wantChunks: 2, wantFirst: p1, wantLast: p2},
		{name: "GiantSingleContinuousString", text: strings.Repeat("X", 4500), limit: 2000, wantChunks: 3},
		{name: "SmallLimitWordBoundary", text: "The quick brown fox jumps over the lazy dog", limit: 20, wantChunks: 3},
		{name: "UnicodeAndEmojiSafety", text: strings.Repeat("✨🌸", 1500), limit: 2000, wantChunks: 2},
		{
			name:       "MarkdownFencesSplit",
			text:       "```python\nline1 = 'hello'\nline2 = 'world'\nline3 = 'foo'\nline4 = 'bar'\n```",
			limit:      35,
			wantChunks: 2,
			checkCode:  true,
		},
		{name: "ZeroLimitDefaultsToMax", text: "Hello world with zero limit", limit: 0, wantChunks: 1, wantFirst: "Hello world with zero limit"},
		{name: "CodeBlockOverflow", text: "```go\n" + strings.Repeat("x", 100) + "\n```", limit: 40, wantChunks: 3, checkCode: true},
		{name: "LongCodeBlockLines", text: "```go\n" + strings.Repeat("x", 2500) + "\n```", limit: 2000, wantChunks: 2, checkCode: true},
		{name: "CodeBlockClosedFollowedByText", text: "```python\nprint(1)\n```\n" + strings.Repeat("A", 1990), limit: 2000, wantChunks: 2},
		{name: "ExactClosingFenceAtLimit", text: "```go\n" + strings.Repeat("x", 1990) + "\n```", limit: 2000, wantChunks: 1},
		{name: "SmallLimitCodeFence", text: "```go\n" + strings.Repeat("a", 100) + "\n```", limit: 50, wantChunks: 3, checkCode: true},
		{
			name:       "LongLineInsideCodeBlockExceedingLimit",
			text:       "```python\n" + strings.Repeat("x", 2500) + "\n```",
			limit:      500,
			wantChunks: 2,
			checkCode:  true,
		},
		{name: "ProseNearLimitBeforeOpeningFence", text: strings.Repeat("A", 1997) + "\n```go\nfmt.Println(1)\n```", limit: 2000, wantChunks: 2, wantFirst: strings.Repeat("A", 1997), wantLast: "```go\nfmt.Println(1)\n```"},
		{name: "ProseWhereOpeningFenceFitsButCodeLineDoesNot", text: strings.Repeat("A", 1989) + "\n```go\nfmt.Println(1)\n```", limit: 2000, wantChunks: 2, wantFirst: strings.Repeat("A", 1989), wantLast: "```go\nfmt.Println(1)\n```"},
		{name: "IndentedClosingFenceExceedingChunkLimit", text: "```go\n" + strings.Repeat("x", 1989) + "\n    ```", limit: 2000, wantChunks: 1, wantFirst: "```go\n" + strings.Repeat("x", 1989) + "\n```", wantLast: "```go\n" + strings.Repeat("x", 1989) + "\n```", checkCode: true},
		{name: "SingleLineInlineCodeFenceAtBoundary", text: strings.Repeat("A", 1990) + "\n```inline```\n" + strings.Repeat("B", 20), limit: 2000, wantChunks: 2, wantFirst: strings.Repeat("A", 1990), wantLast: "```inline```\n" + strings.Repeat("B", 20)},
		{name: "FenceWithLongLanguageTagAtSmallLimit", text: "```javascript\nconsole.log(1)\n```", limit: 12, wantChunks: 2, checkCode: true},
		{
			name:       "NestedListKeepsParentHeaderWithSubbullets",
			text:       strings.Repeat("A", 1930) + "\n- **Core Daemons**:\n  - **coven**: cpu = 100\n  - **hangar**: cpu = 100\n  - **agentsview**: cpu = 1000",
			limit:      2000,
			wantChunks: 2,
			wantFirst:  strings.Repeat("A", 1930),
			wantLast:   "- **Core Daemons**:\n  - **coven**: cpu = 100\n  - **hangar**: cpu = 100\n  - **agentsview**: cpu = 1000",
		},
		{
			name:       "OversizedSublistKeepsFirstChildAndDedentsOrphanChunk",
			text:       strings.Repeat("A", 35) + "\n- **Parent**:\n  1. " + strings.Repeat("B", 20) + "\n  2. " + strings.Repeat("C", 20) + "\n    - nested\n\n  3. " + strings.Repeat("D", 10),
			limit:      60,
			wantChunks: 3,
			wantFirst:  strings.Repeat("A", 35),
			wantLast:   "2. " + strings.Repeat("C", 20) + "\n  - nested\n\n3. " + strings.Repeat("D", 10),
		},
		{
			name:       "DeepToShallowOrphanSublistRatchetsDedent",
			text:       "  \n    * deep item\n  - shallow sibling\n    - child of sibling\nNormal line",
			limit:      2000,
			wantChunks: 1,
			wantFirst:  "* deep item\n- shallow sibling\n  - child of sibling\nNormal line",
		},
		{
			name:       "ThreeLevelNestedListExceedingLimitDoesNotOrphanParent",
			text:       strings.Repeat("A", 35) + "\n- **Parent**:\n  - **Subparent**:\n    - **Grandchild**:" + strings.Repeat("x", 15),
			limit:      60,
			wantChunks: 3,
			wantFirst:  strings.Repeat("A", 35),
			wantLast:   "- **Grandchild**:" + strings.Repeat("x", 15),
		},
		{
			name:       "OversizedSublistWhereEvenFirstChildExceedsLimit",
			text:       strings.Repeat("A", 10) + "\n- **Parent**:\n  - " + strings.Repeat("B", 60),
			limit:      30,
			wantChunks: 3,
			wantFirst:  strings.Repeat("A", 10) + "\n- **Parent**:",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runSplitMessageCase(t, tc)
		})
	}
}

// mockRoundTripper is the closure transport media_test.go uses for its internal HTTP client.
type mockRoundTripper struct {
	roundTrip func(req *http.Request) (*http.Response, error)
}

func (m *mockRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return m.roundTrip(req)
}

// deliveryRoute is one scripted Discord REST response.
type deliveryRoute struct {
	status int // 0 means 200
	body   string
	err    error // transport error instead of a response
}

// deliveryRequest is one request the fake Discord API received.
type deliveryRequest struct {
	Method, Path, Body string
}

// deliveryAPI is a declarative fake Discord REST API: routes are keyed by exact URL path and
// every other request gets the default route. It records every request.
type deliveryAPI struct {
	def    deliveryRoute
	routes map[string]deliveryRoute

	mu       sync.Mutex
	requests []deliveryRequest
}

func (f *deliveryAPI) RoundTrip(req *http.Request) (*http.Response, error) {
	var body []byte
	if req.Body != nil {
		body, _ = io.ReadAll(req.Body)
	}
	f.mu.Lock()
	f.requests = append(f.requests, deliveryRequest{Method: req.Method, Path: req.URL.Path, Body: string(body)})
	f.mu.Unlock()
	route, ok := f.routes[req.URL.Path]
	if !ok {
		route = f.def
	}
	if route.err != nil {
		return nil, route.err
	}
	status := route.status
	if status == 0 {
		status = http.StatusOK
	}
	return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(route.body)), Header: make(http.Header)}, nil
}

// session returns a bot session whose REST calls go to the fake.
func (f *deliveryAPI) session() *discordgo.Session {
	s, _ := discordgo.New("Bot mock_token")
	s.Client = &http.Client{Transport: f}
	return s
}

// payloads returns the non-empty request bodies in order.
func (f *deliveryAPI) payloads() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, r := range f.requests {
		if r.Body != "" {
			out = append(out, r.Body)
		}
	}
	return out
}

func (f *deliveryAPI) last() deliveryRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.requests) == 0 {
		return deliveryRequest{}
	}
	return f.requests[len(f.requests)-1]
}

// deliverySession returns nil when nilSession is set, else a session on api (a default 200
// message API when api is nil).
func deliverySession(nilSession bool, api **deliveryAPI) *discordgo.Session {
	if *api == nil {
		*api = &deliveryAPI{def: deliveryRoute{body: messageResponse}}
	}
	if nilSession {
		return nil
	}
	return (*api).session()
}

func checkDeliveryErr(t *testing.T, err error, wantErr string) bool {
	t.Helper()
	if wantErr == "" {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		return false
	}
	if err == nil || !strings.Contains(err.Error(), wantErr) {
		t.Fatalf("error = %v, want %q", err, wantErr)
	}
	return true
}

const dummyOutboundSecret = "plain-dummy-outbound-secret"

func registerDummyOutboundSecret(t *testing.T) {
	t.Helper()
	sanitizer.RegisterSensitiveTokens(dummyOutboundSecret)
	t.Cleanup(sanitizer.ResetSensitiveTokensForTest)
}

// assertNoSecret fails when any recorded payload carries the registered outbound secret.
func assertNoSecret(t *testing.T, payloads []string) {
	t.Helper()
	for i, p := range payloads {
		if strings.Contains(p, dummyOutboundSecret) {
			t.Errorf("payload %d leaked secret: %s", i, p)
		}
	}
}

const (
	messageResponse = `{"id":"msg-1","channel_id":"ch-1","content":"ok"}`
	threadResponse  = `{"id":"th-1","type":11,"name":"t"}`
)

func TestSendMessageWithAttachments(t *testing.T) {
	registerDummyOutboundSecret(t)
	att := []*Attachment{{Filename: "img.png", ContentType: "image/png", Data: []byte("fake png")}}
	failAPI := func() *deliveryAPI {
		return &deliveryAPI{def: deliveryRoute{status: http.StatusBadRequest, body: `{"message":"Missing permissions","code":50013}`}}
	}
	tests := []struct {
		name            string
		nilSession      bool
		api             *deliveryAPI // nil means a 200 message API
		channelID       string
		text            string
		attachments     []*Attachment
		replyToID       string
		wantErr         string
		wantPayloads    int
		wantFirst       string // substring of payload[0]
		wantFirstRef    string // message_reference id expected on payload[0]
		wantSecondNoRef bool
	}{
		{name: "NilSession", nilSession: true, channelID: "12345", text: "hello", wantErr: "discord session is nil"},
		{name: "SingleMessageNoReply", channelID: "12345", text: "Hello world", wantPayloads: 1, wantFirst: `"content":"Hello world"`},
		{name: "SingleMessageWithReplyRef", channelID: "12345", text: "Hello reply", replyToID: "9876543210", wantPayloads: 1, wantFirstRef: `"message_id":"9876543210"`},
		{
			name: "MultiChunkAttachmentsAndReplyOnFirstChunkOnly", channelID: "12345", text: strings.Repeat("A", 2500), attachments: att, replyToID: "9876543210",
			wantPayloads: 2, wantFirstRef: `"message_id":"9876543210"`, wantSecondNoRef: true,
		},
		{name: "AttachmentsOnlyWithReplyRef", channelID: "12345", attachments: att, replyToID: "1122334455", wantPayloads: 1, wantFirstRef: `"message_id":"1122334455"`},
		{name: "WhitespaceReplyIDIgnored", channelID: "12345", text: "Hello", replyToID: "   ", wantPayloads: 1},
		{name: "EmptyTextAndEmptyAttachments", channelID: "12345"},
		{name: "EmptyChannelIDError", text: "hello", wantErr: "channelID cannot be empty"},
		{name: "APITextSendFailure", api: failAPI(), channelID: "12345", text: "Hello fail", wantErr: "failed to send message chunk 0", wantPayloads: 1},
		{name: "APIAttachmentOnlySendFailure", api: failAPI(), channelID: "12345", attachments: att, wantErr: "failed to send attachments", wantPayloads: 1},
		{name: "SecretMaskedInMessage", channelID: "ch-1", text: "here is the key: " + dummyOutboundSecret, wantPayloads: 1, wantFirst: "here is the key: [REDACTED]"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sess := deliverySession(tc.nilSession, &tc.api)
			err := SendMessageWithAttachments(sess, tc.channelID, tc.text, tc.attachments, tc.replyToID)
			checkDeliveryErr(t, err, tc.wantErr)
			payloads := tc.api.payloads()
			assertNoSecret(t, payloads)
			if len(payloads) != tc.wantPayloads {
				t.Fatalf("payloads = %d, want %d", len(payloads), tc.wantPayloads)
			}
			if tc.wantPayloads == 0 {
				return
			}
			if !strings.Contains(payloads[0], `"flags":4`) {
				t.Errorf("payload[0] missing flags:4: %s", payloads[0])
			}
			if !strings.Contains(payloads[0], tc.wantFirst) {
				t.Errorf("payload[0] missing %q: %s", tc.wantFirst, payloads[0])
			}
			hasRef := strings.Contains(payloads[0], `"message_reference"`)
			switch {
			case tc.wantFirstRef == "" && hasRef:
				t.Errorf("payload[0] should omit message_reference: %s", payloads[0])
			case tc.wantFirstRef != "" && (!strings.Contains(payloads[0], tc.wantFirstRef) || !strings.Contains(payloads[0], `"fail_if_not_exists":false`)):
				t.Errorf("payload[0] missing reference %s with fail_if_not_exists:false: %s", tc.wantFirstRef, payloads[0])
			}
			if tc.wantSecondNoRef && strings.Contains(payloads[1], `"message_reference"`) {
				t.Errorf("payload[1] should omit message_reference: %s", payloads[1])
			}
		})
	}
}

func TestStartTyping(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		nilSession   bool
		channelID    string
		api          *deliveryAPI
		wantRequests int
	}{
		{name: "NilSession", nilSession: true, channelID: "12345"},
		{name: "EmptyChannel", channelID: "", api: &deliveryAPI{def: deliveryRoute{status: http.StatusNoContent}}},
		{name: "TypingSent", channelID: "12345", api: &deliveryAPI{def: deliveryRoute{status: http.StatusNoContent}}, wantRequests: 1},
		{name: "TypingErrorOnlyLogged", channelID: "chan-typing-error", api: &deliveryAPI{def: deliveryRoute{err: errors.New("simulated network typing failure")}}, wantRequests: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sess := deliverySession(tc.nilSession, &tc.api)
			stop := StartTyping(sess, tc.channelID)
			if stop == nil {
				t.Fatal("StartTyping returned a nil stop function")
			}
			stop()
			stop() // repeated stops are safe
			tc.api.mu.Lock()
			got := len(tc.api.requests)
			tc.api.mu.Unlock()
			if got != tc.wantRequests {
				t.Errorf("typing requests = %d, want %d", got, tc.wantRequests)
			}
		})
	}
}

// resolveStateGuild is the cached guild ResolveChannelByNameOrID rows find channels in.
func resolveStateGuild() *discordgo.Guild {
	return &discordgo.Guild{ID: "guild-1", Channels: []*discordgo.Channel{
		{ID: "chan-general", Name: "general"},
		{ID: "chan-aerial-dev", Name: "aerial-dev"},
		{ID: "chan-alerts", Name: "aerial-alerts"},
	}}
}

func TestResolveChannelByNameOrID(t *testing.T) {
	t.Parallel()
	emptyGuilds := func() *deliveryAPI { return &deliveryAPI{def: deliveryRoute{body: `[]`}} }
	tests := []struct {
		name       string
		nilSession bool
		inState    bool // seed resolveStateGuild into the session state
		api        *deliveryAPI
		in         string
		want       string
		wantErr    string
	}{
		{name: "NilSession", nilSession: true, in: "aerial-dev", wantErr: "discord session is nil"},
		{name: "Empty", api: emptyGuilds(), in: "", wantErr: "cannot be empty"},
		{name: "BareHash", api: emptyGuilds(), in: "#", wantErr: "cannot be empty"},
		{name: "Snowflake", api: emptyGuilds(), in: "123456789012345678", want: "123456789012345678"},
		{name: "StateName", inState: true, api: emptyGuilds(), in: "aerial-dev", want: "chan-aerial-dev"},
		{name: "StateHashName", inState: true, api: emptyGuilds(), in: "#aerial-dev", want: "chan-aerial-dev"},
		{name: "StateCaseInsensitive", inState: true, api: emptyGuilds(), in: "#AERIAL-DEV", want: "chan-aerial-dev"},
		{name: "NotFound", inState: true, api: emptyGuilds(), in: "non-existent-channel", wantErr: `channel "non-existent-channel" not found`},
		{
			name: "RESTFallback", in: "remote-channel", want: "chan-remote-999",
			api: &deliveryAPI{def: deliveryRoute{status: http.StatusNotFound, body: `{"message":"Not found"}`}, routes: map[string]deliveryRoute{
				"/api/v9/users/@me/guilds":     {body: `[{"id":"g100","name":"Guild"}]`},
				"/api/v9/guilds/g100/channels": {body: `[{"id":"chan-remote-999","name":"remote-channel"}]`},
			}},
		},
		{
			name: "RESTGuildChannelsErrorSkipsGuild", in: "any-channel", wantErr: `channel "any-channel" not found`,
			api: &deliveryAPI{def: deliveryRoute{status: http.StatusNotFound, body: `{"message":"not found"}`}, routes: map[string]deliveryRoute{
				"/api/v9/users/@me/guilds":      {body: `[{"id":"g-err","name":"Error Guild"}]`},
				"/api/v9/guilds/g-err/channels": {err: errors.New("guild channels fetch failed")},
			}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sess := deliverySession(tc.nilSession, &tc.api)
			if tc.inState {
				_ = sess.State.GuildAdd(resolveStateGuild())
			}
			got, err := ResolveChannelByNameOrID(sess, tc.in)
			if checkDeliveryErr(t, err, tc.wantErr) {
				return
			}
			if got != tc.want {
				t.Errorf("ResolveChannelByNameOrID(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestSendSystemAlert(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		nilSession  bool
		api         *deliveryAPI
		channel     string
		wantErr     string
		wantPayload string
	}{
		{name: "NilSession", nilSession: true, channel: "aerial-dev", wantErr: "discord session is nil"},
		{name: "ChannelNotFound", api: &deliveryAPI{def: deliveryRoute{body: `[]`}}, channel: "missing-chan", wantErr: `failed to resolve system alert channel "missing-chan"`},
		{name: "Delivered", channel: "aerial-alerts", wantPayload: `Aerial System Alert: System Alert**\n\nEverything operational`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sess := deliverySession(tc.nilSession, &tc.api)
			if sess != nil {
				_ = sess.State.GuildAdd(resolveStateGuild())
			}
			err := SendSystemAlert(sess, tc.channel, " System Alert ", " Everything operational ")
			if checkDeliveryErr(t, err, tc.wantErr) {
				return
			}
			if got := tc.api.last(); got.Path != "/api/v9/channels/chan-alerts/messages" || !strings.Contains(got.Body, tc.wantPayload) {
				t.Errorf("alert request = %s %s, want body containing %q", got.Path, got.Body, tc.wantPayload)
			}
		})
	}
}

func TestEditMessage(t *testing.T) {
	registerDummyOutboundSecret(t)
	tests := []struct {
		name        string
		nilSession  bool
		channelID   string
		messageID   string
		text        string
		wantErr     string
		wantPayload string
	}{
		{name: "NilSession", nilSession: true, channelID: "ch1", messageID: "msg1", text: "hello", wantErr: "discord session is nil"},
		{name: "EmptyChannel", messageID: "msg1", text: "hello", wantErr: "channelID cannot be empty"},
		{name: "EmptyMessage", channelID: "ch1", text: "hello", wantErr: "messageID cannot be empty"},
		{name: "BlankText", channelID: "ch1", messageID: "msg1", text: "   ", wantErr: "text cannot be empty"},
		{name: "Edited", channelID: "ch1", messageID: "msg1", text: "updated text", wantPayload: `"content":"updated text"`},
		{name: "TruncatedToDiscordLimit", channelID: "ch1", messageID: "msg1", text: strings.Repeat("A", 2500), wantPayload: `"content":"` + strings.Repeat("A", 2000) + `"`},
		{name: "SecretMaskedInEdit", channelID: "ch1", messageID: "msg1", text: "updated key: " + dummyOutboundSecret, wantPayload: "updated key: [REDACTED]"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var api *deliveryAPI
			sess := deliverySession(tc.nilSession, &api)
			if checkDeliveryErr(t, EditMessage(sess, tc.channelID, tc.messageID, tc.text), tc.wantErr) {
				return
			}
			got := api.last()
			assertNoSecret(t, []string{got.Body})
			if got.Method != http.MethodPatch || got.Path != "/api/v9/channels/ch1/messages/msg1" {
				t.Errorf("edit request = %s %s, want PATCH /api/v9/channels/ch1/messages/msg1", got.Method, got.Path)
			}
			if !strings.Contains(got.Body, `"flags":4`) || !strings.Contains(got.Body, tc.wantPayload) {
				t.Errorf("edit body = %s, want flags:4 and %q", got.Body, tc.wantPayload)
			}
		})
	}
}

func TestDeleteMessage(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		nilSession bool
		channelID  string
		messageID  string
		wantErr    string
	}{
		{name: "NilSession", nilSession: true, channelID: "ch1", messageID: "msg1", wantErr: "discord session is nil"},
		{name: "EmptyChannel", messageID: "msg1", wantErr: "channelID cannot be empty"},
		{name: "EmptyMessage", channelID: "ch1", wantErr: "messageID cannot be empty"},
		{name: "Deleted", channelID: "ch1", messageID: "msg1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			api := &deliveryAPI{def: deliveryRoute{status: http.StatusNoContent}}
			sess := deliverySession(tc.nilSession, &api)
			if checkDeliveryErr(t, DeleteMessage(sess, tc.channelID, tc.messageID), tc.wantErr) {
				return
			}
			if got := api.last(); got.Method != http.MethodDelete || got.Path != "/api/v9/channels/ch1/messages/msg1" {
				t.Errorf("delete request = %s %s, want DELETE /api/v9/channels/ch1/messages/msg1", got.Method, got.Path)
			}
		})
	}
}

func TestErrorClassifiers(t *testing.T) {
	t.Parallel()
	restStatus := func(code int) error { return &discordgo.RESTError{Response: &http.Response{StatusCode: code}} }
	restCode := func(code int) error {
		return &discordgo.RESTError{Response: &http.Response{StatusCode: http.StatusBadRequest}, Message: &discordgo.APIErrorMessage{Code: code}}
	}
	tests := []struct {
		name           string
		err            error
		wantNotFound   bool
		wantArchived   bool
		wantPermission bool
	}{
		{"nil", nil, false, false, false},
		{"unrelated", errors.New("connection reset by peer"), false, false, false},
		{"text 10008 unknown message", errors.New("HTTP 404 Not Found, 10008 Unknown Message"), true, false, false},
		{"text 50083 archived", errors.New("HTTP 400 Bad Request, 50083 Thread is archived"), false, true, false},
		{"text 50084 locked", errors.New("HTTP 403 Forbidden, 50084 Thread is locked"), false, true, true},
		{"text 403", errors.New("HTTP 403 Forbidden"), false, false, true},
		{"text 50001 missing access", errors.New("HTTP 403 Forbidden, 50001 Missing Access"), false, false, true},
		{"text 50013 missing permissions", errors.New("HTTP 403 Forbidden, 50013 Missing Permissions"), false, false, true},
		{"REST 404", restStatus(http.StatusNotFound), true, false, false},
		{"REST 10008", restCode(10008), true, false, false},
		{"REST 50083", restCode(50083), false, true, false},
		{"REST 50084", restCode(50084), false, true, false},
		{"REST 403", restStatus(http.StatusForbidden), false, false, true},
		{"REST 50001", restCode(50001), false, false, true},
		{"REST 50013", restCode(50013), false, false, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := IsMessageNotFoundError(tc.err); got != tc.wantNotFound {
				t.Errorf("IsMessageNotFoundError = %v, want %v", got, tc.wantNotFound)
			}
			if got := IsThreadArchivedOrLockedError(tc.err); got != tc.wantArchived {
				t.Errorf("IsThreadArchivedOrLockedError = %v, want %v", got, tc.wantArchived)
			}
			if got := IsPermissionError(tc.err); got != tc.wantPermission {
				t.Errorf("IsPermissionError = %v, want %v", got, tc.wantPermission)
			}
		})
	}
}

func TestRedactOutbound(t *testing.T) {
	registerDummyOutboundSecret(t)
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"empty", "", ""},
		{"clean text passes through", "deploy finished", "deploy finished"},
		{"registered secret masked", "key " + dummyOutboundSecret + " here", "key [REDACTED] here"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := RedactOutbound(tc.input); got != tc.want {
				t.Errorf("RedactOutbound(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestSendSingleMessage(t *testing.T) {
	registerDummyOutboundSecret(t)
	tests := []struct {
		name       string
		nilSession bool
		channelID  string
		text       string
		wantID     string
		wantErr    string
		wantSubstr string
	}{
		{name: "nil session", nilSession: true, channelID: "ch-1", text: "hi", wantErr: "discord session is nil"},
		{name: "empty channel", text: "hi", wantErr: "channelID cannot be empty"},
		{name: "secret masked in status message", channelID: "ch-1", text: "status " + dummyOutboundSecret, wantID: "msg-1", wantSubstr: "status [REDACTED]"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var api *deliveryAPI
			id, err := SendSingleMessage(deliverySession(tc.nilSession, &api), tc.channelID, tc.text)
			if checkDeliveryErr(t, err, tc.wantErr) {
				return
			}
			if id != tc.wantID {
				t.Errorf("SendSingleMessage id = %q, want %q", id, tc.wantID)
			}
			body := api.last().Body
			assertNoSecret(t, []string{body})
			if !strings.Contains(body, tc.wantSubstr) {
				t.Errorf("payload missing %q: %s", tc.wantSubstr, body)
			}
		})
	}
}

func TestStartMessageThread(t *testing.T) {
	registerDummyOutboundSecret(t)
	tests := []struct {
		name       string
		nilSession bool
		title      string
		wantID     string
		wantErr    string
		wantSubstr string
	}{
		{name: "nil session", nilSession: true, title: "t", wantErr: "discord session is nil"},
		{name: "secret masked in thread title", title: "Title " + dummyOutboundSecret, wantID: "th-1", wantSubstr: "Title [REDACTED]"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			api := &deliveryAPI{def: deliveryRoute{body: threadResponse}}
			ch, err := StartMessageThread(deliverySession(tc.nilSession, &api), "ch-1", "msg-1", tc.title)
			if checkDeliveryErr(t, err, tc.wantErr) {
				return
			}
			if ch == nil || ch.ID != tc.wantID {
				t.Fatalf("StartMessageThread channel = %+v, want ID %q", ch, tc.wantID)
			}
			body := api.last().Body
			assertNoSecret(t, []string{body})
			if !strings.Contains(body, tc.wantSubstr) {
				t.Errorf("payload missing %q: %s", tc.wantSubstr, body)
			}
		})
	}
}

func TestStartThread(t *testing.T) {
	registerDummyOutboundSecret(t)
	tests := []struct {
		name       string
		nilSession bool
		title      string
		wantID     string
		wantErr    string
		wantSubstr string
	}{
		{name: "nil session", nilSession: true, title: "t", wantErr: "discord session is nil"},
		{name: "secret masked in standalone thread title", title: "Routine " + dummyOutboundSecret, wantID: "th-1", wantSubstr: "Routine [REDACTED]"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			api := &deliveryAPI{def: deliveryRoute{body: threadResponse}}
			ch, err := StartThread(deliverySession(tc.nilSession, &api), "ch-1", tc.title)
			if checkDeliveryErr(t, err, tc.wantErr) {
				return
			}
			if ch == nil || ch.ID != tc.wantID {
				t.Fatalf("StartThread channel = %+v, want ID %q", ch, tc.wantID)
			}
			body := api.last().Body
			assertNoSecret(t, []string{body})
			if !strings.Contains(body, tc.wantSubstr) {
				t.Errorf("payload missing %q: %s", tc.wantSubstr, body)
			}
		})
	}
}
