package widget

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/services/vanadis-restate/internal/ericlient"
)

type mockWidgetSender struct {
	sendMsg    *discordgo.Message
	nilSendMsg bool
	sendErr    error
	editMsg    *discordgo.Message
	editErr    error
	deleteErr  error
	msgs       []*discordgo.Message
	msgsErr    error
	channel    *discordgo.Channel
	channelErr error

	lastSendChannelID string
	lastSendData      *discordgo.MessageSend
	lastEditData      *discordgo.MessageEdit
	lastDeleteID      string
}

func clearAllChunkActiveStepsForTest() {
	chunkActiveStepsMu.Lock()
	defer chunkActiveStepsMu.Unlock()
	chunkActiveSteps = make(map[string]string)
	chunkActiveOrder = nil
}

func extractTestComponentsText(components []discordgo.MessageComponent) string {
	var lines []string
	for _, comp := range components {
		switch c := comp.(type) {
		case *discordgo.TextDisplay:
			lines = append(lines, strings.TrimSpace(c.Content))
		case *discordgo.Section:
			lines = append(lines, extractTestComponentsText(c.Components))
		case *discordgo.Container:
			lines = append(lines, extractTestComponentsText(c.Components))
		}
	}
	return strings.Join(lines, "\n")
}

func (m *mockWidgetSender) ChannelMessageSendComplex(channelID string, data *discordgo.MessageSend, _ ...discordgo.RequestOption) (*discordgo.Message, error) {
	m.lastSendChannelID = channelID
	m.lastSendData = data
	if m.sendErr != nil {
		return nil, m.sendErr
	}
	if m.nilSendMsg {
		return nil, nil
	}
	if m.sendMsg != nil {
		return m.sendMsg, nil
	}
	return &discordgo.Message{ID: "msg-default-new", ChannelID: channelID}, nil
}

func (m *mockWidgetSender) ChannelMessageEditComplex(edit *discordgo.MessageEdit, _ ...discordgo.RequestOption) (*discordgo.Message, error) {
	m.lastEditData = edit
	if m.editErr != nil {
		return nil, m.editErr
	}
	if m.editMsg != nil {
		return m.editMsg, nil
	}
	return &discordgo.Message{ID: edit.ID, ChannelID: edit.Channel}, nil
}

func (m *mockWidgetSender) ChannelMessages(_ string, _ int, _, _, _ string, _ ...discordgo.RequestOption) ([]*discordgo.Message, error) {
	if m.msgsErr != nil {
		return nil, m.msgsErr
	}
	return m.msgs, nil
}

func (m *mockWidgetSender) ChannelMessageDelete(_ string, messageID string, _ ...discordgo.RequestOption) error {
	m.lastDeleteID = messageID
	return m.deleteErr
}

func (m *mockWidgetSender) Channel(_ string, _ ...discordgo.RequestOption) (*discordgo.Channel, error) {
	if m.channelErr != nil {
		return nil, m.channelErr
	}
	return m.channel, nil
}

// widgetSenderNow is the fixed clock shared by the sender tables below.
var widgetSenderNow = time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)

// widgetReq builds a status widget request with the given build status and one chunk per chunk status.
func widgetReq(status string, chunkStatuses ...string) ericlient.StatusWidgetRequest {
	req := ericlient.StatusWidgetRequest{Status: status}
	for _, s := range chunkStatuses {
		req.Chunks = append(req.Chunks, ericlient.StatusWidgetChunk{Status: s})
	}
	return req
}

// timedWidgetReq stamps req and its chunks as started at start; non-running chunks complete at end.
func timedWidgetReq(req ericlient.StatusWidgetRequest, start, end time.Time) ericlient.StatusWidgetRequest {
	req.StartedAt = start
	for i := range req.Chunks {
		req.Chunks[i].StartedAt = start
		if req.Chunks[i].Status != "running" {
			req.Chunks[i].CompletedAt = end
		}
	}
	return req
}

// msgs builds n channel messages m1..mn posted age before widgetSenderNow with the given flags.
func msgs(n int, age time.Duration, flags discordgo.MessageFlags) []*discordgo.Message {
	out := make([]*discordgo.Message, 0, n)
	for i := 1; i <= n; i++ {
		out = append(out, &discordgo.Message{ID: fmt.Sprintf("m%d", i), Timestamp: widgetSenderNow.Add(-age), Flags: flags})
	}
	return out
}

// newSenderDeliverer builds a deliverer whose default agent sends through sender (none when nil) and whose
// channel policy redirects every channel to redirectTo when it is set.
func newSenderDeliverer(sender *mockWidgetSender, redirectTo string) *DiscordWidgetDeliverer {
	senders := map[string]DiscordWidgetSender{}
	if sender != nil {
		senders[defaultAgent] = sender
	}
	return &DiscordWidgetDeliverer{
		agentSenders:          senders,
		nowFn:                 func() time.Time { return widgetSenderNow },
		channelPolicyResolver: func(string) (bool, string) { return redirectTo != "", redirectTo },
		resolvedChannels:      make(map[string]cachedResolvedChannel),
		channelMetaCache:      make(map[string]cachedChannelMeta),
	}
}

// senderMock returns the mock sender d routes agent to, or nil when there is none.
func senderMock(d *DiscordWidgetDeliverer, agent string) *mockWidgetSender {
	if d == nil {
		return nil
	}
	m, _ := d.agentSenders[normalizeRunnerAgent(agent)].(*mockWidgetSender)
	return m
}

func TestShouldSinkWidget(t *testing.T) {
	t.Parallel()
	cv2 := discordgo.MessageFlagsIsComponentsV2
	tests := []struct {
		name    string
		sender  *mockWidgetSender
		channel string
		msgID   string
		want    bool
	}{
		{name: "nil sender returns false", channel: "chan-1", msgID: "msg-1"},
		{name: "empty channel ID returns false", sender: &mockWidgetSender{}, msgID: "msg-1"},
		{name: "empty message ID returns false", sender: &mockWidgetSender{}, channel: "chan-1"},
		{name: "messages error returns false", sender: &mockWidgetSender{msgsErr: errors.New("fetch error")}, channel: "chan-1", msgID: "msg-1"},
		{name: "empty messages returns false", sender: &mockWidgetSender{msgs: msgs(0, 0, 0)}, channel: "chan-1", msgID: "msg-1"},
		{
			name: "fewer than threshold and recent returns false", sender: &mockWidgetSender{msgs: msgs(2, 5*time.Minute, 0)}, channel: "chan-1",
			msgID: "msg-1",
		},
		{
			name: "exactly 4 regular messages triggers sink", sender: &mockWidgetSender{msgs: msgs(4, 5*time.Minute, 0)}, channel: "chan-1",
			msgID: "msg-1", want: true,
		},
		{
			name:   "Components V2 messages are ignored from count",
			sender: &mockWidgetSender{msgs: append(msgs(2, 5*time.Minute, cv2), msgs(2, 5*time.Minute, 0)...)}, channel: "chan-1", msgID: "msg-1",
		},
		{name: "self message ID is ignored from count", sender: &mockWidgetSender{msgs: msgs(4, 5*time.Minute, 0)}, channel: "chan-1", msgID: "m1"},
		{
			name: "idle threshold exceeded triggers sink with fewer messages", sender: &mockWidgetSender{msgs: msgs(1, 20*time.Minute, 0)},
			channel: "chan-1", msgID: "msg-1", want: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var sender DiscordWidgetSender
			if tc.sender != nil {
				sender = tc.sender
			}
			assert.Equal(t, tc.want, shouldSinkWidget(sender, tc.channel, tc.msgID, widgetSenderNow))
		})
	}
}

func TestDeliverStatusWidget(t *testing.T) {
	t.Parallel()
	notFoundErr := &discordgo.RESTError{Response: &http.Response{StatusCode: http.StatusNotFound}}
	forbidden := &http.Response{StatusCode: http.StatusForbidden}
	foreignAuthor := &discordgo.APIErrorMessage{Code: 50005, Message: "Cannot edit a message authored by another user"}
	foreignAuthorErr := &discordgo.RESTError{Response: forbidden, Message: foreignAuthor}
	missingPermsErr := &discordgo.RESTError{Response: forbidden, Message: &discordgo.APIErrorMessage{Code: 50013, Message: "Missing Permissions"}}
	missingAccessErr := &discordgo.RESTError{Response: forbidden, Message: &discordgo.APIErrorMessage{Code: 50001, Message: "Missing Access"}}
	buried := msgs(1, 20*time.Minute, 0)
	errBoom := errors.New("boom")

	tests := []struct {
		name         string
		sender       *mockWidgetSender
		channel      string
		msgID        string
		skipSink     bool
		wantID       string
		wantErr      string
		wantSent     bool
		wantDeleteID string
	}{
		{name: "nil sender returns error", channel: "chan-1", wantErr: "discord widget sender is nil"},
		{name: "empty channel ID returns error", sender: &mockWidgetSender{}, wantErr: "channel_id is required"},
		{
			name: "new message success", sender: &mockWidgetSender{sendMsg: &discordgo.Message{ID: "msg-new-123"}}, channel: "chan-1",
			wantID: "msg-new-123", wantSent: true,
		},
		{
			name: "new message send failure", sender: &mockWidgetSender{sendErr: errors.New("network failure")}, channel: "chan-1",
			wantErr: "failed to send status widget message", wantSent: true,
		},
		{
			name: "new message send nil message returns error", sender: &mockWidgetSender{nilSendMsg: true}, channel: "chan-1",
			wantErr: "received nil message", wantSent: true,
		},
		{
			name: "edit existing message success", sender: &mockWidgetSender{editMsg: &discordgo.Message{ID: "msg-orig-1"}}, channel: "chan-1",
			msgID: "msg-orig-1", wantID: "msg-orig-1",
		},
		{
			name:   "edit existing message 404 recovers by creating new",
			sender: &mockWidgetSender{editErr: notFoundErr, sendMsg: &discordgo.Message{ID: "msg-recreated-2"}}, channel: "chan-1",
			msgID: "msg-deleted-1", wantID: "msg-recreated-2", wantSent: true,
		},
		{
			name:   "edit existing message 404 recreate error fails",
			sender: &mockWidgetSender{editErr: notFoundErr, sendErr: errors.New("recreate error")}, channel: "chan-1", msgID: "msg-deleted-1",
			wantErr: "failed to send new status widget message after 404 edit", wantSent: true,
		},
		{
			name: "edit existing message 404 recreate nil message fails", sender: &mockWidgetSender{editErr: notFoundErr, nilSendMsg: true},
			channel: "chan-1", msgID: "msg-deleted-1", wantErr: "received nil message", wantSent: true,
		},
		{
			name: "edit existing non-404 error fails", sender: &mockWidgetSender{editErr: errors.New("edit permission denied")}, channel: "chan-1",
			msgID: "msg-err-1", wantErr: "failed to edit status widget message",
		},
		{
			name:   "edit foreign-authored message re-posts and deletes old",
			sender: &mockWidgetSender{editErr: foreignAuthorErr, sendMsg: &discordgo.Message{ID: "msg-reposted-1"}}, channel: "chan-1",
			msgID: "msg-foreign-1", wantID: "msg-reposted-1", wantSent: true, wantDeleteID: "msg-foreign-1",
		},
		{
			name:    "edit foreign-authored message delete missing permissions still returns new ID",
			sender:  &mockWidgetSender{editErr: foreignAuthorErr, sendMsg: &discordgo.Message{ID: "msg-reposted-2"}, deleteErr: missingPermsErr},
			channel: "chan-1", msgID: "msg-foreign-2", wantID: "msg-reposted-2", wantSent: true, wantDeleteID: "msg-foreign-2",
		},
		{
			name:    "edit foreign-authored message delete 404 still returns new ID",
			sender:  &mockWidgetSender{editErr: foreignAuthorErr, sendMsg: &discordgo.Message{ID: "msg-reposted-3"}, deleteErr: notFoundErr},
			channel: "chan-1", msgID: "msg-foreign-3", wantID: "msg-reposted-3", wantSent: true, wantDeleteID: "msg-foreign-3",
		},
		{
			name:    "edit foreign-authored message delete other error still returns new ID",
			sender:  &mockWidgetSender{editErr: foreignAuthorErr, sendMsg: &discordgo.Message{ID: "msg-reposted-4"}, deleteErr: errBoom},
			channel: "chan-1", msgID: "msg-foreign-4", wantID: "msg-reposted-4", wantSent: true, wantDeleteID: "msg-foreign-4",
		},
		{
			name:   "edit foreign-authored message re-post error fails",
			sender: &mockWidgetSender{editErr: foreignAuthorErr, sendErr: errors.New("repost error")}, channel: "chan-1", msgID: "msg-foreign-5",
			wantErr: "failed to send new status widget message after foreign-author edit", wantSent: true,
		},
		{
			name: "edit foreign-authored message re-post nil message fails", sender: &mockWidgetSender{editErr: foreignAuthorErr, nilSendMsg: true},
			channel: "chan-1", msgID: "msg-foreign-6", wantErr: "after foreign-author edit: received nil message", wantSent: true,
		},
		{
			name: "edit missing access 403 fails without re-posting", sender: &mockWidgetSender{editErr: missingAccessErr}, channel: "chan-1",
			msgID: "msg-noaccess-1", wantErr: "failed to edit status widget message msg-noaccess-1",
		},
		{
			name:   "buried message sinks by creating new and deleting old",
			sender: &mockWidgetSender{msgs: buried, sendMsg: &discordgo.Message{ID: "msg-sunk-new"}}, channel: "chan-1", msgID: "msg-buried-1",
			wantID: "msg-sunk-new", wantSent: true, wantDeleteID: "msg-buried-1",
		},
		{
			name:    "buried message sink delete failure still succeeds",
			sender:  &mockWidgetSender{msgs: buried, sendMsg: &discordgo.Message{ID: "msg-sunk-new-2"}, deleteErr: errors.New("delete failed")},
			channel: "chan-1", msgID: "msg-buried-del-fail", wantID: "msg-sunk-new-2", wantSent: true, wantDeleteID: "msg-buried-del-fail",
		},
		{
			name:    "buried message sink send failure falls back to edit in place",
			sender:  &mockWidgetSender{msgs: buried, sendErr: errors.New("sink failed"), editMsg: &discordgo.Message{ID: "msg-sink-fail"}},
			channel: "chan-1", msgID: "msg-sink-fail", wantID: "msg-sink-fail", wantSent: true,
		},
		{
			name:   "buried message with skip_sink edits in place",
			sender: &mockWidgetSender{msgs: buried, editMsg: &discordgo.Message{ID: "msg-buried-2"}}, channel: "chan-1", msgID: "msg-buried-2",
			skipSink: true, wantID: "msg-buried-2",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var sender DiscordWidgetSender
			if tc.sender != nil {
				sender = tc.sender
			}
			req := ericlient.StatusWidgetRequest{ChannelID: tc.channel, MessageID: tc.msgID, Title: "Build 1", SkipSink: tc.skipSink}
			id, err := DeliverStatusWidget(sender, "example-owner", req, widgetSenderNow)
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tc.wantID, id)
			var mock mockWidgetSender
			if tc.sender != nil {
				mock = *tc.sender
			}
			assert.Equal(t, tc.wantSent, mock.lastSendData != nil)
			assert.Equal(t, tc.wantDeleteID, mock.lastDeleteID)
		})
	}
}

func TestDelivererDeliverStatusWidget(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		d        *DiscordWidgetDeliverer
		req      ericlient.StatusWidgetRequest
		wantID   string
		wantErr  string
		wantText []string
	}{
		{name: "nil deliverer returns error", req: ericlient.StatusWidgetRequest{ChannelID: "c1"}, wantErr: "discord widget deliverer is nil"},
		{
			name: "missing sender returns error", d: newSenderDeliverer(nil, ""), req: ericlient.StatusWidgetRequest{ChannelID: "c1"},
			wantErr: `no discord sender configured for agent "aerial"`,
		},
		{name: "empty channel returns error", d: newSenderDeliverer(&mockWidgetSender{}, ""), wantErr: "channel_id is required"},
		{
			name: "send stamps the deliverer clock and suppresses mentions",
			d:    newSenderDeliverer(&mockWidgetSender{sendMsg: &discordgo.Message{ID: "m-clock"}}, ""),
			req:  ericlient.StatusWidgetRequest{ChannelID: "chan-1", Title: "Build Clock @everyone"}, wantID: "m-clock",
			wantText: []string{"<t:1795089600:R>", "@everyone"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			id, err := tc.d.DeliverStatusWidget(context.Background(), tc.req)
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tc.wantID, id)
			mock := senderMock(tc.d, tc.req.Agent)
			if mock == nil || mock.lastSendData == nil {
				assert.Empty(t, tc.wantText)
				return
			}
			require.NotNil(t, mock.lastSendData.AllowedMentions)
			assert.Empty(t, mock.lastSendData.AllowedMentions.Parse)
			text := extractTestComponentsText(mock.lastSendData.Components)
			for _, want := range tc.wantText {
				assert.Contains(t, text, want)
			}
		})
	}
}

func TestDeliverDirectMessageWithAgent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		d           *DiscordWidgetDeliverer
		agent       string
		channelID   string
		content     string
		wantErr     string
		wantChannel string
		wantContent string
	}{
		{name: "nil deliverer returns error", channelID: "123456", content: "alert", wantErr: "discord widget deliverer is nil"},
		{
			name: "missing sender returns error", d: newSenderDeliverer(nil, ""), channelID: "123456", content: "alert",
			wantErr: `no discord sender configured for agent "aerial"`,
		},
		{name: "empty channel is no-op", d: newSenderDeliverer(&mockWidgetSender{}, ""), channelID: "   ", content: "alert"},
		{name: "empty content is no-op", d: newSenderDeliverer(&mockWidgetSender{}, ""), agent: "aerial", channelID: "123456", content: "   "},
		{
			name: "channel policy redirect when no_build_updates", d: newSenderDeliverer(&mockWidgetSender{}, "999888"), channelID: "123456",
			content: "alert msg", wantChannel: "999888", wantContent: "alert msg",
		},
		{
			name: "successful send suppresses allowed mentions", d: newSenderDeliverer(&mockWidgetSender{}, ""), channelID: " 123456 ",
			content: " hello world ", wantChannel: "123456", wantContent: "hello world",
		},
		{
			name: "send error propagates", d: newSenderDeliverer(&mockWidgetSender{sendErr: errors.New("discord rate limited")}, ""),
			channelID: "123456", content: "hello world", wantErr: "failed to send direct discord message: discord rate limited",
			wantChannel: "123456", wantContent: "hello world",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.d.DeliverDirectMessageWithAgent(context.Background(), tc.agent, tc.channelID, tc.content)
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
			} else {
				require.NoError(t, err)
			}
			var mock mockWidgetSender
			if m := senderMock(tc.d, tc.agent); m != nil {
				mock = *m
			}
			assert.Equal(t, tc.wantChannel, mock.lastSendChannelID)
			if mock.lastSendData == nil {
				assert.Empty(t, tc.wantContent)
				return
			}
			assert.Equal(t, tc.wantContent, mock.lastSendData.Content)
			require.NotNil(t, mock.lastSendData.AllowedMentions)
			assert.Empty(t, mock.lastSendData.AllowedMentions.Parse)
		})
	}
}

func TestDeliverDirectMessage(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		d           *DiscordWidgetDeliverer
		wantErr     string
		wantChannel string
	}{
		{name: "nil deliverer returns error", wantErr: "discord widget deliverer is nil"},
		{name: "routes through the default agent sender", d: newSenderDeliverer(&mockWidgetSender{}, ""), wantChannel: "123456"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.d.DeliverDirectMessage(context.Background(), "123456", "alert")
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
			} else {
				require.NoError(t, err)
			}
			var mock mockWidgetSender
			if m := senderMock(tc.d, defaultAgent); m != nil {
				mock = *m
			}
			assert.Equal(t, tc.wantChannel, mock.lastSendChannelID)
		})
	}
}

func TestFormatChunkDurationTable(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		duration time.Duration
		expected string
	}{
		{"negative duration clamps to 0s", -5 * time.Second, "0s"},
		{"seconds only", 45 * time.Second, "45s"},
		{"exact minute", 2 * time.Minute, "2m"},
		{"minutes and seconds", 2*time.Minute + 15*time.Second, "2m 15s"},
		{"exact hour", 3 * time.Hour, "3h"},
		{"hours and minutes", 1*time.Hour + 20*time.Minute, "1h 20m"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.expected, FormatChunkDuration(tc.duration))
		})
	}
}

func TestAccentColorAndStatusBadgeTable(t *testing.T) {
	t.Parallel()
	tests := []struct {
		status     string
		expectHex  int
		expectIcon string
	}{
		{"completed", 0x10B981, "✅"},
		{"deployed", 0x10B981, "✅"},
		{"failed", 0xEF4444, "❌"},
		{"ci_failed", 0xEF4444, "❌"},
		{"cancelled", 0xF59E0B, "⏳"},
		{"running", 0xA855F7, "🔨"},
		{"pr_opened", 0xA855F7, "👀"},
		{"merged", 0xA855F7, "🔀"},
		{"synced", 0xA855F7, "🔄"},
		{"deploying", 0xA855F7, "🚀"},
		{"pending", 0xA855F7, "⏳"},
	}

	for _, tc := range tests {
		t.Run(tc.status, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.expectHex, AccentColor(tc.status))
			assert.Equal(t, tc.expectIcon, statusBadge(tc.status))
		})
	}
}

func TestActiveStepRegistryOwnedRules(t *testing.T) {
	clearAllChunkActiveStepsForTest()
	t.Cleanup(clearAllChunkActiveStepsForTest)

	tests := []struct {
		name       string
		setupStep  string
		setStep    string
		expectStep string
	}{
		{
			name:       "trimmed text is stored",
			setStep:    "  ⚡ Compiling...  ",
			expectStep: "⚡ Compiling...",
		},
		{
			name:       "empty string deletes existing entry",
			setupStep:  "⚡ Running",
			setStep:    "",
			expectStep: "",
		},
		{
			name:       "whitespace only string deletes existing entry",
			setupStep:  "⚡ Running",
			setStep:    "   \t\n  ",
			expectStep: "",
		},
		{
			name:       "missing chunk returns empty",
			expectStep: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			chunkID := "chunk-" + tc.name
			if tc.setupStep != "" {
				SetChunkActiveStep(chunkID, tc.setupStep)
			}
			if tc.setStep != "" || tc.setupStep != "" {
				SetChunkActiveStep(chunkID, tc.setStep)
			}
			assert.Equal(t, tc.expectStep, GetChunkActiveStep(chunkID))
		})
	}
}

func TestActiveStepRegistryBounding(t *testing.T) {
	clearAllChunkActiveStepsForTest()
	t.Cleanup(clearAllChunkActiveStepsForTest)

	for i := range MaxChunkActiveSteps + 20 {
		SetChunkActiveStep(fmt.Sprintf("bounded-chunk-%04d", i), fmt.Sprintf("step-%d", i))
	}

	chunkActiveStepsMu.RLock()
	total := len(chunkActiveSteps)
	orderTotal := len(chunkActiveOrder)
	chunkActiveStepsMu.RUnlock()

	assert.Equal(t, MaxChunkActiveSteps, total)
	assert.Equal(t, MaxChunkActiveSteps, orderTotal)

	// Oldest chunks should be evicted
	assert.Equal(t, "", GetChunkActiveStep("bounded-chunk-0000"))
	assert.Equal(t, "", GetChunkActiveStep("bounded-chunk-0010"))

	// Latest chunks should exist
	assert.Equal(t, fmt.Sprintf("step-%d", MaxChunkActiveSteps+19), GetChunkActiveStep(fmt.Sprintf("bounded-chunk-%04d", MaxChunkActiveSteps+19)))

	// Clearing removes from both map and order slice
	SetChunkActiveStep("bounded-chunk-to-clear", "some-step")
	ClearChunkActiveStep("bounded-chunk-to-clear")
	assert.Equal(t, "", GetChunkActiveStep("bounded-chunk-to-clear"))

	SetChunkActiveStep("bounded-chunk-empty", "some-step")
	SetChunkActiveStep("bounded-chunk-empty", "")
	assert.Equal(t, "", GetChunkActiveStep("bounded-chunk-empty"))

	chunkActiveStepsMu.RLock()
	assert.Equal(t, len(chunkActiveSteps), len(chunkActiveOrder))
	chunkActiveStepsMu.RUnlock()
}

func TestFormatChunkRuntimeSuffix(t *testing.T) {
	t.Parallel()
	started := time.Date(2026, 11, 19, 10, 0, 0, 0, time.UTC)
	completed := started.Add(45 * time.Minute)

	tests := []struct {
		name     string
		chunk    ericlient.StatusWidgetChunk
		expected string
	}{
		{
			name:     "started at zero returns empty",
			chunk:    ericlient.StatusWidgetChunk{},
			expected: "",
		},
		{
			name:     "running chunk returns relative started format",
			chunk:    ericlient.StatusWidgetChunk{Status: "running", StartedAt: started},
			expected: " *(started <t:1795082400:R>)*",
		},
		{
			name:     "completed chunk with completed_at returns duration",
			chunk:    ericlient.StatusWidgetChunk{Status: "completed", StartedAt: started, CompletedAt: completed},
			expected: " *(45m)*",
		},
		{
			name:     "failed chunk with updated_at returns duration",
			chunk:    ericlient.StatusWidgetChunk{Status: "failed", StartedAt: started, UpdatedAt: completed},
			expected: " *(45m)*",
		},
		{
			name:     "completed chunk without end time returns empty",
			chunk:    ericlient.StatusWidgetChunk{Status: "completed", StartedAt: started},
			expected: "",
		},
		{
			name:     "completed chunk with updated_at returns duration",
			chunk:    ericlient.StatusWidgetChunk{Status: "completed", StartedAt: started, UpdatedAt: completed},
			expected: " *(45m)*",
		},
		{
			name:     "merged chunk with completed_at returns duration",
			chunk:    ericlient.StatusWidgetChunk{Status: "merged", StartedAt: started, CompletedAt: completed},
			expected: " *(45m)*",
		},
		{
			name:     "synced chunk with completed_at returns duration",
			chunk:    ericlient.StatusWidgetChunk{Status: "synced", StartedAt: started, CompletedAt: completed},
			expected: " *(45m)*",
		},
		{
			name:     "unknown chunk status returns empty",
			chunk:    ericlient.StatusWidgetChunk{Status: "unknown_xyz", StartedAt: started},
			expected: "",
		},
		{
			name:     "pending chunk returns empty",
			chunk:    ericlient.StatusWidgetChunk{Status: "pending", StartedAt: started},
			expected: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.expected, formatChunkRuntimeSuffix(tc.chunk))
		})
	}
}

// collapseFixture builds total chunks whose first done entries carry doneStatus and the rest run.
func collapseFixture(total, done int, doneStatus string) []ericlient.StatusWidgetChunk {
	chunks := make([]ericlient.StatusWidgetChunk, 0, total)
	for i := range total {
		st := "running"
		if i < done {
			st = doneStatus
		}
		chunks = append(chunks, ericlient.StatusWidgetChunk{ID: fmt.Sprintf("c%02d", i), Title: fmt.Sprintf("Chunk %02d", i), Status: st})
	}
	return chunks
}

func TestCollapseCompletedWidgetChunks(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		chunks      []ericlient.StatusWidgetChunk
		wantIDs     []string
		wantSummary string
	}{
		{
			name: "ten or fewer chunks does not collapse",
			chunks: []ericlient.StatusWidgetChunk{
				{ID: "c1", Status: "merged"},
				{ID: "c2", Status: "running"},
			},
			wantIDs: []string{"c1", "c2"},
		},
		{
			name:        "twelve chunks with seven merged keeps last two done",
			chunks:      collapseFixture(12, 7, "merged"),
			wantIDs:     []string{"c05", "c06", "c07", "c08", "c09", "c10", "c11"},
			wantSummary: "✅ ***5/12** earlier chunks completed and merged*",
		},
		{
			name:        "twenty two chunks with nineteen done collapses seventeen",
			chunks:      collapseFixture(22, 19, "completed"),
			wantIDs:     []string{"c17", "c18", "c19", "c20", "c21"},
			wantSummary: "✅ ***17/22** earlier chunks completed and merged*",
		},
		{
			name:        "cancelled chunks count as done",
			chunks:      collapseFixture(11, 4, "cancelled"),
			wantIDs:     []string{"c02", "c03", "c04", "c05", "c06", "c07", "c08", "c09", "c10"},
			wantSummary: "✅ ***2/11** earlier chunks completed and merged*",
		},
		{
			name:    "more than ten chunks with only two done does not collapse",
			chunks:  collapseFixture(11, 2, "synced"),
			wantIDs: []string{"c00", "c01", "c02", "c03", "c04", "c05", "c06", "c07", "c08", "c09", "c10"},
		},
		{
			name:    "more than ten chunks with failed chunks keeps them visible",
			chunks:  collapseFixture(11, 9, "failed"),
			wantIDs: []string{"c00", "c01", "c02", "c03", "c04", "c05", "c06", "c07", "c08", "c09", "c10"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			active, summary := collapseCompletedWidgetChunks(tc.chunks)
			ids := make([]string, 0, len(active))
			for _, c := range active {
				ids = append(ids, c.ID)
			}
			assert.Equal(t, tc.wantIDs, ids)
			assert.Equal(t, tc.wantSummary, summary)
		})
	}
}

func TestFormatChunkActiveStepLines(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"empty string returns empty", "", ""},
		{"single line without prefix", "Running tests", "\n-# ↳ Running tests"},
		{"line with existing arrow prefix", "↳ Running tests", "\n-# ↳ Running tests"},
		{"line with existing subtext prefix", "-# ↳ Running tests", "\n-# ↳ Running tests"},
		{"multiline input", "Line 1\nLine 2", "\n-# ↳ Line 1\n-# ↳ Line 2"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.expected, formatChunkActiveStepLines(tc.input))
		})
	}
}

func TestIsWidgetCompletedTable(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		req  ericlient.StatusWidgetRequest
		want bool
	}{
		{"completed build with completed and merged chunks", widgetReq("completed", "completed", "merged"), true},
		{"failed build with terminal chunks", widgetReq("failed", "failed", "cancelled"), true},
		{"error build with terminal chunks", widgetReq("error", "error"), true},
		{"running build with terminal chunks returns false", widgetReq("running", "completed"), false},
		{"coding build with terminal chunks returns false", widgetReq("coding", "completed"), false},
		{"open pr build returns false", widgetReq("open", "completed"), false},
		{"pending build returns false", widgetReq("pending", "completed"), false},
		{"paused build returns false", widgetReq("paused", "completed"), false},
		{"stalled build returns false", widgetReq("stalled", "completed"), false},
		{"failed build with running chunk returns false", widgetReq("failed", "failed", "running"), false},
		{"completed build with running chunk returns false", widgetReq("completed", "running"), false},
		{"empty status and chunks returns false", widgetReq(""), false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, isWidgetCompleted(tc.req))
		})
	}
}

func TestFormatWidgetFooterTextTable(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, 11, 19, 10, 0, 0, 0, time.UTC)
	end := start.Add(45 * time.Minute)
	now := start.Add(60 * time.Minute)
	updated := "*Started <t:1795082400:R> · Updated <t:1795086000:R>*"
	duration := "*Started <t:1795082400:R> · Duration 45m*"

	tests := []struct {
		name   string
		req    ericlient.StatusWidgetRequest
		want   string
		reject string
	}{
		{"zero started at returns updated only", widgetReq("running"), "*Updated <t:1795086000:R>*", "Duration"},
		{"running build shows started and updated", timedWidgetReq(widgetReq("running", "running"), start, end), updated, "Duration"},
		{"completed build shows duration footer", timedWidgetReq(widgetReq("completed", "completed"), start, end), duration, "Updated"},
		{"failed build with terminal chunks shows duration footer", timedWidgetReq(widgetReq("failed", "failed"), start, end), duration, "Updated"},
		{
			"failed build with running chunk shows started and updated", timedWidgetReq(widgetReq("failed", "failed", "running"), start, end),
			updated, "Duration",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := formatWidgetFooterText(tc.req, now)
			assert.Contains(t, got, tc.want)
			assert.NotContains(t, got, tc.reject)
		})
	}
}

func TestNewDiscordWidgetDelivererTokens(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		agent          string
		token          string
		environ        []string
		configPath     string
		wantOwner      string
		wantConfigPath string
		hasSession     bool
	}{
		{"token with Bot prefix", "", "Bot test-token", nil, "", "aerial", "", true},
		{"token without Bot prefix", "aerial", "test-token-raw", nil, " /custom/path ", "aerial", "/custom/path", true},
		{"token from environ", "", "", []string{"DISCORD_TOKEN=env-token"}, "", "aerial", "", true},
		{"empty token", "", "", nil, "", "aerial", "", false},
		{"blank environ token ignored", "", "", []string{"DISCORD_TOKEN=  "}, "", "aerial", "", false},
		{"sibling runner keys environ token under own agent", " Agent-Alpha ", "", []string{"DISCORD_TOKEN=env-token"}, "/share/agent-alpha/config.yaml", "agent-alpha", "/share/agent-alpha/config.yaml", true},
		{"sibling runner keys explicit token under own agent", "agent-alpha", "alpha-token", nil, "", "agent-alpha", "", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			d, err := NewDiscordWidgetDeliverer(tc.agent, tc.token, "example-owner", tc.environ, tc.configPath)
			require.NoError(t, err)
			assert.NotNil(t, d)
			assert.Equal(t, tc.wantConfigPath, d.configPath)
			if !tc.hasSession {
				assert.Nil(t, d.session)
				assert.NotContains(t, d.agentSenders, tc.wantOwner)
				return
			}
			require.NotNil(t, d.session)
			assert.Same(t, d.session, d.agentSenders[tc.wantOwner])
			got, sErr := d.senderForAgent(tc.wantOwner)
			require.NoError(t, sErr)
			assert.Same(t, d.session, got)
		})
	}
}

func TestDeliverStatusWidgetChannelSerialization(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name        string
		channelA    string
		channelB    string
		expectOrder []string
	}{
		{
			name:        "same channel deliveries are serialized sequentially",
			channelA:    "chan-shared",
			channelB:    "chan-shared",
			expectOrder: []string{"first-start", "first-end", "second-start", "second-end"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var (
				mu     sync.Mutex
				events []string
			)

			mock := &mockWidgetSender{
				sendMsg: &discordgo.Message{ID: "msg-sync"},
			}
			deliverer := &DiscordWidgetDeliverer{
				agentSenders: map[string]DiscordWidgetSender{"aerial": mock},
				nowFn:        func() time.Time { return now },
			}

			chStarted := make(chan struct{})
			chRelease := make(chan struct{})

			// Custom sender wrapper to track concurrency order
			firstSender := &mockSerializedSender{
				mock: mock,
				onSend: func() {
					mu.Lock()
					events = append(events, "first-start")
					mu.Unlock()
					close(chStarted)
					<-chRelease
					mu.Lock()
					events = append(events, "first-end")
					mu.Unlock()
				},
			}
			deliverer.agentSenders = map[string]DiscordWidgetSender{"aerial": firstSender}

			doneFirst := make(chan struct{})
			go func() {
				_, _ = deliverer.DeliverStatusWidget(context.Background(), ericlient.StatusWidgetRequest{
					ChannelID: tc.channelA,
					Title:     "Build First",
				})
				close(doneFirst)
			}()

			<-chStarted

			secondSender := &mockSerializedSender{
				mock: mock,
				onSend: func() {
					mu.Lock()
					events = append(events, "second-start", "second-end")
					mu.Unlock()
				},
			}
			deliverer.agentSenders = map[string]DiscordWidgetSender{"aerial": secondSender}

			doneSecond := make(chan struct{})
			go func() {
				_, _ = deliverer.DeliverStatusWidget(context.Background(), ericlient.StatusWidgetRequest{
					ChannelID: tc.channelB,
					Title:     "Build Second",
				})
				close(doneSecond)
			}()

			close(chRelease)
			<-doneFirst
			<-doneSecond

			mu.Lock()
			gotEvents := append([]string(nil), events...)
			mu.Unlock()

			assert.Equal(t, tc.expectOrder, gotEvents)
		})
	}
}

type mockSerializedSender struct {
	mock   *mockWidgetSender
	onSend func()
}

func (m *mockSerializedSender) ChannelMessageSendComplex(channelID string, data *discordgo.MessageSend, options ...discordgo.RequestOption) (*discordgo.Message, error) {
	if m.onSend != nil {
		m.onSend()
	}
	return m.mock.ChannelMessageSendComplex(channelID, data, options...)
}

func (m *mockSerializedSender) ChannelMessageEditComplex(edit *discordgo.MessageEdit, options ...discordgo.RequestOption) (*discordgo.Message, error) {
	return m.mock.ChannelMessageEditComplex(edit, options...)
}

func (m *mockSerializedSender) ChannelMessages(channelID string, limit int, beforeID, afterID, aroundID string, options ...discordgo.RequestOption) ([]*discordgo.Message, error) {
	return m.mock.ChannelMessages(channelID, limit, beforeID, afterID, aroundID, options...)
}

func (m *mockSerializedSender) ChannelMessageDelete(channelID, messageID string, options ...discordgo.RequestOption) error {
	return m.mock.ChannelMessageDelete(channelID, messageID, options...)
}

func TestDiscordWidgetDeliverer_AgentRouting(t *testing.T) {
	t.Parallel()

	mockAerial := &mockWidgetSender{}
	mockAlpha := &mockWidgetSender{}

	d := &DiscordWidgetDeliverer{
		agentSenders: map[string]DiscordWidgetSender{
			"aerial":      mockAerial,
			"agent-alpha": mockAlpha,
		},
	}
	alphaOnly := &DiscordWidgetDeliverer{
		agentSenders: map[string]DiscordWidgetSender{"agent-alpha": mockAlpha},
	}
	nilEntry := &DiscordWidgetDeliverer{
		agentSenders: map[string]DiscordWidgetSender{"aerial": nil},
	}

	tests := []struct {
		name       string
		deliverer  *DiscordWidgetDeliverer
		agent      string
		wantSender DiscordWidgetSender
		wantErr    bool
	}{
		{name: "nil deliverer errors", deliverer: nil, agent: "aerial", wantErr: true},
		{name: "empty agent resolves aerial", deliverer: d, agent: "", wantSender: mockAerial},
		{name: "aerial agent resolves aerial", deliverer: d, agent: "aerial", wantSender: mockAerial},
		{name: "agent is normalized", deliverer: d, agent: " Agent-Alpha ", wantSender: mockAlpha},
		{name: "agent-alpha resolves alpha", deliverer: d, agent: "agent-alpha", wantSender: mockAlpha},
		{name: "unknown agent never falls back to aerial", deliverer: d, agent: "agent-unknown", wantErr: true},
		{name: "default agent never falls back to a sibling", deliverer: alphaOnly, agent: "", wantErr: true},
		{name: "nil sender entry errors", deliverer: nilEntry, agent: "aerial", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := tc.deliverer.senderForAgent(tc.agent)
			if tc.wantErr {
				require.ErrorIs(t, err, ErrNoSenderForAgent)
				assert.Nil(t, got)
				return
			}
			require.NoError(t, err)
			assert.Same(t, tc.wantSender, got)
		})
	}

	newD := &DiscordWidgetDeliverer{agentSenders: map[string]DiscordWidgetSender{"aerial": mockAerial, "agent-beta": mockAlpha}}
	got, err := newD.senderForAgent("aerial")
	require.NoError(t, err)
	assert.Same(t, mockAerial, got)

	got, err = newD.senderForAgent("agent-beta")
	require.NoError(t, err)
	assert.Same(t, mockAlpha, got)
}

func newAlphaOnlyDeliverer(alpha DiscordWidgetSender, now time.Time) *DiscordWidgetDeliverer {
	return &DiscordWidgetDeliverer{
		agentSenders:          map[string]DiscordWidgetSender{"agent-alpha": alpha},
		nowFn:                 func() time.Time { return now },
		channelPolicyResolver: func(string) (bool, string) { return false, "" },
		resolvedChannels:      make(map[string]cachedResolvedChannel),
		channelMetaCache:      make(map[string]cachedChannelMeta),
	}
}

func TestDeliverStatusWidget_NoCrossAgentSender(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC)
	tests := []struct {
		name  string
		agent string
	}{
		{name: "unconfigured sibling agent", agent: "agent-beta"},
		{name: "empty agent on sibling runner", agent: ""},
		{name: "default agent on sibling runner", agent: "aerial"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			alpha := &mockWidgetSender{sendMsg: &discordgo.Message{ID: "msg-alpha"}}
			d := newAlphaOnlyDeliverer(alpha, now)
			_, err := d.DeliverStatusWidget(context.Background(), ericlient.StatusWidgetRequest{Agent: tc.agent, ChannelID: "123456", MessageID: "msg-old", Title: "Build"})
			require.ErrorIs(t, err, ErrNoSenderForAgent)
			assert.Nil(t, alpha.lastSendData)
			assert.Nil(t, alpha.lastEditData)
			assert.Empty(t, alpha.lastDeleteID)
		})
	}
}

func TestDeliverDirectMessageWithAgent_NoCrossAgentSender(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC)
	tests := []struct {
		name  string
		agent string
	}{
		{name: "unconfigured sibling agent", agent: "agent-beta"},
		{name: "empty agent on sibling runner", agent: ""},
		{name: "default agent on sibling runner", agent: "aerial"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			alpha := &mockWidgetSender{sendMsg: &discordgo.Message{ID: "msg-alpha"}}
			d := newAlphaOnlyDeliverer(alpha, now)
			err := d.DeliverDirectMessageWithAgent(context.Background(), tc.agent, "123456", "hello")
			require.ErrorIs(t, err, ErrNoSenderForAgent)
			assert.Nil(t, alpha.lastSendData)
		})
	}
}

func TestDiscoverAgentTokens(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		runner  string
		environ []string
		want    map[string]string
	}{
		{"default runner keys plain token under aerial", "", []string{"DISCORD_TOKEN=token-plain", "DISCORD_TOKEN_AGENT_ALPHA=token-alpha"}, map[string]string{"aerial": "token-plain", "agent-alpha": "token-alpha"}},
		{"explicit aerial runner keys plain token under aerial", "aerial", []string{"DISCORD_TOKEN=token-plain"}, map[string]string{"aerial": "token-plain"}},
		{"sibling runner keys plain token under itself", "agent-beta", []string{"DISCORD_TOKEN=token-plain"}, map[string]string{"agent-beta": "token-plain"}},
		{"sibling runner keeps explicit per-agent token", "agent-beta", []string{"DISCORD_TOKEN=token-plain", "DISCORD_TOKEN_AGENT_BETA=token-beta"}, map[string]string{"agent-beta": "token-beta"}},
		{"sibling runner without plain token", "agent-alpha", []string{"DISCORD_TOKEN_AGENT_ALPHA=token-alpha"}, map[string]string{"agent-alpha": "token-alpha"}},
		{"bot token fallback when plain token absent", "", []string{"DISCORD_BOT_TOKEN= token-bot "}, map[string]string{"aerial": "token-bot"}},
		{"plain token wins over bot token", "", []string{"DISCORD_BOT_TOKEN=token-bot", "DISCORD_TOKEN=token-plain"}, map[string]string{"aerial": "token-plain"}},
		{"malformed blank and empty-suffix entries skipped", "", []string{"NOEQUALS", "DISCORD_TOKEN_AGENT_ALPHA=", "DISCORD_TOKEN_=token-empty", "HOME=/root"}, map[string]string{}},
		{"nil environ yields no tokens", "", nil, map[string]string{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, discoverAgentTokens(tc.runner, tc.environ))
		})
	}
}

func TestDiscordWidgetDeliverer_PerAgentConfigRedirection(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	rootCfgPath := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.WriteFile(rootCfgPath, []byte("system_channel: aerial-dev\n"), 0o600))
	agentAlphaDir := filepath.Join(dir, "services", "agent-alpha")
	require.NoError(t, os.MkdirAll(agentAlphaDir, 0o755))
	alphaCfg := "system_channel: agent-alpha-dev\nchannels:\n  general:\n    no_build_updates: true\n"
	require.NoError(t, os.WriteFile(filepath.Join(agentAlphaDir, "config.yaml"), []byte(alphaCfg), 0o600))
	channelIDs := map[string]string{"agent-alpha-dev": "111111111111111111", "aerial-dev": "222222222222222222"}

	tests := []struct {
		name          string
		agent         string
		channelID     string
		wantChannelID string
	}{
		{name: "default agent uses root config without redirect", agent: "aerial", channelID: "general", wantChannelID: "general"},
		{name: "unspecified agent uses root config without redirect", agent: "", channelID: "general", wantChannelID: "general"},
		{
			name: "agent-alpha redirects no_build_updates to agent-alpha-dev", agent: "agent-alpha", channelID: "general",
			wantChannelID: "111111111111111111",
		},
		{name: "agent-alpha does not redirect regular channel", agent: "agent-alpha", channelID: "lounge", wantChannelID: "lounge"},
		{name: "unconfigured sibling agent falls back to root config", agent: "agent-beta", channelID: "general", wantChannelID: "general"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			d, err := NewDiscordWidgetDeliverer("", "", "example-owner", nil, rootCfgPath)
			require.NoError(t, err)
			d.channelResolver = func(nameOrID string) (string, error) {
				if id, ok := channelIDs[nameOrID]; ok {
					return id, nil
				}
				return "", errors.New("channel not found")
			}
			d.agentSenders = map[string]DiscordWidgetSender{
				"agent-alpha": &mockWidgetSender{sendMsg: &discordgo.Message{ID: "msg-alpha"}},
				"aerial":      &mockWidgetSender{sendMsg: &discordgo.Message{ID: "msg-aerial"}},
				"agent-beta":  &mockWidgetSender{sendMsg: &discordgo.Message{ID: "msg-beta"}},
			}

			assert.Equal(t, tc.wantChannelID, d.redirectChannelIfNoBuildUpdates(tc.agent, tc.channelID))
			req := ericlient.StatusWidgetRequest{Agent: tc.agent, ChannelID: tc.channelID, Title: "Build"}
			_, sErr := d.DeliverStatusWidget(context.Background(), req)
			require.NoError(t, sErr)
			require.NoError(t, d.DeliverDirectMessageWithAgent(context.Background(), tc.agent, tc.channelID, "alert"))
			active := senderMock(d, tc.agent)
			require.NotNil(t, active)
			assert.Equal(t, tc.wantChannelID, active.lastSendChannelID)
		})
	}
}

func TestDiscordWidgetDeliverer_SessionResolutionForAgent(t *testing.T) {
	t.Parallel()
	alphaSess, err := discordgo.New("Bot fake-alpha-token")
	require.NoError(t, err)
	alphaSess.State = discordgo.NewState()
	require.NoError(t, alphaSess.State.GuildAdd(&discordgo.Guild{
		ID: "g-alpha",
		Channels: []*discordgo.Channel{
			{ID: "111111111111111111", Name: "agent-alpha-dev"},
		},
	}))
	for _, ch := range alphaSess.State.Guilds[0].Channels {
		require.NoError(t, alphaSess.State.ChannelAdd(ch))
	}

	aerialSess, err := discordgo.New("Bot fake-aerial-token")
	require.NoError(t, err)
	aerialSess.State = discordgo.NewState()
	require.NoError(t, aerialSess.State.GuildAdd(&discordgo.Guild{
		ID: "g-aerial",
		Channels: []*discordgo.Channel{
			{ID: "222222222222222222", Name: "aerial-dev"},
		},
	}))
	for _, ch := range aerialSess.State.Guilds[0].Channels {
		require.NoError(t, aerialSess.State.ChannelAdd(ch))
	}

	d := &DiscordWidgetDeliverer{
		session: aerialSess,
		agentSenders: map[string]DiscordWidgetSender{
			"aerial":      aerialSess,
			"agent-alpha": alphaSess,
		},
	}

	noSession := &DiscordWidgetDeliverer{}

	tests := []struct {
		name          string
		d             *DiscordWidgetDeliverer
		agent         string
		target        string
		wantChannelID string
	}{
		{
			name: "agent-alpha resolves from its own session", d: d, agent: "agent-alpha", target: "agent-alpha-dev",
			wantChannelID: "111111111111111111",
		},
		{name: "aerial resolves from default session", d: d, agent: "aerial", target: "aerial-dev", wantChannelID: "222222222222222222"},
		{name: "unknown agent falls back to default session", d: d, agent: "agent-beta", target: "aerial-dev", wantChannelID: "222222222222222222"},
		{name: "empty target returns fallback", d: noSession, target: " # ", wantChannelID: "fb"},
		{name: "snowflake target returns itself", d: noSession, target: "123456789012345678", wantChannelID: "123456789012345678"},
		{name: "name without any session returns fallback", d: noSession, target: "nonexistent-chan", wantChannelID: "fb"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.wantChannelID, tc.d.resolveChannelID(tc.agent, tc.target, "fb"))
		})
	}
}
