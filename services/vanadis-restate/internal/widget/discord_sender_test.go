package widget

import (
	"context"
	"errors"
	"fmt"
	"net/http"
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

func TestShouldSinkWidget(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	twentyMinsAgo := now.Add(-20 * time.Minute)
	fiveMinsAgo := now.Add(-5 * time.Minute)

	tests := []struct {
		name       string
		nilSender  bool
		channelID  string
		messageID  string
		msgsErr    error
		msgs       []*discordgo.Message
		expectSink bool
	}{
		{
			name:       "nil sender returns false",
			nilSender:  true,
			channelID:  "chan-1",
			messageID:  "msg-1",
			expectSink: false,
		},
		{
			name:       "empty channel ID returns false",
			channelID:  "",
			messageID:  "msg-1",
			expectSink: false,
		},
		{
			name:       "empty message ID returns false",
			channelID:  "chan-1",
			messageID:  "",
			expectSink: false,
		},
		{
			name:       "messages error returns false",
			channelID:  "chan-1",
			messageID:  "msg-1",
			msgsErr:    errors.New("fetch error"),
			expectSink: false,
		},
		{
			name:       "empty messages returns false",
			channelID:  "chan-1",
			messageID:  "msg-1",
			msgs:       []*discordgo.Message{},
			expectSink: false,
		},
		{
			name:      "fewer than threshold and recent returns false",
			channelID: "chan-1",
			messageID: "msg-1",
			msgs: []*discordgo.Message{
				{ID: "m2", Timestamp: fiveMinsAgo},
				{ID: "m3", Timestamp: fiveMinsAgo},
			},
			expectSink: false,
		},
		{
			name:      "exactly 4 regular messages triggers sink",
			channelID: "chan-1",
			messageID: "msg-1",
			msgs: []*discordgo.Message{
				{ID: "m2", Timestamp: fiveMinsAgo},
				{ID: "m3", Timestamp: fiveMinsAgo},
				{ID: "m4", Timestamp: fiveMinsAgo},
				{ID: "m5", Timestamp: fiveMinsAgo},
			},
			expectSink: true,
		},
		{
			name:      "Components V2 messages are ignored from count",
			channelID: "chan-1",
			messageID: "msg-1",
			msgs: []*discordgo.Message{
				{ID: "m2", Timestamp: fiveMinsAgo, Flags: discordgo.MessageFlagsIsComponentsV2},
				{ID: "m3", Timestamp: fiveMinsAgo},
				{ID: "m4", Timestamp: fiveMinsAgo, Flags: discordgo.MessageFlagsIsComponentsV2},
				{ID: "m5", Timestamp: fiveMinsAgo},
			},
			expectSink: false,
		},
		{
			name:      "self message ID is ignored from count",
			channelID: "chan-1",
			messageID: "msg-1",
			msgs: []*discordgo.Message{
				{ID: "msg-1", Timestamp: fiveMinsAgo},
				{ID: "m2", Timestamp: fiveMinsAgo},
				{ID: "m3", Timestamp: fiveMinsAgo},
				{ID: "m4", Timestamp: fiveMinsAgo},
			},
			expectSink: false,
		},
		{
			name:      "idle threshold exceeded triggers sink with fewer messages",
			channelID: "chan-1",
			messageID: "msg-1",
			msgs: []*discordgo.Message{
				{ID: "m2", Timestamp: twentyMinsAgo},
			},
			expectSink: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var sender DiscordWidgetSender
			if !tc.nilSender {
				sender = &mockWidgetSender{msgs: tc.msgs, msgsErr: tc.msgsErr}
			}
			result := shouldSinkWidget(sender, tc.channelID, tc.messageID, now)
			assert.Equal(t, tc.expectSink, result)
		})
	}
}

func TestDeliverStatusWidgetFunction(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	notFoundErr := &discordgo.RESTError{Response: &http.Response{StatusCode: http.StatusNotFound}}

	tests := []struct {
		name          string
		nilSender     bool
		req           ericlient.StatusWidgetRequest
		nilSendMsg    bool
		sendErr       error
		sendMsg       *discordgo.Message
		editErr       error
		editMsg       *discordgo.Message
		deleteErr     error
		msgs          []*discordgo.Message
		expectID      string
		expectErrSub  string
		expectDeleted bool
	}{
		{
			name:         "nil sender returns error",
			nilSender:    true,
			req:          ericlient.StatusWidgetRequest{ChannelID: "chan-1"},
			expectErrSub: "discord widget sender is nil",
		},
		{
			name:         "empty channel ID returns error",
			req:          ericlient.StatusWidgetRequest{ChannelID: ""},
			expectErrSub: "channel_id is required",
		},
		{
			name:     "new message success",
			req:      ericlient.StatusWidgetRequest{ChannelID: "chan-1", Title: "Build 1", Status: "running"},
			sendMsg:  &discordgo.Message{ID: "msg-new-123"},
			expectID: "msg-new-123",
		},
		{
			name:         "new message send failure",
			req:          ericlient.StatusWidgetRequest{ChannelID: "chan-1", Title: "Build 1"},
			sendErr:      errors.New("network failure"),
			expectErrSub: "failed to send status widget message",
		},
		{
			name:         "new message send nil message returns error",
			req:          ericlient.StatusWidgetRequest{ChannelID: "chan-1", Title: "Build 1"},
			nilSendMsg:   true,
			expectErrSub: "received nil message",
		},
		{
			name:     "edit existing message success",
			req:      ericlient.StatusWidgetRequest{ChannelID: "chan-1", MessageID: "msg-orig-1", Title: "Build 1"},
			editMsg:  &discordgo.Message{ID: "msg-orig-1"},
			expectID: "msg-orig-1",
		},
		{
			name:     "edit existing message 404 recovers by creating new",
			req:      ericlient.StatusWidgetRequest{ChannelID: "chan-1", MessageID: "msg-deleted-1", Title: "Build 1"},
			editErr:  notFoundErr,
			sendMsg:  &discordgo.Message{ID: "msg-recreated-2"},
			expectID: "msg-recreated-2",
		},
		{
			name:         "edit existing message 404 recreate error fails",
			req:          ericlient.StatusWidgetRequest{ChannelID: "chan-1", MessageID: "msg-deleted-1", Title: "Build 1"},
			editErr:      notFoundErr,
			sendErr:      errors.New("recreate error"),
			expectErrSub: "failed to send new status widget message after 404 edit",
		},
		{
			name:         "edit existing message 404 recreate nil message fails",
			req:          ericlient.StatusWidgetRequest{ChannelID: "chan-1", MessageID: "msg-deleted-1", Title: "Build 1"},
			editErr:      notFoundErr,
			nilSendMsg:   true,
			expectErrSub: "received nil message",
		},
		{
			name:         "edit existing non-404 error fails",
			req:          ericlient.StatusWidgetRequest{ChannelID: "chan-1", MessageID: "msg-err-1", Title: "Build 1"},
			editErr:      errors.New("edit permission denied"),
			expectErrSub: "failed to edit status widget message",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var sender DiscordWidgetSender
			if !tc.nilSender {
				sender = &mockWidgetSender{
					sendMsg:    tc.sendMsg,
					nilSendMsg: tc.nilSendMsg,
					sendErr:    tc.sendErr,
					editMsg:    tc.editMsg,
					editErr:    tc.editErr,
					deleteErr:  tc.deleteErr,
					msgs:       tc.msgs,
				}
			}

			id, err := DeliverStatusWidget(sender, tc.req, now)
			if tc.expectErrSub != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.expectErrSub)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expectID, id)
			}
		})
	}
}

func TestDeliverStatusWidgetSinking(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	twentyMinsAgo := now.Add(-20 * time.Minute)

	tests := []struct {
		name          string
		req           ericlient.StatusWidgetRequest
		sendErr       error
		sendMsg       *discordgo.Message
		editMsg       *discordgo.Message
		deleteErr     error
		msgs          []*discordgo.Message
		expectID      string
		expectDeleted bool
	}{
		{
			name: "buried message sinks by creating new and deleting old",
			req:  ericlient.StatusWidgetRequest{ChannelID: "chan-1", MessageID: "msg-buried-1", Title: "Build 1"},
			msgs: []*discordgo.Message{
				{ID: "m2", Timestamp: twentyMinsAgo},
			},
			sendMsg:       &discordgo.Message{ID: "msg-sunk-new"},
			expectID:      "msg-sunk-new",
			expectDeleted: true,
		},
		{
			name: "buried message sink delete failure still succeeds",
			req:  ericlient.StatusWidgetRequest{ChannelID: "chan-1", MessageID: "msg-buried-del-fail", Title: "Build 1"},
			msgs: []*discordgo.Message{
				{ID: "m2", Timestamp: twentyMinsAgo},
			},
			sendMsg:       &discordgo.Message{ID: "msg-sunk-new-2"},
			deleteErr:     errors.New("delete failed"),
			expectID:      "msg-sunk-new-2",
			expectDeleted: true,
		},
		{
			name: "buried message sink send failure falls back to edit in place",
			req:  ericlient.StatusWidgetRequest{ChannelID: "chan-1", MessageID: "msg-buried-sink-fail", Title: "Build 1"},
			msgs: []*discordgo.Message{
				{ID: "m2", Timestamp: twentyMinsAgo},
			},
			sendErr:  errors.New("sink send failed"),
			editMsg:  &discordgo.Message{ID: "msg-buried-sink-fail"},
			expectID: "msg-buried-sink-fail",
		},
		{
			name: "buried message with skip_sink edits in place",
			req:  ericlient.StatusWidgetRequest{ChannelID: "chan-1", MessageID: "msg-buried-2", Title: "Build 1", SkipSink: true},
			msgs: []*discordgo.Message{
				{ID: "m2", Timestamp: twentyMinsAgo},
			},
			editMsg:  &discordgo.Message{ID: "msg-buried-2"},
			expectID: "msg-buried-2",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mock := &mockWidgetSender{
				sendMsg:   tc.sendMsg,
				sendErr:   tc.sendErr,
				editMsg:   tc.editMsg,
				deleteErr: tc.deleteErr,
				msgs:      tc.msgs,
			}

			id, err := DeliverStatusWidget(mock, tc.req, now)
			require.NoError(t, err)
			assert.Equal(t, tc.expectID, id)
			if tc.expectDeleted {
				assert.Equal(t, tc.req.MessageID, mock.lastDeleteID)
			}
		})
	}
}

func TestDelivererNilAndEmptyGuards(t *testing.T) {
	t.Parallel()
	var nilDeliverer *DiscordWidgetDeliverer
	_, err := nilDeliverer.DeliverStatusWidget(context.Background(), ericlient.StatusWidgetRequest{ChannelID: "c1"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "discord widget deliverer is nil")

	d, nErr := NewDiscordWidgetDeliverer("")
	require.NoError(t, nErr)
	_, dErr := d.DeliverStatusWidget(context.Background(), ericlient.StatusWidgetRequest{ChannelID: "c1"})
	require.Error(t, dErr)
	assert.Contains(t, dErr.Error(), "discord widget sender is nil")

	assert.Equal(t, "", d.redirectChannelIfNoBuildUpdates(""))
	assert.Equal(t, "fallback", d.resolveChannelID("", "fallback"))
	assert.Equal(t, "123456789012345678", d.resolveChannelID("123456789012345678", "fallback"))
	assert.Equal(t, "fallback", d.resolveChannelID("nonexistent-chan", "fallback"))

	sess, err := discordgo.New("Bot fake-token")
	require.NoError(t, err)
	dWithSession := &DiscordWidgetDeliverer{
		session: sess,
	}
	_, sErr := dWithSession.DeliverStatusWidget(context.Background(), ericlient.StatusWidgetRequest{})
	require.Error(t, sErr)
	assert.Contains(t, sErr.Error(), "channel_id is required")

	assert.NoError(t, dWithSession.DeliverDirectMessage(context.Background(), "", ""))
}

func TestDeliverDirectMessage(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		name            string
		nilDeliverer    bool
		nilSender       bool
		channelID       string
		content         string
		noBuildUpdates  bool
		sysChannel      string
		sendErr         error
		wantErr         bool
		errContain      string
		expectSent      bool
		expectChannelID string
		expectContent   string
	}{
		{
			name:         "nil deliverer returns error",
			nilDeliverer: true,
			channelID:    "123456",
			content:      "alert",
			wantErr:      true,
			errContain:   "discord widget deliverer is nil",
		},
		{
			name:       "nil sender returns error",
			nilSender:  true,
			channelID:  "123456",
			content:    "alert",
			wantErr:    true,
			errContain: "discord widget sender is nil",
		},
		{
			name:       "empty channel is no-op",
			channelID:  "   ",
			content:    "alert",
			expectSent: false,
		},
		{
			name:       "empty content is no-op",
			channelID:  "123456",
			content:    "   ",
			expectSent: false,
		},
		{
			name:            "channel policy redirect when no_build_updates",
			channelID:       "123456",
			content:         "alert msg",
			noBuildUpdates:  true,
			sysChannel:      "999888",
			expectSent:      true,
			expectChannelID: "999888",
			expectContent:   "alert msg",
		},
		{
			name:            "successful send suppresses allowed mentions",
			channelID:       "123456",
			content:         "hello world",
			expectSent:      true,
			expectChannelID: "123456",
			expectContent:   "hello world",
		},
		{
			name:       "send error propagates",
			channelID:  "123456",
			content:    "hello world",
			sendErr:    errors.New("discord rate limited"),
			wantErr:    true,
			errContain: "failed to send direct discord message: discord rate limited",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var d *DiscordWidgetDeliverer
			var mock *mockWidgetSender
			var sender DiscordWidgetSender

			if !tc.nilDeliverer {
				if !tc.nilSender {
					mock = &mockWidgetSender{sendErr: tc.sendErr}
					sender = mock
				}
				d = &DiscordWidgetDeliverer{
					sender: sender,
					nowFn:  func() time.Time { return now },
					channelPolicyResolver: func(channelID string) (bool, string) {
						return tc.noBuildUpdates, tc.sysChannel
					},
					resolvedChannels: make(map[string]cachedResolvedChannel),
					channelMetaCache: make(map[string]cachedChannelMeta),
				}
			}

			err := d.DeliverDirectMessage(context.Background(), tc.channelID, tc.content)
			if tc.wantErr {
				require.Error(t, err)
				if tc.errContain != "" {
					assert.Contains(t, err.Error(), tc.errContain)
				}
				return
			}
			require.NoError(t, err)
			if !tc.expectSent {
				if mock != nil {
					assert.Nil(t, mock.lastSendData)
				}
				return
			}
			require.NotNil(t, mock)
			require.NotNil(t, mock.lastSendData)
			assert.Equal(t, tc.expectChannelID, mock.lastSendChannelID)
			assert.Equal(t, tc.expectContent, mock.lastSendData.Content)
			require.NotNil(t, mock.lastSendData.AllowedMentions)
			assert.Empty(t, mock.lastSendData.AllowedMentions.Parse)
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

func TestBuildStatusWidgetComponentsTable(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	started := now.Add(-10 * time.Minute)

	tests := []struct {
		name          string
		req           ericlient.StatusWidgetRequest
		expectBanner  bool
		expectSummary string
		expectTextSub string
	}{
		{
			name: "single pr widget with banner",
			req: ericlient.StatusWidgetRequest{
				ID:        "pr-aerial-100",
				Title:     "Add widget",
				Banner:    "Important note",
				Status:    "running",
				StartedAt: started,
				Chunks: []ericlient.StatusWidgetChunk{
					{ID: "c1", Title: "Step 1", Status: "running", PRNumber: 100, Repo: "aerial"},
				},
			},
			expectBanner:  true,
			expectTextSub: "Important note",
		},
		{
			name: "multi-chunk completed build without banner",
			req: ericlient.StatusWidgetRequest{
				ID:        "build-12345",
				Title:     "Multi-chunk build",
				Status:    "completed",
				StartedAt: started,
				Chunks: []ericlient.StatusWidgetChunk{
					{ID: "c1", Title: "Chunk 1", Status: "completed"},
					{ID: "c2", Title: "Chunk 2", Status: "completed"},
				},
			},
			expectBanner:  false,
			expectTextSub: "Multi-chunk build",
		},
		{
			name: "build without title and earlier chunk start and action mode",
			req: ericlient.StatusWidgetRequest{
				ID:     "build-empty-title",
				Status: "completed",
				Chunks: []ericlient.StatusWidgetChunk{
					{
						ID:          "c1",
						Title:       "Action chunk",
						Status:      "completed",
						Mode:        "action",
						StartedAt:   started.Add(-5 * time.Minute),
						CompletedAt: started.Add(10 * time.Minute),
						ActiveStep:  "Step 1\n\n  \nStep 2",
					},
					{
						ID:          "c2",
						Title:       "Standard chunk",
						Status:      "deployed",
						Repo:        "azylman/mirrormere/sub",
						StartedAt:   started,
						CompletedAt: started.Add(20 * time.Minute),
					},
				},
			},
			expectBanner:  false,
			expectTextSub: "`action`",
		},
		{
			name: "collapsing build with more than ten chunks",
			req: ericlient.StatusWidgetRequest{
				ID:        "build-many-chunks",
				Title:     "Mega Build",
				Status:    "running",
				StartedAt: started,
				Chunks:    collapseFixture(15, 9, "merged"),
			},
			expectBanner:  false,
			expectSummary: "✅ ***7/15** earlier chunks completed and merged*",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			comps, err := BuildStatusWidgetComponents(tc.req, now)
			require.NoError(t, err)
			require.Len(t, comps, 1)

			container, ok := comps[0].(*discordgo.Container)
			require.True(t, ok)
			assert.NotNil(t, container.AccentColor)
			assert.NotEmpty(t, container.Components)

			fullText := extractTestComponentsText(comps)
			if tc.expectBanner {
				assert.Contains(t, fullText, tc.req.Banner)
			} else if tc.req.Banner != "" {
				assert.NotContains(t, fullText, tc.req.Banner)
			}
			if tc.expectSummary != "" {
				assert.Contains(t, fullText, tc.expectSummary)
			}
			if tc.expectTextSub != "" {
				assert.Contains(t, fullText, tc.expectTextSub)
			}
		})
	}
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

func TestDelivererWithNow(t *testing.T) {
	t.Parallel()
	fixedTime := time.Date(2026, 11, 19, 15, 0, 0, 0, time.UTC)
	mockSender := &mockWidgetSender{sendMsg: &discordgo.Message{ID: "m-clock"}}
	d, err := NewDiscordWidgetDeliverer("")
	require.NoError(t, err)
	d.sender = mockSender
	d.nowFn = func() time.Time { return fixedTime }

	req := ericlient.StatusWidgetRequest{ChannelID: "chan-1", Title: "Build Clock @everyone"}
	id, dErr := d.DeliverStatusWidget(context.Background(), req)
	require.NoError(t, dErr)
	assert.Equal(t, "m-clock", id)
	require.NotNil(t, mockSender.lastSendData)
	require.NotNil(t, mockSender.lastSendData.AllowedMentions)
	assert.Empty(t, mockSender.lastSendData.AllowedMentions.Parse)
	footerText := extractTestComponentsText(mockSender.lastSendData.Components)
	assert.Contains(t, footerText, fmt.Sprintf("<t:%d:R>", fixedTime.Unix()))
	assert.Contains(t, footerText, "@everyone")
}

func TestIsWidgetCompletedTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		req      ericlient.StatusWidgetRequest
		expected bool
	}{
		{
			name: "completed build with completed and merged chunks",
			req: ericlient.StatusWidgetRequest{
				Status: "completed",
				Chunks: []ericlient.StatusWidgetChunk{
					{Status: "completed"},
					{Status: "merged"},
				},
			},
			expected: true,
		},
		{
			name: "failed build with terminal chunks",
			req: ericlient.StatusWidgetRequest{
				Status: "failed",
				Chunks: []ericlient.StatusWidgetChunk{
					{Status: "failed"},
					{Status: "cancelled"},
				},
			},
			expected: true,
		},
		{
			name: "error build with terminal chunks",
			req: ericlient.StatusWidgetRequest{
				Status: "error",
				Chunks: []ericlient.StatusWidgetChunk{
					{Status: "error"},
				},
			},
			expected: true,
		},
		{
			name: "running build with terminal chunks returns false",
			req: ericlient.StatusWidgetRequest{
				Status: "running",
				Chunks: []ericlient.StatusWidgetChunk{
					{Status: "completed"},
				},
			},
			expected: false,
		},
		{
			name: "coding build with terminal chunks returns false",
			req: ericlient.StatusWidgetRequest{
				Status: "coding",
				Chunks: []ericlient.StatusWidgetChunk{
					{Status: "completed"},
				},
			},
			expected: false,
		},
		{
			name: "open pr build returns false",
			req: ericlient.StatusWidgetRequest{
				Status: "open",
				Chunks: []ericlient.StatusWidgetChunk{
					{Status: "completed"},
				},
			},
			expected: false,
		},
		{
			name: "pending build returns false",
			req: ericlient.StatusWidgetRequest{
				Status: "pending",
				Chunks: []ericlient.StatusWidgetChunk{
					{Status: "completed"},
				},
			},
			expected: false,
		},
		{
			name: "paused build returns false",
			req: ericlient.StatusWidgetRequest{
				Status: "paused",
				Chunks: []ericlient.StatusWidgetChunk{
					{Status: "completed"},
				},
			},
			expected: false,
		},
		{
			name: "stalled build returns false",
			req: ericlient.StatusWidgetRequest{
				Status: "stalled",
				Chunks: []ericlient.StatusWidgetChunk{
					{Status: "completed"},
				},
			},
			expected: false,
		},
		{
			name: "failed build with running chunk returns false",
			req: ericlient.StatusWidgetRequest{
				Status: "failed",
				Chunks: []ericlient.StatusWidgetChunk{
					{Status: "failed"},
					{Status: "running"},
				},
			},
			expected: false,
		},
		{
			name: "completed build with running chunk returns false",
			req: ericlient.StatusWidgetRequest{
				Status: "completed",
				Chunks: []ericlient.StatusWidgetChunk{
					{Status: "running"},
				},
			},
			expected: false,
		},
		{
			name: "empty status and chunks returns false",
			req: ericlient.StatusWidgetRequest{
				Status: "",
			},
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.expected, isWidgetCompleted(tc.req))
		})
	}
}

func TestFormatWidgetFooterTextTable(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, 11, 19, 10, 0, 0, 0, time.UTC)
	end := start.Add(45 * time.Minute)
	now := start.Add(60 * time.Minute)

	tests := []struct {
		name      string
		req       ericlient.StatusWidgetRequest
		expectSub string
		rejectSub string
	}{
		{
			name: "zero started at returns updated only",
			req: ericlient.StatusWidgetRequest{
				Status: "running",
			},
			expectSub: fmt.Sprintf("*Updated <t:%d:R>*", now.Unix()),
			rejectSub: "Duration",
		},
		{
			name: "running build shows started and updated",
			req: ericlient.StatusWidgetRequest{
				Status:    "running",
				StartedAt: start,
				Chunks: []ericlient.StatusWidgetChunk{
					{Status: "running", StartedAt: start},
				},
			},
			expectSub: fmt.Sprintf("*Started <t:%d:R> · Updated <t:%d:R>*", start.Unix(), now.Unix()),
			rejectSub: "Duration",
		},
		{
			name: "completed build shows duration footer",
			req: ericlient.StatusWidgetRequest{
				Status:    "completed",
				StartedAt: start,
				Chunks: []ericlient.StatusWidgetChunk{
					{Status: "completed", StartedAt: start, CompletedAt: end},
				},
			},
			expectSub: fmt.Sprintf("*Started <t:%d:R> · Duration 45m*", start.Unix()),
			rejectSub: "Updated",
		},
		{
			name: "failed build with terminal chunks shows duration footer",
			req: ericlient.StatusWidgetRequest{
				Status:    "failed",
				StartedAt: start,
				Chunks: []ericlient.StatusWidgetChunk{
					{Status: "failed", StartedAt: start, CompletedAt: end},
				},
			},
			expectSub: fmt.Sprintf("*Started <t:%d:R> · Duration 45m*", start.Unix()),
			rejectSub: "Updated",
		},
		{
			name: "failed build with running chunk shows started and updated",
			req: ericlient.StatusWidgetRequest{
				Status:    "failed",
				StartedAt: start,
				Chunks: []ericlient.StatusWidgetChunk{
					{Status: "failed", StartedAt: start, CompletedAt: end},
					{Status: "running", StartedAt: start},
				},
			},
			expectSub: fmt.Sprintf("*Started <t:%d:R> · Updated <t:%d:R>*", start.Unix(), now.Unix()),
			rejectSub: "Duration",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := formatWidgetFooterText(tc.req, now)
			assert.Contains(t, got, tc.expectSub)
			if tc.rejectSub != "" {
				assert.NotContains(t, got, tc.rejectSub)
			}
		})
	}
}

func TestNewDiscordWidgetDelivererTokens(t *testing.T) {
	tests := []struct {
		name       string
		token      string
		envToken   string
		configPath string
		hasSession bool
	}{
		{"token with Bot prefix", "Bot test-token", "", "", true},
		{"token without Bot prefix", "test-token-raw", "", "/custom/path", true},
		{"token from env", "", "env-token", "", true},
		{"empty token", "", "", "", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.envToken != "" {
				t.Setenv("DISCORD_TOKEN", tc.envToken)
			} else {
				t.Setenv("DISCORD_TOKEN", "")
			}
			d, err := NewDiscordWidgetDeliverer(tc.token, tc.configPath)
			require.NoError(t, err)
			assert.NotNil(t, d)
			if tc.hasSession {
				assert.NotNil(t, d.session)
			} else {
				assert.Nil(t, d.session)
			}
			if tc.configPath != "" {
				assert.Equal(t, tc.configPath, d.configPath)
			}
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
				sender: mock,
				nowFn:  func() time.Time { return now },
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
			deliverer.sender = firstSender

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
			deliverer.sender = secondSender

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

func TestIsNotFoundError(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"nil error", nil, false},
		{"standard error", errors.New("boom"), false},
		{"404 RESTError", &discordgo.RESTError{Response: &http.Response{StatusCode: http.StatusNotFound}}, true},
		{"code 10008 RESTError", &discordgo.RESTError{Message: &discordgo.APIErrorMessage{Code: 10008}}, true},
		{"other RESTError", &discordgo.RESTError{Response: &http.Response{StatusCode: http.StatusInternalServerError}}, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.expected, isNotFoundError(tc.err))
		})
	}
}
