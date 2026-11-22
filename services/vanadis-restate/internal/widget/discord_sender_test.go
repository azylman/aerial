package widget

import (
	"context"
	"errors"
	"fmt"
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

			id, err := DeliverStatusWidget(mock, "azylman", tc.req, now)
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

	d, nErr := NewDiscordWidgetDeliverer("", "", "azylman", nil, filepath.Join(t.TempDir(), "config.yaml"))
	require.NoError(t, nErr)
	_, dErr := d.DeliverStatusWidget(context.Background(), ericlient.StatusWidgetRequest{ChannelID: "c1"})
	require.Error(t, dErr)
	require.ErrorIs(t, dErr, ErrNoSenderForAgent)

	assert.Equal(t, "", d.redirectChannelIfNoBuildUpdates(""))
	assert.Equal(t, "fallback", d.resolveChannelID("", "fallback"))
	assert.Equal(t, "123456789012345678", d.resolveChannelID("123456789012345678", "fallback"))
	assert.Equal(t, "fallback", d.resolveChannelID("nonexistent-chan", "fallback"))

	sess, err := discordgo.New("Bot fake-token")
	require.NoError(t, err)
	dWithSession := &DiscordWidgetDeliverer{
		session:      sess,
		agentSenders: map[string]DiscordWidgetSender{"aerial": sess},
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
			name:       "missing sender returns error",
			nilSender:  true,
			channelID:  "123456",
			content:    "alert",
			wantErr:    true,
			errContain: "no discord sender configured for agent \"aerial\"",
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
			senders := map[string]DiscordWidgetSender{}

			if !tc.nilDeliverer {
				if !tc.nilSender {
					mock = &mockWidgetSender{sendErr: tc.sendErr}
					senders["aerial"] = mock
				}
				d = &DiscordWidgetDeliverer{
					agentSenders: senders,
					nowFn:        func() time.Time { return now },
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
	d, err := NewDiscordWidgetDeliverer("", "", "azylman", nil, filepath.Join(t.TempDir(), "config.yaml"))
	require.NoError(t, err)
	d.agentSenders = map[string]DiscordWidgetSender{"aerial": mockSender}
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
			d, err := NewDiscordWidgetDeliverer(tc.agent, tc.token, "azylman", tc.environ, tc.configPath)
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
