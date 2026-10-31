package discord

import (
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
)

type mockWidgetSender struct {
	sendFunc       func(channelID string, data *discordgo.MessageSend, options ...discordgo.RequestOption) (*discordgo.Message, error)
	editFunc       func(m *discordgo.MessageEdit, options ...discordgo.RequestOption) (*discordgo.Message, error)
	afterMsgs      []*discordgo.Message
	messagesErr    error
	sendReplyID    string
	sendErr        error
	editErr        error
	deleteErr      error
	sentPayloads   []*discordgo.MessageSend
	editedPayloads []*discordgo.MessageEdit
	deletedIDs     []string
	queriedAfterID string
	queriedLimit   int
}

func (m *mockWidgetSender) ChannelMessageSendComplex(channelID string, data *discordgo.MessageSend, options ...discordgo.RequestOption) (*discordgo.Message, error) {
	m.sentPayloads = append(m.sentPayloads, data)
	if m.sendFunc != nil {
		return m.sendFunc(channelID, data, options...)
	}
	if m.sendErr != nil {
		return nil, m.sendErr
	}
	id := m.sendReplyID
	if id == "" {
		id = "msg-default-send"
	}
	return &discordgo.Message{ID: id}, nil
}

func (m *mockWidgetSender) ChannelMessageEditComplex(medit *discordgo.MessageEdit, options ...discordgo.RequestOption) (*discordgo.Message, error) {
	m.editedPayloads = append(m.editedPayloads, medit)
	if m.editFunc != nil {
		return m.editFunc(medit, options...)
	}
	if m.editErr != nil {
		return nil, m.editErr
	}
	return &discordgo.Message{ID: medit.ID}, nil
}

func (m *mockWidgetSender) ChannelMessages(channelID string, limit int, beforeID, afterID, aroundID string, options ...discordgo.RequestOption) ([]*discordgo.Message, error) {
	m.queriedAfterID = afterID
	m.queriedLimit = limit
	if m.messagesErr != nil {
		return nil, m.messagesErr
	}
	return m.afterMsgs, nil
}

func (m *mockWidgetSender) ChannelMessageDelete(channelID, messageID string, options ...discordgo.RequestOption) error {
	m.deletedIDs = append(m.deletedIDs, messageID)
	return m.deleteErr
}

func TestAccentColor(t *testing.T) {
	tests := []struct {
		status string
		want   int
	}{
		{"completed", 0x10B981},
		{"deployed", 0x10B981},
		{"failed", 0xEF4444},
		{"conflict", 0xEF4444},
		{"closed", 0xEF4444},
		{"paused", 0xF59E0B},
		{"stalled", 0xF59E0B},
		{"cancelled", 0xF59E0B},
		{"running", 0xA855F7},
		{"coding", 0xA855F7},
		{"merged", 0xA855F7},
		{"synced", 0xA855F7},
		{"deploying", 0xA855F7},
		{"open", 0xA855F7},
		{"pr_opened", 0xA855F7},
		{"pending", 0xA855F7},
		{"unknown", 0xA855F7},
	}

	for _, tt := range tests {
		got := AccentColor(tt.status)
		if got != tt.want {
			t.Errorf("AccentColor(%q) = 0x%X, want 0x%X", tt.status, got, tt.want)
		}
	}
}

func TestStatusBadge(t *testing.T) {
	tests := []struct {
		status string
		want   string
	}{
		{"running", "🔨"},
		{"coding", "🔨"},
		{"pr_opened", "👀"},
		{"open", "👀"},
		{"merged", "🔀"},
		{"synced", "🔄"},
		{"deploying", "🚀"},
		{"deployed", "✅"},
		{"completed", "✅"},
		{"success", "✅"},
		{"failed", "❌"},
		{"conflict", "❌"},
		{"pending", "⏳"},
		{"other", "⏳"},
	}

	for _, tt := range tests {
		got := statusBadge(tt.status)
		if got != tt.want {
			t.Errorf("statusBadge(%q) = %q, want %q", tt.status, got, tt.want)
		}
	}
}

func TestNormalizeRepo(t *testing.T) {
	full, short := NormalizeRepo("aerial")
	if full != "azylman/aerial" || short != "aerial" {
		t.Fatalf("unexpected repo normalization: %s, %s", full, short)
	}

	full, short = NormalizeRepo("azylman/aerial-config")
	if full != "azylman/aerial-config" || short != "aerial-config" {
		t.Fatalf("unexpected repo normalization: %s, %s", full, short)
	}

	full, short = NormalizeRepo("")
	if full != "" || short != "" {
		t.Fatalf("unexpected repo normalization for empty: %s, %s", full, short)
	}
}

func TestBuildStatusWidgetComponents_VanadisBuild(t *testing.T) {
	now := time.Unix(1760000000, 0)
	req := StatusWidgetRequest{
		ChannelID: "1542423172400291873",
		ID:        "build-1792124461-490d903b",
		Tier:      3,
		Title:     "Discord status tracker widget",
		Status:    "running",
		Banner:    "⚠️ Build in progress",
		Chunks: []StatusWidgetChunk{
			{
				Index:    1,
				Repo:     "aerial",
				Title:    "Schema migration",
				Status:   "merged",
				PRNumber: 735,
			},
			{
				Index:         2,
				Repo:          "aerial",
				Title:         "Silent prompt",
				Status:        "deploying",
				ActiveStep:    "-# ↳ `aerial`: ⏳ building image\n\n↳ `synapse`: 🔄 starting\n",
				PRNumber:      736,
				DeployingJobs: []string{"aerial-brain", "synapse"},
			},
			{
				Index:      3,
				Repo:       "aerial",
				Title:      "Stateless widget endpoint",
				Status:     "coding",
				ActiveStep: "Compiling binary",
			},
		},
	}

	comps, err := BuildStatusWidgetComponents(req, now)
	if err != nil {
		t.Fatalf("BuildStatusWidgetComponents failed: %v", err)
	}
	if len(comps) != 1 {
		t.Fatalf("expected 1 container component, got %d", len(comps))
	}

	container, ok := comps[0].(*discordgo.Container)
	if !ok {
		t.Fatalf("expected *discordgo.Container, got %T", comps[0])
	}

	if container.AccentColor == nil || *container.AccentColor != 0xA855F7 {
		t.Errorf("expected accent color 0xA855F7, got %v", container.AccentColor)
	}

	// Inner components check: Header, Banner, Chunk 1 (Section with button), Chunk 2 (Section with button), Chunk 3 (TextDisplay), Footer
	if len(container.Components) != 6 {
		t.Fatalf("expected 6 container components, got %d", len(container.Components))
	}

	header, ok := container.Components[0].(*discordgo.TextDisplay)
	if !ok || header.Content != "### 🛠️ Discord status tracker widget" {
		t.Errorf("unexpected header: %+v", header)
	}

	banner, ok := container.Components[1].(*discordgo.TextDisplay)
	if !ok || banner.Content != "⚠️ Build in progress" {
		t.Errorf("unexpected banner: %+v", banner)
	}

	// Chunk 1 has PRNumber > 0 and Repo, so it's a Section with Button, without [index]
	c1Sec, ok := container.Components[2].(*discordgo.Section)
	if !ok {
		t.Fatalf("expected chunk 1 to be *discordgo.Section, got %T", container.Components[2])
	}
	c1Text := c1Sec.Components[0].(*discordgo.TextDisplay)
	if c1Text.Content != "🔀 **`aerial`**: Schema migration" {
		t.Errorf("unexpected chunk 1 row text: %q", c1Text.Content)
	}
	btn, ok := c1Sec.Accessory.(*discordgo.Button)
	if !ok || btn.URL != "https://github.com/azylman/aerial/pull/735" || btn.Label != "PR #735 ↗" {
		t.Errorf("unexpected button accessory: %+v", c1Sec.Accessory)
	}

	// Chunk 2 has PRNumber > 0, no DeployingJobs rendered in row, but has ActiveStep
	c2Sec, ok := container.Components[3].(*discordgo.Section)
	if !ok {
		t.Fatalf("expected chunk 2 to be *discordgo.Section, got %T", container.Components[3])
	}
	c2Text := c2Sec.Components[0].(*discordgo.TextDisplay)
	if c2Text.Content != "🚀 **`aerial`**: Silent prompt\n-# ↳ `aerial`: ⏳ building image\n-# ↳ `synapse`: 🔄 starting" {
		t.Errorf("unexpected chunk 2 row text: %q", c2Text.Content)
	}

	// Chunk 3 has no PRNumber, so it's a TextDisplay with ActiveStep
	c3Text, ok := container.Components[4].(*discordgo.TextDisplay)
	if !ok || c3Text.Content != "🔨 **`aerial`**: Stateless widget endpoint\n-# ↳ Compiling binary" {
		t.Errorf("unexpected chunk 3 component: %+v", container.Components[4])
	}

	// Footer
	footer, ok := container.Components[5].(*discordgo.TextDisplay)
	if !ok || footer.Content != "*Updated <t:1760000000:R>*" {
		t.Errorf("unexpected footer: %+v", footer)
	}

	// Empty Title fallback on VanadisBuild header + empty chunk Repo fallback
	emptyTitleComps, err := BuildStatusWidgetComponents(StatusWidgetRequest{
		ID:     "build-empty-title",
		Status: "running",
		Chunks: []StatusWidgetChunk{{Title: "Fallback repo chunk", Status: "pending"}},
	}, now)
	if err != nil {
		t.Fatalf("BuildStatusWidgetComponents empty title failed: %v", err)
	}
	emptyContainer := emptyTitleComps[0].(*discordgo.Container)
	emptyHeader := emptyContainer.Components[0].(*discordgo.TextDisplay)
	if emptyHeader.Content != "### 🛠️ Vanadis Build" {
		t.Errorf("expected fallback header '### 🛠️ Vanadis Build', got %q", emptyHeader.Content)
	}
	fallbackChunk := emptyContainer.Components[1].(*discordgo.TextDisplay)
	if fallbackChunk.Content != "⏳ **`aerial`**: Fallback repo chunk" {
		t.Errorf("expected fallback chunk '⏳ **`aerial`**: Fallback repo chunk', got %q", fallbackChunk.Content)
	}
}

func TestFormatChunkActiveStepLines(t *testing.T) {
	tests := []struct {
		name       string
		activeStep string
		want       string
	}{
		{
			name:       "empty string",
			activeStep: "",
			want:       "",
		},
		{
			name:       "whitespace only",
			activeStep: "   \n\t  \n  ",
			want:       "",
		},
		{
			name:       "single line clean",
			activeStep: "Running deployment checks...",
			want:       "\n-# ↳ Running deployment checks...",
		},
		{
			name:       "single line with redundant prefixes and spaces",
			activeStep: "  -#   ↳   Running deployment checks...  ",
			want:       "\n-# ↳ Running deployment checks...",
		},
		{
			name:       "multi-line with empty lines and mixed prefixes",
			activeStep: "-# ↳ `aerial`: ⏳ building image\n\n   \n↳ `synapse`: 🔄 starting\n",
			want:       "\n-# ↳ `aerial`: ⏳ building image\n-# ↳ `synapse`: 🔄 starting",
		},
		{
			name:       "only prefixes and empty lines",
			activeStep: "-# \n↳\n -# ↳  ",
			want:       "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := formatChunkActiveStepLines(tc.activeStep); got != tc.want {
				t.Errorf("formatChunkActiveStepLines(%q) = %q, want %q", tc.activeStep, got, tc.want)
			}
		})
	}
}

func TestBuildStatusWidgetComponents_StandalonePR(t *testing.T) {
	req := StatusWidgetRequest{
		ChannelID: "1542423172400291873",
		Title:     "Fix database connection pool",
		Status:    "deployed",
		Chunks: []StatusWidgetChunk{
			{
				Index:    1,
				Repo:     "azylman/aerial",
				Title:    "Fix connection leak",
				Status:   "deployed",
				PRNumber: 123,
			},
		},
	}

	comps, err := BuildStatusWidgetComponents(req, time.Time{})
	if err != nil {
		t.Fatalf("BuildStatusWidgetComponents failed: %v", err)
	}
	container := comps[0].(*discordgo.Container)
	if container.AccentColor == nil || *container.AccentColor != 0x10B981 {
		t.Errorf("expected emerald color 0x10B981, got %v", container.AccentColor)
	}

	header := container.Components[0].(*discordgo.TextDisplay)
	if !strings.Contains(header.Content, "Pull Request #123 · `aerial`") {
		t.Errorf("unexpected standalone PR header: %s", header.Content)
	}
}

func TestBuildStatusWidgetComponents_CeilingGuard(t *testing.T) {
	var chunks []StatusWidgetChunk
	for i := 1; i <= 15; i++ {
		st := "pending"
		if i <= 8 {
			st = "merged"
		}
		chunks = append(chunks, StatusWidgetChunk{
			Index:  i,
			Repo:   "aerial",
			Title:  "Task",
			Status: st,
		})
	}

	req := StatusWidgetRequest{
		ID:     "build-100",
		Title:  "Large build",
		Status: "running",
		Chunks: chunks,
	}

	comps, err := BuildStatusWidgetComponents(req, time.Now())
	if err != nil {
		t.Fatalf("failed to build components: %v", err)
	}

	container := comps[0].(*discordgo.Container)
	foundSummary := false
	for _, c := range container.Components {
		if td, ok := c.(*discordgo.TextDisplay); ok {
			if strings.Contains(td.Content, "earlier chunks completed and merged") {
				foundSummary = true
				break
			}
		}
	}

	if !foundSummary {
		t.Error("expected collapsed summary component for >10 chunks")
	}
	if len(container.Components) > 40 {
		t.Errorf("component count exceeded 40: %d", len(container.Components))
	}
}

func TestDeliverStatusWidget_CreateNew(t *testing.T) {
	var sentPayload *discordgo.MessageSend
	sender := &mockWidgetSender{
		sendFunc: func(channelID string, data *discordgo.MessageSend, options ...discordgo.RequestOption) (*discordgo.Message, error) {
			sentPayload = data
			return &discordgo.Message{ID: "created-msg-999"}, nil
		},
	}

	req := StatusWidgetRequest{
		ChannelID: "chan-1",
		ID:        "build-1",
		Title:     "Build 1",
		Status:    "running",
	}

	msgID, err := DeliverStatusWidget(sender, req, time.Now())
	if err != nil {
		t.Fatalf("DeliverStatusWidget failed: %v", err)
	}
	if msgID != "created-msg-999" {
		t.Fatalf("expected msgID 'created-msg-999', got %q", msgID)
	}
	if sentPayload == nil || sentPayload.Flags != discordgo.MessageFlagsIsComponentsV2 {
		t.Errorf("expected MessageFlagsIsComponentsV2 on send, got %+v", sentPayload)
	}
}

func TestDeliverStatusWidget_EditSuccess(t *testing.T) {
	var editedPayload *discordgo.MessageEdit
	sender := &mockWidgetSender{
		editFunc: func(m *discordgo.MessageEdit, options ...discordgo.RequestOption) (*discordgo.Message, error) {
			editedPayload = m
			return &discordgo.Message{ID: m.ID}, nil
		},
	}

	req := StatusWidgetRequest{
		ChannelID: "chan-1",
		MessageID: "msg-existing-123",
		ID:        "build-1",
		Title:     "Build 1",
		Status:    "completed",
	}

	msgID, err := DeliverStatusWidget(sender, req, time.Now())
	if err != nil {
		t.Fatalf("DeliverStatusWidget failed: %v", err)
	}
	if msgID != "msg-existing-123" {
		t.Fatalf("expected msgID 'msg-existing-123', got %q", msgID)
	}
	if editedPayload == nil || editedPayload.Flags != discordgo.MessageFlagsIsComponentsV2 {
		t.Errorf("expected MessageFlagsIsComponentsV2 on edit, got %+v", editedPayload)
	}
}

func TestDeliverStatusWidget_Edit404FallbackToSend(t *testing.T) {
	editCalled := false
	sendCalled := false

	sender := &mockWidgetSender{
		editFunc: func(m *discordgo.MessageEdit, options ...discordgo.RequestOption) (*discordgo.Message, error) {
			editCalled = true
			return nil, &discordgo.RESTError{
				Response: &http.Response{StatusCode: http.StatusNotFound},
				Message:  &discordgo.APIErrorMessage{Code: 10008, Message: "Unknown Message"},
			}
		},
		sendFunc: func(channelID string, data *discordgo.MessageSend, options ...discordgo.RequestOption) (*discordgo.Message, error) {
			sendCalled = true
			return &discordgo.Message{ID: "new-fallback-msg"}, nil
		},
	}

	req := StatusWidgetRequest{
		ChannelID: "chan-1",
		MessageID: "msg-stale-404",
		ID:        "build-1",
		Title:     "Build 1",
		Status:    "running",
	}

	msgID, err := DeliverStatusWidget(sender, req, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !editCalled {
		t.Error("expected edit to be called first")
	}
	if !sendCalled {
		t.Error("expected send fallback to be called on 404")
	}
	if msgID != "new-fallback-msg" {
		t.Fatalf("expected 'new-fallback-msg', got %q", msgID)
	}
}

func TestDeliverStatusWidget_AutoSink(t *testing.T) {
	t.Parallel()
	threeMsgs := []*discordgo.Message{{ID: "m1"}, {ID: "m2"}, {ID: "m3"}}
	fourMsgs := []*discordgo.Message{{ID: "m1"}, nil, {ID: "m2"}, {ID: "m3"}, {ID: "m4"}}
	newerWidgetMsgs := []*discordgo.Message{{ID: "m1"}, {ID: "m2"}, {ID: "m3"}, {ID: "msg-newer-widget", Flags: discordgo.MessageFlagsIsComponentsV2}}
	widgetThenFourMsgs := []*discordgo.Message{{ID: "msg-newer-widget", Flags: discordgo.MessageFlagsIsComponentsV2}, {ID: "m1"}, {ID: "m2"}, {ID: "m3"}, {ID: "m4"}}
	sunkFlags := discordgo.MessageFlagsIsComponentsV2 | discordgo.MessageFlagsSuppressNotifications

	tests := []struct {
		name          string
		skipSink      bool
		afterMsgs     []*discordgo.Message
		messagesErr   error
		sendReplyID   string
		sendErr       error
		deleteErr     error
		wantMsgID     string
		wantSentCount int
		wantEditCount int
		wantDeletedID string
		wantSentFlags discordgo.MessageFlags
		wantQueried   bool
	}{
		{name: "fewer messages than threshold edits in place", afterMsgs: threeMsgs, wantMsgID: "msg-old-100", wantEditCount: 1, wantQueried: true},
		{name: "channel messages error falls back to edit in place", messagesErr: errors.New("discord 502"), wantMsgID: "msg-old-100", wantEditCount: 1, wantQueried: true},
		{name: "newer components v2 widget with fewer than threshold regular messages edits in place", afterMsgs: newerWidgetMsgs, wantMsgID: "msg-old-100", wantEditCount: 1, wantQueried: true},
		{name: "adjacent components v2 widget followed by threshold regular messages sinks to bottom", afterMsgs: widgetThenFourMsgs, sendReplyID: "msg-sunk-888", wantMsgID: "msg-sunk-888", wantSentCount: 1, wantDeletedID: "msg-old-100", wantSentFlags: sunkFlags, wantQueried: true},
		{name: "buried by threshold regular messages sinks to bottom and deletes old", afterMsgs: fourMsgs, sendReplyID: "msg-sunk-999", deleteErr: errors.New("ignored delete failure"), wantMsgID: "msg-sunk-999", wantSentCount: 1, wantDeletedID: "msg-old-100", wantSentFlags: sunkFlags, wantQueried: true},
		{name: "sink send error falls back to editing existing message in place", afterMsgs: fourMsgs, sendErr: errors.New("rate limit on send"), wantMsgID: "msg-old-100", wantSentCount: 1, wantEditCount: 1, wantQueried: true},
		{name: "skip sink bypasses auto sinking even when buried", skipSink: true, afterMsgs: fourMsgs, wantMsgID: "msg-old-100", wantEditCount: 1, wantQueried: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sender := &mockWidgetSender{afterMsgs: tc.afterMsgs, messagesErr: tc.messagesErr, sendReplyID: tc.sendReplyID, sendErr: tc.sendErr, deleteErr: tc.deleteErr}
			req := StatusWidgetRequest{ChannelID: "chan-1", MessageID: "msg-old-100", ID: "build-1", Title: "Build 1", Status: "running", SkipSink: tc.skipSink}
			msgID, err := DeliverStatusWidget(sender, req, time.Now())
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if msgID != tc.wantMsgID {
				t.Errorf("got msgID %q, want %q", msgID, tc.wantMsgID)
			}
			if tc.wantQueried {
				if sender.queriedAfterID != "msg-old-100" || sender.queriedLimit != WidgetSinkQueryLimit {
					t.Errorf("got query afterID=%q limit=%d, want afterID=msg-old-100 limit=%d", sender.queriedAfterID, sender.queriedLimit, WidgetSinkQueryLimit)
				}
			} else {
				if sender.queriedAfterID != "" || sender.queriedLimit != 0 {
					t.Errorf("expected no message queries when skipSink=true, got afterID=%q limit=%d", sender.queriedAfterID, sender.queriedLimit)
				}
			}
			if len(sender.sentPayloads) != tc.wantSentCount || len(sender.editedPayloads) != tc.wantEditCount {
				t.Errorf("got sends=%d edits=%d, want sends=%d edits=%d", len(sender.sentPayloads), len(sender.editedPayloads), tc.wantSentCount, tc.wantEditCount)
			}
			gotDeleted := ""
			if len(sender.deletedIDs) > 0 {
				gotDeleted = sender.deletedIDs[0]
			}
			if gotDeleted != tc.wantDeletedID {
				t.Errorf("got deletedID %q, want %q", gotDeleted, tc.wantDeletedID)
			}
			if tc.wantSentFlags != 0 && (len(sender.sentPayloads) == 0 || sender.sentPayloads[0].Flags != tc.wantSentFlags) {
				t.Errorf("got sent flags %v, want %v", sender.sentPayloads[0].Flags, tc.wantSentFlags)
			}
		})
	}
}

func TestDeliverStatusWidget_ValidationAndErrors(t *testing.T) {
	t.Run("nil sender", func(t *testing.T) {
		_, err := DeliverStatusWidget(nil, StatusWidgetRequest{ChannelID: "123"}, time.Now())
		if err == nil {
			t.Fatal("expected error for nil sender")
		}
	})

	t.Run("empty channel_id", func(t *testing.T) {
		sender := &mockWidgetSender{}
		_, err := DeliverStatusWidget(sender, StatusWidgetRequest{}, time.Now())
		if err == nil {
			t.Fatal("expected error for empty channel_id")
		}
	})

	t.Run("edit generic error", func(t *testing.T) {
		sender := &mockWidgetSender{
			editFunc: func(m *discordgo.MessageEdit, options ...discordgo.RequestOption) (*discordgo.Message, error) {
				return nil, errors.New("network timeout")
			},
		}
		_, err := DeliverStatusWidget(sender, StatusWidgetRequest{
			ChannelID: "123",
			MessageID: "456",
		}, time.Now())
		if err == nil || !strings.Contains(err.Error(), "network timeout") {
			t.Fatalf("expected network timeout error, got %v", err)
		}
	})

	t.Run("send generic error", func(t *testing.T) {
		sender := &mockWidgetSender{
			sendFunc: func(channelID string, data *discordgo.MessageSend, options ...discordgo.RequestOption) (*discordgo.Message, error) {
				return nil, errors.New("permission denied")
			},
		}
		_, err := DeliverStatusWidget(sender, StatusWidgetRequest{
			ChannelID: "123",
		}, time.Now())
		if err == nil || !strings.Contains(err.Error(), "permission denied") {
			t.Fatalf("expected permission denied error, got %v", err)
		}
	})

	t.Run("edit 404 fallback send error", func(t *testing.T) {
		sender := &mockWidgetSender{
			editFunc: func(m *discordgo.MessageEdit, options ...discordgo.RequestOption) (*discordgo.Message, error) {
				return nil, &discordgo.RESTError{
					Response: &http.Response{StatusCode: http.StatusNotFound},
				}
			},
			sendFunc: func(channelID string, data *discordgo.MessageSend, options ...discordgo.RequestOption) (*discordgo.Message, error) {
				return nil, errors.New("send failed after 404")
			},
		}
		_, err := DeliverStatusWidget(sender, StatusWidgetRequest{
			ChannelID: "123",
			MessageID: "456",
		}, time.Now())
		if err == nil || !strings.Contains(err.Error(), "send failed after 404") {
			t.Fatalf("expected 'send failed after 404', got %v", err)
		}
	})
}

func TestFormatChunkDuration(t *testing.T) {
	tests := []struct {
		dur  time.Duration
		want string
	}{
		{-10 * time.Second, "0s"},
		{0, "0s"},
		{45 * time.Second, "45s"},
		{3 * time.Minute, "3m"},
		{4*time.Minute + 12*time.Second, "4m 12s"},
		{1 * time.Hour, "1h"},
		{2*time.Hour + 15*time.Minute + 20*time.Second, "2h 15m"},
	}

	for _, tt := range tests {
		if got := FormatChunkDuration(tt.dur); got != tt.want {
			t.Errorf("FormatChunkDuration(%v) = %q, want %q", tt.dur, got, tt.want)
		}
	}
}

func TestBuildStatusWidgetComponents_ChunkRuntimes(t *testing.T) {
	now := time.Unix(1760000500, 0)
	start := time.Unix(1760000000, 0)
	completedAt := start.Add(4*time.Minute + 12*time.Second)

	req := StatusWidgetRequest{
		ChannelID: "1542423172400291873",
		ID:        "build-runtimes",
		Tier:      3,
		Title:     "Add Chunk Runtime Tracking To Status Cards",
		Status:    "running",
		Chunks: []StatusWidgetChunk{
			{
				Index:       0,
				Repo:        "aerial",
				Title:       "Completed chunk with duration",
				Status:      "deployed",
				PRNumber:    748,
				StartedAt:   start,
				CompletedAt: completedAt,
				UpdatedAt:   completedAt.Add(10 * time.Minute),
			},
			{
				Index:     1,
				Repo:      "aerial",
				Title:     "Active chunk with relative timestamp",
				Status:    "running",
				StartedAt: start,
				UpdatedAt: start,
			},
			{
				Index:     2,
				Repo:      "aerial",
				Title:     "Pending chunk with no runtime",
				Status:    "pending",
				StartedAt: start,
				UpdatedAt: start,
			},
			{
				Index:     3,
				Repo:      "aerial",
				Title:     "Completed chunk missing UpdatedAt",
				Status:    "deployed",
				StartedAt: start,
			},
		},
	}

	comps, err := BuildStatusWidgetComponents(req, now)
	if err != nil {
		t.Fatalf("BuildStatusWidgetComponents failed: %v", err)
	}
	container := comps[0].(*discordgo.Container)

	c0Sec := container.Components[1].(*discordgo.Section)
	c0Text := c0Sec.Components[0].(*discordgo.TextDisplay)
	if c0Text.Content != "✅ **`aerial`**: Completed chunk with duration *(4m 12s)*" {
		t.Errorf("unexpected chunk 0 text: %q", c0Text.Content)
	}

	c1Text := container.Components[2].(*discordgo.TextDisplay)
	if c1Text.Content != "🔨 **`aerial`**: Active chunk with relative timestamp *(started <t:1760000000:R>)*" {
		t.Errorf("unexpected chunk 1 text: %q", c1Text.Content)
	}

	c2Text := container.Components[3].(*discordgo.TextDisplay)
	if c2Text.Content != "⏳ **`aerial`**: Pending chunk with no runtime" {
		t.Errorf("unexpected chunk 2 text: %q", c2Text.Content)
	}

	c3Text := container.Components[4].(*discordgo.TextDisplay)
	if c3Text.Content != "✅ **`aerial`**: Completed chunk missing UpdatedAt" {
		t.Errorf("unexpected chunk 3 text: %q", c3Text.Content)
	}
}
