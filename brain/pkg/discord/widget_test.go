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
	sendFunc func(channelID string, data *discordgo.MessageSend, options ...discordgo.RequestOption) (*discordgo.Message, error)
	editFunc func(m *discordgo.MessageEdit, options ...discordgo.RequestOption) (*discordgo.Message, error)
}

func (m *mockWidgetSender) ChannelMessageSendComplex(channelID string, data *discordgo.MessageSend, options ...discordgo.RequestOption) (*discordgo.Message, error) {
	if m.sendFunc != nil {
		return m.sendFunc(channelID, data, options...)
	}
	return &discordgo.Message{ID: "msg-default-send"}, nil
}

func (m *mockWidgetSender) ChannelMessageEditComplex(medit *discordgo.MessageEdit, options ...discordgo.RequestOption) (*discordgo.Message, error) {
	if m.editFunc != nil {
		return m.editFunc(medit, options...)
	}
	return &discordgo.Message{ID: medit.ID}, nil
}

func TestAccentColor(t *testing.T) {
	tests := []struct {
		status string
		want   int
	}{
		{"completed", 0x10B981},
		{"merged", 0x10B981},
		{"deployed", 0x10B981},
		{"failed", 0xEF4444},
		{"conflict", 0xEF4444},
		{"closed", 0xEF4444},
		{"paused", 0xF59E0B},
		{"stalled", 0xF59E0B},
		{"cancelled", 0xF59E0B},
		{"running", 0xA855F7},
		{"coding", 0xA855F7},
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
		{"deploying", "🚀"},
		{"merged", "✅"},
		{"deployed", "✅"},
		{"completed", "✅"},
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
				DiffStat: "+409 -116",
			},
			{
				Index:         2,
				Repo:          "aerial",
				Title:         "Silent prompt",
				Status:        "deploying",
				PRNumber:      736,
				DeployingJobs: []string{"aerial-brain", "webhooks-router"},
			},
			{
				Index:  3,
				Repo:   "aerial",
				Title:  "Stateless widget endpoint",
				Status: "coding",
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
	if !ok || !strings.Contains(header.Content, "Vanadis Build `build-1792124461-490d903b` · Tier 3") {
		t.Errorf("unexpected header: %+v", header)
	}

	banner, ok := container.Components[1].(*discordgo.TextDisplay)
	if !ok || banner.Content != "⚠️ Build in progress" {
		t.Errorf("unexpected banner: %+v", banner)
	}

	// Chunk 1 has PRNumber > 0 and Repo, so it's a Section with Button
	c1Sec, ok := container.Components[2].(*discordgo.Section)
	if !ok {
		t.Fatalf("expected chunk 1 to be *discordgo.Section, got %T", container.Components[2])
	}
	btn, ok := c1Sec.Accessory.(*discordgo.Button)
	if !ok || btn.URL != "https://github.com/azylman/aerial/pull/735" || btn.Label != "PR #735 ↗" {
		t.Errorf("unexpected button accessory: %+v", c1Sec.Accessory)
	}

	// Chunk 2 has DeployingJobs
	c2Sec, ok := container.Components[3].(*discordgo.Section)
	if !ok {
		t.Fatalf("expected chunk 2 to be *discordgo.Section, got %T", container.Components[3])
	}
	c2Text := c2Sec.Components[0].(*discordgo.TextDisplay)
	if !strings.Contains(c2Text.Content, "Deploying: `aerial-brain`, `webhooks-router`") {
		t.Errorf("expected deploying jobs in chunk 2, got: %s", c2Text.Content)
	}

	// Chunk 3 has no PRNumber, so it's a TextDisplay
	c3Text, ok := container.Components[4].(*discordgo.TextDisplay)
	if !ok || !strings.Contains(c3Text.Content, "Stateless widget endpoint") {
		t.Errorf("unexpected chunk 3 component: %+v", container.Components[4])
	}

	// Footer
	footer, ok := container.Components[5].(*discordgo.TextDisplay)
	if !ok || footer.Content != "*Updated <t:1760000000:R>*" {
		t.Errorf("unexpected footer: %+v", footer)
	}
}

func TestBuildStatusWidgetComponents_StandalonePR(t *testing.T) {
	req := StatusWidgetRequest{
		ChannelID: "1542423172400291873",
		Title:     "Fix database connection pool",
		Status:    "merged",
		Chunks: []StatusWidgetChunk{
			{
				Index:    1,
				Repo:     "azylman/aerial",
				Title:    "Fix connection leak",
				Status:   "merged",
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
				Response: &http.Response{StatusCode: 404},
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
					Response: &http.Response{StatusCode: 404},
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

func TestIsMessageNotFound(t *testing.T) {
	if isMessageNotFound(nil) {
		t.Error("expected false for nil error")
	}
	if !isMessageNotFound(errors.New("Error 10008: Unknown Message")) {
		t.Error("expected true for 10008 string")
	}
	if !isMessageNotFound(errors.New("404 not found")) {
		t.Error("expected true for 404 string")
	}
	if isMessageNotFound(errors.New("internal server error")) {
		t.Error("expected false for 500 error")
	}
	if !isMessageNotFound(&discordgo.RESTError{
		Message: &discordgo.APIErrorMessage{Code: 10008},
	}) {
		t.Error("expected true for RESTError code 10008")
	}
}

