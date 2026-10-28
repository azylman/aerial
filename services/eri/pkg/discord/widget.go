package discord

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/delivery"
	"github.com/bwmarrin/discordgo"
)

// StatusWidgetChunk represents a chunk in a Vanadis build or a PR task.
type StatusWidgetChunk struct {
	Index         int       `json:"index"`
	Repo          string    `json:"repo"`
	Title         string    `json:"title"`
	Status        string    `json:"status"`
	PRNumber      int       `json:"pr_number,omitempty"`
	DeployingJobs []string  `json:"deploying_jobs,omitempty"`
	StartedAt     time.Time `json:"started_at,omitempty"`
	UpdatedAt     time.Time `json:"updated_at,omitempty"`
}

// StatusWidgetRequest represents a request to deliver or update a Discord status tracker widget.
type StatusWidgetRequest struct {
	ChannelID string              `json:"channel_id"`
	MessageID string              `json:"message_id,omitempty"`
	ID        string              `json:"id"`
	Tier      int                 `json:"tier,omitempty"`
	Title     string              `json:"title"`
	Status    string              `json:"status"`
	Banner    string              `json:"banner,omitempty"`
	Chunks    []StatusWidgetChunk `json:"chunks"`
}

// WidgetSinkMessageThreshold is the number of newer messages after a status widget
// required before DeliverStatusWidget re-posts the widget at the bottom of the thread.
const WidgetSinkMessageThreshold = 4

// DiscordWidgetSender defines the interface required by DeliverStatusWidget to send, edit, query, and delete Discord messages.
type DiscordWidgetSender interface {
	ChannelMessageSendComplex(channelID string, data *discordgo.MessageSend, options ...discordgo.RequestOption) (*discordgo.Message, error)
	ChannelMessageEditComplex(m *discordgo.MessageEdit, options ...discordgo.RequestOption) (*discordgo.Message, error)
	ChannelMessages(channelID string, limit int, beforeID, afterID, aroundID string, options ...discordgo.RequestOption) ([]*discordgo.Message, error)
	ChannelMessageDelete(channelID, messageID string, options ...discordgo.RequestOption) error
}

func shouldSinkWidget(s DiscordWidgetSender, channelID, messageID string) bool {
	msgs, err := s.ChannelMessages(channelID, WidgetSinkMessageThreshold, "", messageID, "")
	if err != nil || len(msgs) < WidgetSinkMessageThreshold {
		return false
	}
	for _, m := range msgs {
		if m != nil && m.ID != messageID && m.Flags&discordgo.MessageFlagsIsComponentsV2 != 0 {
			return false
		}
	}
	return true
}

// AccentColor returns the hex color integer for the given status string.
func AccentColor(status string) int {
	norm := strings.ToLower(strings.TrimSpace(status))
	switch norm {
	case "completed", "merged", "deployed", "success", "green":
		return 0x10B981 // emerald
	case "failed", "conflict", "closed", "error", "cancelled_error":
		return 0xEF4444 // crimson
	case "paused", "stalled", "cancelled":
		return 0xF59E0B // amber
	case "running", "coding", "deploying", "open", "pr_opened", "pending":
		fallthrough
	default:
		return 0xA855F7 // purple
	}
}

func statusBadge(status string) string {
	norm := strings.ToLower(strings.TrimSpace(status))
	switch norm {
	case "running", "coding":
		return "🔨"
	case "pr_opened", "open":
		return "👀"
	case "deploying":
		return "🚀"
	case "merged", "deployed", "completed", "success":
		return "✅"
	case "failed", "conflict", "error":
		return "❌"
	case "pending":
		fallthrough
	default:
		return "⏳"
	}
}

// NormalizeRepo returns the full repository path (e.g. "azylman/aerial") and short name ("aerial").
func NormalizeRepo(repo string) (fullRepo string, shortRepo string) {
	trimmed := strings.TrimSpace(repo)
	if trimmed == "" {
		return "", ""
	}
	parts := strings.Split(trimmed, "/")
	if len(parts) == 1 {
		return "azylman/" + parts[0], parts[0]
	}
	return trimmed, parts[len(parts)-1]
}

// FormatChunkDuration formats a chunk execution duration into a concise human-readable string.
func FormatChunkDuration(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	totalSecs := int(d.Round(time.Second).Seconds())
	if totalSecs < 60 {
		return fmt.Sprintf("%ds", totalSecs)
	}
	if totalSecs < 3600 {
		mins := totalSecs / 60
		secs := totalSecs % 60
		if secs == 0 {
			return fmt.Sprintf("%dm", mins)
		}
		return fmt.Sprintf("%dm %ds", mins, secs)
	}
	hours := totalSecs / 3600
	mins := (totalSecs % 3600) / 60
	if mins == 0 {
		return fmt.Sprintf("%dh", hours)
	}
	return fmt.Sprintf("%dh %dm", hours, mins)
}

func formatChunkRuntimeSuffix(c StatusWidgetChunk) string {
	if c.StartedAt.IsZero() {
		return ""
	}
	norm := strings.ToLower(strings.TrimSpace(c.Status))
	switch norm {
	case "running", "coding", "pr_opened", "open", "deploying":
		return fmt.Sprintf(" *(started <t:%d:R>)*", c.StartedAt.Unix())
	case "merged", "deployed", "completed", "success", "failed", "conflict", "closed", "error", "cancelled":
		if c.UpdatedAt.IsZero() {
			return ""
		}
		return fmt.Sprintf(" *(%s)*", FormatChunkDuration(c.UpdatedAt.Sub(c.StartedAt)))
	default:
		return ""
	}
}

func buildWidgetHeaderText(req StatusWidgetRequest) string {
	isBuild := strings.HasPrefix(req.ID, "build-") || len(req.Chunks) > 1 || req.Tier > 0
	if isBuild {
		if trimmedTitle := strings.TrimSpace(req.Title); trimmedTitle != "" {
			return fmt.Sprintf("### 🛠️ %s", trimmedTitle)
		}
		return "### 🛠️ Vanadis Build"
	}

	prNum := 0
	shortRepo := ""
	if len(req.Chunks) == 1 {
		prNum = req.Chunks[0].PRNumber
		_, shortRepo = NormalizeRepo(req.Chunks[0].Repo)
	}
	repoTag := shortRepo
	if repoTag == "" {
		repoTag = "aerial"
	}
	if prNum > 0 {
		return fmt.Sprintf("### 🛠️ Pull Request #%d · `%s`\n%s", prNum, repoTag, strings.TrimSpace(req.Title))
	}
	return fmt.Sprintf("### 🛠️ Pull Request · `%s`\n%s", repoTag, strings.TrimSpace(req.Title))
}

func collapseCompletedWidgetChunks(chunks []StatusWidgetChunk) ([]StatusWidgetChunk, string) {
	if len(chunks) <= 10 {
		return chunks, ""
	}

	var activeChunks []StatusWidgetChunk
	var collapsedMerged []StatusWidgetChunk

	for i, c := range chunks {
		norm := strings.ToLower(strings.TrimSpace(c.Status))
		isCompleted := (norm == "merged" || norm == "deployed" || norm == "completed" || norm == "success")
		if isCompleted && i < len(chunks)-5 {
			collapsedMerged = append(collapsedMerged, c)
		} else {
			activeChunks = append(activeChunks, c)
		}
	}

	if len(collapsedMerged) > 0 {
		summaryText := fmt.Sprintf("✅ *%d earlier chunks completed and merged*", len(collapsedMerged))
		return activeChunks, summaryText
	}
	return chunks, ""
}

func buildWidgetChunkComponent(c StatusWidgetChunk) discordgo.MessageComponent {
	badge := statusBadge(c.Status)
	fullRepo, shortRepo := NormalizeRepo(c.Repo)
	repoTag := shortRepo
	if repoTag == "" {
		repoTag = "aerial"
	}

	rowText := fmt.Sprintf("%s **`%s`**: %s%s", badge, repoTag, strings.TrimSpace(c.Title), formatChunkRuntimeSuffix(c))

	if c.PRNumber > 0 && fullRepo != "" {
		prURL := fmt.Sprintf("https://github.com/%s/pull/%d", fullRepo, c.PRNumber)
		return &discordgo.Section{
			Components: []discordgo.MessageComponent{
				&discordgo.TextDisplay{
					Content: rowText,
				},
			},
			Accessory: &discordgo.Button{
				Style: discordgo.LinkButton,
				Label: fmt.Sprintf("PR #%d ↗", c.PRNumber),
				URL:   prURL,
			},
		}
	}
	return &discordgo.TextDisplay{
		Content: rowText,
	}
}

// BuildStatusWidgetComponents constructs a Components V2 Container message component.
func BuildStatusWidgetComponents(req StatusWidgetRequest, now time.Time) ([]discordgo.MessageComponent, error) {
	color := AccentColor(req.Status)
	var containerComponents []discordgo.MessageComponent

	// Header TextDisplay
	headerText := buildWidgetHeaderText(req)
	containerComponents = append(containerComponents, &discordgo.TextDisplay{
		Content: strings.TrimSpace(headerText),
	})

	// Optional Banner
	if strings.TrimSpace(req.Banner) != "" {
		containerComponents = append(containerComponents, &discordgo.TextDisplay{
			Content: strings.TrimSpace(req.Banner),
		})
	}

	// 40-Component Ceiling Guard
	// If len(req.Chunks) > 10, collapse older completed/merged chunks into a single summary TextDisplay.
	chunks, summaryText := collapseCompletedWidgetChunks(req.Chunks)
	if summaryText != "" {
		containerComponents = append(containerComponents, &discordgo.TextDisplay{
			Content: summaryText,
		})
	}

	// Chunk rows
	for _, c := range chunks {
		containerComponents = append(containerComponents, buildWidgetChunkComponent(c))
	}

	// Footer TextDisplay
	if now.IsZero() {
		now = time.Now()
	}
	footerText := fmt.Sprintf("*Updated <t:%d:R>*", now.Unix())
	containerComponents = append(containerComponents, &discordgo.TextDisplay{
		Content: footerText,
	})

	container := &discordgo.Container{
		AccentColor: &color,
		Components:  containerComponents,
	}

	return []discordgo.MessageComponent{container}, nil
}

// DeliverStatusWidget delivers or updates a status widget via Discord Components V2.
func DeliverStatusWidget(s DiscordWidgetSender, req StatusWidgetRequest, now time.Time) (string, error) {
	if s == nil {
		return "", errors.New("discord widget sender is nil")
	}
	if strings.TrimSpace(req.ChannelID) == "" {
		return "", errors.New("channel_id is required")
	}

	components, err := BuildStatusWidgetComponents(req, now)
	if err != nil {
		return "", fmt.Errorf("failed to build status widget components: %w", err)
	}

	trimmedMsgID := strings.TrimSpace(req.MessageID)
	if trimmedMsgID != "" {
		if shouldSinkWidget(s, req.ChannelID, trimmedMsgID) {
			send := &discordgo.MessageSend{
				Flags:      discordgo.MessageFlagsIsComponentsV2 | discordgo.MessageFlagsSuppressNotifications,
				Components: components,
			}
			if created, sendErr := s.ChannelMessageSendComplex(req.ChannelID, send); sendErr == nil && created != nil {
				if delErr := s.ChannelMessageDelete(req.ChannelID, trimmedMsgID); delErr != nil && !delivery.IsMessageNotFoundError(delErr) {
					log.Printf("[Widget] Warning: failed to delete old status widget message %s: %v", trimmedMsgID, delErr)
				}
				return created.ID, nil
			}
		}

		edit := &discordgo.MessageEdit{
			Channel:    req.ChannelID,
			ID:         trimmedMsgID,
			Content:    nil,
			Embeds:     nil,
			Flags:      discordgo.MessageFlagsIsComponentsV2,
			Components: &components,
		}
		msg, editErr := s.ChannelMessageEditComplex(edit)
		if editErr == nil && msg != nil {
			return msg.ID, nil
		}

		// Check if error is 404 / Unknown Message (code 10008)
		if delivery.IsMessageNotFoundError(editErr) {
			// Fall back to sending a new message
			send := &discordgo.MessageSend{
				Flags:      discordgo.MessageFlagsIsComponentsV2,
				Components: components,
			}
			created, sendErr := s.ChannelMessageSendComplex(req.ChannelID, send)
			if sendErr != nil {
				return "", fmt.Errorf("failed to send new status widget message after 404 edit: %w", sendErr)
			}
			return created.ID, nil
		}

		return "", fmt.Errorf("failed to edit status widget message %s: %w", trimmedMsgID, editErr)
	}

	// New message
	send := &discordgo.MessageSend{
		Flags:      discordgo.MessageFlagsIsComponentsV2,
		Components: components,
	}
	created, sendErr := s.ChannelMessageSendComplex(req.ChannelID, send)
	if sendErr != nil {
		return "", fmt.Errorf("failed to send status widget message: %w", sendErr)
	}
	return created.ID, nil
}
