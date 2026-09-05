package delivery

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/azylman/aerial/brain/pkg/metrics"
	"github.com/bwmarrin/discordgo"
)

const MaxDiscordMessageLength = 2000

// SplitMessage splits text into chunks that do not exceed the specified limit (defaults to 2000).
// It is Markdown-aware: if a chunk cuts inside an open code block fence (```<lang>), it closes the
// fence (```) at the end of the chunk and reopens it (```<lang>) at the start of the next chunk.
func SplitMessage(text string, limit int) []string {
	if limit <= 0 {
		limit = MaxDiscordMessageLength
	}
	text = strings.TrimSpace(text)
	if text == "" {
		return nil
	}

	runes := []rune(text)
	if len(runes) <= limit {
		return []string{text}
	}

	var chunks []string
	var current strings.Builder
	var currentRunes int
	var inCodeBlock bool
	var currentLang string

	flushCurrent := func() {
		if current.Len() == 0 {
			return
		}
		if inCodeBlock {
			current.WriteString("\n```")
		}
		chunkStr := strings.TrimRight(current.String(), "\r\n")
		if strings.TrimSpace(chunkStr) != "" {
			chunks = append(chunks, chunkStr)
		}
		current.Reset()
		currentRunes = 0
	}

	startNextChunk := func() {
		if inCodeBlock {
			openFence := "```" + currentLang + "\n"
			current.WriteString(openFence)
			currentRunes = len([]rune(openFence))
		}
	}

	lines := strings.Split(text, "\n")
	for i, line := range lines {
		trimmedLine := strings.TrimSpace(line)
		isFence := strings.HasPrefix(trimmedLine, "```")

		// If this line toggles a code fence
		var willCloseFence bool
		if isFence {
			if !inCodeBlock {
				inCodeBlock = true
				currentLang = strings.TrimSpace(strings.TrimPrefix(trimmedLine, "```"))
			} else {
				willCloseFence = true
			}
		}

		lineRunes := []rune(line)
		lineLen := len(lineRunes)

		// Determine separator if current has content
		sep := ""
		sepLen := 0
		if current.Len() > 0 {
			sep = "\n"
			sepLen = 1
		}

		// Extra safety margin for closing fence if inside code block (and not currently closing it)
		closingMargin := 0
		if inCodeBlock && !willCloseFence {
			closingMargin = len([]rune("\n```"))
		}

		// If adding this line fits within limit
		if currentRunes+sepLen+lineLen+closingMargin <= limit {
			if sepLen > 0 {
				current.WriteString(sep)
				currentRunes += sepLen
			}
			current.WriteString(line)
			currentRunes += lineLen
			if willCloseFence {
				inCodeBlock = false
				currentLang = ""
			}
			continue
		}

		// Line doesn't fit in current chunk
		if current.Len() > 0 {
			flushCurrent()
			startNextChunk()
		}

		// Recompute margin in fresh chunk
		closingMargin = 0
		if inCodeBlock && !willCloseFence {
			closingMargin = len([]rune("\n```"))
		}

		// If line fits in fresh chunk
		if currentRunes+lineLen+closingMargin <= limit {
			current.WriteString(line)
			currentRunes += lineLen
			if willCloseFence {
				inCodeBlock = false
				currentLang = ""
			}
			continue
		}

		// Line is larger than an entire chunk limit: slice line across chunks
		remaining := lineRunes
		for len(remaining) > 0 {
			avail := limit - currentRunes - closingMargin
			if avail <= 0 {
				flushCurrent()
				startNextChunk()
				closingMargin = 0
				if inCodeBlock && !willCloseFence {
					closingMargin = len([]rune("\n```"))
				}
				avail = limit - currentRunes - closingMargin
			}

			take := len(remaining)
			if take > avail {
				take = avail
			}
			part := string(remaining[:take])
			current.WriteString(part)
			currentRunes += len([]rune(part))
			remaining = remaining[take:]

			if len(remaining) > 0 {
				flushCurrent()
				startNextChunk()
				closingMargin = 0
				if inCodeBlock && !willCloseFence {
					closingMargin = len([]rune("\n```"))
				}
			}
		}

		if willCloseFence {
			inCodeBlock = false
			currentLang = ""
		}
		_ = i
	}

	if current.Len() > 0 {
		flushCurrent()
	}

	return chunks
}

// SendMessage delivers text to the target Discord channel/thread, chunking at the 2000-character limit.
func SendMessage(s *discordgo.Session, channelID, text string) error {
	return SendMessageWithAttachments(s, channelID, text, nil)
}

// SendMessageWithAttachments delivers text to the target Discord channel/thread, chunking at 2000 characters,
// and binding any provided file attachments strictly to the final (last) message chunk.
func SendMessageWithAttachments(s *discordgo.Session, channelID, text string, attachments []*Attachment) (err error) {
	start := time.Now()
	defer func() {
		status := "success"
		if err != nil {
			status = "error"
		} else if strings.TrimSpace(text) == "" && len(attachments) == 0 {
			status = "empty"
		}
		metrics.RecordDelivery(status, time.Since(start))
	}()

	if s == nil {
		return fmt.Errorf("discord session is nil")
	}
	if channelID == "" {
		return fmt.Errorf("channelID cannot be empty")
	}

	chunks := SplitMessage(text, MaxDiscordMessageLength)
	if len(chunks) == 0 && len(attachments) == 0 {
		return nil
	}
	if len(chunks) > 1 {
		metrics.RecordMessageChunked()
	}

	var discordFiles []*discordgo.File
	for _, att := range attachments {
		if att != nil && len(att.Data) > 0 {
			discordFiles = append(discordFiles, att.ToDiscordFile())
		}
	}

	if len(chunks) == 0 && len(discordFiles) > 0 {
		msg := &discordgo.MessageSend{
			Files: discordFiles,
		}
		if _, sendErr := s.ChannelMessageSendComplex(channelID, msg); sendErr != nil {
			return fmt.Errorf("failed to send attachments: %w", sendErr)
		}
		return nil
	}

	for i, chunk := range chunks {
		if strings.TrimSpace(chunk) == "" && (i != len(chunks)-1 || len(discordFiles) == 0) {
			continue
		}
		msg := &discordgo.MessageSend{
			Content: chunk,
		}
		if i == len(chunks)-1 && len(discordFiles) > 0 {
			msg.Files = discordFiles
		}

		if _, sendErr := s.ChannelMessageSendComplex(channelID, msg); sendErr != nil {
			return fmt.Errorf("failed to send message chunk %d: %w", i, sendErr)
		}
	}
	return nil
}

// StartTyping sends a Discord typing indicator immediately and keeps it active every 8 seconds until stopped.
func StartTyping(s *discordgo.Session, channelID string) (stop func()) {
	if s == nil || channelID == "" {
		return func() {}
	}

	metrics.DiscordTypingSessionsActive.Inc()
	_ = s.ChannelTyping(channelID)
	stopChan := make(chan struct{})
	var once sync.Once

	go func() {
		ticker := time.NewTicker(8 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				_ = s.ChannelTyping(channelID)
			case <-stopChan:
				return
			}
		}
	}()

	return func() {
		once.Do(func() {
			close(stopChan)
			metrics.DiscordTypingSessionsActive.Dec()
		})
	}
}

func isSnowflake(str string) bool {
	if str == "" {
		return false
	}
	for _, r := range str {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// ResolveChannelByNameOrID resolves a channel name (e.g. "aerial-dev" or "#aerial-dev") or snowflake ID to a channel ID.
func ResolveChannelByNameOrID(s *discordgo.Session, nameOrID string) (string, error) {
	if s == nil {
		return "", fmt.Errorf("discord session is nil")
	}
	trimmed := strings.TrimSpace(nameOrID)
	trimmed = strings.TrimPrefix(trimmed, "#")
	if trimmed == "" {
		return "", fmt.Errorf("channel name or ID cannot be empty")
	}

	if isSnowflake(trimmed) {
		return trimmed, nil
	}

	// 1. Check in-memory session State if available
	if s.State != nil {
		s.State.RLock()
		for _, g := range s.State.Guilds {
			for _, ch := range g.Channels {
				if strings.EqualFold(ch.Name, trimmed) {
					s.State.RUnlock()
					return ch.ID, nil
				}
			}
		}
		s.State.RUnlock()
	}

	// 2. Query guilds and guild channels via Discord API fallback if token and client are initialized
	if s.Ratelimiter != nil && s.Token != "" {
		userGuilds, err := s.UserGuilds(100, "", "", false)
		if err == nil {
			for _, g := range userGuilds {
				channels, err := s.GuildChannels(g.ID)
				if err != nil {
					continue
				}
				for _, ch := range channels {
					if strings.EqualFold(ch.Name, trimmed) {
						return ch.ID, nil
					}
				}
			}
		}
	}

	return "", fmt.Errorf("channel %q not found in any connected guild", nameOrID)
}

// SendSystemAlert resolves the target channel by name or ID and delivers an alert message formatted with markdown warnings.
func SendSystemAlert(s *discordgo.Session, channelNameOrID, title, alertBody string) error {
	if s == nil {
		return fmt.Errorf("discord session is nil")
	}
	resolvedID, err := ResolveChannelByNameOrID(s, channelNameOrID)
	if err != nil {
		return fmt.Errorf("failed to resolve system alert channel %q: %w", channelNameOrID, err)
	}

	formatted := fmt.Sprintf("⚠️ **Aerial System Alert: %s**\n\n%s", strings.TrimSpace(title), strings.TrimSpace(alertBody))
	return SendMessage(s, resolvedID, formatted)
}
