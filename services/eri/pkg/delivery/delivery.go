package delivery

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/metrics"
	"github.com/bwmarrin/discordgo"
)

const MaxDiscordMessageLength = 2000

type messageSplitState struct {
	chunks       []string
	current      strings.Builder
	currentRunes int
	inCodeBlock  bool
	currentLang  string
	limit        int
}

func (st *messageSplitState) flush() {
	if st.current.Len() == 0 {
		return
	}
	if st.inCodeBlock {
		st.current.WriteString("\n```")
	}
	chunkStr := dedentLeadingListChunk(strings.TrimRight(st.current.String(), "\r\n"))
	if strings.TrimSpace(chunkStr) != "" {
		st.chunks = append(st.chunks, chunkStr)
	}
	st.current.Reset()
	st.currentRunes = 0
}

func leadingIndent(s string) int {
	return len(s) - len(strings.TrimLeft(s, " \t"))
}

func isListItem(trimmed string) bool {
	for _, p := range []string{"- ", "* ", "+ ", "• "} {
		if strings.HasPrefix(trimmed, p) {
			return true
		}
	}
	rest := strings.TrimLeft(trimmed, "0123456789")
	return len(rest) < len(trimmed) && (strings.HasPrefix(rest, ". ") || strings.HasPrefix(rest, ") "))
}

func childListFitMargin(lines []string, i, lineLen, margin, limit int) int {
	baseIndent := leadingIndent(lines[i])
	if i > 0 && strings.TrimSpace(lines[i-1]) != "" && baseIndent > leadingIndent(lines[i-1]) {
		return 0
	}
	if i+1 >= len(lines) || leadingIndent(lines[i+1]) <= baseIndent || !isListItem(strings.TrimSpace(lines[i+1])) {
		return 0
	}
	firstChild, total := 1+len([]rune(lines[i+1])), 0
	for j := i + 1; j < len(lines); j++ {
		if t := strings.TrimSpace(lines[j]); t == "" || leadingIndent(lines[j]) <= baseIndent || strings.HasPrefix(t, "```") {
			break
		}
		total += 1 + len([]rune(lines[j]))
	}
	if lineLen+total+margin <= limit {
		return total
	}
	if lineLen+firstChild+margin <= limit {
		return firstChild
	}
	return 0
}

func dedentLeadingListChunk(chunk string) string {
	lines := strings.Split(chunk, "\n")
	start := 0
	for start < len(lines) && strings.TrimSpace(lines[start]) == "" {
		start++
	}
	if start >= len(lines) {
		return chunk
	}
	dedent := leadingIndent(lines[start])
	if dedent == 0 || !isListItem(strings.TrimSpace(lines[start])) {
		return chunk
	}
	for i := start; i < len(lines); i++ {
		line := lines[i]
		t := strings.TrimSpace(line)
		if t == "" {
			continue
		}
		indent := leadingIndent(line)
		if indent == 0 || strings.HasPrefix(t, "```") {
			break
		}
		if indent < dedent {
			dedent = indent
		}
		lines[i] = line[dedent:]
	}
	return strings.Join(lines[start:], "\n")
}

func (st *messageSplitState) startNextChunk() {
	if st.inCodeBlock {
		openFence := "```" + st.currentLang + "\n"
		st.current.WriteString(openFence)
		st.currentRunes = len([]rune(openFence))
	}
}

func (st *messageSplitState) closingMargin(willCloseFence bool) int {
	if st.inCodeBlock && !willCloseFence {
		return len([]rune("\n```"))
	}
	return 0
}

func (st *messageSplitState) splitOversizedLine(lineRunes []rune, willCloseFence bool) {
	remaining := lineRunes
	for len(remaining) > 0 {
		avail := st.limit - st.currentRunes - st.closingMargin(willCloseFence)
		if avail <= 0 {
			st.flush()
			st.startNextChunk()
			avail = st.limit - st.currentRunes - st.closingMargin(willCloseFence)
		}

		take := len(remaining)
		if take > avail {
			take = avail
		}
		part := string(remaining[:take])
		st.current.WriteString(part)
		st.currentRunes += len([]rune(part))
		remaining = remaining[take:]

		if len(remaining) > 0 {
			st.flush()
			st.startNextChunk()
		}
	}
}

func inspectFenceLine(trimmedLine string, inCodeBlock bool, limit int) (willOpen, willClose bool, lang string) {
	isFence := limit > 8 && strings.HasPrefix(trimmedLine, "```") && !(strings.HasSuffix(trimmedLine, "```") && len(trimmedLine) >= 6)
	if !isFence {
		return false, false, ""
	}
	if !inCodeBlock {
		willOpen = true
		lang = strings.TrimSpace(strings.TrimPrefix(trimmedLine, "```"))
		if strings.ContainsAny(lang, " \t`") || len([]rune("```"+lang+"\n"))+4 >= limit {
			lang = ""
		}
		return willOpen, false, lang
	}
	return false, true, ""
}

func (st *messageSplitState) tryFitLine(line string, lineLen, sepLen, fitMargin int, willOpenFence, willCloseFence bool, fenceLang string) bool {
	if st.currentRunes+sepLen+lineLen+fitMargin > st.limit {
		return false
	}
	if sepLen > 0 {
		st.current.WriteString("\n")
		st.currentRunes += sepLen
	}
	st.current.WriteString(line)
	st.currentRunes += lineLen
	if willOpenFence {
		st.inCodeBlock = true
		st.currentLang = fenceLang
	} else if willCloseFence {
		st.inCodeBlock = false
		st.currentLang = ""
	}
	return true
}

func (st *messageSplitState) addLineToChunk(line string, lineRunes []rune, willCloseFence bool) {
	margin := st.closingMargin(willCloseFence)
	if st.currentRunes+len(lineRunes)+margin <= st.limit {
		st.current.WriteString(line)
		st.currentRunes += len(lineRunes)
		return
	}
	st.splitOversizedLine(lineRunes, willCloseFence)
}

func (st *messageSplitState) processLine(i int, line string, lines []string) {
	trimmedLine := strings.TrimSpace(line)
	willOpenFence, willCloseFence, fenceLang := inspectFenceLine(trimmedLine, st.inCodeBlock, st.limit)

	lineRunes := []rune(line)
	lineLen := len(lineRunes)

	sepLen := 0
	if st.current.Len() > 0 {
		sepLen = 1
	}

	margin := 0
	if (st.inCodeBlock && !willCloseFence) || willOpenFence {
		margin = len([]rune("\n```"))
	}
	fitMargin := margin
	if willOpenFence && st.current.Len() > 0 && i+1 < len(lines) {
		fitMargin += 1 + len([]rune(lines[i+1]))
	} else if !st.inCodeBlock && !willCloseFence && st.current.Len() > 0 && trimmedLine != "" {
		fitMargin += childListFitMargin(lines, i, lineLen, margin, st.limit)
	}

	if st.tryFitLine(line, lineLen, sepLen, fitMargin, willOpenFence, willCloseFence, fenceLang) {
		return
	}

	if willCloseFence {
		st.flush()
		st.inCodeBlock = false
		st.currentLang = ""
		return
	}

	if st.current.Len() > 0 {
		st.flush()
		st.startNextChunk()
	}
	if willOpenFence {
		st.inCodeBlock = true
		st.currentLang = fenceLang
	}

	st.addLineToChunk(line, lineRunes, willCloseFence)
}

// SplitMessage splits text into chunks that do not exceed the specified limit (defaults to 2000).
// It is Markdown-aware: if a chunk cuts inside an open code block fence (```<lang>), it closes the
// fence (```) at the end of the chunk and reopens it (```<lang>) at the start of the next chunk.
func SplitMessage(text string, limit int) []string {
	if limit <= 0 {
		limit = MaxDiscordMessageLength
	}
	text = strings.TrimSpace(dedentLeadingListChunk(strings.Trim(text, "\r\n")))
	if text == "" {
		return nil
	}

	runes := []rune(text)
	if len(runes) <= limit {
		return []string{text}
	}

	st := &messageSplitState{limit: limit}
	lines := strings.Split(text, "\n")
	for i, line := range lines {
		st.processLine(i, line, lines)
	}

	if st.current.Len() > 0 {
		st.flush()
	}

	return st.chunks
}

// SendMessage delivers text to the target Discord channel/thread, chunking at the 2000-character limit.
func SendMessage(s *discordgo.Session, channelID, text string) error {
	return SendMessageWithAttachments(s, channelID, text, nil)
}

func collectDiscordFiles(attachments []*Attachment) []*discordgo.File {
	var discordFiles []*discordgo.File
	for _, att := range attachments {
		if att != nil && len(att.Data) > 0 {
			discordFiles = append(discordFiles, att.ToDiscordFile())
		}
	}
	return discordFiles
}

func sendChunkedDiscordMessages(s *discordgo.Session, channelID string, chunks []string, discordFiles []*discordgo.File) error {
	if len(chunks) == 0 && len(discordFiles) > 0 {
		msg := &discordgo.MessageSend{
			Files: discordFiles,
			Flags: discordgo.MessageFlagsSuppressEmbeds,
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
			Flags:   discordgo.MessageFlagsSuppressEmbeds,
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

	chunks := SplitMessage(FormatDiscordLatex(text), MaxDiscordMessageLength)
	if len(chunks) == 0 && len(attachments) == 0 {
		return nil
	}
	if len(chunks) > 1 {
		metrics.RecordMessageChunked()
	}

	discordFiles := collectDiscordFiles(attachments)
	return sendChunkedDiscordMessages(s, channelID, chunks, discordFiles)
}

// StartTyping sends a Discord typing indicator immediately and keeps it active every 8 seconds until stopped.
func StartTyping(s *discordgo.Session, channelID string) (stop func()) {
	if s == nil || channelID == "" {
		return func() {}
	}

	metrics.DiscordTypingSessionsActive.Inc()
	if err := s.ChannelTyping(channelID); err != nil {
		log.Printf("[Delivery] Warning: failed to send typing indicator to channel %s: %v", channelID, err)
	}
	stopChan := make(chan struct{})
	var once sync.Once

	go func() {
		ticker := time.NewTicker(8 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := s.ChannelTyping(channelID); err != nil {
					log.Printf("[Delivery] Warning: failed to send typing indicator to channel %s: %v", channelID, err)
				}
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

func findChannelInState(s *discordgo.Session, trimmed string) (string, bool) {
	if s.State == nil {
		return "", false
	}
	s.State.RLock()
	defer s.State.RUnlock()
	for _, g := range s.State.Guilds {
		for _, ch := range g.Channels {
			if strings.EqualFold(ch.Name, trimmed) {
				return ch.ID, true
			}
		}
	}
	return "", false
}

func findChannelViaREST(s *discordgo.Session, trimmed string) (string, bool) {
	if s.Ratelimiter == nil || s.Token == "" {
		return "", false
	}
	userGuilds, err := s.UserGuilds(100, "", "", false)
	if err != nil {
		return "", false
	}
	for _, g := range userGuilds {
		channels, err := s.GuildChannels(g.ID)
		if err != nil {
			continue
		}
		for _, ch := range channels {
			if strings.EqualFold(ch.Name, trimmed) {
				return ch.ID, true
			}
		}
	}
	return "", false
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

	if id, ok := findChannelInState(s, trimmed); ok {
		return id, nil
	}

	if id, ok := findChannelViaREST(s, trimmed); ok {
		return id, nil
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

// EditMessage edits an existing message in the specified channel or thread.
func EditMessage(s *discordgo.Session, channelID, messageID, text string) error {
	if s == nil {
		return fmt.Errorf("discord session is nil")
	}
	if channelID == "" {
		return fmt.Errorf("channelID cannot be empty")
	}
	if messageID == "" {
		return fmt.Errorf("messageID cannot be empty")
	}
	trimmed := strings.TrimSpace(FormatDiscordLatex(text))
	if trimmed == "" {
		return fmt.Errorf("text cannot be empty")
	}
	if len([]rune(trimmed)) > MaxDiscordMessageLength {
		trimmed = string([]rune(trimmed)[:MaxDiscordMessageLength])
	}

	edit := discordgo.NewMessageEdit(channelID, messageID)
	edit.SetContent(trimmed)
	edit.Flags = discordgo.MessageFlagsSuppressEmbeds
	_, err := s.ChannelMessageEditComplex(edit)
	return err
}

// DeleteMessage deletes an existing message in the specified channel or thread.
func DeleteMessage(s *discordgo.Session, channelID, messageID string) error {
	if s == nil {
		return fmt.Errorf("discord session is nil")
	}
	if channelID == "" {
		return fmt.Errorf("channelID cannot be empty")
	}
	if messageID == "" {
		return fmt.Errorf("messageID cannot be empty")
	}

	return s.ChannelMessageDelete(channelID, messageID)
}

// IsMessageNotFoundError checks if the Discord error represents a 404 Unknown Message (code 10008).
func IsMessageNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	var restErr *discordgo.RESTError
	if errors.As(err, &restErr) {
		if restErr.Response != nil && restErr.Response.StatusCode == 404 {
			return true
		}
		if restErr.Message != nil && restErr.Message.Code == 10008 {
			return true
		}
	}
	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "10008") || strings.Contains(errStr, "unknown message") || strings.Contains(errStr, "404 not found")
}

// IsThreadArchivedOrLockedError checks if the Discord error indicates the thread is archived (50083) or locked (50084).
func IsThreadArchivedOrLockedError(err error) bool {
	if err == nil {
		return false
	}
	var restErr *discordgo.RESTError
	if errors.As(err, &restErr) {
		if restErr.Message != nil && (restErr.Message.Code == 50083 || restErr.Message.Code == 50084) {
			return true
		}
	}
	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "50083") || strings.Contains(errStr, "50084") || strings.Contains(errStr, "thread is archived") || strings.Contains(errStr, "thread is locked")
}

// IsPermissionError checks if the Discord error represents a 403 Forbidden or missing permissions (50001, 50013).
func IsPermissionError(err error) bool {
	if err == nil {
		return false
	}
	var restErr *discordgo.RESTError
	if errors.As(err, &restErr) {
		if restErr.Response != nil && restErr.Response.StatusCode == 403 {
			return true
		}
		if restErr.Message != nil && (restErr.Message.Code == 50001 || restErr.Message.Code == 50013) {
			return true
		}
	}
	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "50001") || strings.Contains(errStr, "50013") || strings.Contains(errStr, "missing access") || strings.Contains(errStr, "missing permissions") || strings.Contains(errStr, "403 forbidden")
}
