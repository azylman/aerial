package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"text/template"
	"time"

	"github.com/bwmarrin/discordgo"
)

const defaultFunnelTemplate = `{"conversation_id": "{{.conversation_id}}", "prompt": "Here's a message someone sent you from Discord:\n\n{{range $k, $v := .}}- {{$k}}: {{$v | escapeJSON}}\n{{end}}\nCRITICAL REQUIREMENT: You MUST send your response back to Discord using the Discord MCP tool via ` + "`" + `call_mcp_tool` + "`" + ` with ServerName: \"discord\":\n{{if .is_thread}}1. This message is ALREADY inside a thread: Execute ` + "`" + `call_mcp_tool` + "`" + ` with ServerName: \"discord\", ToolName: \"discord_send\", and Arguments: {\"channelId\": \"{{.channel_id}}\", \"message\": \"<your response>\"}.\n{{else}}1. This message is a new inquiry (NOT inside a thread): Execute ` + "`" + `call_mcp_tool` + "`" + ` with ServerName: \"discord\", ToolName: \"discord_create_thread\", and Arguments: {\"channelId\": \"{{.channel_id}}\", \"messageId\": \"{{.id}}\", \"name\": \"<concise thread title>\", \"message\": \"<your response>\"}.\n{{end}}Execute the tool call now."}`

func escapeJSON(v any) string {
	b, err := json.Marshal(fmt.Sprint(v))
	if err != nil {
		return fmt.Sprint(v)
	}
	if len(b) >= 2 && b[0] == '"' && b[len(b)-1] == '"' {
		return string(b[1 : len(b)-1])
	}
	return string(b)
}

func toJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(b)
}

func getFunnelConversationID(s *discordgo.Session, m *discordgo.Message) (string, bool) {
	if m.GuildID == "" {
		return m.ChannelID, false
	}
	if ch, err := s.State.Channel(m.ChannelID); err == nil && ch != nil && ch.IsThread() {
		return m.ChannelID, true
	}
	if ch, err := s.Channel(m.ChannelID); err == nil && ch != nil && ch.IsThread() {
		return m.ChannelID, true
	}
	return m.ID, false
}

func buildFunnelMessageData(s *discordgo.Session, m *discordgo.Message) map[string]any {
	data := make(map[string]any)

	convID, isThread := getFunnelConversationID(s, m)
	data["conversation_id"] = convID
	data["is_thread"] = isThread

	data["id"] = m.ID
	data["channel_id"] = m.ChannelID
	data["guild_id"] = m.GuildID
	data["content"] = m.Content
	data["timestamp"] = m.Timestamp.Format(time.RFC3339)
	data["mention_everyone"] = m.MentionEveryone
	data["pinned"] = m.Pinned
	data["tts"] = m.TTS
	data["type"] = int(m.Type)
	data["webhook_id"] = m.WebhookID

	if m.EditedTimestamp != nil {
		data["edited_timestamp"] = m.EditedTimestamp.Format(time.RFC3339)
	} else {
		data["edited_timestamp"] = ""
	}

	if m.Author != nil {
		data["author_id"] = m.Author.ID
		data["author_username"] = m.Author.Username
		data["author_discriminator"] = m.Author.Discriminator
		data["author_global_name"] = m.Author.GlobalName
		data["author_bot"] = m.Author.Bot
		data["author"] = map[string]any{
			"id":            m.Author.ID,
			"username":      m.Author.Username,
			"discriminator": m.Author.Discriminator,
			"global_name":   m.Author.GlobalName,
			"bot":           m.Author.Bot,
		}
	}

	var mentionNames, mentionIDs []string
	for _, u := range m.Mentions {
		if u != nil {
			mentionNames = append(mentionNames, u.Username)
			mentionIDs = append(mentionIDs, u.ID)
		}
	}
	data["mentions"] = mentionNames
	data["mention_ids"] = mentionIDs
	data["mention_roles"] = m.MentionRoles

	if m.Member != nil {
		data["member_nick"] = m.Member.Nick
		data["member_roles"] = m.Member.Roles
	} else {
		data["member_nick"] = ""
		data["member_roles"] = []string{}
	}

	var attachmentURLs []string
	for _, a := range m.Attachments {
		if a != nil {
			attachmentURLs = append(attachmentURLs, a.URL)
		}
	}
	data["attachments"] = attachmentURLs

	return data
}

func isFunnelBotTargeted(s *discordgo.Session, m *discordgo.MessageCreate) bool {
	if m.GuildID == "" {
		return true
	}
	if len(m.Mentions) > 0 {
		return true
	}
	contentLower := strings.ToLower(m.Content)
	if strings.Contains(m.Content, "<@") ||
		strings.Contains(contentLower, "aerial") ||
		strings.Contains(contentLower, "gundam") ||
		strings.Contains(contentLower, "brain") ||
		strings.Contains(contentLower, "bot") {
		return true
	}
	if m.ReferencedMessage != nil || m.MessageReference != nil {
		return true
	}
	if len(m.MentionRoles) > 0 {
		return true
	}
	if ch, err := s.State.Channel(m.ChannelID); err == nil && ch != nil {
		if ch.IsThread() {
			return true
		}
	} else if ch, err := s.Channel(m.ChannelID); err == nil && ch != nil {
		if ch.IsThread() {
			return true
		}
	}
	return false
}

func startDiscordFunnel(db *sql.DB, agyBin, apiKey, model, systemPrompt string, timeoutMinutes int, mcpConfig json.RawMessage) {
	token := getEnv("DISCORD_TOKEN", getEnv("DISCORD_BOT_TOKEN", ""))
	if token == "" {
		log.Println("Discord funnel disabled: DISCORD_BOT_TOKEN/DISCORD_TOKEN not configured")
		return
	}

	tmplStr := getEnv("PAYLOAD_TEMPLATE", defaultFunnelTemplate)
	mentionsOnly := os.Getenv("MENTIONS_ONLY") == "true"

	tmpl, err := template.New("payload").Funcs(template.FuncMap{
		"escapeJSON": escapeJSON,
		"json":       toJSON,
		"quote": func(s any) string {
			return fmt.Sprintf("%q", fmt.Sprint(s))
		},
		"upper": func(s any) string {
			return strings.ToUpper(fmt.Sprint(s))
		},
		"lower": func(s any) string {
			return strings.ToLower(fmt.Sprint(s))
		},
		"trim": func(s any) string {
			return strings.TrimSpace(fmt.Sprint(s))
		},
	}).Parse(tmplStr)
	if err != nil {
		log.Fatalf("Discord funnel invalid template: %v", err)
	}

	dg, err := discordgo.New("Bot " + token)
	if err != nil {
		log.Fatalf("Discord funnel failed to create session: %v", err)
	}

	dg.AddHandler(func(s *discordgo.Session, r *discordgo.Ready) {
		log.Printf("Discord funnel gateway session ready as %s#%s (user ID %s)", r.User.Username, r.User.Discriminator, r.User.ID)
	})

	dg.AddHandler(func(s *discordgo.Session, m *discordgo.MessageCreate) {
		if m.Author == nil || (s.State.User != nil && m.Author.ID == s.State.User.ID) {
			return
		}

		if mentionsOnly && !isFunnelBotTargeted(s, m) {
			log.Printf("Discord funnel ignoring message %s from %s: no trigger matched", m.ID, m.Author.Username)
			return
		}

		data := buildFunnelMessageData(s, m.Message)
		var buf bytes.Buffer
		if err := tmpl.Execute(&buf, data); err != nil {
			log.Printf("Discord funnel template execution error for message %s: %v", m.ID, err)
			return
		}

		var req PromptRequest
		if err := json.Unmarshal(buf.Bytes(), &req); err != nil {
			log.Printf("Discord funnel payload unmarshal error for message %s: %v", m.ID, err)
			return
		}

		log.Printf("Discord funnel received message %s from %s (channel %s, conversation_id: %s)",
			m.ID, m.Author.Username, m.ChannelID, req.ConversationID)

		// Start continuous typing indicator ticker until agent completion
		stopTyping := make(chan struct{})
		go func(channelID string) {
			_ = s.ChannelTyping(channelID)
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					_ = s.ChannelTyping(channelID)
				case <-stopTyping:
					return
				}
			}
		}(m.ChannelID)

		onComplete := func() {
			close(stopTyping)
		}

		executePrompt(db, req, agyBin, apiKey, model, systemPrompt, timeoutMinutes, mcpConfig, onComplete)
	})

	dg.Identify.Intents = discordgo.IntentsGuildMessages | discordgo.IntentsDirectMessages | discordgo.IntentMessageContent
	dg.SyncEvents = false

	if err := dg.Open(); err != nil {
		log.Printf("Discord funnel failed to open session: %v", err)
		return
	}

	log.Printf("Discord funnel worker started successfully inside Brain")
}
