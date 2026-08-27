package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"text/template"
	"time"

	"github.com/bwmarrin/discordgo"
)

const (
	defaultTemplate = `{"prompt": "Here's a message someone sent you from Discord:\n\n{{range $k, $v := .}}- {{$k}}: {{$v | escapeJSON}}\n{{end}}\nIf this message is not already part of a thread, use the Discord MCP discord_create_thread tool to create a thread and post your reply in the thread for this message (channelId: \"{{.channel_id}}\", messageId: \"{{.id}}\"). If you are already replying inside an existing thread (or if creating a thread indicates the channel is not a guild text/news channel), do not create a new thread; instead post your reply directly in the thread using the discord_send tool (channelId: \"{{.channel_id}}\", replyToMessageId: \"{{.id}}\")."}`
	maxRetries      = 3
	initialDelay    = 500 * time.Millisecond
)

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

func buildMessageData(m *discordgo.Message) map[string]any {
	data := make(map[string]any)

	// Top-level Discord fields
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

	// Author fields flattened
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

	// Mentions
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

	// Member details if present
	if m.Member != nil {
		data["member_nick"] = m.Member.Nick
		data["member_roles"] = m.Member.Roles
	} else {
		data["member_nick"] = ""
		data["member_roles"] = []string{}
	}

	// Attachments
	var attachmentURLs []string
	for _, a := range m.Attachments {
		if a != nil {
			attachmentURLs = append(attachmentURLs, a.URL)
		}
	}
	data["attachments"] = attachmentURLs

	return data
}

func isBotTargeted(s *discordgo.Session, m *discordgo.MessageCreate) bool {
	// 1. Direct Messages
	if m.GuildID == "" {
		return true
	}

	// 2. Structured user mentions
	if len(m.Mentions) > 0 {
		return true
	}

	// 3. Raw content checks for mentions or bot names
	contentLower := strings.ToLower(m.Content)
	if strings.Contains(m.Content, "<@") ||
		strings.Contains(contentLower, "gundam") ||
		strings.Contains(contentLower, "brain") ||
		strings.Contains(contentLower, "bot") {
		return true
	}

	// 4. Inline reply to any message in thread/channel
	if m.ReferencedMessage != nil || m.MessageReference != nil {
		return true
	}

	// 5. Role mentions
	if len(m.MentionRoles) > 0 {
		return true
	}

	// 6. Messages inside any Thread channel
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

func sendWithRetry(client *http.Client, targetURL string, payload []byte) error {
	var lastErr error
	delay := initialDelay

	for attempt := 1; attempt <= maxRetries; attempt++ {
		req, err := http.NewRequest("POST", targetURL, bytes.NewReader(payload))
		if err != nil {
			return fmt.Errorf("failed to create request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err == nil {
			defer resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				log.Printf("discord-funnel: successfully forwarded message to %s (status %d)", targetURL, resp.StatusCode)
				return nil
			}
			body, _ := io.ReadAll(resp.Body)
			lastErr = fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
		} else {
			lastErr = err
		}

		log.Printf("discord-funnel: attempt %d/%d to %s failed: %v", attempt, maxRetries, targetURL, lastErr)
		if attempt < maxRetries {
			time.Sleep(delay)
			delay *= 2
		}
	}

	return fmt.Errorf("failed after %d attempts: %w", maxRetries, lastErr)
}

func main() {
	discordToken := flag.String("token", "", "Discord bot token")
	targetURL := flag.String("target", "", "Target URL to POST payload to")
	flag.StringVar(targetURL, "target-url", "", "Target URL to POST payload to (alias)")
	tmplStr := flag.String("template", "", "Go text/template string for payload formatting")
	mentionsOnly := flag.Bool("mentions-only", false, "Only process messages that mention the bot")
	flag.Parse()

	if *discordToken == "" {
		*discordToken = os.Getenv("DISCORD_TOKEN")
	}
	if *targetURL == "" {
		*targetURL = os.Getenv("TARGET_URL")
	}
	if *tmplStr == "" {
		*tmplStr = os.Getenv("PAYLOAD_TEMPLATE")
	}
	if *tmplStr == "" {
		*tmplStr = defaultTemplate
	}

	if *discordToken == "" {
		log.Fatal("discord-funnel: Discord bot token is required")
	}
	if *targetURL == "" {
		log.Fatal("discord-funnel: Target URL is required")
	}

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
	}).Parse(*tmplStr)
	if err != nil {
		log.Fatalf("discord-funnel: invalid template: %v", err)
	}

	dg, err := discordgo.New("Bot " + *discordToken)
	if err != nil {
		log.Fatalf("discord-funnel: failed to create Discord session: %v", err)
	}

	client := &http.Client{Timeout: 15 * time.Second}

	dg.AddHandler(func(s *discordgo.Session, r *discordgo.Ready) {
		log.Printf("discord-funnel: gateway session ready as %s#%s (user ID %s)", r.User.Username, r.User.Discriminator, r.User.ID)
	})

	dg.AddHandler(func(s *discordgo.Session, d *discordgo.Disconnect) {
		log.Printf("discord-funnel: gateway disconnected")
	})

	dg.AddHandler(func(s *discordgo.Session, r *discordgo.Resumed) {
		log.Printf("discord-funnel: gateway session resumed")
	})

	dg.AddHandler(func(s *discordgo.Session, m *discordgo.MessageCreate) {
		// Ignore bot's own messages
		if m.Author == nil || (s.State.User != nil && m.Author.ID == s.State.User.ID) {
			return
		}

		// Filter mentions if enabled
		if *mentionsOnly && !isBotTargeted(s, m) {
			log.Printf("discord-funnel: ignoring message %s from %s: %q (mentions_only enabled, no trigger matched)",
				m.ID, m.Author.Username, m.Content)
			return
		}

		data := buildMessageData(m.Message)
		var buf bytes.Buffer
		if err := tmpl.Execute(&buf, data); err != nil {
			log.Printf("discord-funnel: failed to execute template for message %s: %v", m.ID, err)
			return
		}

		payload := buf.Bytes()
		log.Printf("discord-funnel: processing message %s from %s (channel %s, payload length %d bytes): %q",
			m.ID, m.Author.Username, m.ChannelID, len(payload), m.Content)

		go func(msgID string, p []byte) {
			if err := sendWithRetry(client, *targetURL, p); err != nil {
				log.Printf("discord-funnel: error forwarding message %s: %v", msgID, err)
			}
		}(m.ID, payload)
	})

	dg.Identify.Intents = discordgo.IntentsGuildMessages | discordgo.IntentsDirectMessages | discordgo.IntentMessageContent
	dg.SyncEvents = false

	if err := dg.Open(); err != nil {
		log.Fatalf("discord-funnel: failed to open Discord session: %v", err)
	}
	defer dg.Close()

	log.Printf("discord-funnel: connected to Discord, mentions_only=%t, forwarding messages to %s", *mentionsOnly, *targetURL)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Println("discord-funnel: shutting down")
}
