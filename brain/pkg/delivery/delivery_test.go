package delivery

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
)

func TestSplitMessage(t *testing.T) {
	// 1. Empty string
	if res := SplitMessage("", 2000); len(res) != 0 {
		t.Errorf("Expected 0 chunks for empty string, got %d", len(res))
	}

	// 2. Short text within limit
	short := "Hello world! This is a simple test message."
	res := SplitMessage(short, 2000)
	if len(res) != 1 || res[0] != short {
		t.Errorf("Expected 1 chunk matching input, got %+v", res)
	}

	// 3. Multi-paragraph text exceeding limit
	p1 := strings.Repeat("A", 1200)
	p2 := strings.Repeat("B", 1200)
	text := p1 + "\n\n" + p2
	res = SplitMessage(text, 2000)
	if len(res) != 2 {
		t.Fatalf("Expected 2 chunks, got %d", len(res))
	}
	for i, c := range res {
		if len([]rune(c)) > 2000 {
			t.Errorf("Chunk %d exceeds 2000 runes: %d", i, len([]rune(c)))
		}
	}
	if res[0] != p1 || res[1] != p2 {
		t.Errorf("Chunks did not split cleanly on paragraph boundary")
	}

	// 4. Large single continuous string without spaces
	giant := strings.Repeat("X", 4500)
	res = SplitMessage(giant, 2000)
	if len(res) != 3 {
		t.Fatalf("Expected 3 chunks for 4500 char string, got %d", len(res))
	}
	if len([]rune(res[0])) != 2000 || len([]rune(res[1])) != 2000 || len([]rune(res[2])) != 500 {
		t.Errorf("Unexpected chunk sizes: %d, %d, %d", len([]rune(res[0])), len([]rune(res[1])), len([]rune(res[2])))
	}

	// 5. Small custom limit test (limit = 20)
	words := "The quick brown fox jumps over the lazy dog"
	res = SplitMessage(words, 20)
	for i, c := range res {
		if len([]rune(c)) > 20 {
			t.Errorf("Chunk %d exceeds limit 20: len=%d (%q)", i, len([]rune(c)), c)
		}
	}
	joined := strings.Join(res, " ")
	if strings.ReplaceAll(joined, "  ", " ") != words {
		t.Errorf("Reconstructed words mismatch: %q vs %q", joined, words)
	}
}

func TestSendMessageNilSession(t *testing.T) {
	err := SendMessage(nil, "12345", "hello")
	if err == nil {
		t.Error("Expected error when sending message with nil session")
	}

	err = SendMessage(nil, "", "hello")
	if err == nil {
		t.Error("Expected error when sending message with empty channel ID")
	}
}

func TestStartTypingNilSessionAndStop(t *testing.T) {
	stop := StartTyping(nil, "12345")
	if stop == nil {
		t.Fatal("Expected non-nil stop function")
	}
	// Calling stop should not panic
	stop()
	stop() // multiple calls should be safe

	stop = StartTyping(nil, "")
	stop()
}

func TestSplitMessageUnicode(t *testing.T) {
	// Verify unicode / emoji safety with 2000 rune limit
	sparkles := strings.Repeat("✨🌸", 1500)
	res := SplitMessage(sparkles, 2000)
	if len(res) < 2 {
		t.Fatalf("Expected at least 2 chunks, got %d", len(res))
	}
	for i, chunk := range res {
		if len([]rune(chunk)) > 2000 {
			t.Errorf("Chunk %d exceeds 2000 runes: %d", i, len([]rune(chunk)))
		}
	}
}

func TestSplitMessageMarkdownFences(t *testing.T) {
	codeSnippet := "```python\nline1 = 'hello'\nline2 = 'world'\nline3 = 'foo'\nline4 = 'bar'\n```"
	// Split with a small limit such that it breaks inside the code fence
	res := SplitMessage(codeSnippet, 35)

	if len(res) < 2 {
		t.Fatalf("Expected multiple chunks, got %d", len(res))
	}

	for i, chunk := range res {
		if len([]rune(chunk)) > 35 {
			t.Errorf("Chunk %d exceeds limit 35: len=%d (%q)", i, len([]rune(chunk)), chunk)
		}
		// Count occurrences of ``` in this chunk
		fenceCount := strings.Count(chunk, "```")
		if fenceCount%2 != 0 {
			t.Errorf("Chunk %d has unclosed/odd code fences (%d fences): %q", i, fenceCount, chunk)
		}
	}

	// First chunk should close with ```
	if !strings.HasSuffix(res[0], "```") {
		t.Errorf("Expected first chunk to close with ```, got %q", res[0])
	}

	// Subsequent chunk inside code block should reopen with ```python
	if !strings.HasPrefix(res[1], "```python") {
		t.Errorf("Expected second chunk to reopen with ```python, got %q", res[1])
	}
}

func TestResolveChannelByNameOrID(t *testing.T) {
	// 1. Nil session
	if _, err := ResolveChannelByNameOrID(nil, "aerial-dev"); err == nil {
		t.Error("Expected error for nil session, got nil")
	}

	// 2. Empty string
	sess, _ := discordgo.New("Bot dummy-token")
	if _, err := ResolveChannelByNameOrID(sess, ""); err == nil {
		t.Error("Expected error for empty channel name, got nil")
	}
	if _, err := ResolveChannelByNameOrID(sess, "#"); err == nil {
		t.Error("Expected error for '#' channel name, got nil")
	}

	// 3. Snowflake numeric ID
	snowflake := "123456789012345678"
	resolved, err := ResolveChannelByNameOrID(sess, snowflake)
	if err != nil || resolved != snowflake {
		t.Errorf("Expected snowflake %q, got %q (err: %v)", snowflake, resolved, err)
	}

	// 4. Channel in State.Guilds
	_ = sess.State.GuildAdd(&discordgo.Guild{
		ID: "guild-1",
		Channels: []*discordgo.Channel{
			{ID: "chan-general", Name: "general"},
			{ID: "chan-aerial-dev", Name: "aerial-dev"},
		},
	})

	// Name without #
	resolved, err = ResolveChannelByNameOrID(sess, "aerial-dev")
	if err != nil || resolved != "chan-aerial-dev" {
		t.Errorf("Expected chan-aerial-dev, got %q (err: %v)", resolved, err)
	}

	// Name with #
	resolved, err = ResolveChannelByNameOrID(sess, "#aerial-dev")
	if err != nil || resolved != "chan-aerial-dev" {
		t.Errorf("Expected chan-aerial-dev, got %q (err: %v)", resolved, err)
	}

	// Case-insensitive name
	resolved, err = ResolveChannelByNameOrID(sess, "#AERIAL-DEV")
	if err != nil || resolved != "chan-aerial-dev" {
		t.Errorf("Expected chan-aerial-dev for #AERIAL-DEV, got %q (err: %v)", resolved, err)
	}

	// Non-existent channel
	if _, err := ResolveChannelByNameOrID(sess, "non-existent-channel"); err == nil {
		t.Error("Expected error for non-existent channel name, got nil")
	}
}

func TestSendSystemAlert(t *testing.T) {
	// Nil session
	if err := SendSystemAlert(nil, "aerial-dev", "Title", "Body"); err == nil {
		t.Error("Expected error for nil session, got nil")
	}

	sess, _ := discordgo.New("Bot dummy-token")
	// Channel not found
	if err := SendSystemAlert(sess, "missing-chan", "Title", "Body"); err == nil {
		t.Error("Expected error for missing channel, got nil")
	}
}

func TestSendMessageWithAttachmentsNilSession(t *testing.T) {
	err := SendMessageWithAttachments(nil, "12345", "hello", nil)
	if err == nil {
		t.Error("Expected error when sending message with nil session")
	}

	att := &Attachment{
		Filename:    "test.png",
		ContentType: "image/png",
		Data:        []byte("fake png data"),
	}
	err = SendMessageWithAttachments(nil, "12345", "hello", []*Attachment{att})
	if err == nil {
		t.Error("Expected error when sending message with nil session and attachments")
	}
}

type mockRoundTripper struct {
	roundTrip func(req *http.Request) (*http.Response, error)
}

func (m *mockRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return m.roundTrip(req)
}

func newMockDiscordSession(handler func(req *http.Request) (*http.Response, error)) *discordgo.Session {
	s, _ := discordgo.New("Bot mock_token")
	s.Client = &http.Client{
		Transport: &mockRoundTripper{roundTrip: handler},
	}
	return s
}

func mockJSONResponse(status int, body string) (*http.Response, error) {
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     make(http.Header),
	}, nil
}

func TestSendMessageWithAttachments_MockSession(t *testing.T) {
	sess := newMockDiscordSession(func(req *http.Request) (*http.Response, error) {
		return mockJSONResponse(http.StatusOK, `{"id":"msg-123","channel_id":"12345","content":"ok"}`)
	})

	// 1. Single message
	if err := SendMessage(sess, "12345", "Hello world"); err != nil {
		t.Fatalf("SendMessage failed: %v", err)
	}

	// 2. Multi-chunk message with attachment
	longText := strings.Repeat("A", 2500)
	att := &Attachment{
		Filename:    "img.png",
		ContentType: "image/png",
		Data:        []byte("fake png"),
	}
	if err := SendMessageWithAttachments(sess, "12345", longText, []*Attachment{att}); err != nil {
		t.Fatalf("SendMessageWithAttachments multi-chunk failed: %v", err)
	}

	// 3. Attachments only (empty text)
	if err := SendMessageWithAttachments(sess, "12345", "", []*Attachment{att}); err != nil {
		t.Fatalf("SendMessageWithAttachments attachments only failed: %v", err)
	}

	// 4. Empty text and empty attachments -> returns nil
	if err := SendMessageWithAttachments(sess, "12345", "", nil); err != nil {
		t.Fatalf("SendMessageWithAttachments empty text failed: %v", err)
	}

	// 5. Send error from API
	errSess := newMockDiscordSession(func(req *http.Request) (*http.Response, error) {
		return mockJSONResponse(http.StatusBadRequest, `{"message":"Missing permissions","code":50013}`)
	})
	if err := SendMessage(errSess, "12345", "Hello fail"); err == nil {
		t.Error("expected error from API failure, got nil")
	}
}

func TestStartTyping_MockSession(t *testing.T) {
	sess := newMockDiscordSession(func(req *http.Request) (*http.Response, error) {
		return mockJSONResponse(http.StatusNoContent, "")
	})

	stop := StartTyping(sess, "12345")
	time.Sleep(20 * time.Millisecond)
	stop()
}

func TestSendSystemAlert_Success(t *testing.T) {
	sess := newMockDiscordSession(func(req *http.Request) (*http.Response, error) {
		return mockJSONResponse(http.StatusOK, `{"id":"alert-msg-123","channel_id":"chan-alerts"}`)
	})

	_ = sess.State.GuildAdd(&discordgo.Guild{
		ID: "g1",
		Channels: []*discordgo.Channel{
			{ID: "chan-alerts", Name: "aerial-alerts"},
		},
	})

	if err := SendSystemAlert(sess, "aerial-alerts", "System Alert", "Everything operational"); err != nil {
		t.Fatalf("SendSystemAlert failed: %v", err)
	}
}

func TestResolveChannelByNameOrID_APIFallback(t *testing.T) {
	sess := newMockDiscordSession(func(req *http.Request) (*http.Response, error) {
		if strings.Contains(req.URL.Path, "/users/@me/guilds") {
			return mockJSONResponse(http.StatusOK, `[{"id":"g100","name":"My Guild"}]`)
		}
		if strings.Contains(req.URL.Path, "/guilds/g100/channels") {
			return mockJSONResponse(http.StatusOK, `[{"id":"chan-remote-999","name":"remote-channel"}]`)
		}
		return mockJSONResponse(http.StatusNotFound, `{"message":"Not found"}`)
	})
	sess.Token = "Bot mock_token"

	// Channel is not in state, so it queries API
	chanID, err := ResolveChannelByNameOrID(sess, "remote-channel")
	if err != nil || chanID != "chan-remote-999" {
		t.Errorf("expected chan-remote-999 from API fallback, got %q (err: %v)", chanID, err)
	}
}

func TestSplitMessage_ZeroLimitAndCodeBlockOverflow(t *testing.T) {
	// 1. limit <= 0 defaults to MaxDiscordMessageLength
	res := SplitMessage("Hello world with zero limit", 0)
	if len(res) != 1 || res[0] != "Hello world with zero limit" {
		t.Errorf("unexpected SplitMessage output for 0 limit: %v", res)
	}

	// 2. Giant continuous line inside a code block that exceeds limit
	giantCode := "```go\n" + strings.Repeat("x", 100) + "\n```"
	resCode := SplitMessage(giantCode, 40)
	if len(resCode) < 3 {
		t.Errorf("expected multiple chunks for giant line inside code block, got %d", len(resCode))
	}
}
