package delivery

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
)

func TestSplitMessage(t *testing.T) {
	t.Parallel()
	p1 := strings.Repeat("A", 1200)
	p2 := strings.Repeat("B", 1200)

	tests := []struct {
		name       string
		text       string
		limit      int
		wantChunks int
		wantFirst  string
		wantLast   string
		checkCode  bool
	}{
		{
			name:       "EmptyString",
			text:       "",
			limit:      2000,
			wantChunks: 0,
		},
		{
			name:       "ShortTextWithinLimit",
			text:       "Hello world! This is a simple test message.",
			limit:      2000,
			wantChunks: 1,
			wantFirst:  "Hello world! This is a simple test message.",
		},
		{
			name:       "MultiParagraphExceedingLimit",
			text:       p1 + "\n\n" + p2,
			limit:      2000,
			wantChunks: 2,
			wantFirst:  p1,
			wantLast:   p2,
		},
		{
			name:       "GiantSingleContinuousString",
			text:       strings.Repeat("X", 4500),
			limit:      2000,
			wantChunks: 3,
		},
		{
			name:       "SmallLimitWordBoundary",
			text:       "The quick brown fox jumps over the lazy dog",
			limit:      20,
			wantChunks: 3,
		},
		{
			name:       "UnicodeAndEmojiSafety",
			text:       strings.Repeat("✨🌸", 1500),
			limit:      2000,
			wantChunks: 2,
		},
		{
			name:       "MarkdownFencesSplit",
			text:       "```python\nline1 = 'hello'\nline2 = 'world'\nline3 = 'foo'\nline4 = 'bar'\n```",
			limit:      35,
			wantChunks: 2,
			checkCode:  true,
		},
		{
			name:       "ZeroLimitDefaultsToMax",
			text:       "Hello world with zero limit",
			limit:      0,
			wantChunks: 1,
			wantFirst:  "Hello world with zero limit",
		},
		{
			name:       "CodeBlockOverflow",
			text:       "```go\n" + strings.Repeat("x", 100) + "\n```",
			limit:      40,
			wantChunks: 3,
			checkCode:  true,
		},
		{
			name:       "LongCodeBlockLines",
			text:       "```go\n" + strings.Repeat("x", 2500) + "\n```",
			limit:      2000,
			wantChunks: 2,
			checkCode:  true,
		},
		{
			name:       "CodeBlockClosedFollowedByText",
			text:       "```python\nprint(1)\n```\n" + strings.Repeat("A", 1990),
			limit:      2000,
			wantChunks: 2,
		},
		{
			name:       "ExactClosingFenceAtLimit",
			text:       "```go\n" + strings.Repeat("x", 1990) + "\n```",
			limit:      2000,
			wantChunks: 1,
		},
		{
			name:       "SmallLimitCodeFence",
			text:       "```go\n" + strings.Repeat("a", 100) + "\n```",
			limit:      50,
			wantChunks: 3,
			checkCode:  true,
		},
		{
			name:       "LongLineInsideCodeBlockExceedingLimit",
			text:       "```python\n" + strings.Repeat("x", 2500) + "\n```",
			limit:      500,
			wantChunks: 2,
			checkCode:  true,
		},
		{name: "ProseNearLimitBeforeOpeningFence", text: strings.Repeat("A", 1997) + "\n```go\nfmt.Println(1)\n```", limit: 2000, wantChunks: 2, wantFirst: strings.Repeat("A", 1997), wantLast: "```go\nfmt.Println(1)\n```"},
		{name: "ProseWhereOpeningFenceFitsButCodeLineDoesNot", text: strings.Repeat("A", 1989) + "\n```go\nfmt.Println(1)\n```", limit: 2000, wantChunks: 2, wantFirst: strings.Repeat("A", 1989), wantLast: "```go\nfmt.Println(1)\n```"},
		{name: "IndentedClosingFenceExceedingChunkLimit", text: "```go\n" + strings.Repeat("x", 1989) + "\n    ```", limit: 2000, wantChunks: 1, wantFirst: "```go\n" + strings.Repeat("x", 1989) + "\n```", wantLast: "```go\n" + strings.Repeat("x", 1989) + "\n```", checkCode: true},
		{name: "SingleLineInlineCodeFenceAtBoundary", text: strings.Repeat("A", 1990) + "\n```inline```\n" + strings.Repeat("B", 20), limit: 2000, wantChunks: 2, wantFirst: strings.Repeat("A", 1990), wantLast: "```inline```\n" + strings.Repeat("B", 20)},
		{name: "FenceWithLongLanguageTagAtSmallLimit", text: "```javascript\nconsole.log(1)\n```", limit: 12, wantChunks: 2, checkCode: true},
		{
			name:       "NestedListKeepsParentHeaderWithSubbullets",
			text:       strings.Repeat("A", 1930) + "\n- **Core Daemons**:\n  - **coven**: cpu = 100\n  - **hangar**: cpu = 100\n  - **agentsview**: cpu = 1000",
			limit:      2000,
			wantChunks: 2,
			wantFirst:  strings.Repeat("A", 1930),
			wantLast:   "- **Core Daemons**:\n  - **coven**: cpu = 100\n  - **hangar**: cpu = 100\n  - **agentsview**: cpu = 1000",
		},
		{
			name:       "OversizedSublistKeepsFirstChildAndDedentsOrphanChunk",
			text:       strings.Repeat("A", 35) + "\n- **Parent**:\n  1. " + strings.Repeat("B", 20) + "\n  2. " + strings.Repeat("C", 20) + "\n    - nested\n\n  3. " + strings.Repeat("D", 10),
			limit:      60,
			wantChunks: 3,
			wantFirst:  strings.Repeat("A", 35),
			wantLast:   "2. " + strings.Repeat("C", 20) + "\n  - nested\n\n3. " + strings.Repeat("D", 10),
		},
		{
			name:       "DeepToShallowOrphanSublistRatchetsDedent",
			text:       "  \n    * deep item\n  - shallow sibling\n    - child of sibling\nNormal line",
			limit:      2000,
			wantChunks: 1,
			wantFirst:  "* deep item\n- shallow sibling\n  - child of sibling\nNormal line",
		},
		{
			name:       "ThreeLevelNestedListExceedingLimitDoesNotOrphanParent",
			text:       strings.Repeat("A", 35) + "\n- **Parent**:\n  - **Subparent**:\n    - **Grandchild**:" + strings.Repeat("x", 15),
			limit:      60,
			wantChunks: 3,
			wantFirst:  strings.Repeat("A", 35),
			wantLast:   "- **Grandchild**:" + strings.Repeat("x", 15),
		},
		{
			name:       "OversizedSublistWhereEvenFirstChildExceedsLimit",
			text:       strings.Repeat("A", 10) + "\n- **Parent**:\n  - " + strings.Repeat("B", 60),
			limit:      30,
			wantChunks: 3,
			wantFirst:  strings.Repeat("A", 10) + "\n- **Parent**:",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			res := SplitMessage(tc.text, tc.limit)
			effLimit := tc.limit
			if effLimit <= 0 {
				effLimit = 2000
			}
			if len(tc.text) == 0 {
				if len(res) != 0 {
					t.Fatalf("expected 0 chunks for empty input, got %d", len(res))
				}
				return
			}
			if len(res) < tc.wantChunks {
				t.Fatalf("expected at least %d chunks, got %d", tc.wantChunks, len(res))
			}
			for i, chunk := range res {
				if len([]rune(chunk)) > effLimit {
					t.Errorf("chunk %d exceeds limit %d: %d runes", i, effLimit, len([]rune(chunk)))
				}
				if tc.checkCode {
					fenceCount := strings.Count(chunk, "```")
					if fenceCount%2 != 0 {
						t.Errorf("chunk %d has unclosed/odd code fences (%d): %q", i, fenceCount, chunk)
					}
				}
			}
			if tc.wantFirst != "" && len(res) > 0 && res[0] != tc.wantFirst {
				t.Errorf("want first chunk %q, got %q", tc.wantFirst, res[0])
			}
			if tc.wantLast != "" && len(res) > 0 && res[len(res)-1] != tc.wantLast {
				t.Errorf("want last chunk %q, got %q", tc.wantLast, res[len(res)-1])
			}
		})
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

func TestResolveChannelByNameOrID(t *testing.T) {
	// 1. Nil session
	if _, err := ResolveChannelByNameOrID(nil, "aerial-dev"); err == nil {
		t.Error("Expected error for nil session, got nil")
	}

	// 2. Empty string
	sess := newMockDiscordSession(func(req *http.Request) (*http.Response, error) { return mockJSONResponse(http.StatusOK, `[]`) })
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

	sess := newMockDiscordSession(func(req *http.Request) (*http.Response, error) { return mockJSONResponse(http.StatusOK, `[]`) })
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
	var sentPayloads []string
	sess := newMockDiscordSession(func(req *http.Request) (*http.Response, error) {
		if req.Body != nil {
			bodyBytes, _ := io.ReadAll(req.Body)
			sentPayloads = append(sentPayloads, string(bodyBytes))
		}
		return mockJSONResponse(http.StatusOK, `{"id":"msg-123","channel_id":"12345","content":"ok"}`)
	})

	// 1. Single message
	if err := SendMessage(sess, "12345", "Hello world"); err != nil {
		t.Fatalf("SendMessage failed: %v", err)
	}
	if len(sentPayloads) == 0 || !strings.Contains(sentPayloads[0], `"flags":4`) {
		t.Errorf("expected payload to contain flags:4, got %+v", sentPayloads)
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

	// 6. Empty channelID and attachment send failure on empty text
	if err := SendMessageWithAttachments(sess, "", "hello", nil); err == nil {
		t.Error("expected error for empty channelID")
	}
	if err := SendMessageWithAttachments(errSess, "12345", "", []*Attachment{att}); err == nil {
		t.Error("expected error when attachment send fails")
	}
}

func TestStartTyping_MockSession(t *testing.T) {
	sess := newMockDiscordSession(func(req *http.Request) (*http.Response, error) {
		return mockJSONResponse(http.StatusNoContent, "")
	})

	stop := StartTyping(sess, "12345")
	time.Sleep(20 * time.Millisecond)
	stop()

	// Error branch from ChannelTyping
	sessErr := newMockDiscordSession(func(req *http.Request) (*http.Response, error) {
		return nil, errors.New("simulated network typing failure")
	})
	stopErr := StartTyping(sessErr, "chan-typing-error")
	time.Sleep(10 * time.Millisecond)
	stopErr()
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

	// API error branch
	sessGuildErr := newMockDiscordSession(func(req *http.Request) (*http.Response, error) {
		if strings.Contains(req.URL.Path, "/users/@me/guilds") {
			return mockJSONResponse(http.StatusOK, `[{"id":"g-err","name":"Error Guild"}]`)
		}
		if strings.Contains(req.URL.Path, "/guilds/g-err/channels") {
			return nil, errors.New("guild channels fetch failed")
		}
		return mockJSONResponse(http.StatusNotFound, `{"message":"not found"}`)
	})
	sessGuildErr.Token = "Bot mock-token"
	_, _ = ResolveChannelByNameOrID(sessGuildErr, "any-channel")
}

func TestEditMessage_And_DeleteMessage(t *testing.T) {
	// 1. Validation errors with nil session or empty IDs
	if err := EditMessage(nil, "ch1", "msg1", "hello"); err == nil {
		t.Error("expected error for nil session in EditMessage")
	}
	if err := EditMessage(&discordgo.Session{}, "", "msg1", "hello"); err == nil {
		t.Error("expected error for empty channelID in EditMessage")
	}
	if err := EditMessage(&discordgo.Session{}, "ch1", "", "hello"); err == nil {
		t.Error("expected error for empty messageID in EditMessage")
	}
	if err := EditMessage(&discordgo.Session{}, "ch1", "msg1", "   "); err == nil {
		t.Error("expected error for empty text in EditMessage")
	}

	if err := DeleteMessage(nil, "ch1", "msg1"); err == nil {
		t.Error("expected error for nil session in DeleteMessage")
	}
	if err := DeleteMessage(&discordgo.Session{}, "", "msg1"); err == nil {
		t.Error("expected error for empty channelID in DeleteMessage")
	}
	if err := DeleteMessage(&discordgo.Session{}, "ch1", ""); err == nil {
		t.Error("expected error for empty messageID in DeleteMessage")
	}

	// 2. Successful mock calls
	var lastMethod, lastPath string
	var lastPatchBody string
	sess := newMockDiscordSession(func(req *http.Request) (*http.Response, error) {
		lastMethod = req.Method
		lastPath = req.URL.Path
		if req.Method == http.MethodPatch {
			if req.Body != nil {
				b, _ := io.ReadAll(req.Body)
				lastPatchBody = string(b)
			}
			return mockJSONResponse(http.StatusOK, `{"id":"msg1","channel_id":"ch1","content":"updated"}`)
		}
		if req.Method == http.MethodDelete {
			return mockJSONResponse(http.StatusNoContent, "")
		}
		return mockJSONResponse(http.StatusBadRequest, `{"message":"bad request"}`)
	})

	if err := EditMessage(sess, "ch1", "msg1", "updated text"); err != nil {
		t.Fatalf("EditMessage failed: %v", err)
	}
	if err := EditMessage(sess, "ch1", "msg1", strings.Repeat("A", 2500)); err != nil {
		t.Fatalf("EditMessage with truncation failed: %v", err)
	}
	if lastMethod != http.MethodPatch || !strings.Contains(lastPath, "/channels/ch1/messages/msg1") {
		t.Errorf("unexpected edit request: %s %s", lastMethod, lastPath)
	}
	if !strings.Contains(lastPatchBody, `"flags":4`) {
		t.Errorf("expected edit request body to contain flags:4, got %s", lastPatchBody)
	}

	if err := DeleteMessage(sess, "ch1", "msg1"); err != nil {
		t.Fatalf("DeleteMessage failed: %v", err)
	}
	if lastMethod != http.MethodDelete || !strings.Contains(lastPath, "/channels/ch1/messages/msg1") {
		t.Errorf("unexpected delete request: %s %s", lastMethod, lastPath)
	}
}

func TestErrorClassifiers(t *testing.T) {
	// Nil checks
	if IsMessageNotFoundError(nil) {
		t.Error("expected false for nil error")
	}
	if IsThreadArchivedOrLockedError(nil) {
		t.Error("expected false for nil error")
	}

	// 404 / 10008 errors
	errNotFound := errors.New("HTTP 404 Not Found, 10008 Unknown Message")
	if !IsMessageNotFoundError(errNotFound) {
		t.Error("expected true for 10008 Unknown Message")
	}

	// 50083 / 50084 errors
	errArchived := errors.New("HTTP 400 Bad Request, 50083 Thread is archived")
	if !IsThreadArchivedOrLockedError(errArchived) {
		t.Error("expected true for 50083 Thread is archived")
	}

	errLocked := errors.New("HTTP 403 Forbidden, 50084 Thread is locked")
	if !IsThreadArchivedOrLockedError(errLocked) {
		t.Error("expected true for 50084 Thread is locked")
	}

	// Permission errors (403, 50001, 50013)
	err403 := errors.New("HTTP 403 Forbidden")
	if !IsPermissionError(err403) {
		t.Error("expected true for HTTP 403 Forbidden")
	}
	err50001 := errors.New("HTTP 403 Forbidden, 50001 Missing Access")
	if !IsPermissionError(err50001) {
		t.Error("expected true for 50001 Missing Access")
	}
	err50013 := errors.New("HTTP 403 Forbidden, 50013 Missing Permissions")
	if !IsPermissionError(err50013) {
		t.Error("expected true for 50013 Missing Permissions")
	}

	// Unrelated errors
	unrelated := errors.New("connection reset by peer")
	if IsMessageNotFoundError(unrelated) || IsThreadArchivedOrLockedError(unrelated) || IsPermissionError(unrelated) {
		t.Error("unrelated error should not match classifiers")
	}
	if IsPermissionError(nil) {
		t.Error("expected false for nil error")
	}
}

func TestErrorClassifiers_RESTError(t *testing.T) {
	// 404
	err404 := &discordgo.RESTError{Response: &http.Response{StatusCode: http.StatusNotFound}}
	if !IsMessageNotFoundError(err404) {
		t.Error("expected true for RESTError 404")
	}
	// 10008
	err10008 := &discordgo.RESTError{Message: &discordgo.APIErrorMessage{Code: 10008}}
	if !IsMessageNotFoundError(err10008) {
		t.Error("expected true for RESTError 10008")
	}
	// 50083
	err50083 := &discordgo.RESTError{Message: &discordgo.APIErrorMessage{Code: 50083}}
	if !IsThreadArchivedOrLockedError(err50083) {
		t.Error("expected true for RESTError 50083")
	}
	// 50084
	err50084 := &discordgo.RESTError{Message: &discordgo.APIErrorMessage{Code: 50084}}
	if !IsThreadArchivedOrLockedError(err50084) {
		t.Error("expected true for RESTError 50084")
	}
	// 403
	err403 := &discordgo.RESTError{Response: &http.Response{StatusCode: http.StatusForbidden}}
	if !IsPermissionError(err403) {
		t.Error("expected true for RESTError 403")
	}
	// 50001
	err50001 := &discordgo.RESTError{Message: &discordgo.APIErrorMessage{Code: 50001}}
	if !IsPermissionError(err50001) {
		t.Error("expected true for RESTError 50001")
	}
	// 50013
	err50013 := &discordgo.RESTError{Message: &discordgo.APIErrorMessage{Code: 50013}}
	if !IsPermissionError(err50013) {
		t.Error("expected true for RESTError 50013")
	}
}

func TestIsSnowflake(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{name: "empty snowflake", input: "", want: false},
		{name: "alphanumeric snowflake", input: "12345abc6789", want: false},
		{name: "valid numeric snowflake", input: "123456789012345678", want: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := isSnowflake(tc.input)
			if got != tc.want {
				t.Errorf("isSnowflake(%q) = %v, want %v", tc.input, got, tc.want)
			}
		})
	}
}
