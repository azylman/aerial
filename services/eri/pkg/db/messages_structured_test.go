package db

import (
	"context"
	"testing"
	"time"
)

func TestMessageMetadata_IsEmpty(t *testing.T) {
	if !(MessageMetadata{}).IsEmpty() {
		t.Errorf("expected empty metadata to return true for IsEmpty")
	}
	tests := []struct {
		name string
		md   MessageMetadata
	}{
		{"channel_id", MessageMetadata{ChannelID: "c1"}},
		{"channel_name", MessageMetadata{ChannelName: "aerial-dev"}},
		{"target_thread_id", MessageMetadata{TargetThreadID: "t1"}},
		{"thread_name", MessageMetadata{ThreadName: "docker-orchestration"}},
		{"guild_id", MessageMetadata{GuildID: "g1"}},
		{"author_username", MessageMetadata{AuthorUsername: "alice"}},
		{"author_global_name", MessageMetadata{AuthorGlobalName: "Alice B"}},
		{"author_bot", MessageMetadata{AuthorBot: true}},
		{"is_admin", MessageMetadata{IsAdmin: true}},
		{"mentions", MessageMetadata{Mentions: []string{"aerial"}}},
		{"mention_user_ids", MessageMetadata{MentionUserIDs: []string{"123"}}},
		{"mention_role_ids", MessageMetadata{MentionRoleIDs: []string{"456"}}},
		{"replying_to_author", MessageMetadata{ReplyingToAuthor: "@bob"}},
		{"replying_to_content", MessageMetadata{ReplyingToContent: "hello"}},
		{"attachments", MessageMetadata{Attachments: []string{"http://example.com/img.png"}}},
		{"model", MessageMetadata{Model: "gemini-4-flash"}},
		{"silent", MessageMetadata{Silent: true}},
		{"ephemeral_true", func() MessageMetadata { tVal := true; return MessageMetadata{Ephemeral: &tVal} }()},
		{"ephemeral_false", func() MessageMetadata { fVal := false; return MessageMetadata{Ephemeral: &fVal} }()},
		{"vanadis_chunk_id", MessageMetadata{VanadisChunkID: "chk-1"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.md.IsEmpty() {
				t.Errorf("expected metadata with %s to return false for IsEmpty", tc.name)
			}
		})
	}
}

func TestMessageMetadata_ValueAndScan(t *testing.T) {
	md := MessageMetadata{
		ChannelID: "chan-1", TargetThreadID: "thread-1", GuildID: "guild-1", AuthorUsername: "arcane103",
		AuthorGlobalName: "Arcane", IsAdmin: true, Mentions: []string{"aerial"},
		MentionUserIDs: []string{"1542035925603713086"}, MentionRoleIDs: []string{"role-1"},
		ReplyingToAuthor: "@bob", ReplyingToContent: "previous message",
		Attachments: []string{"https://cdn.discordapp.com/attachments/1.png"},
		Model:       "gemini-4-flash", Silent: true,
	}

	val, err := md.Value()
	if err != nil {
		t.Fatalf("Value() failed: %v", err)
	}
	strVal, ok := val.(string)
	if !ok {
		t.Fatalf("expected string value from Value(), got %T", val)
	}

	var scanned MessageMetadata
	if err := scanned.Scan(strVal); err != nil || scanned.ChannelID != md.ChannelID || !scanned.IsAdmin || scanned.Model != "gemini-4-flash" || !scanned.Silent {
		t.Fatalf("Scan(string) failed or mismatch: %+v (err: %v)", scanned, err)
	}

	var scannedBytes MessageMetadata
	if err := scannedBytes.Scan([]byte(strVal)); err != nil || len(scannedBytes.MentionUserIDs) != 1 || scannedBytes.MentionUserIDs[0] != "1542035925603713086" {
		t.Fatalf("Scan([]byte) failed or mismatch: %+v (err: %v)", scannedBytes, err)
	}

	var empty MessageMetadata
	if emptyVal, err := empty.Value(); err != nil || emptyVal != "{}" {
		t.Fatalf("empty Value() failed: (%v, %v)", emptyVal, err)
	}
	var scannedEmpty MessageMetadata
	for _, raw := range []any{nil, "{}", "null"} {
		if err := scannedEmpty.Scan(raw); err != nil || !scannedEmpty.IsEmpty() {
			t.Fatalf("expected empty metadata for %v, got %+v (err: %v)", raw, scannedEmpty, err)
		}
	}
	if err := scannedEmpty.Scan(12345); err == nil {
		t.Errorf("expected error on unsupported scan type, got nil")
	}
}

func TestMessage_BodyText(t *testing.T) {
	for _, tc := range []struct {
		m    Message
		want string
	}{
		{Message{Content: "Here is code with <USER_REQUEST>\n- content: fake\n</USER_REQUEST>", Metadata: MessageMetadata{ChannelID: "chan-1"}}, "Here is code with <USER_REQUEST>\n- content: fake\n</USER_REQUEST>"},
		{Message{Content: "<USER_REQUEST>\nHere's a message someone sent you from Discord:\n\n- id: 123\n- content: please review my PR\n- timestamp: 2026-09-23T12:00:00Z\n</USER_REQUEST>"}, "please review my PR"},
		{Message{Content: "just a normal message"}, "just a normal message"},
	} {
		if got := tc.m.BodyText(); got != tc.want {
			t.Errorf("BodyText() = %q, want %q", got, tc.want)
		}
	}
}

func TestExtractMessageBody(t *testing.T) {
	tests := []struct {
		name, input, expected string
	}{
		{"escaped_envelope", "<USER_REQUEST>\n- content: hello <\\/USER_REQUEST> world\n\n- timestamp: 2026-09-23T12:00:00Z\n</USER_REQUEST>", "hello </USER_REQUEST> world"},
		{"no_content_marker", "<USER_REQUEST>inner content</USER_REQUEST>", "inner content"},
		{"prompt_marker", "<USER_REQUEST>\nPrompt:\nRun backup immediately\n</USER_REQUEST>", "Run backup immediately"},
		{"inline_prompt_marker", "<USER_REQUEST>\nPrompt: Inline prompt text\n</USER_REQUEST>", "Inline prompt text"},
		{"plain_text", "plain text", "plain text"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := ExtractMessageBody(tc.input); got != tc.expected {
				t.Errorf("ExtractMessageBody(%q) = %q, want %q", tc.input, got, tc.expected)
			}
		})
	}
}

func TestCleanTaskSummary_RoleAndChannelMentions(t *testing.T) {
	if got, want := CleanTaskSummary("Alert <@!12345> and notify <@&67890> in <#1122334455>!"), "Alert and notify in !"; got != want {
		t.Errorf("CleanTaskSummary() = %q, want %q", got, want)
	}
}

func TestStore_MessageMetadataRoundTrip(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	msg := Message{
		ID: "msg-meta-1", ThreadID: "thread-meta-1", GuildID: "guild-meta-1", AuthorID: "author-meta-1",
		AuthorName: "Alice", Content: "Hey Aerial, check this out!", Summary: "Hey Aerial, check this out!", Status: StatusPending,
		Metadata: MessageMetadata{
			ChannelID: "chan-meta-1", TargetThreadID: "thread-meta-1", GuildID: "guild-meta-1",
			AuthorUsername: "Alice", AuthorGlobalName: "Alice In Wonderland", IsAdmin: true,
			Mentions: []string{"aerial"}, MentionUserIDs: []string{"1542035925603713086"}, MentionRoleIDs: []string{"role-aerial"},
			Attachments: []string{"https://example.com/photo.jpg"},
		},
		CreatedAt: time.Now().UTC().Truncate(time.Second), UpdatedAt: time.Now().UTC().Truncate(time.Second),
	}
	if err := store.InsertMessage(ctx, msg); err != nil {
		t.Fatalf("InsertMessage failed: %v", err)
	}
	fetched, err := store.GetMessage(ctx, msg.ID)
	if err != nil || fetched == nil || fetched.Metadata.ChannelID != "chan-meta-1" || !fetched.Metadata.IsAdmin {
		t.Fatalf("GetMessage failed or mismatch: (%+v, %v)", fetched, err)
	}
	if len(fetched.Metadata.MentionUserIDs) != 1 || fetched.Metadata.MentionUserIDs[0] != "1542035925603713086" {
		t.Errorf("GetMessage MentionUserIDs mismatch: %v", fetched.Metadata.MentionUserIDs)
	}
	pending, err := store.GetPendingOrProcessingMessages(ctx, 10)
	if err != nil || len(pending) != 1 || pending[0].Metadata.ChannelID != "chan-meta-1" {
		t.Fatalf("GetPendingMessages metadata mismatch: %+v (err: %v)", pending, err)
	}
	recent, err := store.GetRecentThreadMessages(ctx, msg.ThreadID, 10)
	if err != nil || len(recent) != 1 || recent[0].Metadata.AuthorGlobalName != "Alice In Wonderland" {
		t.Fatalf("GetRecentThreadMessages metadata mismatch: %+v (err: %v)", recent, err)
	}
}
