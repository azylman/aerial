package db

import (
	"context"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/pgutil"
)

var (
	formatPGTextArray = pgutil.FormatPGTextArray
	parsePGTextArray  = pgutil.ParsePGTextArray
)

type pgStringSlice = pgutil.PGStringSlice

func TestMessage_HasStructuredFields(t *testing.T) {
	if (Message{}).HasStructuredFields() {
		t.Errorf("expected empty Message to return false for HasStructuredFields")
	}
	tVal := true
	fVal := false
	tests := []struct {
		name string
		msg  Message
	}{
		{"channel_id", Message{ChannelID: "c1"}},
		{"channel_name", Message{ChannelName: "aerial-dev"}},
		{"thread_name", Message{ThreadName: "docker-orchestration"}},
		{"author_global_name", Message{AuthorGlobalName: "Alice B"}},
		{"author_bot", Message{AuthorBot: true}},
		{"is_admin", Message{IsAdmin: true}},
		{"mentions", Message{Mentions: []string{"aerial"}}},
		{"mention_user_ids", Message{MentionUserIDs: []string{"123"}}},
		{"mention_role_ids", Message{MentionRoleIDs: []string{"456"}}},
		{"replying_to_author", Message{ReplyingToAuthor: "@bob"}},
		{"replying_to_content", Message{ReplyingToContent: "hello"}},
		{"attachments", Message{Attachments: []string{"http://example.com/img.png"}}},
		{"model", Message{Model: "gemini-4-flash"}},
		{"silent", Message{Silent: true}},
		{"ephemeral_true", Message{Ephemeral: &tVal}},
		{"ephemeral_false", Message{Ephemeral: &fVal}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if !tc.msg.HasStructuredFields() {
				t.Errorf("expected message with %s to return true for HasStructuredFields", tc.name)
			}
		})
	}
}

func TestPGTextArray_FormatAndParse(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		expected string
	}{
		{"empty", nil, "{}"},
		{"empty_slice", []string{}, "{}"},
		{"single", []string{"hello"}, "{\"hello\"}"},
		{"multiple", []string{"a", "b", "c"}, "{\"a\",\"b\",\"c\"}"},
		{"special_characters", []string{"comma,here", "quote\"here", "back\\slash"}, "{\"comma,here\",\"quote\\\"here\",\"back\\\\slash\"}"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			formatted := formatPGTextArray(tc.input)
			if formatted != tc.expected {
				t.Errorf("formatPGTextArray() = %q, want %q", formatted, tc.expected)
			}
			parsed, err := parsePGTextArray(formatted)
			if err != nil {
				t.Fatalf("parsePGTextArray() error: %v", err)
			}
			wantLen := len(tc.input)
			if tc.input == nil {
				wantLen = 0
			}
			if len(parsed) != wantLen {
				t.Fatalf("parsed slice length = %d, want %d", len(parsed), wantLen)
			}
			for i, v := range tc.input {
				if parsed[i] != v {
					t.Errorf("parsed[%d] = %q, want %q", i, parsed[i], v)
				}
			}
		})
	}
}

func TestPGStringSlice_Scan(t *testing.T) {
	var target []string
	scanner := &pgStringSlice{Dest: &target}

	for _, tc := range []struct {
		name    string
		input   any
		wantLen int
	}{
		{"nil", nil, 0},
		{"empty_string", "{}", 0},
		{"null_string", "NULL", 0},
		{"bytes", []byte("{\"foo\",\"bar\"}"), 2},
		{"string_slice", []string{"x", "y"}, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := scanner.Scan(tc.input); err != nil {
				t.Fatalf("Scan(%v) failed: %v", tc.input, err)
			}
			if len(target) != tc.wantLen {
				t.Errorf("scanned len = %d, want %d", len(target), tc.wantLen)
			}
		})
	}
	if err := scanner.Scan(12345); err == nil {
		t.Errorf("expected error scanning int, got nil")
	}
}

func TestMessage_BodyText(t *testing.T) {
	for _, tc := range []struct {
		m    Message
		want string
	}{
		{Message{Content: "Here is code with <USER_REQUEST>\n- content: fake\n</USER_REQUEST>", ChannelID: "chan-1"}, "Here is code with <USER_REQUEST>\n- content: fake\n</USER_REQUEST>"},
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

func TestStore_MessageFlattenedFieldsRoundTrip(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	msg := Message{
		ID: "msg-meta-1", ThreadID: "thread-meta-1", GuildID: "guild-meta-1", AuthorID: "author-meta-1",
		AuthorName: "Alice", Content: "Hey Aerial, check this out!", Summary: "Hey Aerial, check this out!", Status: StatusPending,
		ChannelID: "chan-meta-1", ThreadName: "thread-meta-1",
		AuthorGlobalName: "Alice In Wonderland", IsAdmin: true,
		Mentions: []string{"aerial"}, MentionUserIDs: []string{"1542035925603713086"}, MentionRoleIDs: []string{"role-aerial"},
		Attachments: []string{"https://example.com/photo.jpg"},
		CreatedAt:   time.Now().UTC().Truncate(time.Second), UpdatedAt: time.Now().UTC().Truncate(time.Second),
	}
	if err := store.InsertMessage(ctx, msg); err != nil {
		t.Fatalf("InsertMessage failed: %v", err)
	}
	fetched, err := store.GetMessage(ctx, msg.ID)
	if err != nil || fetched == nil || fetched.ChannelID != "chan-meta-1" || !fetched.IsAdmin {
		t.Fatalf("GetMessage failed or mismatch: (%+v, %v)", fetched, err)
	}
	if len(fetched.MentionUserIDs) != 1 || fetched.MentionUserIDs[0] != "1542035925603713086" {
		t.Errorf("GetMessage MentionUserIDs mismatch: %v", fetched.MentionUserIDs)
	}
	pending, err := store.GetPendingOrProcessingMessages(ctx, 10)
	if err != nil || len(pending) != 1 || pending[0].ChannelID != "chan-meta-1" {
		t.Fatalf("GetPendingMessages metadata mismatch: %+v (err: %v)", pending, err)
	}
	recent, err := store.GetRecentThreadMessages(ctx, msg.ThreadID, 10)
	if err != nil || len(recent) != 1 || recent[0].AuthorGlobalName != "Alice In Wonderland" {
		t.Fatalf("GetRecentThreadMessages metadata mismatch: %+v (err: %v)", recent, err)
	}
}
