package db

import (
	"context"
	"testing"
	"time"
)

func TestMessageMetadata_IsEmpty(t *testing.T) {
	var empty MessageMetadata
	if !empty.IsEmpty() {
		t.Errorf("expected empty metadata to return true for IsEmpty")
	}

	tests := []struct {
		name string
		md   MessageMetadata
	}{
		{"channel_id", MessageMetadata{ChannelID: "c1"}},
		{"target_thread_id", MessageMetadata{TargetThreadID: "t1"}},
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
		ChannelID:         "chan-1",
		TargetThreadID:    "thread-1",
		GuildID:           "guild-1",
		AuthorUsername:    "arcane103",
		AuthorGlobalName:  "Arcane",
		AuthorBot:         false,
		IsAdmin:           true,
		Mentions:          []string{"aerial"},
		MentionUserIDs:    []string{"1542035925603713086"},
		MentionRoleIDs:    []string{"role-1"},
		ReplyingToAuthor:  "@bob",
		ReplyingToContent: "previous message",
		Attachments:       []string{"https://cdn.discordapp.com/attachments/1.png"},
	}

	val, err := md.Value()
	if err != nil {
		t.Fatalf("Value() failed: %v", err)
	}

	strVal, ok := val.(string)
	if !ok {
		t.Fatalf("expected string value from Value(), got %T", val)
	}

	// Scan into new metadata
	var scanned MessageMetadata
	if err := scanned.Scan(strVal); err != nil {
		t.Fatalf("Scan(string) failed: %v", err)
	}
	if scanned.ChannelID != md.ChannelID || scanned.AuthorUsername != md.AuthorUsername || !scanned.IsAdmin {
		t.Errorf("scanned metadata mismatch: %+v", scanned)
	}

	// Scan []byte
	var scannedBytes MessageMetadata
	if err := scannedBytes.Scan([]byte(strVal)); err != nil {
		t.Fatalf("Scan([]byte) failed: %v", err)
	}
	if len(scannedBytes.MentionUserIDs) != 1 || scannedBytes.MentionUserIDs[0] != "1542035925603713086" {
		t.Errorf("scanned bytes metadata mismatch: %+v", scannedBytes)
	}

	// Empty metadata Value and Scan
	var empty MessageMetadata
	emptyVal, err := empty.Value()
	if err != nil {
		t.Fatalf("empty Value() failed: %v", err)
	}
	if emptyVal != "{}" {
		t.Errorf("expected '{}' for empty Value(), got %v", emptyVal)
	}

	var scannedEmpty MessageMetadata
	if err := scannedEmpty.Scan(nil); err != nil {
		t.Fatalf("Scan(nil) failed: %v", err)
	}
	if !scannedEmpty.IsEmpty() {
		t.Errorf("expected empty metadata after Scan(nil)")
	}

	if err := scannedEmpty.Scan("{}"); err != nil {
		t.Fatalf("Scan('{}') failed: %v", err)
	}
	if !scannedEmpty.IsEmpty() {
		t.Errorf("expected empty metadata after Scan('{}')")
	}

	if err := scannedEmpty.Scan("null"); err != nil {
		t.Fatalf("Scan('null') failed: %v", err)
	}
	if !scannedEmpty.IsEmpty() {
		t.Errorf("expected empty metadata after Scan('null')")
	}

	// Unsupported type
	if err := scannedEmpty.Scan(12345); err == nil {
		t.Errorf("expected error on unsupported scan type, got nil")
	}
}

func TestMessage_BodyText(t *testing.T) {
	// Case 1: Structured message with Metadata. Raw content is returned as-is even if it contains <USER_REQUEST>
	mStructured := Message{
		Content: "Here is code with <USER_REQUEST>\n- content: fake\n</USER_REQUEST>",
		Metadata: MessageMetadata{
			ChannelID: "chan-1",
		},
	}
	if mStructured.BodyText() != "Here is code with <USER_REQUEST>\n- content: fake\n</USER_REQUEST>" {
		t.Errorf("expected raw user text preserved when Metadata is present, got %q", mStructured.BodyText())
	}

	// Case 2: Legacy prompt envelope without Metadata. Extracts content
	mLegacy := Message{
		Content: "<USER_REQUEST>\nHere's a message someone sent you from Discord:\n\n- id: 123\n- content: please review my PR\n- timestamp: 2026-09-23T12:00:00Z\n</USER_REQUEST>",
	}
	if mLegacy.BodyText() != "please review my PR" {
		t.Errorf("expected extracted body text for legacy envelope, got %q", mLegacy.BodyText())
	}

	// Case 3: Simple raw text without Metadata
	mRaw := Message{
		Content: "just a normal message",
	}
	if mRaw.BodyText() != "just a normal message" {
		t.Errorf("expected normal content, got %q", mRaw.BodyText())
	}
}

func TestExtractMessageBody_EdgeCases(t *testing.T) {
	// Standard envelope with escaping
	raw := "<USER_REQUEST>\n- content: hello <\\/USER_REQUEST> world\n\n- timestamp: 2026-09-23T12:00:00Z\n</USER_REQUEST>"
	body := ExtractMessageBody(raw)
	if body != "hello </USER_REQUEST> world" {
		t.Errorf("expected unescaped body, got %q", body)
	}

	// Envelope without - content: marker
	rawNoMarker := "<USER_REQUEST>inner content</USER_REQUEST>"
	if b := ExtractMessageBody(rawNoMarker); b != "inner content" {
		t.Errorf("expected 'inner content', got %q", b)
	}

	// Non-envelope text
	plain := "plain text"
	if b := ExtractMessageBody(plain); b != plain {
		t.Errorf("expected %q, got %q", plain, b)
	}
}

func TestStore_MessageMetadataRoundTrip(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	msg := Message{
		ID:         "msg-meta-1",
		ThreadID:   "thread-meta-1",
		GuildID:    "guild-meta-1",
		AuthorID:   "author-meta-1",
		AuthorName: "Alice",
		Content:    "Hey Aerial, check this out!",
		Summary:    "Hey Aerial, check this out!",
		Status:     StatusPending,
		Metadata: MessageMetadata{
			ChannelID:        "chan-meta-1",
			TargetThreadID:   "thread-meta-1",
			GuildID:          "guild-meta-1",
			AuthorUsername:   "Alice",
			AuthorGlobalName: "Alice In Wonderland",
			AuthorBot:        false,
			IsAdmin:          true,
			Mentions:         []string{"aerial"},
			MentionUserIDs:   []string{"1542035925603713086"},
			MentionRoleIDs:   []string{"role-aerial"},
			Attachments:      []string{"https://example.com/photo.jpg"},
		},
		CreatedAt: time.Now().UTC().Truncate(time.Second),
		UpdatedAt: time.Now().UTC().Truncate(time.Second),
	}

	if err := store.InsertMessage(ctx, msg); err != nil {
		t.Fatalf("InsertMessage failed: %v", err)
	}

	// Retrieve by ID
	fetched, err := store.GetMessage(ctx, msg.ID)
	if err != nil {
		t.Fatalf("GetMessage failed: %v", err)
	}
	if fetched == nil {
		t.Fatalf("expected message to exist")
	}
	if fetched.Metadata.ChannelID != "chan-meta-1" || !fetched.Metadata.IsAdmin {
		t.Errorf("GetMessage Metadata mismatch: %+v", fetched.Metadata)
	}
	if len(fetched.Metadata.MentionUserIDs) != 1 || fetched.Metadata.MentionUserIDs[0] != "1542035925603713086" {
		t.Errorf("GetMessage MentionUserIDs mismatch: %v", fetched.Metadata.MentionUserIDs)
	}

	// Retrieve pending
	pending, err := store.GetPendingOrProcessingMessages(ctx, 10)
	if err != nil {
		t.Fatalf("GetPendingOrProcessingMessages failed: %v", err)
	}
	if len(pending) != 1 || pending[0].Metadata.ChannelID != "chan-meta-1" {
		t.Errorf("GetPendingMessages metadata mismatch: %+v", pending)
	}

	// Retrieve recent thread messages
	recent, err := store.GetRecentThreadMessages(ctx, msg.ThreadID, 10)
	if err != nil {
		t.Fatalf("GetRecentThreadMessages failed: %v", err)
	}
	if len(recent) != 1 || recent[0].Metadata.AuthorGlobalName != "Alice In Wonderland" {
		t.Errorf("GetRecentThreadMessages metadata mismatch: %+v", recent)
	}
}
