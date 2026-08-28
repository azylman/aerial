package main

import (
	"testing"
)

func TestDBInitializationAndConversationMapping(t *testing.T) {
	// 1. Test initDB with in-memory SQLite database
	db, err := initDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize in-memory SQLite database: %v", err)
	}
	defer db.Close()

	// 2. Test lookup on non-existent external ID
	internalID, err := getInternalConversationID(db, "ext-12345")
	if err != nil {
		t.Fatalf("Error querying non-existent external conversation ID: %v", err)
	}
	if internalID != "" {
		t.Errorf("Expected empty internal ID for non-existent record, got: %s", internalID)
	}

	// 3. Test saving a conversation mapping
	extID := "discord-thread-999"
	intID := "conv-uuid-abc"
	if err := saveConversationMapping(db, extID, intID); err != nil {
		t.Fatalf("Failed to save conversation mapping: %v", err)
	}

	// 4. Test retrieving internal ID by external ID
	gotInternal, err := getInternalConversationID(db, extID)
	if err != nil {
		t.Fatalf("Error retrieving internal conversation ID: %v", err)
	}
	if gotInternal != intID {
		t.Errorf("Expected internal ID %s, got: %s", intID, gotInternal)
	}

	// 5. Test retrieving external ID by internal ID
	gotExternal, err := getExternalConversationID(db, intID)
	if err != nil {
		t.Fatalf("Error retrieving external conversation ID: %v", err)
	}
	if gotExternal != extID {
		t.Errorf("Expected external ID %s, got: %s", extID, gotExternal)
	}

	// 6. Test updating existing mapping (upsert behavior)
	updatedIntID := "conv-uuid-updated"
	if err := saveConversationMapping(db, extID, updatedIntID); err != nil {
		t.Fatalf("Failed to update conversation mapping: %v", err)
	}

	gotUpdated, err := getInternalConversationID(db, extID)
	if err != nil {
		t.Fatalf("Error retrieving updated conversation ID: %v", err)
	}
	if gotUpdated != updatedIntID {
		t.Errorf("Expected updated internal ID %s, got: %s", updatedIntID, gotUpdated)
	}
}

func TestDBNilHandling(t *testing.T) {
	internalID, err := getInternalConversationID(nil, "ext-123")
	if err != nil {
		t.Errorf("Expected nil error for nil db, got: %v", err)
	}
	if internalID != "" {
		t.Errorf("Expected empty result for nil db, got: %s", internalID)
	}

	externalID, err := getExternalConversationID(nil, "int-123")
	if err != nil {
		t.Errorf("Expected nil error for nil db, got: %v", err)
	}
	if externalID != "" {
		t.Errorf("Expected empty result for nil db, got: %s", externalID)
	}

	if err := saveConversationMapping(nil, "ext", "int"); err != nil {
		t.Errorf("Expected nil error for nil db save, got: %v", err)
	}
}
