package db

import (
	"testing"
)

func TestDBInitializationAndConversationMapping(t *testing.T) {
	database, err := InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize in-memory SQLite database: %v", err)
	}
	defer database.Close()

	internalID, err := GetInternalConversationID(database, "ext-12345")
	if err != nil {
		t.Fatalf("Error querying non-existent external conversation ID: %v", err)
	}
	if internalID != "" {
		t.Errorf("Expected empty internal ID for non-existent record, got: %s", internalID)
	}

	extID := "discord-thread-999"
	intID := "conv-uuid-abc"
	if err := SaveConversationMapping(database, extID, intID); err != nil {
		t.Fatalf("Failed to save conversation mapping: %v", err)
	}

	gotInternal, err := GetInternalConversationID(database, extID)
	if err != nil {
		t.Fatalf("Error retrieving internal conversation ID: %v", err)
	}
	if gotInternal != intID {
		t.Errorf("Expected internal ID %s, got: %s", intID, gotInternal)
	}

	gotExternal, err := GetExternalConversationID(database, intID)
	if err != nil {
		t.Fatalf("Error retrieving external conversation ID: %v", err)
	}
	if gotExternal != extID {
		t.Errorf("Expected external ID %s, got: %s", extID, gotExternal)
	}

	updatedIntID := "conv-uuid-updated"
	if err := SaveConversationMapping(database, extID, updatedIntID); err != nil {
		t.Fatalf("Failed to update conversation mapping: %v", err)
	}

	gotUpdated, err := GetInternalConversationID(database, extID)
	if err != nil {
		t.Fatalf("Error retrieving updated conversation ID: %v", err)
	}
	if gotUpdated != updatedIntID {
		t.Errorf("Expected updated internal ID %s, got: %s", updatedIntID, gotUpdated)
	}
}

func TestDBNilHandling(t *testing.T) {
	internalID, err := GetInternalConversationID(nil, "ext-123")
	if err != nil {
		t.Errorf("Expected nil error for nil db, got: %v", err)
	}
	if internalID != "" {
		t.Errorf("Expected empty result for nil db, got: %s", internalID)
	}

	externalID, err := GetExternalConversationID(nil, "int-123")
	if err != nil {
		t.Errorf("Expected nil error for nil db, got: %v", err)
	}
	if externalID != "" {
		t.Errorf("Expected empty result for nil db, got: %s", externalID)
	}

	if err := SaveConversationMapping(nil, "ext", "int"); err != nil {
		t.Errorf("Expected nil error for nil db save, got: %v", err)
	}
}
