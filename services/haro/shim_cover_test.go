package main

import (
	"testing"

	"github.com/azylman/aerial/services/haro/internal/store"
)

// Regression cover for the root-owned nil-context tolerance on
// event-trigger reads that stays behind the internal/store extraction.
// Heartbeat watermark advancement is covered through the public
// HandleDispatchOutcome path (TestHandleDispatchOutcome_Scenarios).
func TestShim_NilCtxDefaults(t *testing.T) {
	tests := []struct {
		name string
		call string
	}{
		{"list event triggers", "list"},
		{"delete event trigger", "delete"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db, err := OpenDB(t.Context(), &Config{DatabaseURL: ":memory:"})
			if err != nil {
				t.Fatalf("OpenDB failed: %v", err)
			}
			defer db.Close()
			switch tc.call {
			case "list":
				if _, err := ListEventTriggers(nil, db, ""); err != nil {
					t.Errorf("ListEventTriggers(nil ctx) err = %v", err)
				}
			default:
				if _, err := store.DeleteEventTrigger(nil, db, "missing"); err != nil {
					t.Errorf("store.DeleteEventTrigger(nil ctx) err = %v", err)
				}
			}
		})
	}
}
