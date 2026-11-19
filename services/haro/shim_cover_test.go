package main

import (
	"testing"

	"github.com/azylman/aerial/services/haro/internal/store"
	"github.com/azylman/aerial/services/haro/internal/testutil"
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
			db, err := testutil.OpenSQLiteTestDB(":memory:")
			if err != nil {
				t.Fatalf("OpenSQLiteTestDB failed: %v", err)
			}
			defer db.Close()
			switch tc.call {
			case "list":
				if _, err := store.ListEventTriggers(nil, db, ""); err != nil {
					t.Errorf("store.ListEventTriggers(nil ctx) err = %v", err)
				}
			default:
				if _, err := store.DeleteEventTrigger(nil, db, "missing"); err != nil {
					t.Errorf("store.DeleteEventTrigger(nil ctx) err = %v", err)
				}
			}
		})
	}
}
