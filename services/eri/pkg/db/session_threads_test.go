package db

import (
	"context"
	"testing"
)

func TestResolveThreadForSession(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		thread  string
		session string
		rotate  string
		query   string
		want    string
	}{
		{name: "empty session resolves empty", query: "   ", want: ""},
		{name: "unknown session resolves empty", query: "nope", want: ""},
		{name: "direct session resolves thread", thread: "123", session: "sess-a", query: "sess-a", want: "discord-123"},
		{name: "previous session resolves thread", thread: "456", session: "sess-old", rotate: "sess-new", query: "sess-old", want: "discord-456"},
		{name: "current session after rotate resolves thread", thread: "456", session: "sess-old", rotate: "sess-new", query: "sess-new", want: "discord-456"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			database := setupTestDB(t)
			defer database.Close()
			store := &SQLStore{db: database}
			ctx := context.Background()
			if tc.session != "" {
				mustOK(t, store.SaveSessionID(ctx, tc.thread, tc.session), "SaveSessionID")
			}
			if tc.rotate != "" {
				mustOK(t, store.RotateSessionID(ctx, tc.thread, tc.rotate), "RotateSessionID")
			}
			got, err := store.ResolveThreadForSession(ctx, tc.query)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("ResolveThreadForSession(%q) = %q, want %q", tc.query, got, tc.want)
			}
		})
	}
}

func TestResolveThreadForSessionNilDB(t *testing.T) {
	t.Parallel()
	store := &SQLStore{}
	if _, err := store.ResolveThreadForSession(context.Background(), "sess-a"); err == nil {
		t.Fatalf("expected error on nil database, got nil")
	}
}
