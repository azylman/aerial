package db

import (
	"strings"
	"testing"
)

func TestResolveTranscriptStack(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name            string
		databaseURL     string
		configuredStack string
		want            string
		wantErr         string
	}{
		{name: "empty stack derives from database url path", databaseURL: "postgres://u:p@h:5432/alpha?sslmode=disable", want: "alpha"},
		{name: "empty stack derives from sqlite memory dsn", databaseURL: ":memory:", want: "memory"},
		{name: "empty stack derives from file path dsn", databaseURL: "/tmp/fixture/local.db", want: "local"},
		{name: "empty stack derives from sqlite dsn with query params", databaseURL: "/tmp/fixture/local.db?mode=ro", want: "local"},
		{name: "explicit stack overrides database url path", databaseURL: "postgres://u:p@h:5432/beta", configuredStack: " custom ", want: "custom"},
		{name: "explicit stack without database path", databaseURL: "postgres://u:p@h:5432", configuredStack: "solo", want: "solo"},
		{name: "empty url and empty stack errors", databaseURL: "  ", wantErr: "database URL cannot be empty"},
		{name: "unparseable url errors", databaseURL: "postgres://u:p@h:bad port/alpha", wantErr: "parse database URL"},
		{name: "no database name and empty stack errors", databaseURL: "postgres://u:p@h:5432", wantErr: "cannot derive stack"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := ResolveTranscriptStack(tc.databaseURL, tc.configuredStack)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("got %q err=%v, want %q", got, err, tc.want)
			}
		})
	}
}
