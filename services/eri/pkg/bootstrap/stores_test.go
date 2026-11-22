package bootstrap

import (
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/azylman/aerial/services/eri/pkg/config"
)

func openLazyPostgres(*config.Config) (*sql.DB, error) {
	return sql.Open("pgx", "postgres://localhost/test")
}

func failOpen(*config.Config) (*sql.DB, error) {
	return nil, errors.New("dial refused")
}

func TestInitDefaultStores(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		failOpen bool
		dbURL    string
		stack    string
		wantErr  string
	}{
		{name: "open error", failOpen: true, wantErr: "failed to initialize database: dial refused"},
		{name: "transcript stack error", wantErr: "failed to resolve transcript stack"},
		{name: "success", dbURL: "postgres://localhost/test", stack: "aerial-test"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			open := openLazyPostgres
			if tc.failOpen {
				open = failOpen
			}
			cfg := config.NewFromData(&config.ConfigData{
				DatabaseURL: tc.dbURL,
				Transcripts: config.TranscriptsConfig{Stack: tc.stack},
			})
			store, txStore, cleanup, err := InitDefaultStores(open, cfg, cfg.Current(), "")
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want containing %q", err, tc.wantErr)
				}
				if store != nil || txStore != nil || cleanup != nil {
					t.Fatalf("expected nil results on error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if store == nil || txStore == nil || cleanup == nil {
				t.Fatalf("expected store, transcript store and cleanup")
			}
			cleanup()
		})
	}
}
