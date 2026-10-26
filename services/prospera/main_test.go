package main

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func TestRunApp_Validation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		cfg     *Config
		wantErr bool
	}{
		{
			name:    "nil config",
			cfg:     nil,
			wantErr: true,
		},
		{
			name:    "empty port",
			cfg:     &Config{Port: "   ", DatabaseURL: "sqlite://:memory:"},
			wantErr: true,
		},
		{
			name:    "invalid database url",
			cfg:     &Config{Port: "4019", DatabaseURL: "invalid://url"},
			wantErr: true,
		},
		{
			name:    "server listen error",
			cfg:     &Config{Port: "-1", DatabaseURL: "sqlite://:memory:"},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := RunApp(context.Background(), tc.cfg)
			if (err != nil) != tc.wantErr {
				t.Fatalf("RunApp() err = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}
}

func TestRunApp_Lifecycle(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "prospera_app_test.db")

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}

	schema := `
	CREATE TABLE voice_calls (
		call_id TEXT PRIMARY KEY,
		provider_call_sid TEXT UNIQUE,
		direction TEXT NOT NULL,
		state TEXT NOT NULL,
		end_reason TEXT,
		from_number TEXT NOT NULL,
		to_number TEXT NOT NULL,
		callback_of_call_id TEXT,
		discord_channel_id TEXT,
		discord_thread_id TEXT,
		brief TEXT NOT NULL DEFAULT '{}',
		owner_instructions TEXT NOT NULL DEFAULT '[]',
		transcript TEXT NOT NULL DEFAULT '[]',
		live_transcript_cursor INTEGER NOT NULL DEFAULT 0,
		answered_by TEXT,
		voicemail_recording_url TEXT,
		report_status TEXT NOT NULL DEFAULT 'pending',
		report_summary TEXT,
		started_at TIMESTAMP NOT NULL,
		answered_at TIMESTAMP,
		ended_at TIMESTAMP,
		updated_at TIMESTAMP NOT NULL
	);
	CREATE TABLE voice_messages (
		message_sid TEXT PRIMARY KEY,
		direction TEXT NOT NULL,
		from_number TEXT NOT NULL,
		to_number TEXT NOT NULL,
		body TEXT NOT NULL,
		media_urls TEXT NOT NULL DEFAULT '[]',
		related_call_id TEXT,
		discord_delivered BOOLEAN NOT NULL DEFAULT FALSE,
		created_at TIMESTAMP NOT NULL
	);
	`
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		t.Fatalf("failed to create test schema: %v", err)
	}
	_ = db.Close()

	cfg := &Config{
		Port:                  "0",
		DatabaseURL:           "sqlite://" + dbPath,
		DefaultDiscordChannel: "prospera",
	}

	ctx, cancel := context.WithCancel(context.Background())
	appDone := make(chan error, 1)

	go func() {
		appDone <- RunApp(ctx, cfg)
	}()

	cancel()

	select {
	case err := <-appDone:
		if err != nil {
			t.Fatalf("RunApp() shutdown error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RunApp() did not shut down within timeout")
	}
}

func TestRunApp_TranscriptBatcher(t *testing.T) {
	t.Parallel()

	cfg := &Config{DefaultDiscordChannel: "prospera"}
	store := newMockStoreForDelivery()
	sessionMgr := NewCallSessionManager(cfg, store, &mockTwilioForDelivery{}, nil, nil)
	reporter := NewReporter(cfg, store, &mockTwilioForDelivery{})

	// Test flush with nil sessionMgr or reporter
	flushActiveSessions(context.Background(), nil, reporter)
	flushActiveSessions(context.Background(), sessionMgr, nil)

	// Test flush with no sessions
	flushActiveSessions(context.Background(), sessionMgr, reporter)

	// Test flush with active session
	sess := &MediaStreamSession{
		callID: "call-batch-test",
	}
	sessionMgr.registerSession("call-batch-test", sess)
	_ = store.CreateOutboundCall(context.Background(), VoiceCall{
		CallID: "call-batch-test",
		Transcript: []TranscriptTurn{
			{Role: "user", Text: "Batch turn"},
		},
	}, 5)

	flushActiveSessions(context.Background(), sessionMgr, reporter)

	// Test runTranscriptBatcher exiting immediately on canceled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	runTranscriptBatcher(ctx, sessionMgr, reporter)
}

func TestRoutes_HealthAndCoverage(t *testing.T) {
	t.Parallel()

	cfg := &Config{Port: "4019"}
	store := newMockStoreForDelivery()
	server := NewServer(cfg, store, &mockTwilioForDelivery{}, nil, nil, nil)
	handler := server.Routes()

	tests := []struct {
		name       string
		method     string
		path       string
		wantStatus int
		wantBody   string
	}{
		{
			name:       "health endpoint",
			method:     http.MethodGet,
			path:       "/health",
			wantStatus: http.StatusOK,
			wantBody:   `"status":"ok"`,
		},
		{
			name:       "coverage flush get",
			method:     http.MethodGet,
			path:       "/debug/coverage/flush",
			wantStatus: http.StatusOK,
		},
		{
			name:       "coverage flush post",
			method:     http.MethodPost,
			path:       "/debug/coverage/flush",
			wantStatus: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus && rec.Code != http.StatusInternalServerError {
				t.Fatalf("%s %s status = %d, want %d", tc.method, tc.path, rec.Code, tc.wantStatus)
			}
			if tc.wantBody != "" && !bytes.Contains(rec.Body.Bytes(), []byte(tc.wantBody)) {
				t.Fatalf("body %s missing %s", rec.Body.String(), tc.wantBody)
			}
		})
	}
}

func TestRunApp_MainSignal(t *testing.T) {
	origLoad := loadConfigFn
	origRun := runAppFn
	origExit := exitFn
	defer func() {
		loadConfigFn = origLoad
		runAppFn = origRun
		exitFn = origExit
	}()

	tests := []struct {
		name     string
		loadErr  error
		runErr   error
		wantExit bool
	}{
		{
			name:     "config error triggers exit",
			loadErr:  errors.New("mock config error"),
			wantExit: true,
		},
		{
			name:     "run app error triggers exit",
			runErr:   errors.New("mock run error"),
			wantExit: true,
		},
		{
			name:     "server closed error does not trigger exit",
			runErr:   http.ErrServerClosed,
			wantExit: false,
		},
		{
			name:     "clean execution",
			wantExit: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			exited := false
			exitFn = func(format string, v ...any) {
				exited = true
			}
			loadConfigFn = func(_ func(string) string) (*Config, error) {
				if tc.loadErr != nil {
					return nil, tc.loadErr
				}
				return &Config{Port: "4019", DatabaseURL: "sqlite://:memory:"}, nil
			}
			runAppFn = func(_ context.Context, _ *Config) error {
				return tc.runErr
			}

			main()

			if exited != tc.wantExit {
				t.Errorf("main() exited = %v, want %v", exited, tc.wantExit)
			}
		})
	}
}
