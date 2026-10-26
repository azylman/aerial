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
		Port:        "0",
		DatabaseURL: "sqlite://" + dbPath,
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

func TestRoutes_Health(t *testing.T) {
	t.Parallel()
	store := setUpTestStore(t)
	handler := setupRoutes(&Config{Port: "4019"}, store)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("GET /health status = %d, want %d", rec.Code, http.StatusOK)
	}
	if !bytes.Contains(rec.Body.Bytes(), []byte(`"status":"ok"`)) {
		t.Errorf("unexpected health response body: %s", rec.Body.String())
	}
}

func TestRoutes_HTTPMethods(t *testing.T) {
	t.Parallel()
	store := setUpTestStore(t)
	handler := setupRoutes(&Config{}, store)

	tests := []struct {
		name   string
		method string
	}{
		{name: "GET method", method: http.MethodGet},
		{name: "POST method", method: http.MethodPost},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequest(tc.method, "/debug/coverage/flush", nil)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusOK && rec.Code != http.StatusInternalServerError {
				t.Errorf("%s /debug/coverage/flush status = %d, want 200 or 500", tc.method, rec.Code)
			}
		})
	}
}

func TestRoutes_DSPPreview(t *testing.T) {
	t.Parallel()
	store := setUpTestStore(t)
	handler := setupRoutes(&Config{}, store)

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "valid sequence",
			body:       `{"sequence":"12w3"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid sequence character",
			body:       `{"sequence":"Z"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "malformed JSON",
			body:       `{not-valid-json`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequest(http.MethodPost, "/internal/dsp-preview", bytes.NewBufferString(tc.body))
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("POST /internal/dsp-preview status = %d, want %d", rec.Code, tc.wantStatus)
			}
		})
	}
}

func TestRoutes_StorePreview(t *testing.T) {
	t.Parallel()
	store := setUpTestStore(t)
	handler := setupRoutes(&Config{}, store)

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "default action",
			body:       `{"action":""}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "malformed JSON",
			body:       `{invalid-json`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "validate_and_build valid",
			body: `{
				"action": "validate_and_build",
				"to_raw": "(510) 555-0199",
				"from_number": "+15105550100",
				"message": "Hello",
				"brief": {"task": "Schedule dinner"}
			}`,
			wantStatus: http.StatusOK,
		},
		{
			name: "validate_and_build invalid phone",
			body: `{
				"action": "validate_and_build",
				"to_raw": "invalid",
				"brief": {"task": "Schedule dinner"}
			}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "list_calls",
			body:       `{"action":"list_calls"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "list_messages",
			body:       `{"action":"list_messages"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "recent_outbound",
			body:       `{"action":"recent_outbound","to_raw":"+15105550199"}`,
			wantStatus: http.StatusOK,
		},
		{
			name: "demo_lifecycle",
			body: `{
				"action": "demo_lifecycle",
				"call_id": "call-preview-1",
				"provider_sid": "CA_prev_1",
				"message_sid": "SM_prev_1",
				"voice_msg": {
					"message_sid": "SM_prev_1",
					"direction": "inbound",
					"from_number": "+15105550199",
					"to_number": "+15105550100",
					"body": "test msg"
				}
			}`,
			wantStatus: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequest(http.MethodPost, "/internal/store-preview", bytes.NewBufferString(tc.body))
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("POST /internal/store-preview (%s) status = %d, want %d (body: %s)", tc.name, rec.Code, tc.wantStatus, rec.Body.String())
			}
		})
	}
}

func TestRoutes_StorePreview_Errors(t *testing.T) {
	t.Parallel()
	db, _ := sql.Open("sqlite", ":memory:")
	store := NewPGStore(db)
	_ = db.Close()
	handler := setupRoutes(&Config{}, store)

	actions := []string{"list_calls", "list_messages", "recent_outbound", "demo_lifecycle"}
	for _, action := range actions {
		req := httptest.NewRequest(http.MethodPost, "/internal/store-preview", bytes.NewBufferString(`{"action":"`+action+`"}`))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusInternalServerError {
			t.Errorf("action %q on closed store status = %d, want %d", action, rec.Code, http.StatusInternalServerError)
		}
	}
}

func TestMain_Execution(t *testing.T) {
	origLoad, origRun, origExit := loadConfigFn, runAppFn, exitFn
	defer func() {
		loadConfigFn, runAppFn, exitFn = origLoad, origRun, origExit
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

func TestRoutes_BridgePreview(t *testing.T) {
	t.Parallel()
	store := setUpTestStore(t)
	handler := setupRoutes(&Config{}, store)

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "default action",
			body:       `{"action":""}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "malformed JSON",
			body:       `{malformed`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "validate signature",
			body:       `{"action":"validate_signature","auth_token":"token","full_url":"https://test","form":{"CallSid":"CA123"},"signature":"sig"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "stream token",
			body:       `{"action":"stream_token","call_id":"c123","ttl_seconds":30}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "twiml",
			body:       `{"action":"twiml","ws_url":"wss://test","greeting":"hello","callback_url":"https://cb"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "amd eval",
			body:       `{"action":"amd_eval","rms":0.05,"elapsed_ms":20,"answered_by":"human"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "consult",
			body:       `{"action":"consult","query":"test"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "session mgr",
			body:       `{"action":"session_mgr","call_id":"c123","steer_message":"msg","steer_mode":"guidance","provider_sid":"CA123","answered_by":"human"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "twilio rest",
			body:       `{"action":"twilio_rest","to_number":"+15105550199","from_number":"+15105550100","twiml_url":"https://test","provider_sid":"CA123","sms_body":"hi","area_code":"510","callback_url":"https://cb","recording_url":"https://api.twilio.com/rec"}`,
			wantStatus: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequest(http.MethodPost, "/internal/bridge-preview", bytes.NewBufferString(tc.body))
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("POST /internal/bridge-preview status = %d, want %d", rec.Code, tc.wantStatus)
			}
		})
	}
}
