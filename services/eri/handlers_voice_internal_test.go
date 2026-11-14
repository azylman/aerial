package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db/dbtest"
	"github.com/azylman/aerial/services/eri/pkg/metrics"
	"github.com/azylman/aerial/services/eri/pkg/queue"
	"github.com/bwmarrin/discordgo"
)

type errReadCloser struct {
	reader   io.Reader
	readErr  error
	closeErr error
}

func (e *errReadCloser) Read(p []byte) (int, error) {
	if e.readErr != nil {
		return 0, e.readErr
	}
	if e.reader != nil {
		return e.reader.Read(p)
	}
	return 0, errors.New("simulated network read error")
}

func (e *errReadCloser) Close() error {
	if e.closeErr != nil {
		return e.closeErr
	}
	return nil
}

type mockHTTPResponseWriter struct {
	header http.Header
	code   int
	body   bytes.Buffer
}

func (w *mockHTTPResponseWriter) Header() http.Header {
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}

func (w *mockHTTPResponseWriter) Write(b []byte) (int, error) {
	return w.body.Write(b)
}

func (w *mockHTTPResponseWriter) WriteHeader(statusCode int) {
	w.code = statusCode
}

type flusherErrWriter struct {
	header http.Header
	code   int
}

func (e *flusherErrWriter) Header() http.Header {
	if e.header == nil {
		e.header = make(http.Header)
	}
	return e.header
}

func (e *flusherErrWriter) Write(p []byte) (int, error) {
	return 0, errors.New("simulated write error")
}

func (e *flusherErrWriter) WriteHeader(statusCode int) {
	e.code = statusCode
}

func (e *flusherErrWriter) Flush() {}

func TestHandleVoiceAsk_Validation(t *testing.T) {
	store := dbtest.NewFakeStore()
	pool := newTestWorkerPool(store)
	pool.Start()
	defer pool.Stop()

	handler := handleVoiceAsk(pool)

	tests := []struct {
		name       string
		method     string
		body       io.Reader
		accept     string
		wantStatus int
		wantSubstr string
	}{
		{"GET not allowed", http.MethodGet, nil, "", http.StatusMethodNotAllowed, "Method not allowed"},
		{"empty prompt", http.MethodPost, strings.NewReader(`{"prompt":""}`), "", http.StatusBadRequest, "'prompt' field is required"},
		{"malformed JSON", http.MethodPost, strings.NewReader(`{invalid-json`), "", http.StatusBadRequest, "Invalid JSON payload"},
		{"body read error", http.MethodPost, &errReadCloser{readErr: errors.New("simulated body read error")}, "", http.StatusBadRequest, "Failed to read request body"},
		{"conversation_id fallback", http.MethodPost, strings.NewReader(`{"conversation_id":"conv-123","prompt":"hello"}`), "", http.StatusOK, ""},
		{"missing session_id", http.MethodPost, strings.NewReader(`{"prompt":"hello"}`), "", http.StatusBadRequest, "session_id"},
		{"node_id provided", http.MethodPost, strings.NewReader(`{"prompt":"hello","node_id":"touch-kiosk-kitchen"}`), "", http.StatusOK, ""},
		{"node_id priority", http.MethodPost, strings.NewReader(`{"prompt":"hello","node_id":"touch-kiosk-kitchen","session_id":"turn-123","conversation_id":"conv-456"}`), "", http.StatusOK, ""},
		{"oversized device ID", http.MethodPost, strings.NewReader(fmt.Sprintf(`{"prompt":"hello","node_id":%q}`, strings.Repeat("a", 129))), "", http.StatusBadRequest, "exceeds 128 characters"},
		{"control characters in device ID", http.MethodPost, strings.NewReader(`{"prompt":"hello","node_id":"dock\ninvalid"}`), "", http.StatusBadRequest, "contains invalid characters"},
		{"body close error handled", http.MethodPost, &errReadCloser{reader: strings.NewReader(`{"prompt":"hello","session_id":"kiosk"}`), closeErr: errors.New("simulated close error")}, "", http.StatusOK, ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := runHTTP(handler, tc.method, "/voice/ask", tc.body, "Accept", tc.accept)
			if w.Code != tc.wantStatus {
				t.Fatalf("got status %d, want %d (body: %s)", w.Code, tc.wantStatus, w.Body.String())
			}
			if tc.wantSubstr != "" && !strings.Contains(w.Body.String(), tc.wantSubstr) {
				t.Errorf("expected substring %q in body, got: %s", tc.wantSubstr, w.Body.String())
			}
			if tc.method != http.MethodPost && w.Header().Get("Allow") != "POST" {
				t.Errorf("expected Allow: POST header, got %q", w.Header().Get("Allow"))
			}
		})
	}

	t.Run("non-flusher response writer for SSE", func(t *testing.T) {
		reqSSE := httptest.NewRequest(http.MethodPost, "/voice/ask", strings.NewReader(`{"prompt":"hello","session_id":"kiosk"}`))
		reqSSE.Header.Set("Accept", "text/event-stream")
		nonFlusher := &mockHTTPResponseWriter{}
		handler(nonFlusher, reqSSE)
		if nonFlusher.code != http.StatusInternalServerError {
			t.Errorf("expected 500 for non-flusher SSE, got %d", nonFlusher.code)
		}
	})
}

func newVoiceHandler(cfg queue.WorkerPoolConfig) (http.HandlerFunc, func()) {
	if cfg.Store == nil {
		cfg.Store = dbtest.NewFakeStore()
	}
	pool := queue.New(nil, cfg)
	pool.Start()
	return handleVoiceAsk(pool), pool.Stop
}

func runVoiceAskSSESuccess(t *testing.T) {
	t.Helper()
	handler, stop := newVoiceHandler(queue.WorkerPoolConfig{
		VoiceRunnerFunc: func(ctx context.Context, prompt, sessionID string, onStatus func(string)) (string, string, error) {
			if onStatus != nil {
				onStatus("⚡ Checking smart lights...")
			}
			return "Lights have been turned off.", "sess-test-456", nil
		},
	})
	defer stop()

	w := runHTTP(handler, http.MethodPost, "/voice/ask", strings.NewReader(`{"prompt":"turn off the lights","session_id":"sess-test-456"}`), "Accept", "text/event-stream")
	if w.Code != http.StatusOK || !strings.Contains(w.Header().Get("Content-Type"), "text/event-stream") || w.Header().Get("X-Accel-Buffering") != "no" {
		t.Fatalf("unexpected headers or status: code=%d, ct=%s", w.Code, w.Header().Get("Content-Type"))
	}
	body := w.Body.String()
	for _, want := range []string{
		"event: status\ndata: {\"status\":\"⚡ Checking smart lights...\"}\n\n",
		"event: reply\ndata: {\"conversation_id\":\"sess-test-456\",\"reply\":\"Lights have been turned off.\"}\n\n",
		"event: done\ndata: {\"conversation_id\":\"sess-test-456\"}\n\n",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing SSE event %q in body:\n%s", want, body)
		}
	}
}

func runVoiceAskSSESentenceStreaming(t *testing.T) {
	t.Helper()
	handler, stop := newVoiceHandler(queue.WorkerPoolConfig{
		VoiceStreamRunnerFunc: func(ctx context.Context, prompt, sessionID string, onStatus func(string), onSentence func(string)) (string, string, error) {
			if onStatus != nil {
				onStatus("⚡ Checking status...")
			}
			if onSentence != nil {
				onSentence("Here is sentence one.")
				onSentence("And here is sentence two.")
			}
			return "Here is sentence one. And here is sentence two.", "sess-stream-789", nil
		},
	})
	defer stop()

	w := runHTTP(handler, http.MethodPost, "/voice/ask", strings.NewReader(`{"prompt":"tell me two things","session_id":"sess-stream-789"}`), "Accept", "text/event-stream")
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", w.Code)
	}
	body := w.Body.String()
	for _, want := range []string{
		"event: status\ndata: {\"status\":\"⚡ Checking status...\"}\n\n",
		"event: sentence\ndata: {\"text\":\"Here is sentence one.\"}\n\n",
		"event: sentence\ndata: {\"text\":\"And here is sentence two.\"}\n\n",
		"event: reply\ndata: {\"conversation_id\":\"sess-stream-789\",\"reply\":\"Here is sentence one. And here is sentence two.\"}\n\n",
		"event: done\ndata: {\"conversation_id\":\"sess-stream-789\"}\n\n",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing SSE event %q in body:\n%s", want, body)
		}
	}
	idxSent1 := strings.Index(body, "event: sentence\ndata: {\"text\":\"Here is sentence one.\"}")
	idxSent2 := strings.Index(body, "event: sentence\ndata: {\"text\":\"And here is sentence two.\"}")
	idxReply := strings.Index(body, "event: reply\ndata: {\"conversation_id\":\"sess-stream-789\"")
	if idxSent1 == -1 || idxSent2 == -1 || idxReply == -1 || idxSent1 > idxSent2 || idxSent2 > idxReply {
		t.Errorf("invalid event ordering: idxSent1=%d, idxSent2=%d, idxReply=%d", idxSent1, idxSent2, idxReply)
	}
}

func runVoiceAskSSEErrors(t *testing.T) {
	t.Helper()
	hErr, stop := newVoiceHandler(queue.WorkerPoolConfig{
		VoiceRunnerFunc: func(ctx context.Context, prompt, sessionID string, onStatus func(string)) (string, string, error) {
			return "", "", errors.New("deliberation timed out")
		},
	})
	defer stop()

	wJSON := runHTTP(hErr, http.MethodPost, "/voice/ask", strings.NewReader(`{"prompt":"fail","session_id":"sess-err-2"}`), "Accept", "application/json")
	if wJSON.Code != http.StatusInternalServerError || !strings.Contains(wJSON.Body.String(), "deliberation timed out") {
		t.Errorf("expected 500 error, got %d: %s", wJSON.Code, wJSON.Body.String())
	}

	hNil := handleVoiceAsk(nil)
	if w := runHTTP(hNil, http.MethodPost, "/voice/ask", strings.NewReader(`{"prompt":"hello","session_id":"sess-nil"}`)); w.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 for nil pool, got %d", w.Code)
	}
	if w := runHTTP(hNil, http.MethodPost, "/voice/ask", strings.NewReader(`{"prompt":"hello","session_id":"sess-nil-sse"}`), "Accept", "text/event-stream"); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "event: error") {
		t.Errorf("expected 200 SSE error for nil pool, got %d: %s", w.Code, w.Body.String())
	}

	mRec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(mRec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, metric := range []string{
		`aerial_brain_voice_ttfr_duration_seconds_bucket{mode="sse",status="error"`,
		`aerial_brain_voice_ttfr_duration_seconds_bucket{mode="json",status="error"`,
	} {
		if !strings.Contains(mRec.Body.String(), metric) {
			t.Errorf("missing metric %q", metric)
		}
	}
}

func TestHandleVoiceAsk_SSE(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		runVoiceAskSSESuccess(t)
	})

	t.Run("SentenceStreaming", func(t *testing.T) {
		runVoiceAskSSESentenceStreaming(t)
	})

	t.Run("CancelledContext", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		handler, stop := newVoiceHandler(queue.WorkerPoolConfig{
			VoiceRunnerFunc: func(c context.Context, prompt, sessionID string, onStatus func(string)) (string, string, error) {
				cancel()
				if onStatus != nil {
					onStatus("status after cancel")
				}
				return "done", "sess-1", nil
			},
		})
		defer stop()

		req := httptest.NewRequest(http.MethodPost, "/voice/ask", strings.NewReader(`{"prompt":"hello","session_id":"kiosk"}`)).WithContext(ctx)
		req.Header.Set("Accept", "text/event-stream")
		handler(httptest.NewRecorder(), req)
	})

	t.Run("VoiceRunnerError", func(t *testing.T) {
		handler, stop := newVoiceHandler(queue.WorkerPoolConfig{
			VoiceRunnerFunc: func(c context.Context, prompt, sessionID string, onStatus func(string)) (string, string, error) {
				return "", "", errors.New("synthetic voice failure")
			},
		})
		defer stop()

		w := runHTTP(handler, http.MethodPost, "/voice/ask", strings.NewReader(`{"prompt":"hello","session_id":"kiosk"}`), "Accept", "text/event-stream")
		if !strings.Contains(w.Body.String(), "event: error") {
			t.Errorf("expected SSE error event, got %q", w.Body.String())
		}
	})

	t.Run("WriteError", func(t *testing.T) {
		handler, stop := newVoiceHandler(queue.WorkerPoolConfig{
			VoiceRunnerFunc: func(c context.Context, prompt, sessionID string, onStatus func(string)) (string, string, error) {
				return "done", "sess-1", nil
			},
		})
		defer stop()

		req := httptest.NewRequest(http.MethodPost, "/voice/ask", strings.NewReader(`{"prompt":"hello","session_id":"kiosk"}`))
		req.Header.Set("Accept", "text/event-stream")
		handler(&flusherErrWriter{}, req)
	})

	t.Run("JSON_Success", func(t *testing.T) {
		handler, stop := newVoiceHandler(queue.WorkerPoolConfig{
			VoiceRunnerFunc: func(ctx context.Context, prompt, sessionID string, onStatus func(string)) (string, string, error) {
				return "The temperature is 72 degrees.", "sess-json-789", nil
			},
		})
		defer stop()

		w := runHTTP(handler, http.MethodPost, "/voice/ask", strings.NewReader(`{"prompt":"what is the temperature?","session_id":"sess-json-789"}`), "Accept", "application/json")
		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", w.Code)
		}
		var resp VoiceAskResponse
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil || resp.Reply != "The temperature is 72 degrees." || resp.SessionID != "sess-json-789" {
			t.Fatalf("unexpected JSON response: %+v, err: %v", resp, err)
		}
	})

	t.Run("Errors", func(t *testing.T) {
		runVoiceAskSSEErrors(t)
	})
}

func TestHandleInternalDiscordMessage(t *testing.T) {
	setupTestConfig(t, `
model: "gemini-2.5-flash"
channels:
  default:
    mode: "threads"
`)

	tests := []struct {
		name       string
		method     string
		body       string
		closedDB   bool
		wantStatus int
		wantSubstr string
	}{
		{
			name:       "ValidTargetedMessage",
			method:     http.MethodPost,
			body:       `{"bot_user_id":"bot-1","message":{"id":"123456789012345678","channel_id":"ch-1","author":{"id":"u-1","username":"alex"},"content":"hey aerial"}}`,
			wantStatus: http.StatusOK,
			wantSubstr: `"enqueued"`,
		},
		{
			name:       "MissingMessageID",
			method:     http.MethodPost,
			body:       `{"message":{}}`,
			wantStatus: http.StatusBadRequest,
			wantSubstr: "Invalid payload",
		},
		{
			name:       "MissingMessage",
			method:     http.MethodPost,
			body:       `{"bot_user_id":"bot-1"}`,
			wantStatus: http.StatusBadRequest,
			wantSubstr: "Invalid payload",
		},
		{
			name:       "MissingAuthor",
			method:     http.MethodPost,
			body:       `{"message":{"id":"123456789012345678"}}`,
			wantStatus: http.StatusBadRequest,
			wantSubstr: "Invalid payload",
		},
		{
			name:       "MethodNotAllowed",
			method:     http.MethodGet,
			body:       "",
			wantStatus: http.StatusMethodNotAllowed,
			wantSubstr: "Method not allowed",
		},
		{
			name:       "MalformedJSON",
			method:     http.MethodPost,
			body:       `{"message":`,
			wantStatus: http.StatusBadRequest,
			wantSubstr: "Invalid payload",
		},
		{
			name:       "SelfMessageIgnored",
			method:     http.MethodPost,
			body:       `{"bot_user_id":"bot-1","message":{"id":"msg-self-1","channel_id":"ch-1","author":{"id":"bot-1","username":"aerial"},"content":"hello"}}`,
			wantStatus: http.StatusOK,
			wantSubstr: `"ignored_self"`,
		},
		{
			name:       "UntargetedMessageIgnored",
			method:     http.MethodPost,
			body:       `{"bot_user_id":"bot-1","message":{"id":"msg-untargeted-1","channel_id":"ch-1","guild_id":"g-1","author":{"id":"u-2","username":"bob"},"content":"random chatter"}}`,
			wantStatus: http.StatusOK,
			wantSubstr: `"ignored"`,
		},
		{
			name:       "DatabaseErrorReturns500",
			method:     http.MethodPost,
			body:       `{"bot_user_id":"bot-1","message":{"id":"msg-db-err-1","channel_id":"ch-1","author":{"id":"u-1","username":"alex"},"content":"hey aerial"}}`,
			closedDB:   true,
			wantStatus: http.StatusInternalServerError,
			wantSubstr: "Failed to process message",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := dbtest.NewFakeStore()
			if tc.closedDB {
				_ = store.Close()
			}
			pool := newTestWorkerPool(store)
			handler := handleInternalDiscordMessage(store, pool)

			req := httptest.NewRequest(tc.method, "/internal/discord/message", strings.NewReader(tc.body))
			rec := httptest.NewRecorder()
			handler(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body: %s)", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if tc.wantSubstr != "" && !strings.Contains(rec.Body.String(), tc.wantSubstr) {
				t.Errorf("body %q does not contain wantSubstr %q", rec.Body.String(), tc.wantSubstr)
			}
		})
	}
}

func TestHandleInternalDiscordInteraction(t *testing.T) {
	origRouter := activeInteractionRouter.Swap(nil)
	defer activeInteractionRouter.Store(origRouter)

	tests := []struct {
		name           string
		method         string
		body           string
		readErr        bool
		hasPoolSession bool
		wantStatus     int
		wantSubstr     string
	}{
		{
			name:       "ValidDirectInteraction",
			method:     http.MethodPost,
			body:       `{"id":"int-1","type":2,"data":{"name":"status"}}`,
			wantStatus: http.StatusOK,
			wantSubstr: `"ok"`,
		},
		{
			name:       "ValidWrappedInteraction",
			method:     http.MethodPost,
			body:       `{"interaction":{"id":"int-2","type":2,"data":{"name":"status"}}}`,
			wantStatus: http.StatusOK,
			wantSubstr: `"ok"`,
		},
		{
			name:           "WithPoolSession",
			method:         http.MethodPost,
			body:           `{"id":"int-3","type":2,"data":{"name":"status"}}`,
			hasPoolSession: true,
			wantStatus:     http.StatusOK,
			wantSubstr:     `"ok"`,
		},
		{
			name:       "BodyReadError",
			method:     http.MethodPost,
			readErr:    true,
			wantStatus: http.StatusBadRequest,
			wantSubstr: "Failed to read request body",
		},
		{
			name:       "MethodNotAllowed",
			method:     http.MethodGet,
			body:       "",
			wantStatus: http.StatusMethodNotAllowed,
			wantSubstr: "Method not allowed",
		},
		{
			name:       "MalformedJSON",
			method:     http.MethodPost,
			body:       `{"id":`,
			wantStatus: http.StatusBadRequest,
			wantSubstr: "Invalid payload",
		},
		{
			name:       "MissingInteractionID",
			method:     http.MethodPost,
			body:       `{"type":2}`,
			wantStatus: http.StatusBadRequest,
			wantSubstr: "Invalid payload",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := dbtest.NewFakeStore()
			pool := newTestWorkerPool(store)
			if tc.hasPoolSession {
				sess, _ := discordgo.New("Bot fake-token")
				pool.SetDiscordSession(sess)
			}
			handler := handleInternalDiscordInteraction(store, pool)

			var r io.Reader = strings.NewReader(tc.body)
			if tc.readErr {
				r = &errReadCloser{readErr: errors.New("simulated body read error")}
			}
			req := httptest.NewRequest(tc.method, "/internal/discord/interaction", r)
			rec := httptest.NewRecorder()
			handler(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body: %s)", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if tc.wantSubstr != "" && !strings.Contains(rec.Body.String(), tc.wantSubstr) {
				t.Errorf("body %q does not contain wantSubstr %q", rec.Body.String(), tc.wantSubstr)
			}
		})
	}
}

type gitHubProxyCase struct {
	name       string
	cfg        *config.Config
	path       string
	wantStatus int
	wantSubstr string
	wantAuth   string
	wantPath   string
	wantQuery  string
}

func runGitHubProxyTests(t *testing.T, handlerMaker func(*config.Config) http.HandlerFunc, tests []gitHubProxyCase, capturedAuth, capturedPath, capturedQuery *string) {
	t.Helper()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			*capturedAuth, *capturedPath, *capturedQuery = "", "", ""
			h := handlerMaker(tc.cfg)
			w := runHTTP(h, http.MethodGet, tc.path, nil)
			if w.Code != tc.wantStatus {
				t.Fatalf("got status=%d, want %d (body: %s)", w.Code, tc.wantStatus, w.Body.String())
			}
			if tc.wantSubstr != "" && !strings.Contains(w.Body.String(), tc.wantSubstr) {
				t.Fatalf("expected substr %q in body: %s", tc.wantSubstr, w.Body.String())
			}
			if tc.wantAuth != "" && *capturedAuth != tc.wantAuth {
				t.Errorf("captured auth = %q, want %q", *capturedAuth, tc.wantAuth)
			}
			if tc.wantPath != "" && *capturedPath != tc.wantPath {
				t.Errorf("captured path = %q, want %q", *capturedPath, tc.wantPath)
			}
			if tc.wantQuery != "" && *capturedQuery != tc.wantQuery {
				t.Errorf("captured query = %q, want %q", *capturedQuery, tc.wantQuery)
			}
		})
	}
}

func TestHandleInternalGitHubAPIProxy(t *testing.T) {
	var capturedAuth, capturedPath, capturedQuery string
	mockAPI := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedAuth = r.Header.Get("Authorization")
		capturedPath = r.URL.Path
		capturedQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer mockAPI.Close()

	t.Setenv("GITHUB_API_URL", mockAPI.URL)

	cfgWithPAT := config.NewFromData(&config.ConfigData{GitHubPAT: "ghp_mockpat12345"})
	cfgWithoutPAT := config.NewFromData(&config.ConfigData{GitHubPAT: ""})

	tests := []gitHubProxyCase{
		{
			name:       "missing PAT returns 503",
			cfg:        cfgWithoutPAT,
			path:       "/internal/github-api/user",
			wantStatus: http.StatusServiceUnavailable,
			wantSubstr: "GitHub PAT not configured",
		},
		{
			name:       "successful proxy with subpath and query",
			cfg:        cfgWithPAT,
			path:       "/internal/github-api/repos/org/repo/pulls?state=open",
			wantStatus: http.StatusOK,
			wantSubstr: `"status":"ok"`,
			wantAuth:   "token ghp_mockpat12345",
			wantPath:   "/repos/org/repo/pulls",
			wantQuery:  "state=open",
		},
	}

	runGitHubProxyTests(t, handleInternalGitHubAPIProxy, tests, &capturedAuth, &capturedPath, &capturedQuery)

	t.Run("upstream network failure returns 502", func(t *testing.T) {
		t.Setenv("GITHUB_API_URL", "http://127.0.0.1:0")
		h := handleInternalGitHubAPIProxy(cfgWithPAT)
		w := runHTTP(h, http.MethodGet, "/internal/github-api/test", nil)
		if w.Code != http.StatusBadGateway {
			t.Fatalf("expected 502, got %d", w.Code)
		}
	})
}

func TestHandleInternalGitHubGitProxy(t *testing.T) {
	var capturedAuth, capturedPath, capturedQuery string
	mockGit := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedAuth = r.Header.Get("Authorization")
		capturedPath = r.URL.Path
		capturedQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/x-git-upload-pack-advertisement")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("001e# service=git-upload-pack\n0000"))
	}))
	defer mockGit.Close()

	t.Setenv("AERIAL_GITHUB_GIT_UPSTREAM", mockGit.URL)

	cfgWithPAT := config.NewFromData(&config.ConfigData{GitHubPAT: "ghp_gitpat98765"})
	cfgWithoutPAT := config.NewFromData(&config.ConfigData{GitHubPAT: ""})

	tests := []gitHubProxyCase{
		{
			name:       "missing PAT returns 503",
			cfg:        cfgWithoutPAT,
			path:       "/internal/github-git/org/repo.git/info/refs",
			wantStatus: http.StatusServiceUnavailable,
			wantSubstr: "GitHub PAT not configured",
		},
		{
			name:       "successful git proxy with basic auth and query",
			cfg:        cfgWithPAT,
			path:       "/internal/github-git/org/repo.git/info/refs?service=git-upload-pack",
			wantStatus: http.StatusOK,
			wantSubstr: "git-upload-pack",
			wantAuth:   "Basic " + base64.StdEncoding.EncodeToString([]byte("x-access-token:ghp_gitpat98765")),
			wantPath:   "/org/repo.git/info/refs",
			wantQuery:  "service=git-upload-pack",
		},
	}

	runGitHubProxyTests(t, handleInternalGitHubGitProxy, tests, &capturedAuth, &capturedPath, &capturedQuery)

	t.Run("upstream network failure returns 502", func(t *testing.T) {
		t.Setenv("AERIAL_GITHUB_GIT_UPSTREAM", "http://127.0.0.1:0")
		h := handleInternalGitHubGitProxy(cfgWithPAT)
		w := runHTTP(h, http.MethodGet, "/internal/github-git/test", nil)
		if w.Code != http.StatusBadGateway {
			t.Fatalf("expected 502, got %d", w.Code)
		}
	})
}
