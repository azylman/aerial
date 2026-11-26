package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/bwmarrin/discordgo"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/eri/internal/restateclient"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/metrics"
	"github.com/azylman/aerial/services/eri/pkg/queue"
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

func TestHandleVoiceAsk_Validation(t *testing.T) {
	pool := newTestWorkerPool(newTestStore(t))
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
		{"missing session_id", http.MethodPost, strings.NewReader(`{"prompt":"hello"}`), "", http.StatusBadRequest, "session_id"},
		{"oversized device ID", http.MethodPost, strings.NewReader(fmt.Sprintf(`{"prompt":"hello","node_id":%q}`, strings.Repeat("a", 129))), "", http.StatusBadRequest, "exceeds 128 characters"},
		{"control characters in device ID", http.MethodPost, strings.NewReader(`{"prompt":"hello","node_id":"dock\ninvalid"}`), "", http.StatusBadRequest, "contains invalid characters"},
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

func seedVoiceTestSession(t *testing.T, store *db.SQLStore, sessionID, seedSession string, seedTurnCount int) {
	t.Helper()
	if seedSession == "" {
		return
	}
	threadKey := "voice-" + strings.TrimSpace(sessionID)
	if err := store.SaveSessionID(context.Background(), threadKey, seedSession); err != nil {
		t.Fatalf("SaveSessionID seed failed: %v", err)
	}
	for range seedTurnCount {
		if _, err := store.IncrementSessionTurnCount(context.Background(), threadKey); err != nil {
			t.Fatalf("IncrementSessionTurnCount seed failed: %v", err)
		}
	}
}

func initTestWorkerPool(t *testing.T, nilPool, nilStore bool, sabotageSQL string) (*queue.WorkerPool, *db.SQLStore) {
	t.Helper()
	if nilPool {
		return nil, nil
	}
	if nilStore {
		p := newTestWorkerPool(nil)
		p.Start()
		return p, nil
	}
	store, database := newTestStoreDB(t)
	if sabotageSQL != "" {
		mustExec(t, database, sabotageSQL)
	}
	p := newTestWorkerPool(store)
	p.Start()
	return p, store
}

func TestPersistVoiceConversationID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		nilPool       bool
		nilStore      bool
		sessionID     string
		convID        string
		seedSession   string
		seedTurnCount int
		sabotageSQL   string
		wantSession   string
		wantTurnCount int
	}{
		{name: "nil pool", nilPool: true, sessionID: "s1", convID: "c1"},
		{name: "nil store in pool", nilStore: true, sessionID: "s1", convID: "c1"},
		{name: "empty sessionID", sessionID: "", convID: "c1"},
		{name: "whitespace sessionID", sessionID: "   ", convID: "c1"},
		{name: "empty convID", sessionID: "s1", convID: ""},
		{name: "whitespace convID", sessionID: "s1", convID: "   "},
		{name: "store error logged", sessionID: "s-err", convID: "c-err", sabotageSQL: failTriggerSQL("sessions", "INSERT", "40001")},
		{name: "success persisted", sessionID: "s-ok", convID: "c-ok", wantSession: "c-ok"},
		{
			name:          "preserves existing turn count",
			sessionID:     "s-count",
			convID:        "c-count",
			seedSession:   "c-count",
			seedTurnCount: 3,
			wantSession:   "c-count",
			wantTurnCount: 3,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pool, store := initTestWorkerPool(t, tc.nilPool, tc.nilStore, tc.sabotageSQL)
			if pool != nil {
				defer pool.Stop()
			}
			if store != nil {
				seedVoiceTestSession(t, store, tc.sessionID, tc.seedSession, tc.seedTurnCount)
			}
			persistVoiceConversationID(context.Background(), pool, tc.sessionID, tc.convID)
			if store == nil {
				return
			}
			threadKey := "voice-" + strings.TrimSpace(tc.sessionID)
			got, err := store.GetSessionID(context.Background(), threadKey)
			if err != nil || got != tc.wantSession {
				t.Fatalf("GetSessionID = %q, err = %v, want %q", got, err, tc.wantSession)
			}
			gotCount, err := store.GetSessionTurnCount(context.Background(), threadKey)
			if err != nil || gotCount != tc.wantTurnCount {
				t.Fatalf("GetSessionTurnCount = %d, err = %v, want %d", gotCount, err, tc.wantTurnCount)
			}
		})
	}
}

func TestHandleInternalDiscordMessage(t *testing.T) {
	setupTestConfig(t, `
model: "gemini-2.5-flash"
channels:
  default:
    mode: "threads"
`)
	pool := newTestWorkerPool(newTestStore(t))

	tests := []struct {
		name       string
		method     string
		body       string
		restateErr bool
		wantStatus int
		wantSubstr string
	}{
		{
			name:       "ValidTargetedMessage",
			method:     http.MethodPost,
			body:       `{"bot_user_id":"bot-1","message":{"id":"123456789012345678","channel_id":"ch-1","author":{"id":"u-1","username":"alice"},"content":"hey aerial"}}`,
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
			name:       "RestateErrorReturns500",
			method:     http.MethodPost,
			body:       `{"bot_user_id":"bot-1","message":{"id":"msg-db-err-1","channel_id":"ch-1","author":{"id":"u-1","username":"alice"},"content":"hey aerial"}}`,
			restateErr: true,
			wantStatus: http.StatusInternalServerError,
			wantSubstr: "Failed to process message",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			restateStatus := http.StatusOK
			if tc.restateErr {
				restateStatus = http.StatusInternalServerError
			}
			installRestateCapture(t, restateStatus, `{"status":"accepted"}`)
			handler := handleInternalDiscordMessage(pool)

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
	SetFunnelQuotaProvider(func() metrics.QuotaSnapshot { return metrics.QuotaSnapshot{} })
	defer SetFunnelQuotaProvider(nil)
	store := newTestStore(t)

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
			name:       "QuotaCommand",
			method:     http.MethodPost,
			body:       `{"id":"int-quota","type":2,"data":{"name":"quota"}}`,
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
			pool := newTestWorkerPool(store)
			if tc.hasPoolSession {
				sess, err := discordgo.New("Bot fake-token")
				if err != nil {
					t.Fatalf("failed to create discord session: %v", err)
				}
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

type syncVoiceMsg struct {
	pubsub.Message

	subject string
	data    []byte
}

func (m syncVoiceMsg) Data() []byte    { return m.data }
func (m syncVoiceMsg) Subject() string { return m.subject }

type syncVoiceSub struct {
	err error
}

func (s syncVoiceSub) Unsubscribe() error { return s.err }

type syncVoiceBus struct {
	pubsub.Bus

	unsubscribeErr error

	mu       sync.RWMutex
	handlers map[string]pubsub.Handler
}

func newSyncVoiceBus() *syncVoiceBus {
	return &syncVoiceBus{
		Bus:      pubsub.NewMemoryBus(),
		handlers: make(map[string]pubsub.Handler),
	}
}

func (b *syncVoiceBus) Subscribe(_ context.Context, subject, _ string, handler pubsub.Handler) (pubsub.Subscription, error) {
	b.mu.Lock()
	b.handlers[subject] = handler
	b.mu.Unlock()
	return syncVoiceSub{err: b.unsubscribeErr}, nil
}

func (b *syncVoiceBus) Publish(ctx context.Context, subject, _ string, data any) error {
	raw, err := json.Marshal(data)
	if err != nil {
		return err
	}
	b.mu.RLock()
	h := b.handlers[subject]
	b.mu.RUnlock()
	if h != nil {
		return h(ctx, syncVoiceMsg{subject: subject, data: raw})
	}
	return nil
}

// noCancelTransport detaches outbound requests from the caller's context so a
// cancelled stream cannot also complete the in-flight AskVoice call.
type noCancelTransport struct {
	base http.RoundTripper
}

func (n noCancelTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return n.base.RoundTrip(req.WithContext(context.WithoutCancel(req.Context())))
}

// awaitVoiceCallback blocks until a stream callback reports event, skipping
// unrelated follow-ups such as the "done" emitted after a reply.
func awaitVoiceCallback(handled <-chan string, event string) {
	for got := range handled {
		if got == event {
			return
		}
	}
}

type voiceStreamTestCase struct {
	name           string
	events         []queue.VoiceStreamEvent
	status         int
	holdAsk        bool
	unsubscribeErr error
	wantStatuses   []string
	wantSentences  []string
	wantSSE        []string
}

func handleMockVoiceStreamIngress(w http.ResponseWriter, r *http.Request, tc voiceStreamTestCase, bus *syncVoiceBus, handled chan string, release chan struct{}) {
	var req queue.VoiceSessionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	topic := "voice.stream." + req.TurnID
	for i, ev := range tc.events {
		if err := bus.Publish(r.Context(), topic, fmt.Sprintf("step-%d", i), ev); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		awaitVoiceCallback(handled, ev.Event)
	}
	if tc.holdAsk {
		<-release
	}
	if tc.status != http.StatusOK {
		http.Error(w, "boom", tc.status)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(queue.VoiceSessionResult{
		Reply:          "Full response.",
		ConversationID: "conv-buff-1",
		DeviceKey:      "kiosk-1",
	})
}

// TestStreamRestateVoiceEvents drives the fake ingress in lockstep with the
// stream loop: after publishing each event the ingress waits until the loop has
// handed it to a callback, and only then publishes the next one or answers. The
// ask result therefore never races a pending event, so every row covers the same
// statements on every run regardless of goroutine scheduling.
func TestStreamRestateVoiceEvents(t *testing.T) {
	t.Parallel()

	tests := []voiceStreamTestCase{
		{
			name: "buffered events emit reply once",
			events: []queue.VoiceStreamEvent{
				{Event: "status", Status: "buffered-wait"},
				{Event: "sentence", Text: "Token chunk 1"},
				{Event: "reply", Reply: "Full response.", ConversationID: "conv-buff-1"},
			},
			status:        http.StatusOK,
			wantStatuses:  []string{"buffered-wait"},
			wantSentences: []string{"Token chunk 1"},
			wantSSE:       []string{"reply", "done"},
		},
		{
			name:    "reply from ask result when no stream events",
			status:  http.StatusOK,
			wantSSE: []string{"reply", "done"},
		},
		{
			name:    "ask error when no stream events",
			status:  http.StatusInternalServerError,
			wantSSE: []string{"error", "done"},
		},
		{
			name:          "stream error event suppresses ask error",
			events:        []queue.VoiceStreamEvent{{Event: "sentence", Text: "Partial"}, {Event: "error", Error: "model crashed"}},
			status:        http.StatusInternalServerError,
			wantSentences: []string{"Partial"},
			wantSSE:       []string{"error", "done"},
		},
		{
			name:           "unsubscribe failure after reply is logged",
			status:         http.StatusOK,
			unsubscribeErr: errors.New("simulated unsubscribe failure"),
			wantSSE:        []string{"reply", "done"},
		},
		{
			name:         "caller cancellation ends stream while ask in flight",
			events:       []queue.VoiceStreamEvent{{Event: "status", Status: "thinking"}},
			status:       http.StatusOK,
			holdAsk:      true,
			wantStatuses: []string{"thinking"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			bus := newSyncVoiceBus()
			bus.unsubscribeErr = tc.unsubscribeErr
			handled := make(chan string, 16)
			release := make(chan struct{})
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				handleMockVoiceStreamIngress(w, r, tc, bus, handled, release)
			}))
			defer ts.Close()
			defer close(release)

			client := restateclient.NewRestateIngressClient(ts.URL, "aerial", &http.Client{Transport: noCancelTransport{base: ts.Client().Transport}})

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var statuses, sentences, sseEvents []string
			onStatus := func(s string) {
				statuses = append(statuses, s)
				if tc.holdAsk {
					cancel()
				}
				handled <- "status"
			}
			onSentence := func(s string) {
				sentences = append(sentences, s)
				handled <- "sentence"
			}
			emitSSE := func(event string, payload any) {
				sseEvents = append(sseEvents, event)
				handled <- event
			}

			req := VoiceAskRequest{Prompt: "test buffered voice", NodeID: "kiosk-1"}
			streamRestateVoiceEvents(ctx, nil, client, bus, req, "kiosk-1", onStatus, onSentence, emitSSE)

			if strings.Join(statuses, ",") != strings.Join(tc.wantStatuses, ",") {
				t.Fatalf("statuses = %+v, want %+v", statuses, tc.wantStatuses)
			}
			if strings.Join(sentences, ",") != strings.Join(tc.wantSentences, ",") {
				t.Fatalf("sentences = %+v, want %+v", sentences, tc.wantSentences)
			}
			if strings.Join(sseEvents, ",") != strings.Join(tc.wantSSE, ",") {
				t.Fatalf("sseEvents = %+v, want %+v", sseEvents, tc.wantSSE)
			}
		})
	}
}

func TestResolveVoiceEventBus_NilPool(t *testing.T) {
	t.Parallel()

	if bus := resolveVoiceEventBus(nil); bus != nil {
		t.Fatalf("resolveVoiceEventBus(nil) = %v, want nil", bus)
	}
}

func TestDrainVoiceEvents(t *testing.T) {
	t.Parallel()

	ch := make(chan queue.VoiceStreamEvent, 2)
	ch <- queue.VoiceStreamEvent{Event: "sentence", Text: "hello"}
	ch <- queue.VoiceStreamEvent{Event: "reply", Reply: "world"}
	var received []string
	drainVoiceEvents(ch, func(ev queue.VoiceStreamEvent) {
		received = append(received, ev.Event)
	})
	if len(received) != 2 || received[0] != "sentence" || received[1] != "reply" {
		t.Fatalf("unexpected drained events: %+v", received)
	}
}
