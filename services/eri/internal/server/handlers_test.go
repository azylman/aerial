package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"

	"github.com/azylman/aerial/pkg/eriwire"
	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/services/eri/internal/restateclient"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/delivery"
	"github.com/azylman/aerial/services/eri/pkg/llmcall"
	"github.com/azylman/aerial/services/eri/pkg/queue"
)

func runHTTP(h http.HandlerFunc, method, target string, body io.Reader, headers ...string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, body)
	for i := 0; i+1 < len(headers); i += 2 {
		if headers[i] != "" && headers[i+1] != "" {
			req.Header.Set(headers[i], headers[i+1])
		}
	}
	w := httptest.NewRecorder()
	h(w, req)
	return w
}

// restateCapture is an httptest Restate ingress that answers every call with
// a fixed response and keeps the body of the last request it received.
type restateCapture struct {
	mu   sync.Mutex
	last []byte
}

func (c *restateCapture) decodeLast(t *testing.T, out any) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.last) == 0 {
		t.Fatal("restate ingress received no request")
	}
	if err := json.Unmarshal(c.last, out); err != nil {
		t.Fatalf("decode restate request: %v", err)
	}
}

// installRestateCapture swaps the active Restate ingress client for one
// backed by a capturing fake until the test ends. Callers must not run in parallel.
func installRestateCapture(t *testing.T, status int, resp string) *restateCapture {
	t.Helper()
	capture := &restateCapture{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		capture.mu.Lock()
		capture.last = body
		capture.mu.Unlock()
		w.WriteHeader(status)
		_, _ = w.Write([]byte(resp))
	}))
	t.Cleanup(ts.Close)
	orig := restateclient.SwapActiveRestateIngressClient(restateclient.NewRestateIngressClient(ts.URL, "aerial", ts.Client()))
	t.Cleanup(func() { restateclient.StoreActiveRestateIngressClient(orig) })
	return capture
}

type promptPayloadCase struct {
	name          string
	pool          *queue.WorkerPool
	payload       string
	msgID         string
	wantThreadID  string
	wantChannelID string
	wantEffort    string
	wantModel     string
	wantContent   string
}

func assertPromptPayloadCase(t *testing.T, capture *restateCapture, tc promptPayloadCase) {
	t.Helper()
	w := runHTTP(handlePrompt(tc.pool), http.MethodPost, "/prompt", strings.NewReader(tc.payload))
	if w.Code != http.StatusAccepted {
		t.Fatalf("got status=%d body=%q, want 202", w.Code, w.Body.String())
	}
	if tc.wantThreadID != "" && !strings.Contains(w.Body.String(), fmt.Sprintf(`"session_id":"%s"`, tc.wantThreadID)) {
		t.Errorf("response body missing session_id %q: %s", tc.wantThreadID, w.Body.String())
	}
	if tc.wantChannelID != "" && !strings.Contains(w.Body.String(), fmt.Sprintf(`"channel_id":"%s"`, tc.wantChannelID)) {
		t.Errorf("response body missing channel_id %q: %s", tc.wantChannelID, w.Body.String())
	}
	var got queue.ThreadEnqueueRequest
	capture.decodeLast(t, &got)
	if tc.msgID != "" && got.MessageID != tc.msgID {
		t.Errorf("message_id=%q, want %q", got.MessageID, tc.msgID)
	}
	if tc.wantChannelID != "" && got.ChannelID != tc.wantChannelID {
		t.Errorf("channel_id=%q, want %q", got.ChannelID, tc.wantChannelID)
	}
	if tc.wantContent != "" && got.Prompt != tc.wantContent {
		t.Errorf("prompt=%q, want %q", got.Prompt, tc.wantContent)
	}
	if tc.wantEffort != "" && got.Effort != tc.wantEffort {
		t.Errorf("effort=%q, want %q", got.Effort, tc.wantEffort)
	}
	if tc.wantModel != "" && got.Model != tc.wantModel {
		t.Errorf("model=%q, want %q", got.Model, tc.wantModel)
	}
}

func TestHandlePromptValidation(t *testing.T) {
	capture := installRestateCapture(t, http.StatusOK, `{"status":"accepted"}`)
	store := newTestStore(t)
	pool := newTestWorkerPool(store)
	pool.Start()
	defer pool.Stop()

	mockSess, err := discordgo.New("Bot fake-token")
	if err != nil {
		t.Fatalf("failed to create discord session: %v", err)
	}
	mockSess.State = discordgo.NewState()
	mockSess.State.GuildAdd(&discordgo.Guild{
		ID: "guild-1",
		Channels: []*discordgo.Channel{
			{ID: "1564150557705248879", Name: "prospera"},
		},
	})
	prosperaPromptPool := queue.New(nil, queue.WorkerPoolConfig{
		Store:          store,
		DiscordSession: mockSess,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			return `{"event":"result","result":{"status":"SUCCESS","response":"mock"}}`, "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error { return nil },
	})
	defer prosperaPromptPool.Stop()

	for _, tc := range []struct {
		name       string
		method     string
		pool       *queue.WorkerPool
		body       io.Reader
		wantStatus int
		wantSubstr string
	}{
		{"method not allowed", http.MethodGet, pool, nil, http.StatusMethodNotAllowed, "Method not allowed"},
		{"read error", http.MethodPost, pool, &errReadCloser{readErr: errors.New("simulated network read error")}, http.StatusBadRequest, "Failed to read request body"},
		{"invalid JSON", http.MethodPost, pool, strings.NewReader("{invalid-json"), http.StatusBadRequest, "Invalid JSON payload"},
		{"empty prompt", http.MethodPost, pool, strings.NewReader(`{"prompt":""}`), http.StatusBadRequest, "Invalid payload: 'prompt' field is required and cannot be empty"},
		{"whitespace prompt", http.MethodPost, pool, strings.NewReader(`{"prompt":"   "}`), http.StatusBadRequest, "Invalid payload: 'prompt' field is required and cannot be empty"},
		{"missing channel_id", http.MethodPost, pool, strings.NewReader(`{"prompt":"hello"}`), http.StatusBadRequest, "Invalid payload: 'channel_id' field is required and cannot be empty"},
		{"non-snowflake channel_id", http.MethodPost, pool, strings.NewReader(`{"prompt":"hello","channel_id":"not-a-snowflake"}`), http.StatusBadRequest, "Invalid payload: 'channel_id' must be a valid numeric Discord snowflake"},
		{"unknown effort", http.MethodPost, pool, strings.NewReader(`{"prompt":"hello","channel_id":"1542423172400291873","effort":"extreme"}`), http.StatusBadRequest, "Invalid payload: 'effort' must be 'low', 'medium', or 'high'"},
		{"channel name resolution prospera", http.MethodPost, prosperaPromptPool, strings.NewReader(`{"prompt":"hello","channel_id":"prospera"}`), http.StatusAccepted, "1564150557705248879"},
		{"channel name resolution hash prospera", http.MethodPost, prosperaPromptPool, strings.NewReader(`{"prompt":"hello","channel_id":"#prospera"}`), http.StatusAccepted, "1564150557705248879"},
		{"channel name unresolvable falls back to snowflake validation", http.MethodPost, prosperaPromptPool, strings.NewReader(`{"prompt":"hello","channel_id":"unknown-channel"}`), http.StatusBadRequest, "must be a valid numeric Discord snowflake"},
		{"body close error", http.MethodPost, pool, &errReadCloser{reader: strings.NewReader(`{"prompt":"hello","channel_id":"1542423172400291873"}`), closeErr: errors.New("close error")}, http.StatusAccepted, "channel_id"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := runHTTP(handlePrompt(tc.pool), tc.method, "/prompt", tc.body)
			if w.Code != tc.wantStatus || !strings.Contains(w.Body.String(), tc.wantSubstr) {
				t.Fatalf("got status=%d body=%q, want status=%d substr=%q", w.Code, w.Body.String(), tc.wantStatus, tc.wantSubstr)
			}
		})
	}

	for _, tc := range []promptPayloadCase{
		{"default effort", pool, `{"prompt":"Test valid prompt","channel_id":"1542423172400291873","message_id":"msg-123"}`, "msg-123", "1542423172400291873", "1542423172400291873", "", "", "Test valid prompt"},
		{"explicit effort", pool, `{"prompt":"Test explicit effort","channel_id":"1542423172400291873","effort":"high","message_id":"msg-effort-high"}`, "msg-effort-high", "1542423172400291873", "1542423172400291873", "high", "", "Test explicit effort"},
		{"inferred medium effort", pool, `{"prompt":"Test medium effort","channel_id":"1542423172400291873","model":"model-medium","message_id":"msg-effort-med"}`, "msg-effort-med", "1542423172400291873", "1542423172400291873", "medium", "model-medium", "Test medium effort"},
		{"inferred low effort", pool, `{"prompt":"Test low effort","channel_id":"1542423172400291873","model":"model-flash","message_id":"msg-effort-flash"}`, "msg-effort-flash", "1542423172400291873", "1542423172400291873", "low", "model-flash", "Test low effort"},
		{"nil pool", nil, `{"prompt":"hello nil pool","channel_id":"1542423172400291873","message_id":"msg-nil-pool"}`, "msg-nil-pool", "1542423172400291873", "1542423172400291873", "", "", "hello nil pool"},
		{"derived id when message_id omitted", pool, `{"prompt":"auto uuid prompt","channel_id":"1542423172400291873"}`, "", "", "", "", "", "auto uuid prompt"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertPromptPayloadCase(t, capture, tc)
		})
	}
}

func TestHandleDispatch(t *testing.T) {
	capture := installRestateCapture(t, http.StatusOK, `{"status":"accepted"}`)
	// Rows share the capture, so they run sequentially against one private
	// database; every accepted row uses a distinct dispatch id.
	store := newTestStore(t)
	for _, tc := range []struct {
		name               string
		method             string
		body               string
		wantStatus         int
		wantSubstr         string
		checkID            string
		wantTargetID       string
		wantVanadisChunkID string
	}{
		{"method not allowed", http.MethodGet, "", http.StatusMethodNotAllowed, "Method not allowed", "", "", ""},
		{"invalid json", http.MethodPost, `{bad`, http.StatusBadRequest, "Invalid JSON payload", "", "", ""},
		{"missing source", http.MethodPost, `{"prompt":"test"}`, http.StatusBadRequest, "'source' must match", "", "", ""},
		{"invalid source", http.MethodPost, `{"source":"unknown!bad","prompt":"test"}`, http.StatusBadRequest, "'source' must match", "", "", ""},
		{"empty prompt", http.MethodPost, `{"source":"heartbeat","prompt":" "}`, http.StatusBadRequest, "'prompt' field is required", "", "", ""},
		{"numeric session_id rejected", http.MethodPost, `{"source":"heartbeat","prompt":"test","session_id":"1234567890"}`, http.StatusBadRequest, "cannot be a numeric snowflake", "", "", ""},
		{"discord prefix session_id rejected", http.MethodPost, `{"source":"heartbeat","prompt":"test","session_id":"discord-123"}`, http.StatusBadRequest, "cannot be a numeric snowflake", "", "", ""},
		{"invalid chars session_id rejected", http.MethodPost, `{"source":"heartbeat","prompt":"test","session_id":"bad:session"}`, http.StatusBadRequest, "cannot be a numeric snowflake", "", "", ""},
		{"invalid effort rejected", http.MethodPost, `{"source":"heartbeat","prompt":"test","effort":"ultra"}`, http.StatusBadRequest, "'effort' must be 'low', 'medium', or 'high'", "", "", ""},
		{"valid stateful accepted", http.MethodPost, `{"id":"disp-1","source":"heartbeat","session_id":"heartbeat-1","prompt":"run check","effort":"low"}`, http.StatusAccepted, `"status":"accepted"`, "disp-1", "", ""},
		{"valid ephemeral accepted", http.MethodPost, `{"id":"disp-2","source":"vanadis","prompt":"execute build"}`, http.StatusAccepted, `"status":"accepted"`, "disp-2", "", ""},
		{"valid with target_id normalized and chunk_id propagated", http.MethodPost, `{"id":"disp-norm-1","source":"vanadis","prompt":"execute chunk","target_id":"discord-987654321012345678","vanadis_chunk_id":"  build-42-chunk-1  "}`, http.StatusAccepted, `"status":"accepted"`, "disp-norm-1", "987654321012345678", "build-42-chunk-1"},
		{"valid with pooled target_id normalized", http.MethodPost, `{"id":"disp-norm-2","source":"vanadis","prompt":"execute chunk","target_id":"stateful_medium:discord-11223344","vanadis_chunk_id":"build-42-chunk-2"}`, http.StatusAccepted, `"status":"accepted"`, "disp-norm-2", "11223344", "build-42-chunk-2"},
		{"headless target_id blanked", http.MethodPost, `{"id":"disp-norm-3","source":"heartbeat","prompt":"pulse","target_id":"heartbeat-hb-1","vanadis_chunk_id":""}`, http.StatusAccepted, `"status":"accepted"`, "disp-norm-3", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := runHTTP(handleDispatch(store), tc.method, "/dispatch", strings.NewReader(tc.body))
			if w.Code != tc.wantStatus || !strings.Contains(w.Body.String(), tc.wantSubstr) {
				t.Fatalf("got status=%d body=%q, want status=%d substr=%q", w.Code, w.Body.String(), tc.wantStatus, tc.wantSubstr)
			}
			if tc.checkID != "" {
				var d eriwire.BrainDispatchRequest
				capture.decodeLast(t, &d)
				if d.ID != tc.checkID {
					t.Errorf("got id %q, want %q", d.ID, tc.checkID)
				}
				if d.TargetID != tc.wantTargetID {
					t.Errorf("got target_id %q, want %q", d.TargetID, tc.wantTargetID)
				}
				if d.VanadisChunkID != tc.wantVanadisChunkID {
					t.Errorf("got vanadis_chunk_id %q, want %q", d.VanadisChunkID, tc.wantVanadisChunkID)
				}
			}
		})
	}
}

func TestHandleCancelDispatch(t *testing.T) {
	capture := installRestateCapture(t, http.StatusOK, `{"status":"ok","cancelled_count":0}`)
	for _, tc := range []struct {
		name           string
		method         string
		body           string
		wantStatus     int
		wantSubstr     string
		wantDispatchID string
		wantBuildID    string
		wantReason     string
	}{
		{"method not allowed", http.MethodGet, "", http.StatusMethodNotAllowed, "Method not allowed", "", "", ""},
		{"invalid json", http.MethodPost, `{invalid`, http.StatusBadRequest, "Invalid JSON payload", "", "", ""},
		{"missing ids", http.MethodPost, `{"reason":"no ids"}`, http.StatusBadRequest, "'dispatch_id' (or 'message_id') or 'build_id' is required", "", "", ""},
		{"cancel by build_id", http.MethodPost, `{"build_id":"build-99"}`, http.StatusOK, `"status":"ok"`, "", "build-99", "cancelled vanadis build build-99"},
		{"cancel by chunk_id", http.MethodPost, `{"chunk_id":"build-99"}`, http.StatusOK, `"status":"ok"`, "", "build-99", "cancelled vanadis build build-99"},
		{"cancel by dispatch_id", http.MethodPost, `{"dispatch_id":"disp-single"}`, http.StatusOK, `"status":"ok"`, "disp-single", "", "cancelled dispatch disp-single"},
		{"cancel by dispatch_id custom reason", http.MethodPost, `{"dispatch_id":"disp-single","reason":"superseded"}`, http.StatusOK, `"status":"ok"`, "disp-single", "", "superseded"},
		{"cancel by message_id alias", http.MethodPost, `{"message_id":"disp-single"}`, http.StatusOK, `"status":"ok"`, "disp-single", "", "cancelled dispatch disp-single"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := runHTTP(handleCancelDispatch(), tc.method, "/dispatch/cancel", strings.NewReader(tc.body))
			if w.Code != tc.wantStatus || !strings.Contains(w.Body.String(), tc.wantSubstr) {
				t.Fatalf("got status=%d body=%q, want status=%d substr=%q", w.Code, w.Body.String(), tc.wantStatus, tc.wantSubstr)
			}
			if tc.wantReason == "" {
				return
			}
			var got queue.CancelDispatchControlRequest
			capture.decodeLast(t, &got)
			if got.DispatchID != tc.wantDispatchID || got.BuildID != tc.wantBuildID || got.Reason != tc.wantReason {
				t.Errorf("got cancel request %+v, want dispatch_id=%q build_id=%q reason=%q", got, tc.wantDispatchID, tc.wantBuildID, tc.wantReason)
			}
		})
	}
}

type directMessageCase struct {
	name       string
	method     string
	pool       *queue.WorkerPool
	body       io.Reader
	wantStatus int
	wantSubstr string
}

func assertDirectMessageCase(t *testing.T, tc directMessageCase) {
	t.Helper()
	w := runHTTP(handleDirectMessage(tc.pool), tc.method, "/discord/message", tc.body)
	if w.Code != tc.wantStatus || !strings.Contains(w.Body.String(), tc.wantSubstr) {
		t.Fatalf("got status=%d body=%q, want status=%d substr=%q", w.Code, w.Body.String(), tc.wantStatus, tc.wantSubstr)
	}
}

func runDirectMessageDeliveryCases(t *testing.T) {
	t.Helper()
	var capturedChannelID, capturedText string
	handler := handleDirectMessage(queue.New(nil, queue.WorkerPoolConfig{
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			capturedChannelID, capturedText = channelID, text
			return nil
		},
	}))

	w := runHTTP(handler, http.MethodPost, "/discord/message", strings.NewReader(`{"channel_id":"1542423172400291873","content":"Deploy succeeded!"}`))
	if w.Code != http.StatusOK || capturedChannelID != "1542423172400291873" || capturedText != "Deploy succeeded!" {
		t.Fatalf("unexpected delivery: code=%d, ch=%q, txt=%q", w.Code, capturedChannelID, capturedText)
	}
	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if resp["status"] != "sent" || resp["channel_id"] != "1542423172400291873" {
		t.Fatalf("unexpected response body: %+v", resp)
	}

	w2 := runHTTP(handler, http.MethodPost, "/discord/message", strings.NewReader(`{"thread_id":"1542423172400291874","text":"Deploy failed!"}`))
	if w2.Code != http.StatusOK || capturedChannelID != "1542423172400291874" || capturedText != "Deploy failed!" {
		t.Fatalf("unexpected fallback: code=%d, ch=%q, txt=%q", w2.Code, capturedChannelID, capturedText)
	}

	w3 := runHTTP(handler, http.MethodPost, "/discord/message", strings.NewReader(`{"channel_id":"1542423172400291873","thread_id":"1542423172400291875","content":"Priority check"}`))
	if w3.Code != http.StatusOK || capturedChannelID != "1542423172400291875" || capturedText != "Priority check" {
		t.Fatalf("unexpected thread priority: code=%d, ch=%q, txt=%q", w3.Code, capturedChannelID, capturedText)
	}

	var capturedAtts []*delivery.Attachment
	attHandler := handleDirectMessage(queue.New(nil, queue.WorkerPoolConfig{
		DeliveryWithAttachmentsFunc: func(s *discordgo.Session, channelID, text string, atts []*delivery.Attachment) error {
			capturedChannelID, capturedText, capturedAtts = channelID, text, atts
			return nil
		},
	}))
	wAtt := runHTTP(attHandler, http.MethodPost, "/discord/message", strings.NewReader(`{"channel_id":"1542423172400291873","content":"Voicemail received","attachments":[{"filename":"test.wav","content_type":"audio/wav","content_base64":"UklGRg=="}]}`))
	if wAtt.Code != http.StatusOK || len(capturedAtts) != 1 || capturedAtts[0].Filename != "test.wav" || string(capturedAtts[0].Data) != "RIFF" {
		t.Fatalf("unexpected attachment delivery: code=%d, atts=%+v", wAtt.Code, capturedAtts)
	}
}

func runDirectMessageMuxCases(t *testing.T) {
	t.Helper()
	pool := queue.New(nil, queue.WorkerPoolConfig{
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error { return nil },
	})
	mux := SetupBrainMux(nil, pool)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/discord/message", strings.NewReader(`{"channel_id":"1542423172400291873","content":"test"}`)))
	if w.Code != http.StatusOK {
		t.Errorf("expected 200 for /discord/message, got %d", w.Code)
	}
	wInternal := httptest.NewRecorder()
	mux.ServeHTTP(wInternal, httptest.NewRequest(http.MethodPost, "/internal/message", strings.NewReader(`{"channel_id":"1542423172400291873","content":"test"}`)))
	if wInternal.Code != http.StatusNotFound {
		t.Errorf("expected 404 for dropped /internal/message, got %d", wInternal.Code)
	}
}

func TestHandleDirectMessage(t *testing.T) {
	mockSess, err := discordgo.New("Bot fake-token")
	if err != nil {
		t.Fatalf("failed to create discord session: %v", err)
	}
	mockSess.State = discordgo.NewState()
	mockSess.State.GuildAdd(&discordgo.Guild{
		ID: "guild-1",
		Channels: []*discordgo.Channel{
			{ID: "1564150557705248879", Name: "prospera"},
		},
	})

	prosperaPool := queue.New(nil, queue.WorkerPoolConfig{
		DiscordSession: mockSess,
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			return nil
		},
		DeliveryWithAttachmentsFunc: func(s *discordgo.Session, channelID, text string, atts []*delivery.Attachment) error {
			return nil
		},
	})
	restErrPool := queue.New(nil, queue.WorkerPoolConfig{
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			return &discordgo.RESTError{Response: &http.Response{StatusCode: http.StatusForbidden}}
		},
	})
	sendErrPool := queue.New(nil, queue.WorkerPoolConfig{
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			return errors.New("cannot send messages to this thread")
		},
	})

	opaqueErrPool := queue.New(nil, queue.WorkerPoolConfig{
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			return errors.New("gateway exploded")
		},
	})

	oversizedB64 := base64.StdEncoding.EncodeToString(make([]byte, 8*1024*1024+1))

	for _, tc := range []directMessageCase{
		{"method not allowed", http.MethodGet, &queue.WorkerPool{}, nil, http.StatusMethodNotAllowed, "Method not allowed"},
		{"nil pool", http.MethodPost, nil, strings.NewReader(`{"channel_id":"1542423172400291873","content":"hello"}`), http.StatusInternalServerError, "Worker pool not available"},
		{"read and close error", http.MethodPost, &queue.WorkerPool{}, &errReadCloser{readErr: errors.New("read err"), closeErr: errors.New("close err")}, http.StatusBadRequest, "Failed to read request body"},
		{"invalid JSON", http.MethodPost, &queue.WorkerPool{}, strings.NewReader("{invalid-json"), http.StatusBadRequest, "Invalid JSON payload"},
		{"missing channel_id", http.MethodPost, &queue.WorkerPool{}, strings.NewReader(`{"content":"hello"}`), http.StatusBadRequest, "Invalid payload: 'channel_id' field is required and cannot be empty"},
		{"non-snowflake channel_id", http.MethodPost, &queue.WorkerPool{}, strings.NewReader(`{"channel_id":"not-a-snowflake","content":"hello"}`), http.StatusBadRequest, "Invalid payload: 'channel_id' must be a valid numeric Discord snowflake"},
		{"unresolvable channel name", http.MethodPost, prosperaPool, strings.NewReader(`{"channel_id":"unknown-channel","content":"hello"}`), http.StatusBadRequest, "Invalid payload: 'channel_id' must be a valid numeric Discord snowflake"},
		{"resolve channel name prospera", http.MethodPost, prosperaPool, strings.NewReader(`{"channel_id":"prospera","content":"hello"}`), http.StatusOK, "1564150557705248879"},
		{"resolve hash channel name prospera", http.MethodPost, prosperaPool, strings.NewReader(`{"channel_id":"#prospera","content":"hello"}`), http.StatusOK, "1564150557705248879"},
		{"empty content without attachments", http.MethodPost, &queue.WorkerPool{}, strings.NewReader(`{"channel_id":"1542423172400291873","content":"  "}`), http.StatusBadRequest, "Invalid payload: 'content' field is required and cannot be empty"},
		{"too many attachments", http.MethodPost, prosperaPool, strings.NewReader(`{"channel_id":"1542423172400291873","content":"hi","attachments":[{"filename":"1","content_base64":"YQ=="},{"filename":"2","content_base64":"YQ=="},{"filename":"3","content_base64":"YQ=="},{"filename":"4","content_base64":"YQ=="},{"filename":"5","content_base64":"YQ=="},{"filename":"6","content_base64":"YQ=="},{"filename":"7","content_base64":"YQ=="},{"filename":"8","content_base64":"YQ=="},{"filename":"9","content_base64":"YQ=="},{"filename":"10","content_base64":"YQ=="},{"filename":"11","content_base64":"YQ=="}]}`), http.StatusBadRequest, "Too many attachments"},
		{"empty attachment filename", http.MethodPost, prosperaPool, strings.NewReader(`{"channel_id":"1542423172400291873","attachments":[{"filename":"   ","content_base64":"YQ=="}]}`), http.StatusBadRequest, "Invalid attachment: 'filename' is required"},
		{"invalid base64", http.MethodPost, prosperaPool, strings.NewReader(`{"channel_id":"1542423172400291873","attachments":[{"filename":"a.txt","content_base64":"not@base64"}]}`), http.StatusBadRequest, "Invalid attachment base64"},
		{"empty base64 decoded data", http.MethodPost, prosperaPool, strings.NewReader(`{"channel_id":"1542423172400291873","attachments":[{"filename":"a.txt","content_base64":""}]}`), http.StatusBadRequest, "Invalid attachment: payload is empty"},
		{"oversized attachment", http.MethodPost, prosperaPool, strings.NewReader(`{"channel_id":"1542423172400291873","attachments":[{"filename":"huge.bin","content_base64":"` + oversizedB64 + `"}]}`), http.StatusBadRequest, "exceeds maximum size"},
		{"attachment with empty content allowed", http.MethodPost, prosperaPool, strings.NewReader(`{"channel_id":"1542423172400291873","attachments":[{"filename":"audio.wav","content_base64":"UklGRg=="}]}`), http.StatusOK, "sent"},
		{"discord session not connected", http.MethodPost, &queue.WorkerPool{}, strings.NewReader(`{"channel_id":"1542423172400291873","content":"hello"}`), http.StatusServiceUnavailable, "Discord session not connected"},
		{"discord client RESTError", http.MethodPost, restErrPool, strings.NewReader(`{"channel_id":"1542423172400291873","content":"hello"}`), http.StatusForbidden, "Discord client error"},
		{"discord client cannot send", http.MethodPost, sendErrPool, strings.NewReader(`{"channel_id":"1542423172400291873","content":"hello"}`), http.StatusBadRequest, "Discord client error"},
		{"opaque delivery error", http.MethodPost, opaqueErrPool, strings.NewReader(`{"channel_id":"1542423172400291873","content":"hello"}`), http.StatusInternalServerError, "Failed to deliver message: gateway exploded"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertDirectMessageCase(t, tc)
		})
	}
}

func TestHandleDirectMessage_Delivery(t *testing.T) {
	runDirectMessageDeliveryCases(t)
}

func TestHandleDirectMessage_MuxRoutes(t *testing.T) {
	runDirectMessageMuxCases(t)
}

func runHandleTasksFilterCases(t *testing.T, handler http.HandlerFunc, now time.Time) {
	t.Helper()
	for _, tc := range []struct {
		name string
		path string
	}{
		{name: "status filter", path: "/tasks?status=PENDING,PROCESSING"},
		{name: "thread_id filter", path: "/tasks?thread_id=thread-1"},
		{name: "is_admin filter", path: "/tasks?is_admin=true"},
		{name: "since filter", path: "/tasks?since=" + url.QueryEscape(now.Add(-time.Hour).Format(time.RFC3339))},
		{name: "after_row_id filter", path: "/tasks?after_row_id=0"},
		{name: "before_row_id filter", path: "/tasks?before_row_id=100"},
		{name: "limit filter", path: "/tasks?limit=10"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rr := runHTTP(handler, http.MethodGet, tc.path, nil)
			if rr.Code != http.StatusOK {
				t.Fatalf("expected 200 OK, got %d: %s", rr.Code, rr.Body.String())
			}
		})
	}
}

func TestParseTasksFilter_Order(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		raw       string
		wantOrder string
		wantRange bool
	}{
		{name: "absent order unset", raw: "", wantOrder: "", wantRange: false},
		{name: "asc sets order", raw: "order=asc", wantOrder: db.TasksOrderAsc, wantRange: true},
		{name: "desc sets order", raw: "order=desc", wantOrder: db.TasksOrderDesc, wantRange: true},
		{name: "uppercase normalized", raw: "order=ASC", wantOrder: db.TasksOrderAsc, wantRange: true},
		{name: "padded normalized", raw: "order=%20desc%20", wantOrder: db.TasksOrderDesc, wantRange: true},
		{name: "invalid ignored", raw: "order=sideways", wantOrder: "", wantRange: false},
		{name: "empty ignored", raw: "order=%20%20", wantOrder: "", wantRange: false},
		{name: "order combines with limit", raw: "order=asc&limit=10", wantOrder: db.TasksOrderAsc, wantRange: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			q, err := url.ParseQuery(tc.raw)
			if err != nil {
				t.Fatalf("ParseQuery(%q) failed: %v", tc.raw, err)
			}
			filter, hasFilter := parseTasksFilter(q)
			if filter.Order != tc.wantOrder {
				t.Errorf("parseTasksFilter(%q).Order = %q, want %q", tc.raw, filter.Order, tc.wantOrder)
			}
			if hasFilter != tc.wantRange {
				t.Errorf("parseTasksFilter(%q) hasFilter = %v, want %v", tc.raw, hasFilter, tc.wantRange)
			}
		})
	}
}

func runHandleTasksErrorCases(t *testing.T, handler http.HandlerFunc) {
	t.Helper()
	if code := runHTTP(handler, http.MethodPost, "/tasks", nil).Code; code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 Method Not Allowed, got %d", code)
	}

	closedStore := newClosedTestStore(t)
	for _, tc := range []struct {
		name    string
		handler http.HandlerFunc
	}{
		{"nil store", handleTasks(nil)},
		{"closed store", handleTasks(closedStore)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if code := runHTTP(tc.handler, http.MethodGet, "/tasks", nil).Code; code != http.StatusInternalServerError {
				t.Fatalf("got status=%d, want 500", code)
			}
		})
	}
}

func TestHandleTasks(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	now := time.Now().UTC()
	longPrompt := strings.Repeat("A", 600) + " plain-internal-detail"
	if err := store.InsertMessage(context.Background(), db.Message{
		ID: "msg-test-task", ThreadID: "thread-1", AuthorID: "user-1",
		AuthorName: "Tester",
		Content:    "Internal prompt " + longPrompt,
		Status:     db.StatusProcessing, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("failed to insert message: %v", err)
	}

	handler := handleTasks(store)
	rr := runHTTP(handler, http.MethodGet, "/tasks", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", rr.Code, rr.Body.String())
	}

	var resp struct {
		Status string          `json:"status"`
		Total  int             `json:"total"`
		Tasks  []db.ActiveTask `json:"tasks"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.Status != "ok" || resp.Total != 1 || len(resp.Tasks) != 1 {
		t.Fatalf("unexpected response payload: %+v", resp)
	}

	task := resp.Tasks[0]
	if task.Prompt != "Internal prompt "+longPrompt || task.Summary == "" || task.AuthorName != "Tester" {
		t.Errorf("internal task fields altered or summary empty: %+v", task)
	}
	if len(task.Prompt) < 600 {
		t.Errorf("prompt unexpectedly truncated: len=%d, text=%s", len(task.Prompt), task.Prompt)
	}

	if err := store.InsertMessage(context.Background(), db.Message{
		ID: "msg-test-task-2", ThreadID: "thread-2", AuthorID: "user-2", AuthorName: "Tester 2",
		Content: "Second prompt", Status: db.StatusPending, CreatedAt: now.Add(time.Second), UpdatedAt: now.Add(time.Second),
	}); err != nil {
		t.Fatalf("failed to insert message: %v", err)
	}
	rrCached := runHTTP(handler, http.MethodGet, "/tasks", nil)
	var cachedResp struct {
		Status string          `json:"status"`
		Total  int             `json:"total"`
		Tasks  []db.ActiveTask `json:"tasks"`
	}
	if err := json.NewDecoder(rrCached.Body).Decode(&cachedResp); err != nil {
		t.Fatalf("failed to decode cached response: %v", err)
	}
	if cachedResp.Total != 1 || len(cachedResp.Tasks) != 1 {
		t.Errorf("expected 1 cached task within 1s TTL, got %d", cachedResp.Total)
	}

	runHandleTasksFilterCases(t, handler, now)
	runHandleTasksErrorCases(t, handler)

	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			handler(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/tasks", http.NoBody))
		})
	}
	wg.Wait()
}

func TestNormalizeRoute(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input, expected string
	}{
		{"/prompt", "/prompt"},
		{"/dispatch", "/dispatch"},
		{"/dispatch/cancel", "/dispatch/cancel"},
		{"/transcripts/abc-123", "/transcripts"},
		{"/api/transcripts/steps", "/transcripts"},
		{"/api/transcripts/search", "/transcripts"},
		{"/tasks", "/tasks"},
		{"/tasks/456", "/tasks"},
		{"/internal/llm", "/internal/llm"},
		{"/internal/discord/message", "/internal/discord/message"},
		{"/internal/discord/interaction", "/internal/discord/interaction"},
		{"/schedules", "unmatched"},
		{"/schedules/runs", "unmatched"},
		{"/transcripts", "unmatched"},
		{"/health", "/health"},
		{"/healthz", "/health"},
		{"/metrics", "/metrics"},
		{"/voice/ask", "/voice/ask"},
		{"/api/voice/ask", "/voice/ask"},
		{"/unknown/path", "unmatched"},
		{"/random", "unmatched"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			if got := normalizeRoute(tt.input); got != tt.expected {
				t.Errorf("normalizeRoute(%q) = %q; want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestMetricsMiddleware(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("/unknown", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	mux.HandleFunc("/implicit", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})

	handler := metricsMiddleware(mux)

	tests := []struct {
		name     string
		method   string
		path     string
		wantCode int
	}{
		{"explicit 200", http.MethodGet, "/health", http.StatusOK},
		{"explicit 404", http.MethodGet, "/unknown", http.StatusNotFound},
		{"implicit 200 on write", http.MethodGet, "/implicit", http.StatusOK},
		{"empty method served as GET", "", "/health", http.StatusOK},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequest(http.MethodGet, tc.path, http.NoBody)
			req.Method = tc.method
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)
			if w.Code != tc.wantCode {
				t.Fatalf("status = %d, want %d", w.Code, tc.wantCode)
			}
		})
	}
}

// errWriteRecorder is a flushable recorder whose body writes always fail, as
// when the client connection has gone away mid-response.
type errWriteRecorder struct {
	*httptest.ResponseRecorder
}

func (errWriteRecorder) Write([]byte) (int, error) {
	return 0, errors.New("simulated client write failure")
}

type mockFlusherRecorder struct {
	*httptest.ResponseRecorder

	flushed bool
}

func (m *mockFlusherRecorder) Flush() {
	m.flushed = true
}

func TestStatusRecorder_Flush(t *testing.T) {
	t.Parallel()

	// With flusher
	rec := &mockFlusherRecorder{ResponseRecorder: httptest.NewRecorder()}
	sr := &statusRecorder{ResponseWriter: rec, statusCode: http.StatusOK}
	sr.Flush()
	if !rec.flushed {
		t.Errorf("Expected Flush() to be forwarded to underlying flusher")
	}

	// Without flusher
	plainRec := httptest.NewRecorder()
	srPlain := &statusRecorder{ResponseWriter: struct{ http.ResponseWriter }{plainRec}, statusCode: http.StatusOK}
	srPlain.Flush() // should not panic
}

// llmIngressCapture is what the Restate ingress stub saw for one call.
type llmIngressCapture struct {
	path    string
	idemKey string
	hasKey  bool
	req     llmcall.Request
}

// newLLMIngressStub serves the Restate ingress with a fixed status and body,
// recording each request into the returned channel.
func newLLMIngressStub(t *testing.T, status int, body string) (*httptest.Server, <-chan llmIngressCapture) {
	t.Helper()
	seen := make(chan llmIngressCapture, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c := llmIngressCapture{path: r.URL.Path}
		c.idemKey = r.Header.Get("Idempotency-Key")
		_, c.hasKey = r.Header["Idempotency-Key"]
		if err := json.NewDecoder(r.Body).Decode(&c.req); err != nil && err != io.EOF {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		seen <- c
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(ts.Close)
	return ts, seen
}

func TestHandleInternalLLM(t *testing.T) {
	t.Parallel()

	const att1 = "http://vision.aerial/api/events/1/snapshot.jpg"
	const att2 = "http://vision.aerial/api/events/1/clip.mp4"
	tooMany := make([]string, 0, 33)
	for i := range 33 {
		tooMany = append(tooMany, fmt.Sprintf(`{"url":"%s","filename":"f%d.jpg"}`, att1, i))
	}
	tooManyBody := `{"prompt":"inspect","attachments":[` + strings.Join(tooMany, ",") + `]}`

	tests := []struct {
		name       string
		method     string
		agent      string
		nilClient  bool
		callerKey  string
		body       string
		stubStatus int
		stubBody   string
		wantStatus int
		wantPath   string
		wantReq    *llmcall.Request
		wantBody   string
	}{
		{name: "method not allowed", method: http.MethodGet, agent: "aerial", wantStatus: http.StatusMethodNotAllowed},
		{name: "nil client", method: http.MethodPost, nilClient: true, body: `{"prompt":"hi"}`, wantStatus: http.StatusServiceUnavailable},
		{name: "invalid json", method: http.MethodPost, agent: "aerial", body: `{bad`, wantStatus: http.StatusBadRequest},
		{name: "empty prompt", method: http.MethodPost, agent: "aerial", body: `{"prompt":"   "}`, wantStatus: http.StatusBadRequest},
		{name: "invalid effort", method: http.MethodPost, agent: "aerial", body: `{"prompt":"extract","effort":"turbo"}`, wantStatus: http.StatusBadRequest},
		{name: "caller key with whitespace", method: http.MethodPost, agent: "aerial", callerKey: "coven:extract a", body: `{"prompt":"extract"}`, wantStatus: http.StatusBadRequest},
		{name: "empty attachment url", method: http.MethodPost, agent: "aerial", body: `{"prompt":"test","attachments":[{"url":"","filename":"a.jpg"}]}`, wantStatus: http.StatusBadRequest},
		{name: "duplicate attachment filename", method: http.MethodPost, agent: "aerial", body: `{"prompt":"test","attachments":[{"url":"` + att1 + `","filename":"dup.jpg"},{"url":"` + att2 + `","filename":"DUP.JPG"}]}`, wantStatus: http.StatusBadRequest},
		{name: "exceeds max attachments", method: http.MethodPost, agent: "aerial", body: tooManyBody, wantStatus: http.StatusBadRequest},
		{
			name: "unkeyed call", method: http.MethodPost, agent: "aerial", body: `{"prompt":"  extract  "}`,
			stubStatus: http.StatusOK, stubBody: `{"response":"facts"}`,
			wantStatus: http.StatusOK, wantPath: "/LLM/Complete", wantReq: &llmcall.Request{Prompt: "extract"}, wantBody: "facts",
		},
		{
			name: "keyed call forwards attachments and effort", method: http.MethodPost, agent: "aerial", callerKey: "vision:caption:event:42:init",
			body:       `{"prompt":"caption","effort":"Medium","attachments":[{"url":"` + att1 + `","filename":"snap.jpg"},{"url":"` + att2 + `","filename":"clip.mp4"}]}`,
			stubStatus: http.StatusOK, stubBody: `{"response":"a cat"}`,
			wantStatus: http.StatusOK, wantPath: "/LLM/Complete",
			wantReq: &llmcall.Request{
				Prompt: "caption", Effort: "medium", CallerKey: "vision:caption:event:42:init",
				Attachments: []llmcall.Attachment{{URL: att1, Filename: "snap.jpg"}, {URL: att2, Filename: "clip.mp4"}},
			},
			wantBody: "a cat",
		},
		{
			name: "sibling agent service", method: http.MethodPost, agent: "miorine", callerKey: "coven:extract:miorine:t1:7", body: `{"prompt":"extract"}`,
			stubStatus: http.StatusOK, stubBody: `{"response":"ok"}`,
			wantStatus: http.StatusOK, wantPath: "/LLM-miorine/Complete", wantReq: &llmcall.Request{Prompt: "extract", CallerKey: "coven:extract:miorine:t1:7"}, wantBody: "ok",
		},
		{
			name: "restate 400 stays 400", method: http.MethodPost, agent: "aerial", body: `{"prompt":"extract"}`,
			stubStatus: http.StatusBadRequest, stubBody: `{"message":"attachment fetch returned 404"}`,
			wantStatus: http.StatusBadRequest, wantPath: "/LLM/Complete", wantReq: &llmcall.Request{Prompt: "extract"}, wantBody: "attachment fetch returned 404",
		},
		{
			name: "restate 500 maps to 502", method: http.MethodPost, agent: "aerial", body: `{"prompt":"extract"}`,
			stubStatus: http.StatusInternalServerError, stubBody: `{"message":"engine down"}`,
			wantStatus: http.StatusBadGateway, wantPath: "/LLM/Complete", wantReq: &llmcall.Request{Prompt: "extract"}, wantBody: "engine down",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts, seen := newLLMIngressStub(t, tc.stubStatus, tc.stubBody)
			var client *restateclient.RestateIngressClient
			if !tc.nilClient {
				client = restateclient.NewRestateIngressClient(ts.URL, tc.agent, ts.Client())
			}
			w := runHTTP(handleInternalLLM(client), tc.method, "/internal/llm", strings.NewReader(tc.body), "Idempotency-Key", tc.callerKey)
			if w.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body %s)", w.Code, tc.wantStatus, w.Body.String())
			}
			if !strings.Contains(w.Body.String(), tc.wantBody) {
				t.Errorf("body %q does not contain %q", w.Body.String(), tc.wantBody)
			}
			if tc.wantReq == nil {
				if len(seen) != 0 {
					t.Fatalf("Restate ingress was called for a rejected request")
				}
				return
			}
			got := <-seen
			if got.path != tc.wantPath {
				t.Errorf("path = %q, want %q", got.path, tc.wantPath)
			}
			if !reflect.DeepEqual(got.req, *tc.wantReq) {
				t.Errorf("forwarded request = %+v, want %+v", got.req, *tc.wantReq)
			}
			wantKey := llmcall.IdempotencyKey(*tc.wantReq)
			if got.hasKey != (wantKey != "") || got.idemKey != wantKey {
				t.Errorf("Idempotency-Key = %q (present %v), want %q", got.idemKey, got.hasKey, wantKey)
			}
		})
	}
}

func TestSetupBrainMux_And_Endpoints(t *testing.T) {
	installRestateCapture(t, http.StatusOK, `{"status":"accepted"}`)
	store := newTestStore(t)
	pool := newTestWorkerPool(store)
	mux := SetupBrainMux(store, pool)

	if w := runHTTP(mux.ServeHTTP, http.MethodGet, "/health", nil); w.Code != http.StatusOK {
		t.Errorf("Expected 200 from /health, got %d", w.Code)
	}
	if w := runHTTP(mux.ServeHTTP, http.MethodGet, "/healthz", nil); w.Code != http.StatusOK {
		t.Errorf("Expected 200 from /healthz, got %d", w.Code)
	}
	failedHealth := errWriteRecorder{httptest.NewRecorder()}
	mux.ServeHTTP(failedHealth, httptest.NewRequest(http.MethodGet, "/health", http.NoBody))
	if failedHealth.Code != http.StatusOK || failedHealth.Body.Len() != 0 {
		t.Errorf("health with failing writer = %d %q, want 200 with no body", failedHealth.Code, failedHealth.Body.String())
	}

	if w := runHTTP(mux.ServeHTTP, http.MethodGet, "/tasks", nil); w.Code != http.StatusOK {
		t.Errorf("Expected 200 from /tasks, got %d", w.Code)
	}
	if w := runHTTP(mux.ServeHTTP, http.MethodPost, "/dispatch", strings.NewReader(`{"source":"heartbeat","prompt":"test"}`)); w.Code != http.StatusAccepted {
		t.Errorf("Expected 202 from /dispatch, got %d", w.Code)
	}
	if w := runHTTP(mux.ServeHTTP, http.MethodPost, "/dispatch/cancel", strings.NewReader(`{"dispatch_id":"disp-1"}`)); w.Code != http.StatusOK {
		t.Errorf("Expected 200 from /dispatch/cancel, got %d", w.Code)
	}
	if w := runHTTP(mux.ServeHTTP, http.MethodGet, "/transcripts", nil); w.Code != http.StatusNotFound {
		t.Errorf("Expected 404 from deleted /transcripts, got %d", w.Code)
	}

	oldFn := httpkit.WriteCountersDirFn
	defer func() { httpkit.WriteCountersDirFn = oldFn }()
	oldMetaFn := httpkit.WriteMetaDirFn
	defer func() { httpkit.WriteMetaDirFn = oldMetaFn }()

	called, metaCalled := false, false
	httpkit.WriteCountersDirFn = func(dir string) error {
		called = true
		return nil
	}
	httpkit.WriteMetaDirFn = func(dir string) error {
		metaCalled = true
		return nil
	}

	if w := runHTTP(mux.ServeHTTP, http.MethodGet, "/debug/coverage/flush", nil); w.Code != http.StatusOK || !called || !metaCalled {
		t.Errorf("expected 200 OK with mocks called, got %d", w.Code)
	}

	httpkit.WriteMetaDirFn = func(dir string) error { return errors.New("simulated meta flush error") }
	if w := runHTTP(mux.ServeHTTP, http.MethodGet, "/debug/coverage/flush", nil); w.Code != http.StatusOK {
		t.Errorf("expected 200 OK when meta flush errors, got %d", w.Code)
	}

	httpkit.WriteCountersDirFn = func(dir string) error { return errors.New("simulated flush error") }
	if w := runHTTP(mux.ServeHTTP, http.MethodGet, "/debug/coverage/flush", nil); w.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 on flush error, got %d", w.Code)
	}
}

func TestHandlePromptDispatch_RelaysRestateStatus(t *testing.T) {
	tests := []struct {
		name       string
		dispatch   bool
		serverResp string
		body       string
		wantCode   int
		wantSubstr string
	}{
		{"prompt skipped_active relayed", false, `{"status":"skipped_active"}`, `{"prompt":"p","channel_id":"1542423172400291873","skip_if_active":true}`, http.StatusOK, `"status":"skipped_active"`},
		{"prompt accepted relayed", false, `{"status":"accepted"}`, `{"prompt":"p","channel_id":"1542423172400291873","skip_if_active":true}`, http.StatusAccepted, `"status":"accepted"`},
		{"dispatch skipped_active relayed", true, `{"status":"skipped_active"}`, `{"source":"heartbeat","session_id":"s1","prompt":"p","skip_if_active":true}`, http.StatusOK, `"status":"skipped_active"`},
		{"dispatch accepted relayed", true, `{"status":"accepted"}`, `{"source":"heartbeat","session_id":"s1","prompt":"p","skip_if_active":true}`, http.StatusAccepted, `"status":"accepted"`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runWithMockRestate(http.StatusOK, tc.serverResp, func() {
				h := handlePrompt(nil)
				path := "/prompt"
				if tc.dispatch {
					// No row sends a dispatch id, so the duplicate check never
					// reaches the store.
					h = handleDispatch(nil)
					path = "/dispatch"
				}
				w := httptest.NewRecorder()
				h(w, httptest.NewRequest(http.MethodPost, path, strings.NewReader(tc.body)))
				if w.Code != tc.wantCode || !strings.Contains(w.Body.String(), tc.wantSubstr) {
					t.Fatalf("got %d %s", w.Code, w.Body.String())
				}
			})
		})
	}
}
