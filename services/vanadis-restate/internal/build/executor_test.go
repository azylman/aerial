package build

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/executor"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

type mockEriClient struct {
	dispatchFn func(ctx context.Context, req executor.BrainDispatchRequest) error
	cancelFn   func(ctx context.Context, chunkID string) error
	promptFn   func(ctx context.Context, threadID, prompt string) error
	widgetFn   func(ctx context.Context, req executor.StatusWidgetRequest) (string, error)
	messageFn  func(ctx context.Context, channelID, content string) error

	lastDispatch *executor.BrainDispatchRequest
	lastCancel   string
	lastThreadID string
	lastPrompt   string
	lastWidget   *executor.StatusWidgetRequest
	lastMsgChan  string
	lastMsgCont  string
}

func (m *mockEriClient) DispatchAccepted(ctx context.Context, req executor.BrainDispatchRequest) error {
	return m.Dispatch(ctx, req)
}

func (m *mockEriClient) Dispatch(ctx context.Context, req executor.BrainDispatchRequest) error {
	m.lastDispatch = &req
	if m.dispatchFn != nil {
		return m.dispatchFn(ctx, req)
	}
	return nil
}

func (m *mockEriClient) CancelDispatch(ctx context.Context, chunkID string) error {
	m.lastCancel = chunkID
	if m.cancelFn != nil {
		return m.cancelFn(ctx, chunkID)
	}
	return nil
}

func (m *mockEriClient) Prompt(ctx context.Context, threadID, prompt string) error {
	m.lastThreadID = threadID
	m.lastPrompt = prompt
	if m.promptFn != nil {
		return m.promptFn(ctx, threadID, prompt)
	}
	return nil
}

func (m *mockEriClient) Widget(ctx context.Context, req executor.StatusWidgetRequest) (string, error) {
	m.lastWidget = &req
	if m.widgetFn != nil {
		return m.widgetFn(ctx, req)
	}
	return "msg-123", nil
}

func (m *mockEriClient) Message(ctx context.Context, channelID, content string) error {
	m.lastMsgChan = channelID
	m.lastMsgCont = content
	if m.messageFn != nil {
		return m.messageFn(ctx, channelID, content)
	}
	return nil
}

type mockStoreForLaunch struct {
	store.VanadisStore

	reserveErr error
}

type mockBusExecutor struct {
	pubsub.Bus

	lastSubj string
}

func (m *mockStoreForLaunch) ReserveChunkForLaunch(ctx context.Context, chunkID string) error {
	return m.reserveErr
}

func (m *mockBusExecutor) Publish(ctx context.Context, subject, msgID string, data any) error {
	m.lastSubj = subject
	return nil
}

func launchRecoveryServer(t *testing.T, id, first string, ready *atomic.Bool) (*httptest.Server, chan string) {
	t.Helper()
	ids := make(chan string, 8)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/dispatch" {
			_, _ = w.Write([]byte(`{}`))
			return
		}
		var req executor.BrainDispatchRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		select {
		case ids <- req.ID:
		default:
			t.Error("unexpected repeated dispatch")
		}
		if req.ID == id && !ready.Load() {
			switch first {
			case "active":
				_, _ = w.Write([]byte(`{"status":"skipped_active"}`))
			case "lost":
				w.Header().Set("Content-Length", "100")
				_, _ = w.Write([]byte(`{"status":"accepted"`))
			default:
				w.WriteHeader(http.StatusServiceUnavailable)
			}
			return
		}
		status := "accepted"
		if first == "lost" {
			status = "already_exists"
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"status": status, "dispatch_id": req.ID})
	}))
	return srv, ids
}

func TestExecuteLaunchChunk_HTTPGenerationAcceptance(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, mode, key, body       string
		nudges                      int
		code                        int
		wantError, wantAcknowledged bool
	}{
		{"pr accepted", "pr", "c", `{"status":"accepted","dispatch_id":"vanadis-c-turn-0"}`, 0, 202, false, true},
		{"pr duplicate", "pr", "c", `{"status":"already_exists","dispatch_id":"vanadis-c-turn-0"}`, 0, 200, false, true},
		{"pr active", "pr", "c", `{"status":"skipped_active"}`, 0, 200, false, false},
		{"pr wrong accepted", "pr", "c", `{"status":"accepted","dispatch_id":"wrong"}`, 0, 202, true, false},
		{"pr wrong duplicate", "pr", "c", `{"status":"already_exists","dispatch_id":"wrong"}`, 0, 200, true, false},
		{"pr malformed", "pr", "c", `invalid`, 0, 200, true, false},
		{"pr missing status", "pr", "c", `{}`, 0, 200, true, false},
		{"pr server failure", "pr", "c", `{}`, 0, 500, true, false},
		{"initial accepted", "action", "c", `{"status":"accepted","dispatch_id":"vanadis-c-turn-0"}`, 0, 202, false, true},
		{"initial duplicate", "action", "c", `{"status":"already_exists","dispatch_id":"vanadis-c-turn-0"}`, 0, 200, false, true},
		{"initial active", "action", "c", `{"status":"skipped_active"}`, 0, 200, false, false},
		{"initial wrong accepted", "action", "c", `{"status":"accepted","dispatch_id":"wrong"}`, 0, 202, true, false},
		{"initial wrong duplicate", "action", "c", `{"status":"already_exists","dispatch_id":"wrong"}`, 0, 200, true, false},
		{"initial malformed", "action", "c", `invalid`, 0, 200, true, false},
		{"initial missing status", "action", "c", `{}`, 0, 200, true, false},
		{"initial server failure", "action", "c", `{}`, 0, 500, true, false},
		{"retry accepted", "action", "dispatch:vanadis-c-turn-1", `{"status":"accepted","dispatch_id":"vanadis-c-turn-1"}`, 1, 202, false, true},
		{"retry duplicate", "action", "dispatch:vanadis-c-turn-1", `{"status":"already_exists","dispatch_id":"vanadis-c-turn-1"}`, 1, 200, false, true},
		{"retry active", "action", "dispatch:vanadis-c-turn-1", `{"status":"skipped_active"}`, 1, 200, false, false},
		{"retry wrong accepted", "action", "dispatch:vanadis-c-turn-1", `{"status":"accepted","dispatch_id":"wrong"}`, 1, 202, true, false},
		{"retry wrong duplicate", "action", "dispatch:vanadis-c-turn-1", `{"status":"already_exists","dispatch_id":"wrong"}`, 1, 200, true, false},
		{"retry malformed", "action", "dispatch:vanadis-c-turn-1", `invalid`, 1, 200, true, false},
		{"retry missing status", "action", "dispatch:vanadis-c-turn-1", `{}`, 1, 200, true, false},
		{"retry server failure", "action", "dispatch:vanadis-c-turn-1", `{}`, 1, 500, true, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.code)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()
			build := model.VanadisBuild{ID: "b", Status: "running"}
			st, _ := dbScript{buildRow: &mockRow{b: &build}, chunks: []model.VanadisChunk{{ID: "c", Mode: tc.mode, Status: "running", StallNudges: tc.nudges}}, execTag: "UPDATE 1"}.open()
			bo := NewBuildObject("", nil).WithAuthority(st, executor.NewExecutor(t.TempDir(), executor.NewHTTPEriClient(srv.URL, srv.Client()), nil, "", nil))
			out, err := bo.executeAuthoritativeTransition(context.Background(), "b", nil, nil, false)
			assert.Equal(t, tc.wantError, err != nil)
			assert.Equal(t, tc.wantAcknowledged, out.LaunchedMap[tc.key])
		})
	}
}

func TestBuildObject_HTTPRecoverySiblingProgress(t *testing.T) {
	t.Parallel()
	accepted := make(chan string, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/dispatch" {
			w.WriteHeader(http.StatusOK)
			return
		}
		var req executor.BrainDispatchRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		// Eri's active-session gate runs before its duplicate-ID check. Either
		// persisted generation therefore produces the same ambiguous response.
		if req.SessionID == "vanadis-retry" {
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "skipped_active", "dispatch_id": req.ID})
			return
		}
		accepted <- req.ID
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "accepted", "dispatch_id": req.ID})
	}))
	defer srv.Close()
	build := model.VanadisBuild{ID: "b", Status: "running"}
	chunks := []model.VanadisChunk{
		{ID: "retry", Mode: "action", Status: "running", StallNudges: 1},
		{ID: "sibling", Mode: "action", Status: "pending"},
		{ID: "dependent", Mode: "action", Status: "pending", DependsOn: []string{"retry"}},
	}
	bo := NewBuildObject("", nil).WithAuthority(&mockStoreForLaunch{}, executor.NewExecutor(t.TempDir(), executor.NewHTTPEriClient(srv.URL, srv.Client()), nil, "", nil))
	launched := map[string]bool{}
	_, err := bo.launchEligibleChunks(context.Background(), build, chunks, launched)
	assert.ErrorIs(t, err, executor.ErrDispatchActive)
	assert.False(t, launched["dispatch:vanadis-retry-turn-1"])
	assert.True(t, launched["sibling"])
	select {
	case id := <-accepted:
		assert.Equal(t, "vanadis-sibling-turn-0", id)
	default:
		t.Error("independent sibling was not accepted")
	}
	assert.False(t, launched["dependent"])
}
