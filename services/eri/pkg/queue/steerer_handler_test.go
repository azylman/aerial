package queue

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/runner"
)

type testSteerTurnLease struct {
	daemonID string
	convID   string
}

func (l *testSteerTurnLease) ActiveTurn() runner.ActiveTurn {
	return runner.ActiveTurn{DaemonID: l.daemonID, ConversationID: l.convID}
}
func (l *testSteerTurnLease) SessionID() string       { return l.convID }
func (*testSteerTurnLease) PreviousSessionID() string { return "" }
func (*testSteerTurnLease) IsCold() bool              { return false }
func (*testSteerTurnLease) IsEphemeral() bool         { return false }
func (*testSteerTurnLease) TurnCount() int            { return 1 }
func (*testSteerTurnLease) MarkDirty()                {}
func (*testSteerTurnLease) Release() error            { return nil }
func (*testSteerTurnLease) Execute(context.Context, *runner.TurnContext) (*runner.TurnResult, error) {
	return &runner.TurnResult{}, nil
}

type mockFailingTransport struct{}

func (mockFailingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("simulated network failure")
}

type matchTestCase struct {
	name        string
	teDaemon    string
	leaseDaemon string
	currSess    string
	leaseSess   string
	ownerConv   string
	reqDaemonID string
	reqConvID   string
	reqSessID   string
	wantMatch   bool
}

func TestMatchTurnExecution(t *testing.T) {
	t.Parallel()

	assert.False(t, matchTurnExecution(nil, "d1", "", ""))

	tests := []matchTestCase{
		{
			name:        "match activeTurnDaemon",
			teDaemon:    "daemon-1",
			reqDaemonID: "daemon-1",
			wantMatch:   true,
		},
		{
			name:        "match lease active turn daemon",
			leaseDaemon: "daemon-2",
			reqDaemonID: "daemon-2",
			wantMatch:   true,
		},
		{
			name:        "daemon mismatch",
			teDaemon:    "daemon-1",
			leaseDaemon: "daemon-2",
			reqDaemonID: "daemon-3",
			wantMatch:   false,
		},
		{
			name:      "match currentSessionID by conversationID",
			currSess:  "sess-abc",
			reqConvID: "sess-abc",
			wantMatch: true,
		},
		{
			name:      "match leaseSessionID by conversationID",
			leaseSess: "sess-def",
			reqConvID: "sess-def",
			wantMatch: true,
		},
		{
			name:      "match owner conversationID by conversationID",
			ownerConv: "conv-xyz",
			reqConvID: "conv-xyz",
			wantMatch: true,
		},
		{
			name:      "match currentSessionID by sessionID",
			currSess:  "sess-abc",
			reqSessID: "sess-abc",
			wantMatch: true,
		},
		{
			name:      "match leaseSessionID by sessionID",
			leaseSess: "sess-def",
			reqSessID: "sess-def",
			wantMatch: true,
		},
		{
			name:      "match owner conversationID by sessionID",
			ownerConv: "conv-xyz",
			reqSessID: "conv-xyz",
			wantMatch: true,
		},
		{
			name:      "all request IDs empty",
			teDaemon:  "daemon-1",
			currSess:  "sess-1",
			wantMatch: false,
		},
		{
			name:      "empty target string inside execution handles safely",
			currSess:  "",
			leaseSess: "",
			ownerConv: "",
			reqConvID: "some-conv",
			wantMatch: false,
		},
		{
			name:      "mismatch conversation and session ID",
			currSess:  "sess-1",
			reqConvID: "sess-2",
			reqSessID: "sess-3",
			wantMatch: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			te := &turnExecution{
				activeTurnDaemon: tc.teDaemon,
				currentSessionID: tc.currSess,
				leaseSessionID:   tc.leaseSess,
			}
			if tc.leaseDaemon != "" || tc.ownerConv != "" {
				te.lease = &testSteerTurnLease{daemonID: tc.leaseDaemon, convID: tc.ownerConv}
			}
			got := matchTurnExecution(te, tc.reqDaemonID, tc.reqConvID, tc.reqSessID)
			assert.Equal(t, tc.wantMatch, got)
		})
	}
}

type handlerValidationCase struct {
	name          string
	method        string
	body          string
	noTurns       bool
	noMatch       bool
	closed        bool
	finalized     bool
	interrupted   bool
	wantStatus    int
	wantSubstr    string
	wantAllowPost bool
}

func setupValidationTestPool(tc handlerValidationCase) (*WorkerPool, func()) {
	if tc.noTurns {
		return nil, func() {}
	}
	pool := NewWorkerPool(WorkerPoolConfig{})
	if tc.noMatch {
		return pool, func() {}
	}
	te := &turnExecution{
		turnID:           "turn-123",
		threadID:         "thread-123",
		activeTurnDaemon: "daemon-1",
		steeringClosed:   tc.closed,
	}
	if tc.finalized {
		te.finalized.Store(true)
	}
	if tc.interrupted {
		te.interrupted.Store(true)
	}
	release, ok := pool.turns.Begin(context.Background(), "turn-123", te)
	if !ok {
		return nil, func() {}
	}
	return pool, release
}

func TestHandleDrainPending_ValidationAndStatus(t *testing.T) {
	t.Parallel()

	tests := []handlerValidationCase{
		{
			name:          "GET returns 405 with Allow POST",
			method:        http.MethodGet,
			wantStatus:    http.StatusMethodNotAllowed,
			wantSubstr:    "method not allowed",
			wantAllowPost: true,
		},
		{
			name:       "invalid JSON body returns 400",
			method:     http.MethodPost,
			body:       "{not-json",
			wantStatus: http.StatusBadRequest,
			wantSubstr: "invalid request",
		},
		{
			name:       "empty JSON object returns 400",
			method:     http.MethodPost,
			body:       `{}`,
			wantStatus: http.StatusBadRequest,
			wantSubstr: "at least one of daemon_id",
		},
		{
			name:       "blank fields return 400",
			method:     http.MethodPost,
			body:       `{"daemon_id":"   ","conversation_id":"  ","session_id":""}`,
			wantStatus: http.StatusBadRequest,
			wantSubstr: "at least one of daemon_id",
		},
		{
			name:       "turn registry nil returns 404",
			method:     http.MethodPost,
			body:       `{"daemon_id":"daemon-1"}`,
			noTurns:    true,
			wantStatus: http.StatusNotFound,
			wantSubstr: "turn registry not available",
		},
		{
			name:       "turn not found returns 404",
			method:     http.MethodPost,
			body:       `{"daemon_id":"unknown-daemon"}`,
			noMatch:    true,
			wantStatus: http.StatusNotFound,
			wantSubstr: "no active turn found",
		},
		{
			name:       "turn steering closed returns 409",
			method:     http.MethodPost,
			body:       `{"daemon_id":"daemon-1"}`,
			closed:     true,
			wantStatus: http.StatusConflict,
			wantSubstr: "turn steering closed or interrupted",
		},
		{
			name:       "turn finalized returns 409",
			method:     http.MethodPost,
			body:       `{"daemon_id":"daemon-1"}`,
			finalized:  true,
			wantStatus: http.StatusConflict,
			wantSubstr: "turn steering closed or interrupted",
		},
		{
			name:        "turn interrupted returns 409",
			method:      http.MethodPost,
			body:        `{"daemon_id":"daemon-1"}`,
			interrupted: true,
			wantStatus:  http.StatusConflict,
			wantSubstr:  "turn steering closed or interrupted",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			pool, cleanup := setupValidationTestPool(tc)
			defer cleanup()

			req := httptest.NewRequest(tc.method, SteerDrainPath, bytes.NewBufferString(tc.body))
			rec := httptest.NewRecorder()

			if pool != nil {
				pool.HandleDrainPending(rec, req)
			} else {
				var nilPool *WorkerPool
				nilPool.HandleDrainPending(rec, req)
			}

			assert.Equal(t, tc.wantStatus, rec.Code)
			if tc.wantSubstr != "" {
				assert.Contains(t, rec.Body.String(), tc.wantSubstr)
			}
			if tc.wantAllowPost {
				assert.Equal(t, http.MethodPost, rec.Header().Get("Allow"))
			}
		})
	}
}

func TestHandleDrainPending_RestateErrors(t *testing.T) {
	t.Parallel()

	t.Run("restate server 500 returns 502", func(t *testing.T) {
		t.Parallel()
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "internal crash", http.StatusInternalServerError)
		}))
		defer srv.Close()

		pool := NewWorkerPool(WorkerPoolConfig{
			RestateIngressURL: srv.URL,
			DrainHTTPClient:   srv.Client(),
		})
		te := &turnExecution{
			turnID:           "turn-restate-err",
			threadID:         "thread-123",
			activeTurnDaemon: "daemon-err",
		}
		release, ok := pool.turns.Begin(context.Background(), te.turnID, te)
		require.True(t, ok)
		defer release()

		body, err := json.Marshal(SteerDrainRequest{DaemonID: "daemon-err"})
		require.NoError(t, err)
		req := httptest.NewRequest(http.MethodPost, SteerDrainPath, bytes.NewReader(body))
		rec := httptest.NewRecorder()

		pool.HandleDrainPending(rec, req)
		assert.Equal(t, http.StatusBadGateway, rec.Code)
		assert.Contains(t, rec.Body.String(), "restate returned status 500")
	})

	t.Run("restate malformed json returns 502", func(t *testing.T) {
		t.Parallel()
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, wErr := w.Write([]byte(`{not json`))
			assert.NoError(t, wErr)
		}))
		defer srv.Close()

		pool := NewWorkerPool(WorkerPoolConfig{
			RestateIngressURL: srv.URL,
			DrainHTTPClient:   srv.Client(),
		})
		te := &turnExecution{
			turnID:           "turn-restate-badjson",
			threadID:         "thread-123",
			activeTurnDaemon: "daemon-badjson",
		}
		release, ok := pool.turns.Begin(context.Background(), te.turnID, te)
		require.True(t, ok)
		defer release()

		body, err := json.Marshal(SteerDrainRequest{DaemonID: "daemon-badjson"})
		require.NoError(t, err)
		req := httptest.NewRequest(http.MethodPost, SteerDrainPath, bytes.NewReader(body))
		rec := httptest.NewRecorder()

		pool.HandleDrainPending(rec, req)
		assert.Equal(t, http.StatusBadGateway, rec.Code)
		assert.Contains(t, rec.Body.String(), "decode restate response")
	})

	t.Run("restate connection error returns 502", func(t *testing.T) {
		t.Parallel()
		srvClosed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
		srvClosed.Close()
		pool := NewWorkerPool(WorkerPoolConfig{
			RestateIngressURL: srvClosed.URL,
			DrainHTTPClient:   srvClosed.Client(),
		})
		te := &turnExecution{
			turnID:           "turn-restate-connerr",
			threadID:         "thread-123",
			activeTurnDaemon: "daemon-connerr",
		}
		release, ok := pool.turns.Begin(context.Background(), te.turnID, te)
		require.True(t, ok)
		defer release()

		body, err := json.Marshal(SteerDrainRequest{DaemonID: "daemon-connerr"})
		require.NoError(t, err)
		req := httptest.NewRequest(http.MethodPost, SteerDrainPath, bytes.NewReader(body))
		rec := httptest.NewRecorder()

		pool.HandleDrainPending(rec, req)
		assert.Equal(t, http.StatusBadGateway, rec.Code)
		assert.Contains(t, rec.Body.String(), "restate drain call failed")
	})

	t.Run("empty RestateIngressURL falls back to default restate host", func(t *testing.T) {
		t.Parallel()
		pool := NewWorkerPool(WorkerPoolConfig{
			RestateIngressURL: "",
			DrainHTTPClient:   &http.Client{Transport: mockFailingTransport{}},
		})
		te := &turnExecution{
			turnID:           "turn-restate-default-url",
			threadID:         "thread-123",
			activeTurnDaemon: "daemon-default-url",
		}
		release, ok := pool.turns.Begin(context.Background(), te.turnID, te)
		require.True(t, ok)
		defer release()

		body, err := json.Marshal(SteerDrainRequest{DaemonID: "daemon-default-url"})
		require.NoError(t, err)
		req := httptest.NewRequest(http.MethodPost, SteerDrainPath, bytes.NewReader(body))
		rec := httptest.NewRecorder()

		pool.HandleDrainPending(rec, req)
		assert.Equal(t, http.StatusBadGateway, rec.Code)
		assert.Contains(t, rec.Body.String(), "restate drain call failed")
	})
}

func TestHandleDrainPending_BurstTurn(t *testing.T) {
	t.Parallel()

	store := testStore(t)
	fixedTime := time.Date(2026, 11, 26, 17, 0, 0, 0, time.UTC)
	m1 := db.Message{ID: "msg-steer-1", ThreadID: "thread-steer-burst", Content: "initial prompt", Status: db.StatusProcessing, CreatedAt: fixedTime}
	m2 := db.Message{ID: "msg-steer-2", ThreadID: "thread-steer-burst", Content: "steer msg A", Status: db.StatusPending, CreatedAt: fixedTime.Add(time.Second)}
	m3 := db.Message{ID: "msg-steer-3", ThreadID: "thread-steer-burst", Content: "steer msg B", Status: db.StatusPending, CreatedAt: fixedTime.Add(2 * time.Second)}
	seedMessageRows(t, store, []db.Message{m1, m2, m3})

	var restatePathSeen string
	var restateBodySeen DrainPendingRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		restatePathSeen = r.URL.Path
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&restateBodySeen))
		w.Header().Set("Content-Type", "application/json")
		assert.NoError(t, json.NewEncoder(w).Encode(DrainPendingResponse{
			DrainedIDs: []string{m2.ID, m3.ID},
		}))
	}))
	defer srv.Close()

	pool := NewWorkerPool(WorkerPoolConfig{
		Store:             store,
		RestateIngressURL: srv.URL,
		DrainHTTPClient:   srv.Client(),
		AgentName:         "aerial",
	})
	pool.appCfg = config.NewFromData(&config.ConfigData{Timezone: "America/New_York"})

	te := &turnExecution{
		turnID:           "turn-burst-test",
		threadID:         "thread-steer-burst",
		activeTurnDaemon: "daemon-burst-42",
		burst:            []db.Message{m1},
	}
	release, ok := pool.turns.Begin(context.Background(), te.turnID, te)
	require.True(t, ok)
	defer release()

	body, err := json.Marshal(SteerDrainRequest{DaemonID: "daemon-burst-42"})
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, SteerDrainPath, bytes.NewReader(body))
	rec := httptest.NewRecorder()

	pool.HandleDrainPending(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "/ThreadSession/thread-steer-burst/DrainPending", restatePathSeen)
	assert.Equal(t, "turn-burst-test", restateBodySeen.TurnID)

	var resp SteerDrainResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	require.Len(t, resp.Prompts, 1)
	assert.Contains(t, resp.Prompts[0], "steer msg A")
	assert.Contains(t, resp.Prompts[0], "steer msg B")
	assert.Contains(t, resp.Prompts[0], "at 12:00:01")
	assert.Contains(t, resp.Prompts[0], "at 12:00:02")

	te.steerMu.Lock()
	assert.Equal(t, []string{m2.ID, m3.ID}, te.drainedIDs)
	assert.Len(t, te.burst, 3)
	te.steerMu.Unlock()

	// Verify messages are claimed (Status = processing) in store
	row2, err := store.GetMessage(context.Background(), m2.ID)
	require.NoError(t, err)
	assert.Equal(t, db.StatusProcessing, row2.Status)

	row3, err := store.GetMessage(context.Background(), m3.ID)
	require.NoError(t, err)
	assert.Equal(t, db.StatusProcessing, row3.Status)

	// Second call with empty drain response
	srvEmpty := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		assert.NoError(t, json.NewEncoder(w).Encode(DrainPendingResponse{DrainedIDs: []string{}}))
	}))
	defer srvEmpty.Close()
	pool.cfg.RestateIngressURL = srvEmpty.URL
	pool.cfg.DrainHTTPClient = srvEmpty.Client()

	recEmpty := httptest.NewRecorder()
	reqEmpty := httptest.NewRequest(http.MethodPost, SteerDrainPath, bytes.NewReader(body))
	pool.HandleDrainPending(recEmpty, reqEmpty)
	require.Equal(t, http.StatusOK, recEmpty.Code)
	var respEmpty SteerDrainResponse
	require.NoError(t, json.NewDecoder(recEmpty.Body).Decode(&respEmpty))
	assert.Empty(t, respEmpty.Prompts)
}

func TestHandleDrainPending_DispatchTurn(t *testing.T) {
	t.Parallel()

	store := testStore(t)
	d1 := db.Dispatch{ID: "disp-1", SessionID: "sess-disp-test", Prompt: "initial prompt", Status: db.StatusProcessing, CreatedAt: time.Now().UTC()}
	d2 := db.Dispatch{ID: "disp-2", SessionID: "sess-disp-test", Prompt: "steer prompt 2", Status: db.StatusPending, CreatedAt: time.Now().UTC().Add(time.Second)}
	d3 := db.Dispatch{ID: "disp-3", SessionID: "sess-disp-test", Prompt: "steer prompt 3", Status: db.StatusPending, CreatedAt: time.Now().UTC().Add(2 * time.Second)}
	seedDispatchRows(t, store, []db.Dispatch{d1, d2, d3})

	var restatePathSeen string
	var restateBodySeen DrainPendingRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		restatePathSeen = r.URL.Path
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&restateBodySeen))
		w.Header().Set("Content-Type", "application/json")
		assert.NoError(t, json.NewEncoder(w).Encode(DrainPendingResponse{
			DrainedIDs: []string{d2.ID, d3.ID},
		}))
	}))
	defer srv.Close()

	pool := NewWorkerPool(WorkerPoolConfig{
		Store:             store,
		RestateIngressURL: srv.URL,
		DrainHTTPClient:   srv.Client(),
		AgentName:         "aerial",
	})

	te := &turnExecution{
		turnID:           "turn-disp-test",
		threadID:         "sess-disp-test",
		currentSessionID: "sess-disp-test",
		dispatch:         &d1,
	}
	release, ok := pool.turns.Begin(context.Background(), te.turnID, te)
	require.True(t, ok)
	defer release()

	body, err := json.Marshal(SteerDrainRequest{SessionID: "sess-disp-test"})
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, SteerDrainPath, bytes.NewReader(body))
	rec := httptest.NewRecorder()

	pool.HandleDrainPending(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "/DispatchSession/sess-disp-test/DrainPending", restatePathSeen)
	assert.Equal(t, "turn-disp-test", restateBodySeen.TurnID)

	var resp SteerDrainResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	require.Equal(t, []string{"steer prompt 2", "steer prompt 3"}, resp.Prompts)

	te.steerMu.Lock()
	assert.Equal(t, []string{d2.ID, d3.ID}, te.drainedIDs)
	require.Len(t, te.steeredDispatches, 2)
	assert.Equal(t, d2.ID, te.steeredDispatches[0].ID)
	assert.Equal(t, d3.ID, te.steeredDispatches[1].ID)
	te.steerMu.Unlock()

	// Verify dispatches are claimed (Status = processing) in store
	row2, err := store.GetDispatch(context.Background(), d2.ID)
	require.NoError(t, err)
	assert.Equal(t, db.StatusProcessing, row2.Status)

	row3, err := store.GetDispatch(context.Background(), d3.ID)
	require.NoError(t, err)
	assert.Equal(t, db.StatusProcessing, row3.Status)
}

func TestRegisterSteerHandler(t *testing.T) {
	t.Parallel()

	// Nil arguments no-op safely
	mux := http.NewServeMux()
	RegisterSteerHandler(nil, NewWorkerPool(WorkerPoolConfig{}))
	RegisterSteerHandler(mux, nil)

	pool := NewWorkerPool(WorkerPoolConfig{})
	RegisterSteerHandler(mux, pool)

	// Remote IP rejected with 403 Forbidden
	reqRemote := httptest.NewRequest(http.MethodPost, SteerDrainPath, bytes.NewBufferString("{}"))
	reqRemote.RemoteAddr = "192.0.2.1:54321"
	recRemote := httptest.NewRecorder()
	mux.ServeHTTP(recRemote, reqRemote)
	assert.Equal(t, http.StatusForbidden, recRemote.Code)

	// Loopback IP allowed through (returns 400 Bad Request because body is empty)
	reqLoopback := httptest.NewRequest(http.MethodPost, SteerDrainPath, bytes.NewBufferString("{}"))
	reqLoopback.RemoteAddr = "127.0.0.1:54321"
	recLoopback := httptest.NewRecorder()
	mux.ServeHTTP(recLoopback, reqLoopback)
	assert.Equal(t, http.StatusBadRequest, recLoopback.Code)
}

func TestDrainHTTPClient(t *testing.T) {
	t.Parallel()

	var nilPool *WorkerPool
	assert.NotNil(t, nilPool.drainHTTPClient())

	pool := NewWorkerPool(WorkerPoolConfig{})
	assert.NotNil(t, pool.drainHTTPClient())

	srvClient := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srvClient.Close()
	custom := srvClient.Client()

	poolCustom := NewWorkerPool(WorkerPoolConfig{DrainHTTPClient: custom})
	assert.Same(t, custom, poolCustom.drainHTTPClient())
}

func TestHandleDrainPending_StoreNil(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		assert.NoError(t, json.NewEncoder(w).Encode(DrainPendingResponse{DrainedIDs: []string{"msg-1"}}))
	}))
	defer srv.Close()

	pool := NewWorkerPool(WorkerPoolConfig{
		RestateIngressURL: srv.URL,
		DrainHTTPClient:   srv.Client(),
	})
	te := &turnExecution{
		turnID:           "turn-no-store",
		threadID:         "thread-no-store",
		activeTurnDaemon: "daemon-no-store",
	}
	release, ok := pool.turns.Begin(context.Background(), te.turnID, te)
	require.True(t, ok)
	defer release()

	body, err := json.Marshal(SteerDrainRequest{DaemonID: "daemon-no-store"})
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, SteerDrainPath, bytes.NewReader(body))
	rec := httptest.NewRecorder()

	pool.HandleDrainPending(rec, req)
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Contains(t, rec.Body.String(), "store not available")
}

func TestHandleDrainPending_ClaimedAndBlankRecords(t *testing.T) {
	t.Parallel()

	store := testStore(t)
	// Seed an already completed message and dispatch
	m1 := db.Message{ID: "msg-done-1", ThreadID: "thread-claimed", Content: "completed msg", Status: db.StatusCompleted, CreatedAt: time.Now().UTC()}
	mBlank := db.Message{ID: "msg-blank-content", ThreadID: "thread-claimed", Content: "   ", Status: db.StatusPending, CreatedAt: time.Now().UTC().Add(time.Second)}
	seedMessageRows(t, store, []db.Message{m1, mBlank})

	d1 := db.Dispatch{ID: "disp-done-1", SessionID: "sess-claimed", Prompt: "completed disp", Status: db.StatusCompleted, CreatedAt: time.Now().UTC()}
	dBlank := db.Dispatch{ID: "disp-blank-prompt", SessionID: "sess-claimed", Prompt: "   ", Status: db.StatusPending, CreatedAt: time.Now().UTC().Add(time.Second)}
	seedDispatchRows(t, store, []db.Dispatch{d1, dBlank})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "ThreadSession") {
			assert.NoError(t, json.NewEncoder(w).Encode(DrainPendingResponse{DrainedIDs: []string{m1.ID, mBlank.ID}}))
		} else {
			assert.NoError(t, json.NewEncoder(w).Encode(DrainPendingResponse{DrainedIDs: []string{d1.ID, dBlank.ID}}))
		}
	}))
	defer srv.Close()

	pool := NewWorkerPool(WorkerPoolConfig{
		Store:             store,
		RestateIngressURL: srv.URL,
		DrainHTTPClient:   srv.Client(),
	})

	// Burst turn where one message is already completed and one is blank content (tests coalesce empty prompt branch)
	teBurst := &turnExecution{
		turnID:           "turn-burst-claimed",
		threadID:         "thread-claimed",
		activeTurnDaemon: "daemon-burst-claimed",
	}
	relBurst, ok := pool.turns.Begin(context.Background(), teBurst.turnID, teBurst)
	require.True(t, ok)
	defer relBurst()

	bodyBurst, err := json.Marshal(SteerDrainRequest{DaemonID: "daemon-burst-claimed"})
	require.NoError(t, err)
	reqBurst := httptest.NewRequest(http.MethodPost, SteerDrainPath, bytes.NewReader(bodyBurst))
	recBurst := httptest.NewRecorder()
	pool.HandleDrainPending(recBurst, reqBurst)
	assert.Equal(t, http.StatusOK, recBurst.Code)
	var respBurst SteerDrainResponse
	require.NoError(t, json.NewDecoder(recBurst.Body).Decode(&respBurst))
	assert.Empty(t, respBurst.Prompts)

	// Dispatch turn where one dispatch is already completed and another has blank prompt
	teDisp := &turnExecution{
		turnID:           "turn-disp-claimed",
		threadID:         "sess-claimed",
		activeTurnDaemon: "daemon-disp-claimed",
		dispatch:         &d1,
	}
	relDisp, ok := pool.turns.Begin(context.Background(), teDisp.turnID, teDisp)
	require.True(t, ok)
	defer relDisp()

	bodyDisp, err := json.Marshal(SteerDrainRequest{DaemonID: "daemon-disp-claimed"})
	require.NoError(t, err)
	reqDisp := httptest.NewRequest(http.MethodPost, SteerDrainPath, bytes.NewReader(bodyDisp))
	recDisp := httptest.NewRecorder()
	pool.HandleDrainPending(recDisp, reqDisp)
	assert.Equal(t, http.StatusOK, recDisp.Code)
	var respDisp SteerDrainResponse
	require.NoError(t, json.NewDecoder(recDisp.Body).Decode(&respDisp))
	assert.Empty(t, respDisp.Prompts)
}

func TestWorkerPool_ReconcileActiveTurnIDs(t *testing.T) {
	t.Parallel()

	// 1. Nil pool / unconfigured pool
	var nilPool *WorkerPool
	assert.Equal(t, []string{"b1"}, nilPool.reconcileActiveTurnIDs(context.Background(), false, "k", "t", []string{"b1"}))

	poolNoURL := NewWorkerPool(WorkerPoolConfig{})
	assert.Equal(t, []string{"b1"}, poolNoURL.reconcileActiveTurnIDs(context.Background(), false, "k", "t", []string{"b1"}))

	// 2. Restate server returns matching active turn items; rejects body and Content-Type
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Content-Type") != "" {
			http.Error(w, "status handler rejects Content-Type", http.StatusBadRequest)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "error reading body", http.StatusBadRequest)
			return
		}
		if len(body) > 0 {
			http.Error(w, "status handler rejects body", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "Status") {
			if strings.Contains(r.URL.Path, "ThreadSession") {
				_ = json.NewEncoder(w).Encode(CoordinatorStatus{
					Active: &ActiveTurn{TurnID: "t-burst", ItemIDs: []string{"b1", "b2"}},
				})
				return
			}
			if strings.Contains(r.URL.Path, "DispatchSession") {
				_ = json.NewEncoder(w).Encode(CoordinatorStatus{
					Active: &ActiveTurn{TurnID: "t-disp", ItemIDs: []string{"d1", "d2"}},
				})
				return
			}
		}
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer srv.Close()

	pool := NewWorkerPool(WorkerPoolConfig{
		RestateIngressURL: srv.URL,
		DrainHTTPClient:   srv.Client(),
	})

	// Empty key and empty turnID checks against configured pool (must not query or expand)
	assert.Equal(t, []string{"b1"}, pool.reconcileActiveTurnIDs(context.Background(), false, "", "t-burst", []string{"b1"}))
	assert.Equal(t, []string{"b1"}, pool.reconcileActiveTurnIDs(context.Background(), false, "thread-key", "", []string{"b1"}))

	gotBurst := pool.reconcileActiveTurnIDs(context.Background(), false, "thread-key", "t-burst", []string{"b1"})
	assert.Equal(t, []string{"b1", "b2"}, gotBurst)

	gotDisp := pool.reconcileActiveTurnIDs(context.Background(), true, "session-key", "t-disp", []string{"d1"})
	assert.Equal(t, []string{"d1", "d2"}, gotDisp)

	// 3. Restate returns non-matching TurnID
	gotMismatch := pool.reconcileActiveTurnIDs(context.Background(), false, "thread-key", "t-other", []string{"b1"})
	assert.Equal(t, []string{"b1"}, gotMismatch)

	// 4. Server returns 500 error
	errSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}))
	defer errSrv.Close()

	errPool := NewWorkerPool(WorkerPoolConfig{
		RestateIngressURL: errSrv.URL,
		DrainHTTPClient:   errSrv.Client(),
	})
	gotErr := errPool.reconcileActiveTurnIDs(context.Background(), false, "thread-key", "t-burst", []string{"b1"})
	assert.Equal(t, []string{"b1"}, gotErr)

	// 5. Invalid JSON response
	badJSONSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("not json"))
	}))
	defer badJSONSrv.Close()
	badJSONPool := NewWorkerPool(WorkerPoolConfig{RestateIngressURL: badJSONSrv.URL, DrainHTTPClient: badJSONSrv.Client()})
	assert.Equal(t, []string{"b1"}, badJSONPool.reconcileActiveTurnIDs(context.Background(), false, "thread-key", "t-burst", []string{"b1"}))

	// 6. Active is nil
	emptyActiveSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(CoordinatorStatus{
			Active: nil,
		})
	}))
	defer emptyActiveSrv.Close()
	emptyActivePool := NewWorkerPool(WorkerPoolConfig{RestateIngressURL: emptyActiveSrv.URL, DrainHTTPClient: emptyActiveSrv.Client()})
	assert.Equal(t, []string{"b1"}, emptyActivePool.reconcileActiveTurnIDs(context.Background(), false, "thread-key", "t-burst", []string{"b1"}))

	// 7. Client transport error
	closedSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	badTransportClient := closedSrv.Client()
	badTransportURL := closedSrv.URL
	closedSrv.Close()
	badTransportPool := NewWorkerPool(WorkerPoolConfig{
		RestateIngressURL: badTransportURL,
		DrainHTTPClient:   badTransportClient,
	})
	assert.Equal(t, []string{"b1"}, badTransportPool.reconcileActiveTurnIDs(context.Background(), false, "thread-key", "t-burst", []string{"b1"}))
}

func TestHandleDrainPending_DrainedPromptsPopulated(t *testing.T) {
	t.Parallel()

	store := testStore(t)
	m1 := db.Message{ID: "msg-p1", ThreadID: "thread-p", Content: "extra burst prompt", Status: db.StatusPending, CreatedAt: time.Now().UTC()}
	seedMessageRows(t, store, []db.Message{m1})

	d1 := db.Dispatch{ID: "disp-p1", SessionID: "sess-p", Prompt: "extra dispatch prompt", Status: db.StatusPending, CreatedAt: time.Now().UTC()}
	seedDispatchRows(t, store, []db.Dispatch{d1})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "ThreadSession") {
			_ = json.NewEncoder(w).Encode(DrainPendingResponse{DrainedIDs: []string{m1.ID}})
		} else {
			_ = json.NewEncoder(w).Encode(DrainPendingResponse{DrainedIDs: []string{d1.ID}})
		}
	}))
	defer srv.Close()

	pool := NewWorkerPool(WorkerPoolConfig{
		Store:             store,
		RestateIngressURL: srv.URL,
		DrainHTTPClient:   srv.Client(),
	})

	// Burst drain check
	teBurst := &turnExecution{
		turnID:           "turn-burst-p",
		threadID:         "thread-p",
		activeTurnDaemon: "daemon-burst-p",
	}
	relBurst, ok := pool.turns.Begin(context.Background(), teBurst.turnID, teBurst)
	require.True(t, ok)
	defer relBurst()

	bodyBurst, err := json.Marshal(SteerDrainRequest{DaemonID: "daemon-burst-p"})
	require.NoError(t, err)
	recBurst := httptest.NewRecorder()
	pool.HandleDrainPending(recBurst, httptest.NewRequest(http.MethodPost, SteerDrainPath, bytes.NewReader(bodyBurst)))
	assert.Equal(t, http.StatusOK, recBurst.Code)
	teBurst.steerMu.Lock()
	assert.Contains(t, teBurst.drainedPrompts, "extra burst prompt")
	assert.Contains(t, teBurst.drainedIDs, m1.ID)
	teBurst.steerMu.Unlock()

	// Dispatch drain check
	teDisp := &turnExecution{
		turnID:           "turn-disp-p",
		threadID:         "sess-p",
		activeTurnDaemon: "daemon-disp-p",
		dispatch:         &db.Dispatch{ID: "disp-initial", SessionID: "sess-p"},
	}
	relDisp, ok := pool.turns.Begin(context.Background(), teDisp.turnID, teDisp)
	require.True(t, ok)
	defer relDisp()

	bodyDisp, err := json.Marshal(SteerDrainRequest{DaemonID: "daemon-disp-p"})
	require.NoError(t, err)
	recDisp := httptest.NewRecorder()
	pool.HandleDrainPending(recDisp, httptest.NewRequest(http.MethodPost, SteerDrainPath, bytes.NewReader(bodyDisp)))
	assert.Equal(t, http.StatusOK, recDisp.Code)
	teDisp.steerMu.Lock()
	assert.Contains(t, teDisp.drainedPrompts, "extra dispatch prompt")
	assert.Contains(t, teDisp.drainedIDs, d1.ID)
	teDisp.steerMu.Unlock()
}

func TestBurstSubjectLoad_CrashReplay_Recovery(t *testing.T) {
	t.Parallel()

	store := testStore(t)
	th := "thread-replay-burst"
	m1 := db.Message{ID: "m-init", ThreadID: th, Content: "initial prompt", Status: db.StatusProcessing, CreatedAt: time.Now().UTC()}
	m2 := db.Message{ID: "m-proc-db", ThreadID: th, Content: "db processing prompt", Status: db.StatusProcessing, CreatedAt: time.Now().UTC().Add(time.Second)}
	m3 := db.Message{ID: "m-pend-status", ThreadID: th, Content: "status pending prompt", Status: db.StatusPending, CreatedAt: time.Now().UTC().Add(2 * time.Second)}
	seedMessageRows(t, store, []db.Message{m1, m2, m3})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(CoordinatorStatus{
			Active: &ActiveTurn{TurnID: "turn-burst-replay", ItemIDs: []string{m1.ID, m3.ID}},
		})
	}))
	defer srv.Close()

	pool := NewWorkerPool(WorkerPoolConfig{
		Store:             store,
		RestateIngressURL: srv.URL,
		DrainHTTPClient:   srv.Client(),
	})

	te := &turnExecution{
		pool:     pool,
		turnID:   "turn-burst-replay",
		threadID: th,
	}

	subj := burstSubject{pool: pool, ids: []string{m1.ID}}
	ids, outcome, err := subj.load(context.Background(), store, te)
	require.NoError(t, err)
	assert.Empty(t, outcome)
	assert.ElementsMatch(t, []string{m1.ID, m2.ID, m3.ID}, ids)

	te.steerMu.Lock()
	assert.ElementsMatch(t, []string{m2.ID, m3.ID}, te.drainedIDs)
	require.Len(t, te.drainedPrompts, 2)
	assert.Contains(t, te.drainedPrompts, "db processing prompt")
	assert.Contains(t, te.drainedPrompts, "status pending prompt")
	te.steerMu.Unlock()

	// Verify m3 was claimed into PROCESSING in the store
	gotM3, err := store.GetMessage(context.Background(), m3.ID)
	require.NoError(t, err)
	require.NotNil(t, gotM3)
	assert.Equal(t, db.StatusProcessing, gotM3.Status)
}

func TestDispatchSubjectLoad_CrashReplay_Recovery(t *testing.T) {
	t.Parallel()

	store := testStore(t)
	sess := "sess-replay-disp"
	d1 := db.Dispatch{ID: "d-init", SessionID: sess, Prompt: "initial prompt", Status: db.StatusProcessing, CreatedAt: time.Now().UTC()}
	d2 := db.Dispatch{ID: "d-proc-db", SessionID: sess, Prompt: "db processing prompt", Status: db.StatusProcessing, CreatedAt: time.Now().UTC().Add(time.Second)}
	d3 := db.Dispatch{ID: "d-pend-status", SessionID: sess, Prompt: "status pending prompt", Status: db.StatusPending, CreatedAt: time.Now().UTC().Add(2 * time.Second)}
	seedDispatchRows(t, store, []db.Dispatch{d1, d2, d3})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(CoordinatorStatus{
			Active: &ActiveTurn{TurnID: "turn-disp-replay", ItemIDs: []string{d1.ID, d3.ID}},
		})
	}))
	defer srv.Close()

	pool := NewWorkerPool(WorkerPoolConfig{
		Store:             store,
		RestateIngressURL: srv.URL,
		DrainHTTPClient:   srv.Client(),
	})

	te := &turnExecution{
		pool:   pool,
		turnID: "turn-disp-replay",
	}

	subj := dispatchSubject{pool: pool, ids: []string{d1.ID}}
	ids, outcome, err := subj.load(context.Background(), store, te)
	require.NoError(t, err)
	assert.Empty(t, outcome)
	assert.ElementsMatch(t, []string{d1.ID, d2.ID, d3.ID}, ids)

	te.steerMu.Lock()
	assert.ElementsMatch(t, []string{d2.ID, d3.ID}, te.drainedIDs)
	assert.Contains(t, te.drainedPrompts, "db processing prompt")
	assert.Contains(t, te.drainedPrompts, "status pending prompt")
	te.steerMu.Unlock()

	// Verify d3 was claimed into PROCESSING in store
	gotD3, err := store.GetDispatch(context.Background(), d3.ID)
	require.NoError(t, err)
	require.NotNil(t, gotD3)
	assert.Equal(t, db.StatusProcessing, gotD3.Status)
}

func TestPostCrash_StopAndFailCleanup(t *testing.T) {
	t.Parallel()

	store := testStore(t)
	pool := NewWorkerPool(WorkerPoolConfig{Store: store})

	// 1. StopBurstTurn cleans up orphaned processing rows on thread
	th1 := "thread-cleanup-stop"
	m1 := db.Message{ID: "m-stop-1", ThreadID: th1, Content: "stop 1", Status: db.StatusProcessing, CreatedAt: time.Now().UTC()}
	m2 := db.Message{ID: "m-stop-2", ThreadID: th1, Content: "stop 2", Status: db.StatusProcessing, CreatedAt: time.Now().UTC().Add(time.Second)}
	seedMessageRows(t, store, []db.Message{m1, m2})

	err := pool.StopBurstTurn(context.Background(), "turn-stop-b", []string{m1.ID})
	require.NoError(t, err)
	rows1, err := store.GetMessagesByID(context.Background(), []string{m1.ID, m2.ID})
	require.NoError(t, err)
	for _, m := range rows1 {
		assert.Equal(t, db.StatusFailed, m.Status)
		assert.Equal(t, CancelledByOperatorReason, m.ErrorMessage)
	}

	// 2. FailBurstTurn cleans up orphaned processing rows on thread
	th2 := "thread-cleanup-fail"
	m3 := db.Message{ID: "m-fail-1", ThreadID: th2, Content: "fail 1", Status: db.StatusProcessing, CreatedAt: time.Now().UTC()}
	m4 := db.Message{ID: "m-fail-2", ThreadID: th2, Content: "fail 2", Status: db.StatusProcessing, CreatedAt: time.Now().UTC().Add(time.Second)}
	seedMessageRows(t, store, []db.Message{m3, m4})

	err = pool.FailBurstTurn(context.Background(), "turn-fail-b", []string{m3.ID}, "crash terminal reason")
	require.NoError(t, err)
	rows2, err := store.GetMessagesByID(context.Background(), []string{m3.ID, m4.ID})
	require.NoError(t, err)
	for _, m := range rows2 {
		assert.Equal(t, db.StatusFailed, m.Status)
		assert.Equal(t, "crash terminal reason", m.ErrorMessage)
	}

	// 3. StopDispatchTurn cleans up orphaned processing dispatches for session
	sess1 := "sess-cleanup-stop"
	d1 := db.Dispatch{ID: "d-stop-1", SessionID: sess1, Prompt: "stop 1", Status: db.StatusProcessing, CreatedAt: time.Now().UTC()}
	d2 := db.Dispatch{ID: "d-stop-2", SessionID: sess1, Prompt: "stop 2", Status: db.StatusProcessing, CreatedAt: time.Now().UTC().Add(time.Second)}
	seedDispatchRows(t, store, []db.Dispatch{d1, d2})

	err = pool.StopDispatchTurn(context.Background(), "turn-stop-d", []string{d1.ID})
	require.NoError(t, err)
	gotD1, err := store.GetDispatch(context.Background(), d1.ID)
	require.NoError(t, err)
	assert.Equal(t, db.StatusFailed, gotD1.Status)
	assert.Equal(t, CancelledByOperatorReason, gotD1.ErrorMessage)
	gotD2, err := store.GetDispatch(context.Background(), d2.ID)
	require.NoError(t, err)
	assert.Equal(t, db.StatusFailed, gotD2.Status)
	assert.Equal(t, CancelledByOperatorReason, gotD2.ErrorMessage)

	// 4. FailDispatchTurn cleans up orphaned processing dispatches for session
	sess2 := "sess-cleanup-fail"
	d3 := db.Dispatch{ID: "d-fail-1", SessionID: sess2, Prompt: "fail 1", Status: db.StatusProcessing, CreatedAt: time.Now().UTC()}
	d4 := db.Dispatch{ID: "d-fail-2", SessionID: sess2, Prompt: "fail 2", Status: db.StatusProcessing, CreatedAt: time.Now().UTC().Add(time.Second)}
	seedDispatchRows(t, store, []db.Dispatch{d3, d4})

	err = pool.FailDispatchTurn(context.Background(), "turn-fail-d", []string{d3.ID}, "crash terminal reason")
	require.NoError(t, err)
	gotD3, err := store.GetDispatch(context.Background(), d3.ID)
	require.NoError(t, err)
	assert.Equal(t, db.StatusFailed, gotD3.Status)
	assert.Equal(t, "crash terminal reason", gotD3.ErrorMessage)
	gotD4, err := store.GetDispatch(context.Background(), d4.ID)
	require.NoError(t, err)
	assert.Equal(t, db.StatusFailed, gotD4.Status)
	assert.Equal(t, "crash terminal reason", gotD4.ErrorMessage)
}

func TestTurnExecution_ApplyDrainedPrompts(t *testing.T) {
	t.Parallel()

	// 1. Drained prompts with blank values and distinct items
	te := &turnExecution{
		drainedIDs:     []string{"d1", "d2", "d3", "d4"},
		drainedPrompts: []string{"", "   ", "continue", "continue"},
	}
	te.applyDrainedPrompts(AttemptInput{
		Prep: TurnPrep{Prompt: "initial"},
		IDs:  []string{"d0"},
	})
	// Identical drained prompts ("continue") from distinct rows must both be included
	assert.Equal(t, "initial\n\ncontinue\n\ncontinue", te.turnPrompt)

	// 2. Row IDs already in AttemptInput.IDs (e.g. carried into in.IDs across retry) are deduplicated by ID, not substring
	te2 := &turnExecution{
		drainedIDs:     []string{"d1", "d2"},
		drainedPrompts: []string{"continue", "different prompt"},
	}
	te2.applyDrainedPrompts(AttemptInput{
		Prep: TurnPrep{Prompt: "initial prompt with continue in it"},
		IDs:  []string{"d0", "d1"},
	})
	// d1 is already in in.IDs, so it is skipped. d2 is not in in.IDs, so it is appended.
	assert.Equal(t, "initial prompt with continue in it\n\ndifferent prompt", te2.turnPrompt)
}

func TestDrainDispatchTurn_BlankPromptAlignment(t *testing.T) {
	t.Parallel()

	store := testStore(t)
	d1 := db.Dispatch{ID: "disp-blank", SessionID: "sess-align", Prompt: "   ", Status: db.StatusPending, CreatedAt: time.Now().UTC()}
	d2 := db.Dispatch{ID: "disp-valid", SessionID: "sess-align", Prompt: "actionable steer prompt", Status: db.StatusPending, CreatedAt: time.Now().UTC().Add(time.Second)}
	seedDispatchRows(t, store, []db.Dispatch{d1, d2})

	pool := NewWorkerPool(WorkerPoolConfig{Store: store})
	te := &turnExecution{
		turnID:   "turn-align",
		threadID: "sess-align",
	}

	// 1. Draining returns only non-blank prompts in HTTP response, but aligns drainedIDs and drainedPrompts 1:1
	prompts := pool.drainDispatchTurn(context.Background(), te, store, []string{d1.ID, d2.ID})
	assert.Equal(t, []string{"actionable steer prompt"}, prompts)

	te.steerMu.Lock()
	assert.Equal(t, []string{d1.ID, d2.ID}, te.drainedIDs)
	assert.Equal(t, []string{"", "actionable steer prompt"}, te.drainedPrompts)
	te.steerMu.Unlock()

	// 2. When in.IDs already contains disp-blank (e.g. from an earlier attempt), disp-valid's prompt is still applied
	te.applyDrainedPrompts(AttemptInput{
		Prep: TurnPrep{Prompt: "initial prompt"},
		IDs:  []string{"d0", d1.ID},
	})
	assert.Equal(t, "initial prompt\n\nactionable steer prompt", te.turnPrompt)
}

func TestPostCrash_StopAndFail_WithLiveExecution(t *testing.T) {
	t.Parallel()

	store := testStore(t)
	pool := NewWorkerPool(WorkerPoolConfig{Store: store})

	// Live burst execution with drainedIDs
	th := "thread-live-stop"
	m1 := db.Message{ID: "m-live-1", ThreadID: th, Status: db.StatusProcessing, CreatedAt: time.Now().UTC()}
	m2 := db.Message{ID: "m-live-2", ThreadID: th, Status: db.StatusProcessing, CreatedAt: time.Now().UTC().Add(time.Second)}
	seedMessageRows(t, store, []db.Message{m1, m2})

	teBurst := &turnExecution{
		turnID:     "turn-live-burst",
		threadID:   th,
		drainedIDs: []string{m2.ID},
	}
	relBurst, ok := pool.turns.Begin(context.Background(), teBurst.turnID, teBurst)
	require.True(t, ok)
	defer relBurst()

	err := pool.StopBurstTurn(context.Background(), teBurst.turnID, []string{m1.ID})
	require.NoError(t, err)
	gotM1, err := store.GetMessage(context.Background(), m1.ID)
	require.NoError(t, err)
	assert.Equal(t, db.StatusFailed, gotM1.Status)
	gotM2, err := store.GetMessage(context.Background(), m2.ID)
	require.NoError(t, err)
	assert.Equal(t, db.StatusFailed, gotM2.Status)

	// Live dispatch execution with drainedIDs
	sess := "sess-live-disp"
	d1 := db.Dispatch{ID: "d-live-1", SessionID: sess, Status: db.StatusProcessing, CreatedAt: time.Now().UTC()}
	d2 := db.Dispatch{ID: "d-live-2", SessionID: sess, Status: db.StatusProcessing, CreatedAt: time.Now().UTC().Add(time.Second)}
	seedDispatchRows(t, store, []db.Dispatch{d1, d2})

	teDisp := &turnExecution{
		turnID:     "turn-live-disp",
		threadID:   sess,
		dispatch:   &d1,
		drainedIDs: []string{d2.ID},
	}
	relDisp, ok := pool.turns.Begin(context.Background(), teDisp.turnID, teDisp)
	require.True(t, ok)
	defer relDisp()

	err = pool.StopDispatchTurn(context.Background(), teDisp.turnID, []string{d1.ID})
	require.NoError(t, err)
	gotD1, err := store.GetDispatch(context.Background(), d1.ID)
	require.NoError(t, err)
	assert.Equal(t, db.StatusFailed, gotD1.Status)
	gotD2, err := store.GetDispatch(context.Background(), d2.ID)
	require.NoError(t, err)
	assert.Equal(t, db.StatusFailed, gotD2.Status)
}
