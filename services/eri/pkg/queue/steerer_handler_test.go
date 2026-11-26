package queue

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

type mockFailingTransport struct {
	gotURL string
}

func (m *mockFailingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req != nil && req.URL != nil {
		m.gotURL = req.URL.String()
	}
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
	name        string
	method      string
	body        string
	noMatch     bool
	closed      bool
	finalized   bool
	interrupted bool
	wantStatus  int
	wantSubstr  string
}

func setupValidationTestPool(tc handlerValidationCase) (*WorkerPool, func()) {
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

			pool.HandleDrainPending(rec, req)

			assert.Equal(t, tc.wantStatus, rec.Code)
			if tc.wantSubstr != "" {
				assert.Contains(t, rec.Body.String(), tc.wantSubstr)
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
		transport := &mockFailingTransport{}
		pool := NewWorkerPool(WorkerPoolConfig{
			RestateIngressURL: "",
			DrainHTTPClient:   &http.Client{Transport: transport},
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
		assert.Equal(t, "http://restate.aerial:8080/ThreadSession/thread-123/DrainPending", transport.gotURL)
	})
}

func TestHandleDrainPending_BurstTurn(t *testing.T) {
	t.Parallel()

	store := testStore(t)
	m1 := db.Message{ID: "msg-steer-1", ThreadID: "thread-steer-burst", Content: "initial prompt", Status: db.StatusProcessing, CreatedAt: time.Now().UTC()}
	m2 := db.Message{ID: "msg-steer-2", ThreadID: "thread-steer-burst", Content: "steer msg A", Status: db.StatusPending, CreatedAt: time.Now().UTC().Add(time.Second)}
	m3 := db.Message{ID: "msg-steer-3", ThreadID: "thread-steer-burst", Content: "steer msg B", Status: db.StatusPending, CreatedAt: time.Now().UTC().Add(2 * time.Second)}
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

	te.steerMu.Lock()
	assert.Equal(t, []string{m2.ID, m3.ID}, te.drainedIDs)
	assert.Len(t, te.steeredMessages, 2)
	te.steerMu.Unlock()
	assert.Len(t, te.burst, 1, "active burst slice must not be mutated while steering is open")

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

	mux := http.NewServeMux()
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

	// Non-POST method (GET) rejected with 405 Method Not Allowed by mux pattern
	reqGET := httptest.NewRequest(http.MethodGet, SteerDrainPath, http.NoBody)
	reqGET.RemoteAddr = "127.0.0.1:54321"
	recGET := httptest.NewRecorder()
	mux.ServeHTTP(recGET, reqGET)
	assert.Equal(t, http.StatusMethodNotAllowed, recGET.Code)
}

func TestDrainHTTPClient_DefaultClientUsedByHandler(t *testing.T) {
	t.Parallel()

	var receivedPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		assert.NoError(t, json.NewEncoder(w).Encode(DrainPendingResponse{DrainedIDs: []string{}}))
	}))
	defer srv.Close()

	// DrainHTTPClient is intentionally nil to test default client fallback
	pool := NewWorkerPool(WorkerPoolConfig{
		RestateIngressURL: srv.URL,
	})

	te := &turnExecution{
		turnID:           "turn-default-client",
		threadID:         "thread-default-client",
		activeTurnDaemon: "daemon-default-client",
	}
	release, ok := pool.turns.Begin(context.Background(), te.turnID, te)
	require.True(t, ok)
	defer release()

	body, err := json.Marshal(SteerDrainRequest{DaemonID: "daemon-default-client"})
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, SteerDrainPath, bytes.NewReader(body))
	rec := httptest.NewRecorder()

	pool.HandleDrainPending(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "/ThreadSession/thread-default-client/DrainPending", receivedPath)
	var resp SteerDrainResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Empty(t, resp.Prompts)
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

type faultDrainStore struct {
	*db.SQLStore

	failClaimMsg  bool
	failReadMsg   bool
	failClaimDisp bool
	failReadDisp  bool
}

func (f *faultDrainStore) ClaimPendingMessage(ctx context.Context, id string) (bool, error) {
	if f.failClaimMsg {
		return false, errors.New("simulated claim message failure")
	}
	return f.SQLStore.ClaimPendingMessage(ctx, id)
}

func (f *faultDrainStore) GetMessagesByID(ctx context.Context, ids []string) ([]db.Message, error) {
	if f.failReadMsg {
		return nil, errors.New("simulated read messages failure")
	}
	return f.SQLStore.GetMessagesByID(ctx, ids)
}

func (f *faultDrainStore) ClaimPendingDispatch(ctx context.Context, id string) (bool, error) {
	if f.failClaimDisp {
		return false, errors.New("simulated claim dispatch failure")
	}
	return f.SQLStore.ClaimPendingDispatch(ctx, id)
}

func (f *faultDrainStore) GetDispatch(ctx context.Context, id string) (*db.Dispatch, error) {
	if f.failReadDisp {
		return nil, errors.New("simulated read dispatch failure")
	}
	return f.SQLStore.GetDispatch(ctx, id)
}

func TestHandleDrainPending_LostResponse_ClaimFailure_ReadFailure_AndConcurrentExecution(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	baseStore := testStore(t)
	fStore := &faultDrainStore{SQLStore: baseStore}

	threadID := "thread-handoff-burst"
	m1 := db.Message{ID: "m-h-1", ThreadID: threadID, Content: "initial", Status: db.StatusProcessing, CreatedAt: time.Now().UTC()}
	m2 := db.Message{ID: "m-h-2", ThreadID: threadID, Content: "steered burst", Status: db.StatusPending, CreatedAt: time.Now().UTC().Add(time.Second)}
	m3 := db.Message{ID: "m-h-3", ThreadID: threadID, Content: "undelivered on finish", Status: db.StatusPending, CreatedAt: time.Now().UTC().Add(2 * time.Second)}
	seedMessageRows(t, baseStore, []db.Message{m1, m2, m3})

	// Drive real ThreadCoordinator.DrainPending through an httptest server to exercise durable draining state.
	h := newCoordHarness(t, threadID, tcInv, coordinatorState{
		pending: []string{m2.ID},
		active:  &ActiveTurn{TurnID: "turn-handoff-1", ItemIDs: []string{m1.ID}, RunnerInvocationID: "inv-h1"},
	})
	coord := NewThreadCoordinator(baseStore, nil, "aerial")
	dropNextResponse := true
	var ackedSeen []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var dReq DrainPendingRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&dReq))
		if len(dReq.AckIDs) > 0 {
			ackedSeen = append(ackedSeen, dReq.AckIDs...)
		}
		dResp, err := coord.DrainPending(h.objectCtx(), dReq)
		require.NoError(t, err)
		if dropNextResponse {
			dropNextResponse = false
			http.Error(w, "lost response after commit", http.StatusBadGateway)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(dResp))
	}))
	defer srv.Close()

	pool := NewWorkerPool(WorkerPoolConfig{
		Store:             fStore,
		RestateIngressURL: srv.URL,
		DrainHTTPClient:   srv.Client(),
		AgentName:         "aerial",
	})
	te := &turnExecution{
		turnID:           "turn-handoff-1",
		threadID:         threadID,
		activeTurnDaemon: "daemon-handoff-1",
		burst:            []db.Message{m1},
	}
	release, ok := pool.turns.Begin(ctx, te.turnID, te)
	require.True(t, ok)
	defer release()

	reqBody, err := json.Marshal(SteerDrainRequest{DaemonID: "daemon-handoff-1"})
	require.NoError(t, err)

	// 1. Lost drain response after Restate committed DrainPending: m2 is in st.draining and still PENDING in DB.
	rec1 := httptest.NewRecorder()
	pool.HandleDrainPending(rec1, httptest.NewRequest(http.MethodPost, SteerDrainPath, bytes.NewReader(reqBody)))
	assert.Equal(t, http.StatusBadGateway, rec1.Code)
	assert.Equal(t, []string{m2.ID}, h.snapshot(t).draining)
	assertMessageRows(t, baseStore, map[string]rowWant{m2.ID: {status: db.StatusPending}})

	// 2. Claim failure: coordinator repeats m2 from st.draining, claim fails -> 500, still unacknowledged.
	fStore.failClaimMsg = true
	rec2 := httptest.NewRecorder()
	pool.HandleDrainPending(rec2, httptest.NewRequest(http.MethodPost, SteerDrainPath, bytes.NewReader(reqBody)))
	assert.Equal(t, http.StatusInternalServerError, rec2.Code)
	assert.Equal(t, []string{m2.ID}, h.snapshot(t).draining)
	assertMessageRows(t, baseStore, map[string]rowWant{m2.ID: {status: db.StatusPending}})
	fStore.failClaimMsg = false

	// 3. Read failure after claim: m2 is claimed to PROCESSING, then GetMessagesByID fails -> 500, still unacknowledged.
	fStore.failReadMsg = true
	rec3 := httptest.NewRecorder()
	pool.HandleDrainPending(rec3, httptest.NewRequest(http.MethodPost, SteerDrainPath, bytes.NewReader(reqBody)))
	assert.Equal(t, http.StatusInternalServerError, rec3.Code)
	assert.Equal(t, []string{m2.ID}, h.snapshot(t).draining)
	assertMessageRows(t, baseStore, map[string]rowWant{m2.ID: {status: db.StatusProcessing}})
	fStore.failReadMsg = false

	// 4. Retry after read failure: adopts already-PROCESSING m2, delivers prompt, and acknowledges m2.
	rec4 := httptest.NewRecorder()
	pool.HandleDrainPending(rec4, httptest.NewRequest(http.MethodPost, SteerDrainPath, bytes.NewReader(reqBody)))
	require.Equal(t, http.StatusOK, rec4.Code)
	var resp4 SteerDrainResponse
	require.NoError(t, json.NewDecoder(rec4.Body).Decode(&resp4))
	require.Len(t, resp4.Prompts, 1)
	assert.Contains(t, resp4.Prompts[0], "steered burst")
	assert.Empty(t, h.snapshot(t).draining)
	assert.Contains(t, ackedSeen, m2.ID)

	// 5. Undelivered row on turn finish: append m3, fail read after claim, then finish attempt via ensureCompleted + OnTurnDone.
	require.NoError(t, coord.Append(h.objectCtx(), AppendRequest{IDs: []string{m3.ID}}))
	fStore.failReadMsg = true
	rec5 := httptest.NewRecorder()
	pool.HandleDrainPending(rec5, httptest.NewRequest(http.MethodPost, SteerDrainPath, bytes.NewReader(reqBody)))
	assert.Equal(t, http.StatusInternalServerError, rec5.Code)
	assertMessageRows(t, baseStore, map[string]rowWant{m3.ID: {status: db.StatusProcessing}})
	fStore.failReadMsg = false

	// Attempt finishes without delivering m3: ensureCompleted requeues m3 to PENDING, OnTurnDone starts next turn for m3.
	require.NoError(t, (burstSubject{pool: pool}).ensureCompleted(ctx, baseStore, te, []string{m1.ID, m2.ID}))
	assertMessageRows(t, baseStore, map[string]rowWant{
		m1.ID: {status: db.StatusCompleted},
		m2.ID: {status: db.StatusCompleted},
		m3.ID: {status: db.StatusPending},
	})
	require.NoError(t, coord.OnTurnDone(h.objectCtx(), TurnDoneRequest{TurnID: "turn-handoff-1", Outcome: OutcomeCompleted}))
	require.NotNil(t, h.snapshot(t).active)
	assert.Equal(t, []string{m3.ID}, h.snapshot(t).active.ItemIDs)
}

func TestHandleDrainPending_ConcurrentDuringExecutingAttempt(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	baseStore := testStore(t)
	raceThread := "thread-race-exec"
	require.NoError(t, baseStore.SaveSessionID(ctx, raceThread, "44444444-4444-4444-4444-444444444444"))
	mr1 := db.Message{ID: "m-race-1", ThreadID: raceThread, Content: "start", Status: db.StatusProcessing, CreatedAt: time.Now().UTC()}
	mr2 := db.Message{ID: "m-race-2", ThreadID: raceThread, Content: "mid-turn steer", Status: db.StatusPending, CreatedAt: time.Now().UTC().Add(time.Second)}
	seedMessageRows(t, baseStore, []db.Message{mr1, mr2})

	raceSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var dReq DrainPendingRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&dReq))
		w.Header().Set("Content-Type", "application/json")
		if len(dReq.AckIDs) > 0 {
			require.NoError(t, json.NewEncoder(w).Encode(DrainPendingResponse{}))
			return
		}
		require.NoError(t, json.NewEncoder(w).Encode(DrainPendingResponse{DrainedIDs: []string{mr2.ID}}))
	}))
	defer raceSrv.Close()

	runnerStarted := make(chan struct{})
	drainCompleted := make(chan struct{})
	racePool := NewWorkerPool(WorkerPoolConfig{
		Store:             baseStore,
		RestateIngressURL: raceSrv.URL,
		DrainHTTPClient:   raceSrv.Client(),
		DeliveryFunc:      (&noticeRecorder{}).deliver,
		RunnerFunc: func(_ context.Context, _, _, _, _, _ string, _ int) (string, string, int, error) {
			close(runnerStarted)
			<-drainCompleted
			return "race turn done", "", 0, nil
		},
	})

	go func() {
		<-runnerStarted
		b, mErr := json.Marshal(SteerDrainRequest{SessionID: "44444444-4444-4444-4444-444444444444"})
		assert.NoError(t, mErr)
		rec := httptest.NewRecorder()
		racePool.HandleDrainPending(rec, httptest.NewRequest(http.MethodPost, SteerDrainPath, bytes.NewReader(b)))
		assert.Equal(t, http.StatusOK, rec.Code)
		close(drainCompleted)
	}()

	attemptRes, err := racePool.RunBurstAttempt(ctx, AttemptInput{
		ThreadID: raceThread,
		TurnID:   "turn-race-1",
		IDs:      []string{mr1.ID},
		Ordinal:  1,
		RunIndex: 1,
		Prep:     TurnPrep{Decision: TurnDecisionRun, SessionID: "44444444-4444-4444-4444-444444444444", Model: "gemini-2.5-flash", Prompt: "start"},
	})
	require.NoError(t, err)
	assert.Equal(t, AttemptKindSuccess, attemptRes.Kind)
	assert.Equal(t, []string{mr2.ID}, attemptRes.DrainedIDs)
	assertMessageRows(t, baseStore, map[string]rowWant{
		mr1.ID: {status: db.StatusCompleted},
		mr2.ID: {status: db.StatusCompleted},
	})
}
