package router

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestEventTriggersDelegatesAndNilGuards(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	var nilSrv *RouterServer
	if err := nilSrv.evaluateEventTriggers(ctx, "src", "push", []string{"t1"}, "hdr"); err == nil {
		t.Fatalf("expected error from evaluateEventTriggers with nil server")
	}
	nilSrv.evaluateFluxEventTriggers(ctx, DeployEvent{})
	nilSrv.evaluateGitHubEventTriggers(ctx, nil, nil)

	noBusSrv := &RouterServer{}
	if err := noBusSrv.evaluateEventTriggers(ctx, "src", "push", []string{"t1"}, "hdr"); err == nil {
		t.Fatalf("expected error from evaluateEventTriggers with nil bus")
	}
	noBusSrv.evaluateFluxEventTriggers(ctx, DeployEvent{})
	noBusSrv.evaluateGitHubEventTriggers(ctx, &ResolvedGitHubEvent{Event: "push"}, nil)

	bus := &mockEventBus{}
	busSrv := newTestRouter(Config{}, nil, nil, bus)
	if err := busSrv.evaluateEventTriggers(ctx, "src", "push", []string{"t1"}, "hdr"); err != nil {
		t.Fatalf("evaluateEventTriggers error: %v", err)
	}
	busSrv.evaluateFluxEventTriggers(ctx, DeployEvent{Event: "sync_success", Repo: "example/aerial"})
	busSrv.evaluateGitHubEventTriggers(ctx, &ResolvedGitHubEvent{Event: "pull_request", Action: "closed", Merged: true, Repo: "example/aerial", Branch: "main"}, nil)
}

func TestAlertsWebhook_InvalidTargetChannel(t *testing.T) {
	t.Parallel()

	cfg := Config{
		SystemChannelID: "invalid-target-snowflake",
	}
	srv := newTestRouter(cfg, nil, nil)

	req := httptest.NewRequest(http.MethodPost, "/api/v2/alerts", bytes.NewBufferString(`[{"status":"firing","labels":{"alertname":"DiskFull"}}]`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	srv.handleAlertsWebhook(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("handleAlertsWebhook code = %d, want %d", w.Code, http.StatusServiceUnavailable)
	}
}

func TestEventsWebhook_Success(t *testing.T) {
	t.Parallel()

	bus := &mockEventBus{}
	cfg := Config{}
	srv := newTestRouter(cfg, nil, nil, bus)

	body := `{"source":"ci","event_type":"build","target":"api"}`
	req := httptest.NewRequest(http.MethodPost, "/api/webhooks/events", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	srv.handleEventsWebhook(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("handleEventsWebhook code = %d, want %d", w.Code, http.StatusOK)
	}

	req2 := httptest.NewRequest(http.MethodPost, "/api/webhooks/events", bytes.NewBufferString(body))
	req2.RequestURI = ""
	req2.Header.Set("Content-Type", "application/json")
	w2 := httptest.NewRecorder()
	srv.handleEventsWebhook(w2, req2)
	if w2.Code != http.StatusOK {
		t.Fatalf("handleEventsWebhook code = %d, want %d", w2.Code, http.StatusOK)
	}
}

func TestEventsWebhook_ErrorCases(t *testing.T) {
	t.Parallel()

	bus := &mockEventBus{}
	srv := newTestRouter(Config{}, nil, nil, bus)

	// 0. Body read error
	reqErr := httptest.NewRequest(http.MethodPost, "/api/webhooks/events", errReader{})
	wErr := httptest.NewRecorder()
	srv.handleEventsWebhook(wErr, reqErr)
	if wErr.Code != http.StatusBadRequest {
		t.Fatalf("read error code = %d, want %d", wErr.Code, http.StatusBadRequest)
	}

	// 1. Invalid JSON
	req := httptest.NewRequest(http.MethodPost, "/api/webhooks/events", bytes.NewBufferString("{invalid-json"))
	w := httptest.NewRecorder()
	srv.handleEventsWebhook(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("invalid json code = %d, want %d", w.Code, http.StatusBadRequest)
	}

	// 2. Missing required fields
	req = httptest.NewRequest(http.MethodPost, "/api/webhooks/events", bytes.NewBufferString(`{"source":""}`))
	w = httptest.NewRecorder()
	srv.handleEventsWebhook(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("missing fields code = %d, want %d", w.Code, http.StatusBadRequest)
	}

	// 3. Bus nil
	noBusSrv := newTestRouter(Config{}, nil, nil, nil)
	req = httptest.NewRequest(http.MethodPost, "/api/webhooks/events", bytes.NewBufferString(`{"source":"ci","event_type":"push","target":"t1"}`))
	w = httptest.NewRecorder()
	noBusSrv.handleEventsWebhook(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("nil bus code = %d, want %d", w.Code, http.StatusServiceUnavailable)
	}

	// 4. evaluateEventTriggers error
	errBus := &mockEventBus{err: errors.New("bus fail")}
	errSrv := newTestRouter(Config{}, nil, nil, errBus)
	req = httptest.NewRequest(http.MethodPost, "/api/webhooks/events", bytes.NewBufferString(`{"source":"ci","event_type":"push","target":"t1"}`))
	w = httptest.NewRecorder()
	errSrv.handleEventsWebhook(w, req)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("bus err code = %d, want %d", w.Code, http.StatusInternalServerError)
	}

	// 5. Success with Details and empty context header
	req = httptest.NewRequest(http.MethodPost, "/api/webhooks/events", bytes.NewBufferString(`{"source":"ci","event_type":"push","target":"t1","details":"build extra info"}`))
	w = httptest.NewRecorder()
	srv.handleEventsWebhook(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("details success code = %d, want %d", w.Code, http.StatusOK)
	}

	// 6. Success with Targets slice and explicit context header
	req = httptest.NewRequest(http.MethodPost, "/api/webhooks/events", bytes.NewBufferString(`{"source":"ci","event_type":"push","targets":["t1","t2"],"context_header":"Custom Header"}`))
	w = httptest.NewRecorder()
	srv.handleEventsWebhook(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("custom header code = %d, want %d", w.Code, http.StatusOK)
	}
}

func TestAlertsWebhook_DispatchLifecycleAndErrors(t *testing.T) {
	t.Parallel()

	targetID := "123456789012345678"
	cfg := Config{SystemChannelID: targetID}

	// 1. Invalid JSON body
	srv := newTestRouter(cfg, nil, nil)
	req := httptest.NewRequest(http.MethodPost, "/api/v2/alerts", bytes.NewBufferString(`{not-json-array`))
	w := httptest.NewRecorder()
	srv.handleAlertsWebhook(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("invalid json code = %d, want %d", w.Code, http.StatusBadRequest)
	}

	// 2. Dispatcher nil
	srvNoDisp := newTestRouter(cfg, nil, nil)
	req = httptest.NewRequest(http.MethodPost, "/api/v2/alerts", bytes.NewBufferString(`[{"status":"firing","labels":{"alertname":"DiskFull"}}]`))
	w = httptest.NewRecorder()
	srvNoDisp.handleAlertsWebhook(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("nil disp code = %d, want %d", w.Code, http.StatusServiceUnavailable)
	}

	// 2b. Config nil with non-nil dispatcher
	disp := &mockOutboundDispatcher{}
	srvNoCfg := &RouterServer{dispatcher: disp}
	req = httptest.NewRequest(http.MethodPost, "/api/v2/alerts", bytes.NewBufferString(`[{"status":"firing","labels":{"alertname":"DiskFull"}}]`))
	w = httptest.NewRecorder()
	srvNoCfg.handleAlertsWebhook(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("nil cfg code = %d, want %d", w.Code, http.StatusServiceUnavailable)
	}

	// 3. Successful dispatch, resolved, non-firing, duplicate firing
	disp = &mockOutboundDispatcher{}
	srvSuccess := newTestRouter(cfg, nil, nil)
	srvSuccess.SetDispatcher(disp)

	payload := `[
		{"status":"resolved","endsAt":"2020-01-01T00:00:00Z","labels":{"alertname":"DiskFull"}},
		{"status":"pending","labels":{"alertname":"PendingAlert"}},
		{"status":"firing","labels":{"alertname":"HighCPU"}},
		{"status":"firing","labels":{"alertname":"HighCPU"}}
	]`
	req = httptest.NewRequest(http.MethodPost, "/api/v2/alerts", bytes.NewBufferString(payload))
	w = httptest.NewRecorder()
	srvSuccess.handleAlertsWebhook(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", w.Code, http.StatusOK)
	}
	if len(disp.directMessageCalls) != 1 {
		t.Fatalf("directMessageCalls = %d, want 1", len(disp.directMessageCalls))
	}

	// 4. Dispatcher error
	errDisp := &mockOutboundDispatcher{directMessageErr: errors.New("failed")}
	srvErr := newTestRouter(cfg, nil, nil)
	srvErr.SetDispatcher(errDisp)
	req = httptest.NewRequest(http.MethodPost, "/api/v2/alerts", bytes.NewBufferString(`[{"status":"firing","labels":{"alertname":"OOM"}}]`))
	w = httptest.NewRecorder()
	srvErr.handleAlertsWebhook(w, req)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("disp error code = %d, want %d", w.Code, http.StatusInternalServerError)
	}
}
