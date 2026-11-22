package router

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
)

type eventTracker struct {
	eventsChan chan pubsub.ExternalEvent
	topicsChan chan string
}

func newEventTracker(t *testing.T, bus pubsub.Bus, topic, subID string) *eventTracker {
	t.Helper()
	tr := &eventTracker{
		eventsChan: make(chan pubsub.ExternalEvent, 20),
		topicsChan: make(chan string, 20),
	}
	_, err := bus.Subscribe(context.Background(), topic, subID, func(_ context.Context, msg pubsub.Message) error {
		var ev pubsub.ExternalEvent
		if err := json.Unmarshal(msg.Data(), &ev); err == nil {
			tr.eventsChan <- ev
			tr.topicsChan <- msg.Subject()
		}
		return msg.Ack()
	})
	if err != nil {
		t.Fatalf("failed to subscribe to memory bus: %v", err)
	}
	return tr
}

func (tr *eventTracker) waitEvents(t *testing.T, count int) ([]pubsub.ExternalEvent, []string) {
	t.Helper()
	var events []pubsub.ExternalEvent
	var topics []string
	for i := range count {
		select {
		case ev := <-tr.eventsChan:
			events = append(events, ev)
			topics = append(topics, <-tr.topicsChan)
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for event %d of %d", i+1, count)
		}
	}
	return events, topics
}

func verifyPublishedEvents(t *testing.T, tracker *eventTracker, wantCount int, wantTopic string, wantTargets []string, wantHeaderSubstr string) {
	t.Helper()
	if tracker == nil || wantCount <= 0 {
		return
	}
	events, topics := tracker.waitEvents(t, wantCount)
	if len(events) != wantCount {
		t.Errorf("published event count = %d, want %d", len(events), wantCount)
	}
	if wantTopic != "" && len(topics) > 0 && topics[0] != wantTopic {
		t.Errorf("published topic = %s, want %s", topics[0], wantTopic)
	}
	if len(wantTargets) > 0 && len(events) > 0 && !slices.Equal(events[0].Targets, wantTargets) {
		t.Errorf("published targets = %v, want %v", events[0].Targets, wantTargets)
	}
	if wantHeaderSubstr != "" && len(events) > 0 && !strings.Contains(events[0].ContextHeader, wantHeaderSubstr) {
		t.Errorf("published context_header = %q, want substring %q", events[0].ContextHeader, wantHeaderSubstr)
	}
}

type recordingBus struct {
	pubsub.Bus

	mu        sync.Mutex
	publishes []string
	events    []pubsub.ExternalEvent
}

func (b *recordingBus) Publish(ctx context.Context, subject, msgID string, payload any) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.publishes = append(b.publishes, subject)
	if ev, ok := payload.(pubsub.ExternalEvent); ok {
		b.events = append(b.events, ev)
	}
	return nil
}

func (b *recordingBus) publishCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.publishes)
}

type evaluateEventTriggersTestCase struct {
	name          string
	nilServer     bool
	useBus        bool
	useSyncBus    bool
	closeBusFirst bool
	source        string
	eventType     string
	targets       []string
	contextHeader string
	wantErr       bool
	wantPubCount  int
	wantPubTopic  string
}

func assertEvaluateEventTriggersScenario(t *testing.T, tc evaluateEventTriggersTestCase) {
	t.Helper()
	srv := NewRouterServer(fixedConfig(Config{}), nil)

	var tracker *eventTracker
	var recBus *recordingBus

	if tc.useSyncBus {
		recBus = &recordingBus{}
		srv.SetEventBus(recBus)
	} else if tc.useBus {
		memBus := pubsub.NewMemoryBus()
		defer memBus.Close()
		tracker = newEventTracker(t, memBus, "aerial.events.>", "test-eval-events")
		if tc.closeBusFirst {
			_ = memBus.Close()
		}
		srv.SetEventBus(memBus)
	}

	if tc.nilServer {
		srv = nil
	}
	err := srv.evaluateEventTriggers(context.Background(), tc.source, tc.eventType, tc.targets, tc.contextHeader)
	if (err != nil) != tc.wantErr {
		t.Fatalf("evaluateEventTriggers() err = %v, wantErr = %v", err, tc.wantErr)
	}

	if recBus != nil && recBus.publishCount() != tc.wantPubCount {
		t.Errorf("recordingBus publish count = %d, want %d", recBus.publishCount(), tc.wantPubCount)
	}
	verifyPublishedEvents(t, tracker, tc.wantPubCount, tc.wantPubTopic, nil, tc.contextHeader)
}

func TestEvaluateEventTriggers_Scenarios(t *testing.T) {
	t.Parallel()

	tests := []evaluateEventTriggersTestCase{
		{
			name:          "publishes to jetstream when bus is present",
			useBus:        true,
			source:        "camera",
			eventType:     "motion",
			targets:       []string{"driveway"},
			contextHeader: "Driveway motion detected",
			wantPubCount:  1,
			wantPubTopic:  "aerial.events.camera.motion",
		},
		{
			name:          "sanitizes subject tokens with spaces or special characters",
			useBus:        true,
			source:        "camera/front",
			eventType:     "person detected*",
			targets:       []string{"porch"},
			contextHeader: "Person detected on porch",
			wantPubCount:  1,
			wantPubTopic:  "aerial.events.camera_front.person_detected_",
		},
		{
			name:         "empty targets is a no-op",
			useSyncBus:   true,
			source:       "camera",
			eventType:    "motion",
			targets:      []string{"   ", ""},
			wantPubCount: 0,
		},
		{
			name:         "empty source is a no-op",
			useSyncBus:   true,
			source:       "",
			eventType:    "motion",
			targets:      []string{"driveway"},
			wantPubCount: 0,
		},
		{
			name:         "empty event type is a no-op",
			useSyncBus:   true,
			source:       "camera",
			eventType:    "",
			targets:      []string{"driveway"},
			wantPubCount: 0,
		},
		{name: "nil server returns error", nilServer: true, source: "camera", eventType: "motion", targets: []string{"driveway"}, wantErr: true},
		{name: "server without a bus returns error", source: "camera", eventType: "motion", targets: []string{"driveway"}, wantErr: true},
		{
			name:          "bus error returns error without fallback",
			useBus:        true,
			closeBusFirst: true,
			source:        "custom",
			eventType:     "state_change",
			targets:       []string{"sensor.temp"},
			contextHeader: "Temperature spike",
			wantErr:       true,
			wantPubCount:  0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertEvaluateEventTriggersScenario(t, tc)
		})
	}
}

func TestEvaluateGitHubEventTriggers(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		res  *ResolvedGitHubEvent
		want []string
	}{
		{name: "nil result fires nothing", res: nil, want: []string{}},
		{name: "merged pull request fires pr_merged", res: &ResolvedGitHubEvent{Event: "pull_request", Action: "closed", Merged: true, Repo: "example/aerial", Branch: "main"}, want: []string{"github:pr_merged"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bus := &recordingBus{}
			srv := newTestRouter(Config{}, nil, nil, bus)
			srv.evaluateGitHubEventTriggers(context.Background(), tc.res, nil)
			got := make([]string, 0, len(bus.events))
			for _, ev := range bus.events {
				got = append(got, ev.Source+":"+ev.EventType)
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("trigger events = %v, want %v", got, tc.want)
			}
		})
	}
}

type handleEventsWebhookTestCase struct {
	name             string
	payload          string
	body             io.Reader
	useBus           bool
	closeBus         bool
	wantStatus       int
	wantPubCount     int
	wantPubTopic     string
	wantTargets      []string
	wantHeaderSubstr string
}

func assertHandleEventsWebhookScenario(t *testing.T, tc handleEventsWebhookTestCase) {
	t.Helper()
	cfg := Config{}
	client := &http.Client{Transport: &zeroCallTransport{}}
	srv := NewRouterServer(fixedConfig(cfg), client)

	var tracker *eventTracker
	if tc.useBus {
		memBus := pubsub.NewMemoryBus()
		defer memBus.Close()
		tracker = newEventTracker(t, memBus, "aerial.events.>", "test-events-webhook")
		if tc.closeBus {
			_ = memBus.Close()
		}
		srv.SetEventBus(memBus)
	}

	body := tc.body
	if body == nil {
		body = strings.NewReader(tc.payload)
	}
	req := httptest.NewRequest(http.MethodPost, "/webhooks/events", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	srv.handleEventsWebhook(w, req)

	if w.Code != tc.wantStatus {
		t.Fatalf("status code = %d, want %d: body=%s", w.Code, tc.wantStatus, w.Body.String())
	}

	if tc.useBus && !tc.closeBus {
		verifyPublishedEvents(t, tracker, tc.wantPubCount, tc.wantPubTopic, tc.wantTargets, tc.wantHeaderSubstr)
	}
}

func TestHandleEventsWebhook_Scenarios(t *testing.T) {
	t.Parallel()

	tests := []handleEventsWebhookTestCase{
		{
			name:             "valid event webhook publishes to jetstream",
			payload:          `{"source":"camera","event_type":"motion","targets":["driveway"],"context_header":"Driveway motion"}`,
			useBus:           true,
			wantStatus:       http.StatusOK,
			wantPubCount:     1,
			wantPubTopic:     "aerial.events.camera.motion",
			wantTargets:      []string{"driveway"},
			wantHeaderSubstr: "Driveway motion",
		},
		{
			name:             "valid event with target singular and details formats context header",
			payload:          `{"source":"camera","event_type":"motion","target":"backyard","details":"high confidence"}`,
			useBus:           true,
			wantStatus:       http.StatusOK,
			wantPubCount:     1,
			wantPubTopic:     "aerial.events.camera.motion",
			wantTargets:      []string{"backyard"},
			wantHeaderSubstr: "External camera event motion on backyard: high confidence",
		},
		{name: "unreadable body returns 400", body: errReader{}, wantStatus: http.StatusBadRequest},
		{
			name:       "invalid json returns 400",
			payload:    `{invalid`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing source returns 400",
			payload:    `{"source":"","event_type":"motion","target":"driveway"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing targets returns 400",
			payload:    `{"source":"camera","event_type":"motion"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "nil event bus returns 503",
			payload:    `{"source":"camera","event_type":"motion","target":"driveway"}`,
			useBus:     false,
			wantStatus: http.StatusServiceUnavailable,
		},
		{
			name:       "closed event bus returns 500",
			payload:    `{"source":"camera","event_type":"motion","target":"driveway"}`,
			useBus:     true,
			closeBus:   true,
			wantStatus: http.StatusInternalServerError,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertHandleEventsWebhookScenario(t, tc)
		})
	}
}

type alertWebhookStep struct {
	payload        string
	updateChannel  string
	clearDispErr   bool
	wantStatus     int
	wantDispatched int
}

func sendAlertsRequest(t *testing.T, handler http.Handler, path, payload string) (int, int) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(payload))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		return rec.Code, -1
	}
	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	dispatchedVal, ok := resp["dispatched"].(float64)
	if !ok {
		return rec.Code, -1
	}
	return rec.Code, int(dispatchedVal)
}

type mutableConfigSource struct {
	cfg Config
}

func (m *mutableConfigSource) Current() *Config { return &m.cfg }

func assertAlertStep(t *testing.T, handler http.Handler, path string, step alertWebhookStep, cfgSrc *mutableConfigSource, disp *mockOutboundDispatcher) {
	t.Helper()
	if step.updateChannel != "" {
		cfgSrc.cfg.SystemChannelID = step.updateChannel
	}
	if step.clearDispErr {
		disp.mu.Lock()
		disp.directMessageErr = nil
		disp.mu.Unlock()
	}
	code, dispatched := sendAlertsRequest(t, handler, path, step.payload)
	if code != step.wantStatus {
		t.Fatalf("status = %d, want %d", code, step.wantStatus)
	}
	if step.wantStatus == http.StatusOK && dispatched != step.wantDispatched {
		t.Fatalf("dispatched = %d, want %d", dispatched, step.wantDispatched)
	}
}

func assertDirectMessages(t *testing.T, calls []DirectMessageRequest, wantCount int, wantSubstr string) {
	t.Helper()
	if len(calls) != wantCount {
		t.Fatalf("DirectMessageCalls count = %d, want %d", len(calls), wantCount)
	}
	if wantSubstr == "" || len(calls) == 0 {
		return
	}
	lastDM := calls[len(calls)-1]
	if !strings.Contains(lastDM.Content, wantSubstr) {
		t.Fatalf("DM content %q missing substr %q", lastDM.Content, wantSubstr)
	}
}

func TestHandleAlertsWebhook(t *testing.T) {
	t.Parallel()

	const sysChannel = "123456789012345678"

	tests := []struct {
		name          string
		path          string
		cfgChannel    string
		noConfig      bool
		noDispatcher  bool
		dispatcherErr error
		steps         []alertWebhookStep
		wantDMCount   int
		wantDMSubstr  string
	}{
		{
			name:       "invalid json returns 400",
			path:       "/api/v2/alerts",
			cfgChannel: sysChannel,
			steps:      []alertWebhookStep{{payload: "invalid-json", wantStatus: http.StatusBadRequest}},
		},
		{
			name:     "missing config with a dispatcher returns 503",
			path:     "/api/v2/alerts",
			noConfig: true,
			steps:    []alertWebhookStep{{payload: `[{"status":"firing","labels":{"alertname":"DiskFull"}}]`, wantStatus: http.StatusServiceUnavailable}},
		},
		{
			name:         "missing dispatcher returns 503",
			path:         "/api/v2/alerts",
			cfgChannel:   sysChannel,
			noDispatcher: true,
			steps:        []alertWebhookStep{{payload: `[{"status":"firing","labels":{"alertname":"DiskFull"}}]`, wantStatus: http.StatusServiceUnavailable}},
		},
		{
			name:       "empty array returns 200 with 0 dispatched",
			path:       "/api/webhooks/alerts",
			cfgChannel: sysChannel,
			steps:      []alertWebhookStep{{payload: "[]", wantStatus: http.StatusOK, wantDispatched: 0}},
		},
		{
			name:         "firing alert dispatches to system channel",
			path:         "/webhooks/alerts",
			cfgChannel:   sysChannel,
			wantDMCount:  1,
			wantDMSubstr: "TestAlert",
			steps: []alertWebhookStep{
				{payload: `[{"status":"firing","labels":{"alertname":"TestAlert","severity":"critical"},"annotations":{"summary":"disk full"}}]`, wantStatus: http.StatusOK, wantDispatched: 1},
			},
		},
		{
			name:        "duplicate firing alert suppressed across consecutive requests",
			path:        "/api/v2/alerts",
			cfgChannel:  sysChannel,
			wantDMCount: 1,
			steps: []alertWebhookStep{
				{payload: `[{"status":"firing","labels":{"alertname":"TestAlert","severity":"critical"}}]`, wantStatus: http.StatusOK, wantDispatched: 1},
				{payload: `[{"status":"firing","labels":{"alertname":"TestAlert","severity":"critical"}}]`, wantStatus: http.StatusOK, wantDispatched: 0},
			},
		},
		{
			name:        "resolution clears firing alert so subsequent firing alert dispatches again",
			path:        "/api/v2/alerts",
			cfgChannel:  sysChannel,
			wantDMCount: 2,
			steps: []alertWebhookStep{
				{payload: `[{"status":"firing","labels":{"alertname":"TestAlert","severity":"critical"}}]`, wantStatus: http.StatusOK, wantDispatched: 1},
				{payload: `[{"status":"resolved","labels":{"alertname":"TestAlert","severity":"critical"}}]`, wantStatus: http.StatusOK, wantDispatched: 0},
				{payload: `[{"status":"firing","labels":{"alertname":"TestAlert","severity":"critical"}}]`, wantStatus: http.StatusOK, wantDispatched: 1},
			},
		},
		{
			name:        "expired endsAt resolution clears firing alert so subsequent firing dispatches",
			path:        "/api/v2/alerts",
			cfgChannel:  sysChannel,
			wantDMCount: 2,
			steps: []alertWebhookStep{
				{payload: `[{"status":"firing","labels":{"alertname":"TestAlert","severity":"critical"}}]`, wantStatus: http.StatusOK, wantDispatched: 1},
				{payload: fmt.Sprintf(`[{"labels":{"alertname":"TestAlert","severity":"critical"},"startsAt":"%s","endsAt":"%s"}]`, time.Now().Add(-10*time.Minute).Format(time.RFC3339), time.Now().Add(-5*time.Minute).Format(time.RFC3339)), wantStatus: http.StatusOK, wantDispatched: 0},
				{payload: `[{"status":"firing","labels":{"alertname":"TestAlert","severity":"critical"}}]`, wantStatus: http.StatusOK, wantDispatched: 1},
			},
		},
		{
			name:          "dispatcher error rolls back firing state and retries successfully",
			path:          "/api/v2/alerts",
			cfgChannel:    sysChannel,
			dispatcherErr: errors.New("discord rate limited"),
			wantDMCount:   2,
			steps: []alertWebhookStep{
				{payload: `[{"status":"firing","labels":{"alertname":"ErrorAlert","severity":"critical"}}]`, wantStatus: http.StatusInternalServerError},
				{payload: `[{"status":"firing","labels":{"alertname":"ErrorAlert","severity":"critical"}}]`, clearDispErr: true, wantStatus: http.StatusOK, wantDispatched: 1},
			},
		},
		{
			name:        "non-firing non-resolved alert is ignored",
			path:        "/api/v2/alerts",
			cfgChannel:  sysChannel,
			wantDMCount: 0,
			steps: []alertWebhookStep{
				{payload: `[{"status":"pending","labels":{"alertname":"PendingAlert","severity":"warning"}}]`, wantStatus: http.StatusOK, wantDispatched: 0},
			},
		},
		{
			name:         "firing alert dispatches to configured channel name",
			path:         "/api/v2/alerts",
			cfgChannel:   "aerial-dev",
			wantDMCount:  1,
			wantDMSubstr: "ChannelNameAlert",
			steps: []alertWebhookStep{
				{payload: `[{"status":"firing","labels":{"alertname":"ChannelNameAlert","severity":"warning"}}]`, wantStatus: http.StatusOK, wantDispatched: 1},
			},
		},
		{
			name:        "invalid system channel returns retryable 503 and recovers on channel config",
			path:        "/api/v2/alerts",
			cfgChannel:  "invalid channel with spaces!",
			wantDMCount: 1,
			steps: []alertWebhookStep{
				{payload: `[{"status":"firing","labels":{"alertname":"RecoverAlert","severity":"warning"}}]`, wantStatus: http.StatusServiceUnavailable},
				{payload: `[{"status":"firing","labels":{"alertname":"RecoverAlert","severity":"warning"}}]`, updateChannel: "aerial-dev", wantStatus: http.StatusOK, wantDispatched: 1},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			disp := &mockOutboundDispatcher{directMessageErr: tc.dispatcherErr}
			cfgSrc := &mutableConfigSource{cfg: Config{SystemChannelID: tc.cfgChannel}}
			var src ConfigSource = cfgSrc
			if tc.noConfig {
				src = nil
			}
			srv := NewRouterServer(src, nil)
			if !tc.noDispatcher {
				srv.SetDispatcher(disp)
			}
			handler := srv.Routes()

			for _, step := range tc.steps {
				assertAlertStep(t, handler, tc.path, step, cfgSrc, disp)
			}
			assertDirectMessages(t, disp.DirectMessageCalls(), tc.wantDMCount, tc.wantDMSubstr)
		})
	}
}
