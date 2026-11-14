package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/azylman/aerial/pkg/pubsub"
)

type eventTracker struct {
	mu     sync.Mutex
	cond   *sync.Cond
	events []pubsub.ExternalEvent
	topics []string
}

func newEventTracker(t *testing.T, bus pubsub.Bus, topic, subID string) *eventTracker {
	t.Helper()
	tr := &eventTracker{}
	tr.cond = sync.NewCond(&tr.mu)
	_, err := bus.Subscribe(context.Background(), topic, subID, func(_ context.Context, msg pubsub.Message) error {
		var ev pubsub.ExternalEvent
		if err := json.Unmarshal(msg.Data(), &ev); err == nil {
			tr.mu.Lock()
			tr.events = append(tr.events, ev)
			tr.topics = append(tr.topics, msg.Subject())
			tr.cond.Broadcast()
			tr.mu.Unlock()
		}
		return msg.Ack()
	})
	if err != nil {
		t.Fatalf("failed to subscribe to memory bus: %v", err)
	}
	return tr
}

func (tr *eventTracker) waitCount(count int) (int, []string) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	for len(tr.events) < count {
		tr.cond.Wait()
	}
	return len(tr.events), tr.topics
}

type evaluateEventTriggersTestCase struct {
	name          string
	useBus        bool
	closeBusFirst bool
	source        string
	eventType     string
	targets       []string
	contextHeader string
	checkMatched  bool
	checkErr      bool
	wantErr       bool
	wantPubCount  int
	wantPubTopic  string
	wantRiverJobs int
}

func assertEvaluateEventTriggersScenario(t *testing.T, tc evaluateEventTriggersTestCase) {
	t.Helper()
	srv := NewRouterServer(fixedConfig(Config{}), nil)
	mockRiver := &mockRiverInserter{}
	srv.SetRiverClient(mockRiver)

	mockReg := &mockPRRegistry{
		checkTriggersRes: tc.checkMatched,
	}
	if tc.checkErr {
		mockReg.checkTriggersErr = errors.New("simulated check error")
	}
	srv.SetRegistry(mockReg)

	var tracker *eventTracker
	if tc.useBus {
		memBus := pubsub.NewMemoryBus()
		defer memBus.Close()
		tracker = newEventTracker(t, memBus, "aerial.events.>", "test-eval-events")
		if tc.closeBusFirst {
			_ = memBus.Close()
		}
		srv.SetEventBus(memBus)
	}

	err := srv.evaluateEventTriggers(context.Background(), tc.source, tc.eventType, tc.targets, tc.contextHeader)
	if (err != nil) != tc.wantErr {
		t.Fatalf("evaluateEventTriggers() err = %v, wantErr = %v", err, tc.wantErr)
	}

	var gotPubCount int
	var publishedTopics []string
	if tracker != nil {
		gotPubCount, publishedTopics = tracker.waitCount(tc.wantPubCount)
	}

	if gotPubCount != tc.wantPubCount {
		t.Errorf("published event count = %d, want %d", gotPubCount, tc.wantPubCount)
	}
	if tc.wantPubTopic != "" && len(publishedTopics) > 0 {
		if publishedTopics[0] != tc.wantPubTopic {
			t.Errorf("published topic = %s, want %s", publishedTopics[0], tc.wantPubTopic)
		}
	}
	if len(mockRiver.insertedEventJobs) != tc.wantRiverJobs {
		t.Errorf("river inserted jobs = %d, want %d", len(mockRiver.insertedEventJobs), tc.wantRiverJobs)
	}
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
			wantRiverJobs: 0,
		},
		{
			name:          "empty targets is a no-op",
			useBus:        true,
			source:        "camera",
			eventType:     "motion",
			targets:       []string{"   ", ""},
			wantPubCount:  0,
			wantRiverJobs: 0,
		},
		{
			name:          "empty source is a no-op",
			useBus:        true,
			source:        "",
			eventType:     "motion",
			targets:       []string{"driveway"},
			wantPubCount:  0,
			wantRiverJobs: 0,
		},
		{
			name:          "empty event type is a no-op",
			useBus:        true,
			source:        "camera",
			eventType:     "",
			targets:       []string{"driveway"},
			wantPubCount:  0,
			wantRiverJobs: 0,
		},
		{
			name:          "nil bus falls back to river when trigger matched",
			useBus:        false,
			source:        "custom",
			eventType:     "state_change",
			targets:       []string{"sensor.temp"},
			contextHeader: "Temperature spike",
			checkMatched:  true,
			wantPubCount:  0,
			wantRiverJobs: 1,
		},
		{
			name:          "nil bus drops event when trigger not matched",
			useBus:        false,
			source:        "custom",
			eventType:     "state_change",
			targets:       []string{"sensor.temp"},
			checkMatched:  false,
			wantPubCount:  0,
			wantRiverJobs: 0,
		},
		{
			name:          "bus error falls back to river",
			useBus:        true,
			closeBusFirst: true,
			source:        "custom",
			eventType:     "state_change",
			targets:       []string{"sensor.temp"},
			contextHeader: "Temperature spike",
			checkMatched:  true,
			wantPubCount:  0,
			wantRiverJobs: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertEvaluateEventTriggersScenario(t, tc)
		})
	}
}

type handleEventsWebhookTestCase struct {
	name         string
	path         string
	payload      string
	useBus       bool
	wantStatus   int
	wantPubCount int
	wantPubTopic string
}

func assertHandleEventsWebhookScenario(t *testing.T, tc handleEventsWebhookTestCase) {
	t.Helper()
	srv := NewRouterServer(fixedConfig(Config{}), nil)

	var tracker *eventTracker
	if tc.useBus {
		memBus := pubsub.NewMemoryBus()
		defer memBus.Close()
		tracker = newEventTracker(t, memBus, "aerial.events.>", "test-events-webhook")
		srv.SetEventBus(memBus)
	}

	req := httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(tc.payload))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	if strings.HasPrefix(tc.path, "/api/") {
		srv.handleExternalEventsWebhook(w, req)
	} else {
		srv.handleEventsWebhook(w, req)
	}

	if w.Code != tc.wantStatus {
		t.Fatalf("status code = %d, want %d: body=%s", w.Code, tc.wantStatus, w.Body.String())
	}

	var gotPubCount int
	var publishedTopics []string
	if tracker != nil {
		gotPubCount, publishedTopics = tracker.waitCount(tc.wantPubCount)
	}

	if gotPubCount != tc.wantPubCount {
		t.Errorf("published events = %d, want %d", gotPubCount, tc.wantPubCount)
	}
	if tc.wantPubTopic != "" && len(publishedTopics) > 0 {
		if publishedTopics[0] != tc.wantPubTopic {
			t.Errorf("published topic = %s, want %s", publishedTopics[0], tc.wantPubTopic)
		}
	}
}

func TestHandleEventsWebhook_Scenarios(t *testing.T) {
	t.Parallel()

	tests := []handleEventsWebhookTestCase{
		{
			name:         "valid event webhook publishes to jetstream",
			path:         "/webhooks/events",
			payload:      `{"source":"camera","event_type":"motion","targets":["driveway"],"context_header":"Driveway motion"}`,
			useBus:       true,
			wantStatus:   http.StatusOK,
			wantPubCount: 1,
			wantPubTopic: "aerial.events.camera.motion",
		},
		{
			name:         "handleExternalEventsWebhook alias processes valid webhook",
			path:         "/api/webhooks/events",
			payload:      `{"source":"vision","event_type":"person_detected","target":"front_door"}`,
			useBus:       true,
			wantStatus:   http.StatusOK,
			wantPubCount: 1,
			wantPubTopic: "aerial.events.vision.person_detected",
		},
		{
			name:       "invalid json returns 400",
			path:       "/webhooks/events",
			payload:    `{broken-json`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing source returns 400",
			path:       "/webhooks/events",
			payload:    `{"source":"","event_type":"motion","target":"driveway"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing targets returns 400",
			path:       "/webhooks/events",
			payload:    `{"source":"camera","event_type":"motion"}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertHandleEventsWebhookScenario(t, tc)
		})
	}
}
