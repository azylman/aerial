package main

import (
	"context"
	"encoding/json"
	"errors"
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

func verifyPublishedEvents(t *testing.T, tracker *eventTracker, wantCount int, wantTopic string, wantTargets []string) {
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
}

type recordingBus struct {
	pubsub.Bus

	mu        sync.Mutex
	publishes []string
}

func (b *recordingBus) Publish(ctx context.Context, subject, msgID string, payload any) error {
	b.mu.Lock()
	b.publishes = append(b.publishes, subject)
	b.mu.Unlock()
	return nil
}

func (b *recordingBus) publishCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.publishes)
}

type evaluateEventTriggersTestCase struct {
	name          string
	useBus        bool
	useSyncBus    bool
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

	err := srv.evaluateEventTriggers(context.Background(), tc.source, tc.eventType, tc.targets, tc.contextHeader)
	if (err != nil) != tc.wantErr {
		t.Fatalf("evaluateEventTriggers() err = %v, wantErr = %v", err, tc.wantErr)
	}

	if recBus != nil && recBus.publishCount() != tc.wantPubCount {
		t.Errorf("recordingBus publish count = %d, want %d", recBus.publishCount(), tc.wantPubCount)
	}
	verifyPublishedEvents(t, tracker, tc.wantPubCount, tc.wantPubTopic, nil)

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
			name:          "sanitizes subject tokens with spaces or special characters",
			useBus:        true,
			source:        "camera/front",
			eventType:     "person detected*",
			targets:       []string{"porch"},
			contextHeader: "Person detected on porch",
			wantPubCount:  1,
			wantPubTopic:  "aerial.events.camera_front.person_detected_",
			wantRiverJobs: 0,
		},
		{
			name:          "empty targets is a no-op",
			useSyncBus:    true,
			source:        "camera",
			eventType:     "motion",
			targets:       []string{"   ", ""},
			wantPubCount:  0,
			wantRiverJobs: 0,
		},
		{
			name:          "empty source is a no-op",
			useSyncBus:    true,
			source:        "",
			eventType:     "motion",
			targets:       []string{"driveway"},
			wantPubCount:  0,
			wantRiverJobs: 0,
		},
		{
			name:          "empty event type is a no-op",
			useSyncBus:    true,
			source:        "camera",
			eventType:     "",
			targets:       []string{"driveway"},
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
	payload      string
	useBus       bool
	wantStatus   int
	wantPubCount int
	wantPubTopic string
	wantTargets  []string
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

	req := httptest.NewRequest(http.MethodPost, "/webhooks/events", strings.NewReader(tc.payload))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	srv.handleEventsWebhook(w, req)

	if w.Code != tc.wantStatus {
		t.Fatalf("status code = %d, want %d: body=%s", w.Code, tc.wantStatus, w.Body.String())
	}

	verifyPublishedEvents(t, tracker, tc.wantPubCount, tc.wantPubTopic, tc.wantTargets)
}

func TestHandleEventsWebhook_Scenarios(t *testing.T) {
	t.Parallel()

	tests := []handleEventsWebhookTestCase{
		{
			name:         "valid event webhook publishes to jetstream",
			payload:      `{"source":"camera","event_type":"motion","targets":["driveway"],"context_header":"Driveway motion"}`,
			useBus:       true,
			wantStatus:   http.StatusOK,
			wantPubCount: 1,
			wantPubTopic: "aerial.events.camera.motion",
			wantTargets:  []string{"driveway"},
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
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertHandleEventsWebhookScenario(t, tc)
		})
	}
}
