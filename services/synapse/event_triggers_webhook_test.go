package main

import (
	"context"
	"encoding/json"
	"slices"
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

func (b *recordingBus) lastEvent() (pubsub.ExternalEvent, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.events) == 0 {
		return pubsub.ExternalEvent{}, false
	}
	return b.events[len(b.events)-1], true
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

	err := srv.evaluateEventTriggers(context.Background(), tc.source, tc.eventType, tc.targets, tc.contextHeader)
	if (err != nil) != tc.wantErr {
		t.Fatalf("evaluateEventTriggers() err = %v, wantErr = %v", err, tc.wantErr)
	}

	if recBus != nil && recBus.publishCount() != tc.wantPubCount {
		t.Errorf("recordingBus publish count = %d, want %d", recBus.publishCount(), tc.wantPubCount)
	}
	verifyPublishedEvents(t, tracker, tc.wantPubCount, tc.wantPubTopic, nil)
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
