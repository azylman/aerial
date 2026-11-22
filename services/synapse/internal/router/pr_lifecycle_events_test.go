package router

import (
	"context"
	"reflect"
	"testing"

	"github.com/azylman/aerial/pkg/pubsub"
)

// wantPREvent is the declarative projection of a published PR lifecycle event.
type wantPREvent struct {
	Event      string
	PRNumber   int
	HeadSHA    string
	MergeSHA   string
	Status     string
	Title      string
	CheckName  string
	CheckState string
}

// newLifecycleServer builds a router through the production constructor; nil
// mocks stay unset so typed-nil pointers never reach the interface fields.
func newLifecycleServer(cfg Config, bus *mockEventBus, disp *mockOutboundDispatcher) *RouterServer {
	srv := newTestRouter(cfg, nil, nil, bus)
	if disp != nil {
		srv.SetDispatcher(disp)
	}
	return srv
}

func assertPREvents(t *testing.T, bus *mockEventBus, want []wantPREvent) {
	t.Helper()
	bus.mu.Lock()
	published := append([]pubsub.PRLifecycleEvent(nil), bus.publishedPREvents...)
	bus.mu.Unlock()
	got := make([]wantPREvent, 0, len(published))
	for _, ev := range published {
		if ev.OccurredAt.IsZero() {
			t.Errorf("published %q event has zero OccurredAt", ev.Event)
		}
		got = append(got, wantPREvent{
			Event: ev.Event, PRNumber: ev.PRNumber, HeadSHA: ev.HeadSHA, MergeSHA: ev.MergeSHA,
			Status: ev.Status, Title: ev.Title, CheckName: ev.CheckName, CheckState: ev.CheckState,
		})
	}
	if len(want) == 0 && len(got) == 0 {
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("published PR events = %+v, want %+v", got, want)
	}
}

func TestPublishPRLifecycleEvent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		ev   pubsub.PRLifecycleEvent
		want []wantPREvent
	}{
		{
			name: "opened event published with fields and stamped OccurredAt",
			ev:   pubsub.PRLifecycleEvent{Event: "opened", Repo: "azylman/aerial", PRNumber: 42, Branch: "feat/something", HeadSHA: "abcdef123456", Status: "open"},
			want: []wantPREvent{{Event: "opened", PRNumber: 42, HeadSHA: "abcdef123456", Status: "open"}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bus := &mockEventBus{}
			srv := newLifecycleServer(Config{}, bus, &mockOutboundDispatcher{})
			if err := srv.publishPRLifecycleEvent(context.Background(), tc.ev); err != nil {
				t.Fatalf("publishPRLifecycleEvent() err = %v", err)
			}
			assertPREvents(t, bus, tc.want)
		})
	}
}
