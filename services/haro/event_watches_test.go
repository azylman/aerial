package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/haro/internal/store"
)

func buildEventTriggerFilterArgs(source string, targets []string, eventType string) (string, []any, bool) {
	return store.BuildEventTriggerFilterArgs(source, targets, eventType)
}

func verifyTriggerState(ctx context.Context, t *testing.T, db *sql.DB, ev pubsub.ExternalEvent, wantInDB bool) {
	t.Helper()
	placeholders, args, ok := buildEventTriggerFilterArgs(ev.Source, ev.Targets, ev.EventType)
	if !ok {
		t.Fatal("invalid event trigger filter args")
	}
	rawQuery := "SELECT EXISTS(SELECT 1 FROM event_triggers WHERE source = $1 AND event_type = $2 AND target IN (" + placeholders + "))"
	var matched bool
	if err := db.QueryRowContext(ctx, rawQuery, args...).Scan(&matched); err != nil {
		t.Fatalf("query event_triggers failed: %v", err)
	}
	if matched != wantInDB {
		t.Errorf("trigger in db = %v, want %v", matched, wantInDB)
	}
}

func verifyDispatchedPrompt(t *testing.T, disp *mockDispatcher, wantChannel, wantPrompt string) {
	t.Helper()
	if len(disp.dispatched) != 1 {
		t.Fatalf("expected 1 dispatched prompt, got %d", len(disp.dispatched))
	}
	if disp.dispatched[0].ChannelID != wantChannel {
		t.Errorf("ChannelID = %s, want %s", disp.dispatched[0].ChannelID, wantChannel)
	}
	if disp.dispatched[0].Prompt != wantPrompt {
		t.Errorf("Prompt = %q, want %q", disp.dispatched[0].Prompt, wantPrompt)
	}
}

func seedExternalEventTriggers(ctx context.Context, db *sql.DB) {
	_ = InsertEventTrigger(ctx, db, EventTrigger{
		ID:        "trig-ext-1",
		ChannelID: "100000000000000001",
		Source:    "camera",
		Target:    "driveway",
		EventType: "vehicle_detected",
		Prompt:    "Vehicle in driveway",
		Effort:    "low",
	})
	_ = InsertEventTrigger(ctx, db, EventTrigger{
		ID:        "trig-ext-fail",
		ChannelID: "100000000000000001",
		Source:    "camera",
		Target:    "backyard",
		EventType: "motion",
		Prompt:    "Motion in backyard",
		Effort:    "low",
	})
	_ = InsertEventTrigger(ctx, db, EventTrigger{
		ID:        "trig-ext-healthcheck",
		ChannelID: "100000000000000001",
		Source:    "healthcheck",
		Target:    "ping",
		EventType: "ping",
		Prompt:    "Healthcheck ping",
		Effort:    "low",
	})
	_ = InsertEventTrigger(ctx, db, EventTrigger{
		ID:         "trig-ext-fail-persistent",
		ChannelID:  "100000000000000001",
		Source:     "camera",
		Target:     "sideyard",
		EventType:  "motion",
		Prompt:     "Motion in sideyard",
		Effort:     "low",
		Persistent: true,
	})
}

func TestHandleExternalEvent_Scenarios(t *testing.T) {
	db := setupTestDB(t.Context(), t)
	ctx := context.Background()

	seedExternalEventTriggers(ctx, db)

	closedDB, err := OpenDB(t.Context(), &Config{DatabaseURL: ":memory:"})
	if err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}
	_ = closedDB.Close()
	serverClosed := NewServer(NewToolHandler(&Config{DatabaseURL: ":memory:"}, closedDB))

	mockDispFail := &mockDispatcher{err: errors.New("eri unavailable")}
	mockDispOK := &mockDispatcher{response: PromptDispatchResponse{Status: "ok", MessageID: "msg-ext-1"}}
	serverOK := NewServer(NewToolHandler(&Config{DatabaseURL: ":memory:"}, db), mockDispOK)
	serverFail := NewServer(NewToolHandler(&Config{DatabaseURL: ":memory:"}, db), mockDispFail)
	serverNoDisp := NewServer(NewToolHandler(&Config{DatabaseURL: ":memory:"}, db))

	tests := []struct {
		name                 string
		server               *Server
		ev                   pubsub.ExternalEvent
		wantErr              bool
		checkDBID            string
		wantInDB             bool
		wantImmediateReclaim bool
	}{
		{
			name:      "healthcheck source is no-op and leaves trigger unclaimed",
			server:    serverOK,
			ev:        pubsub.ExternalEvent{Source: "healthcheck", EventType: "ping", Targets: []string{"ping"}},
			wantErr:   false,
			checkDBID: "trig-ext-healthcheck",
			wantInDB:  true,
		},
		{
			name:    "nil db returns error from ClaimEventTriggers",
			server:  NewServer(NewToolHandler(&Config{DatabaseURL: ":memory:"}, nil)),
			ev:      pubsub.ExternalEvent{Source: "camera", EventType: "vehicle_detected", Targets: []string{"driveway"}},
			wantErr: true,
		},
		{
			name:    "closed db returns error from ClaimEventTriggers",
			server:  serverClosed,
			ev:      pubsub.ExternalEvent{Source: "camera", EventType: "vehicle_detected", Targets: []string{"driveway"}},
			wantErr: true,
		},
		{
			name:    "no matching triggers succeeds without firing",
			server:  serverOK,
			ev:      pubsub.ExternalEvent{Source: "camera", EventType: "vehicle_detected", Targets: []string{"front_door"}},
			wantErr: false,
		},
		{
			name:      "dispatch failure recovers trigger to db and returns nil to avoid redelivery storm",
			server:    serverFail,
			ev:        pubsub.ExternalEvent{Source: "camera", EventType: "motion", Targets: []string{"backyard"}},
			wantErr:   false,
			checkDBID: "trig-ext-fail",
			wantInDB:  true,
		},
		{
			name:      "nil dispatcher recovers trigger to db and returns nil to avoid redelivery storm",
			server:    serverNoDisp,
			ev:        pubsub.ExternalEvent{Source: "camera", EventType: "motion", Targets: []string{"backyard"}},
			wantErr:   false,
			checkDBID: "trig-ext-fail",
			wantInDB:  true,
		},
		{
			name:                 "dispatch failure recovers persistent trigger to db",
			server:               serverFail,
			ev:                   pubsub.ExternalEvent{Source: "camera", EventType: "motion", Targets: []string{"sideyard"}},
			wantErr:              false,
			checkDBID:            "trig-ext-fail-persistent",
			wantInDB:             true,
			wantImmediateReclaim: true,
		},
		{
			name:      "successful dispatch claims and fires trigger",
			server:    serverOK,
			ev:        pubsub.ExternalEvent{Source: "camera", EventType: "vehicle_detected", Targets: []string{"driveway"}, ContextHeader: "Camera Alert"},
			wantErr:   false,
			checkDBID: "trig-ext-1",
			wantInDB:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotErr := tc.server.HandleExternalEvent(ctx, tc.ev)
			if (gotErr != nil) != tc.wantErr {
				t.Fatalf("HandleExternalEvent() err = %v, wantErr = %v", gotErr, tc.wantErr)
			}
			if tc.checkDBID != "" {
				verifyTriggerState(ctx, t, db, tc.ev, tc.wantInDB)
			}
			if tc.wantImmediateReclaim {
				reclaimed, rErr := ClaimEventTriggers(ctx, db, tc.ev.Source, tc.ev.Targets, tc.ev.EventType)
				if rErr != nil {
					t.Fatalf("ClaimEventTriggers failed: %v", rErr)
				}
				if len(reclaimed) != 1 || reclaimed[0].ID != tc.checkDBID {
					t.Fatalf("expected persistent trigger %s to be immediately reclaimable, got %v", tc.checkDBID, reclaimed)
				}
			}
			if tc.name == "successful dispatch claims and fires trigger" {
				verifyDispatchedPrompt(t, mockDispOK, "100000000000000001", "Camera Alert\n\nVehicle in driveway")
			}
		})
	}
}

type ackSubscribeBus struct {
	pubsub.Bus

	lastErr   error
	onHandled func()
}

func (a *ackSubscribeBus) Subscribe(ctx context.Context, subject, durable string, handler pubsub.Handler) (pubsub.Subscription, error) {
	return a.Bus.Subscribe(ctx, subject, durable, func(mCtx context.Context, msg pubsub.Message) error {
		defer a.onHandled()
		err := handler(mCtx, msg)
		a.lastErr = err
		return err
	})
}

func TestHaro_EventsConsumerIntegration(t *testing.T) {
	db := setupTestDB(t.Context(), t)
	ctx := context.Background()

	_ = InsertEventTrigger(ctx, db, EventTrigger{
		ID:        "trig-int-1",
		ChannelID: "100000000000000001",
		Source:    "flux",
		Target:    "aerial",
		EventType: "sync_success",
		Prompt:    "Deploy succeeded",
		Effort:    "low",
	})

	done := make(chan struct{})
	mockDisp := &mockDispatcher{response: PromptDispatchResponse{Status: "ok", MessageID: "msg-ext-int"}}
	server := NewServer(NewToolHandler(&Config{DatabaseURL: ":memory:"}, db), mockDisp)

	memBus := pubsub.NewMemoryBus()
	defer memBus.Close()

	if err := pubsub.EnsureCoreStreams(ctx, memBus); err != nil {
		t.Fatalf("EnsureCoreStreams: %v", err)
	}

	wrapped := &ackSubscribeBus{Bus: memBus, onHandled: func() { close(done) }}
	bus := initHaroJetStreamWithFactory(ctx, server, db, "nats://mock", func(_ context.Context, _ string) (pubsub.Bus, error) {
		return wrapped, nil
	})
	if bus == nil {
		t.Fatal("expected non-nil bus")
	}

	ev := pubsub.ExternalEvent{
		Source:        "flux",
		EventType:     "sync_success",
		Targets:       []string{"aerial"},
		ContextHeader: "Deployment update",
	}
	if err := memBus.Publish(ctx, "aerial.events.flux.sync_success", "ev-1", ev); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for event delivery")
	}

	verifyTriggerState(ctx, t, db, ev, false)
	if wrapped.lastErr != nil {
		t.Errorf("expected nil error on successful event handling, got %v", wrapped.lastErr)
	}
}

type handlerCaptureBus struct {
	pubsub.Bus

	captured pubsub.Handler
}

func (c *handlerCaptureBus) Subscribe(ctx context.Context, subject, durable string, handler pubsub.Handler) (pubsub.Subscription, error) {
	c.captured = handler
	return c.Bus.Subscribe(ctx, subject, durable, handler)
}

func TestHaro_InitJetStreamEdgeCases(t *testing.T) {
	ctx := context.Background()

	memBus := pubsub.NewMemoryBus()
	defer memBus.Close()

	// Test handler delivery retry capping
	closedDB, err := OpenDB(t.Context(), &Config{DatabaseURL: ":memory:"})
	if err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}
	_ = closedDB.Close()
	serverClosed := NewServer(NewToolHandler(&Config{DatabaseURL: ":memory:"}, closedDB))

	captureBus := &handlerCaptureBus{Bus: memBus}
	bus := initHaroJetStreamWithFactory(ctx, serverClosed, nil, "nats://mock", func(_ context.Context, _ string) (pubsub.Bus, error) {
		return captureBus, nil
	})
	if bus == nil || captureBus.captured == nil {
		t.Fatal("expected captured subscribe handler")
	}

	evBytes, _ := json.Marshal(pubsub.ExternalEvent{
		EventID:   "ev-retry-test",
		Source:    "camera",
		EventType: "motion",
		Targets:   []string{"driveway"},
	})

	// When attempts < 5 and handler fails on DB error, returns error (NAK)
	if err := captureBus.captured(ctx, &memoryMsg{data: evBytes, numDelivered: 1}); err == nil {
		t.Error("expected error for numDelivered < 5 on DB failure")
	}

	// When attempts >= 5 and handler fails, drops and returns nil
	if err := captureBus.captured(ctx, &memoryMsg{data: evBytes, numDelivered: 5}); err != nil {
		t.Errorf("expected nil error for numDelivered >= 5 to drop poison event, got %v", err)
	}

	// Bad JSON payload returns nil to avoid poison redelivery loop
	if err := captureBus.captured(ctx, &memoryMsg{data: []byte("invalid-json"), numDelivered: 1}); err != nil {
		t.Errorf("expected nil error for bad json to avoid poison loop, got %v", err)
	}
}
