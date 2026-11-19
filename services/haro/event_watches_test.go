package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/haro/internal/config"
	"github.com/azylman/aerial/services/haro/internal/dispatch"
	"github.com/azylman/aerial/services/haro/internal/model"
	"github.com/azylman/aerial/services/haro/internal/server"
	"github.com/azylman/aerial/services/haro/internal/store"
	"github.com/azylman/aerial/services/haro/internal/testutil"
	"github.com/azylman/aerial/services/haro/internal/tools"
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

	_ = store.InsertEventTrigger(ctx, db, model.EventTrigger{
		ID:        "trig-int-1",
		ChannelID: "100000000000000001",
		Source:    "flux",
		Target:    "aerial",
		EventType: "sync_success",
		Prompt:    "Deploy succeeded",
		Effort:    "low",
	})

	done := make(chan struct{})
	mockDisp := &mockDispatcher{response: dispatch.PromptDispatchResponse{Status: "ok", MessageID: "msg-ext-int"}}
	srv := server.NewServer(tools.NewToolHandler(&config.Config{DatabaseURL: ":memory:"}, db), mockDisp)

	memBus := pubsub.NewMemoryBus()
	defer memBus.Close()

	if err := pubsub.EnsureCoreStreams(ctx, memBus); err != nil {
		t.Fatalf("EnsureCoreStreams: %v", err)
	}

	wrapped := &ackSubscribeBus{Bus: memBus, onHandled: func() { close(done) }}
	bus := initHaroJetStreamWithFactory(ctx, srv, db, "nats://mock", func(_ context.Context, _ string) (pubsub.Bus, error) {
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
	closedDB, err := testutil.OpenSQLiteTestDB(":memory:")
	if err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}
	_ = closedDB.Close()
	serverClosed := server.NewServer(tools.NewToolHandler(&config.Config{DatabaseURL: ":memory:"}, closedDB))

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
