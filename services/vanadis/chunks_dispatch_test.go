package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/nats-io/nats.go"
)

type memoryMsg struct {
	data    []byte
	subject string
}

func (m *memoryMsg) Data() []byte                       { return m.data }
func (m *memoryMsg) Subject() string                    { return m.subject }
func (m *memoryMsg) Header() nats.Header                { return nil }
func (m *memoryMsg) NumDelivered() uint64               { return 1 }
func (m *memoryMsg) Ack() error                         { return nil }
func (m *memoryMsg) NakWithDelay(_ time.Duration) error { return nil }
func (m *memoryMsg) Term() error                        { return nil }
func (m *memoryMsg) Snooze(_ time.Duration) error       { return nil }
func (m *memoryMsg) InProgress() error                  { return nil }

func TestVanadisServer_HandleDispatchOutcomeEvent(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 11, 14, 1, 0, 0, 0, time.UTC)

	tests := []struct {
		name          string
		nilStore      bool
		seedBuild     *VanadisBuild
		seedChunks    []VanadisChunk
		rawPayload    string
		event         *pubsub.DispatchOutcomeEvent
		wantErr       bool
		wantNextQueue bool
		wantNextIndex int
		wantWidget    bool
		wantWake      bool
		wantStatus    string
	}{
		{
			name:       "nil store returns error",
			nilStore:   true,
			rawPayload: `{"status":"completed","vanadis_chunk_id":"c1"}`,
			wantErr:    true,
		},
		{
			name:       "malformed json returns nil to ack poison pill",
			rawPayload: `{"status": invalid-json`,
			wantErr:    false,
		},
		{
			name: "healthcheck ping is ignored",
			seedBuild: &VanadisBuild{
				ID:        "b-ping",
				Status:    "running",
				CreatedAt: now,
			},
			seedChunks: []VanadisChunk{
				{
					ID:         "c1",
					BuildID:    "b-ping",
					ChunkIndex: 0,
					Mode:       "action",
					Status:     "running",
					Title:      "Ping Step",
				},
			},
			event: &pubsub.DispatchOutcomeEvent{
				DispatchID:     "healthcheck-ping",
				VanadisChunkID: "c1",
				Status:         "completed",
			},
			wantErr:    false,
			wantStatus: "running",
		},
		{
			name: "empty vanadis chunk id is ignored",
			event: &pubsub.DispatchOutcomeEvent{
				DispatchID:     "disp-1",
				VanadisChunkID: "",
				Status:         "completed",
			},
			wantErr: false,
		},
		{
			name: "unhandled status returns nil",
			seedBuild: &VanadisBuild{
				ID:        "b-running",
				Status:    "running",
				CreatedAt: now,
			},
			seedChunks: []VanadisChunk{
				{
					ID:         "c1",
					BuildID:    "b-running",
					ChunkIndex: 0,
					Mode:       "action",
					Status:     "running",
					Title:      "Running Step",
				},
			},
			event: &pubsub.DispatchOutcomeEvent{
				DispatchID:     "disp-1",
				VanadisChunkID: "c1",
				Status:         "running",
			},
			wantErr:    false,
			wantStatus: "running",
		},
		{
			name: "completed action chunk advances to next chunk",
			seedBuild: &VanadisBuild{
				ID:        "b-act",
				ThreadID:  "th-1",
				Status:    "running",
				CreatedAt: now,
			},
			seedChunks: []VanadisChunk{
				{
					ID:         "c-act-1",
					BuildID:    "b-act",
					ChunkIndex: 0,
					Mode:       "action",
					Status:     "running",
					Title:      "Action Step 1",
				},
				{
					ID:         "c-act-2",
					BuildID:    "b-act",
					ChunkIndex: 1,
					Mode:       "action",
					Status:     "pending",
					Title:      "Action Step 2",
				},
			},
			event: &pubsub.DispatchOutcomeEvent{
				DispatchID:     "disp-act-1",
				VanadisChunkID: "c-act-1",
				Status:         "completed",
			},
			wantErr:       false,
			wantNextQueue: true,
			wantNextIndex: 1,
			wantWidget:    true,
			wantStatus:    "completed",
		},
		{
			name: "completed final action chunk completes build and wakes thread",
			seedBuild: &VanadisBuild{
				ID:        "b-final",
				ThreadID:  "th-final",
				Status:    "running",
				Objective: "Complete all chunks",
				CreatedAt: now,
			},
			seedChunks: []VanadisChunk{
				{
					ID:         "c-final-1",
					BuildID:    "b-final",
					ChunkIndex: 0,
					Mode:       "action",
					Status:     "running",
					Title:      "Final Step",
				},
			},
			event: &pubsub.DispatchOutcomeEvent{
				DispatchID:     "disp-final",
				VanadisChunkID: "c-final-1",
				Status:         "completed",
			},
			wantErr:    false,
			wantWidget: true,
			wantWake:   true,
			wantStatus: "completed",
		},
		{
			name: "failed chunk transitions to failed and wakes thread",
			seedBuild: &VanadisBuild{
				ID:        "b-fail",
				ThreadID:  "th-fail",
				Status:    "running",
				Objective: "Try chunk",
				CreatedAt: now,
			},
			seedChunks: []VanadisChunk{
				{
					ID:         "c-fail-1",
					BuildID:    "b-fail",
					ChunkIndex: 0,
					Mode:       "action",
					Status:     "running",
					Title:      "Failing Step",
				},
			},
			event: &pubsub.DispatchOutcomeEvent{
				DispatchID:     "disp-fail",
				VanadisChunkID: "c-fail-1",
				Status:         "failed",
				Error:          "command timed out",
			},
			wantErr:    false,
			wantWidget: true,
			wantWake:   true,
			wantStatus: "failed",
		},
		{
			name: "timeout chunk treated as failed",
			seedBuild: &VanadisBuild{
				ID:        "b-to",
				ThreadID:  "th-to",
				Status:    "running",
				Objective: "Timeout test",
				CreatedAt: now,
			},
			seedChunks: []VanadisChunk{
				{
					ID:         "c-to-1",
					BuildID:    "b-to",
					ChunkIndex: 0,
					Mode:       "action",
					Status:     "running",
					Title:      "Timeout Step",
				},
			},
			event: &pubsub.DispatchOutcomeEvent{
				DispatchID:     "disp-to",
				VanadisChunkID: "c-to-1",
				Status:         "timeout",
			},
			wantErr:    false,
			wantWidget: true,
			wantWake:   true,
			wantStatus: "failed",
		},
		{
			name: "non-existent chunk returns nil without error",
			seedBuild: &VanadisBuild{
				ID:     "b-empty",
				Status: "running",
			},
			event: &pubsub.DispatchOutcomeEvent{
				DispatchID:     "disp-missing",
				VanadisChunkID: "c-nonexistent",
				Status:         "completed",
			},
			wantErr: false,
		},
		{
			name: "failed non-existent chunk returns nil without error",
			seedBuild: &VanadisBuild{
				ID:     "b-empty-fail",
				Status: "running",
			},
			event: &pubsub.DispatchOutcomeEvent{
				DispatchID:     "disp-missing-fail",
				VanadisChunkID: "c-missing-fail",
				Status:         "failed",
			},
			wantErr: false,
		},
		{
			name: "failed chunk with empty error uses status fallback",
			seedBuild: &VanadisBuild{
				ID:        "b-fail-noerr",
				ThreadID:  "th-fail-noerr",
				Status:    "running",
				Objective: "Try fallback error",
				CreatedAt: now,
			},
			seedChunks: []VanadisChunk{
				{
					ID:         "c-fail-noerr-1",
					BuildID:    "b-fail-noerr",
					ChunkIndex: 0,
					Mode:       "action",
					Status:     "running",
					Title:      "Failing Step Without Error",
				},
			},
			event: &pubsub.DispatchOutcomeEvent{
				DispatchID:     "disp-fail-noerr",
				VanadisChunkID: "c-fail-noerr-1",
				Status:         "failed",
				Error:          "",
			},
			wantErr:    false,
			wantWidget: true,
			wantWake:   true,
			wantStatus: "failed",
		},
		{
			name: "redelivered completed action chunk re-enqueues next running chunk",
			seedBuild: &VanadisBuild{
				ID:        "b-redeliver",
				ThreadID:  "th-redeliver",
				Status:    "running",
				Objective: "Multi chunk build",
				CreatedAt: now,
			},
			seedChunks: []VanadisChunk{
				{
					ID:         "c-redeliver-0",
					BuildID:    "b-redeliver",
					ChunkIndex: 0,
					Mode:       "action",
					Status:     "completed",
					Title:      "Already Completed Step",
				},
				{
					ID:         "c-redeliver-1",
					BuildID:    "b-redeliver",
					ChunkIndex: 1,
					Mode:       "action",
					Status:     "running",
					Title:      "Running Next Step Needs Enqueue",
				},
			},
			event: &pubsub.DispatchOutcomeEvent{
				DispatchID:     "disp-redeliver",
				VanadisChunkID: "c-redeliver-0",
				Status:         "completed",
			},
			wantErr:       false,
			wantNextQueue: true,
			wantNextIndex: 1,
			wantStatus:    "completed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var mu sync.Mutex
			var widgetCalls int
			var wakeCalls int

			tsMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				switch r.URL.Path {
				case "/internal/widget/sync":
					widgetCalls++
					httpkit.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
				case "/prompt":
					wakeCalls++
					httpkit.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
				default:
					http.NotFound(w, r)
				}
			}))
			defer tsMock.Close()

			var store VanadisStore
			var mem *memoryVanadisStore
			if !tc.nilStore {
				mem = newMemoryVanadisStore()
				if tc.seedBuild != nil {
					mem.builds[tc.seedBuild.ID] = *tc.seedBuild
				}
				if len(tc.seedChunks) > 0 {
					mem.chunks[tc.seedBuild.ID] = tc.seedChunks
				}
				store = mem
			}

			inserter := &mockRiverInserter{}
			server := NewVanadisServer(Config{BrainURL: tsMock.URL, SynapseURL: tsMock.URL}, store, inserter, tsMock.Client())

			var payloadBytes []byte
			if tc.rawPayload != "" {
				payloadBytes = []byte(tc.rawPayload)
			} else if tc.event != nil {
				var err error
				payloadBytes, err = json.Marshal(tc.event)
				if err != nil {
					t.Fatalf("marshal event: %v", err)
				}
			}

			msg := &memoryMsg{data: payloadBytes, subject: "aerial.dispatch.completed"}
			err := server.HandleDispatchOutcomeEvent(context.Background(), msg)
			if (err != nil) != tc.wantErr {
				t.Fatalf("HandleDispatchOutcomeEvent() error = %v, wantErr = %v", err, tc.wantErr)
			}

			mu.Lock()
			gotWidget := widgetCalls > 0
			gotWake := wakeCalls > 0
			mu.Unlock()

			if gotWidget != tc.wantWidget {
				t.Errorf("widget sync called = %v, want %v", gotWidget, tc.wantWidget)
			}
			if gotWake != tc.wantWake {
				t.Errorf("wake prompt called = %v, want %v", gotWake, tc.wantWake)
			}
			if tc.wantNextQueue {
				if len(inserter.inserted) == 0 {
					t.Errorf("expected river job inserted, got 0")
				} else if inserter.inserted[0].ChunkIndex != tc.wantNextIndex {
					t.Errorf("river inserted chunk index = %d, want %d", inserter.inserted[0].ChunkIndex, tc.wantNextIndex)
				}
			}
			if tc.wantStatus != "" && mem != nil && tc.event != nil {
				c, _, _ := mem.GetChunk(context.Background(), tc.event.VanadisChunkID)
				if c != nil && c.Status != tc.wantStatus {
					t.Errorf("chunk status = %q, want %q", c.Status, tc.wantStatus)
				}
			}
		})
	}
}

type ackBus struct {
	pubsub.Bus

	onHandled func()
}

func (a *ackBus) ConsumeQueue(ctx context.Context, subject, queue string, handler pubsub.Handler) (pubsub.Subscription, error) {
	return a.Bus.ConsumeQueue(ctx, subject, queue, func(mCtx context.Context, msg pubsub.Message) error {
		defer a.onHandled()
		return handler(mCtx, msg)
	})
}

func TestVanadisServer_MemoryBusIntegration(t *testing.T) {
	t.Parallel()

	mem := newMemoryVanadisStore()
	mem.builds["b-bus"] = VanadisBuild{
		ID:        "b-bus",
		ThreadID:  "th-bus",
		Status:    "running",
		CreatedAt: time.Now().UTC(),
	}
	mem.chunks["b-bus"] = []VanadisChunk{
		{
			ID:         "c-bus-1",
			BuildID:    "b-bus",
			ChunkIndex: 0,
			Mode:       "action",
			Status:     "running",
			Title:      "Bus Test",
		},
	}

	inserter := &mockRiverInserter{}
	server := NewVanadisServer(Config{}, mem, inserter, nil)

	ctx := context.Background()
	done := make(chan struct{})
	memBus := pubsub.NewMemoryBus()
	defer memBus.Close()

	if err := pubsub.EnsureCoreStreams(ctx, memBus); err != nil {
		t.Fatalf("EnsureCoreStreams: %v", err)
	}

	wrapped := &ackBus{Bus: memBus, onHandled: func() { close(done) }}
	bus := initVanadisJetStreamWithFactory(ctx, server, "nats://mock", func(_ context.Context, _ string) (pubsub.Bus, error) {
		return wrapped, nil
	})
	if bus == nil {
		t.Fatal("expected non-nil bus from initVanadisJetStreamWithFactory")
	}

	ev := pubsub.DispatchOutcomeEvent{
		DispatchID:     "disp-bus-1",
		VanadisChunkID: "c-bus-1",
		Status:         "completed",
		CompletedAt:    time.Now().UTC(),
	}
	if err := memBus.Publish(ctx, "aerial.dispatch.completed", "disp-bus-1", ev); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for message delivery")
	}

	c, _, err := mem.GetChunk(ctx, "c-bus-1")
	if err != nil {
		t.Fatalf("GetChunk: %v", err)
	}
	if c.Status != "completed" {
		t.Fatalf("chunk status = %q, want completed", c.Status)
	}
}

func TestVanadisServer_BusAndJetStreamInit(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server := NewVanadisServer(Config{}, nil, nil, nil)

	// initVanadisJetStream empty natsURL
	if bus := initVanadisJetStream(ctx, server, ""); bus != nil {
		t.Error("expected nil bus for empty natsURL")
	}

	// initVanadisJetStreamWithFactory error branch
	errFactory := func(_ context.Context, _ string) (pubsub.Bus, error) {
		return nil, errors.New("nats connect error")
	}
	if bus := initVanadisJetStreamWithFactory(ctx, server, "nats://mock", errFactory); bus != nil {
		t.Error("expected nil bus when factory returns error")
	}

	// initVanadisJetStreamWithFactory success branch
	memBus := pubsub.NewMemoryBus()
	defer memBus.Close()
	successFactory := func(_ context.Context, _ string) (pubsub.Bus, error) {
		return memBus, nil
	}
	if bus := initVanadisJetStreamWithFactory(ctx, server, "nats://mock", successFactory); bus == nil {
		t.Error("expected non-nil bus when factory succeeds")
	}
	if server.Bus() != memBus {
		t.Error("expected server.Bus() to equal configured bus")
	}
}
