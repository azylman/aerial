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
	data         []byte
	subject      string
	numDelivered uint64
	termed       bool
	acked        bool
	nakDelay     time.Duration
}

func (m *memoryMsg) Data() []byte        { return m.data }
func (m *memoryMsg) Subject() string     { return m.subject }
func (m *memoryMsg) Header() nats.Header { return nil }
func (m *memoryMsg) NumDelivered() uint64 {
	if m.numDelivered > 0 {
		return m.numDelivered
	}
	return 1
}

func (m *memoryMsg) Ack() error {
	m.acked = true
	return nil
}

func (m *memoryMsg) NakWithDelay(d time.Duration) error { m.nakDelay = d; return nil }
func (m *memoryMsg) Term() error                        { m.termed = true; return nil }
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

			bus := &mockChunkBus{}
			server := NewVanadisServer(Config{BrainURL: tsMock.URL}, store, tsMock.Client())
			server.SetBus(bus)

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
			gotWidget := widgetCalls > 0 || bus.widgetCalls > 0
			gotWake := wakeCalls > 0
			mu.Unlock()

			if gotWidget != tc.wantWidget {
				t.Errorf("widget sync called = %v, want %v", gotWidget, tc.wantWidget)
			}
			if gotWake != tc.wantWake {
				t.Errorf("wake prompt called = %v, want %v", gotWake, tc.wantWake)
			}
			if tc.wantNextQueue {
				if len(bus.published) == 0 {
					t.Errorf("expected chunk job published, got 0")
				} else if bus.published[0].ChunkIndex != tc.wantNextIndex {
					t.Errorf("published chunk index = %d, want %d", bus.published[0].ChunkIndex, tc.wantNextIndex)
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

	server := NewVanadisServer(Config{}, mem, nil)

	ctx := context.Background()
	done := make(chan struct{})
	memBus := pubsub.NewMemoryBus()
	defer memBus.Close()

	if err := pubsub.EnsureCoreStreams(ctx, memBus); err != nil {
		t.Fatalf("EnsureCoreStreams: %v", err)
	}

	var closeOnce sync.Once
	wrapped := &ackBus{Bus: memBus, onHandled: func() { closeOnce.Do(func() { close(done) }) }}
	stopBus, err := initVanadisJetStreamWithFactory(ctx, server, "nats://mock", func(_ context.Context, _ string) (pubsub.Bus, error) {
		return wrapped, nil
	})
	if err != nil || stopBus == nil {
		t.Fatalf("expected non-nil stopBus and nil error from initVanadisJetStreamWithFactory, got err=%v", err)
	}
	defer stopBus()

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
	server := NewVanadisServer(Config{}, nil, nil)

	// initVanadisJetStream empty natsURL
	if stop, err := initVanadisJetStream(ctx, server, ""); err != nil || stop == nil {
		t.Errorf("expected non-nil stop and nil error for empty natsURL, got err=%v", err)
	}

	// initVanadisJetStreamWithFactory error branch
	errFactory := func(_ context.Context, _ string) (pubsub.Bus, error) {
		return nil, errors.New("nats connect error")
	}
	if stop, err := initVanadisJetStreamWithFactory(ctx, server, "nats://mock", errFactory); err == nil || stop != nil {
		t.Errorf("expected error and nil stop when factory returns error, got err=%v", err)
	}

	// ensure core streams error branch
	errStreamsBus := &errCoreStreamsBus{Bus: pubsub.NewMemoryBus(), err: errors.New("stream failure")}
	if stop, err := initVanadisJetStreamWithFactory(ctx, server, "nats://mock", func(_ context.Context, _ string) (pubsub.Bus, error) {
		return errStreamsBus, nil
	}); err == nil || stop != nil {
		t.Errorf("expected error and nil stop when EnsureCoreStreams fails, got err=%v", err)
	}

	// subscribe vanadis-dispatch error branch
	errDispatchSubBus := &errQueueSubBus{Bus: pubsub.NewMemoryBus(), matchSubject: "aerial.dispatch.*", err: errors.New("dispatch sub failure")}
	if stop, err := initVanadisJetStreamWithFactory(ctx, server, "nats://mock", func(_ context.Context, _ string) (pubsub.Bus, error) {
		return errDispatchSubBus, nil
	}); err == nil || stop != nil {
		t.Errorf("expected error and nil stop when ConsumeQueue dispatch fails, got err=%v", err)
	}

	// subscribe vanadis-pr-events error branch
	errPRSubBus := &errQueueSubBus{Bus: pubsub.NewMemoryBus(), matchSubject: "aerial.pr.>", err: errors.New("pr sub failure")}
	if stop, err := initVanadisJetStreamWithFactory(ctx, server, "nats://mock", func(_ context.Context, _ string) (pubsub.Bus, error) {
		return errPRSubBus, nil
	}); err == nil || stop != nil {
		t.Errorf("expected error and nil stop when ConsumeQueue pr-events fails, got err=%v", err)
	}

	// subscribe vanadis-chunk error branch
	errChunkSubBus := &errQueueSubBus{Bus: pubsub.NewMemoryBus(), matchSubject: "aerial.jobs.vanadis.chunk", err: errors.New("chunk sub failure")}
	if stop, err := initVanadisJetStreamWithFactory(ctx, server, "nats://mock", func(_ context.Context, _ string) (pubsub.Bus, error) {
		return errChunkSubBus, nil
	}); err == nil || stop != nil {
		t.Errorf("expected error and nil stop when StartJobConsumers fails, got err=%v", err)
	}

	// initVanadisJetStreamWithFactory success branch
	memBus := pubsub.NewMemoryBus()
	defer memBus.Close()
	successFactory := func(_ context.Context, _ string) (pubsub.Bus, error) {
		return memBus, nil
	}
	stop, err := initVanadisJetStreamWithFactory(ctx, server, "nats://mock", successFactory)
	if err != nil || stop == nil {
		t.Errorf("expected non-nil stop and nil error when factory succeeds, got err=%v", err)
	}
	if server.bus != memBus {
		t.Error("expected server.bus to equal configured bus")
	}
	stop()
}

type errCoreStreamsBus struct {
	pubsub.Bus

	err error
}

func (b *errCoreStreamsBus) EnsureCoreStreams(ctx context.Context) error {
	return b.err
}

type errQueueSubBus struct {
	pubsub.Bus

	matchSubject string
	err          error
}

func (b *errQueueSubBus) ConsumeQueue(ctx context.Context, subject, queue string, handler pubsub.Handler) (pubsub.Subscription, error) {
	if b.matchSubject == "" || subject == b.matchSubject {
		return nil, b.err
	}
	return b.Bus.ConsumeQueue(ctx, subject, queue, handler)
}

func TestChunksAction_And_Events_Helpers(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// 1. validateChunkModeAndRepo
	if err := validateChunkModeAndRepo(0, "invalid-mode", "aerial", nil, nil); err == nil {
		t.Fatal("expected error on invalid mode")
	}
	if err := validateChunkModeAndRepo(0, "pr", "", nil, nil); err == nil {
		t.Fatal("expected error on pr with empty repo")
	}
	if err := validateChunkModeAndRepo(0, "action", "aerial", nil, nil); err == nil {
		t.Fatal("expected error on action with repo")
	}
	if err := validateChunkModeAndRepo(0, "action", "", []string{"file.go"}, nil); err == nil {
		t.Fatal("expected error on action with target_files")
	}
	if err := validateChunkModeAndRepo(0, "action", "", nil, []string{"Sym"}); err == nil {
		t.Fatal("expected error on action with target_symbols")
	}
	if err := validateChunkModeAndRepo(0, "action", "", nil, nil); err != nil {
		t.Fatalf("unexpected error on valid action: %v", err)
	}
	if err := validateChunkModeAndRepo(0, "pr", "azylman/aerial", []string{"file.go"}, []string{"Sym"}); err != nil {
		t.Fatalf("unexpected error on valid pr: %v", err)
	}

	// 3. failChunkInternal
	var nilSrv *VanadisServer
	if _, _, _, err := nilSrv.failChunkInternal(ctx, "c1", "reason"); err == nil {
		t.Fatal("expected error on nil server failChunkInternal")
	}

	memStore := newMemoryVanadisStore()
	srv := &VanadisServer{store: memStore}
	if _, _, _, err := srv.failChunkInternal(ctx, "", "reason"); err == nil {
		t.Fatal("expected error on empty chunkID")
	}
	if _, _, _, err := srv.failChunkInternal(ctx, "c1", ""); err == nil {
		t.Fatal("expected error on empty reason")
	}

	// Shutdown interruption reason
	b1 := VanadisBuild{ID: "b-shut", ThreadID: "th-1", Objective: "shut"}
	c1 := VanadisChunk{ID: "c-shut", BuildID: "b-shut", Title: "c-shut"}
	_ = memStore.CreateBuild(ctx, b1, []VanadisChunk{c1})
	bGot, cGot, failed, err := srv.failChunkInternal(ctx, "c-shut", "interrupted by shutdown")
	if err != nil || failed || bGot == nil || cGot == nil {
		t.Fatalf("expected no fail on shutdown interruption, got err=%v failed=%v", err, failed)
	}

	// Normal fail chunk
	bFail := VanadisBuild{ID: "b-f", ThreadID: "th-fail", Objective: "fail obj"}
	cFail := VanadisChunk{ID: "c-f", BuildID: "b-f", Title: "c-f", Status: "running"}
	_ = memStore.CreateBuild(ctx, bFail, []VanadisChunk{cFail})
	bF, cF, failedF, errF := srv.failChunkInternal(ctx, "c-f", "test fail reason")
	if errF != nil || !failedF || bF == nil || cF == nil {
		t.Fatalf("expected failed=true, got err=%v failed=%v", errF, failedF)
	}

	// 4. retryNextRunningChunkEnqueue
	if err := srv.retryNextRunningChunkEnqueue(ctx, nil); err != nil {
		t.Fatalf("expected nil for nil res: %v", err)
	}
	if err := srv.retryNextRunningChunkEnqueue(ctx, &VanadisAdvancementResult{ChunkMode: "pr"}); err != nil {
		t.Fatalf("expected nil for non-action mode: %v", err)
	}
	if err := srv.retryNextRunningChunkEnqueue(ctx, &VanadisAdvancementResult{ChunkMode: "action", BuildStatus: "completed"}); err != nil {
		t.Fatalf("expected nil for non-running status: %v", err)
	}
	if err := srv.retryNextRunningChunkEnqueue(ctx, &VanadisAdvancementResult{ChunkMode: "action", BuildStatus: "running", BuildID: "non-existent"}); err == nil {
		t.Fatal("expected error for non-existent build")
	}

	// Next running chunk enqueue
	bRetry := VanadisBuild{ID: "b-ret", Status: "running"}
	cRet0 := VanadisChunk{ID: "c-ret-0", BuildID: "b-ret", ChunkIndex: 0, Status: "completed"}
	cRet1 := VanadisChunk{ID: "c-ret-1", BuildID: "b-ret", ChunkIndex: 1, Status: "running"}
	_ = memStore.CreateBuild(ctx, bRetry, []VanadisChunk{cRet0, cRet1})
	// Without bus -> enqueue fails
	err = srv.retryNextRunningChunkEnqueue(ctx, &VanadisAdvancementResult{
		ChunkMode:        "action",
		BuildStatus:      "running",
		BuildID:          "b-ret",
		MergedChunkIndex: 0,
	})
	if err == nil {
		t.Fatal("expected error on enqueue without bus")
	}

	// 5. handleFailedDispatchOutcome
	err = srv.handleFailedDispatchOutcome(ctx, pubsub.DispatchOutcomeEvent{
		VanadisChunkID: "c-not-found",
		Status:         "failed",
	})
	if err != nil {
		t.Fatalf("expected nil on not found chunk: %v", err)
	}

	// 6. HandleDispatchOutcomeEvent
	var nilSrvEvents *VanadisServer
	if err := nilSrvEvents.HandleDispatchOutcomeEvent(ctx, &memoryMsg{}); err == nil {
		t.Fatal("expected error on nil server HandleDispatchOutcomeEvent")
	}

	// 7. handleCompletedDispatchOutcome with store error
	storeErr := &errVanadisStore{getBuildErr: errors.New("db error")}
	srvStoreErr := &VanadisServer{store: storeErr}
	completedData, _ := json.Marshal(pubsub.DispatchOutcomeEvent{DispatchID: "d1", VanadisChunkID: "c1", Status: "completed"})
	if err := srvStoreErr.HandleDispatchOutcomeEvent(ctx, &memoryMsg{data: completedData}); err == nil {
		t.Fatal("expected error on CompleteActionChunk store failure")
	}

	// 8. handleFailedDispatchOutcome with internal fail error
	failedData, _ := json.Marshal(pubsub.DispatchOutcomeEvent{DispatchID: "d1", VanadisChunkID: "c1", Status: "failed"})
	if err := srvStoreErr.HandleDispatchOutcomeEvent(ctx, &memoryMsg{data: failedData}); err == nil {
		t.Fatal("expected error on failChunkInternal store failure")
	}
}
