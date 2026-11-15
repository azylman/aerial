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

	"github.com/nats-io/nats.go"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/pubsub"
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

// outcomeBuild is a running build created at a fixed instant.
func outcomeBuild(id, threadID, objective string) *VanadisBuild {
	return &VanadisBuild{ID: id, ThreadID: threadID, Status: "running", Objective: objective, CreatedAt: time.Date(2026, 11, 14, 1, 0, 0, 0, time.UTC)}
}

// actionChunk is an action-mode chunk of buildID at chunk index idx.
func actionChunk(id, buildID string, idx int, status, title string) VanadisChunk {
	return VanadisChunk{ID: id, BuildID: buildID, ChunkIndex: idx, Mode: "action", Status: status, Title: title}
}

// outcomeEvent is a dispatch outcome for chunkID.
func outcomeEvent(dispatchID, chunkID, status, errMsg string) *pubsub.DispatchOutcomeEvent {
	return &pubsub.DispatchOutcomeEvent{DispatchID: dispatchID, VanadisChunkID: chunkID, Status: status, Error: errMsg}
}

func TestVanadisServer_HandleDispatchOutcomeEvent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		nilServer     bool
		nilStore      bool
		getBuildErr   error
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
		{name: "nil store returns error", nilStore: true, rawPayload: `{"status":"completed","vanadis_chunk_id":"c1"}`, wantErr: true},
		{name: "nil server returns error", nilServer: true, wantErr: true},
		{name: "completed event surfaces store failure", getBuildErr: errors.New("db error"), event: outcomeEvent("d1", "c1", "completed", ""), wantErr: true},
		{name: "failed event surfaces store failure", getBuildErr: errors.New("db error"), event: outcomeEvent("d1", "c1", "failed", ""), wantErr: true},
		{name: "malformed json returns nil to ack poison pill", rawPayload: `{"status": invalid-json`},
		{
			name:       "healthcheck ping is ignored",
			seedBuild:  outcomeBuild("b-ping", "", ""),
			seedChunks: []VanadisChunk{actionChunk("c1", "b-ping", 0, "running", "Ping Step")},
			event:      outcomeEvent("healthcheck-ping", "c1", "completed", ""),
			wantStatus: "running",
		},
		{name: "empty vanadis chunk id is ignored", event: outcomeEvent("disp-1", "", "completed", "")},
		{
			name:       "unhandled status returns nil",
			seedBuild:  outcomeBuild("b-running", "", ""),
			seedChunks: []VanadisChunk{actionChunk("c1", "b-running", 0, "running", "Running Step")},
			event:      outcomeEvent("disp-1", "c1", "running", ""),
			wantStatus: "running",
		},
		{
			name:      "completed action chunk advances to next chunk",
			seedBuild: outcomeBuild("b-act", "th-1", ""),
			seedChunks: []VanadisChunk{
				actionChunk("c-act-1", "b-act", 0, "running", "Action Step 1"),
				actionChunk("c-act-2", "b-act", 1, "pending", "Action Step 2"),
			},
			event:         outcomeEvent("disp-act-1", "c-act-1", "completed", ""),
			wantNextQueue: true, wantNextIndex: 1, wantWidget: true, wantStatus: "completed",
		},
		{
			name:       "completed final action chunk completes build and wakes thread",
			seedBuild:  outcomeBuild("b-final", "th-final", "Complete all chunks"),
			seedChunks: []VanadisChunk{actionChunk("c-final-1", "b-final", 0, "running", "Final Step")},
			event:      outcomeEvent("disp-final", "c-final-1", "completed", ""),
			wantWidget: true, wantWake: true, wantStatus: "completed",
		},
		{
			name:       "failed chunk transitions to failed and wakes thread",
			seedBuild:  outcomeBuild("b-fail", "th-fail", "Try chunk"),
			seedChunks: []VanadisChunk{actionChunk("c-fail-1", "b-fail", 0, "running", "Failing Step")},
			event:      outcomeEvent("disp-fail", "c-fail-1", "failed", "command timed out"),
			wantWidget: true, wantWake: true, wantStatus: "failed",
		},
		{
			name:       "timeout chunk treated as failed",
			seedBuild:  outcomeBuild("b-to", "th-to", "Timeout test"),
			seedChunks: []VanadisChunk{actionChunk("c-to-1", "b-to", 0, "running", "Timeout Step")},
			event:      outcomeEvent("disp-to", "c-to-1", "timeout", ""),
			wantWidget: true, wantWake: true, wantStatus: "failed",
		},
		{name: "non-existent chunk returns nil without error", seedBuild: &VanadisBuild{ID: "b-empty", Status: "running"}, event: outcomeEvent("disp-missing", "c-nonexistent", "completed", "")},
		{name: "failed non-existent chunk returns nil without error", seedBuild: &VanadisBuild{ID: "b-empty-fail", Status: "running"}, event: outcomeEvent("disp-missing-fail", "c-missing-fail", "failed", "")},
		{
			name:       "failed chunk with empty error uses status fallback",
			seedBuild:  outcomeBuild("b-fail-noerr", "th-fail-noerr", "Try fallback error"),
			seedChunks: []VanadisChunk{actionChunk("c-fail-noerr-1", "b-fail-noerr", 0, "running", "Failing Step Without Error")},
			event:      outcomeEvent("disp-fail-noerr", "c-fail-noerr-1", "failed", ""),
			wantWidget: true, wantWake: true, wantStatus: "failed",
		},
		{
			name:      "redelivered completed action chunk re-enqueues next running chunk",
			seedBuild: outcomeBuild("b-redeliver", "th-redeliver", "Multi chunk build"),
			seedChunks: []VanadisChunk{
				actionChunk("c-redeliver-0", "b-redeliver", 0, "completed", "Already Completed Step"),
				actionChunk("c-redeliver-1", "b-redeliver", 1, "running", "Running Next Step Needs Enqueue"),
			},
			event:         outcomeEvent("disp-redeliver", "c-redeliver-0", "completed", ""),
			wantNextQueue: true, wantNextIndex: 1, wantStatus: "completed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			brain := newDispatchBrainServer(t)
			store, mem := newDispatchOutcomeStore(tc.nilStore, tc.getBuildErr, tc.seedBuild, tc.seedChunks)
			bus := &mockChunkBus{}
			var server *VanadisServer
			if !tc.nilServer {
				server = NewVanadisServer(Config{BrainURL: brain.URL}, store, brain.Client())
				server.SetBus(bus)
			}

			msg := &memoryMsg{data: dispatchOutcomePayload(t, tc.rawPayload, tc.event), subject: "aerial.dispatch.completed"}
			err := server.HandleDispatchOutcomeEvent(context.Background(), msg)
			if (err != nil) != tc.wantErr {
				t.Fatalf("HandleDispatchOutcomeEvent() error = %v, wantErr = %v", err, tc.wantErr)
			}

			widgetCalls, wakeCalls := brain.calls()
			if gotWidget := widgetCalls > 0 || bus.widgetCalls > 0; gotWidget != tc.wantWidget {
				t.Errorf("widget sync called = %v, want %v", gotWidget, tc.wantWidget)
			}
			if gotWake := wakeCalls > 0; gotWake != tc.wantWake {
				t.Errorf("wake prompt called = %v, want %v", gotWake, tc.wantWake)
			}
			assertPublishedChunkIndex(t, bus, tc.wantNextQueue, tc.wantNextIndex)
			assertDispatchedChunkStatus(t, mem, tc.event, tc.wantStatus)
		})
	}
}

// dispatchBrainServer fakes eri's widget-sync and wake-prompt endpoints and
// counts how often each was hit.
type dispatchBrainServer struct {
	*httptest.Server

	mu          sync.Mutex
	widgetCalls int
	wakeCalls   int
}

func newDispatchBrainServer(t *testing.T) *dispatchBrainServer {
	t.Helper()
	b := &dispatchBrainServer{}
	b.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b.mu.Lock()
		defer b.mu.Unlock()
		switch r.URL.Path {
		case "/internal/widget/sync":
			b.widgetCalls++
			httpkit.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
		case "/prompt":
			b.wakeCalls++
			httpkit.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(b.Close)
	return b
}

func (b *dispatchBrainServer) calls() (widget, wake int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.widgetCalls, b.wakeCalls
}

// newDispatchOutcomeStore returns a store failing every GetBuild with
// getBuildErr when set; otherwise it behaves like seedMemoryStore.
func newDispatchOutcomeStore(nilStore bool, getBuildErr error, seedBuild *VanadisBuild, seedChunks []VanadisChunk) (VanadisStore, *memoryVanadisStore) {
	if getBuildErr != nil {
		return &errVanadisStore{getBuildErr: getBuildErr}, nil
	}
	return seedMemoryStore(nilStore, seedBuild, seedChunks)
}

// dispatchOutcomePayload returns rawPayload verbatim when set, otherwise the
// JSON encoding of event (nil when both are empty).
func dispatchOutcomePayload(t *testing.T, rawPayload string, event *pubsub.DispatchOutcomeEvent) []byte {
	t.Helper()
	if rawPayload != "" {
		return []byte(rawPayload)
	}
	if event == nil {
		return nil
	}
	data, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	return data
}

// assertPublishedChunkIndex checks that the first published chunk job targets
// wantIndex; it is a no-op unless wantPublished is set.
func assertPublishedChunkIndex(t *testing.T, bus *mockChunkBus, wantPublished bool, wantIndex int) {
	t.Helper()
	if !wantPublished {
		return
	}
	if len(bus.published) == 0 {
		t.Errorf("expected chunk job published, got 0")
		return
	}
	if bus.published[0].ChunkIndex != wantIndex {
		t.Errorf("published chunk index = %d, want %d", bus.published[0].ChunkIndex, wantIndex)
	}
}

// assertDispatchedChunkStatus checks the stored status of the event's chunk;
// it is a no-op when want is empty or there is no store or event to inspect.
func assertDispatchedChunkStatus(t *testing.T, mem *memoryVanadisStore, event *pubsub.DispatchOutcomeEvent, want string) {
	t.Helper()
	if want == "" || mem == nil || event == nil {
		return
	}
	c, _, _ := mem.GetChunk(context.Background(), event.VanadisChunkID)
	if c != nil && c.Status != want {
		t.Errorf("chunk status = %q, want %q", c.Status, want)
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

func TestValidateChunkModeAndRepo(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		mode          string
		repo          string
		targetFiles   []string
		targetSymbols []string
		wantErr       bool
	}{
		{"invalid mode", "invalid-mode", "aerial", nil, nil, true},
		{"pr with empty repo", "pr", "", nil, nil, true},
		{"action with repo", "action", "aerial", nil, nil, true},
		{"action with target files", "action", "", []string{"file.go"}, nil, true},
		{"action with target symbols", "action", "", nil, []string{"Sym"}, true},
		{"valid action", "action", "", nil, nil, false},
		{"valid pr", "pr", "azylman/aerial", []string{"file.go"}, []string{"Sym"}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := validateChunkModeAndRepo(0, tc.mode, tc.repo, tc.targetFiles, tc.targetSymbols)
			if (err != nil) != tc.wantErr {
				t.Fatalf("validateChunkModeAndRepo(%q, %q) error = %v, wantErr %v", tc.mode, tc.repo, err, tc.wantErr)
			}
		})
	}
}

// createdBuildStore returns a memory store holding seedBuild and seedChunks
// created through CreateBuild, or an empty store when seedBuild is nil.
func createdBuildStore(t *testing.T, seedBuild *VanadisBuild, seedChunks []VanadisChunk) *memoryVanadisStore {
	t.Helper()
	mem := newMemoryVanadisStore()
	if seedBuild == nil {
		return mem
	}
	if err := mem.CreateBuild(context.Background(), *seedBuild, seedChunks); err != nil {
		t.Fatalf("CreateBuild(%s): %v", seedBuild.ID, err)
	}
	return mem
}

func TestVanadisServer_FailChunkInternal(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		nilServer  bool
		seedBuild  *VanadisBuild
		seedChunks []VanadisChunk
		chunkID    string
		reason     string
		wantErr    bool
		wantFailed bool
		wantFound  bool
	}{
		{"nil server", true, nil, nil, "c1", "reason", true, false, false},
		{"empty chunk id", false, nil, nil, "", "reason", true, false, false},
		{"empty reason", false, nil, nil, "c1", "", true, false, false},
		{
			"shutdown interruption is not a failure", false,
			&VanadisBuild{ID: "b-shut", ThreadID: "th-1", Objective: "shut"},
			[]VanadisChunk{{ID: "c-shut", BuildID: "b-shut", Title: "c-shut"}},
			"c-shut", "interrupted by shutdown", false, false, true,
		},
		{
			"running chunk fails", false,
			&VanadisBuild{ID: "b-f", ThreadID: "th-fail", Objective: "fail obj"},
			[]VanadisChunk{{ID: "c-f", BuildID: "b-f", Title: "c-f", Status: "running"}},
			"c-f", "test fail reason", false, true, true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var srv *VanadisServer
			if !tc.nilServer {
				srv = &VanadisServer{store: createdBuildStore(t, tc.seedBuild, tc.seedChunks)}
			}
			build, chunk, failed, err := srv.failChunkInternal(context.Background(), tc.chunkID, tc.reason)
			if (err != nil) != tc.wantErr {
				t.Fatalf("failChunkInternal() error = %v, wantErr %v", err, tc.wantErr)
			}
			if failed != tc.wantFailed {
				t.Errorf("failed = %v, want %v", failed, tc.wantFailed)
			}
			if gotFound := build != nil && chunk != nil; gotFound != tc.wantFound {
				t.Errorf("build and chunk returned = %v, want %v", gotFound, tc.wantFound)
			}
		})
	}
}

func TestVanadisServer_RetryNextRunningChunkEnqueue(t *testing.T) {
	t.Parallel()
	retryBuild := &VanadisBuild{ID: "b-ret", Status: "running"}
	retryChunks := []VanadisChunk{
		{ID: "c-ret-0", BuildID: "b-ret", ChunkIndex: 0, Status: "completed"},
		{ID: "c-ret-1", BuildID: "b-ret", ChunkIndex: 1, Status: "running"},
	}
	tests := []struct {
		name       string
		seedBuild  *VanadisBuild
		seedChunks []VanadisChunk
		res        *VanadisAdvancementResult
		wantErr    bool
	}{
		{"nil result", nil, nil, nil, false},
		{"pr mode is skipped", nil, nil, &VanadisAdvancementResult{ChunkMode: "pr"}, false},
		{"non-running build is skipped", nil, nil, &VanadisAdvancementResult{ChunkMode: "action", BuildStatus: "completed"}, false},
		{"missing build", nil, nil, &VanadisAdvancementResult{ChunkMode: "action", BuildStatus: "running", BuildID: "non-existent"}, true},
		{"enqueue without bus fails", retryBuild, retryChunks, &VanadisAdvancementResult{ChunkMode: "action", BuildStatus: "running", BuildID: "b-ret", MergedChunkIndex: 0}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := &VanadisServer{store: createdBuildStore(t, tc.seedBuild, tc.seedChunks)}
			err := srv.retryNextRunningChunkEnqueue(context.Background(), tc.res)
			if (err != nil) != tc.wantErr {
				t.Fatalf("retryNextRunningChunkEnqueue() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}
