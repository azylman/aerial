package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/nats-io/nats.go"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/db/dbtest"
	"github.com/azylman/aerial/services/eri/pkg/queue"
)

func TestEriJetStreamInit(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	errFactory := func(_ context.Context, _ string) (pubsub.Bus, error) {
		return nil, errors.New("nats connect error")
	}

	memBus := pubsub.NewMemoryBus()
	defer memBus.Close()
	successFactory := func(_ context.Context, _ string) (pubsub.Bus, error) {
		return memBus, nil
	}

	// attachEriEventBus with nil pool
	nilCleanup := attachEriEventBus(ctx, nil)
	if nilCleanup == nil {
		t.Error("expected non-nil no-op cleanup for nil pool")
	} else {
		nilCleanup()
	}

	// attachEriEventBus with empty pool
	emptyPool := queue.New(nil, queue.WorkerPoolConfig{NATSURL: ""})
	emptyCleanup := attachEriEventBus(ctx, emptyPool)
	if emptyCleanup == nil {
		t.Error("expected non-nil no-op cleanup for unconfigured pool")
	} else {
		emptyCleanup()
	}

	// attachEriEventBusWithFactory error branch
	pool := queue.New(nil, queue.WorkerPoolConfig{
		NATSURL: "nats://127.0.0.1:4222",
	})
	cleanupErr := attachEriEventBusWithFactory(ctx, pool, errFactory)
	if cleanupErr != nil {
		cleanupErr()
	}
	if pool.EventBus() != nil {
		t.Error("expected nil event bus on pool when factory fails")
	}

	// initEriJetStreamWithFactory empty url
	if b := initEriJetStreamWithFactory(ctx, "", nil); b != nil {
		t.Error("expected nil bus for empty natsURL")
	}

	// initEriJetStreamWithFactory with EnsureCoreStreams failure
	failBus := &failEnsureBus{Bus: memBus}
	if b := initEriJetStreamWithFactory(ctx, "nats://mock", func(_ context.Context, _ string) (pubsub.Bus, error) {
		return failBus, nil
	}); b == nil {
		t.Error("expected non-nil bus despite EnsureCoreStreams error")
	}

	// attachEriEventBusWithFactory success branch
	cleanupSuccess := attachEriEventBusWithFactory(ctx, pool, successFactory)
	if pool.EventBus() == nil {
		t.Error("expected non-nil event bus on pool when factory succeeds")
	}
	if cleanupSuccess != nil {
		cleanupSuccess()
	}
}

type failEnsureBus struct {
	pubsub.Bus
}

func (*failEnsureBus) EnsureCoreStreams(_ context.Context) error {
	return errors.New("simulated ensure error")
}

type natsURLResolutionTestCase struct {
	name          string
	poolURL       string
	expectedURL   string
	expectCleanup bool
}

func TestAttachEriEventBus_DefaultAndEnvURL(t *testing.T) {
	t.Parallel()

	tests := []natsURLResolutionTestCase{
		{
			name:          "returns nil when pool url empty",
			poolURL:       "",
			expectedURL:   "",
			expectCleanup: false,
		},
		{
			name:          "returns nil when pool url whitespace",
			poolURL:       "   ",
			expectedURL:   "",
			expectCleanup: false,
		},
		{
			name:          "pool config url connects",
			poolURL:       "nats://pool-nats:4222",
			expectedURL:   "nats://pool-nats:4222",
			expectCleanup: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var capturedURL string
			memBus := pubsub.NewMemoryBus()
			defer memBus.Close()
			capturingFactory := func(_ context.Context, u string) (pubsub.Bus, error) {
				capturedURL = u
				return memBus, nil
			}
			pool := queue.New(nil, queue.WorkerPoolConfig{
				NATSURL: tc.poolURL,
			})
			cleanup := attachEriEventBusWithFactory(context.Background(), pool, capturingFactory)
			if tc.expectCleanup {
				if cleanup == nil {
					t.Fatalf("expected non-nil cleanup function")
				}
				cleanup()
			} else if cleanup != nil {
				t.Errorf("expected nil cleanup function, got non-nil")
			}
			if capturedURL != tc.expectedURL {
				t.Errorf("capturedURL = %q, want %q", capturedURL, tc.expectedURL)
			}
		})
	}
}

type mockJobMessage struct {
	data    []byte
	subject string
}

func (m *mockJobMessage) Data() []byte                     { return m.data }
func (m *mockJobMessage) Subject() string                  { return m.subject }
func (*mockJobMessage) Header() nats.Header                { return nil }
func (*mockJobMessage) NumDelivered() uint64               { return 1 }
func (*mockJobMessage) Ack() error                         { return nil }
func (*mockJobMessage) NakWithDelay(_ time.Duration) error { return nil }
func (*mockJobMessage) Term() error                        { return nil }
func (*mockJobMessage) Snooze(_ time.Duration) error       { return nil }
func (*mockJobMessage) InProgress() error                  { return nil }

type notifyingStore struct {
	*dbtest.FakeStore

	onMsg  chan struct{}
	onDisp chan struct{}
}

func (s *notifyingStore) InsertMessage(ctx context.Context, msg db.Message) error {
	err := s.FakeStore.InsertMessage(ctx, msg)
	select {
	case s.onMsg <- struct{}{}:
	default:
	}
	return err
}

func (s *notifyingStore) InsertDispatch(ctx context.Context, d db.Dispatch) (bool, error) {
	ok, err := s.FakeStore.InsertDispatch(ctx, d)
	select {
	case s.onDisp <- struct{}{}:
	default:
	}
	return ok, err
}

func TestRegisterEriJobConsumers_EndToEnd(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	memBus := pubsub.NewMemoryBus()
	defer memBus.Close()
	if err := pubsub.EnsureCoreStreams(ctx, memBus); err != nil {
		t.Fatalf("EnsureCoreStreams: %v", err)
	}

	onMsg := make(chan struct{}, 1)
	onDisp := make(chan struct{}, 1)
	fakeStore := &notifyingStore{
		FakeStore: dbtest.NewFakeStore(),
		onMsg:     onMsg,
		onDisp:    onDisp,
	}
	deliveredChan := make(chan string, 1)
	deliveryFn := func(_ *discordgo.Session, target, text string) error {
		select {
		case deliveredChan <- target + ":" + text:
		default:
		}
		return nil
	}

	pool := queue.New(nil, queue.WorkerPoolConfig{
		Store:        fakeStore,
		DeliveryFunc: deliveryFn,
	})

	registerEriJobConsumers(ctx, memBus, pool)

	// 1. EriPromptJob
	promptArgs := pubsub.EriPromptJobArgs{
		ChannelID:  "123456789012345678",
		Prompt:     "end-to-end prompt test",
		DispatchID: "msg-prompt-e2e",
		Effort:     "low",
	}
	if err := memBus.Publish(ctx, pubsub.SubjectEriPromptJob, "msg-prompt-e2e", promptArgs); err != nil {
		t.Fatalf("Publish prompt: %v", err)
	}

	// Verify message persisted in store
	select {
	case <-onMsg:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for prompt message in store")
	}

	msgs, _ := fakeStore.GetRecentThreadMessages(ctx, "123456789012345678", 5)
	if len(msgs) == 0 || msgs[0].Content != "end-to-end prompt test" {
		t.Errorf("unexpected msgs: %+v", msgs)
	}

	// 2. EriDispatchJob
	dispatchArgs := pubsub.EriDispatchJobArgs{
		ID:        "disp-e2e-1",
		Source:    "vanadis",
		TargetID:  "build-100",
		SessionID: "sess-e2e",
		Prompt:    "end-to-end dispatch test",
		Effort:    "medium",
	}
	if err := memBus.Publish(ctx, pubsub.SubjectEriDispatchJob, "disp-e2e-1", dispatchArgs); err != nil {
		t.Fatalf("Publish dispatch: %v", err)
	}

	select {
	case <-onDisp:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for dispatch in store")
	}

	d, _ := fakeStore.GetDispatch(ctx, "disp-e2e-1")
	if d == nil || d.Prompt != "end-to-end dispatch test" {
		t.Errorf("unexpected dispatch: %+v", d)
	}

	// 3. EriDiscordMessageJob
	dmArgs := pubsub.EriDiscordMessageJobArgs{
		ChannelID: "123456789012345678",
		Content:   "hello from direct message job",
	}
	if err := memBus.Publish(ctx, pubsub.SubjectEriDiscordMessageJob, "dm-e2e-1", dmArgs); err != nil {
		t.Fatalf("Publish direct message: %v", err)
	}

	select {
	case delivered := <-deliveredChan:
		if delivered != "123456789012345678:hello from direct message job" {
			t.Errorf("delivered = %q, want '123456789012345678:hello from direct message job'", delivered)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for direct message delivery")
	}
}

type promptJobTestCase struct {
	name         string
	isNilMsg     bool
	payload      string
	hasStore     bool
	preInsertMsg bool
	passNilStore bool
	wantErr      bool
	wantInsert   bool
	wantMsgCount int
	checkUUID    bool
}

func setupPromptTestStore(ctx context.Context, tc promptJobTestCase) db.Store {
	if !tc.hasStore {
		return nil
	}
	fake := dbtest.NewFakeStore()
	if tc.preInsertMsg {
		_ = fake.InsertMessage(ctx, db.Message{
			ID:        "pre-msg-1",
			ThreadID:  "123456789012345678",
			Status:    db.StatusPending,
			CreatedAt: time.Now().UTC(),
			UpdatedAt: time.Now().UTC(),
		})
	}
	return fake
}

func verifyPromptJobOutcome(ctx context.Context, t *testing.T, store db.Store, tc promptJobTestCase) {
	t.Helper()
	s, ok := store.(*dbtest.FakeStore)
	if !ok {
		return
	}
	if tc.wantInsert {
		msgs, _ := s.GetRecentThreadMessages(ctx, "123456789012345678", 5)
		if len(msgs) == 0 {
			t.Errorf("expected inserted message, got none")
		}
	}
	if tc.checkUUID {
		msgs, _ := s.GetRecentThreadMessages(ctx, "123456789012345678", 5)
		if len(msgs) == 0 {
			t.Errorf("expected inserted message, got none")
			return
		}
		if msgs[0].ID == "" || msgs[0].ID == "msg-tbl-1" {
			t.Errorf("expected generated UUID, got %q", msgs[0].ID)
		}
	}
	if tc.wantMsgCount > 0 {
		msgs, _ := s.GetRecentThreadMessages(ctx, "123456789012345678", 5)
		if len(msgs) != tc.wantMsgCount {
			t.Errorf("got %d messages, want %d", len(msgs), tc.wantMsgCount)
		}
		if len(msgs) > 0 && msgs[0].ID != "pre-msg-1" {
			t.Errorf("got msg ID %q, want pre-msg-1", msgs[0].ID)
		}
	}
}

func TestHandleEriPromptJob_Table(t *testing.T) {
	t.Parallel()

	tests := []promptJobTestCase{
		{
			name:     "nil message returns error",
			isNilMsg: true,
			wantErr:  true,
		},
		{
			name:    "invalid json drops message without error",
			payload: `not-valid-json`,
			wantErr: false,
		},
		{
			name:    "empty prompt drops message without error",
			payload: `{"channel_id":"123456789012345678","prompt":""}`,
			wantErr: false,
		},
		{
			name:    "invalid snowflake drops message without error",
			payload: `{"channel_id":"not-a-snowflake","prompt":"hello"}`,
			wantErr: false,
		},
		{
			name:     "nil store returns error",
			payload:  `{"channel_id":"123456789012345678","prompt":"hello"}`,
			hasStore: false,
			wantErr:  true,
		},
		{
			name:       "valid prompt job persists and enqueues",
			payload:    `{"channel_id":"123456789012345678","prompt":"hello valid prompt","dispatch_id":"msg-tbl-1"}`,
			hasStore:   true,
			wantErr:    false,
			wantInsert: true,
		},
		{
			name:       "valid prompt job without dispatch_id generates uuid",
			payload:    `{"channel_id":"123456789012345678","prompt":"prompt without id"}`,
			hasStore:   true,
			wantErr:    false,
			wantInsert: true,
			checkUUID:  true,
		},
		{
			name:         "skip_if_active drops message when active turn exists",
			payload:      `{"channel_id":"123456789012345678","prompt":"skip active","skip_if_active":true}`,
			hasStore:     true,
			preInsertMsg: true,
			wantErr:      false,
			wantMsgCount: 1,
		},
		{
			name:         "reads store from pool when store parameter is nil",
			payload:      `{"channel_id":"123456789012345678","prompt":"store from pool"}`,
			hasStore:     true,
			passNilStore: true,
			wantErr:      false,
			wantInsert:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			var msg pubsub.Message
			if !tc.isNilMsg {
				msg = &mockJobMessage{data: []byte(tc.payload)}
			}

			store := setupPromptTestStore(ctx, tc)
			pool := queue.New(nil, queue.WorkerPoolConfig{Store: store})
			callStore := store
			if tc.passNilStore {
				callStore = nil
			}

			err := handleEriPromptJob(ctx, callStore, pool, msg)
			if (err != nil) != tc.wantErr {
				t.Fatalf("handleEriPromptJob() err = %v, wantErr = %v", err, tc.wantErr)
			}
			verifyPromptJobOutcome(ctx, t, store, tc)
		})
	}
}

type uninsertedStore struct {
	*dbtest.FakeStore
}

func (*uninsertedStore) InsertDispatch(_ context.Context, _ db.Dispatch) (bool, error) {
	return false, nil
}

type dispatchJobTestCase struct {
	name                string
	isNilMsg            bool
	payload             string
	hasStore            bool
	useUninsertedStore  bool
	preInsertDispActive bool
	preInsertDispDup    bool
	passNilStore        bool
	wantErr             bool
	wantInsert          bool
	checkNotInserted    bool
	checkSkipActive     bool
	checkDupUnchanged   bool
	checkStatelessID    bool
	wantOutcomeStatus   string
	wantOutcomeID       string
}

func setupDispatchTestStore(ctx context.Context, tc dispatchJobTestCase) db.Store {
	if !tc.hasStore {
		return nil
	}
	fake := dbtest.NewFakeStore()
	if tc.preInsertDispActive {
		_, _ = fake.InsertDispatch(ctx, db.Dispatch{
			ID:        "disp-active-existing",
			SessionID: "sess-active-1",
			Status:    db.StatusPending,
			CreatedAt: time.Now().UTC(),
			UpdatedAt: time.Now().UTC(),
		})
	}
	if tc.preInsertDispDup {
		_, _ = fake.InsertDispatch(ctx, db.Dispatch{
			ID:        "disp-dup-1",
			SessionID: "disp-dup-1",
			Status:    db.StatusCompleted,
			CreatedAt: time.Now().UTC(),
			UpdatedAt: time.Now().UTC(),
		})
	}
	if tc.useUninsertedStore {
		return &uninsertedStore{FakeStore: fake}
	}
	return fake
}

func verifyInsertedDispatch(ctx context.Context, store db.Store) bool {
	s, ok := store.(*dbtest.FakeStore)
	if !ok {
		return false
	}
	if d, _ := s.GetDispatch(ctx, "disp-tbl-1"); d != nil {
		return true
	}
	disps, _ := s.GetPendingOrProcessingDispatches(ctx, 10)
	return len(disps) > 0
}

func verifyDispatchJobOutcome(ctx context.Context, t *testing.T, store db.Store, tc dispatchJobTestCase) {
	t.Helper()
	s, ok := store.(*dbtest.FakeStore)
	if !ok {
		return
	}
	if tc.wantInsert && !verifyInsertedDispatch(ctx, store) {
		t.Errorf("expected inserted dispatch, got none")
	}
	if tc.checkNotInserted {
		if d, _ := s.GetDispatch(ctx, "disp-not-inserted"); d != nil {
			t.Errorf("expected disp-not-inserted not to exist in store: %+v", d)
		}
	}
	if tc.checkSkipActive {
		if d, _ := s.GetDispatch(ctx, "disp-new-incoming"); d != nil {
			t.Errorf("expected disp-new-incoming to be skipped, got %+v", d)
		}
		disps, _ := s.GetPendingOrProcessingDispatches(ctx, 10)
		if len(disps) != 1 || disps[0].ID != "disp-active-existing" {
			t.Errorf("expected only disp-active-existing in pending, got %+v", disps)
		}
	}
	if tc.checkDupUnchanged {
		d, err := s.GetDispatch(ctx, "disp-dup-1")
		if err != nil || d == nil {
			t.Fatalf("expected disp-dup-1 to exist, err = %v", err)
		}
		if d.Status != db.StatusCompleted {
			t.Errorf("expected disp-dup-1 status to remain completed, got %q", d.Status)
		}
	}
	if tc.checkStatelessID {
		verifyStatelessDispatch(ctx, t, s)
	}
}

func verifyStatelessDispatch(ctx context.Context, t *testing.T, s *dbtest.FakeStore) {
	t.Helper()
	disps, _ := s.GetPendingOrProcessingDispatches(ctx, 10)
	if len(disps) == 0 {
		t.Fatalf("expected inserted dispatch for stateless test, got none")
	}
	last := disps[len(disps)-1]
	if last.SessionID != "" {
		t.Errorf("expected empty SessionID for stateless dispatch, got %q", last.SessionID)
	}
	if last.ID == "" {
		t.Errorf("expected auto-generated ID for dispatch, got empty")
	}
}

func verifyDispatchOutcomeEvent(t *testing.T, outcomeChan <-chan pubsub.DispatchOutcomeEvent, tc dispatchJobTestCase) {
	t.Helper()
	if tc.wantOutcomeStatus == "" {
		select {
		case ev := <-outcomeChan:
			t.Errorf("unexpected outcome event published: %+v", ev)
		default:
		}
		return
	}
	select {
	case outcomeEv := <-outcomeChan:
		if outcomeEv.Status != tc.wantOutcomeStatus {
			t.Errorf("outcomeEv.Status = %q, want %q", outcomeEv.Status, tc.wantOutcomeStatus)
		}
		if outcomeEv.DispatchID != tc.wantOutcomeID {
			t.Errorf("outcomeEv.DispatchID = %q, want %q", outcomeEv.DispatchID, tc.wantOutcomeID)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for outcome event")
	}
}

func TestHandleEriDispatchJob_Table(t *testing.T) {
	t.Parallel()

	tests := []dispatchJobTestCase{
		{
			name:     "nil message returns error",
			isNilMsg: true,
			wantErr:  true,
		},
		{
			name:    "invalid json drops message without error",
			payload: `not-valid-json`,
			wantErr: false,
		},
		{
			name:              "invalid source drops message and publishes failed outcome",
			payload:           `{"id":"disp-inv-src","source":"INVALID SOURCE!","prompt":"hello"}`,
			hasStore:          true,
			wantErr:           false,
			wantOutcomeStatus: "failed",
			wantOutcomeID:     "disp-inv-src",
		},
		{
			name:    "empty prompt drops message without error",
			payload: `{"source":"vanadis","prompt":""}`,
			wantErr: false,
		},
		{
			name:     "nil store returns error",
			payload:  `{"source":"vanadis","prompt":"hello"}`,
			hasStore: false,
			wantErr:  true,
		},
		{
			name:       "valid dispatch job persists and enqueues",
			payload:    `{"id":"disp-tbl-1","source":"vanadis","prompt":"hello valid dispatch"}`,
			hasStore:   true,
			wantErr:    false,
			wantInsert: true,
		},
		{
			name:             "valid dispatch job without id generates id and retains empty session_id",
			payload:          `{"source":"vanadis","prompt":"dispatch without id"}`,
			hasStore:         true,
			wantErr:          false,
			wantInsert:       true,
			checkStatelessID: true,
		},
		{
			name:               "insert dispatch returns false skips enqueue without outcome",
			payload:            `{"id":"disp-not-inserted","source":"vanadis","prompt":"uninserted dispatch"}`,
			hasStore:           true,
			useUninsertedStore: true,
			checkNotInserted:   true,
			wantErr:            false,
		},
		{
			name:                "skip_if_active drops message when active dispatch turn exists",
			payload:             `{"id":"disp-new-incoming","source":"vanadis","session_id":"sess-active-1","prompt":"skip active dispatch","skip_if_active":true}`,
			hasStore:            true,
			preInsertDispActive: true,
			checkSkipActive:     true,
			wantErr:             false,
			wantOutcomeStatus:   "skipped_active",
			wantOutcomeID:       "disp-new-incoming",
		},
		{
			name:              "duplicate dispatch drops message without error and without outcome",
			payload:           `{"id":"disp-dup-1","source":"vanadis","prompt":"duplicate dispatch"}`,
			hasStore:          true,
			preInsertDispDup:  true,
			checkDupUnchanged: true,
			wantErr:           false,
		},
		{
			name:         "reads store from pool when store parameter is nil",
			payload:      `{"id":"disp-pool-1","source":"vanadis","prompt":"store from pool dispatch"}`,
			hasStore:     true,
			passNilStore: true,
			wantErr:      false,
			wantInsert:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			var msg pubsub.Message
			if !tc.isNilMsg {
				msg = &mockJobMessage{data: []byte(tc.payload)}
			}

			store := setupDispatchTestStore(ctx, tc)
			memBus := pubsub.NewMemoryBus()
			defer memBus.Close()
			_ = pubsub.EnsureCoreStreams(ctx, memBus)

			outcomeChan := make(chan pubsub.DispatchOutcomeEvent, 1)
			_, _ = memBus.Subscribe(ctx, pubsub.SubjectAerialDispatchPrefix+">", "test-outcome", func(_ context.Context, outcomeMsg pubsub.Message) error {
				var ev pubsub.DispatchOutcomeEvent
				_ = json.Unmarshal(outcomeMsg.Data(), &ev)
				select {
				case outcomeChan <- ev:
				default:
				}
				return nil
			})

			pool := queue.New(nil, queue.WorkerPoolConfig{Store: store})
			pool.SetEventBus(memBus)

			callStore := store
			if tc.passNilStore {
				callStore = nil
			}

			err := handleEriDispatchJob(ctx, callStore, pool, msg)
			if (err != nil) != tc.wantErr {
				t.Fatalf("handleEriDispatchJob() err = %v, wantErr = %v", err, tc.wantErr)
			}
			verifyDispatchJobOutcome(ctx, t, store, tc)
			verifyDispatchOutcomeEvent(t, outcomeChan, tc)
		})
	}
}

func TestHandleEriDiscordMessageJob_Table(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		isNilMsg     bool
		isNilPool    bool
		payload      string
		wantErr      bool
		wantDelivery string
	}{
		{
			name:     "nil message returns error",
			isNilMsg: true,
			wantErr:  true,
		},
		{
			name:      "nil pool returns error",
			isNilPool: true,
			payload:   `{"channel_id":"123456789012345678","content":"hello"}`,
			wantErr:   true,
		},
		{
			name:    "invalid json drops message without error",
			payload: `not-valid-json`,
			wantErr: false,
		},
		{
			name:    "invalid channel id drops message without error",
			payload: `{"channel_id":"not-a-snowflake","content":"hello"}`,
			wantErr: false,
		},
		{
			name:    "empty content drops message without error",
			payload: `{"channel_id":"123456789012345678","content":"   "}`,
			wantErr: false,
		},
		{
			name:         "valid discord message job delivers",
			payload:      `{"channel_id":"123456789012345678","content":"direct message text"}`,
			wantErr:      false,
			wantDelivery: "123456789012345678:direct message text",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			var msg pubsub.Message
			if !tc.isNilMsg {
				msg = &mockJobMessage{data: []byte(tc.payload)}
			}

			var delivered string
			var pool *queue.WorkerPool
			if !tc.isNilPool {
				pool = queue.New(nil, queue.WorkerPoolConfig{
					DeliveryFunc: func(_ *discordgo.Session, target, text string) error {
						delivered = target + ":" + text
						return nil
					},
				})
			}

			err := handleEriDiscordMessageJob(ctx, pool, msg)
			if (err != nil) != tc.wantErr {
				t.Fatalf("handleEriDiscordMessageJob() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantDelivery != "" && delivered != tc.wantDelivery {
				t.Errorf("delivered = %q, want %q", delivered, tc.wantDelivery)
			}
		})
	}
}
