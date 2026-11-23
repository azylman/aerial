package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/x/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/pgtest"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/pubsubtest"
	"github.com/azylman/aerial/services/coven/internal/config"
	"github.com/azylman/aerial/services/coven/internal/store"
)

// mainBusFactory returns an initCovenJetStreamWithFactory factory that hands out b.
func mainBusFactory(b pubsub.Bus) func(context.Context, string) (pubsub.Bus, error) {
	return func(context.Context, string) (pubsub.Bus, error) { return b, nil }
}

// mainWaitHandled returns the next handler result reported on handled.
func mainWaitHandled(t *testing.T, handled chan error) error {
	t.Helper()
	select {
	case err := <-handled:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a handler to finish")
		return nil
	}
}

func TestCoven_InitJetStreamFaults(t *testing.T) {
	ctx := t.Context()

	if bus := initCovenJetStream(ctx, nil, ""); bus != nil {
		t.Error("expected nil bus for empty natsURL")
	}

	errFactory := func(_ context.Context, _ string) (pubsub.Bus, error) {
		return nil, errors.New("connection failed")
	}
	if bus := initCovenJetStreamWithFactory(ctx, nil, "nats://mock", errFactory); bus != nil {
		t.Error("expected nil bus when factory returns error")
	}

	memBus := pubsub.NewMemoryBus()
	defer memBus.Close()

	// Ensure stream failure logs warning but returns bus
	brokenEnsure := &pubsubtest.FaultBus{Bus: memBus, EnsureErr: errors.New("ensure core streams failed")}
	if bus := initCovenJetStreamWithFactory(ctx, nil, "nats://mock", mainBusFactory(brokenEnsure)); bus == nil {
		t.Error("expected non-nil bus even if ensure stream fails")
	}

	// Subscribe failure logs warning, starts the background retry and returns bus
	worker := NewWorker(WorkerConfig{})
	brokenSub := &pubsubtest.FaultBus{Bus: memBus, SubscribeErr: errors.New("subscribe failed")}
	subCtx, subCancel := context.WithCancel(ctx)
	if bus := initCovenJetStreamWithFactory(subCtx, worker, "nats://mock", mainBusFactory(brokenSub)); bus == nil {
		t.Error("expected non-nil bus even if subscribe fails")
	}
	subCancel()

	// Unmarshal error on bad JSON payload
	wrapped := &pubsubtest.FaultBus{Bus: memBus, Handled: make(chan error, 1)}
	if bus := initCovenJetStreamWithFactory(ctx, worker, "nats://mock", mainBusFactory(wrapped)); bus == nil {
		t.Fatal("expected non-nil bus")
	}

	if err := memBus.Publish(ctx, pubsub.SubjectAerialTurnPrefix+"transcripts_indexed", "bad-sync-1", "not-a-valid-json"); err != nil {
		t.Fatalf("Publish bad json: %v", err)
	}
	if err := mainWaitHandled(t, wrapped.Handled); err != nil {
		t.Errorf("expected nil error on bad json unmarshal to avoid poison redelivery loop, got %v", err)
	}
}

func setSubscribeRetryIntervalForTest(d time.Duration) func() {
	prev := subscribeRetryIntervalNs.Swap(int64(d))
	return func() {
		subscribeRetryIntervalNs.Store(prev)
	}
}

func TestCoven_RetrySubscribeCovenTurns(t *testing.T) {
	restore := setSubscribeRetryIntervalForTest(5 * time.Millisecond)
	defer restore()

	memBus := pubsub.NewMemoryBus()
	defer memBus.Close()

	consumers := &covenConsumers{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		retrySubscribeCovenConsumers(t.Context(), memBus, NewWorker(WorkerConfig{}), consumers)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for background retry to subscribe coven consumers")
	}
	if !consumers.subscribedTranscriptsIndexed || !consumers.subscribedExtract {
		t.Errorf("consumers = %+v, want both subscribed", *consumers)
	}
}

type handleCovenExtractJobTestCase struct {
	name         string
	msgData      string
	delivered    uint64
	role         string
	wantErr      bool
	wantTerm     bool
	wantMinNak   time.Duration
	wantExactNak time.Duration
	wantSends    []string
}

// ingressRecorder is a Restate ingress stub recording "<path> key=<Idempotency-Key>" per request;
// bodies naming th-ext-fail get a 500.
type ingressRecorder struct {
	mu    sync.Mutex
	sends []string
}

func (rec *ingressRecorder) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	rec.mu.Lock()
	rec.sends = append(rec.sends, r.URL.Path+" key="+r.Header.Get("Idempotency-Key"))
	rec.mu.Unlock()
	if strings.Contains(string(body), "th-ext-fail") {
		http.Error(w, `{"error":"restate invoke error"}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{}`))
}

func (rec *ingressRecorder) snapshot() []string {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return slices.Clone(rec.sends)
}

func runHandleCovenExtractJobCase(t *testing.T, tc handleCovenExtractJobTestCase) {
	t.Helper()
	ctx := context.Background()
	rec := &ingressRecorder{}
	srv := httptest.NewServer(rec)
	defer srv.Close()
	worker := NewWorker(WorkerConfig{
		Role:              tc.role,
		RestateIngressURL: srv.URL,
		HTTPClient:        srv.Client(),
	})
	msg := &pubsubtest.Message{
		Payload:   []byte(tc.msgData),
		Delivered: tc.delivered,
		Hdr:       nats.Header{pubsub.HeaderMsgID: []string{"msg-test-1"}},
	}

	err := handleCovenExtractJob(ctx, msg, worker)
	if (err != nil) != tc.wantErr {
		t.Fatalf("handleCovenExtractJob() err = %v, wantErr = %v", err, tc.wantErr)
	}
	if msg.Termed() != tc.wantTerm {
		t.Errorf("termed = %v, want %v", msg.Termed(), tc.wantTerm)
	}
	nakDelay, naked := msg.NakDelay()
	if wantNak := tc.wantMinNak > 0 || tc.wantExactNak > 0; naked != wantNak {
		t.Errorf("naked = %v, want %v", naked, wantNak)
	}
	if tc.wantMinNak > 0 && nakDelay < tc.wantMinNak {
		t.Errorf("nakDelay = %v, want >= %v", nakDelay, tc.wantMinNak)
	}
	if tc.wantExactNak > 0 && nakDelay != tc.wantExactNak {
		t.Errorf("nakDelay = %v, want %v", nakDelay, tc.wantExactNak)
	}
	if got := rec.snapshot(); !slices.Equal(got, tc.wantSends) {
		t.Errorf("ingress sends = %q, want %q", got, tc.wantSends)
	}
}

func TestHandleCovenExtractJob_Scenarios(t *testing.T) {
	t.Parallel()
	aerialSend := []string{"/CovenThread/aerial/th-ext-1/Extract/send key=coven:extract:aerial/th-ext-1:msg-test-1"}
	failSend := []string{"/CovenThread/aerial/th-ext-fail/Extract/send key=coven:extract:aerial/th-ext-fail:msg-test-1"}

	tests := []handleCovenExtractJobTestCase{
		{name: "UnmarshalErrorTermsMessageAndReturnsNil", msgData: "bad-json", delivered: 1, wantTerm: true},
		{name: "EmptyThreadIDTermsMessageAndReturnsNil", msgData: `{"thread_id":"   "}`, delivered: 1, wantTerm: true},
		{name: "InvalidAgentSlugTermsMessage", msgData: `{"thread_id":"th-ext-1","agent":"Bad Agent!"}`, delivered: 1, wantTerm: true},
		{name: "MismatchedAgentSkipsExtraction", msgData: `{"thread_id":"th-ext-1","agent":"agent-alpha"}`, delivered: 1},
		{name: "RoleAllAcceptsAnyValidAgent", msgData: `{"thread_id":"th-ext-1","agent":"agent-alpha"}`, delivered: 1, role: "all", wantSends: []string{"/CovenThread/agent-alpha/th-ext-1/Extract/send key=coven:extract:agent-alpha/th-ext-1:msg-test-1"}},
		{name: "SendsKeyedByMessageID", msgData: `{"thread_id":"th-ext-1"}`, delivered: 1, wantSends: aerialSend},
		{name: "IngressErrorAttempt1NaksWithBackoff", msgData: `{"thread_id":"th-ext-fail"}`, delivered: 1, wantErr: true, wantMinNak: 1 * time.Second, wantSends: failSend},
		{name: "IngressErrorAttempt4NaksWithBackoff", msgData: `{"thread_id":"th-ext-fail"}`, delivered: 4, wantErr: true, wantMinNak: 8 * time.Second, wantSends: failSend},
		{name: "IngressErrorAttempt5NaksWithBackoff", msgData: `{"thread_id":"th-ext-fail"}`, delivered: 5, wantErr: true, wantMinNak: 16 * time.Second, wantSends: failSend},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runHandleCovenExtractJobCase(t, tc)
		})
	}
}

func TestCoven_TranscriptsIndexedToCoalescedExtractIntegration(t *testing.T) {
	ctx := context.Background()

	rec := &ingressRecorder{}
	srv := httptest.NewServer(rec)
	defer srv.Close()
	worker := NewWorker(WorkerConfig{RestateIngressURL: srv.URL, HTTPClient: srv.Client()})

	memBus := pubsub.NewMemoryBus()
	defer memBus.Close()

	if err := pubsub.EnsureCoreStreams(ctx, memBus); err != nil {
		t.Fatalf("EnsureCoreStreams: %v", err)
	}

	// Handled reports the transcripts-indexed handler and the coalesced extract handler.
	wrapped := &pubsubtest.FaultBus{Bus: memBus, Handled: make(chan error, 2)}
	if bus := initCovenJetStreamWithFactory(ctx, worker, "nats://mock", mainBusFactory(wrapped)); bus == nil {
		t.Fatal("expected non-nil bus")
	}

	ev := pubsub.TranscriptsIndexedEvent{
		Stack:        "aerial",
		SessionID:    "sess-e2e-1",
		ThreadID:     "th-e2e-1",
		FirstStep:    1,
		LastStep:     2,
		FileMtime:    time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		StepsIndexed: 2,
		Final:        true,
		IndexedAt:    time.Now().UTC(),
	}
	if err := memBus.Publish(ctx, pubsub.SubjectAerialTurnPrefix+"transcripts_indexed", "sync-1", ev); err != nil {
		t.Fatalf("Publish TranscriptsIndexed: %v", err)
	}

	for range 2 {
		if err := mainWaitHandled(t, wrapped.Handled); err != nil {
			t.Fatalf("handler err = %v", err)
		}
	}

	want := []string{"/CovenThread/aerial/th-e2e-1/Extract/send key=coven:extract:aerial/th-e2e-1:coven:extract:aerial:th-e2e-1:sess-e2e-1:1-2:1767225600000"}
	if got := rec.snapshot(); !slices.Equal(got, want) {
		t.Errorf("ingress sends = %q, want %q", got, want)
	}
}

func TestCoven_SubscriptionFaults(t *testing.T) {
	ctx := t.Context()

	worker := NewWorker(WorkerConfig{})
	memBus := pubsub.NewMemoryBus()
	defer memBus.Close()

	// Nil bus or nil worker returns nil safely
	if err := subscribeCovenTranscriptsIndexed(ctx, nil, "aerial"); err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if err := subscribeCovenExtract(ctx, nil, worker); err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if err := subscribeCovenExtract(ctx, memBus, nil); err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if err := (&covenConsumers{}).subscribeAll(ctx, nil, worker); err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if err := (&covenConsumers{}).subscribeAll(ctx, memBus, nil); err != nil {
		t.Errorf("expected nil error, got %v", err)
	}

	// Broken subscribe returns error
	brokenSub := &pubsubtest.FaultBus{Bus: memBus, SubscribeErr: errors.New("subscribe failed")}
	if err := subscribeCovenTranscriptsIndexed(ctx, brokenSub, "aerial"); err == nil {
		t.Error("expected error when subscribe fails")
	}
	if err := subscribeCovenExtract(ctx, brokenSub, worker); err == nil {
		t.Error("expected error when subscribe fails")
	}
	// Fails on coven-transcripts-indexed, the first consumer.
	if err := (&covenConsumers{}).subscribeAll(ctx, brokenSub, worker); err == nil {
		t.Error("expected error when subscribe coven-transcripts-indexed fails")
	}
	// Indexed already subscribed, so the failure comes from coven-extract.
	if err := (&covenConsumers{subscribedTranscriptsIndexed: true}).subscribeAll(ctx, brokenSub, worker); err == nil {
		t.Error("expected error when subscribe coven-extract fails")
	}

	// Canceled retry exits cleanly
	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()
	consumers := &covenConsumers{}
	retrySubscribeCovenConsumers(canceledCtx, memBus, worker, consumers)
}

func TestCovenDurableNamesAndFilters(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name               string
		stack              string
		wantIndexedDurable string
		wantExtractDurable string
		wantExtractFilter  string
	}{
		{
			name:               "aerial stack defaults",
			stack:              "aerial",
			wantIndexedDurable: "coven-transcripts-indexed",
			wantExtractDurable: "coven-extract",
			wantExtractFilter:  "aerial.coalesced.coven_extract.>",
		},
		{
			name:               "empty stack defaults to aerial",
			stack:              "",
			wantIndexedDurable: "coven-transcripts-indexed",
			wantExtractDurable: "coven-extract",
			wantExtractFilter:  "aerial.coalesced.coven_extract.>",
		},
		{
			name:               "agent-alpha stack scoped",
			stack:              "agent-alpha",
			wantIndexedDurable: "coven-transcripts-indexed-agent-alpha",
			wantExtractDurable: "coven-extract-agent-alpha",
			wantExtractFilter:  "aerial.coalesced.coven_extract.agent-alpha.>",
		},
		{
			name:               "agent-beta stack scoped",
			stack:              "agent-beta",
			wantIndexedDurable: "coven-transcripts-indexed-agent-beta",
			wantExtractDurable: "coven-extract-agent-beta",
			wantExtractFilter:  "aerial.coalesced.coven_extract.agent-beta.>",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := covenTranscriptsIndexedDurable(tc.stack); got != tc.wantIndexedDurable {
				t.Errorf("covenTranscriptsIndexedDurable(%q) = %q, want %q", tc.stack, got, tc.wantIndexedDurable)
			}
			if got := covenExtractDurable(tc.stack); got != tc.wantExtractDurable {
				t.Errorf("covenExtractDurable(%q) = %q, want %q", tc.stack, got, tc.wantExtractDurable)
			}
			if got := covenExtractSubjectFilter(tc.stack); got != tc.wantExtractFilter {
				t.Errorf("covenExtractSubjectFilter(%q) = %q, want %q", tc.stack, got, tc.wantExtractFilter)
			}
		})
	}
}

func TestRunApp_Lifecycle(t *testing.T) {
	validDSN := pgtest.NewDSN(t)
	const ingress = "http://127.0.0.1:1"

	tests := []struct {
		name      string
		cfg       *config.Config
		wantErr   bool
		wantErrIs error
	}{
		{
			name:    "NilConfigFails",
			cfg:     nil,
			wantErr: true,
		},
		{
			name:      "EmptyRestateIngressURLFails",
			cfg:       &config.Config{Port: "0", RestatePort: "0", DatabaseURL: validDSN},
			wantErr:   true,
			wantErrIs: errRestateIngressRequired,
		},
		{
			name:    "InvalidDBFails",
			cfg:     &config.Config{DatabaseURL: "mysql://x", RestateIngressURL: ingress},
			wantErr: true,
		},
		{
			name:    "CleanLifecycleWithPostgres",
			cfg:     &config.Config{Port: "0", RestatePort: "0", DatabaseURL: validDSN, RestateIngressURL: ingress},
			wantErr: false,
		},
		{
			name:    "CleanLifecycleExtractorWithTranscriptsURL",
			cfg:     &config.Config{Port: "0", RestatePort: "0", DatabaseURL: validDSN, TranscriptsURL: "http://mock-transcripts", Role: "extractor", RestateIngressURL: ingress},
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			err := RunApp(ctx, tc.cfg)
			if tc.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if tc.wantErrIs != nil && !errors.Is(err, tc.wantErrIs) {
				t.Fatalf("RunApp() err = %v, want %v", err, tc.wantErrIs)
			}
			if !tc.wantErr && err != nil && !errors.Is(err, http.ErrServerClosed) {
				t.Fatalf("RunApp() unexpected err = %v", err)
			}
		})
	}
}

// covenRunContext is the restate.RunContext stub.
type covenRunContext struct {
	context.Context
}

func (covenRunContext) Log() *slog.Logger         { return slog.Default() }
func (covenRunContext) Request() *restate.Request { return nil }

type covenRunCapture struct {
	ran         bool
	fnErr       error
	isTerminal  bool
	hasRetryCap bool
}

func stubCovenRun(mc *mocks.MockContext, keepTerminal bool) *covenRunCapture {
	rec := &covenRunCapture{}
	run := func(fn func(restate.RunContext) (any, error), output any, opts ...restate.RunOption) restate.TerminalError {
		rec.ran = true
		for _, opt := range opts {
			if strings.Contains(fmt.Sprintf("%T", opt), "withMaxRetryAttempts") {
				rec.hasRetryCap = true
			}
		}
		res, err := fn(covenRunContext{Context: context.Background()})
		rec.fnErr = err
		rec.isTerminal = restate.IsTerminalError(err)
		switch {
		case err != nil && keepTerminal && restate.IsTerminalError(err):
			return restate.AsTerminalError(err)
		case err != nil && keepTerminal && rec.hasRetryCap:
			return restate.TerminalErrorf("retries exhausted: %v", err)
		case err != nil && keepTerminal:
			return nil
		case err != nil:
			return restate.TerminalErrorf("%v", err)
		}
		if out := reflect.ValueOf(output); res != nil && out.Kind() == reflect.Pointer && !out.IsNil() {
			out.Elem().Set(reflect.ValueOf(res))
		}
		return nil
	}
	for arity := 2; arity <= 6; arity++ {
		mc.On("Run", slices.Repeat([]any{mock.Anything}, arity)...).Return(run).Maybe()
	}
	return rec
}

type covenSentCall struct {
	service string
	key     string
	method  string
	input   any
	delayed bool
	option  restate.SendOption
}

type covenSendCapture struct {
	mu    sync.Mutex
	calls []covenSentCall
}

func (c *covenSendCapture) record(args mock.Arguments) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, covenSentCall{service: args.String(0), key: args.String(1), method: args.String(2)})
}

func (c *covenSendCapture) complete(input any, delayed bool, option restate.SendOption) {
	c.mu.Lock()
	defer c.mu.Unlock()
	last := &c.calls[len(c.calls)-1]
	last.input = input
	last.delayed = delayed
	last.option = option
}

func (c *covenSendCapture) snapshot() []covenSentCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]covenSentCall(nil), c.calls...)
}

func captureCovenSends(t *testing.T, mc *mocks.MockContext) *covenSendCapture {
	t.Helper()
	rec := &covenSendCapture{}
	client := mocks.NewMockClient(t)
	invocation := mocks.NewMockInvocation(t)
	mc.On("Object", mock.Anything, mock.Anything, mock.Anything).Run(rec.record).Return(client).Maybe()
	client.On("Send", mock.Anything).Run(func(args mock.Arguments) { rec.complete(args.Get(0), false, nil) }).Return(invocation).Maybe()
	client.On("Send", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		var opt restate.SendOption
		if len(args) > 1 {
			if o, ok := args.Get(1).(restate.SendOption); ok {
				opt = o
			}
		}
		rec.complete(args.Get(0), true, opt)
	}).Return(invocation).Maybe()
	return rec
}

func TestNewRestateServer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		thread      *CovenThread
		dreamer     *CovenDreamer
		wantThread  bool
		wantDreamer bool
	}{
		{
			name:        "both bound",
			thread:      NewCovenThread(nil),
			dreamer:     NewCovenDreamer(nil),
			wantThread:  true,
			wantDreamer: true,
		},
		{
			name:        "only thread bound",
			thread:      NewCovenThread(nil),
			dreamer:     nil,
			wantThread:  true,
			wantDreamer: false,
		},
		{
			name:        "only dreamer bound",
			thread:      nil,
			dreamer:     NewCovenDreamer(nil),
			wantThread:  false,
			wantDreamer: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := NewRestateServer(tc.thread, tc.dreamer)
			require.NotNil(t, srv)
			h, err := srv.Handler()
			require.NoError(t, err)

			req := httptest.NewRequest(http.MethodGet, "/discover", http.NoBody)
			req.Header.Set("Accept", "application/vnd.restate.endpointmanifest.v3+json")
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req)

			var manifest struct {
				Services []struct {
					Name string `json:"name"`
				} `json:"services"`
			}
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &manifest))

			hasThread := false
			hasDreamer := false
			for _, s := range manifest.Services {
				if s.Name == "CovenThread" {
					hasThread = true
				}
				if s.Name == "CovenDreamer" {
					hasDreamer = true
				}
			}
			require.Equal(t, tc.wantThread, hasThread, "CovenThread binding mismatch")
			require.Equal(t, tc.wantDreamer, hasDreamer, "CovenDreamer binding mismatch")
		})
	}
}

type covenDreamerArmTestCase struct {
	name        string
	key         string
	nilWorker   bool
	nilStore    bool
	failRun     error
	lastRunAt   *time.Time
	wantErrSub  string
	wantDelayed bool
	wantStack   string
	wantSend    bool
}

// stubDreamDueState backs dreamDueStateKey with *due: Get reads it, Set writes it.
func stubDreamDueState(mc *mocks.MockContext, due *int64) {
	mc.On("Get", dreamDueStateKey, mock.Anything).Run(func(args mock.Arguments) {
		if p, ok := args.Get(1).(*int64); ok && p != nil {
			*p = *due
		}
	}).Return(*due != 0, nil).Maybe()
	mc.On("Set", dreamDueStateKey, mock.Anything).Run(func(args mock.Arguments) {
		if v, ok := args.Get(1).(int64); ok {
			*due = v
		}
	}).Return().Maybe()
}

func runCovenDreamerArmCase(t *testing.T, tc covenDreamerArmTestCase) {
	t.Helper()
	var worker *Worker
	if !tc.nilWorker {
		var s store.Store
		if !tc.nilStore {
			db := newTestDB(t)
			if tc.lastRunAt != nil {
				seedWatermark(t, db, config.NormalizeStack(tc.key), DreamingWatermarkThreadID, 1, tc.lastRunAt)
			}
			s = store.NewSQLStore(db)
		}
		worker = NewWorker(WorkerConfig{Store: s})
	}

	mc := mocks.NewMockContext(t)
	mc.On("Key").Return(tc.key).Maybe()
	mc.On("Clear", legacyArmedStateKey).Return().Maybe()
	mc.On("Clear", legacyFailuresStateKey).Return().Maybe()
	var due int64
	stubDreamDueState(mc, &due)
	sends := captureCovenSends(t, mc)
	if tc.failRun != nil {
		mc.On("Run", mock.Anything, mock.Anything, mock.Anything).Return(restate.TerminalErrorf("%v", tc.failRun))
	} else {
		stubCovenRun(mc, true)
	}

	dreamer := NewCovenDreamer(worker)
	err := dreamer.Arm(restate.WithMockContext(mc), struct{}{})
	if tc.wantErrSub != "" {
		require.Error(t, err)
		require.Contains(t, err.Error(), tc.wantErrSub)
		require.Empty(t, sends.snapshot())
		return
	}
	require.NoError(t, err)

	calls := sends.snapshot()
	require.Len(t, calls, 1)
	require.Equal(t, "CovenDreamer", calls[0].service)
	require.Equal(t, tc.wantStack, calls[0].key)
	require.Equal(t, "Dream", calls[0].method)
	require.Equal(t, tc.wantDelayed, calls[0].delayed)
	require.NotZero(t, due, "Arm must store the due token")
	require.Equal(t, DreamRequest{Due: due}, calls[0].input, "the sent Dream must carry the stored due token")
}

func TestCovenDreamer_Arm(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	recent := now.Add(-2 * time.Hour)
	stale := now.Add(-25 * time.Hour)

	tests := []covenDreamerArmTestCase{
		{name: "nil worker defaults delay to min interval", key: "", nilWorker: true, wantDelayed: true, wantStack: config.DefaultStack, wantSend: true},
		{name: "nil store defaults delay to min interval", key: "agent-beta", nilStore: true, wantDelayed: true, wantStack: "agent-beta", wantSend: true},
		{name: "zero watermark runs immediately without delay", key: "aerial", wantStack: "aerial", wantSend: true},
		{name: "recent watermark arms with remaining delay", key: "aerial", lastRunAt: &recent, wantDelayed: true, wantStack: "aerial", wantSend: true},
		{name: "stale watermark runs immediately without delay", key: "aerial", lastRunAt: &stale, wantStack: "aerial", wantSend: true},
		{name: "compute delay error propagates error", key: "aerial", failRun: errors.New("db compute error"), wantErrSub: "db compute error"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runCovenDreamerArmCase(t, tc)
		})
	}
}

func TestDreamDelay(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC)
	at := func(ago time.Duration) *time.Time { v := now.Add(-ago); return &v }
	tests := []struct {
		name      string
		lastRunAt *time.Time
		want      time.Duration
	}{
		{name: "never ran", lastRunAt: nil, want: 0},
		{name: "zero time", lastRunAt: &time.Time{}, want: 0},
		{name: "ran 2h ago waits the rest of the day", lastRunAt: at(2 * time.Hour), want: DefaultDreamingMinInterval - 2*time.Hour},
		{name: "ran exactly one interval ago", lastRunAt: at(DefaultDreamingMinInterval), want: 0},
		{name: "ran 25h ago", lastRunAt: at(25 * time.Hour), want: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, dreamDelay(tc.lastRunAt, now))
		})
	}
}

func TestCovenDreamer_Dream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		key        string
		nilWorker  bool
		nilDreamer bool
		failOp     string
		storedDue  int64
		reqDue     int64
		wantStack  string
		wantSend   bool
		wantErr    bool
	}{
		{name: "nil dreamer returns error", nilDreamer: true, wantErr: true},
		{name: "nil worker runs safely and schedules next cycle", nilWorker: true, wantStack: config.DefaultStack, wantSend: true},
		{name: "matching token runs consolidation and schedules next cycle", key: "stack-alpha", storedDue: 1700, reqDue: 1700, wantStack: "stack-alpha", wantSend: true},
		{name: "legacy empty payload with no stored token runs", key: "stack-legacy", wantStack: "stack-legacy", wantSend: true},
		{name: "superseded token does nothing", key: "stack-alpha", storedDue: 1800, reqDue: 1700},
		{name: "legacy payload after re-arm does nothing", key: "stack-alpha", storedDue: 1800},
		{name: "consolidate facts error still schedules normal interval", key: "stack-err", failOp: "facts", wantStack: "stack-err", wantSend: true},
		{name: "consolidate skills error still schedules normal interval", key: "stack-err-skills", failOp: "skills", wantStack: "stack-err-skills", wantSend: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			mc := mocks.NewMockContext(t)
			mc.On("Key").Return(tc.key).Maybe()
			due := tc.storedDue
			stubDreamDueState(mc, &due)
			sends := captureCovenSends(t, mc)
			runs := stubCovenRun(mc, true)

			var dreamer *CovenDreamer
			if !tc.nilDreamer {
				var worker *Worker
				if !tc.nilWorker {
					db := newTestDB(t)
					switch tc.failOp {
					case "facts":
						execSQL(t, db, sabotageFacts)
					case "skills":
						execSQL(t, db, sabotageProcedural)
					}
					worker = NewWorker(WorkerConfig{
						Store:  store.NewSQLStore(db),
						Ollama: &MockOllamaClient{embedding: testEmbedding(1.0)},
						LLMClient: func(ctx context.Context, prompt string) (string, error) {
							return `{"facts":[],"clusters":[]}`, nil
						},
						Publisher: &mockSkillPublisher{prNum: 101},
					})
				}
				dreamer = NewCovenDreamer(worker)
			}

			err := dreamer.Dream(restate.WithMockContext(mc), DreamRequest{Due: tc.reqDue})
			if tc.wantErr {
				require.Error(t, err)
				require.True(t, restate.IsTerminalError(err))
				return
			}
			require.NoError(t, err)
			require.False(t, runs.hasRetryCap, "CovenDreamer.Dream must not cap retry attempts")

			calls := sends.snapshot()
			if !tc.wantSend {
				require.Empty(t, calls)
				require.Equal(t, tc.storedDue, due, "a superseded Dream must not touch the live token")
				return
			}
			require.Len(t, calls, 1)
			require.Equal(t, "CovenDreamer", calls[0].service)
			require.Equal(t, tc.wantStack, calls[0].key)
			require.Equal(t, "Dream", calls[0].method)
			require.True(t, calls[0].delayed)
			require.Equal(t, restate.WithDelay(DefaultDreamingMinInterval), calls[0].option)
			require.NotEqual(t, tc.storedDue, due, "Dream must store a fresh due token")
			require.Equal(t, DreamRequest{Due: due}, calls[0].input)
		})
	}
}

// registrationAdminStub serves Restate admin /deployments and ingress CovenDreamer Arm, recording request order.
// /deployments fails with 500 for the first `failures` attempts (forever when negative, cancelling on the 3rd);
// Arm cancels the run so the worker returns.
type registrationAdminStub struct {
	mu       sync.Mutex
	failures int
	cancel   context.CancelFunc
	paths    []string
	bodies   []restateDeploymentRequest
}

func (s *registrationAdminStub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.paths = append(s.paths, r.URL.Path)
	switch r.URL.Path {
	case "/deployments":
		s.serveDeployments(w, r)
	case "/CovenDreamer/aerial/Arm":
		s.cancel()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (s *registrationAdminStub) serveDeployments(w http.ResponseWriter, r *http.Request) {
	var body restateDeploymentRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.bodies = append(s.bodies, body)
	attempt := len(s.bodies)
	if s.failures >= 0 && attempt > s.failures {
		w.WriteHeader(http.StatusCreated)
		return
	}
	if s.failures < 0 && attempt == 3 {
		s.cancel()
	}
	http.Error(w, "boom", http.StatusInternalServerError)
}

// gatheredGauge reads a registered gauge's value from the default Prometheus gatherer.
func gatheredGauge(t *testing.T, name string) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, mf := range families {
		if mf.GetName() == name {
			return mf.GetMetric()[0].GetGauge().GetValue()
		}
	}
	t.Fatalf("metric %s not registered", name)
	return 0
}

func TestRunWorkerAfterRegistration(t *testing.T) {
	// Not t.Parallel(): subtests share the process-wide coven_restate_registered gauge.
	const deployPath = "/deployments"
	const armPath = "/CovenDreamer/aerial/Arm"
	const advertise = "http://coven.default.svc.cluster.local:9080"

	tests := []struct {
		name          string
		adminFailures int // 500s before success; -1 fails forever (ctx cancelled on 3rd attempt)
		noAdminURL    bool
		startWorker   bool
		wantPaths     []string
		wantErr       error
		wantGauge     float64
	}{
		{name: "RegistersThenArms", adminFailures: 0, startWorker: true, wantPaths: []string{deployPath, armPath}, wantErr: context.Canceled, wantGauge: 1},
		{name: "Non2xxRetriesThenArms", adminFailures: 2, startWorker: true, wantPaths: []string{deployPath, deployPath, deployPath, armPath}, wantErr: context.Canceled, wantGauge: 1},
		{name: "NeverRegisteredNeverArms", adminFailures: -1, startWorker: true, wantPaths: []string{deployPath, deployPath, deployPath}, wantErr: context.Canceled, wantGauge: 0},
		{name: "ExtractorRegistersWithoutArming", adminFailures: 0, startWorker: false, wantPaths: []string{deployPath}, wantErr: nil, wantGauge: 1},
		{name: "NoAdminURLSkipsRegistration", noAdminURL: true, startWorker: true, wantPaths: []string{armPath}, wantErr: context.Canceled, wantGauge: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			RestateRegistered.Set(0)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			stub := &registrationAdminStub{failures: tc.adminFailures, cancel: cancel}
			srv := httptest.NewServer(stub)
			defer srv.Close()

			adminURL := srv.URL
			if tc.noAdminURL {
				adminURL = ""
			}
			worker := NewWorker(WorkerConfig{Stack: "aerial", RestateIngressURL: srv.URL, HTTPClient: srv.Client()})

			err := runWorkerAfterRegistration(ctx, srv.Client(), adminURL, advertise, time.Millisecond, worker, tc.startWorker)

			require.ErrorIs(t, err, tc.wantErr) // nil wantErr requires err == nil
			stub.mu.Lock()
			defer stub.mu.Unlock()
			require.Equal(t, tc.wantPaths, stub.paths)
			for _, b := range stub.bodies {
				require.Equal(t, restateDeploymentRequest{URI: advertise, Force: true}, b)
			}
			require.InDelta(t, tc.wantGauge, gatheredGauge(t, "coven_restate_registered"), 0)
		})
	}
}

func TestWorker_StartDreamerArm_Success(t *testing.T) {
	t.Parallel()

	armHandled := make(chan struct{})
	var gotArmEndpoint string
	var gotArmIdempotency string
	ingressServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/CovenDreamer/aerial/Arm") {
			gotArmEndpoint = r.URL.Path
			gotArmIdempotency = r.Header.Get("Idempotency-Key")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
			select {
			case <-armHandled:
			default:
				close(armHandled)
			}
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ingressServer.Close()

	wArm := NewWorker(WorkerConfig{
		Stack:             "aerial",
		RestateIngressURL: ingressServer.URL,
		HTTPClient:        ingressServer.Client(),
	})

	ctxArm, cancelArm := context.WithCancel(context.Background())
	go func() {
		select {
		case <-armHandled:
		case <-time.After(2 * time.Second):
		}
		cancelArm()
	}()
	if err := wArm.Start(ctxArm); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected start error: %v", err)
	}

	select {
	case <-armHandled:
	default:
		t.Fatal("expected arm to be called")
	}
	require.Equal(t, "/CovenDreamer/aerial/Arm", gotArmEndpoint)
	require.Equal(t, "coven:dreamer:arm:aerial", gotArmIdempotency)
}

func TestWorker_StartDreamerArm_Retry(t *testing.T) {
	t.Parallel()

	armRetryAttempts := atomic.Int32{}
	armRetryHandled := make(chan struct{})
	ingressRetryServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/CovenDreamer/aerial/Arm") {
			if armRetryAttempts.Add(1) == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
			select {
			case <-armRetryHandled:
			default:
				close(armRetryHandled)
			}
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ingressRetryServer.Close()

	wArmRetry := NewWorker(WorkerConfig{
		Stack:             "aerial",
		RestateIngressURL: ingressRetryServer.URL,
		HTTPClient:        ingressRetryServer.Client(),
		ArmRetryInterval:  5 * time.Millisecond,
	})
	ctxArmRetry, cancelArmRetry := context.WithCancel(context.Background())
	go func() {
		select {
		case <-armRetryHandled:
		case <-time.After(2 * time.Second):
		}
		cancelArmRetry()
	}()
	if err := wArmRetry.Start(ctxArmRetry); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected start error: %v", err)
	}

	select {
	case <-armRetryHandled:
	default:
		t.Fatal("expected arm retry to succeed")
	}
	require.GreaterOrEqual(t, armRetryAttempts.Load(), int32(2))
}

func TestWorker_StartDreamerArm_ErrorAndExtractor(t *testing.T) {
	t.Parallel()

	errHandled := make(chan struct{})
	ingressErrServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		select {
		case <-errHandled:
		default:
			close(errHandled)
		}
	}))
	defer ingressErrServer.Close()
	wArmErr := NewWorker(WorkerConfig{
		Stack:             "aerial",
		RestateIngressURL: ingressErrServer.URL,
		HTTPClient:        ingressErrServer.Client(),
		ArmRetryInterval:  5 * time.Millisecond,
	})
	ctxArmErr, cancelArmErr := context.WithCancel(context.Background())
	go func() {
		select {
		case <-errHandled:
		case <-time.After(2 * time.Second):
		}
		cancelArmErr()
	}()
	if err := wArmErr.Start(ctxArmErr); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected start error: %v", err)
	}

	select {
	case <-errHandled:
	default:
		t.Fatal("expected arm error endpoint to be called")
	}

	// Extractor role exits immediately on cancellation
	wExtractor := NewWorker(WorkerConfig{Role: "extractor"})
	ctxExt, cancelExt := context.WithCancel(context.Background())
	cancelExt()
	require.ErrorIs(t, wExtractor.Start(ctxExt), context.Canceled)
}

func TestAdminExtract_RestateIngressExtractFn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		path      string
		header    string
		body      string
		wantCode  int
		wantSends []string
	}{
		{name: "UnscopedThreadDefaultsToConfigStackWithoutKey", path: "/admin/extract?thread_id=th-test-admin", wantSends: []string{"/CovenThread/custom-stack/th-test-admin/Extract/send key="}},
		{name: "ScopedDefaultStackIsPreserved", path: "/admin/extract?thread_id=aerial/th-default", wantSends: []string{"/CovenThread/aerial/th-default/Extract/send key="}},
		{name: "RequestIDHeaderKeysOperation", path: "/admin/extract?thread_id=th-1", header: "req-42", wantSends: []string{"/CovenThread/custom-stack/th-1/Extract/send key=admin:extract:custom-stack:req-42"}},
		{name: "RequestIDBodyKeysOperation", path: "/admin/extract", body: `{"thread_id":"aerial/th-2","request_id":" req-7 "}`, wantSends: []string{"/CovenThread/aerial/th-2/Extract/send key=admin:extract:aerial:req-7"}},
		{name: "HeaderWinsOverBodyRequestID", path: "/admin/extract", header: "req-h", body: `{"thread_id":"th-3","request_id":"req-b"}`, wantSends: []string{"/CovenThread/custom-stack/th-3/Extract/send key=admin:extract:custom-stack:req-h"}},
		{name: "ExtractErrorReturns500", path: "/admin/extract?thread_id=th-ext-fail", wantCode: http.StatusInternalServerError, wantSends: []string{"/CovenThread/custom-stack/th-ext-fail/Extract/send key="}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := &ingressRecorder{}
			srv := httptest.NewServer(rec)
			defer srv.Close()
			cfg := &config.Config{Stack: "custom-stack", RestateIngressURL: srv.URL}
			worker := NewWorker(WorkerConfig{Stack: "custom-stack", RestateIngressURL: srv.URL, HTTPClient: srv.Client()})
			server := NewServer(NewToolHandler(cfg, nil, nil), newExtractFn(cfg, worker))

			req := httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(tc.body))
			if tc.header != "" {
				req.Header.Set("X-Request-ID", tc.header)
			}
			rr := httptest.NewRecorder()
			server.Routes().ServeHTTP(rr, req)
			wantCode := http.StatusOK
			if tc.wantCode != 0 {
				wantCode = tc.wantCode
			}
			require.Equal(t, wantCode, rr.Code, rr.Body.String())
			require.Equal(t, tc.wantSends, rec.snapshot())
		})
	}
}

func TestAdminExtractIdempotencyKey(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		stack     string
		requestID string
		want      string
	}{
		{name: "request id names the operation", stack: "agent-alpha", requestID: "r-1", want: "admin:extract:agent-alpha:r-1"},
		{name: "empty stack normalizes to default", stack: "", requestID: "r-2", want: "admin:extract:aerial:r-2"},
		{name: "whitespace request id is trimmed", stack: "aerial", requestID: "  r-3  ", want: "admin:extract:aerial:r-3"},
		{name: "no request id omits the key", stack: "aerial", requestID: "  ", want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, adminExtractIdempotencyKey(tc.stack, tc.requestID))
		})
	}
}

func TestValidateRestateIngress(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		url     string
		wantErr error
	}{
		{name: "configured url passes", url: "http://restate.aerial:8080"},
		{name: "empty url is a config error", url: "", wantErr: errRestateIngressRequired},
		{name: "whitespace url is a config error", url: "   ", wantErr: errRestateIngressRequired},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.ErrorIs(t, validateRestateIngress(tc.url), tc.wantErr)
		})
	}
}

func TestDispatchCovenExtract(t *testing.T) {
	t.Parallel()

	require.NoError(t, dispatchCovenExtract(context.Background(), nil, "aerial", "th-1"))

	var gotEndpoint string
	var gotIdempotency string
	mockRestate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotEndpoint = r.URL.Path
		gotIdempotency = r.Header.Get("Idempotency-Key")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer mockRestate.Close()

	// Ingress with explicit message ID sets Idempotency-Key header and routes to /send
	wIngress := NewWorker(WorkerConfig{
		Stack:             "aerial",
		RestateIngressURL: mockRestate.URL,
		HTTPClient:        mockRestate.Client(),
	})
	require.NoError(t, dispatchCovenExtract(context.Background(), wIngress, "aerial", "th-msg", "custom-msg-id-123"))
	require.Equal(t, "coven:extract:aerial/th-msg:custom-msg-id-123", gotIdempotency)
	require.Contains(t, gotEndpoint, "/CovenThread/aerial/th-msg/Extract/send")

	// Ingress without msgKey omits Idempotency-Key header so distinct events are not collapsed
	require.NoError(t, dispatchCovenExtract(context.Background(), wIngress, "aerial", "th-nomsg"))
	require.Empty(t, gotIdempotency)
}
