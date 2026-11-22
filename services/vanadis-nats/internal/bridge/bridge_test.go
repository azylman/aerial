package bridge

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/pubsubtest"
	"github.com/azylman/aerial/pkg/restateingresstest"
	"github.com/azylman/aerial/services/vanadis-nats/internal/config"
)

// consumerBus records each ConsumeQueue binding as "subject durable" and
// fails the subjects listed in consumeErrs; everything else goes to FaultBus.
type consumerBus struct {
	pubsubtest.FaultBus

	consumeErrs map[string]error
	queues      []string
}

func newConsumerBus(t *testing.T, ensureErr error, consumeErrs map[string]error) *consumerBus {
	t.Helper()
	mem := pubsub.NewMemoryBus()
	t.Cleanup(func() { _ = mem.Close() })
	return &consumerBus{Bus: mem, EnsureErr: ensureErr, consumeErrs: consumeErrs}
}

func (b *consumerBus) ConsumeQueue(ctx context.Context, subject, queue string, h pubsub.Handler) (pubsub.Subscription, error) {
	if err := b.consumeErrs[subject]; err != nil {
		return nil, err
	}
	b.queues = append(b.queues, subject+" "+queue)
	return b.FaultBus.ConsumeQueue(ctx, subject, queue, h)
}

func TestNATSBridge_StartConsumers(t *testing.T) {
	t.Parallel()

	const dispatch, pr = "aerial.dispatch.* vanadis-dispatch", "aerial.pr.> vanadis-pr-events"
	const widget = "aerial.coalesced.widget_sync.> vanadis-widget"
	errBoom := errors.New("boom")
	tests := []struct {
		name       string
		bus        pubsub.Bus
		wantErr    string
		wantQueues []string
	}{
		{name: "nil bus returns error", bus: nil, wantErr: "pubsub bus is nil"},
		{name: "ensure core streams error", bus: newConsumerBus(t, errBoom, nil), wantErr: "ensure core streams: boom"},
		{
			name:    "consume queue dispatch error",
			bus:     newConsumerBus(t, nil, map[string]error{"aerial.dispatch.*": errBoom}),
			wantErr: "subscribe vanadis-dispatch queue: boom",
		},
		{
			name:    "consume queue pr-events error",
			bus:     newConsumerBus(t, nil, map[string]error{"aerial.pr.>": errBoom}),
			wantErr: "subscribe vanadis-pr-events queue: boom", wantQueues: []string{dispatch},
		},
		{
			name:       "consume queue widget-events error",
			bus:        newConsumerBus(t, nil, map[string]error{"aerial.coalesced.widget_sync.>": errBoom}),
			wantErr:    "subscribe vanadis-widget queue: boom",
			wantQueues: []string{dispatch, pr},
		},
		{name: "successful consumers registration", bus: newConsumerBus(t, nil, nil), wantQueues: []string{dispatch, pr, widget}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := NewNATSBridge(config.Config{}, tc.bus, nil).StartConsumers(t.Context())
			if tc.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tc.wantErr)
			}
			if cb, ok := tc.bus.(*consumerBus); ok {
				assert.Equal(t, tc.wantQueues, cb.queues)
			}
		})
	}
}

// ingressCase drives one handler against a recording Restate ingress.
// url "" targets the fake ingress; wantPath "" means no request is expected.
// wantCalls lists the message settlement calls (pubsubtest.Message.Calls).
type ingressCase struct {
	name      string
	data      string
	header    nats.Header
	delivered uint64
	status    int
	url       string
	termErr   error
	wantErr   string
	wantCalls []string
	wantPath  string
	wantKey   string
	wantBody  string
}

type ingressHandler func(*NATSBridge, context.Context, pubsub.Message) error

func runIngressCase(t *testing.T, handle ingressHandler, tc ingressCase) {
	t.Helper()
	ingress := restateingresstest.New(t)
	ingress.Routes = map[string]restateingresstest.Response{tc.wantPath: {Status: tc.status}}
	url := tc.url
	if url == "" {
		url = ingress.URL()
	}
	b := NewNATSBridge(config.Config{RestateIngressURL: url}, nil, ingress.Client())
	msg := &pubsubtest.Message{Payload: []byte(tc.data), Hdr: tc.header, Delivered: tc.delivered, TermErr: tc.termErr}

	err := handle(b, t.Context(), msg)
	if tc.wantErr == "" {
		require.NoError(t, err)
	} else {
		require.ErrorContains(t, err, tc.wantErr)
	}
	assert.Equal(t, tc.wantCalls, msg.Calls())

	calls := ingress.Calls()
	require.NoError(t, ingress.Err())
	if tc.wantPath == "" {
		assert.Empty(t, calls)
		return
	}
	require.Len(t, calls, 1)
	assert.Equal(t, http.MethodPost, calls[0].Method)
	assert.Equal(t, tc.wantPath, calls[0].Path)
	assert.Equal(t, tc.wantKey, calls[0].IdempotencyKey)
	assert.JSONEq(t, tc.wantBody, string(calls[0].Body))
}

// closedIngressURL returns the URL of a started-then-closed server, so a POST
// to it fails at the transport layer.
func closedIngressURL(t *testing.T) string {
	t.Helper()
	ts := httptest.NewServer(http.NotFoundHandler())
	ts.Close()
	return ts.URL
}

func msgIDHeader(v string) nats.Header { return nats.Header{pubsub.HeaderMsgID: []string{v}} }

func prBody(event string, n int) string {
	return fmt.Sprintf(`{"event":%q,"repo":"example/repo","pr_number":%d,"occurred_at":"0001-01-01T00:00:00Z"}`, event, n)
}

func dispatchBody(id, chunk, status string) string {
	return fmt.Sprintf(`{"dispatch_id":%q,"source":"","vanadis_chunk_id":%q,"status":%q,`+
		`"started_at":"0001-01-01T00:00:00Z","completed_at":"0001-01-01T00:00:00Z","duration_ms":0}`, id, chunk, status)
}

const badIngressURL = "://invalid-url\x7f"

func TestHandlePRLifecycleEvent(t *testing.T) {
	t.Parallel()

	const path, opened = "/BuildRouter/RoutePREvent/send", `{"event":"opened","repo":"example/repo","pr_number":10}`
	const key = "pr:example/repo:10:opened::"
	closed := closedIngressURL(t)
	errTerm := errors.New("term failed")
	tests := []ingressCase{
		{name: "malformed json returns nil", data: "{not-json"},
		{name: "empty repo returns nil", data: `{"event":"opened","repo":"","pr_number":10}`},
		{name: "zero pr_number returns nil", data: `{"event":"opened","repo":"example/repo","pr_number":0}`},
		{
			name: "202 accepted forwards trimmed lowercase event",
			data: `{"event":" OPENED ","repo":" example/repo ","pr_number":10}`, status: http.StatusAccepted,
			wantPath: path, wantKey: key, wantBody: prBody("opened", 10),
		},
		{
			name: "400 terms",
			data: `{"event":"synchronize","repo":"example/repo","pr_number":10}`, status: http.StatusBadRequest,
			wantCalls: []string{"term"}, wantPath: path, wantKey: "pr:example/repo:10:synchronize::", wantBody: prBody("synchronize", 10),
		},
		{
			name: "404 terms",
			data: `{"event":"check_updated","repo":"example/repo","pr_number":10}`, status: http.StatusNotFound,
			wantCalls: []string{"term"}, wantPath: path, wantKey: "pr:example/repo:10:check_updated::", wantBody: prBody("check_updated", 10),
		},
		{
			name: "400 with term failure still returns nil",
			data: opened, status: http.StatusBadRequest, termErr: errTerm,
			wantCalls: []string{"term"}, wantPath: path, wantKey: key, wantBody: prBody("opened", 10),
		},
		{
			name: "500 naks with backoff",
			data: opened, delivered: 2, status: http.StatusInternalServerError,
			wantErr: "restate ingress error (status 500)", wantCalls: []string{"nak 2s"}, wantPath: path, wantKey: key,
			wantBody: prBody("opened", 10),
		},
		{
			name: "transport error naks with backoff",
			data: opened, delivered: 3, url: closed,
			wantErr: "post to restate ingress " + closed + path, wantCalls: []string{"nak 4s"},
		},
		{name: "request creation error terms", data: opened, url: badIngressURL, wantErr: "create request to ", wantCalls: []string{"term"}},
		{
			name: "request creation error with term failure still returns error",
			data: opened, url: badIngressURL, termErr: errTerm,
			wantErr: "create request to ", wantCalls: []string{"term"},
		},
		{
			name: "MsgID header is the idempotency key",
			data: opened, header: msgIDHeader("custom-pr-id-123"),
			wantPath: path, wantKey: "custom-pr-id-123", wantBody: prBody("opened", 10),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runIngressCase(t, (*NATSBridge).HandlePRLifecycleEvent, tc)
		})
	}
}

func TestHandleDispatchOutcomeEvent(t *testing.T) {
	t.Parallel()

	const path, completed = "/BuildRouter/RouteDispatchOutcome/send", `{"dispatch_id":"disp-1","vanadis_chunk_id":"chk-1","status":"completed"}`
	const key = "dispatch:disp-1:chk-1:completed"
	closed := closedIngressURL(t)
	tests := []ingressCase{
		{name: "malformed json returns nil", data: "{bad-json"},
		{name: "healthcheck-ping ignored", data: `{"dispatch_id":"healthcheck-ping","vanadis_chunk_id":"chk-1","status":"completed"}`},
		{name: "empty vanadis_chunk_id ignored", data: `{"dispatch_id":"disp-1","vanadis_chunk_id":"","status":"completed"}`},
		{name: "unhandled status ignored", data: `{"dispatch_id":"disp-1","vanadis_chunk_id":"chk-1","status":"running"}`},
		{name: "completed forwards", data: completed, wantPath: path, wantKey: key, wantBody: dispatchBody("disp-1", "chk-1", "completed")},
		{
			name:     "failed forwards",
			data:     `{"dispatch_id":"disp-2","vanadis_chunk_id":"chk-2","status":"failed"}`,
			wantPath: path, wantKey: "dispatch:disp-2:chk-2:failed", wantBody: dispatchBody("disp-2", "chk-2", "failed"),
		},
		{
			name:     "timeout forwards",
			data:     `{"dispatch_id":"disp-3","vanadis_chunk_id":"chk-3","status":"timeout"}`,
			wantPath: path, wantKey: "dispatch:disp-3:chk-3:timeout", wantBody: dispatchBody("disp-3", "chk-3", "timeout"),
		},
		{
			name:     "crashed forwards trimmed lowercase status",
			data:     `{"dispatch_id":" disp-4 ","vanadis_chunk_id":" chk-4 ","status":" CRASHED "}`,
			wantPath: path, wantKey: "dispatch:disp-4:chk-4:crashed", wantBody: dispatchBody("disp-4", "chk-4", "crashed"),
		},
		{
			name: "400 terms",
			data: completed, status: http.StatusBadRequest,
			wantCalls: []string{"term"}, wantPath: path, wantKey: key, wantBody: dispatchBody("disp-1", "chk-1", "completed"),
		},
		{
			name: "429 naks with backoff",
			data: completed, delivered: 3, status: http.StatusTooManyRequests,
			wantErr: "restate ingress error (status 429)", wantCalls: []string{"nak 4s"}, wantPath: path, wantKey: key,
			wantBody: dispatchBody("disp-1", "chk-1", "completed"),
		},
		{
			name: "transport error naks with backoff",
			data: completed, delivered: 1, url: closed,
			wantErr: "post to restate ingress " + closed + path, wantCalls: []string{"nak 1s"},
		},
		{
			name: "MsgID header takes precedence",
			data: completed, header: msgIDHeader("custom-dispatch-key"),
			wantPath: path, wantKey: "custom-dispatch-key", wantBody: dispatchBody("disp-1", "chk-1", "completed"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runIngressCase(t, (*NATSBridge).HandleDispatchOutcomeEvent, tc)
		})
	}
}

func TestHandleWidgetSyncEvent(t *testing.T) {
	t.Parallel()

	const buildPath, prPath = "/BuildRouter/RouteBuildReconcile/send", "/BuildRouter/RoutePREvent/send"
	const prArgs = `{"repo":"example/repo","pr_number":42}`
	closed := closedIngressURL(t)
	tests := []ingressCase{
		{name: "malformed json returns nil", data: "{not-valid-json"},
		{name: "empty args returns nil", data: `{}`},
		{name: "repo without positive pr_number returns nil", data: `{"repo":"example/repo","pr_number":0}`},
		{name: "pr_number without repo returns nil", data: `{"repo":"","pr_number":42}`},
		{
			name:     "build_id routes to reconcile with fallback key",
			data:     `{"build_id":" build-987 "}`,
			wantPath: buildPath, wantKey: "widget:build:build-987", wantBody: `"build-987"`,
		},
		{
			name:     "special characters in build_id are preserved in body and key",
			data:     `{"build_id":"build/987#special"}`,
			wantPath: buildPath, wantKey: "widget:build:build/987#special", wantBody: `"build/987#special"`,
		},
		{
			name: "build_id with MsgID header precedence",
			data: `{"build_id":"build-987"}`, header: msgIDHeader("custom-build-msgid"),
			wantPath: buildPath, wantKey: "custom-build-msgid", wantBody: `"build-987"`,
		},
		{
			name:     "build_id with Idempotency-Key header precedence",
			data:     `{"build_id":"build-987"}`,
			header:   nats.Header{"Idempotency-Key": []string{"custom-build-idem"}},
			wantPath: buildPath, wantKey: "custom-build-idem", wantBody: `"build-987"`,
		},
		{
			name:     "repo and pr_number route to RoutePREvent with fallback key",
			data:     prArgs,
			wantPath: prPath, wantKey: "widget:pr:example/repo:42", wantBody: prBody("widget_sync", 42),
		},
		{
			name: "repo and pr_number with MsgID header precedence",
			data: prArgs, header: msgIDHeader("custom-pr-msgid"),
			wantPath: prPath, wantKey: "custom-pr-msgid", wantBody: prBody("widget_sync", 42),
		},
		{
			name: "500 naks with backoff",
			data: prArgs, delivered: 2, status: http.StatusInternalServerError,
			wantErr: "restate ingress error (status 500)", wantCalls: []string{"nak 2s"}, wantPath: prPath, wantKey: "widget:pr:example/repo:42",
			wantBody: prBody("widget_sync", 42),
		},
		{
			name: "transport error naks with backoff",
			data: `{"build_id":"build-err"}`, delivered: 1, url: closed,
			wantErr: "post to restate ingress " + closed + buildPath, wantCalls: []string{"nak 1s"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runIngressCase(t, (*NATSBridge).HandleWidgetSyncEvent, tc)
		})
	}
}

func TestSettleFailedDelivery_Direct(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		delivered    uint64
		nakErr       error
		wantNaked    bool
		wantTermed   bool
		wantNakDelay time.Duration
	}{
		{
			name:      "delivered 1 with nak error logs warning",
			delivered: 1, nakErr: errors.New("nak network err"),
			wantNaked: true, wantNakDelay: 1 * time.Second,
		},
		{name: "delivered 3 naks with 4s backoff", delivered: 3, wantNaked: true, wantNakDelay: 4 * time.Second},
		{name: "delivered 5 naks with 16s backoff", delivered: 5, wantNaked: true, wantNakDelay: 16 * time.Second},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			msg := &pubsubtest.Message{Delivered: tc.delivered, NakErr: tc.nakErr}
			settleFailedDelivery(context.Background(), msg)
			delay, naked := msg.NakDelay()
			assert.Equal(t, tc.wantNaked, naked)
			assert.Equal(t, tc.wantTermed, msg.Termed())
			if tc.wantNakDelay > 0 {
				assert.Equal(t, tc.wantNakDelay, delay)
			}
		})
	}
}

func keyMsg(hdr nats.Header) *pubsubtest.Message { return &pubsubtest.Message{Hdr: hdr} }

var bothKeyHeaders = nats.Header{pubsub.HeaderMsgID: []string{"msg-id-1"}, "Idempotency-Key": []string{"idem-1"}}

func TestPRIdempotencyKey(t *testing.T) {
	t.Parallel()

	ev := pubsub.PRLifecycleEvent{Repo: "example/repo", PRNumber: 42, Event: "opened", HeadSHA: "abc", CheckName: "ci"}
	evAt := ev
	evAt.OccurredAt = time.Unix(100, 200)
	tests := []struct {
		name string
		msg  pubsub.Message
		ev   pubsub.PRLifecycleEvent
		want string
	}{
		{"Idempotency-Key header", keyMsg(nats.Header{"Idempotency-Key": []string{"idem-456"}}), ev, "idem-456"},
		{"MsgID beats Idempotency-Key", keyMsg(bothKeyHeaders), ev, "msg-id-1"},
		{"nil message zero occurred_at fallback", nil, ev, "pr:example/repo:42:opened:abc:ci"},
		{"blank header non-zero occurred_at fallback", keyMsg(msgIDHeader(" ")), evAt, "pr:example/repo:42:opened:abc:ci:100000000200"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, prIdempotencyKey(tc.msg, tc.ev))
		})
	}
}

func TestDispatchIdempotencyKey(t *testing.T) {
	t.Parallel()

	ev := pubsub.DispatchOutcomeEvent{DispatchID: "disp-1", VanadisChunkID: "chk-1", Status: "failed"}
	tests := []struct {
		name string
		msg  pubsub.Message
		ev   pubsub.DispatchOutcomeEvent
		want string
	}{
		{"Idempotency-Key header", keyMsg(nats.Header{"Idempotency-Key": []string{"disp-idem-456"}}), ev, "disp-idem-456"},
		{"MsgID beats Idempotency-Key", keyMsg(bothKeyHeaders), ev, "msg-id-1"},
		{"nil message fallback", nil, ev, "dispatch:disp-1:chk-1:failed"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, dispatchIdempotencyKey(tc.msg, tc.ev))
		})
	}
}

func TestWidgetSyncIdempotencyKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		msg  pubsub.Message
		args pubsub.WidgetSyncJobArgs
		want string
	}{
		{"MsgID beats Idempotency-Key", keyMsg(bothKeyHeaders), pubsub.WidgetSyncJobArgs{BuildID: "b-1"}, "msg-id-1"},
		{"nil message build fallback", nil, pubsub.WidgetSyncJobArgs{BuildID: " b-1 ", Repo: "example/repo", PRNumber: 7}, "widget:build:b-1"},
		{"blank build_id pr fallback", keyMsg(nil), pubsub.WidgetSyncJobArgs{BuildID: " ", Repo: " repo ", PRNumber: 7}, "widget:pr:repo:7"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, widgetSyncIdempotencyKey(tc.msg, tc.args))
		})
	}
}

func TestIsRetryableIngressStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		code int
		want bool
	}{
		{name: "200 ok is not retryable", code: http.StatusOK, want: false},
		{name: "400 bad request is not retryable", code: http.StatusBadRequest, want: false},
		{name: "404 not found is not retryable", code: http.StatusNotFound, want: false},
		{name: "408 request timeout is retryable", code: http.StatusRequestTimeout, want: true},
		{name: "429 too many requests is retryable", code: http.StatusTooManyRequests, want: true},
		{name: "500 internal server error is retryable", code: http.StatusInternalServerError, want: true},
		{name: "502 bad gateway is retryable", code: http.StatusBadGateway, want: true},
		{name: "503 service unavailable is retryable", code: http.StatusServiceUnavailable, want: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, isRetryableIngressStatus(tc.code))
		})
	}
}
