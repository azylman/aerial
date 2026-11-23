package build

import (
	"bytes"
	"context"
	"encoding/json"
	"iter"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	restate "github.com/restatedev/sdk-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"

	"github.com/azylman/aerial/pkg/pgtest"
	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/vanadis-restate/internal/executor"
)

// TestMain installs tracing and runs the package against one throwaway
// Postgres 18 server loaded from the production schema; every store test
// clones its own database.
func TestMain(m *testing.M) {
	// Disabled config installs the W3C propagator with a noop provider, like production
	// without an exporter: traceparent still flows end to end.
	if _, err := tracing.Setup(context.Background(), "vanadis-restate-test", tracing.Config{}); err != nil {
		panic(err)
	}
	os.Exit(pgtest.Main(m))
}

const (
	attemptTP = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	ingressTP = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
)

func TestTraceparentFromRequest(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		attempt map[string][]string
		headers map[string]string
		want    string
	}{
		{"attempt wins", map[string][]string{"Traceparent": {attemptTP}}, map[string]string{"traceparent": ingressTP}, attemptTP},
		{"ingress fallback", map[string][]string{"X-Other": {"1"}}, map[string]string{"traceparent": ingressTP}, ingressTP},
		{"mixed-case ingress key", nil, map[string]string{"TraceParent": ingressTP}, ingressTP},
		{"empty attempt value falls back", map[string][]string{"traceparent": {" "}}, map[string]string{"traceparent": ingressTP}, ingressTP},
		{"none", nil, nil, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, traceparentFromRequest(tc.attempt, tc.headers))
		})
	}
}

// fakeHeaders satisfies restate.Request.Headers without the SDK's internal constructor.
type fakeHeaders map[string]string

func (h fakeHeaders) Get(key string) string { return h[key] }

func (h fakeHeaders) Iter() iter.Seq2[string, string] {
	return func(yield func(string, string) bool) {
		for k, v := range h {
			if !yield(k, v) {
				return
			}
		}
	}
}

func (h fakeHeaders) ToMap() map[string]string {
	out := make(map[string]string, len(h))
	maps.Copy(out, h)
	return out
}

func (h fakeHeaders) MarshalJSON() ([]byte, error) { return json.Marshal(map[string]string(h)) }

type requestRunContext struct {
	context.Context

	req *restate.Request
	log *slog.Logger
}

func (r requestRunContext) Log() *slog.Logger         { return r.log }
func (r requestRunContext) Request() *restate.Request { return r.req }

func TestRunInSpan_EriClientPropagatesTrace(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		req         *restate.Request
		wantTraceID string
		wantLog     map[string]any
	}{
		{"attempt header", &restate.Request{AttemptHeaders: map[string][]string{"Traceparent": {attemptTP}}, Headers: fakeHeaders{"traceparent": ingressTP}}, "4bf92f3577b34da6a3ce929d0e0e4736", map[string]any{"level": "WARN", "msg": "step warning", "step": "s1", "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736", "span_id": "00f067aa0ba902b7"}},
		{"ingress header", &restate.Request{Headers: fakeHeaders{"traceparent": ingressTP}}, "0af7651916cd43dd8448eb211c80319c", map[string]any{"level": "WARN", "msg": "step warning", "step": "s1", "trace_id": "0af7651916cd43dd8448eb211c80319c", "span_id": "b7ad6b7169203331"}},
		{"no request", nil, "", map[string]any{"level": "WARN", "msg": "step warning", "step": "s1"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := make(chan string, 1)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				got <- r.Header.Get("Traceparent")
				w.WriteHeader(http.StatusOK)
			}))
			defer srv.Close()

			client := executor.NewRestateEriClient(srv.URL, "", "aerial", tracing.Client(srv.Client()))
			var logBuf bytes.Buffer
			logger := slog.New(tracing.SlogHandler(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{
				ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
					if a.Key == slog.TimeKey {
						return slog.Attr{}
					}
					return a
				},
			})))
			rc := requestRunContext{Context: context.Background(), req: tc.req, log: logger}
			_, err := runInSpan(rc, spanPrefix+"test/step", func(trc restate.RunContext) (bool, error) {
				assert.Equal(t, tc.req, trc.Request())
				trc.Log().With("step", "s1").Warn("step warning")
				return true, client.Prompt(trc, "th1", "wake")
			})
			require.NoError(t, err)

			var gotLog map[string]any
			require.NoError(t, json.Unmarshal(logBuf.Bytes(), &gotLog))
			assert.Equal(t, tc.wantLog, gotLog)

			tp := <-got
			traceID := ""
			if parts := strings.Split(tp, "-"); len(parts) == 4 {
				traceID = parts[1]
			}
			assert.Equal(t, tc.wantTraceID, traceID)
		})
	}
}

func TestSpanContextHandler_WithGroup(t *testing.T) {
	t.Parallel()
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{0x01},
		SpanID:     trace.SpanID{0x02},
		TraceFlags: trace.FlagsSampled,
	})
	spanCtx := trace.ContextWithSpanContext(context.Background(), sc)
	var buf bytes.Buffer
	h := spanContextHandler{Handler: slog.NewJSONHandler(&buf, nil), spanCtx: spanCtx}
	grouped, ok := h.WithGroup("test-group").(spanContextHandler)
	require.True(t, ok, "WithGroup must preserve the span handler chain")
	assert.True(t, grouped.spanCtx == spanCtx, "WithGroup must preserve the step span context")
	rec := slog.NewRecord(time.Now(), slog.LevelInfo, "grouped message", 0)
	rec.AddAttrs(slog.String("k", "v"))
	require.NoError(t, grouped.Handle(context.Background(), rec))
	var got map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &got))
	assert.Equal(t, map[string]any{"k": "v"}, got["test-group"])
}
