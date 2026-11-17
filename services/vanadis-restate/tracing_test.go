package main

import (
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

	restate "github.com/restatedev/sdk-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/tracing"
)

func TestMain(m *testing.M) {
	// Disabled config installs the W3C propagator with a noop provider, like production
	// without an exporter: traceparent still flows end to end.
	if _, err := tracing.Setup(context.Background(), "vanadis-restate-test", tracing.Config{}); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
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
}

func (requestRunContext) Log() *slog.Logger           { return slog.Default() }
func (r requestRunContext) Request() *restate.Request { return r.req }

func TestRunInSpan_EriClientPropagatesTrace(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		req         *restate.Request
		wantTraceID string
	}{
		{"attempt header", &restate.Request{AttemptHeaders: map[string][]string{"Traceparent": {attemptTP}}, Headers: fakeHeaders{"traceparent": ingressTP}}, "4bf92f3577b34da6a3ce929d0e0e4736"},
		{"ingress header", &restate.Request{Headers: fakeHeaders{"traceparent": ingressTP}}, "0af7651916cd43dd8448eb211c80319c"},
		{"no request", nil, ""},
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

			client := NewHTTPEriClient(srv.URL, tracing.Client(srv.Client()))
			rc := requestRunContext{Context: context.Background(), req: tc.req}
			_, err := runInSpan(rc, spanPrefix+"test/step", func(trc restate.RunContext) (bool, error) {
				assert.Equal(t, tc.req, trc.Request())
				assert.NotNil(t, trc.Log())
				return true, client.Prompt(trc, "th1", "wake")
			})
			require.NoError(t, err)

			tp := <-got
			traceID := ""
			if parts := strings.Split(tp, "-"); len(parts) == 4 {
				traceID = parts[1]
			}
			assert.Equal(t, tc.wantTraceID, traceID)
		})
	}
}
