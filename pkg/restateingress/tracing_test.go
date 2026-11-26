package restateingress

import (
	"context"
	"encoding/json"
	"errors"
	"iter"
	"maps"
	"reflect"
	"testing"

	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/x/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/azylman/aerial/pkg/restatetest"
)

const (
	testAttemptTP  = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	testIngressTP  = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	testFallbackTP = "00-1234567890abcdef1234567890abcdef-fedcba0987654321-01"
)

type testHeaders map[string]string

func (h testHeaders) Get(key string) string { return h[key] }

func (h testHeaders) Iter() iter.Seq2[string, string] {
	return func(yield func(string, string) bool) {
		for k, v := range h {
			if !yield(k, v) {
				return
			}
		}
	}
}

func (h testHeaders) ToMap() map[string]string {
	out := make(map[string]string, len(h))
	maps.Copy(out, h)
	return out
}

func (h testHeaders) MarshalJSON() ([]byte, error) { return json.Marshal(map[string]string(h)) }

func TestTraceparentFromRequest(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		req  *restate.Request
		want string
	}{
		{
			name: "nil_request",
			req:  nil,
			want: "",
		},
		{
			name: "attempt_header_lowercase",
			req: &restate.Request{
				AttemptHeaders: map[string][]string{"traceparent": {testAttemptTP}},
				Headers:        testHeaders{"traceparent": testIngressTP},
			},
			want: testAttemptTP,
		},
		{
			name: "attempt_header_mixed_case",
			req: &restate.Request{
				AttemptHeaders: map[string][]string{"TraceParent": {testAttemptTP}},
			},
			want: testAttemptTP,
		},
		{
			name: "ingress_header_fallback",
			req: &restate.Request{
				Headers: testHeaders{"traceparent": testIngressTP},
			},
			want: testIngressTP,
		},
		{
			name: "empty_headers",
			req: &restate.Request{
				AttemptHeaders: map[string][]string{"traceparent": {" "}},
				Headers:        testHeaders{"traceparent": " "},
			},
			want: "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := TraceparentFromRequest(tc.req)
			assert.Equal(t, tc.want, got)
		})
	}
}

func makeMockContext(t *testing.T, req *restate.Request) restate.Context {
	t.Helper()
	mc := mocks.NewMockContext(t)
	mc.On("Request").Return(req).Maybe()
	mc.On("Run", mock.Anything, mock.Anything, mock.Anything).Return(func(fn func(restate.RunContext) (any, error), output any, _ ...restate.RunOption) restate.TerminalError {
		res, err := fn(restatetest.NewRunContext())
		if err != nil {
			return restate.ToTerminalError(err)
		}
		if output != nil && res != nil {
			outVal := reflect.ValueOf(output)
			if outVal.Kind() == reflect.Pointer && !outVal.IsNil() {
				outVal.Elem().Set(reflect.ValueOf(res))
			}
		}
		return nil
	}).Maybe()
	return restate.WithMockContext(mc)
}

func TestContextWithTrace(t *testing.T) {
	t.Parallel()

	t.Run("prefers request header over fallback", func(t *testing.T) {
		t.Parallel()
		rc := makeMockContext(t, &restate.Request{
			Headers: testHeaders{"traceparent": testIngressTP},
		})
		ctx := ContextWithTrace(context.Background(), rc, testFallbackTP)
		sc := trace.SpanContextFromContext(ctx)
		assert.True(t, sc.IsValid())
		assert.Equal(t, "0af7651916cd43dd8448eb211c80319c", sc.TraceID().String())
	})

	t.Run("uses fallback when request header is empty", func(t *testing.T) {
		t.Parallel()
		rc := makeMockContext(t, &restate.Request{})
		ctx := ContextWithTrace(context.Background(), rc, "", testFallbackTP)
		sc := trace.SpanContextFromContext(ctx)
		assert.True(t, sc.IsValid())
		assert.Equal(t, "1234567890abcdef1234567890abcdef", sc.TraceID().String())
	})
}

func TestRunInSpan(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	prevTP := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	defer otel.SetTracerProvider(prevTP)

	rc := makeMockContext(t, &restate.Request{
		AttemptHeaders: map[string][]string{"traceparent": {testAttemptTP}},
	})

	t.Run("records child span parented to incoming trace ID", func(t *testing.T) {
		exp.Reset()
		var callbackSpanCtx trace.SpanContext
		val, err := RunInSpan(rc, "step.success", func(ctx context.Context, _ restate.RunContext) (string, error) {
			callbackSpanCtx = trace.SpanContextFromContext(ctx)
			return "done", nil
		})
		require.NoError(t, err)
		assert.Equal(t, "done", val)

		spans := exp.GetSpans()
		require.Len(t, spans, 1)
		span := spans[0]
		assert.Equal(t, "step.success", span.Name)
		assert.Equal(t, trace.SpanKindInternal, span.SpanKind)
		assert.Equal(t, "4bf92f3577b34da6a3ce929d0e0e4736", span.Parent.TraceID().String())
		assert.Equal(t, "00f067aa0ba902b7", span.Parent.SpanID().String())
		assert.Equal(t, span.SpanContext.SpanID(), callbackSpanCtx.SpanID())
		assert.NotEqual(t, span.Parent.SpanID(), callbackSpanCtx.SpanID())
		assert.Equal(t, codes.Unset, span.Status.Code)
	})

	t.Run("propagates cancellation from run context to step context", func(t *testing.T) {
		cancelCtx, cancel := context.WithCancel(context.Background())
		cancel()

		mc := mocks.NewMockContext(t)
		mc.On("Request").Return(&restate.Request{}).Maybe()
		mc.On("Run", mock.Anything, mock.Anything, mock.Anything).Return(func(fn func(restate.RunContext) (any, error), output any, _ ...restate.RunOption) restate.TerminalError {
			runCtx := restatetest.NewRunContext(cancelCtx)
			res, err := fn(runCtx)
			if err != nil {
				return restate.ToTerminalError(err)
			}
			if output != nil && res != nil {
				outVal := reflect.ValueOf(output)
				if outVal.Kind() == reflect.Pointer && !outVal.IsNil() {
					outVal.Elem().Set(reflect.ValueOf(res))
				}
			}
			return nil
		}).Maybe()
		cancellingRC := restate.WithMockContext(mc)

		var stepDoneFired bool
		_, err := RunInSpan(cancellingRC, "step.cancelled", func(ctx context.Context, _ restate.RunContext) (string, error) {
			select {
			case <-ctx.Done():
				stepDoneFired = true
			default:
				stepDoneFired = false
			}
			return "done", nil
		})
		require.NoError(t, err)
		assert.True(t, stepDoneFired, "stepCtx must inherit cancellation from runCtx")
	})

	t.Run("sets codes.Error on step error", func(t *testing.T) {
		exp.Reset()
		stepErr := errors.New("boom")
		_, err := RunInSpan(rc, "step.failure", func(_ context.Context, _ restate.RunContext) (int, error) {
			return 0, stepErr
		})
		require.Error(t, err)

		spans := exp.GetSpans()
		require.Len(t, spans, 1)
		span := spans[0]
		assert.Equal(t, "step.failure", span.Name)
		assert.Equal(t, codes.Error, span.Status.Code)
		assert.Equal(t, "step failed", span.Status.Description)
	})
}
