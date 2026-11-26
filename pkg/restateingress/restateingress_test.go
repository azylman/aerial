package restateingress

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/azylman/aerial/pkg/restateingresstest"
)

func init() {
	otel.SetTextMapPropagator(propagation.TraceContext{})
}

type errBody struct{}

func (errBody) Read([]byte) (int, error) { return 0, errors.New("simulated read failure") }

func (errBody) Close() error { return nil }

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

var (
	failReadClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: errBody{}, Header: make(http.Header)}, nil
	})}
	failDoClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("connection refused")
	})}
)

// postPath is the ingress path every TestPost row posts to.
const postPath = "/Svc/key/Enqueue"

func TestPost(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		ctx         context.Context
		client      *http.Client
		endpoint    string
		key         string
		payload     any
		maxBody     int64
		status      int
		respBody    string
		wantBody    string
		wantErr     string
		wantStatus  int
		wantSent    string
		wantKey     string
		wantKeySet  bool
		wantTraceID string
	}{
		{name: "nil payload sends empty body", status: 200, respBody: `{"status":"accepted"}`, wantBody: `{"status":"accepted"}`},
		{name: "payload marshaled with idempotency key", key: " prompt:1:2 ", payload: map[string]string{"prompt": "hi"}, status: 202, respBody: "ok", wantBody: "ok", wantSent: `{"prompt":"hi"}`, wantKey: "prompt:1:2", wantKeySet: true},
		{name: "blank key omitted", key: "   ", payload: map[string]int{"n": 1}, status: 200, respBody: "ok", wantBody: "ok", wantSent: `{"n":1}`},
		{
			name: "active span injects traceparent header",
			ctx: trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
				TraceID:    trace.TraceID{0x4b, 0xf9, 0x2f, 0x35, 0x77, 0xb3, 0x4d, 0xa6, 0xa3, 0xce, 0x92, 0x9d, 0x0e, 0x0e, 0x47, 0x36},
				SpanID:     trace.SpanID{0x00, 0xf0, 0x67, 0xaa, 0x0b, 0xa9, 0x02, 0xb7},
				TraceFlags: trace.FlagsSampled,
			})),
			status:      200,
			respBody:    "ok",
			wantBody:    "ok",
			wantTraceID: "4bf92f3577b34da6a3ce929d0e0e4736",
		},
		{name: "max body truncates", maxBody: 4, status: 200, respBody: "abcdefgh", wantBody: "abcd"},
		{name: "zero max body unbounded", status: 200, respBody: "abcdefgh", wantBody: "abcdefgh"},
		{name: "json message error", status: 409, respBody: `{"message":" busy ","error":"other"}`, wantErr: "restate ingress returned HTTP 409: busy", wantStatus: 409},
		{name: "json error field error", status: 500, respBody: `{"error":" boom "}`, wantErr: "restate ingress returned HTTP 500: boom", wantStatus: 500},
		{name: "raw body error", status: 502, respBody: "bad gateway", wantErr: "restate ingress returned HTTP 502: bad gateway", wantStatus: 502},
		{name: "unmarshalable payload", payload: make(chan int), wantErr: "json: unsupported type: chan int"},
		{name: "bad endpoint", endpoint: "http://bad host", wantErr: `parse "http://bad host": invalid character " " in host name`},
		{name: "transport error", client: failDoClient, endpoint: "http://restate.invalid", wantErr: `Post "http://restate.invalid": connection refused`},
		{name: "read error", client: failReadClient, endpoint: "http://restate.invalid", wantErr: "simulated read failure"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ingress := restateingresstest.New(t)
			ingress.Routes = map[string]restateingresstest.Response{postPath: {Status: tc.status, Body: tc.respBody}}
			endpoint := tc.endpoint
			if endpoint == "" {
				endpoint = ingress.URL() + postPath
			}
			callCtx := tc.ctx
			if callCtx == nil {
				callCtx = context.Background()
			}
			got, err := Post(callCtx, tc.client, endpoint, tc.key, tc.payload, tc.maxBody)
			if tc.wantErr != "" {
				assertPostError(t, err, tc.wantErr, tc.wantStatus)
				return
			}
			if err != nil {
				t.Fatalf("Post() err = %v", err)
			}
			if string(got) != tc.wantBody {
				t.Errorf("Post() body = %q, want %q", got, tc.wantBody)
			}
			assertRecorded(t, ingress.Calls(), tc.wantSent, tc.wantKey, tc.wantKeySet)
			if tc.wantTraceID != "" {
				tp := ingress.Calls()[0].Header.Get("Traceparent")
				if !strings.Contains(tp, tc.wantTraceID) {
					t.Errorf("Traceparent = %q, want trace ID %q", tp, tc.wantTraceID)
				}
			}
		})
	}
}

func assertPostError(t *testing.T, err error, wantErr string, wantStatus int) {
	t.Helper()
	if err == nil || err.Error() != wantErr {
		t.Fatalf("Post() err = %v, want %q", err, wantErr)
	}
	gotStatus := 0
	if httpErr, ok := errors.AsType[*HTTPError](err); ok {
		gotStatus = httpErr.Status
	}
	if gotStatus != wantStatus {
		t.Errorf("HTTPError status = %d, want %d", gotStatus, wantStatus)
	}
}

func assertRecorded(t *testing.T, calls []restateingresstest.Call, wantSent, wantKey string, wantKeySet bool) {
	t.Helper()
	if len(calls) != 1 {
		t.Fatalf("ingress calls = %d, want 1", len(calls))
	}
	c := calls[0]
	if c.Method != http.MethodPost || c.ContentType != "application/json" || c.Path != postPath {
		t.Errorf("request method/type/path = %q/%q/%q, want POST/application/json/%s", c.Method, c.ContentType, c.Path, postPath)
	}
	if string(c.Body) != wantSent {
		t.Errorf("sent body = %q, want %q", c.Body, wantSent)
	}
	_, keySet := c.Header["Idempotency-Key"]
	if c.IdempotencyKey != wantKey || keySet != wantKeySet {
		t.Errorf("Idempotency-Key = %q (set=%v), want %q (set=%v)", c.IdempotencyKey, keySet, wantKey, wantKeySet)
	}
}

func TestParseHTTPError(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		status    int
		body      string
		want      HTTPError
		wantError string
	}{
		{name: "message wins", status: 409, body: `{"message":"m","error":"e"}`, want: HTTPError{Status: 409, Message: "m", Body: `{"message":"m","error":"e"}`}, wantError: "restate ingress returned HTTP 409: m"},
		{name: "error fallback", status: 400, body: `{"message":"  ","error":" e "}`, want: HTTPError{Status: 400, Message: "e", Body: `{"message":"  ","error":" e "}`}, wantError: "restate ingress returned HTTP 400: e"},
		{name: "json without fields", status: 404, body: `{"code":"x"}`, want: HTTPError{Status: 404, Body: `{"code":"x"}`}, wantError: `restate ingress returned HTTP 404: {"code":"x"}`},
		{name: "non json body", status: 503, body: "unavailable", want: HTTPError{Status: 503, Body: "unavailable"}, wantError: "restate ingress returned HTTP 503: unavailable"},
		{name: "empty body", status: 500, body: "", want: HTTPError{Status: 500}, wantError: "restate ingress returned HTTP 500: "},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ParseHTTPError(tc.status, []byte(tc.body))
			if *got != tc.want {
				t.Errorf("ParseHTTPError() = %+v, want %+v", *got, tc.want)
			}
			if got.Error() != tc.wantError {
				t.Errorf("Error() = %q, want %q", got.Error(), tc.wantError)
			}
		})
	}
}

const ingressURL = "<ingress>"

func TestRegisterDeployment(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		client       *http.Client
		adminURL     string
		advertiseURL string
		status       int
		respBody     string
		wantErr      string
	}{
		{name: "empty admin url no-op", adminURL: "", advertiseURL: "http://svc:9080"},
		{name: "empty advertise url no-op", adminURL: ingressURL, advertiseURL: ""},
		{name: "success", adminURL: ingressURL, advertiseURL: "http://svc:9080", status: 200, respBody: `{"id":"dep-1"}`},
		{name: "admin error returns HTTP status and detail", adminURL: ingressURL, advertiseURL: "http://svc:9080", status: 409, respBody: `{"message":"already exists"}`, wantErr: "restate admin /deployments returned 409: already exists"},
		{name: "admin error fallback to raw body", adminURL: ingressURL, advertiseURL: "http://svc:9080", status: 502, respBody: "bad gateway", wantErr: "restate admin /deployments returned 502: bad gateway"},
		{name: "admin error with empty body", adminURL: ingressURL, advertiseURL: "http://svc:9080", status: 500, respBody: "", wantErr: "restate admin /deployments returned 500"},
		{name: "transport failure wraps error", client: failDoClient, adminURL: "http://restate.invalid", advertiseURL: "http://svc:9080", wantErr: "register restate deployment: Post \"http://restate.invalid/deployments\": connection refused"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ingress := restateingresstest.New(t)
			ingress.Routes = map[string]restateingresstest.Response{"/deployments": {Status: tc.status, Body: tc.respBody}}
			adminURL := tc.adminURL
			if adminURL == ingressURL {
				adminURL = ingress.URL()
			}
			client := tc.client
			if client == nil {
				client = ingress.Client()
			}
			err := RegisterDeployment(context.Background(), client, adminURL, tc.advertiseURL)
			if tc.wantErr != "" {
				if err == nil || err.Error() != tc.wantErr {
					t.Fatalf("RegisterDeployment() err = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("RegisterDeployment() unexpected err = %v", err)
			}
		})
	}
}

func TestStartServer(t *testing.T) {
	t.Parallel()
	type row struct {
		name    string
		addr    string
		wantErr bool
	}
	tc := func(name, addr string, wantErr bool) row {
		return row{name: name, addr: addr, wantErr: wantErr}
	}
	tests := []row{
		tc("valid with port only without colon", "0", false),
		tc("listen failure on invalid address", "127.0.0.1:99999999", true),
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			ln, srv, errChan, err := StartServer(ctx, tt.addr, handler)
			if tt.wantErr {
				if err == nil {
					t.Fatal("StartServer() expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("StartServer() err = %v", err)
			}
			defer ln.Close()
			if srv == nil {
				t.Fatal("StartServer() srv is nil")
			}
			cancel()
			<-errChan
		})
	}
}
