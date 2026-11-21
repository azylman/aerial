package restateingress

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

type errBody struct{}

func (errBody) Read([]byte) (int, error) { return 0, errors.New("simulated read failure") }

func (errBody) Close() error { return nil }

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

var (
	failReadClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: errBody{}, Header: make(http.Header)}, nil
	})}
	passthroughClient = &http.Client{Transport: roundTripFunc(http.DefaultTransport.RoundTrip)}
	failDoClient      = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("connection refused")
	})}
)

// recordingServer echoes a fixed response and records the last request it saw.
type recordingServer struct {
	status    int
	respBody  string
	gotBody   string
	gotKey    string
	gotKeySet bool
	gotType   string
	gotMethod string
}

func (s *recordingServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	raw, _ := io.ReadAll(r.Body)
	s.gotBody = string(raw)
	s.gotKey = r.Header.Get("Idempotency-Key")
	_, s.gotKeySet = r.Header["Idempotency-Key"]
	s.gotType = r.Header.Get("Content-Type")
	s.gotMethod = r.Method
	w.WriteHeader(s.status)
	_, _ = w.Write([]byte(s.respBody))
}

func TestPost(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		client     *http.Client
		endpoint   string
		key        string
		payload    any
		maxBody    int64
		status     int
		respBody   string
		wantBody   string
		wantErr    string
		wantStatus int
		wantSent   string
		wantKey    string
		wantKeySet bool
	}{
		{name: "nil payload sends empty body", status: 200, respBody: `{"status":"accepted"}`, wantBody: `{"status":"accepted"}`},
		{name: "payload marshaled with idempotency key", key: " prompt:1:2 ", payload: map[string]string{"prompt": "hi"}, status: 202, respBody: "ok", wantBody: "ok", wantSent: `{"prompt":"hi"}`, wantKey: "prompt:1:2", wantKeySet: true},
		{name: "blank key omitted", key: "   ", payload: map[string]int{"n": 1}, status: 200, respBody: "ok", wantBody: "ok", wantSent: `{"n":1}`},
		{name: "explicit client used", client: passthroughClient, status: 200, respBody: "ok", wantBody: "ok"},
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
			rec := &recordingServer{status: tc.status, respBody: tc.respBody}
			srv := httptest.NewServer(rec)
			t.Cleanup(srv.Close)
			endpoint := tc.endpoint
			if endpoint == "" {
				endpoint = srv.URL + "/Svc/key/Enqueue"
			}
			got, err := Post(context.Background(), tc.client, endpoint, tc.key, tc.payload, tc.maxBody)
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
			assertRecorded(t, rec, tc.wantSent, tc.wantKey, tc.wantKeySet)
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

func assertRecorded(t *testing.T, rec *recordingServer, wantSent, wantKey string, wantKeySet bool) {
	t.Helper()
	if rec.gotMethod != http.MethodPost || rec.gotType != "application/json" {
		t.Errorf("request method/type = %q/%q, want POST/application/json", rec.gotMethod, rec.gotType)
	}
	if rec.gotBody != wantSent {
		t.Errorf("sent body = %q, want %q", rec.gotBody, wantSent)
	}
	if rec.gotKey != wantKey || rec.gotKeySet != wantKeySet {
		t.Errorf("Idempotency-Key = %q (set=%v), want %q (set=%v)", rec.gotKey, rec.gotKeySet, wantKey, wantKeySet)
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
