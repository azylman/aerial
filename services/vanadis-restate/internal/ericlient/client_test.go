package ericlient

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/eriwire"
)

// eriReplyTransport is a declarative http.RoundTripper standing in for eri:
// it answers every request with status and body, or with a body whose Read
// fails with readErr. It never dials.
type eriReplyTransport struct {
	status  int
	body    string
	readErr error
}

func (r eriReplyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Body != nil {
		_ = req.Body.Close()
	}
	var body io.Reader = strings.NewReader(r.body)
	if r.readErr != nil {
		body = iotest.ErrReader(r.readErr)
	}
	return &http.Response{StatusCode: r.status, Header: http.Header{}, Body: io.NopCloser(body), Request: req}, nil
}

// eriReply is an HTTPEriClient whose eri answers status and body.
func eriReply(status int, body string) *HTTPEriClient {
	return NewHTTPEriClient("http://eri.test", &http.Client{Transport: eriReplyTransport{status: status, body: body}})
}

// eriReadFails is an HTTPEriClient whose eri answers status with a body that
// fails to read.
func eriReadFails(status int) *HTTPEriClient {
	return NewHTTPEriClient("http://eri.test", &http.Client{Transport: eriReplyTransport{status: status, readErr: errors.New("read error")}})
}

// eriClosedURL returns the URL of an httptest server that is already closed,
// so dialing it fails with connection refused on loopback.
func eriClosedURL(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.NotFoundHandler())
	srv.Close()
	return srv.URL
}

// requireErrText asserts err is nil when want is empty, else that its text
// contains want.
func requireErrText(t *testing.T, err error, want string) {
	t.Helper()
	if want == "" {
		require.NoError(t, err)
		return
	}
	require.ErrorContains(t, err, want)
}

func TestHTTPEriClient(t *testing.T) {
	t.Parallel()
	var gotPromptPayload map[string]string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/dispatch":
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		case "/prompt":
			_ = json.NewDecoder(r.Body).Decode(&gotPromptPayload)
		case "/discord/widget":
			_, _ = w.Write([]byte(`{"message_id":"widget-msg-999"}`))
		case "/dispatch/cancel", "/discord/message":
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	client := NewHTTPEriClient(srv.URL, srv.Client())

	ctx := context.Background()
	assert.NoError(t, client.Dispatch(ctx, eriwire.BrainDispatchRequest{ID: "d1"}))
	assert.NoError(t, client.CancelDispatch(ctx, "c1"))
	assert.NoError(t, client.Prompt(ctx, "th1", "wake up"))
	const wantMsgID = "vanadis-prompt-ad174723f0a135a9c1f7db8600f6ecb0"
	assert.Equal(t, wantMsgID, gotPromptPayload["message_id"])
	assert.Equal(t, "th1", gotPromptPayload["channel_id"])
	assert.Equal(t, "wake up", gotPromptPayload["prompt"])
	assert.NoError(t, client.Prompt(ctx, "th1", "wake up"))
	assert.Equal(t, wantMsgID, gotPromptPayload["message_id"])
	assert.NoError(t, client.Prompt(ctx, "th1", "other prompt"))
	assert.NotEqual(t, wantMsgID, gotPromptPayload["message_id"])
	assert.NoError(t, client.Prompt(ctx, "th2", "wake up"))
	assert.NotEqual(t, wantMsgID, gotPromptPayload["message_id"])
	assert.NoError(t, client.Prompt(ctx, "", "wake up"))
	assert.NoError(t, client.Prompt(ctx, "th1", ""))
	msgID, err := client.Widget(ctx, StatusWidgetRequest{ID: "b1"})
	assert.NoError(t, err)
	assert.Equal(t, "widget-msg-999", msgID)
	assert.NoError(t, client.Message(ctx, "ch1", "hello"))
}

func TestHTTPEriClientDispatch(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		client  *HTTPEriClient
		wantErr string
	}{
		{"nil receiver errors", nil, "eri http client is nil"},
		{"empty url errors", NewHTTPEriClient("", nil), "eri url not configured"},
		{"unreachable eri errors", NewHTTPEriClient(eriClosedURL(t), nil), "post eri request: "},
		{"server error carries body", eriReply(http.StatusInternalServerError, "internal error"), "eri returned status 500: internal error"},
		{"server error body read failure", eriReadFails(http.StatusInternalServerError), "eri returned status 500 (failed to read body: read error)"},
		{"empty body is ok", eriReply(http.StatusOK, ""), ""},
		{"ok status is ok", eriReply(http.StatusOK, `{"status":"ok"}`), ""},
		{"skipped active maps to dispatch active", eriReply(http.StatusOK, `{"status":"skipped_active"}`), eriwire.ErrDispatchActive.Error()},
		{"rejected status errors", eriReply(http.StatusOK, `{"status":"rejected"}`), `eri rejected dispatch "d1" (status "rejected")`},
		{"malformed json errors", eriReply(http.StatusOK, `{invalid-json`), "decode eri dispatch response: invalid character 'i'"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			requireErrText(t, tc.client.Dispatch(context.Background(), eriwire.BrainDispatchRequest{ID: "d1"}), tc.wantErr)
		})
	}
}

func TestHTTPEriClientDispatchAccepted(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		client  *HTTPEriClient
		id      string
		wantErr string
	}{
		{"accepted", eriReply(http.StatusOK, `{"status":"accepted","dispatch_id":"d1"}`), "d1", ""},
		{"already exists", eriReply(http.StatusOK, `{"status":"already_exists","dispatch_id":"d1"}`), "d1", ""},
		{"skipped active maps to dispatch active", eriReply(http.StatusOK, `{"status":"skipped_active"}`), "d1", eriwire.ErrDispatchActive.Error()},
		{"identity mismatch errors", eriReply(http.StatusOK, `{"status":"accepted","dispatch_id":"other"}`), "d1", `eri did not accept dispatch "d1" (status "accepted", identity "other")`},
		{"malformed json errors", eriReply(http.StatusOK, `{invalid-json`), "d1", "decode eri dispatch acceptance: invalid character 'i'"},
		{"empty body errors", eriReply(http.StatusOK, ""), "d1", "decode eri dispatch acceptance: unexpected end of JSON input"},
		{"empty request id errors", eriReply(http.StatusOK, `{"status":"accepted","dispatch_id":"d1"}`), "", `eri did not accept dispatch "" (status "accepted", identity "d1")`},
		{"transport error surfaces", eriReply(http.StatusBadGateway, "down"), "d1", "eri returned status 502: down"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			requireErrText(t, tc.client.DispatchAccepted(context.Background(), eriwire.BrainDispatchRequest{ID: tc.id}), tc.wantErr)
		})
	}
}

func TestHTTPEriClientWidget(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		client  *HTTPEriClient
		wantID  string
		wantErr string
	}{
		{"message id decoded", eriReply(http.StatusOK, `{"message_id":"w1"}`), "w1", ""},
		{"server error errors", eriReply(http.StatusInternalServerError, "internal error"), "", "eri returned status 500: internal error"},
		{"non json reply errors", eriReply(http.StatusOK, "not json"), "", "unmarshal eri widget response: invalid character 'o' in literal null"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := tc.client.Widget(context.Background(), StatusWidgetRequest{ID: "b1"})
			requireErrText(t, err, tc.wantErr)
			assert.Equal(t, tc.wantID, got)
		})
	}
}

// The blank-input tables run on a client with no eri URL: a call that gets
// past the blank guard fails with "eri url not configured".

func TestHTTPEriClientCancelDispatch(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		chunkID string
		wantErr string
	}{
		{"blank chunk is a noop", " ", ""},
		{"chunk reaches eri", "c1", "eri url not configured"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			requireErrText(t, NewHTTPEriClient("", nil).CancelDispatch(context.Background(), tc.chunkID), tc.wantErr)
		})
	}
}

func TestHTTPEriClientPrompt(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, thread, prompt, wantErr string
	}{
		{"blank thread is a noop", " ", "prompt", ""},
		{"blank prompt is a noop", "th", " ", ""},
		{"thread and prompt reach eri", "th", "prompt", "eri url not configured"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			requireErrText(t, NewHTTPEriClient("", nil).Prompt(context.Background(), tc.thread, tc.prompt), tc.wantErr)
		})
	}
}

func TestHTTPEriClientMessage(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, channel, content, wantErr string
	}{
		{"blank channel is a noop", " ", "content", ""},
		{"blank content is a noop", "ch", " ", ""},
		{"channel and content reach eri", "ch", "content", "eri url not configured"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			requireErrText(t, NewHTTPEriClient("", nil).Message(context.Background(), tc.channel, tc.content), tc.wantErr)
		})
	}
}

func TestHTTPEriClientPostEriJSON(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		client  *HTTPEriClient
		payload any
		wantErr string
	}{
		{"unmarshalable payload errors", eriReply(http.StatusOK, ""), make(chan int), "marshal request: json: unsupported type: chan int"},
		{"bad url errors", NewHTTPEriClient("http://[::1]:namedport", nil), "test", "create eri request: "},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := tc.client.postEriJSON(context.Background(), "/path", tc.payload, 1024)
			requireErrText(t, err, tc.wantErr)
		})
	}
}
