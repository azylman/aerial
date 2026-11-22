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

	"github.com/stretchr/testify/assert"

	"github.com/azylman/aerial/pkg/eriwire"
)

type badReader struct{}

func (badReader) Read([]byte) (int, error) {
	return 0, errors.New("read error")
}

type roundTripFunc func(req *http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

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

	// Test errors
	badClient := NewHTTPEriClient("http://invalid.localhost:99", srv.Client())
	assert.Error(t, badClient.Dispatch(ctx, eriwire.BrainDispatchRequest{}))
}

func TestHTTPEriClientTransport(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	srv500 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("internal error"))
	}))
	t.Cleanup(srv500.Close)

	tests := []struct {
		name    string
		client  string
		op      string
		wantErr bool
	}{
		{name: "nil receiver dispatch errors", client: "nil", op: "dispatch", wantErr: true},
		{name: "empty url dispatch errors", client: "empty", op: "dispatch", wantErr: true},
		{name: "empty cancel is noop", client: "empty", op: "cancel-empty"},
		{name: "empty prompt thread is noop", client: "empty", op: "prompt-empty-thread"},
		{name: "empty prompt text is noop", client: "empty", op: "prompt-empty-text"},
		{name: "empty message channel is noop", client: "empty", op: "message-empty-channel"},
		{name: "empty message content is noop", client: "empty", op: "message-empty-content"},
		{name: "server error dispatch errors", client: "server500", op: "dispatch", wantErr: true},
		{name: "server error widget errors", client: "server500", op: "widget", wantErr: true},
		{name: "marshal error errors", client: "server500", op: "marshal", wantErr: true},
		{name: "bad url request errors", client: "badurl", op: "rawpost", wantErr: true},
		{name: "unreachable dispatch errors", client: "unreachable", op: "dispatch", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var c *HTTPEriClient
			switch tc.client {
			case "nil":
			case "empty":
				c = NewHTTPEriClient("", nil)
			case "server500":
				c = NewHTTPEriClient(srv500.URL, srv500.Client())
			case "badurl":
				c = NewHTTPEriClient("http://[::1]:namedport", srv500.Client())
			case "unreachable":
				c = NewHTTPEriClient("http://invalid.localhost:99", srv500.Client())
			}
			var err error
			switch tc.op {
			case "dispatch":
				err = c.Dispatch(ctx, eriwire.BrainDispatchRequest{})
			case "widget":
				_, err = c.Widget(ctx, StatusWidgetRequest{})
			case "cancel-empty":
				err = c.CancelDispatch(ctx, "")
			case "prompt-empty-thread":
				err = c.Prompt(ctx, "", "prompt")
			case "prompt-empty-text":
				err = c.Prompt(ctx, "th", "")
			case "message-empty-channel":
				err = c.Message(ctx, "", "content")
			case "message-empty-content":
				err = c.Message(ctx, "ch", "")
			case "marshal":
				_, err = c.postEriJSON(ctx, "/path", make(chan int), 1024)
			case "rawpost":
				_, err = c.postEriJSON(ctx, "/path", "test", 1024)
			}
			if tc.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestHTTPEriClientDispatchResponses(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name       string
		body       string
		status     int
		accepted   bool
		id         string
		wantErr    bool
		wantActive bool
	}{
		{name: "dispatch skipped active", body: `{"status":"skipped_active"}`, status: http.StatusOK, wantActive: true},
		{name: "dispatch rejected", body: `{"status":"rejected"}`, status: http.StatusOK, wantErr: true},
		{name: "dispatch malformed json", body: `{invalid-json`, status: http.StatusOK, wantErr: true},
		{name: "accepted dispatch", body: `{"status":"accepted","dispatch_id":"d1"}`, status: http.StatusOK, accepted: true, id: "d1"},
		{name: "already exists dispatch", body: `{"status":"already_exists","dispatch_id":"d1"}`, status: http.StatusOK, accepted: true, id: "d1"},
		{name: "accepted skipped active", body: `{"status":"skipped_active"}`, status: http.StatusOK, accepted: true, id: "d1", wantActive: true},
		{name: "accepted identity mismatch", body: `{"status":"accepted","dispatch_id":"other"}`, status: http.StatusOK, accepted: true, id: "d1", wantErr: true},
		{name: "accepted malformed json", body: `{invalid-json`, status: http.StatusOK, accepted: true, id: "d1", wantErr: true},
		{name: "dispatch empty body ok", body: ``, status: http.StatusOK},
		{name: "accepted empty body errors", body: ``, status: http.StatusOK, accepted: true, id: "d1", wantErr: true},
		{name: "accepted empty id errors", body: `{"status":"accepted","dispatch_id":"d1"}`, status: http.StatusOK, accepted: true, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()
			c := NewHTTPEriClient(srv.URL, srv.Client())
			var err error
			if tc.accepted {
				err = c.DispatchAccepted(ctx, eriwire.BrainDispatchRequest{ID: tc.id})
			} else {
				err = c.Dispatch(ctx, eriwire.BrainDispatchRequest{ID: tc.id})
			}
			if tc.wantActive {
				if !errors.Is(err, eriwire.ErrDispatchActive) {
					t.Errorf("expected eriwire.ErrDispatchActive, got %v", err)
				}
				return
			}
			if tc.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestHTTPEriClient_ReadError(t *testing.T) {
	t.Parallel()
	client := &http.Client{
		Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusInternalServerError,
				Body:       io.NopCloser(badReader{}),
			}, nil
		}),
	}
	c := NewHTTPEriClient("http://eri.test", client)
	err := c.Dispatch(context.Background(), eriwire.BrainDispatchRequest{})
	if err == nil {
		t.Fatal("expected error")
	}

	clientBadJSON := &http.Client{
		Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader("not json")),
			}, nil
		}),
	}
	cBadJSON := NewHTTPEriClient("http://eri.test", clientBadJSON)
	_, err = cBadJSON.Widget(context.Background(), StatusWidgetRequest{})
	if err == nil {
		t.Fatal("expected unmarshal error")
	}
}
