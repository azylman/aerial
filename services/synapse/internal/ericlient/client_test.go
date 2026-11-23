package ericlient

import (
	"cmp"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/azylman/aerial/services/synapse/internal/metrics"
	"github.com/azylman/aerial/services/synapse/internal/model"
)

const dispatchSnowflake = "100000000000000001"

// dispatchCase is the row shape shared by the per-method Dispatch* tables.
// The fake eri answers 500 to the first failCount hits, then status/body.
type dispatchCase struct {
	name         string
	channel      string
	failCount    int
	status       int
	body         string
	retryDelay   time.Duration
	maxAttempts  int
	noBrainURL   bool
	closedServer bool
	ctxState     string // "" live, "cancelled" before the call, "cancel_on_hit" when the first request lands
	wantHits     int32
	wantRetry    bool
	wantErr      string
}

// newDispatchClient builds the fake eri, the client and the call context
// for one dispatchCase row.
func newDispatchClient(t *testing.T, tc dispatchCase) (*Client, context.Context, *atomic.Int32) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	var hits atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if hit := hits.Add(1); int(hit) <= tc.failCount {
			if tc.ctxState == "cancel_on_hit" {
				cancel()
			}
			http.Error(w, "transient error", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(cmp.Or(tc.status, http.StatusOK))
		_, _ = w.Write([]byte(tc.body))
	}))
	t.Cleanup(ts.Close)
	if tc.closedServer {
		ts.Close()
	}
	brainURL := ts.URL
	if tc.noBrainURL {
		brainURL = ""
	}
	c := NewClient(brainURL, nil)
	c.SetRetryConfig(tc.retryDelay, tc.maxAttempts)
	if tc.ctxState == "cancelled" {
		cancel()
	}
	return c, ctx, &hits
}

func retryCount() float64 {
	return testutil.ToFloat64(metrics.JobRetriesTotal.WithLabelValues(metrics.QueueDefault, "brain_http_dispatch", "brain_http_retry"))
}

// assertDispatch checks the error text, the number of requests the fake
// eri saw and, for retrying rows, that the retry metric moved.
func assertDispatch(t *testing.T, tc dispatchCase, err error, hits int32, retriesBefore float64) {
	t.Helper()
	if tc.wantErr == "" && err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
		t.Fatalf("err = %v, want containing %q", err, tc.wantErr)
	}
	if hits != tc.wantHits {
		t.Errorf("eri hits = %d, want %d", hits, tc.wantHits)
	}
	if tc.wantRetry && retryCount() <= retriesBefore {
		t.Errorf("retry metric did not move from %v", retriesBefore)
	}
}

func TestDispatchDirectMessage(t *testing.T) {
	t.Parallel()
	tests := []dispatchCase{
		{name: "invalid snowflake", channel: "invalid channel!", wantErr: "invalid discord snowflake"},
		{name: "no brain url", channel: dispatchSnowflake, noBrainURL: true, wantErr: "brain URL is not configured"},
		{name: "retry on 500 then success", channel: dispatchSnowflake, failCount: 1, retryDelay: time.Millisecond, body: `{"status":"sent"}`, wantHits: 2, wantRetry: true},
		{name: "400 client error no retry", channel: dispatchSnowflake, status: http.StatusBadRequest, body: `{"error":"bad request"}`, retryDelay: time.Millisecond, wantHits: 1, wantErr: `brain internal message returned status 400: {"error":"bad request"}`},
		{name: "retries exhausted", channel: dispatchSnowflake, failCount: 6, maxAttempts: 2, retryDelay: time.Millisecond, wantHits: 3, wantRetry: true, wantErr: "returned status 500"},
		{name: "cancelled on hit", channel: dispatchSnowflake, failCount: 5, maxAttempts: 3, ctxState: "cancel_on_hit", retryDelay: 5 * time.Second, wantHits: 1, wantErr: "context canceled"},
		{name: "pre-cancelled context", channel: dispatchSnowflake, ctxState: "cancelled", wantErr: "context canceled"},
		{name: "closed server connection error", channel: dispatchSnowflake, closedServer: true, maxAttempts: 1, wantErr: "failed"},
		{name: "default attempts and delay", channel: dispatchSnowflake, maxAttempts: -1, retryDelay: -1, failCount: 1, body: `{"status":"sent"}`, wantHits: 2, wantRetry: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, ctx, hits := newDispatchClient(t, tc)
			before := retryCount()
			err := c.DispatchDirectMessage(ctx, model.DirectMessageRequest{ChannelID: tc.channel, Content: "test message"})
			assertDispatch(t, tc, err, hits.Load(), before)
		})
	}
}

func TestAdvanceBackoffDelay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		delay      time.Duration
		expBackoff bool
		want       time.Duration
	}{
		{name: "no exponential backoff", delay: 100 * time.Millisecond, expBackoff: false, want: 100 * time.Millisecond},
		{name: "exponential backoff scale", delay: 100 * time.Millisecond, expBackoff: true, want: 150 * time.Millisecond},
		{name: "exponential backoff cap", delay: 4 * time.Second, expBackoff: true, want: 5 * time.Second},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := AdvanceBackoffDelay(tc.delay, tc.expBackoff)
			if got != tc.want {
				t.Errorf("AdvanceBackoffDelay(%v, %v) = %v, want %v", tc.delay, tc.expBackoff, got, tc.want)
			}
		})
	}
}

func TestClientMarshalErrors(t *testing.T) {
	t.Parallel()
	c := NewClient("http://localhost:8080", nil)
	unmarshalable := make(chan int)

	if err := c.PostBrainWithRetry(context.Background(), "/prompt", "prompt", "status", "100000000000000001", 1, 0, false, unmarshalable); err == nil {
		t.Errorf("expected marshal error from postBrainWithRetry")
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(ts.Close)
	c2 := NewClient(ts.URL, nil)
	if err := c2.PostBrainWithRetry(context.Background(), "/discord/message", "direct", "status", dispatchSnowflake, 0, 0, false, map[string]string{"k": "v"}); err != nil {
		t.Errorf("postBrainWithRetry with nonpositive maxAttempts and delay err = %v", err)
	}
}

func TestValidateBrainChannel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		channelID    string
		endpointPath string
		wantErr      bool
	}{
		{name: "dispatch endpoint empty channel valid", channelID: "", endpointPath: "/dispatch", wantErr: false},
		{name: "prompt endpoint valid snowflake", channelID: "123456789012345678", endpointPath: "/prompt", wantErr: false},
		{name: "prompt endpoint invalid snowflake", channelID: "not a snowflake", endpointPath: "/prompt", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateBrainChannel(tc.channelID, tc.endpointPath)
			if (err != nil) != tc.wantErr {
				t.Errorf("ValidateBrainChannel(%q, %q) err = %v, wantErr = %v", tc.channelID, tc.endpointPath, err, tc.wantErr)
			}
		})
	}
}

func TestDispatchDiscordToURL_UsesExplicitURL(t *testing.T) {
	t.Parallel()
	var fallbackHits, explicitHits atomic.Int32

	fallbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fallbackHits.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer fallbackServer.Close()

	explicitServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		explicitHits.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer explicitServer.Close()

	c := NewClient(fallbackServer.URL, nil)

	ctx := context.Background()
	msgReq := model.DiscordMessageIngressRequest{
		BotUserID: "bot-1",
		Message:   &discordgo.Message{ID: "m1", Content: "hi"},
	}
	if err := c.DispatchDiscordMessageToURL(ctx, explicitServer.URL, msgReq); err != nil {
		t.Fatalf("DispatchDiscordMessageToURL err = %v", err)
	}

	interaction := &discordgo.Interaction{ID: "i1", Type: discordgo.InteractionApplicationCommand}
	if err := c.DispatchDiscordInteractionToURL(ctx, explicitServer.URL, interaction); err != nil {
		t.Fatalf("DispatchDiscordInteractionToURL err = %v", err)
	}

	if got := explicitHits.Load(); got != 2 {
		t.Errorf("explicitServer hits = %d, want 2", got)
	}
	if got := fallbackHits.Load(); got != 0 {
		t.Errorf("fallbackServer hits = %d, want 0", got)
	}
}

func TestPostDiscordInternalJSONToURL_Errors(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"server error"}`))
	}))
	t.Cleanup(srv.Close)

	closedSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	closedSrv.Close()

	tests := []struct {
		name       string
		brainURL   string
		eriBaseURL string
		client     *http.Client
		payload    any
		wantErr    string
	}{
		{name: "no url configured", brainURL: "", eriBaseURL: "", client: srv.Client(), payload: map[string]string{"k": "v"}, wantErr: "brain URL is not configured"},
		{name: "marshal error", brainURL: srv.URL, eriBaseURL: srv.URL, client: srv.Client(), payload: make(chan int), wantErr: "marshal"},
		{name: "fallback to brainURL", brainURL: srv.URL, eriBaseURL: "", client: srv.Client(), payload: map[string]string{"k": "v"}, wantErr: "returned status 500"},
		{name: "server error 500", brainURL: "", eriBaseURL: srv.URL, client: srv.Client(), payload: map[string]string{"k": "v"}, wantErr: "returned status 500"},
		{name: "connection failed", brainURL: "", eriBaseURL: closedSrv.URL, client: closedSrv.Client(), payload: map[string]string{"k": "v"}, wantErr: "request failed"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := NewClient(tc.brainURL, nil)
			err := c.PostDiscordInternalJSONToURL(context.Background(), tc.eriBaseURL, tc.client, 2*time.Second, "/test", "test", "test status", tc.payload)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v, want substring %q", err, tc.wantErr)
			}
		})
	}
}
